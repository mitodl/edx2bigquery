#!/usr/bin/python
#
# Analyze courseware_studentmodule and tracking logs specifically for capa problems.
#
# 1. Find subset of studentmodule entries reating to problems
#    extract table ("problem_analysis") of:
#
#    course_id, user_id, problem_url_name, item: { answer_id, response, correctness} , npoints, attempts, seed, done, grade, created
#
# 2. Extract from tracking logs all problem_check attempts and fill in information in the problem_check table
#

import os, sys
import csv
import re
import json
import gsutil
import bqutil
import datetime
import process_tracking_logs

from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, openfile

csv.field_size_limit(1310720)

def analyze_problems(course_id, basedir=None, datedir=None, force_recompute=False,
                     use_dataset_latest=False,
                     do_problem_grades=True,
                     do_show_answer=True,
                     do_problem_analysis=True,
                     only_step=None,
                     ):
    '''
    1. Construct the problem_grades table, generated from the studentmodule table.
       This is simple, so we do that first.  

    2. Construct the show_answer_stats_by_user table.

    3. Construct the problem_analysis table, based on working through the local copy
       of the studntmodule.csv file for a course.  
   
       This table contains one line per (course_id, user_id, problem_url_name), with each line
       having one or more records for each item (question with answer box) in the problem.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    if only_step:
        only_step = only_step.split(',')
    else:
        only_step = ['grades', 'show_answer', 'analysis']

    if do_problem_grades and ('grades' in only_step):
        make_problem_grades_table(course_id, dataset, force_recompute)
        make_chapter_grades_table(course_id, dataset, force_recompute)

    if do_show_answer and ('show_answer' in only_step):
        make_show_answer_stats_by_user_table(course_id, dataset, force_recompute)
        make_show_answer_stats_by_course_table(course_id, dataset, force_recompute)

    if do_problem_analysis and ('analysis' in only_step):
        make_problem_analysis(course_id, basedir, datedir, force_recompute=force_recompute, 
                              use_dataset_latest=use_dataset_latest)

    #-----------------------------------------------------------------------------

def make_problem_analysis(course_id, basedir=None, datedir=None, force_recompute=False,
                          use_dataset_latest=False):

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest)
    
    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_problem_analysis.json' % mypath
    the_schema = json.loads(open(SCHEMA_FILE).read())['problem_analysis']
    the_dict_schema = schema2dict(the_schema)

    smfn = lfp / 'studentmodule.csv'
    smfp = openfile(smfn)
    if smfp is None:
        print "--> [analyze_problems] oops, missing %s, cannot process course %s" % (smfn, course_id)
        return

    print "[analyze_problems] processing %s for course %s to create problem_analysis table" % (smfn, course_id)
    sys.stdout.flush()

    if smfp.name.endswith('.gz'):
        smfn += '.gz'
    sm_moddate = gsutil.get_local_file_mtime_in_utc(smfn, make_tz_unaware=True)

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = 'problem_analysis'

    # if table already exists, then check its modification time to see if it's older
    if not force_recompute:
        try:
            table_moddate = bqutil.get_bq_table_last_modified_datetime(dataset, table)
        except Exception as err:
            if "Not Found" in str(err):
                table_moddate = None
            else:
                raise
        
        if table_moddate is not None:
            try:
                is_up_to_date = table_moddate > sm_moddate
            except Exception as err:
                print "oops, cannot compare %s with %s to get is_up_to_date" % (table_moddate, sm_moddate)
                raise
    
            if is_up_to_date:
                print "--> %s.%s already exists in BigQuery-date=%s (sm date=%s)...skipping (use --force-recompute to not skip)" % (dataset, 
                                                                                                                                    table,
                                                                                                                                    table_moddate,
                                                                                                                                    sm_moddate,
                                                                                                                                    )
                return

    data = []
    nlines = 0
    cnt = 0
    for line in csv.DictReader(smfp):
        nlines += 1
        uid = int(line['student_id'])
        if not line['module_type']=='problem':  # bug in edX platform?  too many entries are type=problem
            continue
        mid = line['module_id']

        # May 2015: new location syntax for module_id's, e.g.:
        # block-v1:HarvardX+BUS5.1x+3T2015+type@sequential+block@34c9c30a2bd3486f9e63e18552818286

        (org, num, category, url_name) = mid.rsplit('/',3)
        if not category=='problem':     # filter based on category info in module_id
            continue
        try:
            state = json.loads(line['state'].replace('\\\\','\\'))
        except Exception as err:
            print "oops, failed to parse state in studentmodule entry, err=%s" % str(err)
            print "    %s" % repr(line)
            continue
        
        if 'correct_map' not in state:
            continue
        
        if not state['correct_map']:    # correct map = {} is not of interest
            continue

        answers = state['student_answers']

        items = []
        for aid, cm in state['correct_map'].iteritems():
            item = { 'answer_id': aid,
                     'correctness': cm['correctness'],
                     'correct_bool' : cm['correctness']=='correct',
                     'npoints': cm['npoints'],
                     'msg': cm['msg'],
                     'hint': cm['hint'],
                     'response': json.dumps(answers.get(aid, '')),
                     }
            items.append(item)

        try:
            entry = {'course_id': line['course_id'],
                     'user_id': line['student_id'],
                     'problem_url_name': url_name,
                     'item': items,
                     'attempts': int(state['attempts']),
                     'done': state['done'],
                     'grade': float(line['grade']),
                     'max_grade': float(line['max_grade']),
                     'created': line['created'],
                     }
        except Exception as err:
            print "---> [%d] Oops, error in transcribing entry, err=%s" % (cnt, str(err))
            print "     state = %s" % state
            raise

        check_schema(cnt, entry, the_ds=the_dict_schema, coerce=True)
        data.append(entry)
        cnt += 1

    print "%d problem lines extracted from %d lines in %s" % (cnt, nlines, smfn)

    if cnt==0:
        print "--> No final data: not saving or importing into BigQuery"
        return

    # write out result
    ofnb = 'problem_analysis.json.gz'
    ofn = lfp / ofnb
    ofp = openfile(ofn, 'w')
    for entry in data:
        ofp.write(json.dumps(entry) + '\n')
    ofp.close()

    # upload and import
    gsfn = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest) / ofnb
    gsutil.upload_file_to_gs(ofn, gsfn)

    bqutil.load_data_to_table(dataset, table, gsfn, the_schema, wait=True)
        
#-----------------------------------------------------------------------------

def attempts_correct(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    make stats_attempts_corrct table for specified course_id.

    This table has percentage of attempts which are correct, computed for each user.
    Also includes fraction of all problems which were completed, where the number of
    all problems is determined by the max over all users.

    The table computes the statistics for all viewers.
    '''
    SQL = """# problem attempt correctness percentage, including whether user was certified
             SELECT *, 
               round(nproblems / total_problems, 4) as frac_complete,
             FROM
             (
               SELECT *,
                 max(nproblems) over () as total_problems,
               FROM
               (
               SELECT
                 "{course_id}" as course_id,
                 PA.user_id as user_id,  
                 PC.certified as certified,
                 PC.explored as explored,
                 sum(case when PA.item.correct_bool then 1 else 0 end)
                 / count(PA.item.correct_bool) * 100.0 as percent_correct,
                 count(PA.item.correct_bool) as nattempts,
                 case when nshow_answer_unique_problems is null then 0 else nshow_answer_unique_problems end as nshow_answer_unique_problems,
                 count(DISTINCT problem_url_name) as nproblems
                 FROM [{dataset}.problem_analysis] as PA
                 JOIN EACH
                 (
                   SELECT user_id, certified, explored, viewed, nshow_answer_unique_problems
                   FROM
                   [{dataset}.person_course] a
                   LEFT OUTER JOIN
                   (
                     SELECT username, INTEGER(count(*)) AS nshow_answer_unique_problems
                     FROM
                     (
                       SELECT username, module_id
                       FROM [{dataset}.show_answer]
                       GROUP BY username, module_id
                     )
                     GROUP BY username
                   ) b
                   ON a.username = b.username
                 ) as PC
                 ON PA.user_id = PC.user_id
                 where PC.viewed                 # only participants (viewers)
                 group by user_id, certified, explored, nshow_answer_unique_problems
               )
             )
             order by certified desc, explored desc, percent_correct desc
          """
    
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = 'stats_attempts_correct'
        
    the_sql = SQL.format(dataset=dataset, course_id=course_id)
    bqdat = bqutil.get_bq_table(dataset, tablename, the_sql,
                                force_query=force_recompute, 
                                newer_than=datetime.datetime(2015, 5, 17, 16, 00),
                                depends_on=[ '%s.problem_analysis' % dataset ],
                                )
    return bqdat

#-----------------------------------------------------------------------------

def problem_check_tables(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    make problem_check table for specified course_id.

    The master table holds all the problem_check events extracted from
    the tracking logs for a course.  It isn't split into separate
    days.  It is ordered in time, however.  To update it, a new day's logs
    are processed, then the results appended to this table.

    If the problem_check table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.

    If the query fails because of "Resources exceeded during query execution"
    then try setting the end_date, to do part at a time.
    '''
    
    SQL = """
               SELECT 
                   time, 
                   username,
                   #context.user_id as user_id,
                   '{course_id}' as course_id,
                   module_id,
                   event_struct.answers as student_answers,
                   event_struct.attempts as attempts,
                   event_struct.success as success,
                   event_struct.grade as grade,
               from {DATASETS}
               where (event_type = "problem_check" or event_type = "save_problem_check")
                  and event_source = "server"
                  and time > TIMESTAMP("{last_date}")
               order by time;
            """

    table = 'problem_check'

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
        
#-----------------------------------------------------------------------------

def make_show_answer_table(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    make show_answer table for specified course_id.

    The master table holds all the show_answer events extracted from
    the tracking logs for a course, ordered in time.  To update it, a
    new day's logs are processed, then the results appended to this
    table.

    Elsewhere, these data can be correlated with courseware_studentmodule table
    data to determine, for example, the percentage of questions attempted
    for which the leaner clicked on "show answer".
    '''
    
    SQL = """
               SELECT 
                   time, 
                   username,
                   # context.user_id as user_id,
                   '{course_id}' as course_id,
                   module_id,
               from {DATASETS}
               where (event_type = "show_answer" or event_type = "showanswer")
                  and time > TIMESTAMP("{last_date}")
               order by time;
            """

    table = 'show_answer'

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
        
#-----------------------------------------------------------------------------

def make_problem_grades_table(course_id, dataset, force_recompute):
    '''
    Grades, from studentmodule

    Each row has one (user_id, problem) and indicates the grade
    obtained, and if the problem was attempted.
    '''
    pg_sql = """
                SELECT student_id as user_id,
                  module_id,
                  (case when (grade is not null and grade !="NULL") then FLOAT(grade) end) as grade,
                  FLOAT(max_grade) as max_grade,
                  (case when FLOAT(grade) >= FLOAT(max_grade) then true else false end) as perfect,
                  (case when (grade is null or grade = "NULL") then false else true end) as attempted,
                FROM [{dataset}.studentmodule]
                # where grade is not null and grade != "NULL"
                where module_type = "problem"
             """.format(dataset=dataset)

    pg_table = "problem_grades"
    print "[analyze_problems] Creating %s.problem_grades table for %s" % (dataset, course_id)
    sys.stdout.flush()
    bqdat = bqutil.get_bq_table(dataset, pg_table, pg_sql, force_query=force_recompute,
                                depends_on=["%s.studentmodule" % dataset],
                                allowLargeResults=True,
                                startIndex=-2)
        
#-----------------------------------------------------------------------------

def make_chapter_grades_table(course_id, dataset, force_recompute):
    '''
    Grades, from problems and chapters (via course axis)

    Each row has one (user_id, chapter) and indicates the grade
    obtained, via summing over equally weighted problem grades in the chapter.
    '''
    cg_sql = """
        # generate one (user_id, chapter, chgrade, median_grade, chmax) row
        # take the global-within-chapter user_chapter_max_grade over all users as the max_chapter_grade
        # so we can then use that later for computing chapter grade histogram
        SELECT
            *,
            PERCENTILE_DISC(0.5) over (partition by chapter_mid order by chgrade) as median_grade,
            NTH_VALUE(user_chapter_max_grade, 1)
                over (partition by chapter_mid order by user_chapter_max_grade desc) as chmax,
        FROM
        (
            # sum grades for each user in chapter, weighting all problems equally within each chapter
            SELECT
                user_id,
                chapter_mid,
                # course_axis_index,
                sum(max_grade) as user_chapter_max_grade,
                sum(grade) as chgrade,
                max(due_date) as due_date_max,
                min(due_date) as due_date_min,
            FROM
            (
                # get chapter ID's for each problem
                SELECT PG.user_id as user_id,
                  PG.module_id as module_id,
                  PG.grade as grade,
                  PG.max_grade as max_grade,
                  CA.name as name,
                  CA.gformat as gformat,
                  CA.chapter_mid as chapter_mid,
                  # CA.index as course_axis_index,  # this is the problem's index, not the chapter's!
                  CA.due as due_date,
                FROM [{dataset}.problem_grades] as PG
                JOIN (
                    SELECT *,
                        CONCAT('i4x://', module_id) as i4x_module_id,
                    FROM [{dataset}.course_axis]
                ) as CA
                ON CA.i4x_module_id = PG.module_id
                WHERE PG.grade is not null
                order by due_date
            )
            group by user_id, chapter_mid
            order by user_id
        )
        order by user_id
             """.format(dataset=dataset)

    cg_table = "chapter_grades"
    print "[analyze_problems] Creating %s.chapter_grades table for %s" % (dataset, course_id)
    sys.stdout.flush()

    try:
        tinfo = bqutil.get_bq_table_info(dataset, 'course_axis')
        assert tinfo is not None
    except Exception as err:
        print " --> Err: missing %s.%s?  Skipping creation of chapter_grades" % (dataset, "course_axis")
        sys.stdout.flush()
        return

    bqdat = bqutil.get_bq_table(dataset, cg_table, cg_sql, force_query=force_recompute,
                                depends_on=["%s.problem_grades" % dataset, "%s.course_axis" % dataset],
                                newer_than=datetime.datetime(2015, 3, 20, 18, 21),
                                allowLargeResults=True,
                                startIndex=-2)
        
#-----------------------------------------------------------------------------

def make_show_answer_stats_by_user_table(course_id, dataset, force_recompute):
    '''
    show_answer_stats_by_user is generated from the problem_grades,
    person_course, and show_answer tables.  Each line has a user_id,
    and number of show_answer events for problems not attempted,
    attempted, answered perfectly, and partially answered, as well as
    the number of problems viewed by the student.

    This table can be aggregated to obtain the statistical probability
    of a user clicking on "show answer" for problems, for various
    conditions, e.g.  certified vs. viewed.

    '''
    
    sasbu_table = "show_answer_stats_by_user"

    sasbu_sql = """
# show_answer_stats_by_user

SELECT 
  user_id,
  explored,
  certified,
  verified,
  
  # compute percentages of problems (under various conditions) when show_answer was clicked
  (n_show_answer_problem_seen / n_problems_seen * 100) as pct_show_answer_problem_seen,
  (n_show_answer_not_attempted / n_not_attempted * 100) as pct_show_answer_not_attempted,
  (n_show_answer_attempted / n_attempted * 100) as pct_show_answer_attempted,
  (n_show_answer_perfect / n_perfect * 100) as pct_show_answer_perfect,
  (n_show_answer_partial / n_partial * 100) as pct_show_answer_partial,

  n_show_answer_problem_seen,
  n_problems_seen,

  n_show_answer_not_attempted,
  n_not_attempted,

  n_show_answer_attempted,
  n_attempted,

  n_show_answer_perfect,
  n_perfect,

  n_show_answer_partial,
  n_partial,
FROM
(
    # inner SQL: join (problem grades, person_course) with show_answer table, with one line per user
    SELECT
        A.user_id as user_id,
        PC.explored as explored,
        PC.certified as certified,
        (case when PC.mode = "verified" then true else false end) as verified,
        A.n_show_answer_not_attempted as n_show_answer_not_attempted,
        A.n_not_attempted as n_not_attempted,
        A.n_show_answer_attempted as n_show_answer_attempted,
        A.n_attempted as n_attempted,
        A.n_show_answer_perfect as n_show_answer_perfect,
        A.n_perfect as n_perfect,
        A.n_show_answer_partial as n_show_answer_partial,
        A.n_partial as n_partial,
        A.n_show_answer_problem_seen as n_show_answer_problem_seen,
        A.n_problems_seen as n_problems_seen,
        
    FROM
    (
        # inner-inner SQL: join problem_grades with show_answer
        SELECT 
          PG.user_id as user_id,
          sum(case when (not PG.attempted) and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_not_attempted,
          sum(case when (not PG.attempted) then 1 else 0 end) as n_not_attempted,

          sum(case when PG.attempted and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_attempted,
          sum(case when PG.attempted then 1 else 0 end) as n_attempted,

          sum(case when PG.perfect and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_perfect,
          sum(case when PG.perfect then 1 else 0 end) as n_perfect,

          sum(case when (PG.grade > 0) and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_partial,
          sum(case when PG.grade > 0 then 1 else 0 end) as n_partial,

          sum(case when n_show_answer > 0 then 1 else 0 end) as n_show_answer_problem_seen,
          count(*) as n_problems_seen,
        FROM 
          [{dataset}.problem_grades] as PG
        LEFT JOIN EACH
        (
            SELECT PC.user_id as user_id,
              CONCAT('i4x://', SA.module_id) as module_id,  # studentmodule module_id has i4x:// prefix
              count(*) as n_show_answer,
            FROM [{dataset}.show_answer] as SA
            JOIN EACH [{dataset}.person_course] as PC
            ON SA.username = PC.username
            group by module_id, user_id
            order by user_id
        ) as SA
        ON SA.user_id = PG.user_id
           AND SA.module_id = PG.module_id
        group by user_id
     ) as A
     JOIN EACH [{dataset}.person_course] as PC
     ON A.user_id = PC.user_id
       
     WHERE A.user_id = PC.user_id
        AND ((PC.forumRoles_isStudent = 1) or (PC.forumRoles_isStudent is null))  # exclude staff
)
ORDER BY user_id
                """.format(dataset=dataset)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, sasbu_table, course_id)
    sys.stdout.flush()

    try:
        tinfo = bqutil.get_bq_table_info(dataset, 'show_answer')
        has_show_answer = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, "show_answer")
        has_show_answer = False
    if not has_show_answer:
        print "---> No show_answer table; skipping %s" % sasbu_table
        return

    bqdat = bqutil.get_bq_table(dataset, sasbu_table, sasbu_sql, force_query=force_recompute,
                                depends_on=["%s.show_answer" % dataset,
                                            "%s.problem_grades" % dataset,
                                            "%s.person_course" % dataset,
                                            ],
                                newer_than=datetime.datetime(2015, 3, 13, 20, 0),
                                startIndex=-2)
        
#-----------------------------------------------------------------------------

def make_show_answer_stats_by_course_table(course_id, dataset, force_recompute):
    '''
    The show_answer_stats_by_course table aggregates over users, using the
    show_answer_by_user table as input, to produce show_answer stats for the
    whole course, including, e.g. avg_pct_show_answer_problem_seen, and 
    median_pct_show_answer_problem_seen_certified.
    '''

    table = "show_answer_stats_by_course"
    SQL = """
SELECT
"{course_id}" as course_id,
avg(pct_show_answer_problem_seen) as avg_pct_show_answer_problem_seen,
avg(pct_show_answer_not_attempted) as avg_pct_show_answer_not_attempted,
avg(pct_show_answer_attempted) as avg_pct_show_answer_attempted,
avg(pct_show_answer_perfect) as avg_pct_show_answer_perfect,
avg(pct_show_answer_partial) as avg_pct_show_answer_partial,

avg(case when certified then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_certified,
avg(case when certified then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_certified,
avg(case when certified then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_certified,
avg(case when certified then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_certified,
avg(case when certified then pct_show_answer_partial end) as avg_pct_show_answer_partial_certified,

avg(case when explored then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_explored,
avg(case when explored then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_explored,
avg(case when explored then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_explored,
avg(case when explored then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_explored,
avg(case when explored then pct_show_answer_partial end) as avg_pct_show_answer_partial_explored,

avg(case when verified then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_verified,
avg(case when verified then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_verified,
avg(case when verified then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_verified,
avg(case when verified then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_verified,
avg(case when verified then pct_show_answer_partial end) as avg_pct_show_answer_partial_verified,

max(case when has_pct_show_answer_problem_seen then median_pct_show_answer_problem_seen end) as median_pct_show_answer_problem_seen,
max(case when has_pct_show_answer_not_attempted then median_pct_show_answer_not_attempted end) as median_pct_show_answer_not_attempted,
max(case when has_pct_show_answer_attempted then median_pct_show_answer_attempted end) as median_pct_show_answer_attempted,
max(case when has_pct_show_answer_perfect then median_pct_show_answer_perfect end) as median_pct_show_answer_perfect,
max(case when has_pct_show_answer_partial then median_pct_show_answer_partial end) as median_pct_show_answer_partial,

max(case when has_pct_show_answer_problem_seen_explored then median_pct_show_answer_problem_seen_explored end) as median_pct_show_answer_problem_seen_explored,
max(case when has_pct_show_answer_not_attempted_explored then median_pct_show_answer_not_attempted_explored end) as median_pct_show_answer_not_attempted_explored,
max(case when has_pct_show_answer_attempted_explored then median_pct_show_answer_attempted_explored end) as median_pct_show_answer_attempted_explored,
max(case when has_pct_show_answer_perfect_explored then median_pct_show_answer_perfect_explored end) as median_pct_show_answer_perfect_explored,
max(case when has_pct_show_answer_partial_explored then median_pct_show_answer_partial_explored end) as median_pct_show_answer_partial_explored,

max(case when has_pct_show_answer_problem_seen_certified then median_pct_show_answer_problem_seen_certified end) as median_pct_show_answer_problem_seen_certified,
max(case when has_pct_show_answer_not_attempted_certified then median_pct_show_answer_not_attempted_certified end) as median_pct_show_answer_not_attempted_certified,
max(case when has_pct_show_answer_attempted_certified then median_pct_show_answer_attempted_certified end) as median_pct_show_answer_attempted_certified,
max(case when has_pct_show_answer_perfect_certified then median_pct_show_answer_perfect_certified end) as median_pct_show_answer_perfect_certified,
max(case when has_pct_show_answer_partial_certified then median_pct_show_answer_partial_certified end) as median_pct_show_answer_partial_certified,

max(case when has_pct_show_answer_problem_seen_verified then median_pct_show_answer_problem_seen_verified end) as median_pct_show_answer_problem_seen_verified,
max(case when has_pct_show_answer_not_attempted_verified then median_pct_show_answer_not_attempted_verified end) as median_pct_show_answer_not_attempted_verified,
max(case when has_pct_show_answer_attempted_verified then median_pct_show_answer_attempted_verified end) as median_pct_show_answer_attempted_verified,
max(case when has_pct_show_answer_perfect_verified then median_pct_show_answer_perfect_verified end) as median_pct_show_answer_perfect_verified,
max(case when has_pct_show_answer_partial_verified then median_pct_show_answer_partial_verified end) as median_pct_show_answer_partial_verified,

FROM
(
  SELECT *,
    (case when pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_problem_seen order by pct_show_answer_problem_seen) as median_pct_show_answer_problem_seen,
    (case when pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_not_attempted order by pct_show_answer_not_attempted) as median_pct_show_answer_not_attempted,
    (case when pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_attempted order by pct_show_answer_attempted) as median_pct_show_answer_attempted,
    (case when pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_perfect order by pct_show_answer_perfect) as median_pct_show_answer_perfect,
    (case when pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_partial order by pct_show_answer_partial) as median_pct_show_answer_partial,

    (case when explored and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_explored,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_problem_seen_explored order by pct_show_answer_problem_seen) as median_pct_show_answer_problem_seen_explored,
    (case when explored and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_explored,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_not_attempted_explored order by pct_show_answer_not_attempted) as median_pct_show_answer_not_attempted_explored,
    (case when explored and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_explored,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_attempted_explored order by pct_show_answer_attempted) as median_pct_show_answer_attempted_explored,
    (case when explored and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_explored,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_perfect_explored order by pct_show_answer_perfect) as median_pct_show_answer_perfect_explored,
    (case when explored and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_explored,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_partial_explored order by pct_show_answer_partial) as median_pct_show_answer_partial_explored,

    (case when certified and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_certified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_problem_seen_certified order by pct_show_answer_problem_seen) as median_pct_show_answer_problem_seen_certified,
    (case when certified and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_certified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_not_attempted_certified order by pct_show_answer_not_attempted) as median_pct_show_answer_not_attempted_certified,
    (case when certified and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_certified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_attempted_certified order by pct_show_answer_attempted) as median_pct_show_answer_attempted_certified,
    (case when certified and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_certified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_perfect_certified order by pct_show_answer_perfect) as median_pct_show_answer_perfect_certified,
    (case when certified and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_certified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_partial_certified order by pct_show_answer_partial) as median_pct_show_answer_partial_certified,

    (case when verified and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_verified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_problem_seen_verified order by pct_show_answer_problem_seen) as median_pct_show_answer_problem_seen_verified,
    (case when verified and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_verified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_not_attempted_verified order by pct_show_answer_not_attempted) as median_pct_show_answer_not_attempted_verified,
    (case when verified and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_verified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_attempted_verified order by pct_show_answer_attempted) as median_pct_show_answer_attempted_verified,
    (case when verified and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_verified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_perfect_verified order by pct_show_answer_perfect) as median_pct_show_answer_perfect_verified,
    (case when verified and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_verified,
    PERCENTILE_DISC(0.5) over (partition by has_pct_show_answer_partial_verified order by pct_show_answer_partial) as median_pct_show_answer_partial_verified,

  FROM [{dataset}.show_answer_stats_by_user]
)
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "show_answer_stats_by_user"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_show_answer = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_show_answer = False
    if not has_show_answer:
        print "---> No show_answer table; skipping %s" % table
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                depends_on=["%s.%s" % (dataset, sasbu),
                                            ],
                                newer_than=datetime.datetime(2015, 3, 14, 18, 21),
                                startIndex=-1)
    fields = ['avg_pct_show_answer_problem_seen', 'avg_pct_show_answer_attempted', 'avg_pct_show_answer_perfect',
              'median_pct_show_answer_problem_seen', 'median_pct_show_answer_attempted', 'median_pct_show_answer_perfect',
              'median_pct_show_answer_problem_seen_certified', 'median_pct_show_answer_attempted_certified', 'median_pct_show_answer_perfect_certified',
              ]

    for fn in fields:
        print "    %40s = %s" % (fn, bqdat['data'][0][fn])
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def compute_ip_pair_sybils(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    The stats_ip_pair_sybils table finds all harvester-master pairs of users for 
    which the IP address is the same, and the pair have meaningful disparities
    in performance, including:
      - one earning a certificate and the other not
      - one clicking "show answer" many times and the other not
    Typically, the "master", which earns a certificate, has a high percentage
    of correct attempts, while the "harvester" clicks on "show answer" many times,
    and does not earn a certificate.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_ip_pair_sybils"
    SQL = """
            # northcutt SQL for finding sybils
            SELECT
              "{course_id}" as course_id,
              user_id, username, ip, nshow_answer as nshow_answer_unique_problems,
              percent_correct_attempts,frac_complete, certified
            FROM
            (  
              SELECT
                *,
                sum(certified = true) over (partition by ip) as sum_cert_true,
                sum(certified = false) over (partition by ip) as sum_cert_false
              from
              (
                # If any user in an ip group was flagged remove_ip = true
                # then their entire ip group will be flagged non-zero
                select *,
                  sum(remove_ip_group) over (partition by ip) as zero_only,
                  #filter users with greater than 70% attempts correct or less 5 show answers
                  certified = false and (percent_correct_attempts > 70 or nshow_answer <= 5 or frac_complete = 0) as remove
                from 
                ( 
                  # Add column that we will later user to filter out valid users who earn a
                  # certificate but click show_answer afterwards EVEN WHEN THEY GET IT RIGHT.
                  # If a user in an ip group is certified and has max(show_answer) in ip group then
                  # Remove this ip since user not cheater but checked show_answer a lot after getting it right
                  # Also remove multiple users working independently behind a NAT box
                  #Filter out harvesters with greater than 70% attempts correct or less 5 show answers
                  select *,
                  (nshow_answer = maxsa and certified=true) as remove_ip_group
                  from
                  (
                    # Add column for max and min number of show answers for each ip group
                    select *,
                    max(nshow_answer) over (partition by ip) as maxsa,
                    min(nshow_answer) over (partition by ip) as minsa,
                    from
                    (
                    select user_id, username, ip, nshow_answer, percent_correct_attempts,
                      frac_complete, certified
                    from
                      ( 
                        # Find all users with >1 accounts, same ip address, different certification status
                        select
                          #  pc.course_id as course_id,
                          pc.user_id as user_id,
                          username,
                          ip,
                          nshow_answer_unique_problems as nshow_answer,
                          round(ac.percent_correct, 2) as percent_correct_attempts,
                          frac_complete,
                          ac.certified as certified,
                          max(nshow_answer) over (partition by ip) as maxshow,
                          min(nshow_answer) over (partition by ip) as minshow,
                          sum(pc.certified = true) over (partition by ip) as sum_cert_true,
                          sum(pc.certified = false) over (partition by ip) as sum_cert_false,
                          count(ip) over (partition by ip) as ipcnt
            
                        # Removes any user not in problem_analysis (use outer left join to include)
                        FROM [{dataset}.person_course] as pc
                        JOIN [{dataset}.stats_attempts_correct] as ac
                        on pc.user_id = ac.user_id
                        where pc.ip != ''
                      )
                    # Remove people who just created an account they never used (small nshow_answer) and then earned a certificate validly with small nshow_answer
                    # This also filters out people who only got a couple of answers with a second account.
                    # We can't just say when min(nshow_answer) > threshold because master accounts wil have zero nshow_answer sometimes       
                    where maxshow - minshow >=5
                    # Since clicking show answer or guessing over and over cannot achieve certification, we should have
                    # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
                    and sum_cert_true > 0
                    and sum_cert_false > 0
                    and ipcnt < 8 #Remove NAT or internet cafe ips
            
                    )
                  )
                )
              )
              # Remove all ip groups with valid users who just clicked show_answer after getting it right.
              # Also remove multiple users working independently behind a NAT box
              where zero_only = 0
              and remove = false
            )
            WHERE sum_cert_true > 0
            and sum_cert_false > 0
            # Order by ip to group master and harvesters together. Order by certified so that we always have masters above harvester accounts.
            order by ip asc, certified desc
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_attempts_correct"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No attempts_correct table; skipping %s" % table
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )
    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> Found %s records for %s, corresponding to %d master-harvester pairs" % (nfound, table, int(nfound/2))
    sys.stdout.flush()


#-----------------------------------------------------------------------------

def compute_ip_pair_sybils2(course_id, force_recompute=False, use_dataset_latest=False, uname_ip_groups_table=None):
    '''
    The stats_ip_pair_sybils2 table finds all harvester-master GROUPS of users for 
    which the pair have meaningful disparities
    in performance, including:
      - one earning a certificate and the other not
      - one clicking "show answer" many times and the other not
    Typically, the "master", which earns a certificate, has a high percentage
    of correct attempts, while the "harvester" clicks on "show answer" many times,
    and does not earn a certificate.

    Multiple users can belong to the same group, and their IP addresses can be different.

    This requires a table to be pre-computed, which gives the transitive closure over
    all the (username, ip) pairs from the Sybil (1.0) tables.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_ip_pair_sybils2"

    SQL = """# Northcutt SQL for finding sybils
             ##################################
             # Sybils Version 2.0
             # Instead of same ip, considers users in same grp where
             # where grp is determined by two iterations of the transitive closure 
             # of the original sybils table (username, ip) pairs.
             SELECT
               "{course_id}" as course_id,
               user_id, username, ip, grp, nshow_answer as nshow_answer_unique_problems,
               percent_correct_attempts,frac_complete, certified
             FROM
             (  
               SELECT
                 *,
                 sum(certified = true) over (partition by grp) as sum_cert_true,
                 sum(certified = false) over (partition by grp) as sum_cert_false
               from
               (
                 # If any user in an ip group was flagged remove_ip_group = true
                 # then their entire ip group will be flagged non-zero
                 select *,
                   sum(remove_grp) over (partition by grp) as zero_only,
                   #filter users with greater than 70% attempts correct or less 5 show answers
                   certified = false and (percent_correct_attempts > 70 or nshow_answer <= 5 or frac_complete = 0) as remove
                 from 
                 ( 
                   # Add column that we will later user to filter out valid users who earn a
                   # certificate but click show_answer afterwards EVEN WHEN THEY GET IT RIGHT.
                   # If a user in an ip group is certified and has max(show_answer) in ip group then
                   # Remove this ip since user not cheater but checked show_answer a lot after getting it right
                   # Also remove multiple users working independently behind a NAT box
                   #Filter out harvesters with greater than 70% attempts correct or less 5 show answers
                   select *,
                   (nshow_answer = maxsa and certified=true) as remove_grp #max show answer must NOT be the master
                   from
                   (
                     # Add column for max and min number of show answers for each ip group
                     select *,
                     max(nshow_answer) over (partition by grp) as maxsa,
                     min(nshow_answer) over (partition by grp) as minsa,
                     from
                     (
                     select user_id, username, ip, grp, nshow_answer, percent_correct_attempts,
                       frac_complete, certified
                     from
                       ( 
                         # Find all users with >1 accounts, same ip address, different certification status
                         select
                           #  pc.course_id as course_id,
                           pc.user_id as user_id,
                           username,
                           ip, grp,
                           nshow_answer_unique_problems as nshow_answer,
                           round(ac.percent_correct, 2) as percent_correct_attempts,
                           frac_complete,
                           ac.certified as certified,
                           max(nshow_answer) over (partition by grp) as maxshow,
                           min(nshow_answer) over (partition by grp) as minshow,
                           sum(pc.certified = true) over (partition by grp) as sum_cert_true,
                           sum(pc.certified = false) over (partition by grp) as sum_cert_false,
                           count(grp) over (partition by grp) as ipcnt
             
                           #Adds a column with transitive closure group number for each user
                           from
                           (
                             select user_id, a.username as username, a.ip as ip, certified, grp
                             FROM [{dataset}.person_course] a
                             JOIN [{uname_ip_groups_table}] b
                             ON a.username = b.username AND a.ip = b.ip
                           )as pc
                            JOIN [{dataset}.stats_attempts_correct] as ac
                            on pc.user_id = ac.user_id
                         )
                       
                     # Remove people who just created an account they never used (small nshow_answer) and then earned a 
                     #  certificate validly with small nshow_answer
                     # This also filters out people who only got a couple of answers with a second account.
                     # We can't just say when min(nshow_answer) > threshold because master accounts wil have zero nshow_answer sometimes       

                     where maxshow - minshow >=5

                     # Since clicking show answer or guessing over and over cannot achieve certification, we should have
                     # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
                     and sum_cert_true > 0
                     and sum_cert_false > 0
                     and ipcnt < 10 #Remove NAT or internet cafe ips
             
                     )
                   )
                 )
               )
               # Remove all ip groups with valid users who just clicked show_answer after getting it right.
               # Also remove multiple users working independently behind a NAT box
               where zero_only = 0
               and remove = false
             )
             # Remove entire group if all the masters or all the harvesters were removed
             WHERE sum_cert_true > 0
             and sum_cert_false > 0
             # Order by ip to group master and harvesters together. Order by certified so that we always have masters above harvester accounts.
             order by grp asc, certified desc;
          """.format(dataset=dataset, course_id=course_id, uname_ip_groups_table=uname_ip_groups_table)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_attempts_correct"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No attempts_correct table; skipping %s" % table
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )
    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> [%s] Sybils 2.0 Found %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()

    compute_sybils_show_ans_before(course_id, force_recompute, use_dataset_latest)

    compute_ip_pair_sybils2_features(course_id, force_recompute, use_dataset_latest)
    
#-----------------------------------------------------------------------------

def compute_sybils_show_ans_before(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    An ancillary table for sybils which computes the percent of shadow account show answers 
    that occur before the corresponding certified CAMEO accounts correct answer submission.
    This table also computes avg_max_dt_seconds which is the average time between the shadow's
    show_answer and the certified accounts' correct answer.

    This table chooses the FIRST show_answer and the LAST correct submission, to ensure any cheating
    is caught, even if the user tried to figure it out without cheating first.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_sybils_show_ans_before"

    SQL = """
              SELECT 
              "{course_id}" as course_id,
              shadow,
              certified_account,
              sum(sa_before_pa) as show_ans_before,
              count(*) as prob_in_common,
              round(sum(sa_before_pa) / count(*), 4) * 100 as percent_show_ans_before,
              round(avg(max_dt), 4) as avg_max_dt_seconds
              FROM
              (  
                select sa.username as shadow,
                pa.username as certified_account,
                sa.time, pa.time,
                sa.time < pa.time as sa_before_pa,
                (case when sa.time < pa.time then (pa.time - sa.time) / 1e6 else NULL end) as max_dt
                FROM
                (
                  #Certified = false, show answer (first) account must be shadow
                  SELECT grp, sa.username as username, index, time
                  FROM
                  (
                    SELECT sa.username as username, index, sa.time as time
                    FROM
                    (
                      SELECT 
                      username, index, FIRST(time) as time
                      FROM [{dataset}.show_answer] a
                      JOIN [{dataset}.course_axis] b
                      ON a.course_id = b.course_id and a.module_id = b.module_id
                      group by username, index
                    ) sa
                    JOIN [{dataset}.person_course] pc
                    ON sa.username = pc.username
                    where certified = false
                  )sa
                  JOIN [{dataset}.stats_ip_pair_sybils2] s
                  on sa.username = s.username
                ) sa
                JOIN
                (
                  #certified = true, problem activity (second) account must be surface
                  SELECT grp, pa.username as username, index, time
                  FROM
                  (
                    SELECT 
                    username, index, LAST(time) as time
                    FROM [{dataset}.problem_check] a
                    JOIN [{dataset}.course_axis] b
                    ON a.course_id = b.course_id and a.module_id = b.module_id
                    where category = 'problem'
                    and success = 'correct'
                    group by username, index
                ) pa
                  JOIN [{dataset}.stats_ip_pair_sybils2] s
                  on pa.username = s.username
                  where certified = true
                ) pa
                on sa.grp = pa.grp and sa.index = pa.index
                WHERE sa.username != pa.username
              )
              group by shadow, certified_account
              order by avg_max_dt_seconds asc
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_ip_pair_sybils2"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )
    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def compute_ip_pair_sybils2_features(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    The stats_ip_pair_sybils2_features table has all the info in stats_ip_pair_sybils2, but also
    includes statistical features drawn from person_course and stats_attempts_correct
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_ip_pair_sybils2_features"

    SQL = """
            SELECT 
            course_id,
            user_id,
            username,
            case when certified_account is null then username else certified_account end as certfied_cameo,
            ip,
            grp,
            certified,
            nshow_answer_unique_problems,
            percent_correct,
            percent_show_ans_before,
            avg_max_dt_seconds,
            nproblems,
            frac_complete,
            verified,
            countryLabel,
            start_time,
            last_event,
            nforum_posts,
            nprogcheck,
            nvideo,
            time_active_in_days,
            grade
            FROM
            (
               SELECT 
               "{course_id}" as course_id,
               s.user_id as user_id,
               s.username as username,
               s.ip as ip,
               s.grp as grp,
               s.certified as certified,
               pc.nshow_answer_unique_problems as nshow_answer_unique_problems,
               percent_correct,
               nproblems,
               pc.frac_complete as frac_complete,
               pc.verified as verified,
               pc.countryLabel as countryLabel,
               pc.start_time as start_time,
               pc.last_event as last_event,
               pc.nforum_posts as nforum_posts,
               pc.nprogcheck as nprogcheck,
               pc.nvideo as nvideo,
               pc.time_active_in_days as time_active_in_days,
               pc.grade as grade
               FROM 
               (
                 SELECT
                 pc.user_id as user_id,
                 percent_correct,
                 nshow_answer_unique_problems,
                 nproblems,
                 frac_complete,
                 pc.course_id as course_id,
                 (case when pc.mode = "verified" then true else false end) as verified,
                 countryLabel,
                 start_time,
                 last_event,
                 nforum_posts,
                 nprogcheck,
                 nvideo,
                 (sum_dt / 60 / 60/ 24) as time_active_in_days,
                 grade
                 FROM
                 [{dataset}.person_course] pc
                 JOIN [{dataset}.stats_attempts_correct] as ac
                 ON pc.user_id = ac.user_id
               ) pc
               JOIN [{dataset}.stats_ip_pair_sybils2] s
               ON pc.user_id = s.user_id
               order by grp asc, certified desc
            ) a
            LEFT OUTER JOIN [{dataset}.stats_sybils_show_ans_before] b
            ON a.username = b.shadow
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_ip_pair_sybils2"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )
    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def compute_temporal_fingerprints(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    This computes temporal fingerprints for problem_check and show_answer, and looks for correlations between them.

    Each temporal fingerprint is a table which includes the event time for each (username, problem_index) pair,
    where the problem index is the course_axis index for a specific CAPA problem in the courseware.
    '''
    compute_problem_check_temporal_fingerprint(course_id, force_recompute, use_dataset_latest)
    compute_show_answer_temporal_fingerprint(course_id, force_recompute, use_dataset_latest)
    compute_temporal_fingerprint_correlations(course_id, force_recompute, use_dataset_latest)


def compute_problem_check_temporal_fingerprint(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute problem_check_temporal_fingerprint
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "problem_check_temporal_fingerprint"

    SQL = """
            # construct problem check temporal fingerprint for each person
            # each row contains the first problem_check timestamp for (person, indexed-item)
            # average timestamp for indexed-item, and deviation
            # of the person's timestamp from the average.
            SELECT
                username,
                user_id, index, 
                min_time as time_of_first_check,
                max_time as time_of_last_check,
                USEC_TO_TIMESTAMP(avg_first_check_time_usec) as avg_first_check_time,
                (TIMESTAMP_TO_USEC(min_time) - avg_first_check_time_usec)/1.0e6/60/60 as delta_t_hours,
            FROM
            (
                SELECT
                    *,
                    AVG(TIMESTAMP_TO_USEC(min_time)) OVER (PARTITION BY index) as avg_first_check_time_usec,
                FROM
                (
                    SELECT
                      PC.username as username, PC.user_id as user_id, SATF.index as index, min_time, max_time,
                    FROM
                    (
                       SELECT 
                         username,
                         index,
                         min_time,
                         max_time
                       FROM
                       (
                         SELECT min(time) as min_time, max(time) as max_time, username, module_id
                         FROM [{dataset}.problem_check]
                         GROUP BY username, module_id
                       ) as SM
                       JOIN
                       (
                          # get index and module_id from course_axis
                          SELECT index,
                              # CONCAT("i4x://", module_id) as module_id,
                              module_id,
                          FROM [{dataset}.course_axis] 
                          WHERE category="problem"
                       ) as CA
                       ON CA.module_id = SM.module_id
                       order by username, index
                    ) as SATF
                    JOIN EACH [{dataset}.person_course] as PC  # get certified from person_course
                    on SATF.username = PC.username
                    WHERE
                      PC.certified
                    order by user_id, index
                )
            )
            order by user_id, index
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "problem_check"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No problem_check table; skipping %s" % table
        return

    try:
        bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                    newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                    depends_on=["%s.%s" % (dataset, sasbu),
                                            ],
                                    startIndex=-2,
                                )
    except Exception as err:
        print "[compute_problem_check_temporal_fingerprint] oops, failed in running SQL=%s, err=%s" % (SQL, err)
        raise

    print "--> Done with %s" % (table)
    sys.stdout.flush()


def compute_show_answer_temporal_fingerprint(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute show_answer_temporal_fingerprint
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "show_answer_temporal_fingerprint"

    SQL = """
            # construct show answer temporal fingerprint for each person
            # each row contains activity timestamp for (person, indexed-item),
            # and the certified flag.
            SELECT
              PC.username as username,
              user_id, SATF.index as index, SATF.time as time,
              certified,
              # (case when certified then index end) as index_certified,
            FROM
            (
               SELECT 
                 username, index, FIRST(time) as time
                 FROM [{dataset}.show_answer] a
                 JOIN [{dataset}.course_axis] b
                 ON a.course_id = b.course_id and a.module_id = b.module_id
                 group by username, index
            ) as SATF
            JOIN EACH [{dataset}.person_course] as PC  # get user_id via username from person_course
            on SATF.username = PC.username
            WHERE
              PC.nshow_answer > 10
            order by user_id, index
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "show_answer"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No show_answer table; skipping %s" % table
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                                startIndex=-2,
                            )

    print "--> Done with %s" % (table)
    sys.stdout.flush()

def compute_temporal_fingerprint_correlations(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute stats_temporal_fingerprint_correlations
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_temporal_fingerprint_correlations"

    SQL = """
            # corrlate problem check temporal fingerprint with show answer temporal fingerprint
            # this version uses the average time for each item (averaged over all certified problem
            # check times) as a baseline, and subtracts that baseline time from each
            # problem activity time and show answer time.  That removes the baseline
            # linear correlation between times, due to the natural temporal progression of
            # the course content as it is released sequentially.
            #
            # Note that a threshold (on n_show_answer_times) is used to discard cases when there are 
            # two few points being correlated.
            SELECT
               "{course_id}" as course_id,
                CAMEO_username,
                harvester_username as shadow_username,
                CAMEO_uid,
                harvester_uid as shadow_uid,
                pearson_corr as fpt_pearson_corr,
                n_show_answer_times,
                avg_dt_show_answer_to_problem_check_in_min,
                percent_correct as CAMEO_percent_correct,
                frac_complete as CAMEO_frac_complete,
            FROM
            (
                SELECT 
                    A.user_id as CAMEO_uid, 
                    A.username as CAMEO_username,
                    B.user_id as harvester_uid,
                    B.username as harvester_username,
                    # CORR(B.time, A.time) as pearson_corr,
                    CORR(A.delta_t_hours, B.time - A.avg_first_check_time) as pearson_corr,
                    count(B.time) as n_show_answer_times,
                    count(A.time_of_first_check) as n_problem_activity_times,
                    AVG(A.time_of_first_check - B.time) / 1.0e6/60 as avg_dt_show_answer_to_problem_check_in_min,
                FROM [{dataset}.problem_check_temporal_fingerprint] as A
                FULL OUTER JOIN EACH [{dataset}.show_answer_temporal_fingerprint] as B
                ON A.index = B.index
                WHERE
                   not B.certified
                   and not (A.user_id = B.user_id)
                   and B.time < A.time_of_first_check     # only correlate when show_answer happens BEFORE problem submission
                GROUP EACH BY CAMEO_uid, CAMEO_username, harvester_uid, harvester_username
                HAVING n_show_answer_times > 20 and pearson_corr > 0.99    # TODO: make these adjust, e.g. use half number of problems
                ORDER BY pearson_corr desc
            ) as C
            JOIN [{dataset}.stats_attempts_correct] as SAC
            ON C.CAMEO_uid = SAC.user_id
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_attempts_correct"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )

    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> Done with %s, %d entries found" % (table, nfound)
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def compute_temporal_fingerprints(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    This computes temporal fingerprints for problem_check and show_answer, and looks for correlations between them.

    Each temporal fingerprint is a table which includes the event time for each (username, problem_index) pair,
    where the problem index is the course_axis index for a specific CAPA problem in the courseware.
    '''
    compute_problem_check_temporal_fingerprint(course_id, force_recompute, use_dataset_latest)
    compute_show_answer_temporal_fingerprint(course_id, force_recompute, use_dataset_latest)
    compute_temporal_fingerprint_correlations(course_id, force_recompute, use_dataset_latest)


def compute_problem_check_temporal_fingerprint(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute problem_check_temporal_fingerprint
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "problem_check_temporal_fingerprint"

    SQL = """
            # construct problem check temporal fingerprint for each person
            # each row contains the first problem_check timestamp for (person, indexed-item)
            # average timestamp for indexed-item, and deviation
            # of the person's timestamp from the average.
            SELECT
                username,
                user_id, index, 
                min_time as time_of_first_check,
                max_time as time_of_last_check,
                USEC_TO_TIMESTAMP(avg_first_check_time_usec) as avg_first_check_time,
                (TIMESTAMP_TO_USEC(min_time) - avg_first_check_time_usec)/1.0e6/60/60 as delta_t_hours,
            FROM
            (
                SELECT
                    *,
                    AVG(TIMESTAMP_TO_USEC(min_time)) OVER (PARTITION BY index) as avg_first_check_time_usec,
                FROM
                (
                    SELECT
                      PC.username as username, PC.user_id as user_id, SATF.index as index, min_time, max_time,
                    FROM
                    (
                       SELECT 
                         username,
                         index,
                         min_time,
                         max_time
                       FROM
                       (
                         SELECT min(time) as min_time, max(time) as max_time, username, module_id
                         FROM [{dataset}.problem_check]
                         GROUP BY username, module_id
                       ) as SM
                       JOIN
                       (
                          # get index and module_id from course_axis
                          SELECT index,
                              # CONCAT("i4x://", module_id) as module_id,
                              module_id,
                          FROM [{dataset}.course_axis] 
                          WHERE category="problem"
                       ) as CA
                       ON CA.module_id = SM.module_id
                       order by username, index
                    ) as SATF
                    JOIN EACH [{dataset}.person_course] as PC  # get certified from person_course
                    on SATF.username = PC.username
                    WHERE
                      PC.certified
                    order by user_id, index
                )
            )
            order by user_id, index
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "problem_check"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No problem_check table; skipping %s" % table
        return

    try:
        bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                    newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                    depends_on=["%s.%s" % (dataset, sasbu),
                                            ],
                                    startIndex=-2,
                                )
    except Exception as err:
        print "[compute_problem_check_temporal_fingerprint] oops, failed in running SQL=%s, err=%s" % (SQL, err)
        raise

    print "--> Done with %s" % (table)
    sys.stdout.flush()


def compute_show_answer_temporal_fingerprint(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute show_answer_temporal_fingerprint
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "show_answer_temporal_fingerprint"

    SQL = """
            # construct show answer temporal fingerprint for each person
            # each row contains activity timestamp for (person, indexed-item),
            # and the certified flag.
            SELECT
              PC.username as username,
              user_id, SATF.index as index, SATF.time as time,
              certified,
              # (case when certified then index end) as index_certified,
            FROM
            (
               SELECT 
                 username, index, FIRST(time) as time
                 FROM [{dataset}.show_answer] a
                 JOIN [{dataset}.course_axis] b
                 ON a.course_id = b.course_id and a.module_id = b.module_id
                 group by username, index
            ) as SATF
            JOIN EACH [{dataset}.person_course] as PC  # get user_id via username from person_course
            on SATF.username = PC.username
            WHERE
              PC.nshow_answer > 10
            order by user_id, index
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "show_answer"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No show_answer table; skipping %s" % table
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                                startIndex=-2,
                            )

    print "--> Done with %s" % (table)
    sys.stdout.flush()


def compute_temporal_fingerprint_correlations(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute stats_temporal_fingerprint_correlations
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_temporal_fingerprint_correlations"

    SQL = """
            # corrlate problem check temporal fingerprint with show answer temporal fingerprint
            # this version uses the average time for each item (averaged over all certified problem
            # check times) as a baseline, and subtracts that baseline time from each
            # problem activity time and show answer time.  That removes the baseline
            # linear correlation between times, due to the natural temporal progression of
            # the course content as it is released sequentially.
            #
            # Note that a threshold (on n_show_answer_times) is used to discard cases when there are 
            # two few points being correlated.
            SELECT
               "{course_id}" as course_id,
                CAMEO_username,
                harvester_username as shadow_username,
                CAMEO_uid,
                harvester_uid as shadow_uid,
                pearson_corr as fpt_pearson_corr,
                n_show_answer_times,
                avg_dt_show_answer_to_problem_check_in_min,
                percent_correct as CAMEO_percent_correct,
                frac_complete as CAMEO_frac_complete,
            FROM
            (
                SELECT 
                    A.user_id as CAMEO_uid, 
                    A.username as CAMEO_username,
                    B.user_id as harvester_uid,
                    B.username as harvester_username,
                    # CORR(B.time, A.time) as pearson_corr,
                    CORR(A.delta_t_hours, B.time - A.avg_first_check_time) as pearson_corr,
                    count(B.time) as n_show_answer_times,
                    count(A.time_of_first_check) as n_problem_activity_times,
                    AVG(A.time_of_first_check - B.time) / 1.0e6/60 as avg_dt_show_answer_to_problem_check_in_min,
                FROM [{dataset}.problem_check_temporal_fingerprint] as A
                FULL OUTER JOIN EACH [{dataset}.show_answer_temporal_fingerprint] as B
                ON A.index = B.index
                WHERE
                   not B.certified
                   and not (A.user_id = B.user_id)
                   and B.time < A.time_of_first_check     # only correlate when show_answer happens BEFORE problem submission
                GROUP EACH BY CAMEO_uid, CAMEO_username, harvester_uid, harvester_username
                HAVING n_show_answer_times > 20 and pearson_corr > 0.99    # TODO: make these adjust, e.g. use half number of problems
                ORDER BY pearson_corr desc
            ) as C
            JOIN [{dataset}.stats_attempts_correct] as SAC
            ON C.CAMEO_uid = SAC.user_id
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_attempts_correct"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )

    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> Done with %s, %d entries found" % (table, nfound)
    sys.stdout.flush()
