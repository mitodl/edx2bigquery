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
import time
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
              user_id, username, ip, nshow_answer_unique_problems,
              percent_correct, frac_complete, certified
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
                  #filter users with greater than 70% attempts correct or less 5 show answers
                  certified = false and (percent_correct > 70 or nshow_answer_unique_problems <= 10 or frac_complete = 0) as remove
                from 
                ( 
                  (
                  select user_id, username, ip, nshow_answer_unique_problems, percent_correct,
                    frac_complete, certified
                  from
                    ( 
                      # Find all users with >1 accounts, same ip address, different certification status
                      select
                        #  pc.course_id as course_id,
                        pc.user_id as user_id,
                        username,
                        ip,
                        nshow_answer_unique_problems,
                        round(ac.percent_correct, 2) as percent_correct,
                        frac_complete,
                        ac.certified as certified,
                        sum(pc.certified = true) over (partition by ip) as sum_cert_true,
                        sum(pc.certified = false) over (partition by ip) as sum_cert_false,
                        count(ip) over (partition by ip) as ipcnt
          
                      # Removes any user not in problem_analysis (use outer left join to include)
                      FROM [{dataset}.person_course] as pc
                      JOIN [{dataset}.stats_attempts_correct] as ac
                      on pc.user_id = ac.user_id
                      where pc.ip != ''
                    )
                  # Since clicking show answer or guessing over and over cannot achieve certification, we should have
                  # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
                  where sum_cert_true > 0
                  and sum_cert_false > 0
                  and ipcnt < 8 #Remove NAT or internet cafe ips
            
                  )
                )
              )
              where remove = false
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
               user_id, username, ip, grp, nshow_answer_unique_problems,
               percent_correct, frac_complete, certified
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
                   #filter users with greater than 70% attempts correct or less 5 show answers
                   certified = false and (percent_correct > 70 or nshow_answer_unique_problems <= 10 or frac_complete = 0) as remove
                 from 
                 (
                   select user_id, username, ip, grp, nshow_answer_unique_problems, percent_correct,
                     frac_complete, certified
                   from
                     ( 
                       # Find all users with >1 accounts, same ip address, different certification status
                       select
                         #  pc.course_id as course_id,
                         pc.user_id as user_id,
                         username,
                         ip, grp,
                         nshow_answer_unique_problems,
                         round(ac.percent_correct, 2) as percent_correct,
                         frac_complete,
                         ac.certified as certified,
                         sum(pc.certified = true) over (partition by grp) as sum_cert_true,
                         sum(pc.certified = false) over (partition by grp) as sum_cert_false,
                         count(distinct username) over (partition by grp) as ipcnt
           
                         #Adds a column with transitive closure group number for each user
                         from
                         (
                           select user_id, a.username as username, a.ip as ip, certified, grp
                           FROM [{dataset}.person_course] a
                           JOIN [{uname_ip_groups_table}] b
                           ON a.ip = b.ip
                           group by user_id, username, ip, certified, grp
                         )as pc
                          JOIN [{dataset}.stats_attempts_correct] as ac
                          on pc.user_id = ac.user_id
                       )
                   # Since clicking show answer or guessing over and over cannot achieve certification, we should have
                   # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
                   where sum_cert_true > 0
                   and sum_cert_false > 0
                   and ipcnt < 8 #Remove NAT or internet cafe ips                                                     
                 )
               )
               where remove = false
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
 

#-----------------------------------------------------------------------------

def compute_show_ans_before_high_score(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Computes the percentage of show_ans_before and avg_max_dt_seconds between all certified and uncertied users 
    cameo candidate - certified | shadow candidate - uncertified
    ONLY SELECTS the cameo candidate which is MOST SIMILAR (SCORE) to the shadow candidate
    Only selects pairs with at least 10 show ans before
    An ancillary table for sybils which computes the percent of candidate shadow show answers 
    that occur before the corresponding candidate cameo accounts correct answer submission.
    This table also computes median_max_dt_seconds which is the median time between the shadow's
    show_answer and the certified accounts' correct answer. This table also computes the normalized
    pearson correlation.
    This table chooses the FIRST show_answer and the LAST correct submission, to ensure catching
    cases, even if the user tried to figure it out without gaming first.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_show_ans_before_high_score"

    SQL = """
            #Northcutt Code - Show Answer Before, Pearson Correlations, Median Average Times, Optimal Scoring
            ###########################################################################
            #Computes the percentage of show_ans_before and avg_max_dt_seconds between all certified and uncertied users 
            #cameo candidate - certified | shadow candidate - uncertified
            #ONLY SELECTS the cameo candidate which is MOST SIMILAR (SCORE) to the shadow candidate
            #Only selects pairs with at least 10 show ans before
            SELECT
            "{course_id}" as course_id,
            cameo_candidate, shadow_candidate, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
            norm_pearson_corr, unnormalized_pearson_corr, show_ans_before, prob_in_common, best_score
            FROM
            (
              SELECT *,  
                  #Compute score = sum of z-scores - time z-score and find max score
                  (case when (percent_show_ans_before - avgpsa) / stdpsa is null then 0 else (percent_show_ans_before - avgpsa) / stdpsa end) + 
                  (case when 0.5*(show_ans_before - avgpsa) / stdsa is null then 0 else 0.5*(show_ans_before - avgpsa) / stdsa end) + 
                  (case when (norm_pearson_corr - avgcorr) / stdcorr is null then 0 else (norm_pearson_corr - avgcorr) / stdcorr end) - 
                  2*(median_max_dt_seconds  - (case when avgdt_med is null then avgdt_avg else avgdt_med end)) / (case when stddt_med is null then stddt_avg else stddt_med end) as score,
                  max(score) over (partition by cameo_candidate) as best_score
              FROM
              (
                SELECT *, 
                  #std and mean to compute z-scores to allow summing on same scale
                  stddev(avg_max_dt_seconds) over (partition by cameo_candidate) AS stddt_avg,
                  stddev(median_max_dt_seconds) over (partition by cameo_candidate) AS stddt_med,
                  stddev(norm_pearson_corr) over (partition by cameo_candidate) AS stdcorr,
                  stddev(percent_show_ans_before) over (partition by cameo_candidate) AS stdpsa,
                  stddev(show_ans_before) over (partition by cameo_candidate) AS stdsa,
                  avg(median_max_dt_seconds) over (partition by cameo_candidate) AS avgdt_med,
                  avg(avg_max_dt_seconds) over (partition by cameo_candidate) AS avgdt_avg,
                  avg(norm_pearson_corr) over (partition by cameo_candidate) AS avgcorr,
                  avg(percent_show_ans_before) over (partition by cameo_candidate) AS avgpsa,
                  avg(show_ans_before) over (partition by cameo_candidate) AS avgsa
                FROM
                (
                  SELECT
                  cameo_candidate,
                  shadow_candidate,
                  round(median_max_dt_seconds, 3) as median_max_dt_seconds,
                  round(sum(sa_before_pa) / count(*), 4)*100 as percent_show_ans_before,
                  sum(sa_before_pa) as show_ans_before,
                  count(*) as prob_in_common,
                  round(avg(max_dt), 3) as avg_max_dt_seconds,
                  round(corr(sa.time - min_first_check_time, pa.time - min_first_check_time), 8) as norm_pearson_corr,
                  round(corr(sa.time, pa.time), 8) as unnormalized_pearson_corr
                  FROM
                  ( 
                    select sa.username as shadow_candidate,
                    pa.username as cameo_candidate,
                    sa.time, pa.time,
                    sa.time < pa.time as sa_before_pa,
                    (case when sa.time < pa.time then (pa.time - sa.time) / 1e6 else null end) as max_dt,
                    #Calculate median - includes null values - so if you start with many nulls
                    #the median will be a lot farther left (smaller) than it should be.
                    percentile_cont(.5) OVER (PARTITION BY shadow_candidate, cameo_candidate ORDER BY max_dt) AS median_max_dt_seconds,
                    USEC_TO_TIMESTAMP(min_first_check_time) as min_first_check_time
                    FROM
                    (
                      #Certified = false for shadow candidate
                      SELECT sa.username as username, index, sa.time as time  
                      FROM
                      (
                          SELECT 
                          username, index, MIN(time) as time, 
                          count(distinct index) over (partition by username) as nshow_ans_distinct
                          FROM [{dataset}.show_answer] a
                          JOIN [{dataset}.course_axis] b
                          ON a.course_id = b.course_id and a.module_id = b.module_id
                          group each by username, index
                      ) sa
                      JOIN EACH [{dataset}.person_course] pc
                      ON sa.username = pc.username
                      where certified = false
                      and nshow_ans_distinct >  #Shadow must click answer on 40% of problems
                      (
                        SELECT  
                        count(distinct module_id) * 2 / 5
                        FROM [{dataset}.show_answer]
                      )
                    )sa
                    JOIN EACH
                    (
                      #certified = true for cameo candidate
                      SELECT pa.username as username, index, time,
                      MIN(TIMESTAMP_TO_USEC(first_check)) over (partition by index) as min_first_check_time
                      FROM
                      (
                        SELECT 
                        username, index, MAX(time) as time, MIN(time) as first_check
                        FROM [{dataset}.problem_check] a
                        JOIN [{dataset}.course_axis] b
                        ON a.course_id = b.course_id and a.module_id = b.module_id
                        where category = 'problem'
                        and success = 'correct'
                        group each by username, index
                      ) pa
                      JOIN EACH [{dataset}.person_course] pc
                      on pa.username = pc.username
                      where certified = true
                    ) pa
                    on sa.index = pa.index
                    WHERE sa.username != pa.username
                  )
                  group EACH by shadow_candidate, cameo_candidate, median_max_dt_seconds
                  having show_ans_before > 10
                )
              )
            )
            #Only select the cameo shadow candidate pair with the best score
            where score = best_score
            order by avg_max_dt_seconds asc;
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

def compute_show_ans_before(course_id, force_recompute=False, use_dataset_latest=False, force_num_partitions=None):
    '''
    Computes the percentage of show_ans_before and avg_max_dt_seconds between all certified and uncertied users 
    cameo candidate - certified | shadow candidate - uncertified

    An ancillary table for sybils which computes the percent of candidate shadow show answers 
    that occur before the corresponding candidate cameo accounts correct answer submission.
    
    This table also computes median_max_dt_seconds which is the median time between the shadow's
    show_answer and the certified accounts' correct answer. This table also computes the normalized
    pearson correlation.

    This table chooses the FIRST show_answer and the LAST correct submission, to ensure catching
    cases, even if the user tried to figure it out without gaming first.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_show_ans_before"

    #-------------------- partition by nshow_ans_distinct

    def make_sad_partition(pmin, pmax):

        partition = '''nshow_ans_distinct >  # Shadow must click answer on between {pmin} and {pmax} of problems
                      (
                        SELECT  
                        count(distinct module_id) * {pmin}
                        FROM [{dataset}.show_answer]
                      )
        '''.format(dataset=dataset, pmin=pmin, pmax=pmax)
        
        if pmax:
            partition += '''
                      and nshow_ans_distinct <=  
                      (
                        SELECT  
                        count(distinct module_id) * {pmax}
                        FROM [{dataset}.show_answer]
                      )
            '''.format(dataset=dataset, pmin=pmin, pmax=pmax)
        return partition

    num_partitions = 5
    
    if 0:
        partition1 = make_sad_partition('1.0/50', '1.0/20')
        partition2 = make_sad_partition('1.0/20', '1.0/10')
        partition3 = make_sad_partition('1.0/10', '1.0/5')
        partition4 = make_sad_partition('1.0/5', '1.0/3')
        partition5 = make_sad_partition('1.0/3', None)
               
        the_partition = [partition1, partition2, partition3, partition4, partition5]

    #-------------------- partition by username hash
    
    num_persons = bqutil.get_bq_table_size_rows(dataset, 'person_course')
    if force_num_partitions:
        num_partitions = force_num_partitions
        print " --> Force the number of partitions to be %d" % force_num_partitions
    else:
        if num_persons > 60000:
            num_partitions = int(num_persons / 3000)  # because the number of certificate earners is also likely higher
        elif num_persons > 10000:
            num_partitions = int(num_persons / 5000)
        else:
            num_partitions = 2
    print " --> number of persons in %s.person_course is %s; splitting query into %d partitions" % (dataset, num_persons, num_partitions)

    def make_username_partition(nparts, pnum):
        psql = """\n    ABS(HASH(pc.username)) %% %d = %d\n""" % (nparts, pnum)
        return psql

    the_partition = []
    for k in range(num_partitions):
        the_partition.append(make_username_partition(num_partitions, k))

    #-------------------- build sql, one for each partition

    sql = []
    for i in range(num_partitions):
        item = """
            #Northcutt Code - Show Answer Before, Pearson Correlations, Median Average Times, Optimal Scoring
            ###########################################################################
            #Computes the percentage of show_ans_before and avg_max_dt_seconds between all certified and uncertied users 
            #cameo candidate - certified | shadow candidate - uncertified
            #Only selects pairs with at least 10 show ans before
              SELECT
              "{course_id}" as course_id,
              cameo_candidate,
              shadow_candidate,
              round(median_max_dt_seconds, 3) as median_max_dt_seconds,
              round(sum(sa_before_pa) / count(*), 4)*100 as percent_show_ans_before,
              sum(sa_before_pa) as show_ans_before,
              count(*) as prob_in_common,
              round(avg(max_dt), 3) as avg_max_dt_seconds,
              round(corr(sa.time - min_first_check_time, pa.time - min_first_check_time), 8) as norm_pearson_corr,
              round(corr(sa.time, pa.time), 8) as unnormalized_pearson_corr,
              third_quartile_dt_seconds - first_quartile_dt_seconds as dt_iqr,
              dt_std_dev
              FROM
              (
                SELECT *,
                  MAX(median_with_null_rows) OVER (PARTITION BY shadow_candidate, cameo_candidate) AS median_max_dt_seconds,
                  MAX(first_quartile_with_null_rows) OVER (PARTITION BY shadow_candidate, cameo_candidate) AS first_quartile_dt_seconds,
                  MAX(third_quartile_with_null_rows) OVER (PARTITION BY shadow_candidate, cameo_candidate) AS third_quartile_dt_seconds,
                  STDDEV(max_dt) OVER (PARTITION BY shadow_candidate, cameo_candidate) as dt_std_dev
                FROM
                (
                  SELECT *,
                  (CASE WHEN max_dt IS NOT NULL THEN true END) AS has_max_dt,
                  PERCENTILE_CONT(.5) OVER (PARTITION BY has_max_dt,shadow_candidate, cameo_candidate ORDER BY max_dt) AS median_with_null_rows,
                  PERCENTILE_CONT(.25) OVER (PARTITION BY has_max_dt,shadow_candidate, cameo_candidate ORDER BY max_dt) AS first_quartile_with_null_rows,
                  PERCENTILE_CONT(.75) OVER (PARTITION BY has_max_dt,shadow_candidate, cameo_candidate ORDER BY max_dt) AS third_quartile_with_null_rows
                  FROM
                  ( 
                    select sa.username as shadow_candidate,
                    pa.username as cameo_candidate,
                    sa.time, pa.time,
                    sa.time < pa.time as sa_before_pa,
                    (case when sa.time < pa.time then (pa.time - sa.time) / 1e6 end) as max_dt,
                    USEC_TO_TIMESTAMP(min_first_check_time) as min_first_check_time
                    FROM
                    (
                      #Certified = false for shadow candidate
                      SELECT sa.username as username, module_id, sa.time as time  
                      FROM
                      (
                          SELECT 
                          username, module_id, MIN(time) as time, 
                          count(distinct module_id) over (partition by username) as nshow_ans_distinct
                          FROM [{dataset}.show_answer] 
                          group each by username, module_id
                      ) sa
                      JOIN EACH [{dataset}.person_course] pc
                      ON sa.username = pc.username
                      where certified = false
                      and {partition}
                    )sa
                    JOIN EACH
                    (
                      #certified = true for cameo candidate
                      SELECT pa.username as username, module_id, time,
                      MIN(TIMESTAMP_TO_USEC(first_check)) over (partition by module_id) as min_first_check_time
                      FROM
                      (
                        SELECT 
                        username, module_id, MAX(time) as time, MIN(time) as first_check
                        FROM [{dataset}.problem_check]
                        where success = 'correct'
                        group each by username, module_id
                      ) pa
                      JOIN EACH [{dataset}.person_course] pc
                      on pa.username = pc.username
                      where certified = true
                    ) pa
                    on sa.module_id = pa.module_id
                    WHERE sa.username != pa.username
                  )
                )
              )
              group EACH by shadow_candidate, cameo_candidate, median_max_dt_seconds, first_quartile_dt_seconds,
                            third_quartile_dt_seconds, dt_iqr, dt_std_dev
              #having show_ans_before > 10
              having norm_pearson_corr > -1
              and avg_max_dt_seconds is not null
          """.format(dataset=dataset, course_id = course_id, partition=the_partition[i])
        sql.append(item)


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
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    newer_than = datetime.datetime(2015, 6, 2, 19, 0)

    try:
        table_date = bqutil.get_bq_table_last_modified_datetime(dataset, table)
    except Exception as err:
        if 'Not Found' in str(err):
            table_date = None
        else:
            raise
    if not table_date:
        force_recompute = True

    if table_date and (table_date < newer_than):
        print("--> Forcing query recomputation of %s.%s, table_date=%s, newer_than=%s" % (dataset, table,
                                                                                          table_date, newer_than))
        force_recompute = True

    depends_on=["%s.%s" % (dataset, sasbu), ]
    if table_date and not force_recompute:
            # get the latest mod time of tables in depends_on:
            modtimes = [ bqutil.get_bq_table_last_modified_datetime(*(x.split('.',1))) for x in depends_on]
            latest = max([x for x in modtimes if x is not None])
        
            if not latest:
                raise Exception("[make_problem_analysis] Cannot get last mod time for %s (got %s), needed by %s.%s" % (depends_on, modtimes, dataset, table))

            if table_date < latest:
                force_recompute = True

    if force_recompute:
        for i in range(num_partitions): 
            print "--> Running SQL for partition %d of %d" % (i, num_partitions)
            sys.stdout.flush()
            if i == 0:
                try:
                    bqutil.create_bq_table(dataset, table, sql[i], overwrite=True)
                except Exception as err:
                    if (not force_num_partitions) and 'Response too large' in str(err):
                        print err
                        print "==> SQL query failed!  Trying again, with 50% more many partitions"
                        return compute_show_ans_before(course_id, force_recompute=force_recompute, 
                                                       use_dataset_latest=use_dataset_latest, 
                                                       force_num_partitions=int(num_partitions*1.5))
                    else:
                        raise
            else:
                bqutil.create_bq_table(dataset, table, sql[i], overwrite='append')
        if num_partitions > 5:
            print "--> sleeping for 60 seconds to try avoiding bigquery system error - maybe due to data transfer time"
            sys.stdout.flush()
            time.sleep(60)

    nfound = bqutil.get_bq_table_size_rows(dataset, table)
    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()


#-----------------------------------------------------------------------------

def compute_ip_pair_sybils3(course_id, force_recompute=False, use_dataset_latest=False, uname_ip_groups_table=None):
    
    '''
    Sybils3 uses the transitive closure of person course
    The stats_ip_pair_sybils3 table finds all harvester-master GROUPS of users for 
    which the pair have meaningful disparities
    in performance, including:
      - one earning a certificate and the other not
      - one clicking "show answer" many times and the other not

    Typically, the "master", which earns a certificate, has a high percentage
    of correct attempts, while the "harvester" clicks on "show answer" many times,
    and does not earn a certificate.

    Multiple users can belong to the same group, and their IP addresses can be different.

    This requires a table to be pre-computed, which gives the transitive closure over
    all the (username, ip) pairs from both HarvardX and MITx person_course
    '''

    compute_show_ans_before(course_id, force_recompute, use_dataset_latest) 

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_ip_pair_sybils3"

    SQL = """##################################
            # Sybils Version 3.0
            # Instead of same ip, considers users in same grp where
            # where grp is determined by the full transitive closure 
            # of all person_course (username, ip) pairs.
            SELECT
             "{course_id}" as course_id, username,
             user_id, shadow, ip, grp, certified, percent_show_ans_before, show_ans_before, median_max_dt_seconds, 
             norm_pearson_corr, unnormalized_pearson_corr,nshow_answer_unique_problems, percent_correct, frac_complete, avg_max_dt_seconds,
             verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
            FROM
            (  
              SELECT 
               user_id, username, shadow, ip, grp, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified,
               verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade,
               show_ans_before, unnormalized_pearson_corr,
               SUM(certified = true) OVER (PARTITION BY grp) AS sum_cert_true,
               SUM(certified = false) OVER (PARTITION BY grp) AS sum_cert_false
              FROM
              ( 
                # Find all users with >1 accounts, same ip address, different certification status
                SELECT
                 pc.user_id as user_id,
                 username, shadow, ip, grp,
                 percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                 nshow_answer_unique_problems, show_ans_before, unnormalized_pearson_corr,
                 ROUND(ac.percent_correct, 2) AS percent_correct,
                 frac_complete,
                 ac.certified AS certified,
                 verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade,                       
                 COUNT(DISTINCT username) OVER (PARTITION BY grp) AS ipcnt
                #Adds a column with transitive closure group number for each user
                FROM
                (
                  SELECT 
                   user_id, username, shadow, ip, certified, grp, 
                   pc.show_ans_before AS show_ans_before, pc.unnormalized_pearson_corr AS unnormalized_pearson_corr,
                   pc.percent_show_ans_before AS percent_show_ans_before, pc.avg_max_dt_seconds AS avg_max_dt_seconds, 
                   pc.median_max_dt_seconds AS median_max_dt_seconds, pc.norm_pearson_corr AS norm_pearson_corr,
                   verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
                  FROM
                  (
                    SELECT
                     user_id, username, shadow_candidate AS shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                     percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                     verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade,
                     GROUP_CONCAT(username) OVER (PARTITION BY grp) AS grp_usernames
                    FROM
                    (
                      SELECT
                       user_id, a.username, a.ip AS ip, certified, grp,
                       GROUP_CONCAT(a.username) OVER (partition by grp) AS grp_usernames,
                       (CASE WHEN a.mode = "verified" THEN true ELSE false END) AS verified,
                       countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
                       (sum_dt / 60 / 60/ 24) as time_active_in_days, grade
                      FROM [{dataset}.person_course] AS a
                      JOIN EACH [{uname_ip_groups_table}] AS b
                      ON a.ip = b.ip and a.username = b.username
                      GROUP BY user_id, a.username, ip, certified, grp, verified, countryLabel, start_time, last_event,
                              nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
                    ) AS pc
                    OUTER LEFT JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                    ON pc.username = sa.cameo_candidate #join on cameo candidate
                    #Remove if few show answer before or too high time between show answer and submission
                    WHERE (sa.percent_show_ans_before >= 20
                    AND sa.median_max_dt_seconds < 5e4
                    AND sa.norm_pearson_corr > 0
                    AND (','+grp_usernames+',' CONTAINS ','+sa.shadow_candidate+','))#Only keep rows where cameo's shadow is in pc
                    OR certified = false #Keep all shadows for now
                  ) AS pc
                  OUTER LEFT JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                  ON pc.username = sa.shadow_candidate #join on shadow candidate
                  #Remove if few show answer before or too high time between show answer and submission
                  WHERE (sa.percent_show_ans_before >= 20
                  AND sa.median_max_dt_seconds < 5e4
                  AND sa.norm_pearson_corr > 0
                  AND (','+grp_usernames+',' CONTAINS ','+sa.cameo_candidate+','))#Only keep shadows if cameo in pc
                  OR certified = true #Keep previously found cameos
                  GROUP BY user_id, username, shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                    percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                    verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
                ) AS pc
                JOIN EACH [{dataset}.stats_attempts_correct] AS ac
                ON pc.user_id = ac.user_id
                #remove shadows with greater than 70% attempts correct
                WHERE pc.certified = true
                OR percent_correct < 70
              )
              WHERE ipcnt < 10 #Remove NAT or internet cafe ips                                                     
            )
            # Since clicking show answer or guessing over and over cannot achieve certification, we should have
            # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
            # Remove entire group if all the masters or all the harvesters were removed
            WHERE sum_cert_true > 0
            AND sum_cert_false > 0
            # Order by ip to group master and harvesters together. Order by certified so that we always have masters above harvester accounts.
            ORDER BY grp ASC, certified DESC
          """.format(dataset=dataset, course_id=course_id, uname_ip_groups_table=uname_ip_groups_table)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_show_ans_before"
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
                                newer_than=datetime.datetime(2015, 6, 2, 19, 0),
                                depends_on=["%s.%s" % (dataset, sasbu),
                                        ],
                            )
    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> [%s] Sybils 3.0 Found %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()
    

    


#-----------------------------------------------------------------------------

#def compute_ip_pair_sybils3_features(course_id, force_recompute=False, use_dataset_latest=False):
#    '''
#    The stats_ip_pair_sybils3_features table has all the info in stats_ip_pair_sybils3, but also
#    includes statistical features drawn from person_course and stats_attempts_correct
#    '''

#    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
#    table = "stats_ip_pair_sybils3_features"

#    SQL = """
#          SELECT 
#          "{course_id}" as course_id,
#          s.user_id as user_id,
#          s.username as username,
#          s.shadow as shadow,
#          s.ip as ip,
#          s.grp as grp,
#          s.certified as certified,
#          #count(*) over (partition by username) as total_cameos,
#          s.percent_show_ans_before as percent_show_ans_before,
#          s.median_max_dt_seconds as median_max_dt_seconds,
#          s.norm_pearson_corr as norm_pearson_corr,
#          pc.nshow_answer_unique_problems as nshow_answer_unique_problems,
#          pc.percent_correct as percent_correct,
#          pc.nproblems as nproblems,
#          pc.frac_complete as frac_complete,
#          s.avg_max_dt_seconds as avg_max_dt_seconds,
#          pc.verified as verified,
#          pc.countryLabel as countryLabel,
#          pc.start_time as start_time,
#          pc.last_event as last_event,
#          pc.nforum_posts as nforum_posts,
#          pc.nprogcheck as nprogcheck,
#          pc.nvideo as nvideo,
#          pc.time_active_in_days as time_active_in_days,
#          pc.grade as grade
#          FROM 
#          (
#            SELECT
#            pc.user_id as user_id,
#            percent_correct,
#            nshow_answer_unique_problems,
#            nproblems,
#            frac_complete,
#            pc.course_id as course_id,
#            (case when pc.mode = "verified" then true else false end) as verified,
#            countryLabel,
#            start_time,
#            last_event,
#            nforum_posts,
#            nprogcheck,
#            nvideo,
#            (sum_dt / 60 / 60/ 24) as time_active_in_days,
#            grade
#            FROM
#            [{dataset}.person_course] pc
#            JOIN EACH [{dataset}.stats_attempts_correct] as ac
#            ON pc.user_id = ac.user_id
#          ) pc
#          JOIN [{dataset}.stats_ip_pair_sybils3] s
#          ON pc.user_id = s.user_id
#          ORDER BY grp asc, certified DESC
#          """.format(dataset=dataset, course_id=course_id)

#    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
#    sys.stdout.flush()

#    sasbu = "stats_ip_pair_sybils3"
#    try:
#        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
#        has_attempts_correct = (tinfo is not None)
#    except Exception as err:
#        print "Error %s getting %s.%s" % (err, dataset, sasbu)
#        has_attempts_correct = False
#    if not has_attempts_correct:
#        print "---> No %s table; skipping %s" % (sasbu, table)
#        return

#    bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
#                                newer_than=datetime.datetime(2015, 5, 21, 03, 10),
#                                depends_on=["%s.%s" % (dataset, sasbu),
#                                        ],
#                            )
#    if not bqdat:
#        nfound = 0
#    else:
#        nfound = len(bqdat['data'])
#    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
#    sys.stdout.flush()

#-----------------------------------------------------------------------------

def compute_ip_pair_sybils3_unfiltered(course_id, force_recompute=False, use_dataset_latest=False, uname_ip_groups_table=None):
    
    #compute_show_ans_before(course_id, force_recompute, use_dataset_latest)

    '''
    Sybils3 uses the transitive closure of person course UNFILTERED
    The stats_ip_pair_sybils3 table finds all harvester-master GROUPS of users for 
    which the pair have meaningful disparities
    in performance, including:
      - one earning a certificate and the other not
      - one clicking "show answer" many times and the other not
    Typically, the "master", which earns a certificate, has a high percentage
    of correct attempts, while the "harvester" clicks on "show answer" many times,
    and does not earn a certificate.
    Multiple users can belong to the same group, and their IP addresses can be different.
    This requires a table to be pre-computed, which gives the transitive closure over
    all the (username, ip) pairs from both HarvardX and MITx person_course
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_ip_pair_sybils3_unfiltered"

    SQL = """# Northcutt SQL for finding sybils UNFILTERED
            ##################################
            # Sybils Version 3.0
            # Instead of same ip, considers users in same grp where
            # where grp is determined by the full transitive closure 
            # of all person_course (username, ip) pairs.
            SELECT
             "{course_id}" as course_id, user_id, username, shadow as harvester, ip, grp, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified,
               show_ans_before, unnormalized_pearson_corr, ipcnt
            FROM
            (  
              SELECT 
               user_id, username, shadow, ip, grp, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified,
               show_ans_before, unnormalized_pearson_corr, ipcnt,
               SUM(certified = true) OVER (PARTITION BY grp) AS sum_cert_true,
               SUM(certified = false) OVER (PARTITION BY grp) AS sum_cert_false
              FROM
              ( 
                # Find all users with >1 accounts, same ip address, different certification status
                SELECT
                 pc.user_id as user_id,
                 username, shadow, ip, grp,
                 percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                 nshow_answer_unique_problems, show_ans_before, unnormalized_pearson_corr,
                 ROUND(ac.percent_correct, 2) AS percent_correct,
                 frac_complete,
                 ac.certified AS certified,                       
                 COUNT(DISTINCT username) OVER (PARTITION BY grp) AS ipcnt
                #Adds a column with transitive closure group number for each user
                FROM
                (
                  SELECT 
                   user_id, username, shadow, ip, certified, grp, 
                   pc.show_ans_before AS show_ans_before, pc.unnormalized_pearson_corr AS unnormalized_pearson_corr,
                   pc.percent_show_ans_before AS percent_show_ans_before, pc.avg_max_dt_seconds AS avg_max_dt_seconds, 
                   pc.median_max_dt_seconds AS median_max_dt_seconds, pc.norm_pearson_corr AS norm_pearson_corr
                  FROM
                  (
                    SELECT
                     user_id, username, shadow_candidate AS shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                     percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                     GROUP_CONCAT(username) OVER (PARTITION BY grp) AS grp_usernames
                    FROM
                    (
                      SELECT
                       user_id, a.username, a.ip AS ip, certified, grp,
                       GROUP_CONCAT(a.username) OVER (partition by grp) AS grp_usernames
                      FROM [{dataset}.person_course] AS a
                      JOIN EACH [{uname_ip_groups_table}] AS b
                      ON a.ip = b.ip and a.username = b.username
                      GROUP BY user_id, a.username, ip, certified, grp
                    ) AS pc
                    OUTER LEFT JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                    ON pc.username = sa.cameo_candidate #join on cameo candidate
                    #Remove if few show answer before or too high time between show answer and submission
                    #WHERE (sa.percent_show_ans_before >= 20
                    #AND sa.median_max_dt_seconds < 5e4
                    #AND sa.norm_pearson_corr > 0
                    WHERE (','+grp_usernames+',' CONTAINS ','+sa.shadow_candidate+',')#Only keep rows where cameo's shadow is in pc
                    OR certified = false #Keep all shadows for now
                  ) AS pc
                  OUTER LEFT JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                  ON pc.username = sa.shadow_candidate #join on shadow candidate
                  #Remove if few show answer before or too high time between show answer and submission
                  #WHERE (sa.percent_show_ans_before >= 20
                  #AND sa.median_max_dt_seconds < 5e4
                  #AND sa.norm_pearson_corr > 0
                  WHERE (','+grp_usernames+',' CONTAINS ','+sa.cameo_candidate+',')#Only keep shadows if cameo in pc
                  OR certified = true #Keep previously found cameos
                  GROUP BY user_id, username, shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                    percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr
                ) AS pc
                JOIN EACH [{dataset}.stats_attempts_correct] AS ac
                ON pc.user_id = ac.user_id
                #remove shadows with greater than 70% attempts correct
                #WHERE pc.certified = true
                #OR percent_correct < 70
              )
              WHERE ipcnt < 10 #Remove NAT or internet cafe ips                                                     
            )
            # Since clicking show answer or guessing over and over cannot achieve certification, we should have
            # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
            # Remove entire group if all the masters or all the harvesters were removed
            WHERE sum_cert_true > 0
            AND sum_cert_false > 0
            # Order by ip to group master and harvesters together. Order by certified so that we always have masters above harvester accounts.
            ORDER BY grp ASC, certified DESC
          """.format(dataset=dataset, course_id=course_id, uname_ip_groups_table=uname_ip_groups_table)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_show_ans_before"
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
    print "--> [%s] Sybils 3.0 Found %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()
    
#-----------------------------------------------------------------------------

def compute_cameo_demographics(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute count, gender, bachplus, in_US, median age, for a four-way dichotemy of participants,
    with respect to CAMEOs:

        certified_cameo, 
        certified_non_cameo, 
        non_certified_cameo_harvester, 
        non_certified_participant_non_harvester
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = "stats_cameo_demographics"
    
    SQL = """# Compute CAMEO demographics, based on 4-way dichotemy of participants wrt CAMEOs
           SELECT 
              "{course_id}" as course_id,
               sum(case when certified_cameo then 1 end) as n_certified_cameo,
               sum(case when certified_cameo and gender='m' then 1 end) as n_male_certified_cameo,
               sum(case when certified_cameo and gender='f' then 1 end) as n_female_certified_cameo,
               sum(case when certified_cameo and bachplus=1 then 1 end) as n_bachplus_certified_cameo,
               sum(case when certified_cameo and sub_bach=1 then 1 end) as n_sub_bachplus_certified_cameo,
               sum(case when certified_cameo and in_US then 1 end) as n_in_US_certified_cameo,
               sum(case when certified_cameo and not_in_US then 1 end) as n_not_in_US_certified_cameo,
               sum(case when certified_cameo and verified then 1 end) as n_id_verified_certified_cameo,
               max(case when certified_cameo then median_age_in_2015_certified_cameo end) as median_age_in_2015_certified_cameo,
           
               sum(case when certified_non_cameo then 1 end) as n_certified_non_cameo,
               sum(case when certified_non_cameo and gender='m' then 1 end) as n_male_certified_non_cameo,
               sum(case when certified_non_cameo and gender='f' then 1 end) as n_female_certified_non_cameo,
               sum(case when certified_non_cameo and bachplus=1 then 1 end) as n_bachplus_certified_non_cameo,
               sum(case when certified_non_cameo and sub_bach=1 then 1 end) as n_sub_bachplus_certified_non_cameo,
               sum(case when certified_non_cameo and in_US then 1 end) as n_in_US_certified_non_cameo,
               sum(case when certified_non_cameo and not_in_US then 1 end) as n_not_in_US_certified_non_cameo,
               sum(case when certified_non_cameo and verified then 1 end) as n_id_verified_certified_non_cameo,
               max(case when certified_non_cameo then median_age_in_2015_certified_non_cameo end) as median_age_in_2015_certified_non_cameo,
           
               sum(case when certified then 1 end) as n_certified,
           
               sum(case when non_certified_cameo_harvester then 1 end) as n_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and gender='m' then 1 end) as n_male_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and gender='f' then 1 end) as n_female_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and bachplus=1 then 1 end) as n_bachplus_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and sub_bach=1 then 1 end) as n_sub_bachplus_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and in_US then 1 end) as n_in_US_certified_non_certified_cameo_harvester,
               sum(case when non_certified_cameo_harvester and not_in_US then 1 end) as n_not_in_US_certified_non_certified_cameo_harvester,
               max(case when non_certified_cameo_harvester then median_age_in_2015_non_certified_cameo_harvester end) as median_age_in_2015_non_certified_cameo_harvester,
           
               sum(case when non_certified_participant_non_harvester then 1 end) as n_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and gender='m' then 1 end) as n_male_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and gender='f' then 1 end) as n_female_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and bachplus=1 then 1 end) as n_bachplus_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and sub_bach=1 then 1 end) as n_sub_bachplus_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and in_US then 1 end) as n_in_US_certified_non_certified_participant_non_harvester,
               sum(case when non_certified_participant_non_harvester and not_in_US then 1 end) as n_not_in_US_certified_non_certified_participant_non_harvester,
               max(case when non_certified_participant_non_harvester then median_age_in_2015_non_certified_participant_non_harvester end) as median_age_in_2015_non_certified_participant_non_harvester,
           
               sum(case when viewed then 1 end) as n_participants,
               sum(case when (viewed and not certified) then 1 end) as n_non_certified_participants,
           FROM
           (
               SELECT *,
                   # age variables, dichotemized by cameos, for the median calculation
                   (case when certified_cameo then course_id end) cid_certified_cameo,
                   (case when certified_non_cameo then course_id end) cid_certified_non_cameo,
                   (case when non_certified_cameo_harvester then course_id end) cid_non_certified_cameo_harvester,
                   (case when non_certified_participant_non_harvester then course_id end) cid_non_certified_participant_non_harvester,
                   
                   PERCENTILE_DISC(0.5) over (partition by cid_certified_cameo order by age_in_2015) as median_age_in_2015_certified_cameo,
                   PERCENTILE_DISC(0.5) over (partition by cid_certified_non_cameo order by age_in_2015) as median_age_in_2015_certified_non_cameo,
                   PERCENTILE_DISC(0.5) over (partition by cid_non_certified_cameo_harvester order by age_in_2015) as median_age_in_2015_non_certified_cameo_harvester,
                   PERCENTILE_DISC(0.5) over (partition by cid_non_certified_participant_non_harvester order by age_in_2015) as median_age_in_2015_non_certified_participant_non_harvester,
               FROM
               (
                   SELECT PC.course_id as course_id,
                       PC.username as username,
                       SY.username as cameo_username,
                       PC.certified as certified,
                       PC.viewed as viewed,
                       PC.gender as gender,
                       PC.LoE as LoE,
                       (2015 - PC.YoB) as age_in_2015,
                       PC.YoB as YoB,
                       PC.cc_by_ip as cc_by_ip,
                       (PC.mode="verified") as verified,
                       PC.sum_dt / 60 / 60 / 24 as time_active_in_days,
                       PC.email_domain as email_domain,
                       (cc_by_ip = "US") as in_US,
                       (cc_by_ip is not null and not (cc_by_ip = "US")) as not_in_US,
           
                       # bachelor's degree or higher, versus sub-bachelor's degree
                       (case when ((LoE="b") or (LoE="m") or (LoE="p") or (LoE="p_oth") or (LoE="p_se")) then 1 else 0 end) as bachplus,
                       (case when ((LoE="a") or (LoE="el") or (LoE="hs") or (LoE="jhs") or (LoE="none")) then 1 else 0 end) as sub_bach,
           
                       # four variables which dichotemize cameos (certified or not, harvester or not)
                       (case when (SY.username = PC.username) and PC.certified then true else false end) as certified_cameo,
                       (case when ((SY.username is null) or (SY.username != PC.username)) and PC.certified then true else false end) as certified_non_cameo,
                       (case when (SY.username = PC.username) and not PC.certified and PC.viewed then true else false end) as non_certified_cameo_harvester,
                       (case when ((SY.username is null) or (SY.username != PC.username)) and not PC.certified and PC.viewed then true else false end) as non_certified_participant_non_harvester,
           
                   FROM 
                       [{dataset}.person_course] PC
                   LEFT JOIN
                       (   SELECT username from [{dataset}.stats_ip_pair_sybils3] group by username ) SY
                   on  # SY.course_id = PC.course_id AND
                       SY.username = PC.username
                   WHERE PC.viewed
               )
           ) 
           GROUP BY course_id
           ORDER BY course_id
          """.format(dataset=dataset, course_id=course_id)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_ip_pair_sybils3"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        has_attempts_correct = (tinfo is not None)
    except Exception as err:
        print "Error %s getting %s.%s" % (err, dataset, sasbu)
        has_attempts_correct = False
    if not has_attempts_correct:
        print "---> No %s table; skipping %s" % (sasbu, table)
        return

    try:
        bqdat = bqutil.get_bq_table(dataset, table, SQL, force_query=force_recompute,
                                    newer_than=datetime.datetime(2015, 4, 29, 22, 00),
                                    depends_on=["%s.%s" % (dataset, sasbu),
                                            ],
                                    startIndex=-2,
                                )
    except Exception as err:
        print "[compute_cameo_demographics] oops, failed in running SQL=%s, err=%s" % (SQL, err)
        raise

    print "--> Done with %s, results=%s" % (table, json.dumps(bqdat['data'][0], indent=4))
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
                         where success = 'correct'
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
                 username, index, MIN(time) as time
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
