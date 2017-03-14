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

csv.field_size_limit(13107200)

def analyze_problems(course_id, basedir=None, datedir=None, force_recompute=False,
                     use_dataset_latest=False,
                     do_problem_grades=True,
                     do_show_answer=True,
                     do_problem_analysis=True,
                     only_step=None,
                     use_latest_sql_dir=False,
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
                              use_dataset_latest=use_dataset_latest,
                              use_latest_sql_dir=use_latest_sql_dir,
                          )
        
    #-----------------------------------------------------------------------------

def make_problem_analysis(course_id, basedir=None, datedir=None, force_recompute=False,
                          use_dataset_latest=False, raise_exception_on_parsing_error=False,
                          use_latest_sql_dir=False):

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest or use_latest_sql_dir)
    
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

        if 'student_answers' not in state:  #'student_answers' did not exist in some Davidson courses
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
                     'attempts': int(state.get('attempts', 0)),
                     'done': state['done'],
                     'grade': float(line.get('grade', 0)),
                     'max_grade': float(line.get('max_grade', 0)),
                     'created': line['created'],
                     }
        except Exception as err:
            print "---> [%d] Oops, error in transcribing entry, err=%s" % (cnt, str(err))
            print "     state = %s" % state
            print "     line = %s" % line
            if raise_exception_on_parsing_error:
                raise
            else:
                print "    skipping line!"

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
               # round(nproblems / total_problems, 4) as frac_complete,
               (nproblems / total_problems) as frac_complete,
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
                                newer_than=datetime.datetime(2016, 4, 21, 16, 00),
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
          
                      # Removes any user not in problem_analysis (use LEFT OUTER join to include)
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
            master_candidate, harvester_candidate, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
            norm_pearson_corr, unnormalized_pearson_corr, show_ans_before, prob_in_common, best_score
            FROM
            (
              SELECT *,  
                  #Compute score = sum of z-scores - time z-score and find max score
                  (case when (percent_show_ans_before - avgpsa) / stdpsa is null then 0 else (percent_show_ans_before - avgpsa) / stdpsa end) + 
                  (case when 0.5*(show_ans_before - avgpsa) / stdsa is null then 0 else 0.5*(show_ans_before - avgpsa) / stdsa end) + 
                  (case when (norm_pearson_corr - avgcorr) / stdcorr is null then 0 else (norm_pearson_corr - avgcorr) / stdcorr end) - 
                  2*(median_max_dt_seconds  - (case when avgdt_med is null then avgdt_avg else avgdt_med end)) / (case when stddt_med is null then stddt_avg else stddt_med end) as score,
                  max(score) over (partition by master_candidate) as best_score
              FROM
              (
                SELECT *, 
                  #std and mean to compute z-scores to allow summing on same scale
                  stddev(avg_max_dt_seconds) over (partition by master_candidate) AS stddt_avg,
                  stddev(median_max_dt_seconds) over (partition by master_candidate) AS stddt_med,
                  stddev(norm_pearson_corr) over (partition by master_candidate) AS stdcorr,
                  stddev(percent_show_ans_before) over (partition by master_candidate) AS stdpsa,
                  stddev(show_ans_before) over (partition by master_candidate) AS stdsa,
                  avg(median_max_dt_seconds) over (partition by master_candidate) AS avgdt_med,
                  avg(avg_max_dt_seconds) over (partition by master_candidate) AS avgdt_avg,
                  avg(norm_pearson_corr) over (partition by master_candidate) AS avgcorr,
                  avg(percent_show_ans_before) over (partition by master_candidate) AS avgpsa,
                  avg(show_ans_before) over (partition by master_candidate) AS avgsa
                FROM
                (
                  SELECT
                  master_candidate,
                  harvester_candidate,
                  round(median_max_dt_seconds, 3) as median_max_dt_seconds,
                  round(sum(sa_before_pa) / count(*), 4)*100 as percent_show_ans_before,
                  sum(sa_before_pa) as show_ans_before,
                  count(*) as prob_in_common,
                  round(avg(max_dt), 3) as avg_max_dt_seconds,
                  round(corr(sa.time - min_first_check_time, pa.time - min_first_check_time), 8) as norm_pearson_corr,
                  round(corr(sa.time, pa.time), 8) as unnormalized_pearson_corr
                  FROM
                  ( 
                    select sa.username as harvester_candidate,
                    pa.username as master_candidate,
                    sa.time, pa.time,
                    sa.time < pa.time as sa_before_pa,
                    (case when sa.time < pa.time then (pa.time - sa.time) / 1e6 else null end) as max_dt,
                    #Calculate median - includes null values - so if you start with many nulls
                    #the median will be a lot farther left (smaller) than it should be.
                    percentile_cont(.5) OVER (PARTITION BY harvester_candidate, master_candidate ORDER BY max_dt) AS median_max_dt_seconds,
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
                  group EACH by harvester_candidate, master_candidate, median_max_dt_seconds
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

def ncertified(course_id, use_dataset_latest=True,
               testing=False, project_id=None):
  '''Returns the number of certified users in course course_id
  '''
  dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
  SQL = '''
  SELECT COUNT(certified) as ncertified
  FROM [{dataset}.person_course]
  WHERE certified = True
  '''.format(dataset=project_id + ":" + dataset if testing else dataset, course_id=course_id)

  table_id = 'ncertified' + str(abs(hash(course_id))) #Unique id to prevent issues when running in parallel
  dataset = "curtis_northcutt" if testing else dataset
  bqutil.create_bq_table(dataset_id=dataset, 
                         table_id=table_id, sql=SQL, overwrite=True)

  data = bqutil.get_table_data(dataset_id=dataset, table_id=table_id)
  n = int(data['data'][0]['ncertified'])

  bqutil.delete_bq_table(dataset_id=dataset, table_id=table_id)
  return n




SQL = '''
SELECT
  harvester_candidate, master_candidate,
  sa.time, ca.time,  
  sa.time < ca.time as sa_before_pa,
  sa.ip, ca.ip, ncorrect, nattempts,
  sa.ip == ca.ip as same_ip,
  (case when sa.time < ca.time then (ca.time - sa.time) / 1e6 end) as dt,
  (CASE WHEN sa.time < ca.time THEN true END) AS has_dt,
  USEC_TO_TIMESTAMP(min_first_check_time) as min_first_check_time,
  percent_attempts_correct,
  CH_modal_ip, modal_ip,

  #Haversine: Uses (latitude, longitude) to compute straight-line distance in miles
  7958.75 * ASIN(SQRT(POW(SIN((RADIANS(lat2) - RADIANS(lat1))/2),2) 
  + COS(RADIANS(lat1)) * COS(RADIANS(lat2)) 
  * POW(SIN((RADIANS(lon2) - RADIANS(lon1))/2),2))) AS mile_dist_between_modal_ips,

  #Compute lags ONLY for show answers that occur before correct answers
  LAG(sa.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY sa.time ASC) AS sa_lag,
  LAG(ca.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ca.time ASC) AS ma_lag,

  #Find inter-time of dt for each pair
  #LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY dt ASC) AS dt_lag
  LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ca.time ASC) AS dt_lag
FROM
(
  SELECT
    *
  FROM
  (
    SELECT
      *,
      MIN(sa.time) OVER (PARTITION BY harvester_candidate, master_candidate, sa.module_id) AS min_sa_time,
      MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY harvester_candidate, master_candidate, sa.module_id) AS nearest_sa_before
    FROM
    (
      #HARVESTER
      SELECT
        sa.a.username as harvester_candidate, a.module_id as module_id, sa.a.time as time, sa.ip as ip, 
        pc.ip as CH_modal_ip, pc.latitude as lat2, pc.longitude as lon2,
        nshow_ans_distinct
      FROM
      (
        SELECT 
          a.time,
          a.username,
          a.module_id,
          ip,
          count(distinct a.module_id) over (partition by a.username) as nshow_ans_distinct
        FROM [{dataset}.show_answer] a
        JOIN EACH [{problem_check_show_answer_ip_table}] b
        ON a.time = b.time AND a.username = b.username AND a.module_id = b.module_id
        WHERE b.event_type = 'show_answer'
      ) sa
      JOIN EACH [{dataset}.person_course] pc
      ON sa.a.username = pc.username
      WHERE {not_certified_filter}
      AND nshow_ans_distinct >= 5 #to reduce size
      AND {partition}
    )sa
    JOIN EACH
    (
      #Certified CAMEO USER
      SELECT 
        ca.a.username AS master_candidate, module_id, time, ca.ip as ip,
        pc.ip as modal_ip, pc.latitude as lat1, pc.longitude as lon1,
        min_first_check_time, ncorrect, nattempts,
        100.0 * ncorrect / nattempts AS percent_attempts_correct
      FROM
      (
        SELECT 
        a.username, a.module_id as module_id,
        MIN(a.time) as time,  #MIN == FIRST because ordered by time
        FIRST(ip) AS ip, min_first_check_time, nattempts,
        COUNT(DISTINCT module_id) OVER (PARTITION BY a.username) as ncorrect
        FROM 
        (
          SELECT 
            a.time,
            a.username,
            a.module_id,
            a.success,
            ip,
            MIN(TIMESTAMP_TO_USEC(a.time)) OVER (PARTITION BY a.module_id) as min_first_check_time,
            COUNT(DISTINCT CONCAT(a.module_id, STRING(a.attempts))) OVER (PARTITION BY a.username) AS nattempts #unique
          FROM [{dataset}.problem_check] a
          JOIN EACH [{problem_check_show_answer_ip_table}] b
          ON a.time = b.time AND a.username = b.username AND a.course_id = b.course_id AND a.module_id = b.module_id
          WHERE b.event_type = 'problem_check'
        )
        WHERE a.success = 'correct' 
        GROUP EACH BY a.username, module_id, min_first_check_time, nattempts
        ORDER BY time ASC
      ) ca
      JOIN EACH [{dataset}.person_course] pc
      ON ca.a.username = pc.username
      WHERE {certified_filter}
      AND ncorrect >= 5 #to reduce size
    ) ca
    ON sa.module_id = ca.module_id
    WHERE master_candidate != harvester_candidate
  )
  WHERE (nearest_sa_before IS NULL AND sa.time = min_sa_time) #if no positive dt, use min show answer time resulting in least negative dt 
  OR (nearest_sa_before = sa.time) #otherwise use show answer time resulting in smallest positive dt 
)'''
                    

#-----------------------------------------------------------------------------

def compute_stats_problems_cameod(course_id, use_dataset_latest=True, 
                                  cameo_users_table="mitx-research:core.cameo_master",
                                  testing=False, testing_dataset=None, project_id=None, 
                                  problem_check_show_answer_ip_table=None, overwrite=True):
    '''Computes all problems for which the CAMEO users in cameo_users_table
    likely used the CAMEO strategy to obtain correct answers.
    '''
    table = "stats_problems_cameod"
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    #[c_p,c_d,c_t] = cameo_users_table.replace(':',' ').replace('.',' ').split()

    if testing and testing_dataset is None:
      print "If testing == true, valid testing_dataset must be provided."
      raise ValueError("When testing == true, valid testing_dataset must be provided.")
      
    if problem_check_show_answer_ip_table is None:
        ip_table_id = 'stats_problem_check_show_answer_ip'
        if testing:
            default_table_info = bqutil.get_bq_table_info(dataset, ip_table_id, project_id=project_id)
            if default_table_info is None:
                #Default table doesn't exist, Check if testing table exists
                testing_table_info = bqutil.get_bq_table_info(testing_dataset,
                                     dataset + '_' + ip_table_id, project_id='mitx-research')
                if testing_table_info is None:
                    compute_problem_check_show_answer_ip(course_id, use_dataset_latest, testing=True, 
                                                         testing_dataset=testing_dataset, project_id=project_id)
                problem_check_show_answer_ip_table = 'mitx-research:' + testing_dataset + '.' + dataset + '_' + ip_table_id
            else:
                problem_check_show_answer_ip_table = project_id + ':'+ dataset + '.' + ip_table_id            
        else:
            default_table_info = bqutil.get_bq_table_info(dataset,ip_table_id)
            if default_table_info is None:
                compute_problem_check_show_answer_ip(course_id, use_dataset_latest)
            problem_check_show_answer_ip_table = dataset + '.' + ip_table_id

    SQL = '''
    SELECT
      cameo_master, cameo_harvester, 
      ca.module_id as module_id,
      ca.time, sa.time,
      ca.ip, sa.ip,
      sa.ip == ca.ip as same_ip,
      (case when sa.time < ca.time then (ca.time - sa.time) / 1e6 end) as dt
    FROM
    (
      SELECT
        *
      FROM
      (
        SELECT
          *, CONCAT(cameo_master, cameo_harvester) AS grp,
          MIN(sa.time) OVER (PARTITION BY cameo_harvester, cameo_master, sa.module_id) AS min_sa_time,
          MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY cameo_harvester, cameo_master, sa.module_id) AS nearest_sa_before
        FROM
        (
          #HARVESTER
          SELECT 
            a.time as time,
            a.username as cameo_harvester,
            a.module_id as module_id,
            ip
          FROM [{dataset}.show_answer] a
          JOIN EACH [{problem_check_show_answer_ip_table}] b
          ON a.time = b.time AND a.username = b.username AND a.module_id = b.module_id
          WHERE b.event_type = 'show_answer'
          AND a.username in 
          (
            SELECT CH
            FROM [{cameo_users_table}]
            WHERE course_id = "{course_id}"
          )
        ) sa
        JOIN EACH
        (
          #Certified CAMEO USER
          SELECT 
            MIN(a.time) as time,  #First correct answer
            a.username as cameo_master,
            a.module_id as module_id,
            FIRST(ip) AS ip #Associated first ip (because order by time)
          FROM [{dataset}.problem_check] a
          JOIN EACH [{problem_check_show_answer_ip_table}] b
          ON a.time = b.time AND a.username = b.username AND a.course_id = b.course_id AND a.module_id = b.module_id
          WHERE b.event_type = 'problem_check'
          AND a.success = 'correct'
          AND a.username in
          (
            SELECT username
            FROM [{cameo_users_table}]
            WHERE course_id = "{course_id}"
          )
          GROUP EACH BY cameo_master, module_id
          ORDER BY time ASC
        ) ca
        ON sa.module_id = ca.module_id
        HAVING grp in #Only select CAMEO pairs
        (
          SELECT CONCAT(username, CH)
          FROM [{cameo_users_table}]
          WHERE course_id = "{course_id}"
        )
      )
      WHERE ((nearest_sa_before IS NULL AND sa.time = min_sa_time) #if no positive dt, use min show answer time resulting in least negative dt 
      OR (nearest_sa_before = sa.time)) #otherwise use show answer time resulting in smallest positive dt 
    )
    WHERE sa.time < ca.time # Show answer occurs before correct answer
    ORDER BY cameo_master, cameo_harvester, ca.time'''.format(dataset=project_id + ":" + dataset if testing else dataset, 
                course_id=course_id, 
                problem_check_show_answer_ip_table=problem_check_show_answer_ip_table,
                cameo_users_table=cameo_users_table)

    print "[compute_stats_problems_cameod] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    bqutil.create_bq_table(testing_dataset if testing else dataset,
                           dataset+'_'+table if testing else table, 
                           SQL, overwrite=overwrite, allowLargeResults=True,
                           sql_for_description="\nCreated by Curtis G. Northcutt\n\n"+SQL)

    nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset if testing else dataset, 
                                           table_id=dataset+'_'+table if testing else table)

    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()

    return ("mitx_research:"+testing_dataset+"."+dataset+'_'+table) if testing else (dataset+"."+table)


#-----------------------------------------------------------------------------

def course_started_after_switch_to_verified_only(course_id, use_dataset_latest=True,
                 testing=False, project_id=None):
    '''Returns true if course started after December 7,l 2015,
    the date when certificates were switched to verified only
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    SQL = '''
    SELECT 
      MAX(min_start_time) >= TIMESTAMP('2015-12-07 00:00:00') as after_switch_to_verified
    FROM
    (
      SELECT MIN(due) AS min_start_time
      FROM [{dataset}.course_axis] 
    ),
    (
      SELECT TIMESTAMP(min_start_time) AS min_start_time
      FROM [mitx-data:course_report_latest.broad_stats_by_course],
      [harvardx-data:course_report_latest.broad_stats_by_course]
      WHERE course_id = "{course_id}"
    )
    '''.format(dataset=project_id + ":" + dataset if testing else dataset, course_id=course_id)

    table_id = 'start_date' + str(abs(hash(course_id))) #Unique id to prevent issues when running in parallel
    dataset = "curtis_northcutt" if testing else dataset
    bqutil.create_bq_table(dataset_id=dataset, table_id=table_id, sql=SQL, overwrite=True)

    data = bqutil.get_table_data(dataset_id=dataset, table_id=table_id)
    result = data['data'][0]['after_switch_to_verified'] in (['true', 'True', 'TRUE'])

    bqutil.delete_bq_table(dataset_id=dataset, table_id=table_id)
    if result:
      print "\n", '-' * 80 ,"\n", course_id, "started after December 7,l 2015 when certificates switched to verified only.\n", '-' * 80 ,"\n"
    return result

#-----------------------------------------------------------------------------
def compute_upper_bound_date_of_cert_activity(course_id, use_dataset_latest = True, testing=False, testing_dataset=None, project_id=None):
    '''Returns one week past the certification date if certificates have been rewarded.
    Otherwise, returns one week past the last due date.
    If no certificates have been rewarded and no due dates are specified, returns one year past the start date.'''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    SQL = '''
    SELECT 
      #If due dates are invalid (end_date less than a week after course starts) consider up to a year past the start date
      CASE WHEN (end_date IS NULL) OR (end_date < DATE_ADD(min_start_time, 1, "WEEK")) THEN one_year_past_start_date ELSE end_date END AS last
    FROM
    (
      SELECT DATE_ADD(MAX(end_date), 1, "WEEK") as end_date #one week past last date
      FROM
      (
        SELECT MAX(due) as end_date
        FROM [{dataset}.course_axis] 
      ),
      (
        SELECT MAX(certificate_created_date) as end_date
        FROM [{dataset}.user_info_combo] 
      )
    ) a
    CROSS JOIN
    (
      SELECT 
        MAX(min_start_time) as min_start_time, 
        MAX(one_year_past_start_date) as one_year_past_start_date
      FROM
      (
        SELECT
          MIN(due) AS min_start_time,
          DATE_ADD(MIN(due), 1, "YEAR") as one_year_past_start_date
        FROM [{dataset}.course_axis] 
      ),
      (
        SELECT
          TIMESTAMP(min_start_time) AS min_start_time,
          DATE_ADD(min_start_time, 1, "YEAR") as one_year_past_start_date
        FROM [mitx-data:course_report_latest.broad_stats_by_course],
        [harvardx-data:course_report_latest.broad_stats_by_course]
        WHERE course_id = "{course_id}"
      )
    ) b 
    '''.format(dataset=project_id + ":" + dataset if testing else dataset, course_id=course_id)
    table_id = 'temp' + str(abs(hash(course_id))) #Unique id to prevent issues when running in parallel
    dataset = "curtis_northcutt" if testing else dataset
    bqutil.create_bq_table(dataset_id=dataset, 
                           table_id=table_id, sql=SQL, overwrite=True)

    data = bqutil.get_table_data(dataset_id=dataset, table_id=table_id)
    last_date = datetime.datetime.fromtimestamp(float(data['data'][0]['last']))

    #print "--> sleeping for 15 seconds to try avoiding bigquery system error - maybe due to data transfer time"
    #sys.stdout.flush()
    #time.sleep(15)

    bqutil.delete_bq_table(dataset_id=dataset, table_id=table_id)
    return last_date

#-----------------------------------------------------------------------------

def compute_problem_check_show_answer_ip(course_id, use_dataset_latest=False, num_partitions = 1, last_date=None,
                                         overwrite=True, testing=False, testing_dataset=None, project_id=None):
    
    '''
    This table holds all the problem_check and show_answer events extracted from
    all of the tracking logs for a course. 

    The primary purpose of this table is to obtain the associated ip addresses for each event
    in the problem_check and show_answer tables.

    Unlike problem_check and show_answer - this does not append each day, but is
    ran over the entire set of all days of tracking logs in a single iteration.

    This function is expensive and should be called seldomly.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tracking_log_dataset = project_id + ":" + bqutil.course_id2dataset(course_id, use_dataset_latest=False) + '_logs'
    table = "stats_problem_check_show_answer_ip"

    if last_date is None:
      last_date = compute_upper_bound_date_of_cert_activity(course_id, use_dataset_latest, testing, testing_dataset, project_id)
    
    print "="*80 + "\nComputing problem check and show answer ip table until end date: " + str(last_date) + "\n" + "="*80 
    
    cap = str(last_date.year) + ('0'+str(last_date.month))[-2:] + ('0'+str(last_date.day))[-2:] #Formatted as YYYYMMDD

    sql = []
    for i in range(num_partitions):
      item = """
      SELECT
        time,
        username,
        context.user_id as user_id,
        course_id as course_id,
        module_id,
        CASE WHEN event_type = "problem_check" or event_type = "save_problem_check"
             THEN 'problem_check'
             ELSE 'show_answer'
             END AS event_type,
        ip
      from (
        TABLE_QUERY([{tracking_log_dataset}],
             "integer(regexp_extract(table_id, r'tracklog_([0-9]+)')) <= {cap}" #Formatted as YYYYMMDD
           )
        )

      where (event_type = "problem_check" or event_type = "save_problem_check" or event_type = "show_answer" or event_type = "showanswer")
        and event_source = "server"
        and time > TIMESTAMP("2010-10-01 01:02:03")
        and HASH(username) % {num_partitions} = {partition}
      order by time
      """.format(tracking_log_dataset=tracking_log_dataset,cap=cap,num_partitions=num_partitions, partition = i)
      sql.append(item)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    for i in range(num_partitions): 
      print "--> Running compute_problem_check_show_answer_ip_table SQL for partition %d of %d" % (i + 1, num_partitions)
      sys.stdout.flush()
      tries = 0 #Counts number of times "internal error occurs."
      while(tries < 10):
        try:
          bqutil.create_bq_table(testing_dataset if testing else dataset,
                                 dataset+'_'+table if testing else table, 
                                 sql[i], overwrite=overwrite if i==0 else 'append', allowLargeResults=True,
                                 sql_for_description="\nNUM_PARTITIONS="+str(num_partitions)+"\n\n"+sql[i])
          break #Success - no need to keep trying - no internal error occurred.
        except Exception as err:
          if 'internal error' in str(err) and tries < 10:
            tries += 1
            print "---> Internal Error occurred. Sleeping 100 seconds and retrying."
            sys.stdout.flush()
            time.sleep(100) #100 seconds
            continue
          elif (num_partitions < 20) and ('internal error' in str(err) or 'Response too large' in str(err) or 'Resources exceeded' in str(err) or u'resourcesExceeded' in str(err)):
            print err
            print "="*80,"\n==> SQL query failed! Recursively trying compute_problem_check_show_answer_ip_table again, with 50% more many partitions\n", "="*80
            return compute_problem_check_show_answer_ip(course_id, use_dataset_latest=use_dataset_latest, overwrite=overwrite, 
                                                        num_partitions=int(round(num_partitions*1.5)), last_date=last_date,
                                                        testing=testing, testing_dataset=testing_dataset, project_id=project_id)
          else:
            raise err

    nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset if testing else dataset, 
                                           table_id=dataset+'_'+table if testing else table,
                                           project_id='mitx-research')

    print "--> [%s] Processed %s records for %s" % (course_id, nfound, table)
    sys.stdout.flush()

    return num_partitions

#-----------------------------------------------------------------------------

def compute_show_ans_before(course_id, force_recompute=False, use_dataset_latest=True, force_num_partitions=None, 
                            testing=False, testing_dataset= None, project_id = None, force_online=True,
                            problem_check_show_answer_ip_table=None, cameo_master_table="mitx-research:core.cameo_master"):
  '''
  Computes the percentage of show_ans_before and avg_max_dt_seconds between all certified and uncertied users 
  cameo candidate - certified | shadow candidate - uncertified

  This function's default functionality has changed. By default force_online = True now. This means
  that all pairs are considered irrespective of certification. This changes follows the shift from
  Honor certificates to strictly ID verified certification (and much lower certification rates.)

  An ancillary table for sybils which computes the percent of candidate shadow show answers 
  that occur before the corresponding candidate cameo accounts correct answer submission.
  
  This table also computes median_max_dt_seconds which is the median time between the shadow's
  show_answer and the certified accounts' correct answer. This table also computes the normalized
  pearson correlation.

  This table chooses the FIRST show_answer and the LAST correct submission, to ensure catching
  cases, even if the user tried to figure it out without gaming first.

  Uses a user-defined function (a modified version of Levenshtein) to compute name similarity.
  '''

  udf ='''
  bigquery.defineFunction(
    'levenshtein',  // 1. Name of the function
    ['a', 'b'], // 2. Input schema
    [{name: 'a', type:'string'}, // 3. Output schema
     {name: 'b', type:'string'},
     {name: 'optimized_levenshtein_similarity', type:'float'}],
    function(r, emit) { // 4. The function
      
    function maxLevenshsteinSimilarity(a, b) {
      str1 = a.toLowerCase();
      str2 = b.toLowerCase();
      
      shorter = str1.length < str2.length ? str1 : str2
      longer = str1.length < str2.length ? str2 : str1
      
      max = -1;
      
      for (i = 0; i < shorter.length; i++) { 
        levenshtein_similarity = levenshteinSimilarity(shorter.slice(i,shorter.length).concat(shorter.slice(0,i)), longer);
        if (levenshtein_similarity > max) {
          max = levenshtein_similarity
        }
      }
      return max   
    };
    
    function levenshteinSimilarity(a, b) {
      return 1 - dynamicLevenshteinDist(a,b) / Math.max(a.length, b.length);
    };
    
    function dynamicLevenshteinDist(a, b) {
      //Fast computation of Levenshtein Distance between two strings a and b
      //Uses dynamic programming
      
      if(a.length === 0) return b.length; 
      if(b.length === 0) return a.length; 

      var matrix = [];

      // increment along the first column of each row
      var i;
      for(i = 0; i <= b.length; i++){
        matrix[i] = [i];
      }

      // increment each column in the first row
      var j;
      for(j = 0; j <= a.length; j++){
        matrix[0][j] = j;
      }

      // Fill in the rest of the matrix
      for(i = 1; i <= b.length; i++){
        for(j = 1; j <= a.length; j++){
          if(b.charAt(i-1) == a.charAt(j-1)){
            matrix[i][j] = matrix[i-1][j-1];
          } else {
            matrix[i][j] = Math.min(matrix[i-1][j-1] + 1, // substitution
                                    Math.min(matrix[i][j-1] + 1, // insertion
                                             matrix[i-1][j] + 1)); // deletion
          }
        }
      }

      return matrix[b.length][a.length];
    };
    
    emit({a: r.a, b:r.b, optimized_levenshtein_similarity: maxLevenshsteinSimilarity(r.a, r.b)});
    
  });
  '''

  dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
  table = "stats_show_ans_before"

  #Verify that dependency ip table actually exists
  if problem_check_show_answer_ip_table is not None:
    try:
      [ip_p,ip_d,ip_t] = problem_check_show_answer_ip_table.replace(':',' ').replace('.',' ').split()
      if bqutil.get_bq_table_size_rows(dataset_id=ip_d, table_id=ip_t, project_id=ip_p) is None:
        problem_check_show_answer_ip_table = None
    except:
      problem_check_show_answer_ip_table = None


  #Check that course actually contains participants, otherwise this function will recursively keep retrying when it fails.
  if bqutil.get_bq_table_size_rows(dataset_id=dataset, table_id='person_course',
                                   project_id=project_id if testing else 'mitx-research') <= 10:
            print "Error! --> Course contains no participants; exiting show_ans_before for course", course_id
            return False

  if problem_check_show_answer_ip_table is None:
      ip_table_id = 'stats_problem_check_show_answer_ip'
      if testing:
          default_table_info = bqutil.get_bq_table_info(dataset, ip_table_id, project_id=project_id)
          if default_table_info is not None and not force_recompute:
              problem_check_show_answer_ip_table = project_id + ':'+ dataset + '.' + ip_table_id
          else:
              #Default table doesn't exist, Check if testing table exists
              testing_table_info = bqutil.get_bq_table_info(testing_dataset,
                                   dataset + '_' + ip_table_id, project_id='mitx-research')
              if force_recompute or testing_table_info is None:
                  compute_problem_check_show_answer_ip(course_id, use_dataset_latest, testing=True, 
                                                       testing_dataset=testing_dataset, project_id=project_id,
                                                       overwrite=force_recompute)
              problem_check_show_answer_ip_table = 'mitx-research:' + testing_dataset + '.' + dataset + '_' + ip_table_id
      else:
          default_table_info = bqutil.get_bq_table_info(dataset,ip_table_id)
          if force_recompute or default_table_info is None:
              compute_problem_check_show_answer_ip(course_id, use_dataset_latest, overwrite=force_recompute)
          problem_check_show_answer_ip_table = dataset + '.' + ip_table_id

  if force_online is False:
    #Force show_ans_before to run online if there are no certifications
    n = ncertified(course_id, use_dataset_latest=use_dataset_latest, testing=testing, project_id=project_id)
    force_online = n <= 5 or course_started_after_switch_to_verified_only(course_id,
                                                                          use_dataset_latest=use_dataset_latest, 
                                                                          testing=testing, 
                                                                          project_id=project_id)
    print "\n", '-' * 80 ,"\n", course_id, "has", str(n), "certified users. Online status for show_ans_before:", force_online, "\n", '-' * 80 ,"\n"

  #-------------------- partition by nshow_ans_distinct

  def make_sad_partition(pmin, pmax):

      partition = '''nshow_ans_distinct >  # Shadow must click answer on between {pmin} and {pmax} of problems
                    (
                      SELECT  
                      count(distinct module_id) * {pmin}
                      FROM [{dataset}.show_answer]
                    )
      '''.format(dataset=project_dataset if testing else dataset, pmin=pmin, pmax=pmax)
      
      if pmax:
          partition += '''
                    and nshow_ans_distinct <=  
                    (
                      SELECT  
                      count(distinct module_id) * {pmax}
                      FROM [{dataset}.show_answer]
                    )
          '''.format(dataset=project_dataset if testing else dataset, pmin=pmin, pmax=pmax)
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
  if testing:
      num_persons = bqutil.get_bq_table_size_rows(dataset, 'person_course', project_id)
  if force_num_partitions:
      num_partitions = force_num_partitions
      print " --> Force the number of partitions to be %d" % force_num_partitions
  else:
      #Twice the paritions if running in a course that hasn't completed (force_online == True) since we consider more pairs
      if num_persons > 60000:
          num_partitions = min(int(round(num_persons / (10000 if force_online else 20000))), 5)  # because the number of certificate earners is also likely higher
      elif num_persons > 20000:
          num_partitions = min(int(round(num_persons / (10000 if force_online else 15000))), 5)
      else:
          num_partitions = 1
  print " --> number of persons in %s.person_course is %s; splitting query into %d partitions" % (dataset, num_persons, num_partitions)

  def make_username_partition(nparts, pnum):
      psql = """ABS(HASH(a.username)) %% %d = %d\n""" % (nparts, pnum)
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
            master_candidate AS master_candidate, 
            harvester_candidate AS harvester_candidate, 
            name_similarity, median_max_dt_seconds, percent_show_ans_before, 
            show_ans_before, percent_attempts_correct, 
            CASE WHEN ncameo IS NULL THEN INTEGER(0) ELSE INTEGER(ncameo) END AS ncameo, 
            CASE WHEN ncameo_both IS NULL THEN INTEGER(0) ELSE INTEGER(ncameo_both) END AS ncameo_both,
            x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
            x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
            x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
            percent_correct_using_show_answer, ncorrect, 
            sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
            sa_ca_dt_chi_sq_ordered, sa_ca_dt_chi_squared, sa_ca_dt_corr_ordered, sa_ca_dt_correlation,
            northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
            prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
            dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
            mile_dist_between_modal_ips, 
            CH_nblank_fr_submissions,
            CH_most_common_free_response, CH_mcfr_cnt, CH_mcfr_percent_of_users_free_responses,
            CH_most_common_multiple_choice_answer, CH_mcmc_cnt, CH_mcmc_percent_of_users_multiple_choice,
            dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
            dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
            dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
            sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
            sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
            sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
            ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
            ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
            ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95,
            cheating_likelihood
            #Removing some quartiles because BQ can only group by 64 fields
            #ca_dt_p65, ca_dt_p75, ca_dt_p25, ca_dt_p35, sa_dt_p65, sa_dt_p75, sa_dt_p25, sa_dt_p35
          FROM
          (
            SELECT
              *,
              #Comptute rank1 seperately beecause it depends on the alias cheating_likelihood
              RANK() OVER (PARTITION BY master_candidate ORDER BY cheating_likelihood DESC) AS rank1
            FROM
              (
              SELECT
                *,
                (1.0 + name_similarity) *
                (1.0 + percent_same_ip) *  
                (1.00001 + CASE WHEN norm_pearson_corr IS NULL THEN INTEGER(0) ELSE INTEGER(norm_pearson_corr) END) *
                (1.0 + nsame_ip_given_sab) *
                (1.0 + x10m) *
                (1.0 + percent_same_ip_given_sab) * 
                (1.0 + x01h_same_ip) *
                (1.0 + x05m_same_ip) *
                percent_show_ans_before * 
                (1.00001 + CASE WHEN sa_ca_dt_corr_ordered IS NULL THEN INTEGER(0) ELSE INTEGER(sa_ca_dt_corr_ordered) END) *
                (1.0 + CASE WHEN CH_mcfr_cnt IS NULL THEN INTEGER(0) ELSE INTEGER(CH_mcfr_cnt) END) *
                (1.0 + CASE WHEN CH_nblank_fr_submissions IS NULL THEN INTEGER(0) ELSE INTEGER(CH_nblank_fr_submissions) END) *
                (1.0 + CASE WHEN CH_mcfr_percent_of_users_free_responses IS NULL THEN 0.0 ELSE CH_mcfr_percent_of_users_free_responses END) /
                (dt_std_dev * dt_dt_p95 * median_max_dt_seconds) AS cheating_likelihood,

                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x01h DESC) AS rank2,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x01h_same_ip DESC) AS rank3,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x05m DESC) AS rank4,
                RANK() OVER (PARTITION BY master_candidate ORDER BY sa_ca_dt_corr_ordered DESC) AS rank5,
                RANK() OVER (PARTITION BY master_candidate ORDER BY name_similarity DESC) AS rank6,
                RANK() OVER (PARTITION BY master_candidate ORDER BY norm_pearson_corr DESC) AS rank7,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY percent_same_ip_given_sab DESC) AS rank8,
                RANK() OVER (PARTITION BY master_candidate ORDER BY x01d DESC) AS rank9,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_mcfr_cnt DESC) AS rank10,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_nblank_fr_submissions DESC) AS rank11,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_mcfr_percent_of_users_free_responses DESC) AS rank12,
                RANK() OVER (PARTITION BY master_candidate ORDER BY dt_std_dev ASC) AS rank13,
                RANK() OVER (PARTITION BY master_candidate ORDER BY dt_dt_p95 ASC) AS rank14,
                RANK() OVER (PARTITION BY master_candidate ORDER BY median_max_dt_seconds ASC) AS rank15,
                RANK() OVER (PARTITION BY master_candidate ORDER BY percentile90_dt_seconds ASC) AS rank16
              FROM
              (
                SELECT
                  course_id, 
                  master_candidate AS master_candidate, 
                  harvester_candidate AS harvester_candidate, 
                  name_similarity, median_max_dt_seconds, percent_show_ans_before, 
                  show_ans_before, percent_attempts_correct, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                  x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                  x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                  percent_correct_using_show_answer, ncorrect, 
                  sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
                  sa_ca_dt_chi_sq_ordered, sa_ca_dt_chi_squared, sa_ca_dt_corr_ordered, sa_ca_dt_correlation,
                  northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                  prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                  dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                  mile_dist_between_modal_ips, 
                  CH_nblank_fr_submissions,
                  CH_most_common_free_response, CH_mcfr_cnt, CH_mcfr_percent_of_users_free_responses,
                  CH_most_common_multiple_choice_answer, CH_mcmc_cnt, CH_mcmc_percent_of_users_multiple_choice,
                  dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                  dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                  dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                  sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
                  sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
                  sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
                  ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
                  ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
                  ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95
                  #Removing some quartiles because BQ can only group by 64 fields
                  #ca_dt_p65, ca_dt_p75, ca_dt_p25, ca_dt_p35, sa_dt_p65, sa_dt_p75, sa_dt_p25, sa_dt_p35,
                FROM
                (
                  SELECT
                    course_id, master_candidate, harvester_candidate, name_similarity, median_max_dt_seconds, percent_show_ans_before, 
                    show_ans_before, percent_attempts_correct, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                    x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                    x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                    percent_correct_using_show_answer, ncorrect, 
                    sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
                    sa_ca_dt_chi_sq_ordered, sa_ca_dt_chi_squared, sa_ca_dt_corr_ordered, sa_ca_dt_correlation,
                    northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                    prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                    dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                    mile_dist_between_modal_ips, 
                    dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                    dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                    dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                    sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
                    sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
                    sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
                    ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
                    ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
                    ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95,
                    #Removing some quartiles because BQ can only group by 64 fields
                    #ca_dt_p65, ca_dt_p75, ca_dt_p25, ca_dt_p35, sa_dt_p65, sa_dt_p75, sa_dt_p25, sa_dt_p35,
                  FROM
                    (
                      SELECT
                        course_id, master_candidate, harvester_candidate, median_max_dt_seconds, percent_show_ans_before, percent_attempts_correct,
                        show_ans_before, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                        x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                        x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                        ncorrect, percent_correct_using_show_answer, 
                        sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
                        sa_ca_dt_chi_sq_ordered, sa_ca_dt_chi_squared, sa_ca_dt_corr_ordered, sa_ca_dt_correlation,
                        northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                        prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                        dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                        mile_dist_between_modal_ips, 
                        dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                        dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                        dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                        sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
                        sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
                        sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
                        ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
                        ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
                        ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95
                        #Removing some quartiles because BQ can only group by 64 fields
                        #ca_dt_p65, ca_dt_p75, ca_dt_p25, ca_dt_p35, sa_dt_p65, sa_dt_p75, sa_dt_p25, sa_dt_p35,
                      FROM
                      (
                        SELECT
                        "{course_id}" as course_id,
                        master_candidate,
                        harvester_candidate,
                        median_max_dt_seconds,
                        sum(sa_before_pa) / count(*) * 100 as percent_show_ans_before,
                        sum(sa_before_pa) as show_ans_before,
                        percent_attempts_correct,
                        sum(sa_before_pa and (dt <= 15)) as x15s,
                        sum(sa_before_pa and (dt <= 30)) as x30s,
                        sum(sa_before_pa and (dt <= 60)) as x01m,
                        sum(sa_before_pa and (dt <= 60 * 5)) as x05m,
                        sum(sa_before_pa and (dt <= 60 * 10)) as x10m,
                        sum(sa_before_pa and (dt <= 60 * 30)) as x30m,
                        sum(sa_before_pa and (dt <= 60 * 60)) as x01h,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 1)) as x01d,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 3)) as x03d,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 5)) as x05d,
                        sum(sa_before_pa and (dt <= 15) and same_ip) as x15s_same_ip,
                        sum(sa_before_pa and (dt <= 30) and same_ip) as x30s_same_ip,
                        sum(sa_before_pa and (dt <= 60) and same_ip) as x01m_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 5) and same_ip) as x05m_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 10) and same_ip) as x10m_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 30) and same_ip) as x30m_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 60) and same_ip) as x01h_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 1) and same_ip) as x01d_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 3) and same_ip) as x03d_same_ip,
                        sum(sa_before_pa and (dt <= 60 * 60 * 24 * 5) and same_ip) as x05d_same_ip,
                        ncorrect, #nattempts,
                        sum(sa_before_pa) / ncorrect * 100 as percent_correct_using_show_answer,
                        sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
                        SUM(chi) AS sa_ca_dt_chi_squared,
                        CORR(show_answer_dt, correct_answer_dt) AS sa_ca_dt_correlation,
                        #1 - ABS((ABS(LN(sa_dt_p90 / ca_dt_p90)) + ABS(LN(sa_dt_p50 / ca_dt_p50))) / 2) as northcutt_ratio,
                        1 - ABS(LN(sa_dt_p90 / ca_dt_p90)) as northcutt_ratio,
                        #ABS((sa_dt_p90 - sa_dt_p50) - (ca_dt_p90 - ca_dt_p50)) AS l1_northcutt_similarity,
                        #SQRT(POW(sa_dt_p90 - ca_dt_p90, 2) + POW(sa_dt_p50 - ca_dt_p50, 2)) AS l2_northcutt_similarity,
                        sum(same_ip) as nsame_ip,
                        sum(case when sa_before_pa and same_ip then 1 else 0 end) as nsame_ip_given_sab,
                        sum(case when sa_before_pa and same_ip then 1 else 0 end) / sum(sa_before_pa) * 100 as percent_same_ip_given_sab,
                        sum(same_ip) / count(*) * 100 as percent_same_ip,
                        count(*) as prob_in_common,
                        avg(dt) as avg_max_dt_seconds,
                        corr(sa.time - min_first_check_time, ca.time - min_first_check_time)as norm_pearson_corr,
                        corr(sa.time, ca.time) as unnormalized_pearson_corr,
                        third_quartile_dt_seconds - first_quartile_dt_seconds as dt_iqr,
                        dt_std_dev,
                        percentile90_dt_seconds,
                        third_quartile_dt_seconds as percentile75_dt_seconds,
                        modal_ip,
                        CH_modal_ip,
                        mile_dist_between_modal_ips,
                        dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                        dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                        dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                        sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
                        sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
                        sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
                        ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
                        ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
                        ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95
                        #Removing some quartiles because BQ can only group by 64 fields
                        #ca_dt_p65, ca_dt_p75, ca_dt_p25, ca_dt_p35, sa_dt_p65, sa_dt_p75, sa_dt_p25, sa_dt_p35, 
                        FROM
                        (
                          SELECT *,
                            POW(correct_answer_dt - show_answer_dt, 2) / correct_answer_dt as chi,

                            MAX(median_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS median_max_dt_seconds,
                            MAX(first_quartile_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS first_quartile_dt_seconds,
                            MAX(third_quartile_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS third_quartile_dt_seconds,
                            MAX(percentile90_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS percentile90_dt_seconds,
                            STDDEV(dt) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_std_dev,

                            MAX(CASE WHEN has_dt THEN sa_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p05,
                            MAX(CASE WHEN has_dt THEN sa_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p10,
                            MAX(CASE WHEN has_dt THEN sa_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p15,
                            MAX(CASE WHEN has_dt THEN sa_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p20,
                            MAX(CASE WHEN has_dt THEN sa_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p25,
                            MAX(CASE WHEN has_dt THEN sa_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p30,
                            MAX(CASE WHEN has_dt THEN sa_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p35,
                            MAX(CASE WHEN has_dt THEN sa_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p40,
                            MAX(CASE WHEN has_dt THEN sa_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p45,
                            MAX(CASE WHEN has_dt THEN sa_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p50,
                            MAX(CASE WHEN has_dt THEN sa_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p55,
                            MAX(CASE WHEN has_dt THEN sa_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p60,
                            MAX(CASE WHEN has_dt THEN sa_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p65,
                            MAX(CASE WHEN has_dt THEN sa_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p70,
                            MAX(CASE WHEN has_dt THEN sa_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p75,
                            MAX(CASE WHEN has_dt THEN sa_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p80,
                            MAX(CASE WHEN has_dt THEN sa_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p85,
                            MAX(CASE WHEN has_dt THEN sa_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p90,
                            MAX(CASE WHEN has_dt THEN sa_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as sa_dt_p95,

                            MAX(CASE WHEN has_dt THEN ca_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p05,
                            MAX(CASE WHEN has_dt THEN ca_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p10,
                            MAX(CASE WHEN has_dt THEN ca_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p15,
                            MAX(CASE WHEN has_dt THEN ca_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p20,
                            MAX(CASE WHEN has_dt THEN ca_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p25,
                            MAX(CASE WHEN has_dt THEN ca_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p30,
                            MAX(CASE WHEN has_dt THEN ca_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p35,
                            MAX(CASE WHEN has_dt THEN ca_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p40,
                            MAX(CASE WHEN has_dt THEN ca_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p45,
                            MAX(CASE WHEN has_dt THEN ca_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p50,
                            MAX(CASE WHEN has_dt THEN ca_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p55,
                            MAX(CASE WHEN has_dt THEN ca_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p60,
                            MAX(CASE WHEN has_dt THEN ca_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p65,
                            MAX(CASE WHEN has_dt THEN ca_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p70,
                            MAX(CASE WHEN has_dt THEN ca_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p75,
                            MAX(CASE WHEN has_dt THEN ca_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p80,
                            MAX(CASE WHEN has_dt THEN ca_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p85,
                            MAX(CASE WHEN has_dt THEN ca_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p90,
                            MAX(CASE WHEN has_dt THEN ca_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ca_dt_p95,

                            MAX(CASE WHEN has_dt THEN dt_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p05,
                            MAX(CASE WHEN has_dt THEN dt_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p10,
                            MAX(CASE WHEN has_dt THEN dt_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p15,
                            MAX(CASE WHEN has_dt THEN dt_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p20,
                            MAX(CASE WHEN has_dt THEN dt_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p25,
                            MAX(CASE WHEN has_dt THEN dt_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p30,
                            MAX(CASE WHEN has_dt THEN dt_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p35,
                            MAX(CASE WHEN has_dt THEN dt_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p40,
                            MAX(CASE WHEN has_dt THEN dt_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p45,
                            MAX(CASE WHEN has_dt THEN dt_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p50,
                            MAX(CASE WHEN has_dt THEN dt_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p55,
                            MAX(CASE WHEN has_dt THEN dt_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p60,
                            MAX(CASE WHEN has_dt THEN dt_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p65,
                            MAX(CASE WHEN has_dt THEN dt_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p70,
                            MAX(CASE WHEN has_dt THEN dt_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p75,
                            MAX(CASE WHEN has_dt THEN dt_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p80,
                            MAX(CASE WHEN has_dt THEN dt_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p85,
                            MAX(CASE WHEN has_dt THEN dt_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p90,
                            MAX(CASE WHEN has_dt THEN dt_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p95

                          FROM
                          (
                            SELECT *,

                            #Find percentiles of correct answer - show answer dt time differences
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS median_with_null_rows,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS first_quartile_with_null_rows,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS third_quartile_with_null_rows,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS percentile90_with_null_rows, 

                            #Find percentiles for each user show answer dt time differences
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS sa_dt_p95_intermediate,

                            #Find percentiles for each user correct answer dt time differences
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ca_dt_p95_intermediate,

                            #Find dt of dt percentiles for each pair 
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p95_intermediate

                            FROM
                            ( 
                              SELECT
                                *,
                                (sa.time - sa_lag) / 1e6 AS show_answer_dt, 
                                (ca.time - ca_lag) / 1e6 AS correct_answer_dt, 
                                ABS(dt - dt_lag) AS dt_dt #only consider magnitude of difference
                              FROM
                              ( 
                                SELECT 
                                  *,

                                  #Compute lags ONLY for show answers that occur before correct answers
                                  LAG(sa.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY sa.time ASC) AS sa_lag,
                                  LAG(ca.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ca.time ASC) AS ca_lag,

                                  #Find inter-time of dt for each pair
                                  #LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY dt ASC) AS dt_lag
                                  LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ca.time ASC) AS dt_lag
                                FROM
                                (
                                  SELECT
                                    harvester_candidate, master_candidate,
                                    sa.time, ca.time,  
                                    sa.time < ca.time as sa_before_pa,
                                    sa.ip, ca.ip, ncorrect, nattempts,
                                    sa.ip == ca.ip as same_ip,
                                    (case when sa.time < ca.time then (ca.time - sa.time) / 1e6 end) as dt,
                                    (CASE WHEN sa.time < ca.time THEN true END) AS has_dt,
                                    USEC_TO_TIMESTAMP(min_first_check_time) as min_first_check_time,
                                    percent_attempts_correct,
                                    CH_modal_ip, modal_ip,

                                    #Haversine: Uses (latitude, longitude) to compute straight-line distance in miles
                                    7958.75 * ASIN(SQRT(POW(SIN((RADIANS(lat2) - RADIANS(lat1))/2),2) 
                                    + COS(RADIANS(lat1)) * COS(RADIANS(lat2)) 
                                    * POW(SIN((RADIANS(lon2) - RADIANS(lon1))/2),2))) AS mile_dist_between_modal_ips
                                  FROM
                                  (
                                    SELECT
                                      *
                                    FROM
                                    (
                                      SELECT
                                        *,
                                        MIN(sa.time) OVER (PARTITION BY harvester_candidate, master_candidate, sa.module_id) AS min_sa_time,
                                        MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY harvester_candidate, master_candidate, sa.module_id) AS nearest_sa_before
                                      FROM
                                      (
                                        #HARVESTER
                                        SELECT
                                          sa.a.username as harvester_candidate, a.module_id as module_id, sa.a.time as time, sa.ip as ip, 
                                          pc.ip as CH_modal_ip, pc.latitude as lat2, pc.longitude as lon2,
                                          nshow_ans_distinct
                                        FROM
                                        (
                                          SELECT 
                                            a.time,
                                            a.username,
                                            a.module_id,
                                            ip,
                                            count(distinct a.module_id) over (partition by a.username) as nshow_ans_distinct
                                          FROM [{dataset}.show_answer] a
                                          JOIN EACH [{problem_check_show_answer_ip_table}] b
                                          ON a.time = b.time AND a.username = b.username AND a.module_id = b.module_id
                                          WHERE b.event_type = 'show_answer'
                                        ) sa
                                        JOIN EACH [{dataset}.person_course] pc
                                        ON sa.a.username = pc.username
                                        WHERE {not_certified_filter}
                                        AND nshow_ans_distinct >= 10 #to reduce size
                                      )sa
                                      JOIN EACH
                                      (
                                        #MASTER
                                        SELECT 
                                          ca.a.username AS master_candidate, a.module_id as module_id, time, ca.ip as ip,
                                          pc.ip as modal_ip, pc.latitude as lat1, pc.longitude as lon1,
                                          min_first_check_time, ncorrect, nattempts,
                                          100.0 * ncorrect / nattempts AS percent_attempts_correct
                                        FROM
                                        (
                                          SELECT 
                                          a.username, a.module_id, a.time as time, ip, min_first_check_time, nattempts,
                                          COUNT(DISTINCT a.module_id) OVER (PARTITION BY a.username) as ncorrect
                                          FROM 
                                          (
                                            SELECT 
                                              a.time,
                                              a.username,
                                              a.module_id,
                                              a.success,
                                              ip,
                                              MIN(a.time) OVER (PARTITION BY a.username, a.module_id) as min_time,
                                              MIN(TIMESTAMP_TO_USEC(a.time)) OVER (PARTITION BY a.module_id) as min_first_check_time,
                                              COUNT(DISTINCT CONCAT(a.module_id, STRING(a.attempts))) OVER (PARTITION BY a.username) AS nattempts #unique
                                            FROM [{dataset}.problem_check] a
                                            JOIN EACH [{problem_check_show_answer_ip_table}] b
                                            ON a.time = b.time AND a.username = b.username AND a.course_id = b.course_id AND a.module_id = b.module_id
                                            WHERE b.event_type = 'problem_check'
                                            AND {partition} #PARTITION
                                          )
                                          WHERE a.success = 'correct' 
                                          AND a.time = min_time #Only keep first correct answer
                                          ###GROUP EACH BY a.username, a.module_id, min_first_check_time, nattempts
                                          ###ORDER BY time ASC
                                        ) ca
                                        JOIN EACH [{dataset}.person_course] pc
                                        ON ca.a.username = pc.username
                                        WHERE {certified_filter}
                                        AND ncorrect >= 10 #to reduce size
                                      ) ca
                                      ON sa.module_id = ca.module_id
                                      WHERE master_candidate != harvester_candidate
                                    )
                                    WHERE (nearest_sa_before IS NULL AND sa.time = min_sa_time) #if no positive dt, use min show answer time resulting in least negative dt 
                                    OR (nearest_sa_before = sa.time) #otherwise use show answer time resulting in smallest positive dt 
                                  )
                                )
                              )
                            )
                          )
                        )
                        group EACH by harvester_candidate, master_candidate, median_max_dt_seconds, first_quartile_dt_seconds,
                                      third_quartile_dt_seconds, dt_iqr, dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds,
                                      modal_ip, CH_modal_ip, mile_dist_between_modal_ips, ncorrect, percent_attempts_correct, northcutt_ratio,
                                      sa_dt_p50, sa_dt_p90, ca_dt_p50, ca_dt_p90, 
                                      dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                                      dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                                      dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                                      sa_dt_p05, sa_dt_p10, sa_dt_p15, sa_dt_p20, sa_dt_p30,
                                      sa_dt_p40, sa_dt_p45, sa_dt_p55, sa_dt_p60,  
                                      sa_dt_p70, sa_dt_p80, sa_dt_p85, sa_dt_p95, 
                                      ca_dt_p05, ca_dt_p10, ca_dt_p15, ca_dt_p20, ca_dt_p30,
                                      ca_dt_p40, ca_dt_p45, ca_dt_p55, ca_dt_p60,  
                                      ca_dt_p70, ca_dt_p80, ca_dt_p85, ca_dt_p95
                        HAVING show_ans_before >= 10 #to reduce size
                        AND median_max_dt_seconds <= (60 * 60 * 24 * 7) #to reduce size, less than one week
                      ) a
                      LEFT OUTER JOIN EACH  

                      #=========================================================
                      #COMPUTE sa_ca_dt_corr_ordered and sa_ca_dt_chi_sq_ordered
                      #=========================================================

                      ( 
                        SELECT 
                          CM, CH, 
                          CORR(sa_dt_ordered, ca_dt_ordered) AS sa_ca_dt_corr_ordered,
                          SUM(chi) AS sa_ca_dt_chi_sq_ordered
                        FROM
                        (
                          SELECT 
                            sao.CH as CH, sao.CM as CM, 
                            sao.show_answer_dt as sa_dt_ordered, 
                            cao.correct_answer_dt as ca_dt_ordered,
                            POW(cao.correct_answer_dt - sao.show_answer_dt, 2) / cao.correct_answer_dt as chi,
                          FROM
                          (
                            SELECT 
                              sa.CH, ca.CM, 
                              (sa.time - sa_lag) / 1e6 AS show_answer_dt, 
                              ROW_NUMBER() OVER (PARTITION BY sa.CH, ca.CM ORDER BY show_answer_dt) AS sa_dt_order
                            FROM
                            (
                              SELECT
                                *, 
                                COUNT(*) OVER (PARTITION BY CH, CM) AS X, #only positive dt rows at this point
                                #Compute lags ONLY for dt > 0
                                LAG(sa.time, 1) OVER (PARTITION BY CH, CM ORDER BY sa.time ASC) AS sa_lag
                              FROM
                              (
                                SELECT
                                  *,
                                  MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY CH, CM, sa.module_id) AS nearest_sa_before
                                FROM
                                ( #HARVESTER
                                  SELECT 
                                    time,
                                    username as CH,
                                    module_id
                                  FROM [{dataset}.show_answer] 
                                )sa
                                JOIN EACH
                                ( #MASTER
                                  SELECT 
                                    time,
                                    username AS CM,
                                    module_id
                                  FROM [{dataset}.problem_check]
                                  WHERE success = 'correct'
                                  AND {partition_without_prefix}
                                ) ca
                                ON sa.module_id = ca.module_id
                                WHERE CM != CH
                              )
                              WHERE (nearest_sa_before = sa.time) #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt 
                            )
                            WHERE X >=10 #For tractability
                          ) sao
                          JOIN EACH
                          (
                            SELECT 
                              sa.CH as CH, ca.CM as CM, 
                              (ca.time - ca_lag) / 1e6 AS correct_answer_dt,
                              ROW_NUMBER() OVER (PARTITION BY CH, CM ORDER BY correct_answer_dt) ca_dt_order
                            FROM
                            (
                              SELECT
                                *, 
                                COUNT(*) OVER (PARTITION BY CH, CM) AS X, #only positive dt rows at this point
                                #Compute lags ONLY for dt > 0
                                LAG(ca.time, 1) OVER (PARTITION BY CH, CM ORDER BY ca.time ASC) AS ca_lag
                              FROM
                              (
                                SELECT
                                  *,
                                  MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY CH, CM, sa.module_id) AS nearest_sa_before
                                FROM
                                ( #HARVESTER
                                  SELECT 
                                    time,
                                    username as CH,
                                    module_id
                                  FROM [{dataset}.show_answer] 
                                )sa
                                JOIN EACH
                                ( #MASTER
                                  SELECT 
                                    time,
                                    username AS CM,
                                    module_id
                                  FROM [{dataset}.problem_check]
                                  WHERE success = 'correct'
                                  AND {partition_without_prefix} #PARTITION
                                ) ca
                                ON sa.module_id = ca.module_id
                                WHERE CM != CH
                              )
                              WHERE (nearest_sa_before = sa.time) #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt
                            )
                            WHERE X >=10 #For tractability
                          ) cao
                          ON sao.CH = cao.CH AND sao.CM = cao.CM AND sao.sa_dt_order = cao.ca_dt_order
                        )
                        GROUP BY CH, CM
                      ) b
                      ON a.master_candidate = b.CM AND a.harvester_candidate = b.CH
                    ) a
                    LEFT OUTER JOIN EACH

                    #===============================================================================
                    #COMPUTE name_similarity using a modified levenshtein UDF by Curtis G. Northcutt
                    #===============================================================================

                    (
                      SELECT a as CM, b as CH, optimized_levenshtein_similarity as name_similarity
                      FROM levenshtein ( 
                        SELECT
                          CM as a, CH as b, COUNT(*) AS X #only positive dt rows at this point
                        FROM
                        (
                          SELECT
                            *,
                            MAX(CASE WHEN sa.time < ca.time THEN sa.time ELSE NULL END) OVER (PARTITION BY CH, CM, sa.module_id) AS nearest_sa_before
                          FROM
                          ( #HARVESTER
                            SELECT 
                              time,
                              username as CH,
                              module_id
                            FROM [{dataset}.show_answer]
                          )sa
                          JOIN EACH
                          ( #MASTER
                            SELECT 
                              time,
                              username AS CM,
                              module_id
                            FROM [{dataset}.problem_check]
                            WHERE success = 'correct'
                            AND {partition_without_prefix} #PARTITION
                          ) ca
                          ON sa.module_id = ca.module_id
                          WHERE CM != CH
                        )
                        WHERE nearest_sa_before = sa.time #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt 
                        GROUP BY a, b
                        HAVING X>=10 #For tractability
                      )
                    ) b
                    ON a.master_candidate = b.CM AND a.harvester_candidate = b.CH
                  ) a
                  LEFT OUTER JOIN EACH
                  (

                    #====================================================
                    #MOST COMMON RESPONSE ANAYLSIS by Curtis G. Northcutt
                    #====================================================

                    SELECT
                      username,
                      FIRST(nblank_fr_submissions) as CH_nblank_fr_submissions,
                      FIRST(CASE WHEN response_type = 'free_response' THEN response ELSE NULL END) AS CH_most_common_free_response,
                      FIRST(CASE WHEN response_type = 'free_response' THEN response_cnt ELSE NULL END) AS CH_mcfr_cnt,
                      FIRST(CASE WHEN response_type = 'free_response' THEN percent_mcr_free_response ELSE NULL END) AS CH_mcfr_percent_of_users_free_responses,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN response ELSE NULL END) AS CH_most_common_multiple_choice_answer,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN response_cnt ELSE NULL END) AS CH_mcmc_cnt,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN percent_mcr_multiple_choice ELSE NULL END) AS CH_mcmc_percent_of_users_multiple_choice
                    FROM
                    (
                      #When there are multiple most common responses, break tie by selecting the first.
                      SELECT
                        *, ROW_NUMBER() OVER (PARTITION BY username, response_type) AS row_num
                      FROM
                      (
                        #Compute the most common free response and multiple choice response
                        SELECT
                          username,
                          response,
                          response_type,
                          COUNT(*) AS response_cnt,
                          CASE WHEN response_type = 'multiple_choice' THEN COUNT(*) * 100.0 / nmultiple_choice ELSE NULL END AS percent_mcr_multiple_choice,
                          CASE WHEN response_type = 'free_response' THEN COUNT(*) * 100.0 / nfree_response ELSE NULL END AS percent_mcr_free_response,
                          nmultiple_choice,
                          nfree_response,
                          nblank_fr_submissions,
                          MAX(CASE WHEN response_type = 'multiple_choice' THEN INTEGER(response_cnt) ELSE -1 END) OVER (PARTITION BY username) as max_nmultiple_choice,
                          MAX(CASE WHEN response_type = 'free_response' THEN INTEGER(response_cnt) ELSE -1 END) OVER (PARTITION BY username) as max_nfree_response
                        FROM
                        (
                          #Compute number of occurrences for all responses of all users.
                          SELECT
                            username,
                            item.response AS response,
                            CASE WHEN (item.response CONTAINS 'choice_' 
                                       OR item.response CONTAINS 'dummy_default' 
                                       OR item.response CONTAINS 'option_'
                                       OR data.itype = 'multiplechoiceresponse'
                                       OR data.itype = 'choiceresponse') THEN 'multiple_choice' ELSE 'free_response' END AS response_type,
                            #COUNT(1) OVER (PARTITION BY username) as nproblems,
                            SUM(CASE WHEN response_type = 'multiple_choice' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nmultiple_choice,
                            SUM(CASE WHEN response_type = 'free_response' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nfree_response,
                            SUM(CASE WHEN response = '""' OR response = 'null' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nblank_fr_submissions
                          FROM 
                          (
                            SELECT 
                              user_id, item.response, data.itype
                            FROM [{dataset}.problem_analysis] a
                            LEFT OUTER JOIN [{dataset}.course_axis] b
                            ON a.problem_url_name = b.url_name
                          ) a
                          JOIN [{dataset}.person_course] b
                          ON a.user_id = b.user_id
                        )
                        GROUP BY username, response, response_type, nmultiple_choice, nfree_response, nblank_fr_submissions
                      )
                      WHERE (response_cnt = max_nmultiple_choice and response_type = "multiple_choice")
                      OR (response_cnt = max_nfree_response and response_type = "free_response")
                    )
                    WHERE row_num = 1
                    GROUP BY username
                  ) b
                  ON a.harvester_candidate = b.username
                ) a
                LEFT OUTER JOIN EACH
                (
                  #====================================================
                  #Add number of times master_candidated previously by CAMEO.
                  #====================================================
                  
                  SELECT
                    username, 
                    EXACT_COUNT_DISTINCT(course_id) AS ncameo
                  FROM [{cameo_master_table}] 
                  GROUP BY username
                ) b
                ON a.master_candidate = b.username
              ) a
              LEFT OUTER JOIN EACH
              (
                #====================================================
                #Add number of times CM, CH pair previously detected by CAMEO.
                #====================================================

                SELECT
                  #use new variable names to avoid namespace collusion
                  username as master, CH as harvester,
                  EXACT_COUNT_DISTINCT(course_id) AS ncameo_both
                FROM [{cameo_master_table}] 
                GROUP BY master, harvester
              ) b
              ON a.master_candidate = b.master AND a.harvester_candidate = b.harvester
            )
            #Only keep relevant harvesters
            WHERE rank1 <= 5
            OR rank2 <= 3
            OR rank3 <= 3
            OR rank4 <= 3
            OR rank5 <= 3
            OR rank6 <= 3
            OR rank7 <= 3
            OR rank8 <= 3
            OR rank9 <= 3
            OR rank10 <= 3
            OR rank11 <= 3
            OR rank12 <= 3
            OR rank13 <= 3
            OR rank14 <= 3
            OR rank15 <=3
            OR rank16 <=3 
            OR ncameo_both > 0
            ORDER BY cheating_likelihood DESC
            """.format(dataset=project_id + ':' + dataset if testing else dataset, 
                       course_id = course_id, 
                       partition=the_partition[i],
                       partition_without_prefix=the_partition[i].replace('a.', ''),
                       problem_check_show_answer_ip_table=problem_check_show_answer_ip_table,
                       not_certified_filter='nshow_ans_distinct >= 10' if force_online else 'certified = false',
                       certified_filter= 'ncorrect >= 10' if force_online else "certified = true",
                       cameo_master_table=cameo_master_table)
      sql.append(item)

  print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
  sys.stdout.flush()

 # sasbu = "show_answer"
 # try:
 #     tinfo = bqutil.get_bq_table_info(dataset, sasbu)
 #     if testing:
 #         tinfo = bqutil.get_bq_table_info(dataset, sasbu, project_id)
 #     has_attempts_correct = (tinfo is not None)
 # except Exception as err:
 #     print "Error %s getting %s.%s" % (err, dataset, sasbu)
 #     has_attempts_correct = False
 # if not has_attempts_correct:
 #     print "---> No %s table; skipping %s" % (sasbu, table)
 #     return
 #
 # newer_than = datetime.datetime(2015, 6, 2, 19, 0)
 #
 # try:
 #     table_date = bqutil.get_bq_table_last_modified_datetime(dataset, table)
 #     if testing:
 #         table_date = bqutil.get_bq_table_last_modified_datetime(dataset, table, project_id)
 # except Exception as err:
 #     if 'Not Found' in str(err):
 #         table_date = None
 #     else:
 #         raise
 # if not table_date:
 #     force_recompute = True

 # if table_date and (table_date < newer_than):
 #     print("--> Forcing query recomputation of %s.%s, table_date=%s, newer_than=%s" % (dataset, table,
 #                                                                                       table_date, newer_than))
 #     force_recompute = True
 #
 # depends_on=["%s.%s" % (dataset, sasbu), ]
 # if table_date and not force_recompute:
 #         # get the latest mod time of tables in depends_on:
 #         modtimes = [ bqutil.get_bq_table_last_modified_datetime(*(x.split('.',1))) for x in depends_on]
 #         if testing:
 #             modtimes = [ bqutil.get_bq_table_last_modified_datetime(*(x.split('.',1)), project_id = project_id) for x in depends_on]
 #         latest = max([x for x in modtimes if x is not None])
 #     
 #         if not latest:
 #             raise Exception("[make_problem_analysis] Cannot get last mod time for %s (got %s), needed by %s.%s" % (depends_on, modtimes, dataset, table))
 #
 #         if table_date < latest:
 #             force_recompute = True

  force_recompute=True #REMOVE ONCE SAB IS STABLE!!!!!!!!!
  if force_recompute:
      for i in range(num_partitions): 
        print "--> Running SQL for partition %d of %d" % (i + 1, num_partitions)
        sys.stdout.flush()
        tries = 0 #Counts number of times "internal error occurs."
        while(tries < 10):
          try:
            bqutil.create_bq_table(testing_dataset if testing else dataset, 
                                   dataset + '_' + table if testing else table, 
                                   sql[i], overwrite=True if i==0 else 'append', allowLargeResults=True, 
                                   sql_for_description="\nNUM_PARTITIONS="+str(num_partitions)+"\n\n"+sql[i], udfs=[udf])
            break #Success - no need to keep trying - no internal error occurred.
          except Exception as err:
            if 'internal error' in str(err) and tries < 10:
              tries += 1
              print "---> Internal Error occurred. Sleeping 2 minutes and retrying."
              sys.stdout.flush()
              time.sleep(120) #2 minutes
              continue
            elif (num_partitions < 300) and ('internal error' in str(err) or 'Response too large' in str(err) or 'Resources exceeded' in str(err) or u'resourcesExceeded' in str(err)):
              print "---> Resources exceeded. Sleeping 1 minute and retrying."
              sys.stdout.flush()
              time.sleep(160) #1 minute
              print err
              print "="*80,"\n==> SQL query failed! Recursively trying again, with 50% more many partitions\n", "="*80
              return compute_show_ans_before(course_id, force_recompute=force_recompute, 
                                              use_dataset_latest=use_dataset_latest, force_num_partitions=int(round(num_partitions*1.5)), 
                                              testing=testing, testing_dataset= testing_dataset, 
                                              project_id = project_id, force_online = force_online,
                                              problem_check_show_answer_ip_table=problem_check_show_answer_ip_table)
            else:
              raise err
          

      if num_partitions > 5:
        print "--> sleeping for 60 seconds to try avoiding bigquery system error - maybe due to data transfer time"
        sys.stdout.flush()
        time.sleep(60)

  nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset if testing else dataset, 
                                           table_id=dataset+'_'+table if testing else table,
                                           project_id='mitx-research')
  if testing:
      nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset, table_id=table, project_id='mitx-research')
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
             norm_pearson_corr, unnormalized_pearson_corr,nshow_answer_unique_problems, percent_correct, frac_complete, 
             dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds, avg_max_dt_seconds,
             verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
            FROM
            (  
              SELECT 
               user_id, username, shadow, ip, grp, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified,
               verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade,
               show_ans_before, unnormalized_pearson_corr,
               dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                 dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                   pc.dt_iqr as dt_iqr, pc.dt_std_dev as dt_std_dev, pc.percentile75_dt_seconds as percentile75_dt_seconds,
                   pc.percentile90_dt_seconds as percentile90_dt_seconds,
                   verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
                  FROM
                  (
                    SELECT
                     user_id, username, harvester_candidate AS shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                     percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                     dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                    LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                    ON pc.username = sa.master_candidate #join on cameo candidate
                    #Remove if few show answer before or too high time between show answer and submission
                    WHERE (sa.percent_show_ans_before >= 20
                    AND sa.median_max_dt_seconds < 5e4
                    AND sa.norm_pearson_corr > 0
                    AND (','+grp_usernames+',' CONTAINS ','+sa.harvester_candidate+','))#Only keep rows where cameo's shadow is in pc
                    OR certified = false #Keep all shadows for now
                  ) AS pc
                  LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                  ON pc.username = sa.harvester_candidate #join on shadow candidate
                  #Remove if few show answer before or too high time between show answer and submission
                  WHERE (sa.percent_show_ans_before >= 20
                  AND sa.median_max_dt_seconds < 5e4
                  AND sa.norm_pearson_corr > 0
                  AND (','+grp_usernames+',' CONTAINS ','+sa.master_candidate+','))#Only keep shadows if cameo in pc
                  OR certified = true #Keep previously found cameos
                  GROUP BY user_id, username, shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                    percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                    dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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

def compute_ans_coupling(course_id, force_recompute=False, use_dataset_latest=True, force_num_partitions=None, 
                            testing=False, testing_dataset= None, project_id = None, force_online=True,
                            problem_check_show_answer_ip_table=None, 
                            cameo_master_table="mitx-research:core.cameo_master",
                            only_graded_problems=True):
  '''
  The goal of "answer coupling analysis" is to measure how tightly coupled
    one learner's answers are to another learner. If students are working
    together to "effectively boost" their number of attempts, this would
    produce a strong signal among a set of features used in the analysis.

  For the sake of time, I haven't changed the variable names from 
    compute_show_ans_before, so read show_ans_before as "number of
    attempts by the harvester before the master on the same problem"
    and similarly for other features. 

  The only restriction which defines answer coupling analysis currently
    is that master account grade >= harvester account grade for any pair of
    answers considered. If it is smaller, then there is no reason to think
    the master benefitted from the harvester.

  For more information about what each compute feature does, see
    compute_show_ans_before

  '''

  udf ='''
  bigquery.defineFunction(
    'levenshtein',  // 1. Name of the function
    ['a', 'b'], // 2. Input schema
    [{name: 'a', type:'string'}, // 3. Output schema
     {name: 'b', type:'string'},
     {name: 'optimized_levenshtein_similarity', type:'float'}],
    function(r, emit) { // 4. The function
      
    function maxLevenshsteinSimilarity(a, b) {
      str1 = a.toLowerCase();
      str2 = b.toLowerCase();
      
      shorter = str1.length < str2.length ? str1 : str2
      longer = str1.length < str2.length ? str2 : str1
      
      max = -1;
      
      for (i = 0; i < shorter.length; i++) { 
        levenshtein_similarity = levenshteinSimilarity(shorter.slice(i,shorter.length).concat(shorter.slice(0,i)), longer);
        if (levenshtein_similarity > max) {
          max = levenshtein_similarity
        }
      }
      return max   
    };
    
    function levenshteinSimilarity(a, b) {
      return 1 - dynamicLevenshteinDist(a,b) / Math.max(a.length, b.length);
    };
    
    function dynamicLevenshteinDist(a, b) {
      //Fast computation of Levenshtein Distance between two strings a and b
      //Uses dynamic programming
      
      if(a.length === 0) return b.length; 
      if(b.length === 0) return a.length; 

      var matrix = [];

      // increment along the first column of each row
      var i;
      for(i = 0; i <= b.length; i++){
        matrix[i] = [i];
      }

      // increment each column in the first row
      var j;
      for(j = 0; j <= a.length; j++){
        matrix[0][j] = j;
      }

      // Fill in the rest of the matrix
      for(i = 1; i <= b.length; i++){
        for(j = 1; j <= a.length; j++){
          if(b.charAt(i-1) == a.charAt(j-1)){
            matrix[i][j] = matrix[i-1][j-1];
          } else {
            matrix[i][j] = Math.min(matrix[i-1][j-1] + 1, // substitution
                                    Math.min(matrix[i][j-1] + 1, // insertion
                                             matrix[i-1][j] + 1)); // deletion
          }
        }
      }

      return matrix[b.length][a.length];
    };
    
    emit({a: r.a, b:r.b, optimized_levenshtein_similarity: maxLevenshsteinSimilarity(r.a, r.b)});
    
  });
  '''

  dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
  table = "stats_ans_coupling"

  #Verify that dependency ip table actually exists
  if problem_check_show_answer_ip_table is not None:
    try:
      [ip_p,ip_d,ip_t] = problem_check_show_answer_ip_table.replace(':',' ').replace('.',' ').split()
      if bqutil.get_bq_table_size_rows(dataset_id=ip_d, table_id=ip_t, project_id=ip_p) is None:
        problem_check_show_answer_ip_table = None
    except:
      problem_check_show_answer_ip_table = None


  #Check that course actually contains participants, otherwise this function will recursively keep retrying when it fails.
  if bqutil.get_bq_table_size_rows(dataset_id=dataset, table_id='person_course',
                                   project_id=project_id if testing else 'mitx-research') <= 10:
            print "Error! --> Course contains no participants; exiting stats_ans_coupling for course", course_id
            return False

  if problem_check_show_answer_ip_table is None:
      ip_table_id = 'stats_problem_check_show_answer_ip'
      if testing:
          default_table_info = bqutil.get_bq_table_info(dataset, ip_table_id, project_id=project_id)
          if default_table_info is not None and not force_recompute:
              problem_check_show_answer_ip_table = project_id + ':'+ dataset + '.' + ip_table_id
          else:
              #Default table doesn't exist, Check if testing table exists
              testing_table_info = bqutil.get_bq_table_info(testing_dataset,
                                   dataset + '_' + ip_table_id, project_id='mitx-research')
              if force_recompute or testing_table_info is None:
                  compute_problem_check_show_answer_ip(course_id, use_dataset_latest, testing=True, 
                                                       testing_dataset=testing_dataset, project_id=project_id,
                                                       overwrite=force_recompute)
              problem_check_show_answer_ip_table = 'mitx-research:' + testing_dataset + '.' + dataset + '_' + ip_table_id
      else:
          default_table_info = bqutil.get_bq_table_info(dataset,ip_table_id)
          if force_recompute or default_table_info is None:
              compute_problem_check_show_answer_ip(course_id, use_dataset_latest, overwrite=force_recompute)
          problem_check_show_answer_ip_table = dataset + '.' + ip_table_id

  if force_online is False:
    #Force stats_ans_coupling to run online if there are no certifications
    n = ncertified(course_id, use_dataset_latest=use_dataset_latest, testing=testing, project_id=project_id)
    force_online = n <= 5 or course_started_after_switch_to_verified_only(course_id,
                                                                          use_dataset_latest=use_dataset_latest, 
                                                                          testing=testing, 
                                                                          project_id=project_id)
    print "\n", '-' * 80 ,"\n", course_id, "has", str(n), "certified users. Online status for show_ans_before:", force_online, "\n", '-' * 80 ,"\n"

  #-------------------- partition by nshow_ans_distinct

  def make_sad_partition(pmin, pmax):

      partition = '''nshow_ans_distinct >  # Shadow must click answer on between {pmin} and {pmax} of problems
                    (
                      SELECT  
                      count(distinct module_id) * {pmin}
                      FROM [{dataset}.show_answer]
                    )
      '''.format(dataset=project_dataset if testing else dataset, pmin=pmin, pmax=pmax)
      
      if pmax:
          partition += '''
                    and nshow_ans_distinct <=  
                    (
                      SELECT  
                      count(distinct module_id) * {pmax}
                      FROM [{dataset}.show_answer]
                    )
          '''.format(dataset=project_dataset if testing else dataset, pmin=pmin, pmax=pmax)
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
  if testing:
      num_persons = bqutil.get_bq_table_size_rows(dataset, 'person_course', project_id)
  if force_num_partitions:
      num_partitions = force_num_partitions
      print " --> Force the number of partitions to be %d" % force_num_partitions
  else:
      #Twice the paritions if running in a course that hasn't completed (force_online == True) since we consider more pairs
      if num_persons > 60000:
          num_partitions = min(int(round(num_persons / (10000 if force_online else 20000))), 5)  # because the number of certificate earners is also likely higher
      elif num_persons > 20000:
          num_partitions = min(int(round(num_persons / (10000 if force_online else 15000))), 5)
      else:
          num_partitions = 1
  print " --> number of persons in %s.person_course is %s; splitting query into %d partitions" % (dataset, num_persons, num_partitions)

  def make_username_partition(nparts, pnum):
      psql = """ABS(HASH(a.username)) %% %d = %d\n""" % (nparts, pnum)
      return psql

  the_partition = []
  for k in range(num_partitions):
      the_partition.append(make_username_partition(num_partitions, k))

  #-------------------- build sql, one for each partition

  # Remove non-graded problems from problem_check if only_graded_problems.
  problem_check_table = \
  '''
                                            (
                                              # Remove non-graded problems from problem_check
                                              SELECT 
                                                a.course_id as course_id,
                                                a.time as time,
                                                a.username as username,
                                                a.module_id as module_id,
                                                a.grade as grade,
                                                a.attempts as attempts
                                              FROM [{dataset}.problem_check] a
                                              JOIN [{dataset}.course_axis] b
                                              ON a.module_id = b.module_id
                                              WHERE b.graded = 'true' 
                                            )'''.format(dataset=project_id + ':' + dataset if testing else dataset) \
    if only_graded_problems else "[{dataset}.problem_check]".format(dataset=project_id + ':' + dataset if testing else dataset)

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
            master_candidate AS master_candidate, 
            harvester_candidate AS harvester_candidate, 
            name_similarity, median_max_dt_seconds, percent_show_ans_before, 
            show_ans_before, percent_attempts_correct, 
            CASE WHEN ncameo IS NULL THEN INTEGER(0) ELSE INTEGER(ncameo) END AS ncameo, 
            CASE WHEN ncameo_both IS NULL THEN INTEGER(0) ELSE INTEGER(ncameo_both) END AS ncameo_both,
            x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
            x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
            x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
            percent_correct_using_show_answer, ncorrect, 
            ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
            ha_ma_dt_chi_sq_ordered, ha_ma_dt_chi_squared, ha_ma_dt_corr_ordered, ha_ma_dt_correlation,
            northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
            prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
            dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
            mile_dist_between_modal_ips, 
            CH_nblank_fr_submissions,
            CH_most_common_free_response, CH_mcfr_cnt, CH_mcfr_percent_of_users_free_responses,
            CH_most_common_multiple_choice_answer, CH_mcmc_cnt, CH_mcmc_percent_of_users_multiple_choice,
            dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
            dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
            dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
            ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
            ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
            ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
            ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
            ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
            ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95,
            cheating_likelihood
            #Removing some quartiles because BQ can only group by 64 fields
            #ma_dt_p65, ma_dt_p75, ma_dt_p25, ma_dt_p35, ha_dt_p65, ha_dt_p75, ha_dt_p25, ha_dt_p35
          FROM
          (
            SELECT
              *,
              #Comptute rank1 seperately beecause it depends on the alias cheating_likelihood
              RANK() OVER (PARTITION BY master_candidate ORDER BY cheating_likelihood DESC) AS rank1
            FROM
              (
              SELECT
                *,
                (1.0 + name_similarity) *
                (1.0 + percent_same_ip) *  
                (1.00001 + CASE WHEN norm_pearson_corr IS NULL THEN INTEGER(0) ELSE INTEGER(norm_pearson_corr) END) *
                (1.0 + nsame_ip_given_sab) *
                (1.0 + x10m) *
                (1.0 + percent_same_ip_given_sab) * 
                (1.0 + x01h_same_ip) *
                (1.0 + x05m_same_ip) *
                percent_show_ans_before * 
                (1.00001 + CASE WHEN ha_ma_dt_corr_ordered IS NULL THEN INTEGER(0) ELSE INTEGER(ha_ma_dt_corr_ordered) END) *
                (1.0 + CASE WHEN CH_mcfr_cnt IS NULL THEN INTEGER(0) ELSE INTEGER(CH_mcfr_cnt) END) *
                (1.0 + CASE WHEN CH_nblank_fr_submissions IS NULL THEN INTEGER(0) ELSE INTEGER(CH_nblank_fr_submissions) END) *
                (1.0 + CASE WHEN CH_mcfr_percent_of_users_free_responses IS NULL THEN 0.0 ELSE CH_mcfr_percent_of_users_free_responses END) /
                (dt_std_dev * dt_dt_p95 * median_max_dt_seconds) AS cheating_likelihood,

                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x01h DESC) AS rank2,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x01h_same_ip DESC) AS rank3,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY x05m DESC) AS rank4,
                RANK() OVER (PARTITION BY master_candidate ORDER BY ha_ma_dt_corr_ordered DESC) AS rank5,
                RANK() OVER (PARTITION BY master_candidate ORDER BY name_similarity DESC) AS rank6,
                RANK() OVER (PARTITION BY master_candidate ORDER BY norm_pearson_corr DESC) AS rank7,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY percent_same_ip_given_sab DESC) AS rank8,
                RANK() OVER (PARTITION BY master_candidate ORDER BY x01d DESC) AS rank9,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_mcfr_cnt DESC) AS rank10,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_nblank_fr_submissions DESC) AS rank11,
                ROW_NUMBER() OVER (PARTITION BY master_candidate ORDER BY CH_mcfr_percent_of_users_free_responses DESC) AS rank12,
                RANK() OVER (PARTITION BY master_candidate ORDER BY dt_std_dev ASC) AS rank13,
                RANK() OVER (PARTITION BY master_candidate ORDER BY dt_dt_p95 ASC) AS rank14,
                RANK() OVER (PARTITION BY master_candidate ORDER BY median_max_dt_seconds ASC) AS rank15,
                RANK() OVER (PARTITION BY master_candidate ORDER BY percentile90_dt_seconds ASC) AS rank16
              FROM
              (
                SELECT
                  course_id, 
                  master_candidate AS master_candidate, 
                  harvester_candidate AS harvester_candidate, 
                  name_similarity, median_max_dt_seconds, percent_show_ans_before, 
                  show_ans_before, percent_attempts_correct, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                  x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                  x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                  percent_correct_using_show_answer, ncorrect, 
                  ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
                  ha_ma_dt_chi_sq_ordered, ha_ma_dt_chi_squared, ha_ma_dt_corr_ordered, ha_ma_dt_correlation,
                  northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                  prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                  dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                  mile_dist_between_modal_ips, 
                  CH_nblank_fr_submissions,
                  CH_most_common_free_response, CH_mcfr_cnt, CH_mcfr_percent_of_users_free_responses,
                  CH_most_common_multiple_choice_answer, CH_mcmc_cnt, CH_mcmc_percent_of_users_multiple_choice,
                  dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                  dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                  dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                  ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
                  ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
                  ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
                  ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
                  ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
                  ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95
                  #Removing some quartiles because BQ can only group by 64 fields
                  #ma_dt_p65, ma_dt_p75, ma_dt_p25, ma_dt_p35, ha_dt_p65, ha_dt_p75, ha_dt_p25, ha_dt_p35,
                FROM
                (
                  SELECT
                    course_id, master_candidate, harvester_candidate, name_similarity, median_max_dt_seconds, percent_show_ans_before, 
                    show_ans_before, percent_attempts_correct, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                    x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                    x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                    percent_correct_using_show_answer, ncorrect, 
                    ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
                    ha_ma_dt_chi_sq_ordered, ha_ma_dt_chi_squared, ha_ma_dt_corr_ordered, ha_ma_dt_correlation,
                    northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                    prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                    dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                    mile_dist_between_modal_ips, 
                    dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                    dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                    dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                    ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
                    ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
                    ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
                    ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
                    ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
                    ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95,
                    #Removing some quartiles because BQ can only group by 64 fields
                    #ma_dt_p65, ma_dt_p75, ma_dt_p25, ma_dt_p35, ha_dt_p65, ha_dt_p75, ha_dt_p25, ha_dt_p35,
                  FROM
                    (
                      SELECT
                        course_id, master_candidate, harvester_candidate, median_max_dt_seconds, percent_show_ans_before, percent_attempts_correct,
                        show_ans_before, x15s, x30s, x01m, x05m, x10m, x30m, x01h, x01d, x03d, x05d,
                        x15s_same_ip, x30s_same_ip, x01m_same_ip, x05m_same_ip, x10m_same_ip, x30m_same_ip, 
                        x01h_same_ip, x01d_same_ip, x03d_same_ip, x05d_same_ip,
                        ncorrect, percent_correct_using_show_answer, 
                        ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
                        ha_ma_dt_chi_sq_ordered, ha_ma_dt_chi_squared, ha_ma_dt_corr_ordered, ha_ma_dt_correlation,
                        northcutt_ratio, nsame_ip, nsame_ip_given_sab, percent_same_ip_given_sab, percent_same_ip,
                        prob_in_common, avg_max_dt_seconds, norm_pearson_corr, unnormalized_pearson_corr, dt_iqr,
                        dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds, modal_ip, CH_modal_ip,
                        mile_dist_between_modal_ips, 
                        dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                        dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                        dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                        ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
                        ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
                        ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
                        ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
                        ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
                        ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95
                        #Removing some quartiles because BQ can only group by 64 fields
                        #ma_dt_p65, ma_dt_p75, ma_dt_p25, ma_dt_p35, ha_dt_p65, ha_dt_p75, ha_dt_p25, ha_dt_p35,
                      FROM
                      (
                        SELECT
                        "{course_id}" as course_id,
                        master_candidate,
                        harvester_candidate,
                        median_max_dt_seconds,
                        sum(ha_before_pa) / count(*) * 100 as percent_show_ans_before,
                        sum(ha_before_pa) as show_ans_before,
                        percent_attempts_correct,
                        sum(ha_before_pa and (dt <= 15)) as x15s,
                        sum(ha_before_pa and (dt <= 30)) as x30s,
                        sum(ha_before_pa and (dt <= 60)) as x01m,
                        sum(ha_before_pa and (dt <= 60 * 5)) as x05m,
                        sum(ha_before_pa and (dt <= 60 * 10)) as x10m,
                        sum(ha_before_pa and (dt <= 60 * 30)) as x30m,
                        sum(ha_before_pa and (dt <= 60 * 60)) as x01h,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 1)) as x01d,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 3)) as x03d,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 5)) as x05d,
                        sum(ha_before_pa and (dt <= 15) and same_ip) as x15s_same_ip,
                        sum(ha_before_pa and (dt <= 30) and same_ip) as x30s_same_ip,
                        sum(ha_before_pa and (dt <= 60) and same_ip) as x01m_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 5) and same_ip) as x05m_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 10) and same_ip) as x10m_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 30) and same_ip) as x30m_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 60) and same_ip) as x01h_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 1) and same_ip) as x01d_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 3) and same_ip) as x03d_same_ip,
                        sum(ha_before_pa and (dt <= 60 * 60 * 24 * 5) and same_ip) as x05d_same_ip,
                        ncorrect, #nattempts,
                        sum(ha_before_pa) / ncorrect * 100 as percent_correct_using_show_answer,
                        ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
                        SUM(chi) AS ha_ma_dt_chi_squared,
                        CORR(show_answer_dt, correct_answer_dt) AS ha_ma_dt_correlation,
                        #1 - ABS((ABS(LN(ha_dt_p90 / ma_dt_p90)) + ABS(LN(ha_dt_p50 / ma_dt_p50))) / 2) as northcutt_ratio,
                        1 - ABS(LN(ha_dt_p90 / ma_dt_p90)) as northcutt_ratio,
                        #ABS((ha_dt_p90 - ha_dt_p50) - (ma_dt_p90 - ma_dt_p50)) AS l1_northcutt_similarity,
                        #SQRT(POW(ha_dt_p90 - ma_dt_p90, 2) + POW(ha_dt_p50 - ma_dt_p50, 2)) AS l2_northcutt_similarity,
                        sum(same_ip) as nsame_ip,
                        sum(case when ha_before_pa and same_ip then 1 else 0 end) as nsame_ip_given_sab,
                        sum(case when ha_before_pa and same_ip then 1 else 0 end) / sum(ha_before_pa) * 100 as percent_same_ip_given_sab,
                        sum(same_ip) / count(*) * 100 as percent_same_ip,
                        count(*) as prob_in_common,
                        avg(dt) as avg_max_dt_seconds,
                        corr(ha.time - min_first_check_time, ma.time - min_first_check_time)as norm_pearson_corr,
                        corr(ha.time, ma.time) as unnormalized_pearson_corr,
                        third_quartile_dt_seconds - first_quartile_dt_seconds as dt_iqr,
                        dt_std_dev,
                        percentile90_dt_seconds,
                        third_quartile_dt_seconds as percentile75_dt_seconds,
                        modal_ip,
                        CH_modal_ip,
                        mile_dist_between_modal_ips,
                        dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                        dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                        dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                        ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
                        ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
                        ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
                        ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
                        ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
                        ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95
                        #Removing some quartiles because BQ can only group by 64 fields
                        #ma_dt_p65, ma_dt_p75, ma_dt_p25, ma_dt_p35, ha_dt_p65, ha_dt_p75, ha_dt_p25, ha_dt_p35, 
                        FROM
                        (
                          SELECT *,
                            POW(correct_answer_dt - show_answer_dt, 2) / correct_answer_dt as chi,

                            MAX(median_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS median_max_dt_seconds,
                            MAX(first_quartile_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS first_quartile_dt_seconds,
                            MAX(third_quartile_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS third_quartile_dt_seconds,
                            MAX(percentile90_with_null_rows) OVER (PARTITION BY harvester_candidate, master_candidate) AS percentile90_dt_seconds,
                            STDDEV(dt) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_std_dev,

                            MAX(CASE WHEN has_dt THEN ha_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p05,
                            MAX(CASE WHEN has_dt THEN ha_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p10,
                            MAX(CASE WHEN has_dt THEN ha_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p15,
                            MAX(CASE WHEN has_dt THEN ha_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p20,
                            MAX(CASE WHEN has_dt THEN ha_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p25,
                            MAX(CASE WHEN has_dt THEN ha_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p30,
                            MAX(CASE WHEN has_dt THEN ha_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p35,
                            MAX(CASE WHEN has_dt THEN ha_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p40,
                            MAX(CASE WHEN has_dt THEN ha_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p45,
                            MAX(CASE WHEN has_dt THEN ha_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p50,
                            MAX(CASE WHEN has_dt THEN ha_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p55,
                            MAX(CASE WHEN has_dt THEN ha_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p60,
                            MAX(CASE WHEN has_dt THEN ha_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p65,
                            MAX(CASE WHEN has_dt THEN ha_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p70,
                            MAX(CASE WHEN has_dt THEN ha_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p75,
                            MAX(CASE WHEN has_dt THEN ha_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p80,
                            MAX(CASE WHEN has_dt THEN ha_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p85,
                            MAX(CASE WHEN has_dt THEN ha_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p90,
                            MAX(CASE WHEN has_dt THEN ha_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ha_dt_p95,

                            MAX(CASE WHEN has_dt THEN ma_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p05,
                            MAX(CASE WHEN has_dt THEN ma_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p10,
                            MAX(CASE WHEN has_dt THEN ma_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p15,
                            MAX(CASE WHEN has_dt THEN ma_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p20,
                            MAX(CASE WHEN has_dt THEN ma_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p25,
                            MAX(CASE WHEN has_dt THEN ma_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p30,
                            MAX(CASE WHEN has_dt THEN ma_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p35,
                            MAX(CASE WHEN has_dt THEN ma_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p40,
                            MAX(CASE WHEN has_dt THEN ma_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p45,
                            MAX(CASE WHEN has_dt THEN ma_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p50,
                            MAX(CASE WHEN has_dt THEN ma_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p55,
                            MAX(CASE WHEN has_dt THEN ma_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p60,
                            MAX(CASE WHEN has_dt THEN ma_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p65,
                            MAX(CASE WHEN has_dt THEN ma_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p70,
                            MAX(CASE WHEN has_dt THEN ma_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p75,
                            MAX(CASE WHEN has_dt THEN ma_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p80,
                            MAX(CASE WHEN has_dt THEN ma_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p85,
                            MAX(CASE WHEN has_dt THEN ma_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p90,
                            MAX(CASE WHEN has_dt THEN ma_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as ma_dt_p95,

                            MAX(CASE WHEN has_dt THEN dt_dt_p05_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p05,
                            MAX(CASE WHEN has_dt THEN dt_dt_p10_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p10,
                            MAX(CASE WHEN has_dt THEN dt_dt_p15_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p15,
                            MAX(CASE WHEN has_dt THEN dt_dt_p20_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p20,
                            MAX(CASE WHEN has_dt THEN dt_dt_p25_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p25,
                            MAX(CASE WHEN has_dt THEN dt_dt_p30_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p30,
                            MAX(CASE WHEN has_dt THEN dt_dt_p35_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p35,
                            MAX(CASE WHEN has_dt THEN dt_dt_p40_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p40,
                            MAX(CASE WHEN has_dt THEN dt_dt_p45_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p45,
                            MAX(CASE WHEN has_dt THEN dt_dt_p50_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p50,
                            MAX(CASE WHEN has_dt THEN dt_dt_p55_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p55,
                            MAX(CASE WHEN has_dt THEN dt_dt_p60_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p60,
                            MAX(CASE WHEN has_dt THEN dt_dt_p65_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p65,
                            MAX(CASE WHEN has_dt THEN dt_dt_p70_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p70,
                            MAX(CASE WHEN has_dt THEN dt_dt_p75_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p75,
                            MAX(CASE WHEN has_dt THEN dt_dt_p80_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p80,
                            MAX(CASE WHEN has_dt THEN dt_dt_p85_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p85,
                            MAX(CASE WHEN has_dt THEN dt_dt_p90_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p90,
                            MAX(CASE WHEN has_dt THEN dt_dt_p95_intermediate ELSE -1e8 END) OVER (PARTITION BY harvester_candidate, master_candidate) as dt_dt_p95

                          FROM
                          (
                            SELECT *,

                            #Find percentiles of correct answer - show answer dt time differences
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS median_with_null_rows,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS first_quartile_with_null_rows,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS third_quartile_with_null_rows,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt) AS percentile90_with_null_rows, 

                            #Find percentiles for each user show answer dt time differences
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY show_answer_dt) AS ha_dt_p95_intermediate,

                            #Find percentiles for each user correct answer dt time differences
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY correct_answer_dt) AS ma_dt_p95_intermediate,

                            #Find dt of dt percentiles for each pair 
                            PERCENTILE_CONT(.05) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p05_intermediate,
                            PERCENTILE_CONT(.10) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p10_intermediate,
                            PERCENTILE_CONT(.15) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p15_intermediate,
                            PERCENTILE_CONT(.20) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p20_intermediate,
                            PERCENTILE_CONT(.25) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p25_intermediate,
                            PERCENTILE_CONT(.30) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p30_intermediate,
                            PERCENTILE_CONT(.35) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p35_intermediate,
                            PERCENTILE_CONT(.40) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p40_intermediate,
                            PERCENTILE_CONT(.45) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p45_intermediate,
                            PERCENTILE_CONT(.50) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p50_intermediate,
                            PERCENTILE_CONT(.55) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p55_intermediate,
                            PERCENTILE_CONT(.60) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p60_intermediate,
                            PERCENTILE_CONT(.65) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p65_intermediate,
                            PERCENTILE_CONT(.70) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p70_intermediate,
                            PERCENTILE_CONT(.75) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p75_intermediate,
                            PERCENTILE_CONT(.80) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p80_intermediate,
                            PERCENTILE_CONT(.85) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p85_intermediate,
                            PERCENTILE_CONT(.90) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p90_intermediate,
                            PERCENTILE_CONT(.95) OVER (PARTITION BY has_dt, harvester_candidate, master_candidate ORDER BY dt_dt) AS dt_dt_p95_intermediate

                            FROM
                            ( 
                              SELECT
                                *,
                                (ha.time - ha_lag) / 1e6 AS show_answer_dt, 
                                (ma.time - ma_lag) / 1e6 AS correct_answer_dt, 
                                ABS(dt - dt_lag) AS dt_dt #only consider magnitude of difference
                              FROM
                              ( 
                                SELECT 
                                  *,

                                  #Compute lags ONLY for show answers that occur before correct answers
                                  LAG(ha.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ha.time ASC) AS ha_lag,
                                  LAG(ma.time, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ma.time ASC) AS ma_lag,

                                  #Find inter-time of dt for each pair
                                  #LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY dt ASC) AS dt_lag
                                  LAG(dt, 1) OVER (PARTITION BY harvester_candidate, master_candidate, has_dt ORDER BY ma.time ASC) AS dt_lag
                                FROM
                                (
                                  SELECT
                                    harvester_candidate, master_candidate,
                                    ha.time, ma.time,  
                                    ha.grade, ma.grade,
                                    ha.time < ma.time as ha_before_pa,
                                    ha.ip, ma.ip, ncorrect, nattempts,
                                    ha.ip == ma.ip as same_ip,
                                    (case when ha.time < ma.time then (ma.time - ha.time) / 1e6 end) as dt,
                                    (CASE WHEN ha.time < ma.time THEN true END) AS has_dt,
                                    USEC_TO_TIMESTAMP(min_first_check_time) as min_first_check_time,
                                    percent_attempts_correct,
                                    CH_modal_ip, modal_ip,

                                    #Haversine: Uses (latitude, longitude) to compute straight-line distance in miles
                                    7958.75 * ASIN(SQRT(POW(SIN((RADIANS(lat2) - RADIANS(lat1))/2),2) 
                                    + COS(RADIANS(lat1)) * COS(RADIANS(lat2)) 
                                    * POW(SIN((RADIANS(lon2) - RADIANS(lon1))/2),2))) AS mile_dist_between_modal_ips
                                  FROM
                                  (
                                    SELECT
                                      *
                                    FROM
                                    (
                                      SELECT
                                        *,
                                        MIN(ha.time) OVER (PARTITION BY harvester_candidate, master_candidate, ha.module_id) AS min_ha_time,
                                        MAX(CASE WHEN ha.time < ma.time THEN ha.time ELSE NULL END) OVER (PARTITION BY harvester_candidate, master_candidate, ha.module_id) AS nearest_ha_before
                                      FROM
                                      (
                                        #HARVESTER
                                        SELECT
                                          ha.a.username as harvester_candidate, ha.a.module_id as module_id, ha.a.time as time, ha.ip as ip, ha.a.grade as grade,
                                          pc.ip as CH_modal_ip, pc.latitude as lat2, pc.longitude as lon2,
                                          nshow_ans_distinct
                                        FROM
                                        (
                                          SELECT 
                                            a.time,
                                            a.username,
                                            a.module_id,
                                            a.grade,
                                            ip,
                                            count(distinct a.module_id) over (partition by a.username) as nshow_ans_distinct
                                          FROM {problem_check_table} a
                                          JOIN EACH [{problem_check_show_answer_ip_table}] b
                                          ON a.time = b.time AND a.username = b.username AND a.module_id = b.module_id
                                          WHERE b.event_type = 'problem_check'
                                        ) ha
                                        JOIN EACH [{dataset}.person_course] pc
                                        ON ha.a.username = pc.username
                                        WHERE {not_certified_filter}
                                        ######AND nshow_ans_distinct >= 10 #to reduce size
                                      ) ha
                                      JOIN EACH
                                      (
                                        #MASTER
                                        SELECT 
                                          ma.a.username AS master_candidate, a.module_id as module_id, time, ma.ip as ip, ma.grade as grade,
                                          pc.ip as modal_ip, pc.latitude as lat1, pc.longitude as lon1,
                                          min_first_check_time, ncorrect, nattempts,
                                          100.0 * ncorrect / nattempts AS percent_attempts_correct
                                        FROM
                                        (
                                          SELECT 
                                          a.username, a.module_id, a.time AS time, ip, grade, min_first_check_time, nattempts,
                                          COUNT(DISTINCT a.module_id) OVER (PARTITION BY a.username) as ncorrect
                                          FROM 
                                          (
                                            SELECT 
                                              a.time,
                                              a.username,
                                              a.module_id,
                                              #a.success,
                                              a.grade,
                                              ip,
                                              MAX(a.time) OVER (PARTITION BY a.username, a.module_id) as last_time, 
                                              MIN(TIMESTAMP_TO_USEC(a.time)) OVER (PARTITION BY a.module_id) as min_first_check_time,
                                              COUNT(DISTINCT CONCAT(a.module_id, STRING(a.attempts))) OVER (PARTITION BY a.username) AS nattempts #unique
                                            FROM {problem_check_table} a
                                            JOIN EACH [{problem_check_show_answer_ip_table}] b
                                            ON a.time = b.time AND a.username = b.username AND a.course_id = b.course_id AND a.module_id = b.module_id
                                            WHERE b.event_type = 'problem_check'
                                            AND {partition} #PARTITION
                                          )
                                          #####WHERE a.success = 'correct' 
                                          WHERE a.time = last_time
                                        ) ma
                                        JOIN EACH [{dataset}.person_course] pc
                                        ON ma.a.username = pc.username
                                        WHERE {certified_filter}
                                        #####AND ncorrect >= 10 #to reduce size
                                      ) ma
                                      ON ha.module_id = ma.module_id
                                      WHERE master_candidate != harvester_candidate
                                    )
                                    WHERE (nearest_ha_before IS NULL AND ha.time = min_ha_time) #if no positive dt, use min show answer time resulting in least negative dt 
                                    OR (nearest_ha_before = ha.time) #otherwise use show answer time resulting in smallest positive dt 
                                  )
                                  # Masters's grade should be no smaller than harvester's grade, or no benefit occurred
                                  WHERE ma.grade >= ha.grade
                                )
                              )
                            )
                          )
                        )
                        group EACH by harvester_candidate, master_candidate, median_max_dt_seconds, first_quartile_dt_seconds,
                                      third_quartile_dt_seconds, dt_iqr, dt_std_dev, percentile90_dt_seconds, percentile75_dt_seconds,
                                      modal_ip, CH_modal_ip, mile_dist_between_modal_ips, ncorrect, percent_attempts_correct, northcutt_ratio,
                                      ha_dt_p50, ha_dt_p90, ma_dt_p50, ma_dt_p90, 
                                      dt_dt_p05, dt_dt_p10, dt_dt_p15, dt_dt_p20, dt_dt_p25, dt_dt_p30, dt_dt_p35,
                                      dt_dt_p40, dt_dt_p45, dt_dt_p50, dt_dt_p55, dt_dt_p60, dt_dt_p65, dt_dt_p70, 
                                      dt_dt_p75, dt_dt_p80, dt_dt_p85, dt_dt_p90, dt_dt_p95,
                                      ha_dt_p05, ha_dt_p10, ha_dt_p15, ha_dt_p20, ha_dt_p30,
                                      ha_dt_p40, ha_dt_p45, ha_dt_p55, ha_dt_p60,  
                                      ha_dt_p70, ha_dt_p80, ha_dt_p85, ha_dt_p95, 
                                      ma_dt_p05, ma_dt_p10, ma_dt_p15, ma_dt_p20, ma_dt_p30,
                                      ma_dt_p40, ma_dt_p45, ma_dt_p55, ma_dt_p60,  
                                      ma_dt_p70, ma_dt_p80, ma_dt_p85, ma_dt_p95
                        ####HAVING show_ans_before >= 10 #to reduce size
                        ####AND median_max_dt_seconds <= (60 * 60 * 24 * 7) #to reduce size, less than one week
                        HAVING median_max_dt_seconds <= (60 * 60 * 24 * 7) #to reduce size, less than one week
                      ) a
                      LEFT OUTER JOIN EACH  

                      #=========================================================
                      #COMPUTE ha_ma_dt_corr_ordered and ha_ma_dt_chi_sq_ordered
                      #=========================================================

                      ( 
                        SELECT 
                          CM, CH, 
                          CORR(ha_dt_ordered, ma_dt_ordered) AS ha_ma_dt_corr_ordered,
                          SUM(chi) AS ha_ma_dt_chi_sq_ordered
                        FROM
                        (
                          SELECT 
                            sao.CH as CH, sao.CM as CM, 
                            sao.show_answer_dt as ha_dt_ordered, 
                            cao.correct_answer_dt as ma_dt_ordered,
                            POW(cao.correct_answer_dt - sao.show_answer_dt, 2) / cao.correct_answer_dt as chi,
                          FROM
                          (
                            SELECT 
                              ha.CH, ma.CM, 
                              (ha.time - ha_lag) / 1e6 AS show_answer_dt, 
                              ROW_NUMBER() OVER (PARTITION BY ha.CH, ma.CM ORDER BY show_answer_dt) AS ha_dt_order
                            FROM
                            (
                              SELECT
                                *, 
                                COUNT(*) OVER (PARTITION BY CH, CM) AS X, #only positive dt rows at this point
                                #Compute lags ONLY for dt > 0
                                LAG(ha.time, 1) OVER (PARTITION BY CH, CM ORDER BY ha.time ASC) AS ha_lag
                              FROM
                              (
                                SELECT
                                  *,
                                  MAX(CASE WHEN ha.time < ma.time THEN ha.time ELSE NULL END) OVER (PARTITION BY CH, CM, ha.module_id) AS nearest_ha_before
                                FROM
                                ( #HARVESTER
                                  SELECT 
                                    time,
                                    username as CH,
                                    module_id,
                                    grade
                                  FROM {problem_check_table}
                                ) ha
                                JOIN EACH
                                ( #MASTER
                                  SELECT 
                                    time,
                                    username AS CM,
                                    module_id,
                                    grade
                                  FROM {problem_check_table}
                                  #####WHERE success = 'correct'
                                  WHERE {partition_without_prefix}
                                ) ma
                                ON ha.module_id = ma.module_id
                                WHERE CM != CH
                                # Masters's grade should be no smaller than harvester's grade, or no benefit occurred
                                AND ma.grade >= ha.grade
                              )
                              WHERE (nearest_ha_before = ha.time) #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt 
                            )
                            ######WHERE X >=10 #For tractability
                          ) sao
                          JOIN EACH
                          (
                            SELECT 
                              ha.CH as CH, ma.CM as CM, 
                              (ma.time - ma_lag) / 1e6 AS correct_answer_dt,
                              ROW_NUMBER() OVER (PARTITION BY CH, CM ORDER BY correct_answer_dt) ma_dt_order
                            FROM
                            (
                              SELECT
                                *, 
                                COUNT(*) OVER (PARTITION BY CH, CM) AS X, #only positive dt rows at this point
                                #Compute lags ONLY for dt > 0
                                LAG(ma.time, 1) OVER (PARTITION BY CH, CM ORDER BY ma.time ASC) AS ma_lag
                              FROM
                              (
                                SELECT
                                  *,
                                  MAX(CASE WHEN ha.time < ma.time THEN ha.time ELSE NULL END) OVER (PARTITION BY CH, CM, ha.module_id) AS nearest_ha_before
                                FROM
                                ( #HARVESTER
                                  SELECT 
                                    time,
                                    username as CH,
                                    module_id,
                                    grade
                                  FROM {problem_check_table}
                                ) ha
                                JOIN EACH
                                ( #MASTER
                                  SELECT 
                                    time,
                                    username AS CM,
                                    module_id,
                                    grade
                                  FROM {problem_check_table}
                                  #####WHERE success = 'correct'
                                  WHERE {partition_without_prefix} #PARTITION
                                ) ma
                                ON ha.module_id = ma.module_id
                                WHERE CM != CH
                                # Masters's grade should be no smaller than harvester's grade, or no benefit occurred
                                AND ma.grade >= ha.grade
                              )
                              WHERE (nearest_ha_before = ha.time) #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt
                            )
                            #####WHERE X >=10 #For tractability
                          ) cao
                          ON sao.CH = cao.CH AND sao.CM = cao.CM AND sao.ha_dt_order = cao.ma_dt_order
                        )
                        GROUP BY CH, CM
                      ) b
                      ON a.master_candidate = b.CM AND a.harvester_candidate = b.CH
                    ) a
                    LEFT OUTER JOIN EACH

                    #===============================================================================
                    #COMPUTE name_similarity using a modified levenshtein UDF by Curtis G. Northcutt
                    #===============================================================================

                    (
                      SELECT a as CM, b as CH, optimized_levenshtein_similarity as name_similarity
                      FROM levenshtein ( 
                        SELECT
                          CM as a, CH as b, COUNT(*) AS X #only positive dt rows at this point
                        FROM
                        (
                          SELECT
                            *,
                            MAX(CASE WHEN ha.time < ma.time THEN ha.time ELSE NULL END) OVER (PARTITION BY CH, CM, ha.module_id) AS nearest_ha_before
                          FROM
                          ( #HARVESTER
                            SELECT 
                              time,
                              username as CH,
                              module_id,
                              grade
                            FROM {problem_check_table}
                          ) ha
                          JOIN EACH
                          ( #MASTER
                            SELECT 
                              time,
                              username AS CM,
                              module_id,
                              grade
                            FROM {problem_check_table}
                            #####WHERE success = 'correct'
                            WHERE {partition_without_prefix} #PARTITION
                          ) ma
                          ON ha.module_id = ma.module_id
                          WHERE CM != CH
                          # Masters's grade should be no smaller than harvester's grade, or no benefit occurred
                          AND ma.grade >= ha.grade
                        )
                        WHERE nearest_ha_before = ha.time #DROP NEGATIVE DTs and use sa time resulting in smallest positive dt 
                        GROUP BY a, b
                        ####HAVING X>=10 #For tractability
                      )
                    ) b
                    ON a.master_candidate = b.CM AND a.harvester_candidate = b.CH
                  ) a
                  LEFT OUTER JOIN EACH
                  (

                    #====================================================
                    #MOST COMMON RESPONSE ANAYLSIS by Curtis G. Northcutt
                    #====================================================

                    SELECT
                      username,
                      FIRST(nblank_fr_submissions) as CH_nblank_fr_submissions,
                      FIRST(CASE WHEN response_type = 'free_response' THEN response ELSE NULL END) AS CH_most_common_free_response,
                      FIRST(CASE WHEN response_type = 'free_response' THEN response_cnt ELSE NULL END) AS CH_mcfr_cnt,
                      FIRST(CASE WHEN response_type = 'free_response' THEN percent_mcr_free_response ELSE NULL END) AS CH_mcfr_percent_of_users_free_responses,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN response ELSE NULL END) AS CH_most_common_multiple_choice_answer,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN response_cnt ELSE NULL END) AS CH_mcmc_cnt,
                      FIRST(CASE WHEN response_type = 'multiple_choice' THEN percent_mcr_multiple_choice ELSE NULL END) AS CH_mcmc_percent_of_users_multiple_choice
                    FROM
                    (
                      #When there are multiple most common responses, break tie by selecting the first.
                      SELECT
                        *, ROW_NUMBER() OVER (PARTITION BY username, response_type) AS row_num
                      FROM
                      (
                        #Compute the most common free response and multiple choice response
                        SELECT
                          username,
                          response,
                          response_type,
                          COUNT(*) AS response_cnt,
                          CASE WHEN response_type = 'multiple_choice' THEN COUNT(*) * 100.0 / nmultiple_choice ELSE NULL END AS percent_mcr_multiple_choice,
                          CASE WHEN response_type = 'free_response' THEN COUNT(*) * 100.0 / nfree_response ELSE NULL END AS percent_mcr_free_response,
                          nmultiple_choice,
                          nfree_response,
                          nblank_fr_submissions,
                          MAX(CASE WHEN response_type = 'multiple_choice' THEN INTEGER(response_cnt) ELSE -1 END) OVER (PARTITION BY username) as max_nmultiple_choice,
                          MAX(CASE WHEN response_type = 'free_response' THEN INTEGER(response_cnt) ELSE -1 END) OVER (PARTITION BY username) as max_nfree_response
                        FROM
                        (
                          #Compute number of occurrences for all responses of all users.
                          SELECT
                            username,
                            item.response AS response,
                            CASE WHEN (item.response CONTAINS 'choice_' 
                                       OR item.response CONTAINS 'dummy_default' 
                                       OR item.response CONTAINS 'option_'
                                       OR data.itype = 'multiplechoiceresponse'
                                       OR data.itype = 'choiceresponse') THEN 'multiple_choice' ELSE 'free_response' END AS response_type,
                            #COUNT(1) OVER (PARTITION BY username) as nproblems,
                            SUM(CASE WHEN response_type = 'multiple_choice' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nmultiple_choice,
                            SUM(CASE WHEN response_type = 'free_response' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nfree_response,
                            SUM(CASE WHEN response = '""' OR response = 'null' THEN 1 ELSE 0 END) OVER (PARTITION BY username) as nblank_fr_submissions
                          FROM 
                          (
                            SELECT 
                              user_id, item.response, data.itype
                            FROM [{dataset}.problem_analysis] a
                            LEFT OUTER JOIN [{dataset}.course_axis] b
                            ON a.problem_url_name = b.url_name
                          ) a
                          JOIN [{dataset}.person_course] b
                          ON a.user_id = b.user_id
                        )
                        GROUP BY username, response, response_type, nmultiple_choice, nfree_response, nblank_fr_submissions
                      )
                      WHERE (response_cnt = max_nmultiple_choice and response_type = "multiple_choice")
                      OR (response_cnt = max_nfree_response and response_type = "free_response")
                    )
                    WHERE row_num = 1
                    GROUP BY username
                  ) b
                  ON a.harvester_candidate = b.username
                ) a
                LEFT OUTER JOIN EACH
                (
                  #====================================================
                  #Add number of times master_candidated previously by CAMEO.
                  #====================================================
                  
                  SELECT
                    username, 
                    EXACT_COUNT_DISTINCT(course_id) AS ncameo
                  FROM [{cameo_master_table}] 
                  GROUP BY username
                ) b
                ON a.master_candidate = b.username
              ) a
              LEFT OUTER JOIN EACH
              (
                #====================================================
                #Add number of times CM, CH pair previously detected by CAMEO.
                #====================================================

                SELECT
                  #use new variable names to avoid namespace collusion
                  username as master, CH as harvester,
                  EXACT_COUNT_DISTINCT(course_id) AS ncameo_both
                FROM [{cameo_master_table}] 
                GROUP BY master, harvester
              ) b
              ON a.master_candidate = b.master AND a.harvester_candidate = b.harvester
            )
            #Only keep relevant harvesters
            WHERE rank1 <= 5
            OR rank2 <= 3
            OR rank3 <= 3
            OR rank4 <= 3
            OR rank5 <= 3
            OR rank6 <= 3
            OR rank7 <= 3
            OR rank8 <= 3
            OR rank9 <= 3
            OR rank10 <= 3
            OR rank11 <= 3
            OR rank12 <= 3
            OR rank13 <= 3
            OR rank14 <= 3
            OR rank15 <=3
            OR rank16 <=3 
            OR ncameo_both > 0
            ORDER BY cheating_likelihood DESC
            """.format(dataset=project_id + ':' + dataset if testing else dataset, 
                       course_id = course_id, 
                       partition=the_partition[i],
                       partition_without_prefix=the_partition[i].replace('a.', ''),
                       problem_check_show_answer_ip_table=problem_check_show_answer_ip_table,
                       not_certified_filter='nshow_ans_distinct >= 10' if force_online else 'certified = false',
                       certified_filter= 'ncorrect >= 10' if force_online else "certified = true",
                       cameo_master_table=cameo_master_table,
                       problem_check_table=problem_check_table)
      sql.append(item)

  print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
  sys.stdout.flush()

 # sasbu = "show_answer"
 # try:
 #     tinfo = bqutil.get_bq_table_info(dataset, sasbu)
 #     if testing:
 #         tinfo = bqutil.get_bq_table_info(dataset, sasbu, project_id)
 #     has_attempts_correct = (tinfo is not None)
 # except Exception as err:
 #     print "Error %s getting %s.%s" % (err, dataset, sasbu)
 #     has_attempts_correct = False
 # if not has_attempts_correct:
 #     print "---> No %s table; skipping %s" % (sasbu, table)
 #     return
 #
 # newer_than = datetime.datetime(2015, 6, 2, 19, 0)
 #
 # try:
 #     table_date = bqutil.get_bq_table_last_modified_datetime(dataset, table)
 #     if testing:
 #         table_date = bqutil.get_bq_table_last_modified_datetime(dataset, table, project_id)
 # except Exception as err:
 #     if 'Not Found' in str(err):
 #         table_date = None
 #     else:
 #         raise
 # if not table_date:
 #     force_recompute = True

 # if table_date and (table_date < newer_than):
 #     print("--> Forcing query recomputation of %s.%s, table_date=%s, newer_than=%s" % (dataset, table,
 #                                                                                       table_date, newer_than))
 #     force_recompute = True
 #
 # depends_on=["%s.%s" % (dataset, sasbu), ]
 # if table_date and not force_recompute:
 #         # get the latest mod time of tables in depends_on:
 #         modtimes = [ bqutil.get_bq_table_last_modified_datetime(*(x.split('.',1))) for x in depends_on]
 #         if testing:
 #             modtimes = [ bqutil.get_bq_table_last_modified_datetime(*(x.split('.',1)), project_id = project_id) for x in depends_on]
 #         latest = max([x for x in modtimes if x is not None])
 #     
 #         if not latest:
 #             raise Exception("[make_problem_analysis] Cannot get last mod time for %s (got %s), needed by %s.%s" % (depends_on, modtimes, dataset, table))
 #
 #         if table_date < latest:
 #             force_recompute = True

  force_recompute=True #REMOVE ONCE SAB IS STABLE!!!!!!!!!
  if force_recompute:
      for i in range(num_partitions): 
        print "--> Running SQL for partition %d of %d" % (i + 1, num_partitions)
        sys.stdout.flush()
        tries = 0 #Counts number of times "internal error occurs."
        while(tries < 10):
          try:
            bqutil.create_bq_table(testing_dataset if testing else dataset, 
                                   dataset + '_' + table if testing else table, 
                                   sql[i], overwrite=True if i==0 else 'append', allowLargeResults=True, 
                                   sql_for_description="\nNUM_PARTITIONS="+str(num_partitions)+"\n\n"+sql[i], udfs=[udf])
            break #Success - no need to keep trying - no internal error occurred.
          except Exception as err:
            if 'internal error' in str(err) and tries < 10:
              tries += 1
              print "---> Internal Error occurred. Sleeping 2 minutes and retrying."
              sys.stdout.flush()
              time.sleep(120) #2 minutes
              continue
            elif (num_partitions < 300) and ('internal error' in str(err) or 'Response too large' in str(err) or 'Resources exceeded' in str(err) or u'resourcesExceeded' in str(err)):
              print "---> Resources exceeded. Sleeping 1 minute and retrying."
              sys.stdout.flush()
              time.sleep(160) #1 minute
              print err
              print "="*80,"\n==> SQL query failed! Recursively trying again, with 50% more many partitions\n", "="*80
              return compute_show_ans_before(course_id, force_recompute=force_recompute, 
                                              use_dataset_latest=use_dataset_latest, force_num_partitions=int(round(num_partitions*1.5)), 
                                              testing=testing, testing_dataset= testing_dataset, 
                                              project_id = project_id, force_online = force_online,
                                              problem_check_show_answer_ip_table=problem_check_show_answer_ip_table)
            else:
              raise err
          

      if num_partitions > 5:
        print "--> sleeping for 60 seconds to try avoiding bigquery system error - maybe due to data transfer time"
        sys.stdout.flush()
        time.sleep(60)

  nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset if testing else dataset, 
                                           table_id=dataset+'_'+table if testing else table,
                                           project_id='mitx-research')
  if testing:
      nfound = bqutil.get_bq_table_size_rows(dataset_id=testing_dataset, table_id=table, project_id='mitx-research')
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
             norm_pearson_corr, unnormalized_pearson_corr,nshow_answer_unique_problems, percent_correct, frac_complete, 
             dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds, avg_max_dt_seconds,
             verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
            FROM
            (  
              SELECT 
               user_id, username, shadow, ip, grp, percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified,
               verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade,
               show_ans_before, unnormalized_pearson_corr,
               dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                 dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                   pc.dt_iqr as dt_iqr, pc.dt_std_dev as dt_std_dev, pc.percentile75_dt_seconds as percentile75_dt_seconds,
                   pc.percentile90_dt_seconds as percentile90_dt_seconds,
                   verified, countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, grade
                  FROM
                  (
                    SELECT
                     user_id, username, harvester_candidate AS shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                     percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                     dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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
                    LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                    ON pc.username = sa.master_candidate #join on cameo candidate
                    #Remove if few show answer before or too high time between show answer and submission
                    WHERE (sa.percent_show_ans_before >= 20
                    AND sa.median_max_dt_seconds < 5e4
                    AND sa.norm_pearson_corr > 0
                    AND (','+grp_usernames+',' CONTAINS ','+sa.harvester_candidate+','))#Only keep rows where cameo's shadow is in pc
                    OR certified = false #Keep all shadows for now
                  ) AS pc
                  LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                  ON pc.username = sa.harvester_candidate #join on shadow candidate
                  #Remove if few show answer before or too high time between show answer and submission
                  WHERE (sa.percent_show_ans_before >= 20
                  AND sa.median_max_dt_seconds < 5e4
                  AND sa.norm_pearson_corr > 0
                  AND (','+grp_usernames+',' CONTAINS ','+sa.master_candidate+','))#Only keep shadows if cameo in pc
                  OR certified = true #Keep previously found cameos
                  GROUP BY user_id, username, shadow, ip, certified, grp, show_ans_before, unnormalized_pearson_corr,
                    percent_show_ans_before, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                    dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
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

def compute_ip_pair_sybils3_unfiltered(course_id, force_recompute=False, use_dataset_latest=False, uname_ip_groups_table=None, course_info_table=None,
                                       testing=False, testing_dataset= None, project_id = None):
    
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

    if testing:
        project_dataset = project_id + ':' + dataset

    SQL = """# Northcutt SQL for finding sybils UNFILTERED
            ##################################
            # Sybils Version 3.0
            # Instead of same ip, considers users in same grp where
            # where grp is determined by the full transitive closure 
            # of all person_course (username, ip) pairs.
            SELECT
             "{course_id}" as course_id, user_id, username, shadow as harvester, ip, grp, 
             show_ans_before, prob_in_common, 
             show_ans_before / prob_in_common AS frac_show_ans_before,
             show_ans_before * 100.0 / prob_in_common AS percent_show_ans_before,
             grade, min_certifying_grade, grade / min_certifying_grade AS frac_certified,
             median_max_dt_seconds, norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified, verified,
             dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds, avg_max_dt_seconds, unnormalized_pearson_corr, ipcnt, 
             countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days
            FROM
            (  
              SELECT 
               user_id, username, shadow, ip, grp, avg_max_dt_seconds, median_max_dt_seconds,
               norm_pearson_corr, nshow_answer_unique_problems, percent_correct, frac_complete, certified, verified,
               countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
               time_active_in_days, grade, min_certifying_grade,
               show_ans_before, prob_in_common, unnormalized_pearson_corr, ipcnt,
               dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
               SUM(certified = true) OVER (PARTITION BY grp) AS sum_cert_true,
               SUM(certified = false) OVER (PARTITION BY grp) AS sum_cert_false
              FROM
              ( 
                # Find all users with >1 accounts, same ip address, different certification status
                SELECT
                 pc.user_id as user_id,
                 username, shadow, ip, grp, verified, min_certifying_grade,
                 countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
                 time_active_in_days, grade, avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                 nshow_answer_unique_problems, show_ans_before, prob_in_common, unnormalized_pearson_corr,
                 dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
                 ROUND(ac.percent_correct, 2) AS percent_correct,
                 frac_complete,
                 ac.certified AS certified,                       
                 COUNT(DISTINCT username) OVER (PARTITION BY grp) AS ipcnt
                #Adds a column with transitive closure group number for each user
                FROM
                (
                  SELECT 
                   user_id, username, shadow, ip, certified, grp, verified,
                   countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
                   time_active_in_days, grade, min_certifying_grade,
                   pc.show_ans_before AS show_ans_before, pc.prob_in_common AS prob_in_common, 
                   pc.unnormalized_pearson_corr AS unnormalized_pearson_corr, pc.avg_max_dt_seconds AS avg_max_dt_seconds, 
                   pc.median_max_dt_seconds AS median_max_dt_seconds, pc.norm_pearson_corr AS norm_pearson_corr,
                   pc.dt_iqr as dt_iqr, pc.dt_std_dev as dt_std_dev, pc.percentile75_dt_seconds as percentile75_dt_seconds,
                   pc.percentile90_dt_seconds as percentile90_dt_seconds
                  FROM
                  (
                    SELECT
                     user_id, username, harvester_candidate AS shadow, ip, certified, grp, verified,
                     countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, min_certifying_grade,
                     time_active_in_days, grade, show_ans_before, prob_in_common, unnormalized_pearson_corr,
                     avg_max_dt_seconds, median_max_dt_seconds, norm_pearson_corr,
                     dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds,
                     GROUP_CONCAT(username) OVER (PARTITION BY grp) AS grp_usernames
                    FROM
                    (
                      SELECT
                       user_id, a.username, a.ip AS ip, grp, certified, verified,
                       countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
                       time_active_in_days, grade, min_certifying_grade,
                       GROUP_CONCAT(a.username) OVER (partition by grp) AS grp_usernames
                      FROM 
                      (
                        SELECT
                         user_id, username, ip, certified, min_certifying_grade,
                         (CASE WHEN a.mode = "verified" THEN true ELSE false END) AS verified,
                         countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, 
                         (sum_dt / 60 / 60/ 24) as time_active_in_days, grade
                        FROM 
                        [{dataset}.person_course] AS a
                        JOIN
                        [{course_info_table}] AS b
                        ON a.course_id = b.course_id
                      ) AS a
                      JOIN EACH [{uname_ip_groups_table}] AS b
                      ON a.ip = b.ip and a.username = b.username
                    ) AS pc
                    LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                    ON pc.username = sa.master_candidate #join on cameo candidate
                    #Remove if few show answer before or too high time between show answer and submission
                    #WHERE (sa.percent_show_ans_before >= 20
                    #AND sa.median_max_dt_seconds < 5e4
                    #AND sa.norm_pearson_corr > 0
                    WHERE (','+grp_usernames+',' CONTAINS ','+sa.harvester_candidate+',')#Only keep rows where cameo's shadow is in pc
                    OR certified = false #Keep all shadows for now
                  ) AS pc
                  LEFT OUTER JOIN EACH [{dataset}.stats_show_ans_before] AS sa
                  ON pc.username = sa.harvester_candidate #join on shadow candidate
                  #Remove if few show answer before or too high time between show answer and submission
                  #WHERE (sa.percent_show_ans_before >= 20
                  #AND sa.median_max_dt_seconds < 5e4
                  #AND sa.norm_pearson_corr > 0
                  WHERE (','+grp_usernames+',' CONTAINS ','+sa.master_candidate+',')#Only keep shadows if cameo in pc
                  OR certified = true #Keep previously found cameos
                  GROUP BY user_id, username, shadow, ip, certified, grp, verified,
                   countryLabel, start_time, last_event, nforum_posts, nprogcheck, nvideo, time_active_in_days, 
                   grade, min_certifying_grade, unnormalized_pearson_corr, norm_pearson_corr,
                   show_ans_before, prob_in_common,  avg_max_dt_seconds, median_max_dt_seconds,
                   dt_iqr, dt_std_dev, percentile75_dt_seconds, percentile90_dt_seconds
                ) AS pc
                JOIN EACH [{dataset}.stats_attempts_correct] AS ac
                ON pc.user_id = ac.user_id
                #remove shadows with greater than 70% attempts correct
                #WHERE pc.certified = true
                #OR percent_correct < 70
              )
              #WHERE ipcnt < 10 #Remove NAT or internet cafe ips                                                     
            )
            # Since clicking show answer or guessing over and over cannot achieve certification, we should have
            # at least one (not certified) harvester, and at least one (certified) master who uses the answers.
            # Remove entire group if all the masters or all the harvesters were removed
            WHERE sum_cert_true > 0
            AND sum_cert_false > 0
            # Order by ip to group master and harvesters together. Order by certified so that we always have masters above harvester accounts.
            ORDER BY grp ASC, certified DESC
          """.format(dataset=project_dataset if testing else dataset, course_id=course_id, uname_ip_groups_table=uname_ip_groups_table, course_info_table=course_info_table)

    print "[analyze_problems] Creating %s.%s table for %s" % (dataset, table, course_id)
    sys.stdout.flush()

    sasbu = "stats_show_ans_before"
    try:
        tinfo = bqutil.get_bq_table_info(dataset, sasbu)
        if testing:
            tinfo = bqutil.get_bq_table_info(dataset, sasbu, project_id)
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
        if testing:
            bqutil.create_bq_table(testing_dataset, table, SQL, overwrite=True)
            
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
