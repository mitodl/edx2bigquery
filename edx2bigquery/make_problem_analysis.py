#!/usr/bin/python
#
# Analyze courseware_studentmodule and tracking logs specifically for capa problems.
#
# 1. Find subset of studentmodule entries reating to problems
#    extract table ("problem_analysis") of:
#
#    course_id, user_id, problem_url_name, item: { answer_id, response, correctness} , npoints, attempts, seed, done, grade, created
#
# 2. Extract from tracking logs all problem_check attempts and fill in information in the "attempts_DAY" table inside {dataset}_pcday 
#    for each day.
#

import os, sys
import csv
import re
import json
import gsutil
import bqutil
import datetime

from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, openfile

csv.field_size_limit(1310720)

def analyze_problems(course_id, basedir=None, datedir=None, force_recompute=False,
                     use_dataset_latest=False,
                     ):

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

    print "[analyze_problems] processing %s for course %s" % (smfn, course_id)
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
        if not line['module_type']=='problem':	# bug in edX platform?  too many entries are type=problem
            continue
        mid = line['module_id']
        (org, num, category, url_name) = mid.rsplit('/',3)
        if not category=='problem':		# filter based on category info in module_id
            continue
        try:
            state = json.loads(line['state'].replace('\\\\','\\'))
        except Exception as err:
            print "oops, failed to parse state in studentmodule entry, err=%s" % str(err)
            print "    %s" % repr(line)
            continue
        
        if 'correct_map' not in state:
            continue
        
        if not state['correct_map']:	# correct map = {} is not of interest
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

def problem_check_tables(course_id, force_recompute=False, use_dataset_latest=False):
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
               where event_type = "problem_check"
                  and event_source = "server"
               order by time;
            """

    table = 'problem_check'
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    log_dataset = bqutil.course_id2dataset(course_id, dtype="logs")

    existing = bqutil.get_list_of_table_ids(dataset)

    log_tables = [x for x in bqutil.get_list_of_table_ids(log_dataset) if x.startswith('tracklog_20')]
    log_dates = [x[9:] for x in log_tables]
    min_date = min(log_dates)
    max_date = max(log_dates)

    overwrite = False
    if table in existing:
        # find out what the end date is of the current problem_check table
        pc_last = bqutil.get_table_data(dataset, table, startIndex=-10, maxResults=100)
        last_dates = [datetime.datetime.utcfromtimestamp(float(x['time'])) for x in pc_last['data']]
        table_max_date = max(last_dates).strftime('%Y%m%d')
        if max_date <= table_max_date:
            print '--> %s already exists, max_date=%s, but tracking log data min=%s, max=%s, nothing new!' % (table, 
                                                                                                              table_max_date,
                                                                                                              min_date,
                                                                                                              max_date)
            return
        min_date = (max(last_dates) + datetime.timedelta(days=1)).strftime('%Y%m%d')
        print '--> %s already exists, max_date=%s, adding tracking log data from %s to max=%s' % (table, 
                                                                                                  table_max_date,
                                                                                                  min_date,
                                                                                                  max_date)
        overwrite = 'append'

    from_datasets = """(
                  TABLE_QUERY({dataset},
                       "integer(regexp_extract(table_id, r'tracklog_([0-9]+)')) BETWEEN {start} and {end}"
                     )
                  )
         """.format(dataset=log_dataset, start=min_date, end=max_date)

    the_sql = SQL.format(course_id=course_id, DATASETS=from_datasets)

    print "Making new problem_check table for course %s (start=%s, end=%s) [%s]"  % (course_id, min_date, max_date, datetime.datetime.now())
    sys.stdout.flush()

    bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite)

    if overwrite=='append':
        txt = '[%s] added tracking log data from %s to %s' % (datetime.datetime.now(), min_date, max_date)
        bqutil.add_description_to_table(dataset, table, txt, append=True)
    
    print "Done with course %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
        
