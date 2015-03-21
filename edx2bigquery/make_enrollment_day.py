#!/usr/bin/python
#
# File:   make_enrollment_day.py
# Date:   14-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# Make enrollment-day tables on BigQuery.  Each daily dataset has the following fields:
#
# course_id
# time
# username
# diff_enrollment (+1 for activate, -1 for deactivate)
#
# These are computed by doing queries over each day's tracking logs, for a given
# course.  
#
# Stores results in the *_pcday dataset

import os
import sys
import json
import bqutil
import datetime
import process_tracking_logs

from path import path
from gsutil import get_gs_file_list

#-----------------------------------------------------------------------------

def old_process_course(course_id, force_recompute=False):
    '''
    DEPRACATED - instead of creating one table per day, because there is so little
    total data, create one enrollday_all table (see other function below).

    make enrollday2_* tables for specified course_id
    '''

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
	            time, 
                    event_struct.user_id as user_id, 
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "honor")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "honor")
                          then -1 
                          else 0 end) as diff_enrollment_honor,
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "verified")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "verified")
                          then -1 
                          else 0 end) as diff_enrollment_verified,
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "audit")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "audit")
                          then -1 
                          else 0 end) as diff_enrollment_audit,
            FROM [{dataset}.{table_id}] 
            where (event_type = "edx.course.enrollment.activated") or
                  (event_type = "edx.course.enrollment.deactivated")
            order by time;
            """

    course_dir = course_id.replace('/','__')
    dataset = bqutil.course_id2dataset(course_id)
    log_dataset = bqutil.course_id2dataset(course_id, dtype="logs")
    pcd_dataset = bqutil.course_id2dataset(course_id, dtype="pcday")

    print "Processing course %s (start %s)"  % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    log_tables = bqutil.get_tables(log_dataset)

    try:
        bqutil.create_dataset_if_nonexistent(pcd_dataset)
    except Exception as err:
        print "Oops, err when creating %s, err=%s" % (pcd_dataset, str(err))
        
    pcday_tables_info = bqutil.get_tables(pcd_dataset)
    pcday_tables = [x['tableReference']['tableId'] for x in pcday_tables_info.get('tables', [])]

    # print "pcday_tables = ", pcday_tables

    log_table_list = log_tables['tables']
    log_table_list.sort()

    for table in log_table_list:
        tr = table['tableReference']
        table_id = tr['tableId']
        if not table_id.startswith('tracklog'):
            continue
    
        date = table_id[9:]
    
        table_out = 'enrollday2_%s' % date
    
        if (table_out in pcday_tables) and not force_recompute:
            print "%s...already done, skipping" % table_id
            sys.stdout.flush()
            continue

        if bqutil.get_bq_table_size_rows(log_dataset, table_id)==0:
            print "...zero size table %s, skipping" % table_id
            sys.stdout.flush()
            continue

        print ("Creating %s " % table_out), 
        
        the_sql = SQL.format(course_id=course_id, 
                             dataset=log_dataset,
                             table_id=table_id)
    
        sys.stdout.flush()
    
        bqutil.create_bq_table(pcd_dataset, table_out, the_sql, wait=False)
    
    print "Done with course %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
        
#-----------------------------------------------------------------------------

def process_course(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    Create one enrollday_all table, containing time, user_id, and enrollment stats.
    Just as is done for problem_check tables, the master table holds all the events 
    extracted from the tracking logs for a course.  It isn't split into separate
    days.  It is ordered in time, however.  To update it, a new day's logs
    are processed, then the results appended to this table.

    If the enrollday_all table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.
    '''

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
	            time, 
                    event_struct.user_id as user_id, 
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "honor")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "honor")
                          then -1 
                          else 0 end) as diff_enrollment_honor,
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "verified")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "verified")
                          then -1 
                          else 0 end) as diff_enrollment_verified,
                    (case when (event_type = "edx.course.enrollment.activated" 
                                and event_struct.mode = "audit")
                          then 1 
                          when (event_type = "edx.course.enrollment.deactivated" 
                                and event_struct.mode = "audit")
                          then -1 
                          else 0 end) as diff_enrollment_audit,
            FROM {DATASETS} 
            where (event_type = "edx.course.enrollment.activated") or
                  (event_type = "edx.course.enrollment.deactivated")
                  and time > TIMESTAMP("{last_date}")
            order by time;
            """

    table = 'enrollday_all'

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
        
#-----------------------------------------------------------------------------

def make_enrollment_events(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    Create an enrollment_events table, based on scanning the tracking logs.
    Record all enrollment events, ordered by time.
    '''

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
	            time,
                    (case when (event_struct.user_id is not null) then event_struct.user_id
                         when (context.user_id is not null) then context.user_id end) as user_id,
                    (case when (event_struct.mode is not null) then event_struct.mode
                         else JSON_EXTRACT_SCALAR(event, "$.mode") end) as mode,
                    (case when (event_type = "edx.course.enrollment.activated") then True else False end) as activated,
                    (case when (event_type = "edx.course.enrollment.deactivated") then True else False end) as deactivated,
                    (case when (event_type = "edx.course.enrollment.mode_changed") then True else False end) as mode_changed,
                    (case when (event_type = "edx.course.enrollment.upgrade.succeeded") then True else False end) as upgraded,
                    event_type,
            FROM {DATASETS} 
            where (event_type contains "edx.course.enrollment")
                  and time > TIMESTAMP("{last_date}")
            order by time;
            """

    table = 'enrollment_events'

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
