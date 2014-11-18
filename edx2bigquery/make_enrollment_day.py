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

def process_course(course_id, force_recompute=False, use_dataset_latest=False):
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

    TBD: update this to use process_tracking_logs.run_query_on_tracking_logs
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
            order by time;
            """

    table = 'enrollday_all'
    course_dir = course_id.replace('/','__')
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    log_dataset = bqutil.course_id2dataset(course_id, dtype="logs")

    print "Processing course %s (start %s)"  % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    existing = bqutil.get_list_of_table_ids(dataset)

    log_tables = [x for x in bqutil.get_list_of_table_ids(log_dataset) if x.startswith('tracklog_20')]
    log_dates = [x[9:] for x in log_tables]
    min_date = min(log_dates)
    max_date = max(log_dates)

    overwrite = False
    if table in existing:
        # find out what the end date is of the current table
        ct_last = bqutil.get_table_data(dataset, table, startIndex=-10, maxResults=100)
        last_dates = [datetime.datetime.utcfromtimestamp(float(x['time'])) for x in ct_last['data']]
        if last_dates:
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
