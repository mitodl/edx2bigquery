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

def process_course(course_id, force_recompute=False):
    '''
    make enrollday_* tables for specified course_id
    '''

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
	            time, 
		    username, 
	            (case when event_type = "edx.course.enrollment.activated" then 1 
                          when event_type = "edx.course.enrollment.deactivated" then -1 
			  else 0 end) as diff_enrollment
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
    
        table_out = 'enrollday_%s' % date
    
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
        
