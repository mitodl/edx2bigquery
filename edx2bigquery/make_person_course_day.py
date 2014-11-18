#!/usr/bin/python
#
# File:   make_person_course_day.py
# Date:   14-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# Make person-course-day tables on BigQuery.  Each daily dataset has the following fields:
#
# course_id
# username
# user_id
# day_num (proleptic gregorian ordinal)
# nevents
# nproblem_check
# nforum_events
# nplay_video
# nshow_answer
# nshow_transcript
# nseq_goto
# nseek_video
#
# These are computed by doing queries over each day's tracking logs, for a given
# course.  
#
# ----------------------------------------
# Usage:
#
#     python make_person_course_day.py course_id
#
# e.g. 
#
#     python make_person_course_day.py MITx/6.SFMx/1T2014
#
# This assumes the tracking logs are already loaded into a bigquery database of name
#
#     MITx__6.SFMx__1T2014_logs
#
# with tables named
#
#     tracklog_YYYYMMDD
#
# with one tracklog_* file per day.
#
# The daily data tables are stored as tables with name:
#
#    pcday_YYYYMMDD
#
# inside the dataset named:
#
#    <course_id>_pcday
#
# where <course_id> is the course_id with '__' instead of '/'.


import os
import sys
import json
import bqutil
import datetime
from path import path
from gsutil import get_gs_file_list

import process_tracking_logs

#-----------------------------------------------------------------------------

def process_course(course_id, force_recompute=False):
    '''
    make person_course_day tables for specified course_id
    '''

    PCDAY_SQL = """
    select username, 
           "{course_id}" as course_id,
           sum(bevent) as nevents,
           sum(bprogress) as nprogcheck,
           sum(bshow_answer) as nshow_answer,
           sum(bvideo) as nvideo, 
           sum(bproblem_check) as nproblem_check,
           sum(bforum) as nforum,
           sum(bshow_transcript) as ntranscript,
           sum(bseq_goto) as nseq_goto,
           sum(bseek_video) as nseek_video,
           sum(bpause_video) as npause_video,
           MAX(time) as last_event,
           AVG(
               case when (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 then null
               else (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 end
               ) as avg_dt,
           STDDEV(
               case when (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 then null
               else (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 end
           ) as sdv_dt,
           MAX(
               case when (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 then null
               else (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 end
           ) as max_dt,
           COUNT(
               case when (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 then null
               else (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 end
           ) as n_dt,
           SUM(
               case when (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 then null
               else (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 end
           ) as sum_dt
    from
    (SELECT username, 
      case when event_type = "play_video" then 1 else 0 end as bvideo,
      case when event_type = "problem_check" then 1 else 0 end as bproblem_check,
      case when username != "" then 1 else 0 end as bevent,
      case when regexp_match(event_type, "^/courses/{course_id}/discussion/.*") then 1 else 0 end as bforum,
      case when regexp_match(event_type, "^/courses/{course_id}/progress") then 1 else 0 end as bprogress,
      case when event_type in ("show_answer", "showanswer") then 1 else 0 end as bshow_answer,
      case when event_type = 'show_transcript' then 1 else 0 end as bshow_transcript,
      case when event_type = 'seq_goto' then 1 else 0 end as bseq_goto,
      case when event_type = 'seek_video' then 1 else 0 end as bseek_video,
      case when event_type = 'pause_video' then 1 else 0 end as bpause_video,
      # case when event_type = 'edx.course.enrollment.activated' then 1 else 0 end as benroll,
      # case when event_type = 'edx.course.enrollment.deactivated' then 1 else 0 end as bunenroll
      time,
      lag(time, 1) over (partition by username order by time) last_time
      FROM [{dataset}.{table_id}]
      WHERE
        NOT event_type contains "/xblock/"
        AND username != ""
    )
    group by course_id, username
    order by sdv_dt desc
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

    print "pcday_tables = ", pcday_tables

    log_table_list = log_tables['tables']
    log_table_list.sort()

    for table in log_table_list:
        tr = table['tableReference']
        table_id = tr['tableId']
        if not table_id.startswith('tracklog'):
            continue
    
        date = table_id[9:]
    
        table_out = 'pcday_%s' % date
    
        if (table_out in pcday_tables) and not force_recompute:
            print "%s...already done, skipping" % table_id
            sys.stdout.flush()
            continue

        if bqutil.get_bq_table_size_rows(log_dataset, table_id)==0:
            print "...zero size table %s, skipping" % table_id
            sys.stdout.flush()
            continue

        print ("Creating %s " % table_out),
        
        the_sql = PCDAY_SQL.format(course_id=course_id, 
                                   dataset=log_dataset,
                                   table_id=table_id)
    
        sys.stdout.flush()
    
        bqutil.create_bq_table(pcd_dataset, table_out, the_sql, wait=False)
    
    print "Done with course %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
        
#-----------------------------------------------------------------------------

def compute_person_course_day_ip_table(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    make pcday_ip_counts table for specified course_id.

    The master table holds all the (username, course_id, date, ip_addr, nip_count)
    data for a course.  It isn't split into separate
    days.  It is ordered in time, however.  To update it, a new day's logs
    are processed, then the results appended to this table.

    This is used in computing the modal IP address of users.

    If the pcday_ip_counts table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.

    If the query fails because of "Resources exceeded during query execution"
    then try setting the end_date, to do part at a time.
    '''

    SQL = """
                    SELECT username, ip, 
                       date(time) as date, 
                       count(*) as ipcount,
                       '{course_id}' as course_id,
                    FROM {DATASETS}
                    where username != ""
                    group by username, ip, date
                    order by date 
          """

    table = 'pcday_ip_counts'

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf)


    
