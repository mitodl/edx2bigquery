#!/usr/bin/python
#
# File:   make_times_on_task.py
# Date:   05-Mar-2015
# Author: I. Chuang <ichuang@mit.edu>
#
# Make table of daily time-on-task from daily tracking logs.  This includes:
#
# date
# course_id
# user_id
# total_time30          - total time on system, with 30 minute idle cutoff
# total_time5           - total time on system, with 5 minute idle cutoff
# total_video_time30    - total time using video, with 30 min idle cutoff
# total_video_time5     - total time using video, with 5 min idle cutoff
# total_problem_time30  - total time using CAPA problems, with 30 min idle cutoff
# total_problem_time5   - total time using CAPA problems, with 5 min idle cutoff
# total_forum_time30    - total time using forum, with 30 min idle cutoff
# total_forum_time5     - total time using forum, with 5 min idle cutoff
#
# These are computed by doing queries over each day's tracking logs, for a given
# course.  

import os
import sys
import json
import bqutil
import datetime
import process_tracking_logs

from path import path
from gsutil import get_gs_file_list
        
#-----------------------------------------------------------------------------

def process_course_time_on_task(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    Create the time_on_task table, containing time, user_id, and time
    on task stats.  This table isn't split into separate days.  It is
    ordered in time, however.  To update it, a new day's logs are
    processed, then the results appended to this table.

    If the table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.
    '''

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
                    date(time) as date,
                    username, 

                    # total time spent on system
                    SUM( case when dt < 5*60 then dt end ) as total_time_5,
                    SUM( case when dt < 30*60 then dt end ) as total_time_30,

                    # total time spent watching videos
                    SUM( case when (dt_video is not null) and (dt_video < 5*60) then dt_video end ) as total_video_time_5,
                    SUM( case when (dt_video is not null) and (dt_video < 30*60) then dt_video end ) as total_video_time_30,

                    # total time spent doing problems
                    SUM( case when (dt_problem is not null) and (dt_problem < 5*60) then dt_problem end ) as total_problem_time_5,
                    SUM( case when (dt_problem is not null) and (dt_problem < 30*60) then dt_problem end ) as total_problem_time_30,

            FROM
              (
              SELECT time,
                username,
                (time - last_time)/1.0E6 as dt,		# dt is in seconds
                case when is_video then (time - last_time_video)/1.0E6 end as dt_video,
                case when is_problem then (time - last_time_problem)/1.0E6 end as dt_problem,
              FROM
                (
                SELECT time, 
                    username,
                    last_username,
                    USEC_TO_TIMESTAMP(last_time) as last_time,
                    (case when is_video then USEC_TO_TIMESTAMP(last_time_video) end) as last_time_video,
                    # last_username_video,
                    # last_event_video,
                    is_problem,
                    is_video,
                    (case when is_problem then USEC_TO_TIMESTAMP(last_time_problem) end) as last_time_problem,
                    # last_username_problem,
                    # last_event_problem,
                FROM
                  (SELECT time,
                    username,
                    lag(time, 1) over (partition by username order by time) last_time,
                    lag(username, 1) over (partition by username order by time) last_username,
                    is_video,
                    is_problem,
                    case when is_problem then username else '' end as uname_problem,
                    case when is_video then username else '' end as uname_video,
                    lag(time, 1) over (partition by uname_video order by time) last_time_video,
                    # lag(event_type, 1) over (partition by uname_video order by time) last_event_video,
                    # lag(uname_video, 1) over (partition by uname_video order by time) last_username_video,
                    lag(time, 1) over (partition by uname_problem order by time) last_time_problem,
                    # lag(event_type, 1) over (partition by uname_problem order by time) last_event_problem,
                    # lag(uname_problem, 1) over (partition by uname_problem order by time) last_username_problem,
                  FROM
                    (SELECT time, 
                      username,
                      event_type,
                      case when (REGEXP_MATCH(event_type ,r'\w+_video')
                               or REGEXP_MATCH(event_type, r'\w+_transcript')
                              ) then True else False end as is_video,
                      case when REGEXP_MATCH(event_type, r'problem_\w+') then True else False end as is_problem,
                    FROM {DATASETS}
                    WHERE       
                                     NOT event_type contains "/xblock/"
                                     AND username is not null
                                     AND username != ""
                                     and time > TIMESTAMP("{last_date}")
                    )
                  )
                WHERE last_time is not null
                ORDER BY username, time
                )
              )
            group by course_id, username, date
            order by date, username;
          """

    table = 'time_on_task'

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
