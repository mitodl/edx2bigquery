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

def process_course_time_on_task(course_id, force_recompute=False, use_dataset_latest=False, end_date=None,
                                just_do_totals=False, limit_query_size=False, table_max_size_mb=800,
                                skip_totals=False, start_date=None, config_parameter_overrides=None):
    '''Create the time_on_task table, containing time, user_id, and time
    on task stats.  This table isn't split into separate days.  It is
    ordered in time, however.  To update it, a new day's logs are
    processed, then the results appended to this table.

    If the table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.

    Compute totals and store in time_on_task_totals, by summing over all dates, 
    grouped by user.

    config_parameter_overrides may be provided as a dict, which
    overrides the default timeout parameters (5 min and 30 min) and
    output table name; used for bounday analysis testing.

    How are time_on_task numbers computed?

    Let TO be the timeout (eg 5 min or 30 min).  For all time-ordered
    pairs of events, at times t1 and t2, with dt = t2-t1, the total
    time is the sum of all dt such that dt <= TO.

    Solo (unpaired) events do not cause addition of any time to the total.

    For video (or text or problems), events are filtered and only
    include events of those types.

    This methodology undercounts, for example, if the learner ends
    watching a video by closing the browser, or by switching elsewhere
    without stopping the video player first.  (note there does exist a
    stop_video event, which occurs when the video stop button is
    clicked).

    This methodology overcounts, for example, if the learner watches a
    video for awhile, switches to a problem, then back to the video.
    If the initial and final video intervals happen within TO, then
    the intermediate interval is counted towards the video total time,
    despite the learner actually doing something else (the problem)
    during that time.

    '''

    if just_do_totals:
        return process_time_on_task_totals(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest,
                                           config_parameter_overrides=config_parameter_overrides)

    config_parameters = {
        'timeout_short': 5,
        'timeout_long': 30,
        'time_on_task_table_name': 'time_on_task',
        'course_id': course_id,
        }

    config_parameters.update(config_parameter_overrides or {})

    SQL_TOP = """
            SELECT 
  		    "{course_id}" as course_id,
                    date(time) as date,
                    username, 

                    # total time spent on system
                    SUM( case when dt < {timeout_short}*60 then dt end ) as total_time_{timeout_short},
                    SUM( case when dt < {timeout_long}*60 then dt end ) as total_time_{timeout_long},

                    # total time spent watching videos
                    SUM( case when (dt_video is not null) and (dt_video < {timeout_short}*60) then dt_video end ) as total_video_time_{timeout_short},
                    SUM( case when (dt_video is not null) and (dt_video < {timeout_long}*60) then dt_video end ) as total_video_time_{timeout_long},
                    SUM( case when (serial_dt_video is not null) and (serial_dt_video < {timeout_long}*60) then serial_dt_video end ) as serial_video_time_{timeout_long},

                    # total time spent doing problems
                    SUM( case when (dt_problem is not null) and (dt_problem < {timeout_short}*60) then dt_problem end ) as total_problem_time_{timeout_short},
                    SUM( case when (dt_problem is not null) and (dt_problem < {timeout_long}*60) then dt_problem end ) as total_problem_time_{timeout_long},
                    SUM( case when (serial_dt_problem is not null) and (serial_dt_problem < {timeout_long}*60) then serial_dt_problem end ) as serial_problem_time_{timeout_long},

                    # total time spent on forum
                    SUM( case when (dt_forum is not null) and (dt_forum < {timeout_short}*60) then dt_forum end ) as total_forum_time_{timeout_short},
                    SUM( case when (dt_forum is not null) and (dt_forum < {timeout_long}*60) then dt_forum end ) as total_forum_time_{timeout_long},
                    SUM( case when (serial_dt_forum is not null) and (serial_dt_forum < {timeout_long}*60) then serial_dt_forum end ) as serial_forum_time_{timeout_long},

                    # total time spent with textbook or wiki
                    SUM( case when (dt_text is not null) and (dt_text < {timeout_short}*60) then dt_text end ) as total_text_time_{timeout_short},
                    SUM( case when (dt_text is not null) and (dt_text < {timeout_long}*60) then dt_text end ) as total_text_time_{timeout_long},
                    SUM( case when (serial_dt_text is not null) and (serial_dt_text < {timeout_long}*60) then serial_dt_text end ) as serial_text_time_{timeout_long},

    """

    SQL_BOT = """
            FROM
              (
              SELECT time,
                username,
                (time - last_time)/1.0E6 as dt,         # dt is in seconds
                case when is_video then (time - last_time_video)/1.0E6 end as dt_video,
                case when is_problem then (time - last_time_problem)/1.0E6 end as dt_problem,
                case when is_forum then (time - last_time_forum)/1.0E6 end as dt_forum,
                case when is_text then (time - last_time_text)/1.0E6 end as dt_text,

                case when is_video then (time - last_time_xevent)/1.0E6 end as serial_dt_video,
                case when is_problem then (time - last_time_xevent)/1.0E6 end as serial_dt_problem,
                case when is_forum then (time - last_time_xevent)/1.0E6 end as serial_dt_forum,
                case when is_text then (time - last_time_xevent)/1.0E6 end as serial_dt_text,
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
                    is_forum,
                    is_text,
                    (case when is_forum then USEC_TO_TIMESTAMP(last_time_forum) end) as last_time_forum,
                    (case when is_text then USEC_TO_TIMESTAMP(last_time_text) end) as last_time_text,
                    is_xevent,
                    (case when is_xevent then USEC_TO_TIMESTAMP(last_time_xevent) end) as last_time_xevent,
                FROM
                  (SELECT time,
                    username,
                    lag(time, 1) over (partition by username order by time) last_time,
                    lag(username, 1) over (partition by username order by time) last_username,
                    is_video,
                    is_problem,
                    is_forum,
                    is_text,
                    (is_video or is_problem or is_forum or is_text) as is_xevent,   # x = video, problem, forum, or text: any event
                    case when is_problem then username else '' end as uname_problem,
                    case when is_video then username else '' end as uname_video,
                    case when is_forum then username else '' end as uname_forum,
                    case when is_text then username else '' end as uname_text,
                    case when (is_video or is_problem or is_forum or is_text) then username else '' end as uname_xevent,
                    lag(time, 1) over (partition by uname_video order by time) last_time_video,
                    # lag(event_type, 1) over (partition by uname_video order by time) last_event_video,
                    # lag(uname_video, 1) over (partition by uname_video order by time) last_username_video,
                    lag(time, 1) over (partition by uname_problem order by time) last_time_problem,
                    # lag(event_type, 1) over (partition by uname_problem order by time) last_event_problem,
                    # lag(uname_problem, 1) over (partition by uname_problem order by time) last_username_problem,
                    lag(time, 1) over (partition by uname_forum order by time) last_time_forum,
                    lag(time, 1) over (partition by uname_text order by time) last_time_text,
                    lag(time, 1) over (partition by uname_xevent order by time) last_time_xevent,
                  FROM
                    (SELECT time, 
                      username,
                      event_type,
                      case when (REGEXP_MATCH(event_type ,r'\w+_video')
                               or REGEXP_MATCH(event_type, r'\w+_transcript')
                              ) then True else False end as is_video,
                      case when REGEXP_MATCH(event_type, r'problem_\w+') then True else False end as is_problem,
                      case when (REGEXP_MATCH(event_type ,r'^edx\.forum\..*')
                               or event_type contains "/discussion/forum"
                               or event_type contains "/discussion/threads"
                               or event_type contains "/discussion/comments"
                              ) then True else False end as is_forum,
                      case when (REGEXP_MATCH(event_type ,r'^textbook\..*')
                               or event_type contains "/wiki/"
                              ) then True else False end as is_text,
                    FROM {DATASETS}
                    WHERE       
                                     NOT event_type contains "/xblock/"
                                     AND username is not null
                                     AND username != ""
                                     and time > TIMESTAMP("{last_date}")
                                     {hash_limit}
                    )
                  )
                WHERE last_time is not null
                ORDER BY username, time
                )
              )
            group by course_id, username, date
            order by date, username;
          """

    SQL = SQL_TOP.format(**config_parameters) + SQL_BOT

    table = config_parameters.get('time_on_task_table_name') or 'time_on_task'

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     start_date=start_date,
                                                     get_date_function=gdf,
                                                     days_delta=0,
                                                     has_hash_limit=True,
                                                     newer_than=datetime.datetime(2015,3,15),	# schema change
                                                     table_max_size_mb=table_max_size_mb,
                                                     limit_query_size=limit_query_size)

    if not skip_totals:
        return process_time_on_task_totals(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest,
                                           config_parameter_overrides=config_parameter_overrides)

    return

def process_time_on_task_totals(course_id, force_recompute=False, use_dataset_latest=False, config_parameter_overrides=None):

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    config_parameters = {
        'timeout_short': 5,
        'timeout_long': 30,
        'time_on_task_table_name': 'time_on_task',
        'time_on_task_totals_table_name': 'time_on_task_totals',
        'course_id': course_id,
        'dataset': dataset,
        }

    config_parameters.update(config_parameter_overrides or {})

    SQL = """
            SELECT 
  		    "{course_id}" as course_id,
                    username, 

                    sum(total_time_{timeout_short}) as total_time_{timeout_short},
                    sum(total_time_{timeout_long}) as total_time_{timeout_long},

                    sum(total_video_time_{timeout_short}) as total_video_time_{timeout_short},
                    sum(total_video_time_{timeout_long}) as total_video_time_{timeout_long},
                    sum(serial_video_time_{timeout_long}) as serial_video_time_{timeout_long},

                    sum(total_problem_time_{timeout_short}) as total_problem_time_{timeout_short},
                    sum(total_problem_time_{timeout_long}) as total_problem_time_{timeout_long},
                    sum(serial_problem_time_{timeout_long}) as serial_problem_time_{timeout_long},

                    sum(total_forum_time_{timeout_short}) as total_forum_time_{timeout_short},
                    sum(total_forum_time_{timeout_long}) as total_forum_time_{timeout_long},
                    sum(serial_forum_time_{timeout_long}) as serial_forum_time_{timeout_long},

                    sum(total_text_time_{timeout_short}) as total_text_time_{timeout_short},
                    sum(total_text_time_{timeout_long}) as total_text_time_{timeout_long},
                    sum(serial_text_time_{timeout_long}) as serial_text_time_{timeout_long},

            FROM [{dataset}.{time_on_task_table_name}]
            GROUP BY course_id, username
            order by username
         """

    the_sql = SQL.format(**config_parameters)

    tablename = config_parameters.get('time_on_task_totals_table_name') or 'time_on_task_totals'

    print "Computing %s for %s" % (tablename, dataset)
    sys.stdout.flush()

    bqdat = bqutil.get_bq_table(dataset, tablename, the_sql,
                                force_query=force_recompute,
                                depends_on=[ '%s.time_on_task' % dataset ],
                                )

    return bqdat
