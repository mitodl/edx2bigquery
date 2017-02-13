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

def obsolete_process_course(course_id, force_recompute=False, check_dates=True):
    '''
    make person_course_day tables for specified course_id.  This version
    produces one table for each day.  It is inefficient when there are 
    many days with very small daily tracking log tables.
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
            skip = True
            if check_dates:
                table_out_date = bqutil.get_bq_table_last_modified_datetime(pcd_dataset, table_out)
                log_table_date = bqutil.get_bq_table_last_modified_datetime(log_dataset, table_id)
                if log_table_date > table_out_date:
                    skip = False
                    print "%s...already exists, but table_out date=%s and log_table date=%s, so re-computing" % (table_out,
                                                                                                                 table_out_date,
                                                                                                                 log_table_date)
            if skip:
                print "%s...already done, skipping" % table_out
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

def process_course(course_id, force_recompute=False, use_dataset_latest=False, end_date=None, 
                   check_dates=True, skip_last_day=False):
    '''
    Make {course_id}.person_course_day table for specified course_id.

    This is a single course-specific table, which contains all day's data.
    It is incrementally updated when new tracking logs data comes in,
    by appending rows to the end.  The rows are kept in time order.

    check_dates is disregarded.

    If skip_last_day is True then do not include the last day of tracking log data
    in the processing.  This is done to avoid processing partial data, e.g. when
    tracking log data are incrementally loaded with a delta of less than one day.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    videoTableExists = False
    try:

        tinfo_video = bqutil.get_bq_table_info(dataset, 'video_stats_day')
        assert tinfo_video is not None, "Video stats table missing... Not including video stats"
	videoTableExists = True

    except (AssertionError, Exception) as err:
        #print " --> Err: missing %s.%s?  Skipping creation of chapter_grades" % (dataset, "course_axis")
        sys.stdout.flush()
	pass
        #return

    forumTableExists = False
    try:

        tinfo_forum = bqutil.get_bq_table_info(dataset, 'forum_events')
        assert tinfo_forum is not None, "Forum events table missing... Not including forum stats"
	forumTableExists = True

    except (AssertionError, Exception) as err:
        #print " --> Err: missing %s.%s?  Skipping creation of chapter_grades" % (dataset, "course_axis")
        sys.stdout.flush()
	pass
        #return

    problemTableExists = False
    try:

        tinfo_personproblem = bqutil.get_bq_table_info(dataset, 'person_problem')
        tinfo_courseproblem = bqutil.get_bq_table_info(dataset, 'course_problem')
        tinfo_courseaxis = bqutil.get_bq_table_info(dataset, 'course_axis')
        tinfo_personcourse = bqutil.get_bq_table_info(dataset, 'person_course')
	# Check course axis and person course, course problem
        assert tinfo_personproblem is not None, "Person problem table missing... Not including problem stats"
        assert tinfo_courseproblem is not None, "Course problem table missing... Not including problem stats"
        assert tinfo_courseaxis is not None, "Course axis table missing... Not including problem stats"
        assert tinfo_personcourse is not None, "Person Course table missing... Not including problem stats"
	problemTableExists = True

    except (AssertionError, Exception) as err:
        #print " --> Err: missing %s.%s?  Skipping creation of chapter_grades" % (dataset, "course_axis")
        sys.stdout.flush()
	pass

    PCDAY_SQL_BASE_SELECT = """
			  SELECT username,
				 '{course_id}' AS course_id,
				 DATE(time) AS date,
				 SUM(bevent) AS nevents,
				 SUM(bprogress) AS nprogcheck,
				 SUM(bshow_answer) AS nshow_answer,
				 SUM(bvideo) AS nvideo,
				 SUM(bproblem_check) AS nproblem_check,
				 SUM(bforum) AS nforum,
				 SUM(bshow_transcript) AS ntranscript,
				 SUM(bseq_goto) AS nseq_goto,
				 SUM(bseek_video) AS nseek_video,
				 SUM(bpause_video) AS npause_video,
		    """

    PCDAY_SQL_VIDEO_EXISTS = """
			  	 COUNT(DISTINCT video_id) AS nvideos_viewed, # New Video - Unique videos viewed
				 SUM(case when position is not null then FLOAT(position) else FLOAT(0.0) end) AS nvideos_watched_sec, # New Video - # sec watched using max video position
		    """

    PCDAY_SQL_VIDEO_DNE = """
				 0 AS nvideos_viewed, # New Video - Unique videos viewed
				 FLOAT(0.0) AS nvideos_watched_sec, # New Video - # sec watched using max video position
		    """
    PCDAY_SQL_VIDEO_SELECT = PCDAY_SQL_VIDEO_EXISTS if videoTableExists else PCDAY_SQL_VIDEO_DNE

    PCDAY_SQL_FORUM_EXISTS = """
				 SUM(case when read is not null then read else 0 end) AS nforum_reads, # New discussion - Forum reads
				 SUM(case when write is not null then write else 0 end) AS nforum_posts, # New discussion - Forum posts
				 COUNT(DISTINCT thread_id ) AS nforum_threads, # New discussion - Unique forum threads interacted with
		    """

    PCDAY_SQL_FORUM_DNE = """
				 0 AS nforum_reads, # New discussion - Forum reads
				 0 AS nforum_posts, # New discussion - Forum posts
				 0 AS nforum_threads, # New discussion - Unique forum threads interacted with
		    """
    PCDAY_SQL_FORUM_SELECT = PCDAY_SQL_FORUM_EXISTS if forumTableExists else PCDAY_SQL_FORUM_DNE

    PCDAY_SQL_PROBLEM_EXISTS = """
				 COUNT(DISTINCT problem_nid ) AS nproblems_answered, # New Problem - Unique problems attempted
				 SUM(case when n_attempts is not null then n_attempts else 0 end) AS nproblems_attempted, # New Problem - Total attempts
				 SUM(case when ncount_problem_multiplechoice is not null then ncount_problem_multiplechoice else 0 end) as nproblems_multiplechoice,
				 SUM(case when ncount_problem_choice is not null then ncount_problem_choice else 0 end) as nproblems_choice,
				 SUM(case when ncount_problem_numerical is not null then ncount_problem_numerical else 0 end) as nproblems_numerical,
				 SUM(case when ncount_problem_option is not null then ncount_problem_option else 0 end) as nproblems_option,
				 SUM(case when ncount_problem_custom is not null then ncount_problem_custom else 0 end) as nproblems_custom,
				 SUM(case when ncount_problem_string is not null then ncount_problem_string else 0 end) as nproblems_string,
				 SUM(case when ncount_problem_mixed is not null then ncount_problem_mixed else 0 end) as nproblems_mixed,
				 SUM(case when ncount_problem_formula is not null then ncount_problem_formula else 0 end) as nproblems_forumula,
				 SUM(case when ncount_problem_other is not null then ncount_problem_other else 0 end) as nproblems_other,
		    """

    PCDAY_SQL_PROBLEM_DNE = """
				 0 AS nproblems_answered, # New Problem - Unique problems attempted
				 0 AS nproblems_attempted, # New Problem - Total attempts
				 0 AS nproblems_multiplechoice,
				 0 AS nproblems_choice,
				 0 AS nproblems_numerical,
				 0 AS nproblems_option,
				 0 AS nproblems_custom,
				 0 AS nproblems_string,
				 0 AS nproblems_mixed,
				 0 AS nproblems_forumula,
				 0 AS nproblems_other,
		    """
    PCDAY_SQL_PROBLEM_SELECT = PCDAY_SQL_PROBLEM_EXISTS if problemTableExists else PCDAY_SQL_PROBLEM_DNE

    PCDAY_SQL_MID = """
				 MIN(time) AS first_event,
				 MAX(time) AS last_event,
				 AVG( CASE WHEN (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 THEN NULL ELSE (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 END ) AS avg_dt,
				 STDDEV( CASE WHEN (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 THEN NULL ELSE (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 END ) AS sdv_dt,
				 MAX( CASE WHEN (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 THEN NULL ELSE (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 END ) AS max_dt,
				 COUNT( CASE WHEN (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 THEN NULL ELSE (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 END ) AS n_dt,
				 SUM( CASE WHEN (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 > 5*60 THEN NULL ELSE (TIMESTAMP_TO_USEC(time) - last_time)/1.0E6 END ) AS sum_dt
			FROM (
			  SELECT
			    *
			  FROM (
			    SELECT
			      username,
			      CASE WHEN event_type = "play_video" THEN 1 ELSE 0 END AS bvideo,
			      CASE WHEN event_type = "problem_check" THEN 1 ELSE 0 END AS bproblem_check,
			      CASE WHEN username != "" THEN 1 ELSE 0 END AS bevent,
			      CASE WHEN REGEXP_MATCH(event_type, "^/courses/{course_id}/discussion/.*") then 1 else 0 end as bforum,
			      CASE WHEN REGEXP_MATCH(event_type, "^/courses/{course_id}/progress") then 1 else 0 end as bprogress,
			      CASE WHEN event_type IN ("show_answer",
				"showanswer") THEN 1 ELSE 0 END AS bshow_answer,
			      CASE WHEN event_type = 'show_transcript' THEN 1 ELSE 0 END AS bshow_transcript,
			      CASE WHEN event_type = 'seq_goto' THEN 1 ELSE 0 END AS bseq_goto,
			      CASE WHEN event_type = 'seek_video' THEN 1 ELSE 0 END AS bseek_video,
			      CASE WHEN event_type = 'pause_video' THEN 1 ELSE 0 END AS bpause_video,
			      # case when event_type = 'edx.course.enrollment.activated' then 1 else 0 end as benroll,
			      # case when event_type = 'edx.course.enrollment.deactivated' then 1 else 0 end as bunenroll
			      time,
			      LAG(time, 1) OVER (PARTITION BY username ORDER BY time) last_time
			    FROM {DATASETS}
			    WHERE
			      NOT event_type CONTAINS "/xblock/"
			      AND username != "" )
		    """


    PCDAY_SQL_VIDEO = """ ,
			  ( # Video events
				  SELECT TIMESTAMP(date) as time,
				         '{course_id}' as course_id,
				         username,
				         video_id,
				         position,
				  FROM [{dataset}.video_stats_day]
				  WHERE TIMESTAMP(date)>= TIMESTAMP("{min_date_start}") and TIMESTAMP(date) <= TIMESTAMP("{max_date_end}")

			  )
                      """
    PCDAY_SQL_ADD = PCDAY_SQL_VIDEO if videoTableExists else ''

    PCDAY_SQL_FORUM = """ ,
			  ( # Forum Events
				   SELECT time,
					  username,
				          '{course_id}' as course_id,
				          thread_id,
				          (CASE WHEN (forum_action == "reply" or forum_action == "comment_reply"
						      or forum_action == "created_thread" or forum_action == "created_response" or forum_action == "created_comment")
						THEN 1 ELSE 0 END) AS write,
					  (CASE WHEN (forum_action == "read" or forum_action == "read_inline") THEN 1 ELSE 0 END) AS read,
				   FROM [{dataset}.forum_events]
				   WHERE (forum_action == "reply" or forum_action == "comment_reply"
					  or forum_action == "created_thread" or forum_action == "created_response" or forum_action == "created_comment"
					  or forum_action == "read" or forum_action == "read_inline")
				          and ( time >= TIMESTAMP("{min_date_start}") and time <= TIMESTAMP("{max_date_end}") )
			  )
                      """
    PCDAY_SQL_ADD = PCDAY_SQL_ADD + PCDAY_SQL_FORUM if forumTableExists else PCDAY_SQL_ADD

    PCDAY_SQL_PROBLEM = """,
			  ( # Problems
				   SELECT pc.username AS username,
				          pp.problem_nid AS problem_nid,
				          pp.n_attempts AS n_attempts,
				          pp.time AS time,
				          '{course_id}' as course_id,
					  pp.ncount_problem_multiplechoice as ncount_problem_multiplechoice,
					  pp.ncount_problem_choice as ncount_problem_choice,
					  pp.ncount_problem_numerical as ncount_problem_numerical,
					  pp.ncount_problem_option as ncount_problem_option,
					  pp.ncount_problem_custom as ncount_problem_custom,
					  pp.ncount_problem_string as ncount_problem_string,
					  pp.ncount_problem_mixed as ncount_problem_mixed,
					  pp.ncount_problem_formula as ncount_problem_formula,
					  pp.ncount_problem_other as ncount_problem_other,
				   FROM (

					   (
					      SELECT PP.user_id as user_id,
						     PP.problem_nid AS problem_nid,
						     PP.n_attempts as n_attempts,
						     PP.date as time,
						     (Case when CP_CA.data_itype == "multiplechoiceresponse" then 1 else 0 end) as ncount_problem_multiplechoice, # Choice
					             (Case when CP_CA.data_itype == "choiceresponse" then 1 else 0 end) as ncount_problem_choice,       # Choice
						     (Case when CP_CA.data_itype == "numericalresponse" then 1 else 0 end) as ncount_problem_numerical, #input
						     (Case when CP_CA.data_itype == "optionresponse" then 1 else 0 end) as ncount_problem_option,       # Choice
					             (Case when CP_CA.data_itype == "customresponse" then 1 else 0 end) as ncount_problem_custom,       # Custom
					             (Case when CP_CA.data_itype == "stringresponse" then 1 else 0 end) as ncount_problem_string,       # Input
					             (Case when CP_CA.data_itype == "mixed" then 1 else 0 end) as ncount_problem_mixed,                 # Mixed
					             (Case when CP_CA.data_itype == "forumula" then 1 else 0 end) as ncount_problem_formula,            # Input
					             (Case when CP_CA.data_itype != "multiplechoiceresponse" and
							        CP_CA.data_itype != "choiceresponse" and
							        CP_CA.data_itype != "numericalresponse" and
							        CP_CA.data_itype != "optionresponse" and
							        CP_CA.data_itype != "customresponse" and
							        CP_CA.data_itype != "stringresponse" and
							        CP_CA.data_itype != "mixed" and
							        CP_CA.data_itype != "forumula"
							   then 1 else 0 end) as ncount_problem_other, # Input
						     #MAX(n_attempts) AS n_attempts,
						     #MAX(date) AS time,
					      FROM [{dataset}.person_problem] PP
					      LEFT JOIN
					      (
							SELECT CP.problem_nid as problem_nid,
							       INTEGER(CP.problem_id) as problem_id,
							       CA.data.itype as data_itype,
						        FROM [{dataset}.course_problem] CP
						        LEFT JOIN [{dataset}.course_axis] CA
						        ON CP.problem_id == CA.url_name
					      ) as CP_CA
					      ON PP.problem_nid == CP_CA.problem_nid
					      GROUP BY time, user_id, problem_nid, n_attempts,
						       ncount_problem_multiplechoice,
						       ncount_problem_choice,
						       ncount_problem_choice,
						       ncount_problem_numerical,
						       ncount_problem_option,
						       ncount_problem_custom,
						       ncount_problem_string,
						       ncount_problem_mixed,
						       ncount_problem_formula,
						       ncount_problem_other
					      )

					      #FROM [{dataset}.person_item] PI
					      #JOIN [{dataset}.course_item] CI
					      #ON PI.item_nid = CI.item_nid
					      #GROUP BY user_id,
						       #problem_nid
					      #ORDER BY
						       #user_id,
						       #problem_nid
					) AS pp
				        LEFT JOIN (
							      SELECT username,
								     user_id
							      FROM [{dataset}.person_course] 
					) AS pc
					ON pc.user_id = pp.user_id
				        WHERE time >= TIMESTAMP("{min_date_start}") and time <= TIMESTAMP("{max_date_end}")
			  )
 
                        """
    PCDAY_SQL_ADD = PCDAY_SQL_ADD + PCDAY_SQL_PROBLEM if problemTableExists else PCDAY_SQL_ADD

    PCDAY_SQL_END = """
			  )
			  WHERE time > TIMESTAMP("{last_date}")
			  GROUP BY course_id,
				   username,
				   date
			  ORDER BY date
		    """


    PCDAY_SQL_NEW = PCDAY_SQL_BASE_SELECT + PCDAY_SQL_VIDEO_SELECT + PCDAY_SQL_FORUM_SELECT + PCDAY_SQL_PROBLEM_SELECT + PCDAY_SQL_MID + PCDAY_SQL_ADD + PCDAY_SQL_END

    PCDAY_SQL = PCDAY_SQL_NEW.format( dataset=dataset, course_id="{course_id}", DATASETS="{DATASETS}", last_date="{last_date}", min_date_start="{min_date_start}", max_date_end="{max_date_end}")

    table = 'person_course_day'

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    print "=== Processing person_course_day for %s (start %s)"  % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    # Major person_course_day schema revision 19-Jan-2016 adds new fields; if table exists, ensure it 
    # has new schema, else force recompute.
    try:
        tinfo = bqutil.get_bq_table_info(dataset, table)
    except Exception as err:
        tinfo = None
    if tinfo:
        fields = tinfo['schema']['fields']
        field_names = [x['name'] for x in fields]
        if not 'nvideos_viewed' in field_names:
            cdt = tinfo['creationTime']
            print "    --> person_course_day created %s; missing nvideos_viewed field in schema; forcing recompute - this may take a long time!" % cdt
            sys.stdout.flush()
            force_recompute = True

    process_tracking_logs.run_query_on_tracking_logs(PCDAY_SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     newer_than=datetime.datetime( 2017, 2, 8, 16, 30 ),
                                                     skip_last_day=skip_last_day)
    
    print "Done with person_course_day for %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
        
#-----------------------------------------------------------------------------

def compute_person_course_day_trlang_table(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    make pcday_trlang_counts for specified course_id
    
    This couse table holds all the (username, course_id, date, transcript_language, count)
    data for a course.

    This is used in computing the modal video transcript language of users

    '''
    table = 'pcday_trlang_counts'

    SQL = '''
		SELECT
		  username,
		  course_id,
		  DATE(time) AS date,
		  LAST(time) AS last_time,
		  resource,
		  CASE
		    WHEN resource_event_type = 'transcript_download' AND prev_event_data IS NOT NULL THEN prev_event_data
		    WHEN resource_event_type = 'transcript_download'
		  AND prev_event_data IS NULL THEN 'en'
		    WHEN resource_event_type = 'transcript_language' THEN resource_event_data
		    ELSE resource_event_data
		  END AS resource_event_data,
		  resource_event_type,
		  SUM(lang_count) AS langcount
		FROM (
		  SELECT
		    course_id,
		    username,
		    resource,
		    resource_event_data,
		    resource_event_type,
		    LAG(time, 1) OVER (PARTITION BY username ORDER BY time) AS prev_time,
		    LAG(resource_event_type, 1) OVER (PARTITION BY username ORDER BY time) AS prev_event_type,
		    LAG(resource_event_data, 1) OVER (PARTITION BY username ORDER BY time) AS prev_event_data,
		    time,
		    COUNT(*) AS lang_count,
		    event_type
		  FROM (
		    SELECT
		      time,
		      course_id,
		      username,
		      'video' AS resource,
		      CASE
			WHEN module_id IS NOT NULL THEN REGEXP_EXTRACT(module_id, r'.*video/(.*)')                          # Newer data .. May 2016?
			ELSE REGEXP_EXTRACT(event_type, r'.*;_video;_(.*)\/handler\/transcript\/translation\/.*') END   # Older data
		      AS resource_id,
		      CASE
			WHEN (event_type = 'edx.ui.lms.link_clicked'                                                     # Trasncript Download
			AND REGEXP_MATCH(JSON_EXTRACT(event, '$.target_url'), r'(.*handler/transcript/download)') ) THEN 'transcript_download'
			WHEN (REGEXP_MATCH(event_type, "/transcript/translation/.*") ) THEN 'transcript_language'
			ELSE NULL
		      END AS resource_event_type,
		      REGEXP_EXTRACT(event_type, r'.*\/handler\/transcript\/translation\/(.*)') AS resource_event_data,
		      event_type,
		    FROM {DATASETS}
		    WHERE
		      time > TIMESTAMP("2010-10-01 01:02:03")
		      AND username != ""
		      AND ( (NOT event_type CONTAINS "/xblock/"                                                          # Trasncript Download
			  AND event_type = 'edx.ui.lms.link_clicked'                                                  # Trasncript Download
			  AND REGEXP_MATCH(JSON_EXTRACT(event, '$.target_url'), r'(.*handler/transcript/download)') ) # Transcript Download
			OR (REGEXP_MATCH(event_type, "/transcript/translation/.*") )                                   # Transcript Language Toggle
			)
		    ORDER BY
		      time )
		  GROUP BY
		    username,
		    course_id,
		    resource,
		    resource_event_data,
		    resource_event_type,
		    event_type,
		    time )
		GROUP BY
		  username,
		  course_id,
		  date,
		  resource,
		  resource_event_data,
		  resource_event_type,
		ORDER BY
		  date ASC,
		  username ASC
		  '''

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf)


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


    
