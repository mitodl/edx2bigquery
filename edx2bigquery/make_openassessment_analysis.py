#!/usr/bin/python
#
# Analyze tracking logs specifically for openassessment problems.
#
# Extract from tracking logs all openassessment events and append to the ora_events table.
#

import os, sys
import csv
import re
import json
import gsutil
import bqutil
import datetime
import process_tracking_logs

try:
	from path import Path as path
except:
	from path import path

from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, openfile

def get_ora_events(course_id, force_recompute=False, use_dataset_latest=False, end_date=None):
    '''
    make ora_events table for specified course_id.

    The master table holds all the openassessment events extracted from
    the tracking logs for a course.  It isn't split into separate
    days.  It is ordered in time, however.  To update it, a new day's logs
    are processed, then the results appended to this table.

    If the ora_events table doesn't exist, then run it once on all
    the existing tracking logs.  
    '''
    
    SQL = """
        SELECT 
           (case when module_id_sometimes is not null then REGEXP_EXTRACT(module_id_sometimes, ".*/([^/]+)")
                when submission_uuid is not null then REGEXP_EXTRACT(last_module_id, ".*/([^/]+)")
                end) as problem_id,
           *,
           (case when module_id_sometimes is not null then module_id_sometimes
                when submission_uuid is not null then last_module_id
                end) as module_id,
        FROM        
          (SELECT 
             *,
             # must aggregate to get module_id, which is not available for some events, like peer_assess
             # this means module_id may be missing, e.g. if the creation event happens in a different day's tracking log
             NTH_VALUE(module_id_sometimes,1) 
                 over (partition by submission_uuid order by module_id_sometimes desc) last_module_id,
           from
               (SELECT 
                   time, 
                   username,
                   '{course_id}' as course_id,
               (case when REGEXP_MATCH(event_type, "openassessmentblock..*") then 
                          REGEXP_EXTRACT(event_type, "openassessmentblock.(.*)")
                     when REGEXP_MATCH(event_type, "/courses/.*") then
                          REGEXP_EXTRACT(event_type, "/courses/.*/(handler/.*)")
                     end) as action,
               (case when event_type = "openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                     when event_type = "openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                     when event_type = "openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                     when event_type = "openassessmentblock.get_peer_submission" then JSON_EXTRACT_SCALAR(event, "$.submission_returned_uuid")
                    end) as submission_uuid,
               (case when event_type="openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].option.points")
                     when event_type="openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].option.points")
                    end) as part1_points,
               (case when event_type="openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].feedback")
                     when event_type="openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].feedback")
                    end) as part1_feedback,
               (case when event_type="openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.attempt_number")
                    end) as attempt_num,
               (case 
                     when event_type="openassessmentblock.get_peer_submission" then JSON_EXTRACT_SCALAR(event, "$.item_id")
                     when event_type="openassessmentblock.create_submission" then 
                                  REGEXP_EXTRACT( JSON_EXTRACT_SCALAR(event, "$.answer.file_key"), ".*(i4x://.*)")
                     when REGEXP_MATCH(event_type, "/courses/.*/xblock/i4x:.*") then
                                  REGEXP_REPLACE( REGEXP_EXTRACT( event_type, "/courses/.*/xblock/(i4x:;_[^/]+)/handler.*" ),
                                                  ";_", "/")
                     end) as module_id_sometimes,
               event_type,
               event,
               event_source,
               (case when event_type="openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.answer.text")
                    end) as answer_text,
               from {DATASETS}
               where event_type contains "openassessment"
                  and event != '{{"POST": {{}}, "GET": {{}}}}'	# disregard if user did nothing but look at the page
                  and time > TIMESTAMP("{last_date}")
               order by time
             )
          order by time
         )
        order by time
            """

    table = 'ora_events'

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     get_date_function=gdf,
                                                     days_delta=0)
        
