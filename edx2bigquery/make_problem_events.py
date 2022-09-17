#!/usr/bin/python
'''
Problem Event Analysis
'''

import os, sys
# import csv
import unicodecsv as csv
import re
import json
from . import gsutil
from . import bqutil
import datetime
from . import process_tracking_logs

#-----------------------------------------------------------------------------
# CONSTANTS
#-----------------------------------------------------------------------------

TABLE_PROBLEM_EVENTS = "problem_events"

#-----------------------------------------------------------------------------
# METHODS
#-----------------------------------------------------------------------------

def ExtractProblemEvents( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None):
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = TABLE_PROBLEM_EVENTS
    the_sql = """
SELECT  
    context.user_id as user_id, 
    time,
    event_source,
    REGEXP_EXTRACT(
      (CASE when module_id is not null then module_id 
          when event_type contains "/xblock/i4x:;_" then REPLACE(REGEXP_EXTRACT(event_type, r"i4x:;_;_(.*)/handler/xmodule"),";_", "/")
          else REPLACE(event_struct.problem, "i4x://", "")
          end),
      "[^/]+/problem/([^/]+)") as problem_url,
    (CASE when event_type contains "/xblock/i4x:;_" then REGEXP_EXTRACT(event_type, r"xmodule_handler/(.[^/]+)")
          when event_type contains "type@problem+block" then REGEXP_EXTRACT(event_type, r"xmodule_handler/(.[^/]+)")
          else event_type
          end) as event_type,
   event_struct.attempts as attempts,
   event_struct.success as success,
   event_struct.grade as grade,          
FROM {DATASETS}
WHERE       
   ( REGEXP_MATCH(event_type, r'problem_\w+') 
     OR event_type = "showanswer"
   )
   AND context.user_id is not null
   and time > TIMESTAMP("{last_date}")
   {hash_limit}
order by user_id, time
    """

    try:
        tinfo = bqutil.get_bq_table_info(dataset, table )
        assert tinfo is not None, "[make_problem_events] Creating %s.%s table for %s" % (dataset, table, course_id)

        print("[make_problem_events] Appending latest data to %s.%s table for %s" % (dataset, table, course_id))
        sys.stdout.flush()

    except (AssertionError, Exception) as err:
        print(str(err))
        sys.stdout.flush()
        print(" --> Missing %s.%s?  Attempting to create..." % ( dataset, table ))
        sys.stdout.flush()
        pass

    print("=== Processing Forum Events for %s (start %s)"  % (course_id, datetime.datetime.now()))
    sys.stdout.flush()

    def gdf(row):
        return datetime.datetime.utcfromtimestamp(float(row['time']))

    process_tracking_logs.run_query_on_tracking_logs(the_sql, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     get_date_function=gdf,
                                                     has_hash_limit=True,
                                                     end_date=end_date,
                                                     skip_last_day=skip_last_day
                                                    )

    print("Done with Problem Events for %s (end %s)"  % (course_id, datetime.datetime.now()))
    print("="*77)
    sys.stdout.flush()
