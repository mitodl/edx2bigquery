#!/usr/bin/python
'''
Forum Usage Analysis
'''

import os, sys
import csv
import re
import json
import gsutil
import bqutil
import datetime
import process_tracking_logs

from path import path
from collections import OrderedDict
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, openfile

import re
from time import sleep
import urllib2
import json
import os
import datetime
import gzip

#-----------------------------------------------------------------------------
# CONSTANTS
#-----------------------------------------------------------------------------

TABLE_FORUM_EVENTS = "forum_events"

#-----------------------------------------------------------------------------
# METHODS
#-----------------------------------------------------------------------------

def AnalyzeForums( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None):
    '''
    Run full analysis on forums, including creation of forum_events table.
    '''
    CreateForumEvents(course_id, force_recompute, use_dataset_latest, skip_last_day, end_date)

def CreateForumEvents( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None):
    '''
    Create forum events table, based on tracking logs.  Extracts all forum-related events, including forum post reads,
    into the date-time ordered table.  Repeated calls to this procedure will append new events to the table.  If no
    new events are found, the existing table is left unchanged.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    logs = bqutil.course_id2dataset(course_id, dtype='logs')

    table = TABLE_FORUM_EVENTS
    
    # event_type for forums may be like:
    #  /courses/UnivX/123.4x/2T2015/discussion/forum/The_Subject/threads/5460c918a2a525003a0007fa
    #  /courses/UnivX/123.4x/2T2015/discussion/forum/The_Subject/inline
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/545e4f5da2a5251aac000672/reply
    #  /courses/UnivX/123.4x/2T2015/discussion/forum/users/4051854/followed
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/545770e9dad66c17cd0001d5/upvote
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/545770e9dad66c17cd0001d5/unvote
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/5447c22e892b213c7b0001f3/update
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/54493025892b2120a1000335/pin
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/54492e9c35c79cb03e00030c/delete
    #  /courses/UnivX/123.4x/2T2015/discussion/forum/General/inline
    #  /courses/UnivX/123.4x/2T2015/instructor/api/list_forum_members
    #  /courses/UnivX/123.4x/2T2015/instructor/api/update_forum_role_membership
    #     \"GET\": {\"action\": [\"allow\"], \"rolename\": [\"Administrator\"], \"unique_student_identifier\": [\"NEW_ADMIN_USER\"]}}"}
    #
    # module_id will be like:
    # "module_id": "UnivX/123.4x/forum/54492f0c892b21597e00030a"

    the_sql = """
              SELECT time, 
                     username,
                     '{course_id}' as course_id,
                     (case when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/reply') then "reply"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/upvote') then "upvote"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unvote') then "unvote"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/update') then "update"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/delete') then "delete"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/close') then "close"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/follow') then "follow_thread"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unfollow') then "unfollow_thread"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/pin') then "pin"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unpin') then "unpin"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/downvote') then "downvote"  # does this happen?
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/users/[^/]+/followed') then "follow_user"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/users/[^/]+$') then "target_user"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/[^/]+/threads/[^/]+') then "read"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/[^/]+/inline') then "read_inline"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/search') then "search"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum$') then "enter_forum"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/$') then "enter_forum"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/instructor/api/(.*)') then REGEXP_EXTRACT(event_type, r'/courses/.*/instructor/api/(.*)')
                           when event_type = "edx.forum.thread.created" then "created_thread"
                           when event_type = "edx.forum.response.created" then "created_response"
                           when event_type = "edx.forum.comment.created" then "created_comment"
                           when event_type = "edx.forum.searched" then "searched"
                           else event_type end) as forum_action,
                     REGEXP_EXTRACT(module_id, r'[^/]+/[^/]+/forum/([^/]+)') as thread_id,
                     REGEXP_EXTRACT(event_type, r'/courses/.*/forum/([^/]+)/') as subject,
                     REGEXP_EXTRACT(event_type, r'/courses/.*/forum/users/([^/]+)') as target_user_id,
                     event_struct.query as search_query,   # unavailable before June 1, 2015
                     event_struct.GET as event_GET,        # unavailable before June 1, 2015
              FROM {DATASETS}
              WHERE  (REGEXP_MATCH(event_type ,r'^edx\.forum\..*')
                      or event_type contains "/discussion/forum"
                      or event_type contains "/discussion/threads"
                      or event_type contains "list-forum-"
                      or event_type contains "list_forum_"
                      or event_type contains "add-forum-"
                      or event_type contains "add_forum_"
                      or event_type contains "remove-forum-"
                      or event_type contains "remove_forum_"
                      or event_type contains "update_forum_"
                     ) 
                    AND username is not null
                    AND event is not null
                    and time > TIMESTAMP("{last_date}")
                    {hash_limit}
              order by time
              """
    try:
        tinfo = bqutil.get_bq_table_info(dataset, table )
        assert tinfo is not None, "[make_forum_analysis] Creating %s.%s table for %s" % (dataset, table, course_id)

        print "[make_forum_analysis] Appending latest data to %s.%s table for %s" % (dataset, table, course_id)
        sys.stdout.flush()

    except (AssertionError, Exception) as err:
        print str(err)
        sys.stdout.flush()
        print " --> Missing %s.%s?  Attempting to create..." % ( dataset, table )
        sys.stdout.flush()
        pass

    print "=== Processing Forum Events for %s (start %s)"  % (course_id, datetime.datetime.now())
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

    print "Done with Forum Events for %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
