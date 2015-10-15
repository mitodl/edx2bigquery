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
import math

#-----------------------------------------------------------------------------
# CONSTANTS
#-----------------------------------------------------------------------------

TABLE_FORUM_EVENTS = "forum_events"
TABLE_FORUM_POSTS = "forum_posts"
TABLE_FORUM_PERSON = "forum_person"
TABLE_FORUM = "forum"
TABLE_PERSON_COURSE = "person_course"

POST_PREVIEW_CHAR_COUNT = 100
HASH = 2

#-----------------------------------------------------------------------------
# METHODS
#-----------------------------------------------------------------------------

def AnalyzeForums( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None):
    '''
    Run full analysis on forums, including creation of forum_events table.
    '''
    CreateForumEvents( course_id, force_recompute, use_dataset_latest, skip_last_day, end_date )
    CreateForumPosts( course_id, force_recompute, use_dataset_latest, skip_last_day, end_date )
    CreateForumPerson( course_id, force_recompute, use_dataset_latest, skip_last_day, end_date )


def CreateForumPosts( course_id, force_recompute=True, use_dataset_latest=False, skip_last_day=False, end_date=None):
    '''
    Create Forum posts table, based on forum data. Categorizes forum posts as initial_post, response_post or comment.
    Also extracts first 100 characters of the post content as a preview.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = TABLE_FORUM_POSTS

    the_sql = """
                        SELECT * FROM
                        (
                             SELECT ADDED_TITLE.FA.username as username,
				    "{course_id}" as course_id,
                                    ADDED_TITLE.FA.slug_id as slug_id,
                                    ADDED_TITLE.FA.slug_type as slug_type,
                                    ADDED_TITLE.FA.thread_id as thread_id,
                                    ADDED_TITLE.FA.parent_id as parent_id,
                                    ADDED_TITLE.IP.username as original_poster,
                                    ADD_RESPOND_TO.username as responded_to,
                                    ADDED_TITLE.IP.title as title,
                                    ADDED_TITLE.FA.first_time as first_time,
                                    ADDED_TITLE.FA.body_preview as body_preview,
                             FROM 
                             (
                                  SELECT * FROM 
                                  (
                                       SELECT author_username as username,
                                              mongoid as slug_id,
                                              (case when _type = "Comment" and comment_thread_id is not null and parent_id is null and mongoid is not null then "response_post" else
                                                (case when _type = "Comment" and parent_id is not null then "comment" else null end) end) as slug_type,
                                              comment_thread_id as thread_id,
                                              parent_id,
                                              title,
                                              created_at as first_time,
                                              SUBSTR(body, 0, {post_preview_char_count}) as body_preview,
                                       FROM [{dataset}.{forum}]
                                       WHERE (_type = "Comment" and parent_id is not null)
                                  ) as FA # 3rd level comment
                                  LEFT JOIN EACH
                                  ( 
                                       SELECT * FROM
                                       ( 
                                            SELECT author_username as username,
                                                   mongoid as slug_id,
                                                   (case when _type = "Comment" and comment_thread_id is not null and parent_id is null and mongoid is not null then "response_post" else
                                                     (case when _type = "Comment" and parent_id is not null then "comment" else null end) end) as slug_type,
                                                   comment_thread_id as thread_id,
                                                   parent_id,
                                                   title,
                                                   created_at as first_time,
                                                   SUBSTR(body, 0, {post_preview_char_count}) as body_preview,
                                            FROM [{dataset}.{forum}]
                                            WHERE (_type = "CommentThread" and comment_thread_id is null and parent_id is null and mongoid is not null)
                                       )
                                  ) as IP
                                  ON FA.thread_id = IP.slug_id
                             ) as ADDED_TITLE
                             LEFT JOIN EACH
                             (
                                  SELECT author_username as username,
                                         mongoid as slug_id,
                                         comment_thread_id as thread_id,
                                         parent_id,
                                  FROM [{dataset}.{forum}]
                             ) as ADD_RESPOND_TO
                             ON ADDED_TITLE.FA.parent_id = ADD_RESPOND_TO.slug_id
                             WHERE ADDED_TITLE.FA.slug_type = "comment"
                        ) as RC,
                        (
                             SELECT FA.username as username,
				    "{course_id}" as course_id,
                                    FA.slug_id as slug_id,
                                    FA.slug_type as slug_type,
                                    FA.thread_id as thread_id,
                                    FA.parent_id as parent_id,
                                    IP.username as original_poster,
                                    IP.title as title,
                                    FA.first_time as first_time,
                                    FA.body_preview as body_preview
                             FROM 
                             (
                                  SELECT author_username as username,
                                         mongoid as slug_id,
                                         (case when _type = "Comment" and comment_thread_id is not null and parent_id is null and mongoid is not null then "response_post" else
                                           (case when _type = "Comment" and parent_id is not null then "comment" else null end) end) as slug_type,
                                         comment_thread_id as thread_id,
                                         parent_id,
                                         title,
                                         created_at as first_time,
                                         SUBSTR(body, 0, {post_preview_char_count}) as body_preview,
                                  FROM [{dataset}.{forum}]
                                  WHERE (_type = "Comment" and comment_thread_id is not null and parent_id is null and mongoid is not null)
                             ) as FA # 2nd level comment
                             LEFT JOIN EACH
                             (
                                  SELECT * FROM 
                                  (    
                                       SELECT author_username as username,
                                              mongoid as slug_id,
                                              (case when _type = "Comment" and comment_thread_id is not null and parent_id is null and mongoid is not null then "response_post" else
                                                (case when _type = "Comment" and parent_id is not null then "comment" else null end) end) as slug_type,
                                              comment_thread_id as thread_id,
                                              parent_id,
                                              title,
                                              created_at as first_time,
                                              SUBSTR(body, 0, {post_preview_char_count}) as body_preview,
                                       FROM [{dataset}.{forum}]
                                       WHERE (_type = "CommentThread" and comment_thread_id is null and parent_id is null and mongoid is not null)
                                  )
                             ) as IP
                             ON FA.thread_id = IP.slug_id
                        ) as RC2,
                        (
                             SELECT * FROM
                             (
                                  SELECT author_username as username,
				         "{course_id}" as course_id,
                                         mongoid as slug_id,
                                         (case when _type = "CommentThread" and comment_thread_id is null and parent_id is null and mongoid is not null then "initial_post" end) as slug_type,
                                         comment_thread_id as thread_id,
                                         parent_id,
                                         title,
                                         created_at as first_time,
                                         SUBSTR(body, 0, {post_preview_char_count}) as body_preview,
                                  FROM [{dataset}.{forum}]
                                  WHERE (_type = "CommentThread" and comment_thread_id is null and parent_id is null and mongoid is not null)
                             )
                        ) as NA
              """.format( dataset=dataset, course_id=course_id, forum=TABLE_FORUM, post_preview_char_count=POST_PREVIEW_CHAR_COUNT )

    print "[make_forum_analysis] Creating %s.%s table for %s" % ( dataset, TABLE_FORUM_POSTS, course_id )
    sys.stdout.flush()

    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_FORUM )
        assert tinfo is not None, "[make_forum_analysis] %s table depends on %s, which does not exist" % ( TABLE_FORUM_POSTS, TABLE_FORUM )

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s?  Skipping creation of %s" % ( dataset, TABLE_FORUM, TABLE_FORUM_POSTS )
        sys.stdout.flush()
        return

    bqdat = bqutil.get_bq_table(dataset, table, the_sql, force_query=force_recompute,
                                depends_on=["%s.%s" % (dataset, TABLE_FORUM)],
                                )

    return bqdat

def CreateForumPerson( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None, has_hash_limit=False, hash_limit=HASH ):
    '''
    Create Forum Person table, based on forum events and forum posts tables. 
    This table contains both read and writes for all forum posts, for all users.
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = TABLE_FORUM_PERSON

    original_the_sql = """

                  SELECT (case when PP.username_fp is not null then PP.username_fp else FE.username_fe end) as username,
			 "{course_id}" as course_id,
                         (case when PP.username_fp is not null then PP.slug_id else FE.slug_id end) as slug_id,
                         (case when PP.username_fp is not null then PP.slug_type else FE.slug_type end) as slug_type,
                         (case when PP.username_fp is not null then PP.thread_id else FE.thread_id end) as thread_id,
                         (case when PP.username_fp is not null then PP.parent_id else FE.parent_id end) as parent_id,
                         (case when PP.original_poster is not null then PP.original_poster else FE.original_poster end) as original_poster,
                         (case when PP.responded_to is not null then PP.responded_to else FE.responded_to end) as responded_to,
                         (case when PP.username_fp is not null then PP.title else FE.title end) as title,
                         (case when PP.username_fp is not null then PP.wrote else 0 end) as wrote,
                         FE.read as read,
                         FE.pin as pinned,
                         FE.upvote as upvoted,
                         FE.unvote as unvoted,
                         #FE.del as deleted,
                         FE.follow as followed,
                         (case when PP.first_time is not null and FE.last_time is not null and (TIMESTAMP(PP.first_time) <= TIMESTAMP(FE.last_time))
                                   then TIMESTAMP(PP.first_time)
                               else (case when PP.first_time is not null and FE.last_time is null
                                     then TIMESTAMP(PP.first_time) else
                                         (case when FE.first_time is not null
                                               then TIMESTAMP(FE.first_time)
                                              else FE.last_time end) end) end) as first_time,
                         (case when PP.first_time is not null and FE.last_time is not null and (TIMESTAMP(PP.first_time) >= TIMESTAMP(FE.last_time))
                                   then TIMESTAMP(PP.first_time)
                               else (case when PP.first_time is not null and FE.last_time is null
                                     then TIMESTAMP(PP.first_time) else
                                         (case when FE.last_time is not null
                                               then TIMESTAMP(FE.last_time)
                                              else FE.first_time end) end) end) as last_time,


                  FROM
                  (
                          # Find 1st level posting => "original_post"
                          SELECT username as username_fp,
                                 slug_id,
                                 slug_type,
                                 thread_id,
                                 parent_id,
                                 original_poster,
                                 responded_to,
                                 title,
                                 1 as wrote,
                                 #created_at as first_time,
                                 first_time
                          FROM [{dataset}.{forum_posts}]
                          {hash_limit_where}
                          ORDER by username_fp, first_time
                  ) PP
                  FULL OUTER JOIN EACH
                  (
                          SELECT username as username_fe, 
                                 MIN(TIMESTAMP(time)) as first_time,
                                 MAX(TIMESTAMP(time)) as last_time,
                                 slug_id,
                                 FE.thread_id as thread_id,
                                 FIRST(parent_id) as parent_id,
				 F.slug_type as slug_type,
				 F.original_poster as original_poster,
				 F.responded_to as responded_to,
				 F.title as title,
                                 #1 as read,
                                 sum(case when forum_action = "read" or forum_action = "read_inline" then 1 else 0 end) as read,
                                 sum(case when forum_action = "pin" then 1 else 0 end) as pin,
                                 sum(case when forum_action = "upvote" then 1 else 0 end) as upvote,
                                 sum(case when forum_action = "unvote" then 1 else 0 end) as unvote,
                                 #sum(case when forum_action = "delete" then 1 else 0 end) as del,
                                 sum(case when forum_action = "follow_thread" then 1 else 0 end) as follow,      
                          FROM [{dataset}.{forum_events}] FE
                          JOIN EACH 
                          (
                                 SELECT username as username_fe,
                                        slug_id,
                                        slug_type,
                                        thread_id,
                                        parent_id,
                                        original_poster,
                                        responded_to,
                                        title,
                                        first_time,
                                 FROM [{dataset}.{forum_posts}]
                                 {hash_limit_where}
                          ) as F
                          ON F.thread_id = FE.thread_id
                          WHERE ((FE.forum_action = "read") or 
                                (FE.forum_action = "read_inline") or
                                (FE.forum_action = "pin") or 
                                (FE.forum_action = "upvote") or 
                                (FE.forum_action = "unvote") or
                                #(FE.forum_action = "delete") or
                                (FE.forum_action = "follow_thread"))
                                {hash_limit_and}
                          GROUP BY username_fe, slug_id, thread_id, slug_type, original_poster, responded_to, title
                  ) as FE
                  ON (PP.username_fp = FE.username_fe) AND (PP.slug_id = FE.slug_id)
                  WHERE (PP.username_fp is not null and PP.username_fp != '') or (FE.username_fe is not null and FE.username_fe != '')

              """

    the_sql = original_the_sql.format( dataset=dataset, course_id=course_id, forum=TABLE_FORUM, forum_posts=TABLE_FORUM_POSTS, forum_events=TABLE_FORUM_EVENTS, hash_limit_and='', hash_limit_where='' )

    print "[make_forum_analysis] Creating %s.%s table for %s" % (dataset, TABLE_FORUM_PERSON, course_id)
    sys.stdout.flush()

    try:

        tinfo_fe = bqutil.get_bq_table_info( dataset, TABLE_FORUM_EVENTS )
        trows_fe = int(tinfo_fe['numRows'])
	print "[make_forum_analysis] %s Forum Events found " % trows_fe
        tinfo_fp = bqutil.get_bq_table_info( dataset, TABLE_FORUM_POSTS )
        trows_fp = int(tinfo_fp['numRows'])
	print "[make_forum_analysis] %s Forum Posts found " % trows_fp

        assert tinfo_fe is not None and trows_fe != 0, "[make_forum_analysis] %s table depends on %s, which does not exist" % ( TABLE_FORUM_PERSON, TABLE_FORUM_EVENTS )
        assert tinfo_fp is not None and trows_fp != 0, "[make_forum_analysis] %s table depends on %s, which does not exist" % ( TABLE_FORUM_PERSON, TABLE_FORUM_POSTS ) 

    except (AssertionError, Exception) as err:

        print " --> Err: missing %s.%s and/or %s (including 0 rows in table)?  Skipping creation of %s" % ( dataset, TABLE_FORUM_POSTS, TABLE_FORUM_EVENTS, TABLE_FORUM_PERSON )
        sys.stdout.flush()
        return

    # Now try to create table
    try:

        if has_hash_limit:

            overwrite = True
            hash_limit = int( hash_limit )
            for k in range( hash_limit ):
                hash_limit_where = "WHERE ABS(HASH(username)) %% %d = %d" % ( hash_limit, k )
                hash_limit_and = "and ABS(HASH(username)) %% %d = %d" % ( hash_limit, k )

                retry_the_sql = original_the_sql.format( dataset=dataset, forum=TABLE_FORUM, forum_posts=TABLE_FORUM_POSTS, forum_events=TABLE_FORUM_EVENTS, hash_limit_and=hash_limit_and, hash_limit_where=hash_limit_where )
                print "[make_forum_analysis] Retrying with this query...", retry_the_sql
                sys.stdout.flush()
                bqutil.create_bq_table( dataset, table, retry_the_sql, wait=True, overwrite=overwrite, allowLargeResults=True )
                overwrite = "append"

        else:

	    overwrite = True
            bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite, allowLargeResults=True)

    except Exception as err:

        has_hash_limit = True
        if ( (('Response too large to return.' in str(err)) or ('Resources exceeded during query execution' in str(err))) and has_hash_limit ):

            # 'Resources exceeded during query execution'
            # try using hash limit on username
            # e.g. WHERE ABS(HASH(username)) % 4 = 0
            print '[make_forum_analysis] Response too large to return. Attempting to break down into multiple queries and append instead... using hash of %s' % hash_limit

            try:

                for k in range( hash_limit ):

		    hash_limit_where = "WHERE ABS(HASH(username)) %% %d = %d" % ( hash_limit, k )
		    hash_limit_and = "and ABS(HASH(username)) %% %d = %d" % ( hash_limit, k )

                    retry_the_sql = original_the_sql.format( dataset=dataset, forum=TABLE_FORUM, forum_posts=TABLE_FORUM_POSTS, forum_events=TABLE_FORUM_EVENTS, hash_limit_and=hash_limit_and, hash_limit_where=hash_limit_where )
                    print "[make_forum_analysis] Retrying with this query...", retry_the_sql
                    sys.stdout.flush()
                    bqutil.create_bq_table( dataset, table, retry_the_sql, wait=True, overwrite=overwrite, allowLargeResults=True )
                    overwrite = "append"
  
            except Exception as err:

                if ( (('Response too large to return.' in str(err)) or ('Resources exceeded during query execution' in str(err))) and has_hash_limit ):

                    hash_limit = int( hash_limit * 2.0 )
                    print '[make_forum_analysis] Response too large to return. Attempting to break down into multiple queries and append instead... using hash of %s' % hash_limit
                    CreateForumPerson( course_id, force_recompute, use_dataset_latest, skip_last_day, end_date, has_hash_limit=True, hash_limit=hash_limit )

                else:

                    print '[make_forum_analysis] An error occurred with this query: %s' % the_sql
                    raise

        else:

	    print '[make_forum_analysis] An error occurred with this query: %s' % the_sql
            raise

    print "Done with Forum Person for %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()

    return

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
    #  /courses/UnivX/123.4x/2T2015/discussion/forum/users/4051854/followed
    #  /courses/UnivX/123.4x/2T2015/discussion/comments/54593f21a2a525003a000351/reply
    #  /courses/UnivX/123.4x/2T2015/discussion/threads/545e4f5da2a5251aac000672/reply
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
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/comments/[^/]+/reply') then "comment_reply"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/comments/[^/]+/upvote') then "comment_upvote"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/comments/[^/]+/update') then "comment_update"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/comments/[^/]+/unvote') then "comment_unvote"
                           when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/comments/[^/]+/delete') then "comment_delete"
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
                           (case when module_id is not null then REGEXP_EXTRACT(module_id, r'[^/]+/[^/]+/forum/([^/]+)') # For old-school courses with transparent course ids
                                      else (case when module_id is null # otherwise, for new opaque course ids, use regex to find thread_id from event_type, since module_id is null
                                               then (case when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/forum/[^/]+/threads/[^/]+') then REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/forum/[^/]+/threads/([^/]+)') # read
                                                      else (case when REGEXP_MATCH(event_type, r'/courses/(.*)/discussion/threads/[^/]+/') then REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/threads/([^/]+)') # upvote, pinned, upvoted, unvoted, deleted, followed
                                                             else REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/comments/([^/]+)/') end) # comment
                                                                end) end) end) as thread_id,
                     REGEXP_EXTRACT(event_type, r'/courses/.*/forum/([^/]+)/') as subject,
                     REGEXP_EXTRACT(event_type, r'/courses/.*/forum/users/([^/]+)') as target_user_id,
                     event_struct.query as search_query,   # unavailable before June 1, 2015
                     event_struct.GET as event_GET,        # unavailable before June 1, 2015
              FROM {DATASETS}
              WHERE  (REGEXP_MATCH(event_type ,r'^edx\.forum\..*')
                      or event_type contains "/discussion/forum"
                      or event_type contains "/discussion/threads"
                      or event_type contains "/discussion/comments"
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
