#!/usr/bin/python
'''
Create Video Statistics
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
from unidecode import unidecode

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

VIDEO_LENGTH = 'video_length'
VIDEO_ID = 'youtube_id'

YOUTUBE_PARTS = "contentDetails,statistics"
MIN_IN_SECS = 60
HOURS_IN_SECS = MIN_IN_SECS * 60
DAYS_IN_SECS = HOURS_IN_SECS * 24
WEEKS_IN_SECS = DAYS_IN_SECS * 7
MONTHS_IN_SECS = WEEKS_IN_SECS * 4
YEAR_IN_SECS = MONTHS_IN_SECS * 12

TABLE_VIDEO_STATS = 'video_stats'
TABLE_VIDEO_STATS_PER_DAY = 'video_stats_day'
TABLE_VIDEO_AXIS = 'video_axis'
TABLE_COURSE_AXIS = 'course_axis'
TABLE_PERSON_COURSE_VIDEO_WATCHED = "person_course_video_watched"

FILENAME_VIDEO_AXIS = TABLE_VIDEO_AXIS + ".json.gz"

SCHEMA_VIDEO_AXIS = 'schemas/schema_video_axis.json'
SCHEMA_VIDEO_AXIS_NAME = 'video_axis'

DATE_DEFAULT_START = '20120101'
DATE_DEFAULT_END = datetime.datetime.today().strftime("%Y%m%d")
DATE_DEFAULT_END_NEW = datetime.datetime.today().strftime("%Y-%m-%d")

#-----------------------------------------------------------------------------
# METHODS
#-----------------------------------------------------------------------------

def analyze_videos(course_id, api_key=None, basedir=None, 
                   datedir=None, force_recompute=False,
                   use_dataset_latest=False,
                   use_latest_sql_dir=False,
               ):

    make_video_stats(course_id, api_key, basedir, datedir, force_recompute, use_dataset_latest, use_latest_sql_dir)
    pass # Add new video stat methods here

def make_video_stats(course_id, api_key, basedir, datedir, force_recompute, use_dataset_latest, use_latest_sql_dir):
    '''
    Create Video stats for Videos Viewed and Videos Watched.
    First create a video axis, based on course axis. Then use tracking logs to count up videos viewed and videos watched
    '''

    assert api_key is not None, "[analyze videos]: Public API Key is missing from configuration file. Visit https://developers.google.com/console/help/new/#generatingdevkeys for details on how to generate public key, and then add to edx2bigquery_config.py as API_KEY variable"

    # Get Course Dir path
    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest or use_latest_sql_dir)
    
    # get schema
    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/%s' % ( mypath, SCHEMA_VIDEO_AXIS )
    the_schema = json.loads(open(SCHEMA_FILE).read())[ SCHEMA_VIDEO_AXIS_NAME ]
    the_dict_schema = schema2dict(the_schema)

    # Create initial video axis
    videoAxisExists = False
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    va_date = None
    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_VIDEO_AXIS )
        assert tinfo is not None, "[analyze videos] %s.%s does not exist. First time creating table" % ( dataset, TABLE_VIDEO_AXIS )
	videoAxisExists = True
        va_date = tinfo['lastModifiedTime']		# datetime
    except (AssertionError, Exception) as err:
        print "%s --> Attempting to process %s table" % ( str(err), TABLE_VIDEO_AXIS )
        sys.stdout.flush()

    # get course axis time
    ca_date = None
    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_COURSE_AXIS )
        ca_date = tinfo['lastModifiedTime']		# datetime
    except (AssertionError, Exception) as err:
        pass

    if videoAxisExists and (not force_recompute) and ca_date and va_date and (ca_date > va_date):
        force_recompute = True
        print "video_axis exists, but has date %s, older than course_axis date %s; forcing recompute" % (va_date, ca_date)
        sys.stdout.flush()

    if not videoAxisExists or force_recompute:
        force_recompute = True
        createVideoAxis(course_id=course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)

        # Get video lengths
        va = bqutil.get_table_data(dataset, TABLE_VIDEO_AXIS)
        assert va is not None, "[analyze videos] Possibly no data in video axis table. Check course axis table"
        va_bqdata = va['data']
        fileoutput = lfp / FILENAME_VIDEO_AXIS
        getYoutubeDurations( dataset=dataset, bq_table_input=va_bqdata, api_key=api_key, outputfilename=fileoutput, schema=the_dict_schema, force_recompute=force_recompute )

        # upload and import video axis
        gsfn = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest) / FILENAME_VIDEO_AXIS
        gsutil.upload_file_to_gs(fileoutput, gsfn)
        table = TABLE_VIDEO_AXIS
        bqutil.load_data_to_table(dataset, table, gsfn, the_schema, wait=True)

    else:
        print "[analyze videos] %s.%s already exists (and force recompute not specified). Skipping step to generate %s using latest course axis" % ( dataset, TABLE_VIDEO_AXIS, TABLE_VIDEO_AXIS )

    # Lastly, create video stats
    createVideoStats_day( course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest )
    createVideoStats( course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest )

    # also create person_course_video_watched
    createPersonCourseVideo( course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest )

#-----------------------------------------------------------------------------

def createVideoAxis(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Video axis depends on the current course axis, and looks for the category field defines as video.
    In addition, the edx video id is extracted (with the full path stripped, in order to generalize tracking log searches for video ids where it
    was found that some courses contained the full path beginning with i4x, while other courses only had the edx video id), youtube id
    and the chapter name / index for that respective video
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = TABLE_VIDEO_AXIS
    
    # Get Video results
    the_sql = """
                SELECT chapters.index as index_chapter,
                       videos.index as index_video,
                       videos.category as category,
                       videos.course_id as course_id,
                       videos.name as name,
                       videos.vid_id as video_id,
                       videos.yt_id as youtube_id,
                       chapters.name as chapter_name
                      FROM ( SELECT index, category, course_id, name, chapter_mid, 
                             #REGEXP_REPLACE(module_id, '[.]', '_') as vid_id, # vid id containing full path
                             REGEXP_EXTRACT(REGEXP_REPLACE(module_id, '[.]', '_'), r'(?:.*\/)(.*)') as vid_id, # Only containing video id
                             REGEXP_EXTRACT(data.ytid, r'\:(.*)') as yt_id,
                      FROM [{dataset}.course_axis]
                      WHERE category = "video") as videos
                      LEFT JOIN 
                      ( SELECT name, module_id, index
                        FROM [{dataset}.course_axis]
                      ) as chapters
                      ON videos.chapter_mid = chapters.module_id
                      ORDER BY videos.index asc
              """.format(dataset=dataset)
    
    print "[analyze_videos] Creating %s.%s table for %s" % (dataset, TABLE_VIDEO_AXIS, course_id)
    sys.stdout.flush()
        
    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_COURSE_AXIS )
        assert tinfo is not None, "[analyze videos] %s table depends on %s, which does not exist" % ( TABLE_VIDEO_AXIS, TABLE_COURSE_AXIS )

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s?  Skipping creation of %s" % ( dataset, TABLE_COURSE_AXIS, TABLE_VIDEO_AXIS )
        sys.stdout.flush()
        return

    bqdat = bqutil.get_bq_table(dataset, table, the_sql, force_query=force_recompute,
                                depends_on=["%s.course_axis" % (dataset)],
                                )
    return bqdat

#-----------------------------------------------------------------------------

def createVideoStats_day( course_id, force_recompute=False, use_dataset_latest=False, skip_last_day=False, end_date=None):
    '''
    Create video statistics per ay for viewed by looking for users who had a video position > 0, and watched by looking for users who had a video
    position > 95% of the total video length duration.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    logs = bqutil.course_id2dataset(course_id, dtype='logs')

    table = TABLE_VIDEO_STATS_PER_DAY
    
    the_sql = """
              SELECT date(time)as date, username,
                              #module_id as video_id,
                              #REGEXP_REPLACE(REGEXP_EXTRACT(JSON_EXTRACT(event, '$.id'), r'(?:i4x-)(.*)(?:"$)'), '-', '/') as video_id, # Old method takes full video id path
                              (case when REGEXP_MATCH( JSON_EXTRACT(event, '$.id') , r'([-])' ) then REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', ''), r'(?:.*\/)(.*)') else REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', '') end) as video_id, # This takes video id only
                              max(case when JSON_EXTRACT_SCALAR(event, '$.speed') is not null then float(JSON_EXTRACT_SCALAR(event,'$.speed'))*float(JSON_EXTRACT_SCALAR(event, '$.currentTime')) else  float(JSON_EXTRACT_SCALAR(event, '$.currentTime')) end) as position,
                       FROM {DATASETS}
                       WHERE (event_type = "play_video" or event_type = "pause_video" or event_type = "stop_video") and
                              event is not null
                       group by username, video_id, date
                       order by date
              """
    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_VIDEO_STATS_PER_DAY )
        assert tinfo is not None, "[analyze_videos] Creating %s.%s table for %s" % (dataset, TABLE_VIDEO_STATS_PER_DAY, course_id)

        print "[analyze_videos] Appending latest data to %s.%s table for %s" % (dataset, TABLE_VIDEO_STATS_PER_DAY, course_id)
        sys.stdout.flush()

    except (AssertionError, Exception) as err:
        print str(err)
        sys.stdout.flush()
        print " --> Missing %s.%s?  Attempting to create..." % ( dataset, TABLE_VIDEO_STATS_PER_DAY )
        sys.stdout.flush()
        pass

    print "=== Processing Video Stats Per Day for %s (start %s)"  % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(the_sql, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     get_date_function=gdf,
                                                     skip_last_day=skip_last_day)

    print "Done with Video Stats Per Day for %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def createVideoStats( course_id, force_recompute=False, use_dataset_latest=False ):
    '''
    Final step for video stats is to run through daily video stats table and aggregate for entire course for videos watch and videos viewed
    Join results with video axis to get detailed metadata per video for dashboard data
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    logs = bqutil.course_id2dataset(course_id, dtype='logs')

    table = TABLE_VIDEO_STATS

    the_sql = """
              SELECT index_chapter,
                     index_video,
                     name,
                     video_id,
                     chapter_name,
                     sum(case when position > 0 then 1 else 0 end) as videos_viewed, 
                     sum(case when position > video_length*0.95 then 1 else 0 end) as videos_watched,
               FROM (
                     SELECT username, index_chapter,
                            index_video,
                            name,
                            video_id, 
                            chapter_name,
                            max(position) as position,
                            video_length,
                     FROM (SELECT * FROM [{dataset}.{videostatsperday}]) as video_log,
                           LEFT JOIN EACH
                          (SELECT video_length,
                                  video_id as vid_id,
                                  name,
                                  index_video,
                                  index_chapter,
                                  chapter_name
                           FROM [{dataset}.{videoaxis}]
                           ) as video_axis
                           ON video_log.video_id = video_axis.vid_id
                           WHERE video_id is not null and username is not null
                           group by username, video_id, name, index_chapter, index_video, chapter_name, video_length
                           order by video_id asc)
                GROUP BY video_id, index_chapter, index_video, name, chapter_name
                ORDER BY index_video asc;
                """.format(dataset=dataset, videoaxis=TABLE_VIDEO_AXIS, videostatsperday=TABLE_VIDEO_STATS_PER_DAY)

    print "[analyze_videos] Creating %s.%s table for %s" % (dataset, TABLE_VIDEO_STATS, course_id)
    sys.stdout.flush()
        
    try:
        tinfo_va = bqutil.get_bq_table_info( dataset, TABLE_VIDEO_AXIS )
        trows_va = int(tinfo_va['numRows'])
        tinfo_va_day = bqutil.get_bq_table_info( dataset, TABLE_VIDEO_STATS_PER_DAY )
        trows_va_day = int(tinfo_va['numRows'])
        assert tinfo_va is not None and trows_va != 0, "[analyze videos] %s table depends on %s, which does not exist" % ( TABLE_VIDEO_STATS, TABLE_VIDEO_AXIS ) 
        assert tinfo_va_day is not None and trows_va_day != 0, "[analyze videos] %s table depends on %s, which does not exist" % ( TABLE_VIDEO_STATS, TABLE_VIDEO_STATS_PER_DAY ) 

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s and/or %s (including 0 rows in table)?  Skipping creation of %s" % ( dataset, TABLE_VIDEO_AXIS, TABLE_VIDEO_STATS_PER_DAY, TABLE_VIDEO_STATS )
        sys.stdout.flush()
        return

    bqdat = bqutil.get_bq_table(dataset, table, the_sql, force_query=force_recompute,
                                depends_on=["%s.%s" % (dataset, TABLE_VIDEO_AXIS)],
                                )
    return bqdat


#-----------------------------------------------------------------------------

def createPersonCourseVideo( course_id, force_recompute=False, use_dataset_latest=False ):
    '''
    Create the person_course_video_watched table, based on video_stats.
    Each row gives the number of unique videos watched by a given user, for the given course.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    table = TABLE_PERSON_COURSE_VIDEO_WATCHED

    the_sql = """
                  SELECT user_id, 
                      "{course_id}" as course_id,
                      count(*) n_unique_videos_watched,
                      count(*) / n_total_videos as fract_total_videos_watched,
                      viewed, certified, verified
                  FROM
                  (
                      SELECT PC.user_id as user_id, UV.username as username,
                          video_id, 
                          n_views,
                          NV.n_total_videos as n_total_videos,
                          certified,
                          viewed,
                          (mode=="verified") as verified,
                      FROM
                      (
                          SELECT username, video_id, count(*) as n_views
                          FROM [{dataset}.video_stats_day] 
                          GROUP BY username, video_id
                      ) UV
                      JOIN [{dataset}.person_course] PC
                      on UV.username = PC.username
                      CROSS JOIN 
                      (
                          SELECT count(*) as n_total_videos
                          FROM [{dataset}.video_axis]
                      ) NV
                      WHERE PC.roles = 'Student'
                  )
                  GROUP BY user_id, certified, viewed, verified, n_total_videos
                  order by user_id
              """

    the_sql = the_sql.format(course_id=course_id, dataset=dataset)
    bqdat = bqutil.get_bq_table(dataset, table, the_sql, force_query=force_recompute,
                                depends_on=["%s.%s" % (dataset, TABLE_VIDEO_STATS)],
                                newer_than=datetime.datetime( 2017, 2, 6, 18, 30 ),
                                startIndex=-2)
    if not bqdat:
        nfound = 0
    else:
        nfound = bqutil.get_bq_table_size_rows(dataset, table)
    print "--> Done with %s for %s, %d entries found" % (table, course_id, nfound)
    sys.stdout.flush()

    return bqdat
    
#-----------------------------------------------------------------------------

def createVideoStats_obsolete( course_id, force_recompute=False, use_dataset_latest=False, startDate=DATE_DEFAULT_START, endDate=DATE_DEFAULT_END ):
    '''
    Create video statistics for viewed by looking for users who had a video position > 0, and watched by looking for users who had a video
    position > 95% of the total video length duration.
    This was the original method used, but is not the most efficient since it queries entire log set. Instead, generate video stats per day, then incrementally
    append to that data table as the daily log data comes in.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    logs = bqutil.course_id2dataset(course_id, dtype='logs')

    table = TABLE_VIDEO_STATS
    
    the_sql = """
                 SELECT index_chapter,
                        index_video,
                        name,
                        video_id, 
                        chapter_name,
                        sum(case when position > 0 then 1 else 0 end) as videos_viewed, 
                        sum(case when position > video_length*0.95 then 1 else 0 end) as videos_watched,
                 FROM (SELECT username,
                              #module_id as video_id,
                              #REGEXP_REPLACE(REGEXP_EXTRACT(JSON_EXTRACT(event, '$.id'), r'(?:i4x-)(.*)(?:"$)'), '-', '/') as video_id, # Old method takes full video id path
                              (case when REGEXP_MATCH( JSON_EXTRACT(event, '$.id') , r'[-]' ) then REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', ''), r'(?:.*\/)(.*)') else REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', '') end) as video_id, # This takes video id only
                              max(case when JSON_EXTRACT_SCALAR(event, '$.speed') is not null then float(JSON_EXTRACT_SCALAR(event,'$.speed'))*float(JSON_EXTRACT_SCALAR(event, '$.currentTime')) else  float(JSON_EXTRACT_SCALAR(event, '$.currentTime')) end) as position,
                       FROM (TABLE_QUERY({logs},
                             "integer(regexp_extract(table_id, r'tracklog_([0-9]+)')) BETWEEN {start_date} and {end_date}"))
                       WHERE (event_type = "play_video" or event_type = "pause_video" or event_type = "stop_video") and
                              event is not null
                       group by username, video_id
                       order by username, video_id) as video_log,
                       LEFT JOIN EACH
                       (SELECT video_length,
                                video_id as vid_id,
                                name,
                                index_video,
                                index_chapter,
                                chapter_name
                        FROM [{dataset}.{videoaxis}]
                        ) as {videoaxis}
                        ON video_log.video_id = {videoaxis}.vid_id
                        WHERE video_id is not null
                        group by video_id, name, index_chapter, index_video, chapter_name
                        order by index_video asc;
                """.format(dataset=dataset,start_date=startDate,end_date=endDate,logs=logs, videoaxis=TABLE_VIDEO_AXIS)

    print "[analyze_videos] Creating %s.%s table for %s" % (dataset, TABLE_VIDEO_STATS, course_id)
    sys.stdout.flush()
        
    try:
        tinfo = bqutil.get_bq_table_info(dataset, TABLE_VIDEO_AXIS )
        assert tinfo is not None, "[analyze videos] %s table depends on %s, which does not exist" % ( TABLE_VIDEO_STATS, TABLE_VIDEO_AXIS ) 

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s?  Skipping creation of %s" % ( dataset, TABLE_VIDEO_AXIS, TABLE_VIDEO_STATS )
        sys.stdout.flush()
        return

    bqdat = bqutil.get_bq_table(dataset, table, the_sql, force_query=force_recompute,
                                depends_on=["%s.%s" % (dataset, TABLE_VIDEO_AXIS)],
                                )
    return bqdat
    
#-----------------------------------------------------------------------------

def get_youtube_api_stats(youtube_id, api_key, part, delay_secs=0):
    '''
    Youtube video duration lookup, using specified API_KEY from configuration file
    Visit https://developers.google.com/console/help/new/#generatingdevkeys for details on how to generate public key
    '''
    if youtube_id is '': return None
    sleep(delay_secs)

    try:
        assert api_key is not None, "[analyze videos] Public API Key is missing from configuration file."
        #url = "http://gdata.youtube.com/feeds/api/videos/" + youtube_id + "?v=2&alt=jsonc"    # Version 2 API has been deprecated
        url = "https://www.googleapis.com/youtube/v3/videos?part=" + part + "&id=" + youtube_id + "&key=" + api_key # Version 3.0 API
        data = urllib2.urlopen(url).read().decode("utf-8")
    except (AssertionError, Exception) as err:
        error = str(err)
        if "504" in error or "403" in error: 
            # rate-limit issue: try again with double timeout
            if delay_secs > MIN_IN_SECS:
                print "[Giving up] %s\n%s" % (youtube_id, url)
                return None, None
            new_delay = max(1.0, delay_secs * 2.0)
            print "[Rate-limit] <%s> - Trying again with delay: %s" % (youtube_id, str(new_delay))
            return get_youtube_api_stats(youtube_id=youtube_id, api_key=api_key, delay_secs=new_delay)
        else:
            print "[Error] <%s> - Unable to get duration.\n%s" % (youtube_id, url)

        raise
        
    d = json.loads(data)
    contentDetails = d['items'][0]['contentDetails']
    statistics = d['items'][0]['statistics']
    return contentDetails, statistics

#-----------------------------------------------------------------------------

def parseISOduration(isodata):
    '''
    Parses time duration for video length
    '''
    # see http://en.wikipedia.org/wiki/ISO_8601#Durations
    ISO_8601_period_rx = re.compile(
        'P'   # designates a period
        '(?:(?P<years>\d+)Y)?'   # years
        '(?:(?P<months>\d+)M)?'  # months
        '(?:(?P<weeks>\d+)W)?'   # weeks
        '(?:(?P<days>\d+)D)?'    # days
        '(?:T' # time part must begin with a T
        '(?:(?P<hours>\d+)H)?'   # hourss
        '(?:(?P<minutes>\d+)M)?' # minutes
        '(?:(?P<seconds>\d+)S)?' # seconds
        ')?'   # end of time part
        )
    
    parsedISOdata = ISO_8601_period_rx.match(isodata).groupdict()
    return parsedISOdata

#-----------------------------------------------------------------------------

def getTotalTimeSecs(data):
    '''
    Convert parsed time duration dict into seconds
    '''

    sec = 0
    for timeData in data:
        if data[timeData] is not None:

            if timeData == 'years':
                sec = sec + int(data[timeData])*YEAR_IN_SECS
            if timeData == 'months':
                sec = sec + int(data[timeData])*MONTHS_IN_SECS
            if timeData == 'weeks':
                sec = sec + int(data[timeData])*WEEKS_IN_SECS
            if timeData == 'hours':
                sec = sec + int(data[timeData])*HOURS_IN_SECS
            if timeData == 'minutes':
                sec = sec + int(data[timeData])*MIN_IN_SECS
            if timeData == 'seconds':
                sec = sec + int(data[timeData])
    return sec

#-----------------------------------------------------------------------------

def findVideoLength(dataset, youtube_id, api_key=None):
    '''
    Handle video length lookup
    '''
    try:
        youtube_id = unidecode(youtube_id)
    except Exception as err:
        print "youtube_id is not ascii?  ytid=", youtube_id
        return 0
    try:
        assert youtube_id is not None, "[analyze videos] youtube id does not exist"
        content, stats = get_youtube_api_stats(youtube_id=youtube_id, api_key=api_key, part=YOUTUBE_PARTS)
        durationDict = parseISOduration(content['duration'].encode("ascii","ignore"))
        length = getTotalTimeSecs(durationDict)
        print "[analyze videos] totalTime for youtube video %s is %s sec" % (youtube_id, length)
    except (AssertionError, Exception) as err:
        print "Failed to lookup video length for %s!  Error=%s, data=%s" % (youtube_id, err, dataset)
        length = 0
    return length

#-----------------------------------------------------------------------------

def openfile(fn, mode='r'):
    '''
    Properly open file according to file extension type
    '''

    if (not os.path.exists(fn)) and (not fn.endswith('.gz')):
        fn += ".gz"
    if mode=='r' and not os.path.exists(fn):
        return None   # failure, no file found, return None
    if fn.endswith('.gz'):
        return gzip.GzipFile(fn, mode)
    return open(fn, mode)

#-----------------------------------------------------------------------------

def getYoutubeDurations(dataset, bq_table_input, api_key, outputfilename, schema, force_recompute):
    '''
    Add youtube durations to Video Axis file using youtube id's and then write out to specified local path to prep for google storage / bigquery upload
    '''
    
    fp = openfile(outputfilename, 'w')
    linecnt = 0
    for row_dict in bq_table_input:
        
        linecnt += 1
        verified_row = OrderedDict()
        
        # Initial pass-through of keys in current row
        for keys in row_dict:
            
            # Only include keys defined in schema
            if keys in schema.keys():
                verified_row[keys] = row_dict[keys]
            
        # Recompute Video Length durations
        if force_recompute:
            verified_row[VIDEO_LENGTH] = findVideoLength( dataset=dataset, youtube_id=verified_row[VIDEO_ID], api_key=api_key )
        
        # Ensure schema type
        check_schema(linecnt, verified_row, the_ds=schema, coerce=True)
        
        try:
            fp.write(json.dumps(verified_row)+'\n')
        except Exception as err:
            print "Failed to write line %s!  Error=%s, data=%s" % (linecnt, str(err), dataset)
    
    fp.close()

#-----------------------------------------------------------------------------



