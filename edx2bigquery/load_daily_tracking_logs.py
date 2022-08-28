#!/usr/bin/python
#
# File:   load_daily_tracking_logs.py
# Date:   15-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# load daily tracking logs into BigQuery via Google Cloud Storage
#
# Needs gsutil to be setup and working.
# Uses bqutil to do the interface to BigQuery.
#
# Usage: 
#
#    python load_daily_tracking_logs.py COURSE_ID
#
# Assumes daily tracking log files are already uploaded to google storage.

import os
import sys
from . import auth
import json
import time
import glob
import re
import datetime
from . import bqutil
from . import gsutil

#-----------------------------------------------------------------------------

def load_all_daily_logs_for_course(course_id, gsbucket="gs://x-data", verbose=True, wait=False,
                                   check_dates=True):
    '''
    Load daily tracking logs for course from google storage into BigQuery.
    
    If wait=True then waits for loading jobs to be completed.  It's desirable to wait
    if subsequent jobs which need these tables (like person_day) are to be run
    immediately afterwards.
    '''

    print("Loading daily tracking logs for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now()))
    sys.stdout.flush()
    gsroot = gsutil.path_from_course_id(course_id)

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA = json.loads(open('%s/schemas/schema_tracking_log.json' % mypath).read())['tracking_log']

    gsdir = '%s/%s/DAILY/' % (gsbucket, gsroot)

    fnset = gsutil.get_gs_file_list(gsdir)
  
    dataset = bqutil.course_id2dataset(gsroot, dtype="logs")
  
    # create this dataset if necessary
    bqutil.create_dataset_if_nonexistent(dataset)

    tables = bqutil.get_list_of_table_ids(dataset)
    tables = [x for x in tables if x.startswith('track')]
  
    if verbose:
        print("-"*77)
        print("current tables loaded:", json.dumps(tables, indent=4))
        print("files to load: ", json.dumps(list(fnset.keys()), indent=4))
        print("-"*77)
        sys.stdout.flush()
  
    for fn, fninfo in fnset.items():

        if int(fninfo['size'])<=45:
            print("Zero size file %s, skipping" % fn)
            continue

        m = re.search('(\d\d\d\d-\d\d-\d\d)', fn)
        if not m:
            continue
        date = m.group(1)
        tablename = "tracklog_%s" % date.replace('-','')	# YYYYMMDD for compatibility with table wildcards

        # file_date = gsutil.get_local_file_mtime_in_utc(fn, make_tz_unaware=True)
        file_date = fninfo['date'].replace(tzinfo=None)
  
        if tablename in tables:
            skip = True
            if check_dates:
                table_date = bqutil.get_bq_table_last_modified_datetime(dataset, tablename)
                if not (table_date > file_date):
                    print("Already have table %s, but %s file_date=%s, table_date=%s; re-loading from gs" % (tablename, fn, file_date, table_date))
                    skip = False
                    
            if skip:
                if verbose:
                    print("Already have table %s, skipping file %s" % (tablename, fn))
                    sys.stdout.flush()
                continue

        #if date < '2014-07-27':
        #  continue
  
        print("Loading %s into table %s " % (fn, tablename))
        if verbose:
            print("start [%s]" % datetime.datetime.now())
        sys.stdout.flush()
        gsfn = fninfo['name']
        ret = bqutil.load_data_to_table(dataset, tablename, gsfn, SCHEMA, wait=wait, maxbad=1000)
  
    if verbose:
        print("-" * 77)
        print("done with %s [%s]" % (course_id, datetime.datetime.now()))
    print("=" * 77)
    sys.stdout.flush()
  
#-----------------------------------------------------------------------------
