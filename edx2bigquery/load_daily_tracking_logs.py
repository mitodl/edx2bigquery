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
import auth
import json
import time
import glob
import re
import datetime
import bqutil
import gsutil

#-----------------------------------------------------------------------------

def load_all_daily_logs_for_course(course_id, gsbucket="gs://x-data", verbose=True, wait=False):
    '''
    Load daily tracking logs for course from google storage into BigQuery.
    
    If wait=True then waits for loading jobs to be completed.  It's desirable to wait
    if subsequent jobs which need these tables (like person_day) are to be run
    immediately afterwards.
    '''

    print "Loading daily tracking logs for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()
    gsroot = course_id.replace('/','__')

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA = json.loads(open('%s/schemas/schema_tracking_log.json' % mypath).read())['tracking_log']

    gsdir = '%s/%s/DAILY/' % (gsbucket, gsroot)

    fnset = gsutil.get_gs_file_list(gsdir)
  
    dataset = bqutil.course_id2dataset(gsroot, dtype="logs")
  
    # create this dataset if necessary
    bqutil.create_dataset_if_nonexistent(dataset)

    tables = os.popen('bq ls -n 10000 %s' % dataset).readlines()
    tables = [x.strip().split(' ',1)[0] for x in tables]
    tables = [x for x in tables if x.startswith('track')]
  
    if verbose:
        print "-"*77
        print "current tables loaded:", json.dumps(tables, indent=4)
        print "files to load: ", json.dumps(fnset.keys(), indent=4)
        print "-"*77
        sys.stdout.flush()
  
    for fn, fninfo in fnset.iteritems():

        if int(fninfo['size'])<=45:
            print "Zero size file %s, skipping" % fn
            continue

        m = re.search('(\d\d\d\d-\d\d-\d\d)', fn)
        if not m:
            continue
        date = m.group(1)
        tablename = "tracklog_%s" % date.replace('-','')	# YYYYMMDD for compatibility with table wildcards
  
        if tablename in tables:
            if verbose:
                print "Already have table %s, skipping file %s" % (tablename, fn)
                sys.stdout.flush()
            continue
        
        #if date < '2014-07-27':
        #  continue
  
        print "Loading %s into table %s " % (fn, tablename)
        if verbose:
            print "start [%s]" % datetime.datetime.now()
        sys.stdout.flush()
        gsfn = fninfo['name']
        ret = bqutil.load_data_to_table(dataset, tablename, gsfn, SCHEMA, wait=wait, maxbad=1000)
  
    if verbose:
        print "-" * 77
        print "done with %s [%s]" % (course_id, datetime.datetime.now())
    print "=" * 77
    sys.stdout.flush()
  
#-----------------------------------------------------------------------------
