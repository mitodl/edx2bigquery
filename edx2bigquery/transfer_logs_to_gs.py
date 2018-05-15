#!/usr/bin/python
#
# File:   transfer_logs_to_gs.py
# Date:   13-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# transfer log files to google storage, following conventions.
#
# gsutil cp TRACKING_LOGS/X__8.01x__2013_SOND/tracklog-201*   gs://x-data-test/X__8.01x__2013_SOND/DAILY/
#
# usage:
#
# python transfer_logs_to_gs <course_logs_directory>
#
# e.g.
#
# python transfer_logs_to_gs X__8.01x__2013_SOND
#
# only copies files which don't already exist.

import os, sys
import glob
import datetime
import pytz
import dateutil.parser
try:
	from path import Path as path
except:
	from path import path
import gsutil

def process_dir(course_id, gspath='gs://x-data', logs_directory="TRACKING_LOGS", verbose=True):

    cdir = path(logs_directory) / gsutil.path_from_course_id(course_id)

    print "="*77
    print "Transferring tracking logs for %s from directory %s (start %s)" % (course_id, cdir, datetime.datetime.now())
    print "="*77

    if not os.path.exists(cdir):
        print "Oops!  non-existent course tracking logs directory %s" % cdir
        return

    sys.stdout.flush()
    cdir = path(cdir)
    gp = path(gspath + "/" + cdir.basename()) / 'DAILY'
    filelist = gsutil.get_gs_file_list(gp)
    # print filelist
    local_files = glob.glob(cdir / 'tracklog*.gz')
    local_files.sort()
    
    for fn in local_files:
        fnp = path(fn)
        fnb = fnp.basename()
        statbuf = os.stat(fn)
        mt = datetime.datetime.fromtimestamp(statbuf.st_mtime)

        # do some date checking to upload log files which have changed, and are newer than that on google cloud storage
        local = pytz.timezone ("America/New_York")
        # naive = datetime.datetime.strptime ("2001-2-3 10:11:12", "%Y-%m-%d %H:%M:%S")
        local_dt = local.localize(mt, is_dst=None)
        utc_dt = local_dt.astimezone (pytz.utc)

        if fnb in filelist and filelist[fnb]['date'] > utc_dt:
            if verbose:
                print "%s already exists, skipping" % fn
            continue
        elif fnb in filelist:
            print "%s already exists, but has date=%s and mtime=%s, re-uploading" % (fn, filelist[fnb]['date'], mt)
        cmd = 'gsutil cp %s %s' % (fn, gp + '/')
        print cmd
        sys.stdout.flush()
        os.system(cmd)

    print "done with %s (%s)" % (cdir, datetime.datetime.now())
    print "-"*77

