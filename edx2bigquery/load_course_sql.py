#!/usr/bin/python
#
# File:   load_course_sql.py
# Date:   15-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# load course SQL files into BigQuery via Google Cloud Storage
#
# Needs gsutil to be setup and working.
# Uses bqutil to do the interface to BigQuery.
#
# Usage: 
#
#    python load_course_sql.py COURSE_ID
#
# or
#
#    python load_course_sql.py --y2
#
# Uploads SQL files to GS if requested (use --upload-to-gs flag)
#

import os
import sys
import auth
import json
import time
import glob
import re
import datetime
import pytz
import gzip
import bqutil
import gsutil
from path import path
from gsutil import get_gs_file_list

#-----------------------------------------------------------------------------

def find_course_sql_dir(course_id, basedir, datedir):
    basedir = path(basedir or '')

    course_dir = course_id.replace('/','__')

    lfp = (basedir or '.') / course_dir / (datedir or '.')
    if not os.path.exists(lfp):
        # maybe course directory uses dashes instead of __ (due to edX convention)?
        olfp = lfp
        lfp = (basedir or '.') / course_id.replace('/','-') / (datedir or '.')
        if not os.path.exists(lfp):
            msg = "Error!  Cannot find course SQL directory %s or %s" % (olfp , lfp)
            print msg
            raise Exception(msg)

    return lfp

#-----------------------------------------------------------------------------

def openfile(fn, mode='r'):
    if (not os.path.exists(fn)) and (not fn.endswith('.gz')):
        fn += ".gz"
    if mode=='r' and not os.path.exists(fn):
        return None			# failure, no file found, return None
    if fn.endswith('.gz'):
        return gzip.GzipFile(fn, mode)
    return open(fn, mode)

def tsv2csv(fn_in, fn_out):
    import csv
    fp = openfile(fn_out, 'w')
    csvfp = csv.writer(fp)
    for line in openfile(fn_in):
        csvfp.writerow(line[:-1].split('\t'))
    fp.close()

#-----------------------------------------------------------------------------

def load_sql_for_course(course_id, gsbucket="gs://x-data", basedir="X-Year-2-data-sql", datedir="2014-09-21", do_gs_copy=False):
    
    print "Loading SQL for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    lfp = find_course_sql_dir(course_id, basedir, datedir)

    print "Using this directory for local files: ", lfp
    sys.stdout.flush()
                          
    # convert studentmodule if necessary

    fn_sm = lfp / 'studentmodule.csv.gz'
    if not fn_sm.exists():
        fn_sm = lfp / 'studentmodule.csv'
        if not fn_sm.exists():
            fn_sm = lfp / 'studentmodule.sql.gz'
            if not fn_sm.exists():
                fn_sm = lfp / 'studentmodule.sql'
                if not fn_sm.exists():
                    print "Error!  Missing studentmodule.[sql,csv][.gz]"
            if fn_sm.exists():	# have .sql or .sql.gz version: convert to .csv
                newfn = lfp / 'studentmodule.csv.gz'
                print "--> Converting %s to %s" % (fn_sm, newfn)
                tsv2csv(fn_sm, newfn)
                fn_sm = newfn

    def convert_sql(fnroot):
        if os.path.exists(fnroot + ".csv") or os.path.exists(fnroot + ".csv.gz"):
            return
        if os.path.exists(fnroot + ".sql") or os.path.exists(fnroot + ".sql.gz"):
            infn = fnroot + '.sql'
            outfn = fnroot + '.csv.gz'
            print "--> Converting %s to %s" % (infn, outfn)
            tsv2csv(infn, outfn)

    # convert sql files if necesssary
    fnset = ['users', 'certificates', 'enrollment', "profiles", 'user_id_map']
    for fn in fnset:
        convert_sql(lfp / fn)

    local_files = glob.glob(lfp / '*')
    gsdir = gsutil.gs_path_from_course_id(course_id, gsbucket=gsbucket)

    local = pytz.timezone ("America/New_York")

    if do_gs_copy:
        try:
            fnset = get_gs_file_list(gsdir)
        except Exception as err:
            fnset = []
        
        def copy_if_newer(fn, fnset, options='-z csv,json'):
            statbuf = os.stat(fn)
            mt = datetime.datetime.fromtimestamp(statbuf.st_mtime)
            
            # do some date checking to upload files which have changed, and are newer than that on google cloud storage
            local_dt = local.localize(mt, is_dst=None)
            utc_dt = local_dt.astimezone (pytz.utc)

            fnb = os.path.basename(fn)
            if fnb in fnset and fnset[fnb]['date'] > utc_dt:
                print "...%s already copied, skipping" % fn
                sys.stdout.flush()
                return
            elif fnb in fnset:
                print "...%s already exists, but has date=%s and mtime=%s, re-uploading" % (fn, fnset[fnb]['date'], mt)

            gsutil.upload_file_to_gs(fn, gsdir / fnb, options=options, verbose=True)

        for fn in local_files:
            fnb = os.path.basename(fn)
            if fnb=='course_image.jpg':
                copy_if_newer(fn, fnset, options='-a public-read')
            if not (fnb.endswith('.csv') or fnb.endswith('.json') or fnb.endswith('.csv.gz') 
                    or fnb.endswith('.json.gz') or fnb.endswith('.mongo.gz')):
                print "...unknown file type %s, skipping" % fn
                sys.stdout.flush()
                continue
            copy_if_newer(fn, fnset)

    # load into bigquery
    dataset = bqutil.course_id2dataset(course_id)
    bqutil.create_dataset_if_nonexistent(dataset)
    mypath = os.path.dirname(os.path.realpath(__file__))

    # load user_info_combo
    uicfn = lfp / 'user_info_combo.json.gz'
    if uicfn.exists():
        uic_schema = json.loads(open('%s/schemas/schema_user_info_combo.json' % mypath).read())['user_info_combo']
        bqutil.load_data_to_table(dataset, 'user_info_combo', gsdir / "user_info_combo.json.gz", uic_schema, wait=False)
    else:
        print "--> File %s does not exist, not loading user_info_combo into BigQuery" % uicfn
    
    # load studentmodule
                
    if fn_sm.exists():
        schemas = json.loads(open('%s/schemas/schemas.json' % mypath).read())
        cwsm_schema = schemas['courseware_studentmodule']
        bqutil.load_data_to_table(dataset, 'studentmodule', gsdir / fn_sm.basename(), cwsm_schema, format='csv', wait=False, skiprows=1)
    else:
        print "--> Not loading studentmodule: file %s not found" % fn_sm


        
