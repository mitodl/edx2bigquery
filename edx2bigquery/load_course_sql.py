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

def load_sql_for_course(course_id, gsbucket="gs://x-data", basedir="X-Year-2-data-sql", datedir="2014-09-21", do_gs_copy=False):
    
    print "Loading SQL for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    lfp = find_course_sql_dir(course_id, basedir, datedir)

    print "Using this directory for local files: ", lfp
    sys.stdout.flush()

    local_files = glob.glob(lfp / '*')
                          
    gsdir = gsutil.gs_path_from_course_id(course_id, gsbucket=gsbucket)

    if do_gs_copy:
        try:
            fnset = get_gs_file_list(gsdir)
        except Exception as err:
            fnset = []
        
        for fn in local_files:
            fnb = os.path.basename(fn)
            if not (fnb.endswith('.csv') or fnb.endswith('.json') or fnb.endswith('.csv.gz') 
                    or fnb.endswith('.json.gz') or fnb.endswith('.mongo.gz')):
                print "...unknown file type %s, skipping" % fn
                sys.stdout.flush()
                continue
            if fnb in fnset:
                print "...%s already copied, skipping" % fn
                sys.stdout.flush()
                continue

            cmd = 'gsutil cp -z csv,json %s %s/' % (fn, gsdir)
            print cmd
            sys.stdout.flush()
            os.system(cmd)
            
    # load into bigquery
    dataset = bqutil.course_id2dataset(course_id)
    DO_LOAD = ['user_info_combo.json.gz', 'studentmodule.csv.gz']

    if not os.path.exists(lfp / 'studentmodule.csv.gz'):
        if os.path.exists(lfp / 'studentmodule.csv'):
            DO_LOAD = ['user_info_combo.json.gz', 'studentmodule.csv']
        else:
            print "Error!  Missing studentmodule.csv[.gz]"

    bqutil.create_dataset_if_nonexistent(dataset)

    mypath = os.path.dirname(os.path.realpath(__file__))

    if 1:
        uic_schema = json.loads(open('%s/schemas/schema_user_info_combo.json' % mypath).read())['user_info_combo']
        bqutil.load_data_to_table(dataset, 'user_info_combo', gsdir + '/' + "user_info_combo.json.gz", uic_schema, wait=False)
    
    if 1:
        schemas = json.loads(open('%s/schemas/schemas.json' % mypath).read())
        cwsm_schema = schemas['courseware_studentmodule']
        bqutil.load_data_to_table(dataset, 'studentmodule', gsdir + '/' + DO_LOAD[1], cwsm_schema, format='csv', wait=False, skiprows=1)


        
