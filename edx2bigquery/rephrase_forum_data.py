#!/usr/bin/python
#
# File:   rephrase_forum_data.py
# Date:   15-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# rephrase forum data so they can be loaded into BigQuery
#
# Some names need to be mangled, because they start with dollar signs.
# other fields, like created_at and updated_at, need to be turned into
# TIMESTAMP fields.

import os
import sys
import json
import gzip
import string
import datetime
import traceback
from addmoduleid import add_module_id
from check_schema_tracking_log import check_schema, schema2dict
from load_course_sql import find_course_sql_dir
from path import path
import bqutil
import gsutil

sfn = 'schema_forum.json'

mypath = os.path.dirname(os.path.realpath(__file__))
SCHEMA = json.loads(open('%s/schemas/%s' % (mypath, sfn)).read())['forum']
SCHEMA_DICT = schema2dict(SCHEMA)

def do_rephrase(data, do_schema_check=True, linecnt=0):

    if '_id' in data:
        data['mongoid'] = data['_id']['$oid']
        data.pop('_id')

    if 'parent_id' in data:
        data['parent_id'] = data['parent_id']['$oid']

    def fix_date(dstr):
        if dstr:
            dtime = int(dstr)
            if dtime:
                try:
                    dt = datetime.datetime.utcfromtimestamp(dtime/1000.0)
                except Exception as err:
                    print "oops, failed to convert in utcfromtimestamp dtime=%s, dstr=%s" % (dtime, dstr)
                    raise
                return str(dt)
        return None

    def do_fix_date(field, rec):
        if field in rec:
            rec[field] = fix_date(rec[field]['$date'])

    do_fix_date('time', data.get('endorsement',{}) or {})

    if 'updated_at' in data:
        data['updated_at'] = fix_date(data['updated_at']['$date'])

    if 'created_at' in data:
        data['created_at'] = fix_date(data['created_at']['$date'])

    if 'last_activity_at' in data:
        data['last_activity_at'] = fix_date(data['last_activity_at']['$date'])

    if 'comment_thread_id' in data:
        data['comment_thread_id'] = data['comment_thread_id']['$oid']

    if ('endorsement' in data) and ((data['endorsement']=='null') or (not data['endorsement']) or (data['endorsement'] is None)):
        data.pop('endorsement')

    if 'parent_ids' in data:
        data['parent_ids'] = ' '.join([x['$oid'] for x in data['parent_ids']])

    def mkstring(key, rec):
        if key in rec:
            rec[key] = str(rec[key])

    mkstring('historical_abuse_flaggers', data)
    mkstring('abuse_flaggers', data)
    mkstring('at_position_list', data)
    mkstring('tags_array', data)

    mkstring('up', data.get('votes', {}))
    mkstring('down', data.get('votes', {}))

    # check for any funny keys, recursively
    funny_key_sections = []
    def check_for_funny_keys(entry, name='toplevel'):
        for key, val in entry.iteritems():
            if key.startswith('i4x-') or key.startswith('xblock.'):
                sys.stderr.write("[rephrase] oops, funny key at %s in entry: %s, data=%s\n" % (name, entry, ''))
                funny_key_sections.append(name)
                return True
            if len(key)>25:
                sys.stderr.write("[rephrase] suspicious key at %s in entry: %s, data=%s\n" % (name, entry, ''))

            if key[0] in '0123456789':
                sys.stderr.write("[rephrase] oops, funny key at %s in entry: %s, data=%s\n" % (name, entry, ''))
                funny_key_sections.append(name)
                return True
                
            if '-' in key or '.' in key:
                # bad key name!  rename it, chaning "-" to "_"
                newkey = key.replace('-','_').replace('.','__')
                sys.stderr.write("[rephrase] oops, bad keyname at %s in entry: %s newkey+%s\n" % (name, entry, newkey))
                entry[newkey] = val
                entry.pop(key)
                key = newkey
            if type(val)==dict:
                ret = check_for_funny_keys(val, name + '/' + key)
                if ret is True:
                    sys.stderr.write("        coercing section %s to become a string\n" % (name+"/"+key) )
                    entry[key] = json.dumps(val)
        return False

    check_for_funny_keys(data)

    try:
        check_schema(linecnt, data, the_ds=SCHEMA_DICT, coerce=True)
    except Exception as err:
        sys.stderr.write('[%d] oops, err=%s, failed in check_schema %s\n' % (linecnt, str(err), json.dumps(data, indent=4)))
        sys.stderr.write(traceback.format_exc())
        return

def do_rephrase_line(line, linecnt=0):
    try:
        data = json.loads(line)
    except Exception as err:
        sys.stderr.write('oops, bad forum data line %s\n' % line)
        return

    try:
        do_rephrase(data, do_schema_check=True, linecnt=linecnt)
    except Exception as err:
        sys.stderr.write('[%d] oops, err=%s, bad log line %s\n' % (linecnt, str(err), line))
        sys.stderr.write(traceback.format_exc())
        return
            
    return json.dumps(data)+'\n'

#-----------------------------------------------------------------------------

def rephrase_forum_json_for_course(course_id, gsbucket="gs://x-data", 
                                   basedir="X-Year-2-data-sql", 
                                   datedir=None, 
                                   do_gs_copy=False,
                                   use_dataset_latest=False,
                                   ):
    
    print "Loading SQL for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest=use_dataset_latest)

    print "Using this directory for local files: ", lfp
    sys.stdout.flush()

    fn = 'forum.mongo'
    gsdir = gsutil.gs_path_from_course_id(course_id, gsbucket, use_dataset_latest)
    
    def openfile(fn, mode='r'):
        if (not os.path.exists(lfp / fn)) and (not fn.endswith('.gz')):
            fn += ".gz"
        if fn.endswith('.gz'):
            return gzip.GzipFile(lfp / fn, mode)
        return open(lfp / fn, mode)

    fp = openfile(fn)

    ofn = lfp / "forum-rephrased.json.gz"

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    bqutil.create_dataset_if_nonexistent(dataset)

    if os.path.exists(ofn):

        tables = bqutil.get_list_of_table_ids(dataset)
        if not 'forum' in tables:
            print "Already done?  But no forums table loaded into datasaet %s.  Redoing." % dataset
        else:
            print "Already done %s -> %s (skipping)" % (fn, ofn)
            sys.stdout.flush()
            return

    print "Processing %s -> %s (%s)" % (fn, ofn, datetime.datetime.now())
    sys.stdout.flush()

    cnt = 0
    ofp = gzip.GzipFile('tmp.json.gz', 'w')
    for line in fp:
        cnt += 1
        newline = do_rephrase_line(line, linecnt=cnt)
        ofp.write(newline)
    ofp.close()

    print "...done (%s)" % datetime.datetime.now()

    if cnt==0:
        print "...but cnt=0 entries found, skipping forum loading"
        sys.stdout.flush()
        return

    print "...copying to gsc"
    sys.stdout.flush()

    # do upload twice, because GSE file metadata doesn't always make it to BigQuery right away?
    gsfn = gsdir + '/' + "forum-rephrased.json.gz"
    cmd = 'gsutil cp tmp.json.gz %s' % (gsfn)
    os.system(cmd)
    os.system(cmd)

    table = 'forum'
    bqutil.load_data_to_table(dataset, table, gsfn, SCHEMA, wait=True)
    msg = "Original data from %s" % (lfp / fn)
    bqutil.add_description_to_table(dataset, table, msg)    

    os.system('mv tmp.json.gz "%s"' % (ofn))

    print "...done (%s)" % datetime.datetime.now()
    sys.stdout.flush()

#-----------------------------------------------------------------------------
