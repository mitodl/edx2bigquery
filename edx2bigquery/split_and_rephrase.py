#!/usr/bin/python
#
# File:   split_and_rephrase.py
# Date:   13-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# split tracking logs by course_id, into files in separate directories.
# runs log entries through rephrasing also (option to turn that off).
#

import os, sys
import datetime
import string
import re
import json
import gzip
from rephrase_tracking_logs import do_rephrase

ofpset = {}

LOGS_DIR = "TRACKING_LOGS"

#-----------------------------------------------------------------------------

cidre1 = re.compile('^http[s]*://[^/]+/courses/([^/]+/[^/]+/[^/]+)/')
cidre2 = re.compile('/courses/([^/]+/[^/]+/[^/]+)/')
cidre3 = re.compile('i4x://([^/]+/[^/]+)/')
cidre4 = re.compile('^/courses/([^/]+/[^/]+)[/$]')  # when "event_type": "/courses/MITx/8.02x/"

def guess_course_id(data, org="MITx"):
    '''
    Old edX tracking logs (pre-2014) did not define course_id explicitly.  For those old logs,
    the course_id must be reconstructured heuristically.  This is fallible.
    '''
    # if data_source==browser: from url https://www.edx.org/courses/MITx/6.00x/2012_Fall/
    if data['event_source']=='browser':
        page = data['page']
        m = re.search(cidre1, page)
        if m:
            return m.group(1)
        return ''
    m = re.search(cidre2, data['event_type'])
    if m:
        return m.group(1)

    # if 'problem_id' in data['event']:
    #    m = re.search(cidre3, data['event']['problem_id'])
    #    if m:
    #        short_cid = m.group(1)	# only partial course_id - missing run date
    #        return fix_course_id(short_cid, data['time'], org=org)
    #
    #m = cidre4.search(data['event_type'])
    #if m:
    #    return fix_course_id(m.group(1), data['time'])
            
    return ''	# no guess
 
#-----------------------------------------------------------------------------


def do_split(line, linecnt=0, run_rephrase=True, date=None, do_zip=False, org='MITx', logs_dir=LOGS_DIR):
    
    line = line.strip()
    if not line.startswith('{'):
        # bug workaround for very old logs
        # some 2012 lines start like this: "Sep  9 00:00:48 localhost {"
        if 'localhost {' in line[:27]:
            line = line[26:]

    try:
        data = json.loads(line)
    except Exception as err:
        sys.stderr.write('[%d] oops, bad log line err=%s, line=%s\n' % (linecnt, str(err), line))
        return

    if date is None:
        datestr = ''
    else:
        datestr = '-' + date

    # add course_id?
    if 'course_id' not in data:
        cid = data.get('context',{}).get('course_id','')
        if cid:
            data['course_id'] = cid
    else:
        cid = data['course_id']

    if cid is None or not cid:
        cid = guess_course_id(data, org=org)

    if run_rephrase:
        do_rephrase(data)

    ofn = cid.replace('/','__')     # determine output filename
    
    if ofn in ofpset:
        ofp = ofpset[ofn]
    else:
        ofp_dir = '%s/%s' % (logs_dir, ofn)
        if not os.path.exists(ofp_dir):
            os.mkdir(ofp_dir)
        if not do_zip:
            ofp = open('%s/tracklog%s.json' % (ofp_dir, datestr), 'a')
        else:
            ofp = gzip.GzipFile('%s/tracklog%s.json.gz' % (ofp_dir, datestr), 'w')
        ofpset[ofn] = ofp

    ofp.write(json.dumps(data)+'\n')

#-----------------------------------------------------------------------------

def do_file(fn, logs_dir=LOGS_DIR):
    if fn.endswith('.gz'):
        fp = gzip.GzipFile(fn)
        ofn = string.rsplit(os.path.basename(fn), '.', 2)[0]
    else:
        fp = open(fn)	# expect it ends with .log
        ofn = string.rsplit(os.path.basename(fn), '.', 1)[0]

    # if file has been done, then there will be a file denoting this in the META subdir
    ofn = '%s/META/%s' % (logs_dir, ofn)
    if os.path.exists(ofn):
        print "Already done %s -> %s (skipping)" % (fn, ofn)
        sys.stdout.flush()
        return

    print "Processing %s -> %s (%s)" % (fn, ofn, datetime.datetime.now())
    sys.stdout.flush()

    m = re.search('(\d\d\d\d-\d\d-\d\d)', fn)
    if m:
        the_date = m.group(1)
    else:
        the_date = None

    cnt = 0
    for line in fp:
        cnt += 1
        try:
            newline = do_split(line, linecnt=cnt, run_rephrase=True, date=the_date, do_zip=True, logs_dir=logs_dir)
        except Exception as err:
            print "[split_and_rephrase] ===> OOPS, failed err=%s in parsing line %s" % (str(err), line)
            raise

    mdir = '%s/META' % logs_dir
    if not os.path.exists(mdir):
        os.mkdir(mdir)
    open(ofn, 'a').write(' ') 	    # mark META

    # close all file pointers
    for fn, fp in ofpset.items():
        fp.close()
        ofpset.pop(fn)

    print "...done (%s)" % datetime.datetime.now()
    
    sys.stdout.flush()

#-----------------------------------------------------------------------------

if __name__=="__main__":

    if len(sys.argv)>1:
        # arguments are filenames; process each file

        if sys.argv[1]=="--logsdir":
            LOGS_DIR = sys.argv[2]
            sys.argv.pop(1)
            sys.argv.pop(1)
            print "Outputting files in directory %s" % LOGS_DIR

        for fn in sys.argv[1:]:
            do_file(fn)

    else:
        for line in sys.stdin:
            do_split(line)
