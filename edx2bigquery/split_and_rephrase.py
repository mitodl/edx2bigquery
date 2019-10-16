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
import dateutil.parser
import pytz
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


def do_split(line, linecnt=0, run_rephrase=True, date=None, do_zip=False, org='MITx', logs_dir=LOGS_DIR,
             dynamic_dates=False, timezone=None):
    '''
    if dynamic_dates=True, then use the date on each tracking log line for the date string in the filename.
    
    if timezone is specified (as a pytz timezone), and if dynamic_dates=True, then use the timezone in parsing dates,
    instead of the default UTC.
    '''
    
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

    # determine the date to be used for the filename
    if dynamic_dates:
        the_time = data['time']	        # "time": "2015-02-10T05:17:08.011728+00:00"
        dt = dateutil.parser.parse(the_time)
        if timezone:
            dt = dt.astimezone(timezone)
        datestr = '-' + dt.strftime('%Y-%m-%d')
    else:
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

    # un-mangle opaque keys version of course_id, e.g. course-v1:MITx+6.00.2x_3+1T2015
    if cid.startswith('course-v1:'):
        cid = cid.split('course-v1:',1)[1].replace('+','/')
        data['course_id'] = cid

    if cid is None or not cid:
        if not 'event_source' in data:
            print "[split_and_rephrase] Badly formatted log line, skipping: %s" % data
            return
        cid = guess_course_id(data, org=org)

    if run_rephrase:
        do_rephrase(data)

    ofn = cid.replace('/','__')     # determine output filename
    
    mode = 'w'
    if dynamic_dates:
        mode = 'a'	# note - append to file!

    if ofn in ofpset:
        ofp = ofpset[ofn]
    else:
        ofp_dir = '%s/%s' % (logs_dir, ofn)
        if not os.path.exists(ofp_dir):
            os.mkdir(ofp_dir)
        if not do_zip:
            ofn_actual = '%s/tracklog%s.json' % (ofp_dir, datestr)
            ofp = open(ofn_actual, mode)
        else:
            ofn_actual = '%s/tracklog%s.json.gz' % (ofp_dir, datestr)
            ofp = gzip.GzipFile(ofn_actual, mode)
        ofpset[ofn] = ofp

    ofp.write(json.dumps(data)+'\n')

#-----------------------------------------------------------------------------

def do_file(fn, logs_dir=LOGS_DIR, dynamic_dates=False, timezone=None, logfn_keepdir=False):
    if fn.endswith('.gz'):
        fp = gzip.GzipFile(fn)
        if logfn_keepdir:
            fnb = fn.replace('/', '__')
        else:
            fnb = os.path.basename(fn)
        if dynamic_dates:
            ofn = string.rsplit(fnb, '.', 1)[0]
        else:
            ofn = string.rsplit(fnb, '.', 2)[0]
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
    try:
        for line in fp:
            cnt += 1
            try:
                newline = do_split(line, linecnt=cnt, run_rephrase=True, date=the_date, do_zip=True, logs_dir=logs_dir,
                                   dynamic_dates=dynamic_dates, timezone=timezone)
            except Exception as err:
                print "[split_and_rephrase] ===> OOPS, failed err=%s in parsing line %s" % (str(err), line)
                raise
            if ((cnt % 10000)==0):
                sys.stdout.write('.')
                sys.stdout.flush()
    except Exception as err:
        print("[split_and_rephrase] =====> ERROR, failed in parsing line=%s, file=%s, err=%s" % (cnt, fn, str(err)))
    print

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
