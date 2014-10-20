#!/usr/bin/python
#
# extract tracking logs from mongodb, rephrase, and upload to google cloud storage.
# do this one day at a time, for a specific course_id
#

import os, sys
import datetime
from path import path
from gsutil import get_gs_file_list, gs_path_from_course_id, path_from_course_id, upload_file_to_gs

DBNAME = 'harvardxdb'

def daterange(start, end):
    k = start
    dates = []
    while (k <= end):
        # dates.append('%04d%02d%02d' % (k.year, k.month, k.day))
        dstr = '%04d-%02d-%02d' % (k.year, k.month, k.day)
        start = dstr + "T00:00:00.000000"

        k += datetime.timedelta(days=1)

        dstr2 = '%04d-%02d-%02d' % (k.year, k.month, k.day)
        endstr = dstr2 + "T00:00:00.000000"
        dates.append({'start': start, 'dstr': dstr, 'end': endstr})
    return dates

def d2dt(date):
    return datetime.datetime.strptime(date, '%Y-%m-%d')

def extract_logs_mongo2gs(course_id, start="2012-09-01", end="2014-09-24", verbose=False):
    print "extracting logs for course %s" % course_id

    # list of dates to dump
    dates = daterange(d2dt(start), d2dt(end))
    
    if verbose:
        print "Dates to dump:", [x['dstr'] for x in dates]

    # what files already on gs?
    gspath = "%s/DAILY" % gs_path_from_course_id(course_id)
    gsfiles = get_gs_file_list(gspath)

    collection = 'tracking_log'
    DIR = "TRACKING_LOGS"
    if not os.path.exists(DIR):
        os.mkdir(DIR)
    DIR += '/' + path_from_course_id(course_id)
    if not os.path.exists(DIR):
        os.mkdir(DIR)

    filebuf = []
    for k in range(len(dates)-1):
                          
        d = dates[k]
        ofn = '%s/tracklog-%s.json.gz' % (DIR, d['dstr'])
        start = d['start']
        end = d['end']

        ofnb = os.path.basename(ofn)

        if ofnb in gsfiles:
            print "Already have %s, skipping" % ofnb
            sys.stdout.flush()
            continue

        # dump tracking log of certain date using mongoexport, if needed
        if not os.path.exists(ofn):
            # db.tracking_log.find({'course_id': "HarvardX/ER22x/2013_Spring", 
            #                       'time': { '$gte': "2013-08-01T00:00:00.000000", '$lt': "2013-08-02T00:00:00.000000" }}).count()
            query = '{"course_id": "%s", "time": {"$gte": "%s", "$lt": "%s" }}' % (course_id, start, end)
            cmd = "mongoexport -d %s -c %s -q '%s'| edx2bigquery rephrase_logs | gzip -9 > %s" % (DBNAME, collection, query, ofn)
            # print cmd
            os.system(cmd)
        
        upload_file_to_gs(ofn, gspath + '/' + ofnb)

        filebuf.append(ofn)

        if len(filebuf)>20:
            ffn = filebuf.pop(0)
            os.unlink(ffn)
            print "...Deleted %s" % ffn
            sys.stdout.flush()
