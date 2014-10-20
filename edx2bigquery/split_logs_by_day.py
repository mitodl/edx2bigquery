#!/usr/bin/python
#
# File:   split_logs_by_day.py
# Date:   13-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# split tracking log by day

import os, sys, gzip, json

ofpset = {}

def do_file(fn):
    if fn.endswith('.gz'):
        fp = gzip.GzipFile(fn)
    else:
        fp = open(fn)
    fndir = os.path.dirname(fn)

    for line in fp:
        data = json.loads(line)
        time = data['time']
        date = time[:10]
        
        ofdir = '%s/DAILY' % (fndir)
        if not os.path.exists(ofdir):
            os.mkdir(ofdir)
            
        ofn = '%s/tracklog%s.json.gz' % (ofdir, date)
        if ofn in ofpset:
            ofp = ofpset[ofn]
        else:
            ofp = gzip.GzipFile(ofn, 'w')
            ofpset[ofn] = ofp
            
        ofp.write(json.dumps(data)+"\n")
        
#-----------------------------------------------------------------------------

if __name__=="__main__":
    
    fn = sys.argv[1]
    print "Processing %s" % fn
    do_file(fn)
