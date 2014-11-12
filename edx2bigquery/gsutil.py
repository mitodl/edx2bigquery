#!/usr/bin/python
#
# google storage utility functions
#

import os, sys
import dateutil.parser
from path import path
from collections import OrderedDict
import datetime
import pytz

sys.path.append(os.path.abspath(os.curdir))
try:
    import edx2bigquery_config
except:
    print "Warning: could not import edx2bigquery_config"
    class edx2bigquery_config(object):
        GS_BUCKET = "gs://dummy-gs-bucket"

def path_from_course_id(course_id):
    return path(course_id.replace('/', '__'))

def gs_path_from_course_id(course_id, gsbucket=None, use_dataset_latest=False):
    gsp = path("%s/%s" % (gsbucket or edx2bigquery_config.GS_BUCKET, path_from_course_id(course_id)))
    if use_dataset_latest:
        gsp = gsp / 'latest'
    return gsp

def gs_download_link(gspath):
    return "https://storage.cloud.google.com/" + gspath[5:]   # drop gs:// prefix    

def get_gs_file_list(path):
    if not path.startswith('gs://'):
        path = edxbigquery_config.GS_BUCKET + path
    print "Getting file list from %s" % path
    fnset = OrderedDict()
    for dat in os.popen('gsutil ls -l ' + path).readlines():
        if dat.strip().startswith('TOTAL'):
            continue
        try:
            x = dat.strip().split()
            if len(x)==1:
                continue
            (size, date, name) = x
        except Exception as err:
            print "oops, err=%s, dat=%s" % (str(err), dat)
            raise
        date = dateutil.parser.parse(date)
        size = int(size)
        fnb = os.path.basename(name)
        fnset[fnb] = {'size': size, 'date': date, 'name': name, 'basename': fnb}
    return fnset

def upload_file_to_gs(src, dst, options='', verbose=False):
    cmd = 'gsutil cp %s %s %s' % (options, src, dst)
    if verbose:
        print "--> %s" % cmd
        sys.stdout.flush()
    os.system(cmd)

def get_local_file_mtime_in_utc(fn, make_tz_unaware=False):
    statbuf = os.stat(fn)
    mt = datetime.datetime.fromtimestamp(statbuf.st_mtime)

    local = pytz.timezone ("America/New_York")

    # do some date checking to upload files which have changed, and are newer than that on google cloud storage
    local_dt = local.localize(mt, is_dst=None)
    utc_dt = local_dt.astimezone (pytz.utc)
    if make_tz_unaware:
        utc_dt = utc_dt.replace(tzinfo=None)
    return utc_dt

#-----------------------------------------------------------------------------

def test_path_from_cid():
    x = path_from_course_id('MITx/8.05/2_T2018')
    assert(x=='MITx__8.05__2_T2018')

def test_local_file_mtime():
    x = get_local_file_mtime_in_utc('/etc/passwd')
    assert(x.year==2014)
