#!/usr/bin/python
#
# google storage utility functions
#

import os, sys
import dateutil.parser
from path import path

sys.path.append(os.path.abspath(os.curdir))
import edx2bigquery_config

def path_from_course_id(course_id):
    return path(course_id.replace('/', '__'))

def gs_path_from_course_id(course_id, gsbucket=None):
    return path("%s/%s" % (gsbucket or edx2bigquery_config.GS_BUCKET, path_from_course_id(course_id)))

def gs_download_link(gspath):
    return "https://storage.cloud.google.com/" + gspath[5:]   # drop gs:// prefix    

def get_gs_file_list(path):
    if not path.startswith('gs://'):
        path = edxbigquery_config.GS_BUCKET + path
    print "Getting file list from %s" % path
    fnset = {}
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

