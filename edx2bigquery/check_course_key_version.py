#!/usr/bin/python
#
# guess what course ("opaque") key verison is being used for a specified course

import gzip
import glob
from . import gsutil
from path import Path as path

def course_key_version(course_id, logs_dir="TRACKING_LOGS", verbose=False):

    cdir = path(logs_dir) / gsutil.path_from_course_id(course_id)
    
    log_files = glob.glob(cdir / "*.json.gz")
    
    # find a file that's not too small
    for fn in log_files:
        if path(fn).stat().st_size > 65536:
            break

    if verbose:
        print("Using %s for course_key_version determination" % fn)
    count = 0
    n = 0
    with gzip.GzipFile(fn) as fp:
        for k in fp:
            n += 1
            if 'block-v1:' in k:
                count += 1
            if (count > 32):
                break
            
    if (count > 32):
        version = "v1"
    else:
        version = "standard"

    print("--> %s: %s" % (course_id, version))
