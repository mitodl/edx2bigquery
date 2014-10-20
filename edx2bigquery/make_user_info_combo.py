#!/usr/bin/python
#
# File:   make_user_info_combo.py
# Date:   13-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# make single JSON file containing edX SQL information from:
#
#    users.csv (auth_user)
#    profiles.csv (auth_userprofile)
#    enrollment.csv
#    certificates.csv 
#    user_id_map.csv
#
# one line is generated for each user.
#
# This makes it easier to load data into BigQuery and databases
# where joins are not allowed or are expensive.
#
# It also puts into one place much of the source information
# needed for the person course dataset.
#
# Usage:  
#
#    python make_user_info_combo.py <course_directory>
#
# e.g.:
#
#    python make_user_info_combo.py 6.SFMx
#
# files are assumed to already be in CSV format, with the HarvardX
# sql data file naming conventions.
#
# Fields included are:
#
# from users.csv:  user_id,username,email,password,is_staff,last_login,date_joined
# from profiles.csv: name,language,location,meta,courseware,gender,mailing_address,year_of_birth,level_of_education,goals,allow_certificate,country,city
# from enrollment.csv: course_id,created,is_active,mode
# from certificates.csv: download_url,grade,course_id,key,distinction,status,verify_uuid,download_uuid,name,created_date,modified_date,error_reason,mode
# from user_id_map.csv: hash_id
#
# these are all joined on user_id
#
# the profile, enrollment, certificates, and user_id data are stored with those table names
# as key prefixes, e.g. profile -> name is stored as key profile_name,
# just in case more fields are added later, with colliding names.
# 
# Each record's schema is checked for validity afterwards.

import os, sys
import csv
import gzip
import json
import gsutil

from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir

#csv.field_size_limit(sys.maxsize)
csv.field_size_limit(1310720)

def process_file(course_id, basedir=None, datedir=None):

    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir)

    cdir = lfp
    print "Processing %s from files in %s" % (course_id, cdir)
    sys.stdout.flush()

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_user_info_combo.json' % mypath
    
    the_dict_schema = schema2dict(json.loads(open(SCHEMA_FILE).read())['user_info_combo'])
    
    uic = defaultdict(dict)		# dict with key = user_id, and val = dict to be written out as JSON line
    
    def copy_elements(src, dest, fields, prefix=""):
        for key in fields:
            if src[key]=='NULL':
                continue
            dest[prefix + key] = src[key]
    
    def openfile(fn, mode='r'):
        if (not os.path.exists(cdir / fn)) and (not fn.endswith('.gz')):
            fn += ".gz"
        if fn.endswith('.gz'):
            return gzip.GzipFile(cdir / fn, mode)
        return open(cdir / fn, mode)
    
    for line in csv.DictReader(openfile('users.csv')):
        uid = int(line['id'])
        fields = ['username', 'email', 'is_staff', 'last_login', 'date_joined']
        copy_elements(line, uic[uid], fields)
        uic[uid]['user_id'] = uid
    
    for line in csv.DictReader(openfile('profiles.csv')):
        uid = int(line['user_id'])
        fields = ['name', 'language', 'location', 'meta', 'courseware', 
                   'gender', 'mailing_address', 'year_of_birth', 'level_of_education', 'goals', 
                   'allow_certificate', 'country', 'city']
        copy_elements(line, uic[uid], fields, prefix="profile_")
    
    for line in csv.DictReader(openfile('enrollment.csv')):
        uid = int(line['user_id'])
        fields = ['course_id', 'created', 'is_active', 'mode', ]
        copy_elements(line, uic[uid], fields, prefix="enrollment_")
    
    for line in csv.DictReader(openfile('certificates.csv')):
        uid = int(line['user_id'])
        fields = ['download_url', 'grade', 'course_id', 'key', 'distinction', 'status', 
                  'verify_uuid', 'download_uuid', 'name', 'created_date', 'modified_date', 'error_reason', 'mode',]
        copy_elements(line, uic[uid], fields, prefix="certificate_")
    
    for line in csv.DictReader(openfile('user_id_map.csv')):
        uid = int(line['id'])
        fields = ['hash_id']
        copy_elements(line, uic[uid], fields, prefix="id_map_")
    
    # sort by userid
    uidset = uic.keys()
    uidset.sort()
    
    # write out result, checking schema along the way
    
    fieldnames = the_dict_schema.keys()
    ofp = openfile('user_info_combo.json.gz', 'w')
    ocsv = csv.DictWriter(openfile('user_info_combo.csv.gz', 'w'), fieldnames=fieldnames)
    ocsv.writeheader()
    
    for uid in uidset:
        data = uic[uid]
        check_schema(uid, data, the_ds=the_dict_schema, coerce=True)
        if ('enrollment_course_id' not in data) and ('certificate_course_id' not in data):
            print "Oops!  missing course_id in user_info_combo line: inconsistent SQL?"
            print "data = %s" % data
            print "Suppressing this row"
            continue
        row_course_id = data.get('enrollment_course_id', data.get('certificate_course_id', ''))
        if not row_course_id==course_id:
            print "Oops!  course_id=%s in user_info_combo line: inconsistent with expected=%s" % (row_course_id, course_id)
            print "data = %s" % data
            print "Suppressing this row"
            continue
        ofp.write(json.dumps(data) + '\n')
        ocsv.writerow(data)
    
