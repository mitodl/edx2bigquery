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
csv.field_size_limit(13107200)

def process_file(course_id, basedir=None, datedir=None, use_dataset_latest=False):

    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest=use_dataset_latest)

    cdir = lfp
    print "Processing %s from files in %s" % (course_id, cdir)
    sys.stdout.flush()

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_user_info_combo.json' % mypath
    
    the_dict_schema = schema2dict(json.loads(open(SCHEMA_FILE).read())['user_info_combo'])
    
    uic = defaultdict(dict)		# dict with key = user_id, and val = dict to be written out as JSON line
    
    def copy_elements(src, dest, fields, prefix="", skip_empty=False):
        for key in fields:
            if skip_empty and (not key in src):
                src[key] = None
            if src[key]=='NULL':
                continue
            if key=='course_id' and src[key].startswith('course-v1:'):
                # special handling for mangled "opaque keys" version of course_id, e.g. course-v1:MITx+6.00.2x_3+1T2015
                src[key] = src[key].split(':',1)[1].replace('+','/')
            dest[prefix + key] = src[key]
    
    def openfile(fn_in, mode='r', add_dir=True):
        if add_dir:
            fn = cdir / fn_in
        else:
            fn = fn_in
        if (not os.path.exists(fn)) and (not fn.endswith('.gz')):
            fn += ".gz"
        if mode=='r' and not os.path.exists(fn):
            newfn = convert_sql(fn)		# try converting from *.sql file, if that exists
            if not newfn:
                return None			# failure, no file found, return None
            fn = newfn
        if fn.endswith('.gz'):
            return gzip.GzipFile(fn, mode)
        return open(fn, mode)
    
    def tsv2csv(fn_in, fn_out):
        import csv
        fp = openfile(fn_out, 'w', add_dir=False)
        csvfp = csv.writer(fp)
        for line in openfile(fn_in, add_dir=False):
            csvfp.writerow(line[:-1].split('\t'))
        fp.close()
    
    def convert_sql(fnroot):
        '''
        Returns filename if suitable file exists or was created by conversion of tab separated values to comma separated values.
        Returns False otherwise.
        '''
        if fnroot.endswith('.gz'):
            fnroot = fnroot[:-3]
        if fnroot.endswith('.csv'):
            fnroot = fnroot[:-4]
        if os.path.exists(fnroot + ".csv"):
            return fnroot + ".csv"
        if os.path.exists(fnroot + ".csv.gz"):
            return fnroot + ".csv.gz"
        if os.path.exists(fnroot + ".sql") or os.path.exists(fnroot + ".sql.gz"):
            infn = fnroot + '.sql'
            outfn = fnroot + '.csv.gz'
            print "--> Converting %s to %s" % (infn, outfn)
            tsv2csv(infn, outfn)
            return outfn
        return False

    nusers = 0
    fields = ['username', 'email', 'is_staff', 'last_login', 'date_joined']
    for line in csv.DictReader(openfile('users.csv')):
        uid = int(line['id'])
        copy_elements(line, uic[uid], fields)
        uic[uid]['user_id'] = uid
        nusers += 1
        uic[uid]['y1_anomalous'] = None
        uic[uid]['edxinstructordash_Grade'] = None
        uic[uid]['edxinstructordash_Grade_timestamp'] = None
    
    print "  %d users loaded from users.csv" % nusers

    fp = openfile('profiles.csv')
    if fp is None:
        print "--> Skipping profiles.csv, file does not exist"
    else:
        nprofiles = 0
        fields = ['name', 'language', 'location', 'meta', 'courseware', 
                  'gender', 'mailing_address', 'year_of_birth', 'level_of_education', 'goals', 
                  'allow_certificate', 'country', 'city']
        for line in csv.DictReader(fp):
            uid = int(line['user_id'])
            copy_elements(line, uic[uid], fields, prefix="profile_")
            nprofiles += 1
        print "  %d profiles loaded from profiles.csv" % nprofiles
    
    fp = openfile('enrollment.csv')
    if fp is None:
        print "--> Skipping enrollment.csv, file does not exist"
    else:
        nenrollments = 0
        fields = ['course_id', 'created', 'is_active', 'mode', ]
        for line in csv.DictReader(fp):
            uid = int(line['user_id'])
            copy_elements(line, uic[uid], fields, prefix="enrollment_")
            nenrollments += 1
        print "  %d enrollments loaded from profiles.csv" % nenrollments
    
    # see if from_mongodb files are present for this course; if so, merge in that data
    mongodir = cdir.dirname() / 'from_mongodb'
    if mongodir.exists():
        print "--> %s exists, merging in users, profile, and enrollment data from mongodb" % mongodir
        sys.stdout.flush()
        fp = gzip.GzipFile(mongodir / "users.json.gz")
        fields = ['username', 'email', 'is_staff', 'last_login', 'date_joined']
        nadded = 0
        for line in fp:
            pdata = json.loads(line)
            uid = int(pdata['_id'])
            if not uid in uic:
                copy_elements(pdata, uic[uid], fields, skip_empty=True)
                uic[uid]['user_id'] = uid
                nadded += 1
        fp.close()
        print "  %d additional users loaded from %s/users.json.gz" % (nadded, mongodir)
                
        fp = gzip.GzipFile(mongodir / "profiles.json.gz")
        fields = ['name', 'language', 'location', 'meta', 'courseware', 
                  'gender', 'mailing_address', 'year_of_birth', 'level_of_education', 'goals', 
                  'allow_certificate', 'country', 'city']
        nadd_profiles = 0
        def fix_unicode(elem, fields):
            for k in fields:
                if (k in elem) and elem[k]:
                    elem[k] = elem[k].encode('utf8')

        for line in fp:
            pdata = json.loads(line.decode('utf8'))
            uid = int(pdata['user_id'])
            if not uic[uid].get('profile_name', None):
                copy_elements(pdata, uic[uid], fields, prefix="profile_", skip_empty=True)
                fix_unicode(uic[uid], ['profile_name', 'profile_mailing_address', 'profile_goals', 'profile_location', 'profile_language'])
                uic[uid]['y1_anomalous'] = 1
                nadd_profiles += 1
        fp.close()
        print "  %d additional profiles loaded from %s/profiles.json.gz" % (nadd_profiles, mongodir)
                
        # if datedir is specified, then do not add entries from mongodb where the enrollment happened after the datedir cutoff
        cutoff = None
        if datedir:
            cutoff = "%s 00:00:00" % datedir

        fp = gzip.GzipFile(mongodir / "enrollment.json.gz")
        fields = ['course_id', 'created', 'is_active', 'mode', ]
        nadd_enrollment = 0
        n_removed_after_cutoff = 0
        for line in fp:
            pdata = json.loads(line.decode('utf8'))
            uid = int(pdata['user_id'])
            if not uic[uid].get('enrollment_course_id', None):
                if cutoff and (pdata['created'] > cutoff) and (uic[uid].get('y1_anomalous')==1):	# remove if enrolled after datedir cutoff
                    uic.pop(uid)
                    n_removed_after_cutoff += 1
                else:
                    copy_elements(pdata, uic[uid], fields, prefix="enrollment_", skip_empty=True)
                    nadd_enrollment += 1
        fp.close()
        print "  %d additional enrollments loaded from %s/enrollment.json.gz" % (nadd_enrollment, mongodir)

        print "     from mongodb files, added %s (of %s) new users (%s profiles, %s enrollments, %s after cutoff %s)" % (nadded - n_removed_after_cutoff,
                                                                                                                         nadded, nadd_profiles, nadd_enrollment,
                                                                                                                         n_removed_after_cutoff,
                                                                                                                         cutoff)
        sys.stdout.flush()

    # See if instructor grade reports are present for this course; if so, merge in that data
    edxinstructordash = cdir.dirname() / 'from_edxinstructordash'
    if edxinstructordash.exists():
        print "--> %s exists, merging in users, profile, and enrollment data from_edxinstructordash" % edxinstructordash
        sys.stdout.flush()

        grade_report_fn = ( edxinstructordash / 'grade_report.csv' )
        fp = openfile( grade_report_fn, add_dir=False )
        if fp is None:
            print "--> Skipping grade_report.csv, file does not exist in dir from_edxinstructordash"
        nadded = 0
        print fp
        for line in csv.DictReader(fp):
            uid = int(line['Student ID'])
            fields = [ 'Grade', 'Grade_timestamp' ]
                     #['course_id','Student ID','Email','Username','Grade' ]
                     #'Enrollment Track',' Verification Status','Certificate Eligible','Certificate Delivered','Certificate Type' ]
            copy_elements(line, uic[uid], fields, prefix="edxinstructordash_")
            nadded += 1
        fp.close()
        print "  %d grades loaded from %s/grade_report.csv" % (nadded, edxinstructordash )
        sys.stdout.flush()


    fp = openfile('certificates.csv')
    if fp is None:
        print "--> Skipping certificates.csv, file does not exist"
    else:
        for line in csv.DictReader(fp):
            uid = int(line['user_id'])
            fields = ['download_url', 'grade', 'course_id', 'key', 'distinction', 'status', 
                      'verify_uuid', 'download_uuid', 'name', 'created_date', 'modified_date', 'error_reason', 'mode',]
            copy_elements(line, uic[uid], fields, prefix="certificate_")
            if 'user_id' not in uic[uid]:
                uic[uid]['user_id'] = uid
    
    # sanity check for entries with user_id but missing username
    nmissing_uname = 0
    for uid, entry in uic.iteritems():
        if (not 'username' in entry) or (not entry['username']):
            nmissing_uname += 1
            if nmissing_uname < 10:
                print "missing username: %s" % entry
    print "--> %d entries missing username" % nmissing_uname
    sys.stdout.flush()
    
    # sanity check for entries missing course_id
    nmissing_cid = 0
    for uid, entry in uic.iteritems():
        if (not 'enrollment_course_id' in entry) or (not entry['enrollment_course_id']):
            nmissing_cid += 1
            entry['enrollment_course_id'] = course_id
    print "--> %d entries missing enrollment_course_id (all fixed by setting to %s)" % (nmissing_cid, course_id)
    sys.stdout.flush()

    fp = openfile('user_id_map.csv')
    if fp is None:
        print "--> Skipping user_id_map.csv, file does not exist"
    else:
        for line in csv.DictReader(fp):
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
        try:
            ocsv.writerow(data)
        except Exception as err:
            print "failed to write data=%s" % data
            raise
    
    print "Done with make_user_info_combo for %s" % course_id
    sys.stdout.flush()
