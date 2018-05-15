#!/usr/bin/python

import os, sys
import gzip
import json
import gsutil

try:
	from path import Path as path
except:
	from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir


def mongo_dump_user_info_files(course_id, basedir=None, datedir=None, dbname=None, use_dataset_latest=False):
    '''
    In early SQL tables from edX, enrollment records were deleted when a learner un-enrolled.
    This would then cause that learner's records to no longer appear in the SQL data
    dump to researchers.  This further caused problems because there would be learners
    who received certificates (and appeared in the certificates tables) but weren't
    in the auth_user or auth_userprofile or enrollment tables.

    One way around this was to incrementally load all the user, userprofile, etc. tables
    from every weekly dump, so that if a learner ever appeared as being registered
    in a course, the learner would stay that way.

    This workaround didn't completely solve the problem, however, for early courses,
    and thus additional data had to be manualy requested from edX.

    These issues were all resolved for courses after ~Spring 2014, when un-enrollment
    was changed such that it did not cause deletion of the enrollment record, but
    rather, just a change of the "active" flag within the enrollment record.

    Here, to workaround the problem, for a given course_id, we generate users.csv, profiles.csv, 
    enrollment.csv, certificates.csv, from collections 
    stored in mongodb, for a specified course.  These mongodb collections were curated
    and produced for the Hx and MITx Year 1 reports.
    
    Uses mongoexport.
    '''

    basedir = path(basedir or '')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest=use_dataset_latest)
    mongodir = lfp.dirname() / 'from_mongodb'

    print "[mongo_dump_user_info_files] processing %s, output directory = %s" % (course_id, mongodir)
    if not mongodir.exists():
        os.mkdir(mongodir)

    def do_mongo_export(collection, ofn, ckey='course_id'):
        query = '{"%s": "%s"}' % (ckey, course_id)
        cmd = "mongoexport -d %s -c %s -q '%s' | gzip -9 > %s" % (dbname, collection, query, ofn)
        print "--> %s" % cmd
        sys.stdout.flush()
        os.system(cmd)

    # make users with javascript join

    js = """conn = new Mongo(); 
          db = conn.getDB('%s');
          var cursor = db.student_courseenrollment.find({'course_id': '%s'});
          while (cursor.hasNext()) { 
              var doc = cursor.next();
              udoc = db.auth_user.find({_id: doc.user_id})[0];
              print(JSON.stringify(udoc));
          }
          var course = '%s';
          var cursor = db.certificates_generatedcertificate.find({'course_id': course});
          while (cursor.hasNext()) {
              var doc = cursor.next();
              usc = db.student_courseenrollment.find({'course_id': course, 'user_id': doc.user_id });
              if (usc.length()==0){
                  udoc = db.auth_user.find({_id: doc.user_id})[0];
                  db.auth_userprofile.update({'user_id' : doc.user_id}, {\$addToSet : {courses: course }});
                  print(JSON.stringify(udoc));
              }
          }
         """ % (dbname, course_id, course_id)

    ofn = mongodir / "users.json.gz"
    cmd = 'echo "%s" | mongo --quiet | tail -n +3 | gzip -9 > %s' % (js.replace('\n',''), ofn)
    print "--> %s" % cmd
    sys.stdout.flush()
    os.system(cmd)

    # profiles and enrollment and certificates

    do_mongo_export("auth_userprofile", mongodir / "profiles.json.gz", 'courses')
    do_mongo_export("student_courseenrollment", mongodir / "enrollment.json.gz")
    do_mongo_export("certificates_generatedcertificate", mongodir / "certificates.json.gz")
