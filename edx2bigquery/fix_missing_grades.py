#!/usr/bin/python

import os, sys
import gzip
import json
import gsutil
import time
try:
    import edxapi
except:
    from edxcut import edxapi
import datetime

from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, parseCourseIdField


TIME_TO_WAIT = 300 # Every 5 minutes

def fix_missing_grades( course_id, 
                        getGrades=None,
                        download_only=None,
                        course_id_type='opaque', # Assume newer courses with format 'course-v1:HarvardX+course+term'
                        org_list=None, # org list is passed in order to properly grab data from edX instructor dashboard
                        basedir=None, 
                        datedir=None, 
                        use_dataset_latest=False):

    lfp = find_course_sql_dir(course_id=course_id, basedir=basedir)# , org_list=org_list)
    print lfp
    gradereport_dir = lfp + '/from_edxinstructordash'
    print gradereport_dir

    if not gradereport_dir.exists():
        os.mkdir(gradereport_dir)

    # Download grade report for single course
    def do_grade_report_export( ofn_dir, ofn, course_id):

        # Properly convert to opaque, if needed
        # Use Case: course_list contains transparent course ids, 
        # but need to pass opaque course id for edX instructor dashboard
        # course_id_type can be set using --course-id-type opaque, for example, on the command line
        # Important to properly pass the original course id, as it exists in the edX platform
	course_id = parseCourseIdField( course=course_id, org_list=org_list, course_id_type=course_id_type )
        print 'Processing course %s' % course_id
        sys.stdout.flush()
        
        try:
            # Make Grade Report Request
            getGrades.make_grade_report_request( course_id )
            
            # Check current set of grade reports
	    grade_reports_dict = getGrades.get_grade_reports( course_id, ofn_dir )

            if download_only:
                    print 'Download grade report only specified'
                    sys.stdout.flush()
		    getGrades.get_latest_grade_report( grade_reports_dict, ofn, ofn_dir )

            else:
		    # Keep checking until the Grade Report is Ready
		    grade_report_ready = False
		    while not grade_report_ready:
			current_time = datetime.datetime.now()
			print "Checking grade report status at %s" % current_time
			sys.stdout.flush()
			time.sleep( TIME_TO_WAIT )
			if grade_report_download_ready( course_id ):
			    grade_reports_dict = getGrades.get_grade_reports( course_id, ofn_dir )
			    getGrades.get_latest_grade_report( grade_reports_dict, ofn, ofn_dir )
			    grade_report_ready = True
	
        except Exception as err:
            print str(err)
            grade_report_downloaded = False

	    # Try one last time
	    try:
		    # If failure, then just try grabbing the latest grade report, just in case
		    grade_reports_dict = getGrades.get_grade_reports( course_id, ofn_dir )
		    getGrades.get_latest_grade_report( grade_reports_dict, ofn, ofn_dir )
		    grade_report_downloaded = True
            except Exception as err:
                    print 'Failure on second attempt %s' % str(err)
                    sys.stdout.flush()
		    raise

            if grade_report_downloaded:
                print 'Success on second attempt %s'
                sys.stdout.flush()
            else:
                print 'Failure on second attempt %s' % str(err)
                sys.stdout.flush()
	        raise


    def grade_report_download_ready( course_id ):
        
        getGrades.set_course_id( course_id)
        if getGrades.list_instructor_tasks()['tasks']:
            return False
        else:
            return True

    # Do single Course
    do_grade_report_export( gradereport_dir,  "grade_report.csv", course_id)





