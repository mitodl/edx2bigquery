#!/usr/bin/python
#
# create grades_all table containing all sources of grades
# 1) Certificate grades
# 2) Grades persistent
# 3) EdX Instructor Grades (if executed)
#
# part of the edx2bigquery package.

import datetime
import gzip
import os
import sys
import json

import gsutil
import bqutil

import unicodecsv as csv

try:
	from path import Path as path
except:
	from path import path
import load_course_sql

# ---------
# CONSTANTS
# ---------
NULL_DATE = '1900-01-01'
TABLE_GRADES_ANALYSIS = 'grades_analysis'

# ---------
# FUNCTIONS
# ---------

def analyze_grades( course_id,
		    basedir=None,
		    datedir=None,
		    force_recompute=False,
		    use_dataset_latest=False,
		    use_latest_sql_dir=False,
		  ):


    make_grades_latest( course_id,
			force_recompute,
			use_dataset_latest )


def make_grades_latest( course_id, 
			force_recompute,
			use_dataset_latest ):

    dataset = bqutil.course_id2dataset( course_id, 
					use_dataset_latest=use_dataset_latest)
    tablename = TABLE_GRADES_ANALYSIS

    gradingPolicyExists = False
    overall_cutoff = 'overall_cutoff_for_pass'
    try:

        tinfo_gradingpolicy = bqutil.get_bq_table_info(dataset, 'grading_policy')
        assert tinfo_gradingpolicy is not None, "grading_policy table missing... "
	gradingPolicyExists = True
	
	if tinfo_gradingpolicy:
		fields = tinfo_gradingpolicy['schema']['fields']
		for x in fields:
			if 'overall_cutoff' in x['name']:
				overall_cutoff = x['name']

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s " % ( dataset, "grading_policy" )
        sys.stdout.flush()
	pass

    gradesSubsectionExists = False
    try:
        tinfo_gradingsubsection = bqutil.get_bq_table_info(dataset, 'grades_persistent_subsection')
        assert tinfo_gradingsubsection is not None, "grades_persistent_subsection table missing... "
	gradesSubsectionExists = True

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s " % ( dataset, "grades_persistent_subsection" )
        sys.stdout.flush()
	pass

    gradesPersistentExists = False
    try:
        tinfo_gp = bqutil.get_bq_table_info(dataset, 'grades_persistent')
        assert tinfo_gp is not None, "grades_persistent table missing... "
	gradesPersistentExists = True

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s " % ( dataset, "grades_persistent" )
        sys.stdout.flush()
	pass

    uicExists = False
    try:
        tinfo_uic = bqutil.get_bq_table_info(dataset, 'user_info_combo')
        assert tinfo_uic is not None, "user_info_combo table missing... "
	uicExists = True

    except (AssertionError, Exception) as err:
        print " --> Err: missing %s.%s " % ( dataset, "user_info_combo" )
        sys.stdout.flush()
	pass
	
    # Construct conditional select based on existence of Grade Persistent table
    SQL_GP = ''
    SQL_GP_SELECT = '''
			  (CASE
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) ) 
			      THEN uic.edxinstructordash_Grade
			      ELSE uic.certificate_grade
			    END ) AS grade_latest,
			  (CASE
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) ) 
			      THEN TIMESTAMP(uic.edxinstructordash_Grade_timestamp)
			      ELSE uic.certificate_modified_date
			    END ) AS grade_latest_timestamp,
			  (CASE
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) ) 
			      THEN 'edxdashboard'
			      WHEN TIMESTAMP(uic.certificate_modified_date) IS NOT NULL THEN 'certificate'
			      ELSE NULL
			    END ) AS grade_latest_source,
			  CAST(NULL as FLOAT) AS grade_persistent,
			  CAST(NULL as TIMESTAMP) AS grade_persistent_created,
			  CAST(NULL as TIMESTAMP) AS grade_persistent_modified,
			  CAST(NULL as TIMESTAMP) AS grade_persistent_passed_timestamp,
		    '''.format( dataset=dataset, 
		                course_id=course_id,
			        NULL_DATE=NULL_DATE )
    if gradesPersistentExists:

	    SQL_GP = '''
			LEFT JOIN (
			  SELECT
			    *
			  FROM
			    [{dataset}.grades_persistent] ) AS gp
			ON
			  uic.enrollment_course_id = gp.course_id
			  AND uic.user_id = gp.user_id
		      '''.format( dataset=dataset, 
		                  course_id=course_id )
	    SQL_GP_SELECT = '''
			  (CASE
			      WHEN ( IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) AND IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) ) THEN gp.percent_grade
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) )
			      AND IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(gp.modified, TIMESTAMP({NULL_DATE})) ) THEN uic.edxinstructordash_Grade
			      ELSE uic.certificate_grade
			    END ) AS grade_latest,
			  (CASE
			      WHEN ( IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) AND IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) ) THEN gp.modified
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) )
			      AND IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(gp.modified, TIMESTAMP({NULL_DATE})) ) THEN TIMESTAMP(uic.edxinstructordash_Grade_timestamp)
			      ELSE uic.certificate_modified_date
			    END ) AS grade_latest_timestamp,
			  (CASE
			      WHEN ( IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) ) AND IFNULL(gp.modified, TIMESTAMP({NULL_DATE}) ) > IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) ) THEN 'grades_persistent'
			      WHEN ( IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(uic.certificate_modified_date, TIMESTAMP({NULL_DATE}) )
			      AND IFNULL(TIMESTAMP(uic.edxinstructordash_Grade_timestamp), TIMESTAMP({NULL_DATE}) ) > IFNULL(gp.modified, TIMESTAMP({NULL_DATE})) ) THEN 'edxdashboard'
			      WHEN TIMESTAMP(uic.certificate_modified_date) IS NOT NULL THEN 'certificate'
			      ELSE NULL
			    END ) AS grade_latest_source,
			  gp.percent_grade AS grade_persistent,
			  gp.created AS grade_persistent_created,
			  gp.modified AS grade_persistent_modified,
			  gp.passed_timestamp AS grade_persistent_passed_timestamp,
			    '''.format( dataset=dataset, 
			                course_id=course_id,
			                NULL_DATE=NULL_DATE )

    # Construct conditional select based on existence of Grading Policy table
    SQL_GPOLICY = ''
    SQL_GPOLICY_SELECT = 'CAST(NULL as FLOAT) AS grade_overall_cutoff_for_pass,'
    if gradingPolicyExists:

	    SQL_GPOLICY = '''
			LEFT JOIN (
			  SELECT
			    FIRST({overall_cutoff}) as overall_cutoff_for_pass,
			    "{course_id}" AS course_id
			  FROM
			    [{dataset}.grading_policy] ) AS gpolicy
			ON
			  uic.enrollment_course_id = gpolicy.course_id
			  '''
	    SQL_GPOLICY = SQL_GPOLICY.format( dataset=dataset, 
					      course_id=course_id,
					      overall_cutoff=overall_cutoff )
	    SQL_GPOLICY_SELECT = 'gpolicy.overall_cutoff_for_pass AS grade_overall_cutoff_for_pass,'

    # Construct conditional select based on existence of Grades Subsection table
    SQL_GPS = ''
    SQL_GPS_SELECT = 'CAST(NULL as TIMESTAMP) AS grade_persistent_first_attempted_problem_subsection,'
    if gradesSubsectionExists:
	    print 'grades subsection exists'
	    SQL_GPS = '''
			LEFT JOIN (
			  SELECT
			    course_id,
			    user_id,
			    MIN(first_attempted) AS first_attempted_problem_subsection
			  FROM
			    [{dataset}.grades_persistent_subsection]
			  GROUP BY
			    user_id,
			    course_id ) AS gps
			ON
			  uic.enrollment_course_id = gps.course_id
			  AND uic.user_id = gps.user_id
		      '''
	    SQL_GPS = SQL_GPS.format( dataset=dataset, 
				      course_id=course_id )
	    SQL_GPS_SELECT = 'gps.first_attempted_problem_subsection AS grade_persistent_first_attempted_problem_subsection,'
	   

    SQL = '''

	SELECT
	  user_id,
	  course_id,
	  grade_overall_cutoff_for_pass,
	  (
	    CASE
	      WHEN grade_latest >= grade_overall_cutoff_for_pass THEN TRUE
	      ELSE FALSE
	    END ) AS grade_latest_pass,
	  grade_latest,
	  grade_latest_timestamp,
	  grade_latest_source,
	  grade_cert,
	  grade_persistent,
	  grade_edxdash,
	  grade_cert_created_date,
	  grade_cert_modified_date,
	  grade_persistent_created,
	  grade_persistent_modified,
	  grade_persistent_passed_timestamp,
	  grade_persistent_first_attempted_problem_subsection,
	  grade_edxdash_timestamp
	FROM (

		SELECT
		  uic.user_id AS user_id,
		  uic.enrollment_course_id AS course_id,
		  uic.certificate_grade AS grade_cert,
		  uic.edxinstructordash_Grade AS grade_edxdash,
		  uic.certificate_created_date AS grade_cert_created_date,
		  uic.certificate_modified_date AS grade_cert_modified_date,
		  uic.edxinstructordash_Grade_timestamp AS grade_edxdash_timestamp,
		  {sql_gpolicy_select}
		  {sql_gp_select}
		  {sql_gps_select}
		FROM (
		  SELECT
		    user_id,
		    enrollment_course_id,
		    certificate_grade,
		    certificate_created_date,
		    certificate_modified_date,
		    edxinstructordash_Grade,
		    edxinstructordash_Grade_timestamp
		  FROM
		    [{dataset}.user_info_combo]) AS uic
		{sql_gp}
		{sql_gps}
		{sql_gpolicy}
		ORDER BY
		  uic.edxinstructordash_Grade DESC
	)

	  '''

    the_sql = SQL.format( dataset=dataset, 
			  course_id=course_id,
			  sql_gp=SQL_GP,
			  sql_gp_select=SQL_GP_SELECT,
			  sql_gpolicy=SQL_GPOLICY,
			  sql_gpolicy_select=SQL_GPOLICY_SELECT,
			  sql_gps=SQL_GPS,
			  sql_gps_select=SQL_GPS_SELECT )
    print the_sql

    print "[grades_analysis] Creating %s.%s table for %s" % (dataset, TABLE_GRADES_ANALYSIS, course_id)
    sys.stdout.flush()
    print "[grades_analysis] user_info_combo: %s, grading_policy: %s, grades_persistent: %s, grades_persistent_subsection: %s" % ( uicExists,
																   gradingPolicyExists,
																   gradesPersistentExists,
																   gradesSubsectionExists )

    try:
         depends_on = [ '%s.user_info_combo' % dataset ] # Mininum dataset
	 bqdat = None
	 if uicExists or gradingPolicyExists or gradesPersistentExists or gradesSubsectionExists:
		 bqdat = bqutil.get_bq_table( dataset, tablename, the_sql,
					      force_query=force_recompute, 
					      newer_than=datetime.datetime(2018, 6, 5, 00, 00),
					      depends_on=depends_on )
         assert bqdat is not None, "[grades analysis] %s table depends on %s (one or more may not exist)" % ( TABLE_GRADES_ANALYSIS, depends_on )

    except (AssertionError, Exception) as err:
	print str(err)

        print " --> Err: missing %s.%s?  Skipping creation of %s" % ( dataset, depends_on,  TABLE_GRADES_ANALYSIS )
	sys.stdout.flush()
	return

    return bqdat



