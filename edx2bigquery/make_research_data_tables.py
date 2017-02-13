#!/usr/bin/python
#
# File:   make_research_data_tables.py
# Date:   21-Jan-16
# Author: G. Lopez
#
# extract/create research data tables from BQ

import sys
import bqutil
import gsutil
import datetime
import json
import path
import collections
import gzip
from load_course_sql import find_course_sql_dir


#-----------------------------------------------------------------------------
# CONSTANTS
#-----------------------------------------------------------------------------

# Course Axis
RDP_COURSE_AXIS = 'course_axis'

# Person Course
RDP_PERSON_COURSE = 'person_course'
RDP_PERSON_COURSE_SURVEY = 'person_course_survey'
RDP_PERSON_COURSE_DAY = 'person_course_day'
RDP_PERSON_COURSE_DAY_ALL = 'person_course_day_allcourses' 

# Enrollment
RDP_ENROLLDAY_ALL = 'enrollday_all'
RDP_ENROLLMENT_EVENTS = 'enrollment_events'

# Discussion Forums
RDP_FORUM_EVENTS = 'forum_events'
RDP_FORUM_PERSON = 'forum_person'
RDP_FORUMS = 'forum'
RDP_FORUM_POSTS = 'forum_posts'

# Problems
RDP_COURSE_ITEM = 'course_item'
RDP_COURSE_PROBLEM = 'course_problem'
RDP_PERSON_PROBLEM_WIDE = 'person_problem_wide'
RDP_PERSON_PROBLEM = 'person_problem'
RDP_PROBLEM_ANALYSIS = 'problem_analysis'
RDP_PROBLEM_EVENTS = 'problem_events'
RDP_PROBLEM_CHECK = 'problem_check'
RDP_PERSON_ITEM = 'person_item'

# Time on Task
RDP_TIME_ON_TASK = 'time_on_task'
RDP_TIME_ON_TASK_TOTALS = 'time_on_task_totals'

# Video
RDP_VIDEO_AXIS = 'video_axis'
RDP_VIDEO_STATS = 'video_stats'
RDP_VIDEO_STATS_DAY = 'video_stats_day'
RDP_PERSON_VIDEO_WATCHED = 'person_course_video_watched'

# Pre course survey
RDP_PRECOURSE_SURVEY = 'survey_precourse_download'

FILE_EXT = ".csv.gz"

# List of Research Data Products to extract
RESEARCH_DATA_PRODUCTS = collections.OrderedDict( [\
			# Person Course
			#( RDP_COURSE_AXIS, RDP_COURSE_AXIS+ FILE_EXT ),
			#( RDP_PERSON_COURSE, RDP_PERSON_COURSE + FILE_EXT ), # Exported through 'do_all' command
			#( RDP_PERSON_COURSE_SURVEY, RDP_PERSON_COURSE_SURVEY + FILE_EXT ), # Exported through survey workflow
			( RDP_PERSON_COURSE_DAY, RDP_PERSON_COURSE_DAY + FILE_EXT ),
			# Enrollment
			#( RDP_ENROLLDAY_ALL, RDP_ENROLLDAY_ALL + FILE_EXT ),
			#( RDP_ENROLLMENT_EVENTS, RDP_ENROLLMENT_EVENTS + FILE_EXT ),
			# Discussion Forums
			( RDP_FORUM_EVENTS, RDP_FORUM_EVENTS + FILE_EXT ),
			##( RDP_FORUM_PERSON, RDP_FORUM_PERSON + FILE_EXT ),
			#( RDP_FORUMS, RDP_FORUMS + FILE_EXT ), # Data exported through 'load_forum' command, during processing while in memory. Issue with exporting tables with nested json otherwise
			( RDP_FORUM_POSTS, RDP_FORUM_POSTS + FILE_EXT ),
			# Time on Task
			( RDP_TIME_ON_TASK,RDP_TIME_ON_TASK + FILE_EXT ),
			( RDP_TIME_ON_TASK_TOTALS, RDP_TIME_ON_TASK_TOTALS + FILE_EXT ),
			# Video 
			( RDP_VIDEO_AXIS, RDP_VIDEO_AXIS + FILE_EXT ),
			( RDP_VIDEO_STATS, RDP_VIDEO_STATS + FILE_EXT ),
			( RDP_VIDEO_STATS_DAY, RDP_VIDEO_STATS_DAY + FILE_EXT ),
			( RDP_PERSON_VIDEO_WATCHED, RDP_PERSON_VIDEO_WATCHED + FILE_EXT ),
			# Problems
			( RDP_COURSE_PROBLEM, RDP_COURSE_PROBLEM + FILE_EXT ),
			( RDP_PERSON_PROBLEM, RDP_PERSON_PROBLEM + FILE_EXT ),
			( RDP_PROBLEM_CHECK, RDP_PROBLEM_CHECK + FILE_EXT ),
			( RDP_COURSE_ITEM, RDP_COURSE_ITEM + FILE_EXT ),
			( RDP_PERSON_PROBLEM, RDP_PERSON_PROBLEM + FILE_EXT ),
			#( RDP_PROBLEM_ANALYSIS, RDP_PROBLEM_ANALYSIS + FILE_EXT ),
			# Pre Course Survey
			#( RDP_PRECOURSE_SURVEY, RDP_PRECOURSE_SURVEY+FILE_EXT ),
			])

class ResearchDataProducts(object):

    def __init__(self, course_id_set, basedir='', datedir='', output_project_id=None, nskip=0, 
                 output_dataset_id=None, 
                 output_bucket=None,
                 use_dataset_latest=False,
                 only_step=None,
                 end_date=None,
                 ):
        '''
	Extract Research Datasets, based on defined list of tables
        '''
        
        if only_step and ',' in only_step:
            only_step = only_step.split(',')
        self.only_step = only_step

        self.end_date = end_date;

        if not course_id_set:
            print "ERROR! Must specify list of course_id's for report.  Aborting."
            return

        org = course_id_set[0].split('/',1)[0]	# extract org from first course_id
        self.org = org

        self.output_project_id = output_project_id

        crname = ('course_report_%s' % org)
        if use_dataset_latest:
            crname = 'course_report_latest'
        self.dataset = output_dataset_id or crname

        self.gsbucket = gsutil.gs_path_from_course_id(crname, gsbucket=output_bucket)
        self.course_id_set = course_id_set
	course_id = course_id_set

        #course_datasets = [ bqutil.course_id2dataset(x, use_dataset_latest=use_dataset_latest) for x in course_id_set]
        #course_datasets_dict = { x:bqutil.course_id2dataset(x, use_dataset_latest=use_dataset_latest) for x in course_id_set}
	course_dataset = bqutil.course_id2dataset( course_id, use_dataset_latest=use_dataset_latest )

	self.rdp_matrix = collections.OrderedDict()
        #for course_id in course_datasets_dict.keys():

	print "[researchData] Processing data for course %s" % ( course_id )
	sys.stdout.flush()
	for rdp in RESEARCH_DATA_PRODUCTS.keys():
		try:
			table = bqutil.get_bq_table_info( course_dataset, rdp )
			#table = bqutil.get_bq_table_info( course_id, rdp )
			if table is not None:
				#[print "[researchData] %s found for %s dataset" % ( rdp, course_datasets_dict[ course_id ] )
				print "[researchData] %s found" % ( rdp )
				sys.stdout.flush()
				if rdp not in self.rdp_matrix:
					#self.rdp_matrix[ str(rdp) ] = cd
					self.rdp_matrix[ str(rdp) ] = ( course_id, course_dataset )
					#self.rdp_matrix[ str(rdp) ] = ( course_id, course_id )
				else:
					self.rdp_matrix[ str(rdp) ].append( (course_id, course_dataset ) )
					#self.rdp_matrix[ str(rdp) ].append( (course_id,  course_id ) )

		except Exception as err:
			#print str(err)
			print "[researchData] Err: %s not found for %s dataset" % ( rdp, course_id )

	# Extract to archival storage
	for researchDataProduct in self.rdp_matrix:
	
		the_dataset = self.rdp_matrix[ researchDataProduct ][1]
		course_id = self.rdp_matrix[ researchDataProduct ][0] #the_dataset.replace( '__', '/' )
		self.extractResearchData( course_id=course_id, tablename=researchDataProduct, the_dataset=the_dataset, rdp=researchDataProduct, rdp_format='csv', output_bucket=output_bucket, basedir=basedir, datedir=datedir )

        print "="*100
        print "Done extracting Research Data tables -> %s" % RESEARCH_DATA_PRODUCTS.keys()
        print "="*100
        sys.stdout.flush()

    def extractResearchData( self, course_id, tablename, the_dataset=None, rdp=None, rdp_format='csv', output_bucket=None, basedir='', datedir='', do_gzip=True):
	'''
		Get research data output into tables and archive onto server
	'''
	
	# Archive location
	if course_id is not None: # Individual Course Research Data Products

		self.gsp = gsutil.gs_path_from_course_id( course_id=course_id, gsbucket=output_bucket, use_dataset_latest=True )
		gsfilename  = "%s/%s" % ( self.gsp, RESEARCH_DATA_PRODUCTS[ rdp ] )

	else: 
		print "ERROR! Must specify course_id's.  Aborting."
		return

	try:
		# Copy to Google Storage
		msg = "[researchData]: Copying Research Data table %s to %s" % ( tablename, gsfilename )
		print msg
		#gsfilename  = "%s/%s-*.csv.gz" % ( self.gsp, tablename ) # temp
		gsfilename  = "%s/%s.csv.gz" % ( self.gsp, tablename ) # temp
		ret = bqutil.extract_table_to_gs( the_dataset, tablename, gsfilename, format=rdp_format, do_gzip=True, wait=True)
		msg = "[researchData]: CSV download link: %s" % gsutil.gs_download_link( gsfilename )
		print msg
		sys.stdout.flush()
	
	except Exception as err:

		print str(err)
		if ('BQ Error creating table' in str(err) ):
			msg = "[researchData]: Retrying... by sharding."
			print msg
			sys.stdout.flush()
			gsfilename  = "%s/%s-*.csv.gz" % ( self.gsp, tablename )
			print gsfilename
			sys.stdout.flush()
			ret = bqutil.extract_table_to_gs( the_dataset, tablename, gsfilename, format=rdp_format, do_gzip=True, wait=True)
			msg = "[researchData]: CSV download link: %s" % gsutil.gs_download_link( gsfilename )
			print msg
			sys.stdout.flush()
	

	# Copy from Google Storage to Secure Data Warehouse for archiving
	archiveLocation = find_course_sql_dir(course_id=course_id, basedir=basedir, datedir=datedir, use_dataset_latest=True)
	#time.sleep( CFG.TIME_TO_WAIT_30s ) # delay needed to allow for GS to upload file fully (This should be size dependent, and may not be enough time)
	msg = "[researchData]: Archiving Research Data table %s from %s to %s" % ( tablename, gsfilename, archiveLocation )
	print msg
        sys.stdout.flush()
	gsutil.upload_file_to_gs(src=gsfilename, dst=archiveLocation, verbose=True)

	pass




