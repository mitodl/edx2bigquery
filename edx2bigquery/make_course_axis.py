#!/usr/bin/python

import os
import sys
import edx2course_axis
import load_course_sql
import axis2bigquery

def process_course(course_id, basedir, datedir, use_dataset_latest, verbose=False):
    sdir = load_course_sql.find_course_sql_dir(course_id, 
                                               basedir=basedir,
                                               datedir=datedir,
                                               use_dataset_latest=use_dataset_latest,
                                               )
    edx2course_axis.DATADIR = sdir
    edx2course_axis.VERBOSE_WARNINGS = verbose
    fn = sdir / 'course.xml.tar.gz'
    if not os.path.exists(fn):
        fn = sdir / 'course-prod-analytics.xml.tar.gz'
        if not os.path.exists(fn):
            print "---> oops, cannot generate course axis for %s, file %s (or 'course.xml.tar.gz') missing!" % (course_id, fn)
            sys.stdout.flush()
            return

    # TODO: only create new axis if the table is missing, or the course axis is not already created

    try:
        edx2course_axis.process_xml_tar_gz_file(fn,
                                               use_dataset_latest=use_dataset_latest)
    except Exception as err:
        print err
        # raise
