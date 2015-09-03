#!/usr/bin/python

import os
import sys
import traceback
import edx2course_axis
import load_course_sql
import axis2bigquery

def process_course(course_id, basedir, datedir, use_dataset_latest, verbose=False, pin_date=None):
    if pin_date:
        datedir = pin_date
    sdir = load_course_sql.find_course_sql_dir(course_id, 
                                               basedir=basedir,
                                               datedir=datedir,
                                               use_dataset_latest=(use_dataset_latest and not pin_date),
                                               )
    edx2course_axis.DATADIR = sdir
    edx2course_axis.VERBOSE_WARNINGS = verbose

    fn_to_try = ['course.xml.tar.gz',
                'course-prod-analytics.xml.tar.gz',
                'course-prod-edge-analytics.xml.tar.gz',
                'course-prod-edx-replica.xml.tar.gz',
            ]

    for fntt in fn_to_try:
        fn = sdir / fntt
        if os.path.exists(fn):
            break

    if not os.path.exists(fn):
        print "---> oops, cannot generate course axis for %s, file %s (or 'course.xml.tar.gz' or 'course-prod-edge-analytics.xml.tar.gz') missing!" % (course_id, fn)
        sys.stdout.flush()
        return

    # TODO: only create new axis if the table is missing, or the course axis is not already created

    try:
        edx2course_axis.process_xml_tar_gz_file(fn,
                                                use_dataset_latest=use_dataset_latest,
                                                force_course_id=course_id)
    except Exception as err:
        print err
        traceback.print_exc()
        
        # raise
