#!/usr/bin/python
#
# save edx grades persistent data to bigquery table
#
# part of the edx2bigquery package.

import copy
import os
import json
import tarfile
import re
import gsutil
import bqutil

import unicodecsv as csv

from path import path
import load_course_sql
import datetime
from check_schema_tracking_log import check_schema, schema2dict





def make_grades_table(course_id, basedir=None, datedir=None,
                  use_dataset_latest=False,
                  verbose=False,
                  pin_date=None):

    if pin_date:
        datedir = pin_date

    sdir = load_course_sql.find_course_sql_dir(course_id, 
                                               basedir=basedir,
                                               datedir=datedir,
                                               use_dataset_latest=(use_dataset_latest and not pin_date),
                                               )


    gpstr, gpfn = read_grading_policy_from_tar_file(fn)
    fields, gptab, schema = load_grading_policy(gpstr, verbose=verbose, gpfn=gpfn)
    
    ofnb = 'grading_policy.csv'
    ofn = sdir / ofnb
    ofp = open(ofn, 'w')
    cdw = csv.DictWriter(ofp, fieldnames=fields)
    cdw.writeheader()
    cdw.writerows(gptab)
    ofp.close()

    # upload to google storage
    gsdir = path(gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest))
    gsutil.upload_file_to_gs(ofn, gsdir / ofnb, verbose=False)
    
    # import into BigQuery
    table = "grading_policy"
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    bqutil.load_data_to_table(dataset, table, gsdir / ofnb, schema, format='csv', skiprows=1)



def already_exists(course_id, use_dataset_latest):
    table = "grading_policy"
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tables = bqutil.get_list_of_table_ids(dataset)
    return table in tables

