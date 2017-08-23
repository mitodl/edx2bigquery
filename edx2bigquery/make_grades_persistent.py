#!/usr/bin/python
#
# save edx grades persistent data to bigquery table
#
# part of the edx2bigquery package.

import copy
import gzip
import os
import json
import tarfile
import re

from path import path

import gsutil
import bqutil

import unicodecsv as csv

from path import path
import load_course_sql
from make_grading_policy_table import already_exists
import datetime
from check_schema_tracking_log import check_schema, schema2dict
from edx2bigquery import gsutil, bqutil




def remove_nulls_from_row(row_dict, column):
    """Replace the "NULL" values with an empty string

    :param row_dict: A dictionary representing a row
    :param column: A string representing the column to be replaced
    :type row_dict: dict
    :type column: str
    :return: the modified row
    :rtype: dict
    """
    if row_dict[column] == "NULL":
        row_dict[column] = ""
    return row_dict


def remove_nulls_from_grade_persistent(csvfn, tempfn):
    """Removes the null values from grades_persistentcoursegrade.csv.gz. This operation permanently modifies the CSV.

    :param csvfn: The path of the csv.gz to be modified
    :param tempfn: The path of the temporary csv.gz
    :type csvfn: str
    :type tempfn: str
    """
    with gzip.open(csvfn, "r") as open_csv:
        csv_dict = csv.DictReader(open_csv)
        with gzip.open(tempfn, "w+") as write_csv_file:
            write_csv = csv.DictWriter(write_csv_file, fieldnames=csv_dict.fieldnames)
            write_csv.writeheader()
            for row in csv_dict:
                row_dict = remove_nulls_from_row(row, "passed_timestamp")
                write_csv.writerow(row_dict)
    os.rename(tempfn, csvfn)


def upload_grades_persistent_data(cid, basedir, datedir, use_dataset_latest=False, subsection=False):
    """Upload grades_persistent csv.gz to Google Storage, create the BigQuery table, then insert the data into the table

    :param cid: the course id
    :param basedir: the base directory path
    :param datedir: the date directory name (represented as YYYY-MM-DD)
    :param use_dataset_latest: should the most recent dataset be used?
    :param subsection: should grades_persistentsubsection be uploaded?
    :type cid: str
    :type basedir: str
    :type datedir: str
    :type use_dataset_latest: bool
    :type subsection: bool
    """
    gsdir = path(gsutil.gs_path_from_course_id(cid, use_dataset_latest=use_dataset_latest))

    if subsection:
        csv_name = "grades_persistentsubsectiongrade.csv.gz"
        temp_name = "grades_persistentsubsectiongrade_temp.csv.gz"
        table = "grades_persistent_subsection"
    else:
        csv_name = "grades_persistentcoursegrade.csv.gz"
        temp_name = "grades_persistentcoursegrade_temp.csv.gz"
        table = "grades_persistent"


    csvfn = '%s/%s/%s/%s' % (basedir, cid.replace('/', '__'), datedir, csv_name)
    tempfn = '%s/%s/%s/%s' % (basedir, cid.replace('/', '__'), datedir, temp_name)

    mypath = os.path.dirname(os.path.realpath(__file__))
    the_schema = json.loads(open('%s/schemas/schema_%s.json' % (mypath, table)).read())[table]

    if not subsection:
        remove_nulls_from_grade_persistent(csvfn, tempfn)

    gsutil.upload_file_to_gs(csvfn, gsdir, options="-z csv", verbose=True)

    dataset = bqutil.course_id2dataset(cid, use_dataset_latest=use_dataset_latest)
    bqutil.create_dataset_if_nonexistent(dataset)  # create dataset if not already existent


    bqutil.load_data_to_table(dataset,
                              table,
                              gsdir / csv_name,
                              the_schema,
                              format="csv",
                              skiprows=1)