#!/usr/bin/python
#
# save edx grades persistent data to bigquery table
#
# part of the edx2bigquery package.

import gzip
import os
import json

from . import gsutil
from . import bqutil

import unicodecsv as csv

from path import Path as path
from . import load_course_sql


def fix_course_ids(row_dict, column="course_id"):
    """
    Course ids are coming from edX in their URL
    form. So we apply something like course_id.split(":")[-1].replace("+", "/")
    to get the course id in its expected format.

    :param row_dict: A dictionary representing a row
    :param column: A string representing the column to be replaced
    :type row_dict: dict
    :type column: str
    :return: the modified row
    :rtype: dict
    """
    row_dict[column] = row_dict.get(column, "").split(":")[-1].replace("+", "/")
    return row_dict



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


def cleanup_rows_from_grade_persistent(csvfn, tempfn, field_to_fix="passed_timestamp"):
    """
    Removes the null values from grades_persistentcoursegrade.csv.gz.
    The function also fixes course ids by changing them from their
    edX URL format to their usual format. For instance,
    course-v1:MITx+STL.162x+2T2017 should be MITx/STL.162x/2T2017.

    This operation permanently modifies the CSV.

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
                row_dict = remove_nulls_from_row(row, field_to_fix)
                row_dict = fix_course_ids(row_dict)
                write_csv.writerow(row_dict)
    os.rename(tempfn, csvfn)


def upload_grades_persistent_data(cid, basedir, datedir, use_dataset_latest=False, subsection=False):
    """
    Upload grades_persistent csv.gz to Google Storage,
    create the BigQuery table,
    then insert the data into the table.

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

    sdir = load_course_sql.find_course_sql_dir(cid,
                                               basedir=basedir,
                                               datedir=datedir,
                                               use_dataset_latest=(use_dataset_latest),
                                               )

    csvfn = sdir / csv_name
    tempfn = sdir / temp_name

    mypath = os.path.dirname(os.path.realpath(__file__))
    the_schema = json.loads(open('%s/schemas/schema_%s.json' % (mypath, table)).read())[table]

    if not os.path.exists(csvfn):
        print("[edx2bigquery] make_grades_persistent: missing file %s, skipping" % csvfn)
        return

    if not subsection:
        cleanup_rows_from_grade_persistent(csvfn, tempfn)
    else:
        cleanup_rows_from_grade_persistent(csvfn, tempfn, field_to_fix="first_attempted")

    gsutil.upload_file_to_gs(csvfn, gsdir / csv_name, options="-z csv", verbose=True)

    dataset = bqutil.course_id2dataset(cid, use_dataset_latest=use_dataset_latest)
    bqutil.create_dataset_if_nonexistent(dataset)  # create dataset if not already existent

    bqutil.load_data_to_table(dataset,
                              table,
                              gsdir / csv_name,
                              the_schema,
                              format="csv",
                              skiprows=1)
