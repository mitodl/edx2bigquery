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


def do_save_grades_persistent(cid, caset_in, xbundle, datadir, log_msg, use_dataset_latest=False):
    '''
    Save course axis data to bigquery

    cid = course_id
    caset = list of course axis data in dict format
    xbundle = XML bundle of course (everything except static files)
    datadir = directory where output files should be written
    log_msg = list of messages about processing errors and issues
    '''

    # BigQuery requires data to fit within a schema; let's make sure our lines all fit the schema
    mypath = os.path.dirname(os.path.realpath(__file__))
    the_schema = json.loads(open('%s/schemas/schema_grades_persistent.json' % mypath).read())['grades_persistent']
    dict_schema = schema2dict(the_schema)

    caset = copy.deepcopy(caset_in)

    datadir = path(datadir)
    cafn = datadir / 'course_axis.json'
    xbfn = datadir / ('xbundle_%s.xml' % (cid.replace('/', '__')))
    fp = open(cafn, 'w')
    linecnt = 0

    for ca in caset:
        linecnt += 1
        ca['course_id'] = cid
        data = ca['data']
        if data and not type(data) == dict:
            try:
                ca['data'] = json.loads(data)  # make it native, for mongo
            except Exception as err:
                print "failed to create json for %s, error=%s" % (data, err)
        if ca['start'] is not None:
            ca['start'] = str(ca['start'])  # datetime to string
        if ca['due'] is not None:
            ca['due'] = str(ca['due'])  # datetime to string
        if (ca['data'] is None) or (ca['data'] == ''):
            ca.pop('data')
        check_schema(linecnt, ca, the_ds=dict_schema, coerce=True)
        try:
            # db.course_axis.insert(ca)
            fp.write(json.dumps(ca) + '\n')
        except Exception as err:
            print "Failed to save!  Error=%s, data=%s" % (err, ca)
    fp.close()

    # upload axis.json file and course xbundle
    gsdir = path(gsutil.gs_path_from_course_id(cid, use_dataset_latest=use_dataset_latest))
    if 1:
        gsutil.upload_file_to_gs(cafn, gsdir, options="-z json", verbose=False)
        gsutil.upload_file_to_gs(xbfn, gsdir, options='-z xml', verbose=False)

    # import into BigQuery
    dataset = bqutil.course_id2dataset(cid, use_dataset_latest=use_dataset_latest)
    bqutil.create_dataset_if_nonexistent(dataset)  # create dataset if not already existent
    table = "grades_persistent"
    bqutil.load_data_to_table(dataset, table, gsdir / (cafn.basename()), the_schema)

    msg = "=" * 100 + '\n'
    msg += "Grades Persistent for %s\n" % (cid)
    msg += "=" * 100 + '\n'
    msg += '\n'.join(log_msg)
    msg = msg[:16184]  # max message length 16384

    bqutil.add_description_to_table(dataset, table, msg, append=True)

    print "    Done - inserted %s records into grades_persistent" % len(caset)


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

