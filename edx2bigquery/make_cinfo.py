#!/usr/bin/python
#
# make course info listings

import os
# import csv
import unicodecsv as csv
from . import gsutil
from . import bqutil
import json
from .load_course_sql import find_course_sql_dir

def do_course_listings(course_listings_fn):
    dataset = 'courses'
    table = 'listings'
    bqutil.create_dataset_if_nonexistent(dataset)
    mypath = os.path.dirname(os.path.realpath(__file__))

    gsfn = gsutil.gs_path_from_course_id('courses') / 'listings.csv'
    gsutil.upload_file_to_gs(course_listings_fn, gsfn)

    schema = json.loads(open('%s/schemas/schema_course_listings.json' % mypath).read())['course_listings']
    bqutil.load_data_to_table(dataset, table, gsfn, schema, wait=True, format='csv', skiprows=1)
    
    
