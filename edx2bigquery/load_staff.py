
import os
import csv
import gsutil
import bqutil
import json
from load_course_sql import find_course_sql_dir

def do_staff_csv(staff_csv_fn):
    dataset = 'courses'
    table = 'staff'
    bqutil.create_dataset_if_nonexistent(dataset)
    mypath = os.path.dirname(os.path.realpath(__file__))

    gsfn = gsutil.gs_path_from_course_id('courses') / 'staff.csv'
    gsutil.upload_file_to_gs(staff_csv_fn, gsfn)

    schema = json.loads(open('%s/schemas/schema_staff.json' % mypath).read())['staff']
    bqutil.load_data_to_table(dataset, table, gsfn, schema, wait=True, format='csv', skiprows=1)
    
    
