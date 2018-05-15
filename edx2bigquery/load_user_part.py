
import os
import sys
import gsutil
import bqutil
import gzip
import json
try:
	from path import Path as path
except:
	from path import path
import unicodecsv as csv

from load_course_sql import find_course_sql_dir

def do_user_part_csv(course_id, basedir=None, datedir=None, 
                  use_dataset_latest=False,
                  verbose=False,
                  pin_date=None):
    sdir = find_course_sql_dir(course_id, 
                               basedir=basedir,
                               datedir=datedir,
                               use_dataset_latest=(use_dataset_latest and not pin_date),
    )
    # upload to google storage
    dfn = sdir / "user_api_usercoursetag.csv.gz"

    if not os.path.exists(dfn):
        print("[load_user_part] Missing %s, skipping" % dfn)
        return

    # reformat True / False to 1/0 for "value" field
    if verbose:
        print("[load_user_part] extracting user partition data from %s" % dfn)
        sys.stdout.flush()

    cdr = csv.DictReader(gzip.GzipFile(dfn))
    fields = cdr.fieldnames
    if verbose:
        print("fieldnames = %s" % fields)
    fixed_data = []
    bmap = {'true': 1, 'false': 0}
    for row in cdr:
        vstr = row['value'].lower()
        row['value'] = bmap.get(vstr, vstr)
        fixed_data.append(row)

    ofnb = 'user_partitions.csv.gz'
    odfn = sdir / ofnb
    with gzip.GzipFile(odfn, 'w') as ofp:
        cdw = csv.DictWriter(ofp, fieldnames=fields)
        cdw.writeheader()
        cdw.writerows(fixed_data)
    if verbose:
        print("[load_user_part] Wrote %d rows of user partition data to %s" % (len(fixed_data), odfn))
        sys.stdout.flush()

    gsdir = path(gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest))
    gsutil.upload_file_to_gs(odfn, gsdir / ofnb, verbose=False)
    
    mypath = os.path.dirname(os.path.realpath(__file__))
    schema = json.loads(open('%s/schemas/schema_user_partitions.json' % mypath).read())['user_partitions']

    # import into BigQuery
    table = "user_partitions"
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    bqutil.load_data_to_table(dataset, table, gsdir / ofnb, schema, format='csv', skiprows=1)
    
    
def already_exists(course_id, use_dataset_latest, table="user_partitions"):
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tables = bqutil.get_list_of_table_ids(dataset)
    return table in tables
