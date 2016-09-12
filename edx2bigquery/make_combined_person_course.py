#!/usr/bin/python

import os, sys
import json
import datetime
from path import path
import gsutil
import bqutil

def do_combine(course_id_set, project_id, outdir="DATA", nskip=0,
               output_project_id=None, output_dataset_id=None, output_bucket=None,
               use_dataset_latest=False,
               extract_subset_tables=True,
               ):
    '''
    Combine individual person_course tables (from the set of specified course_id's) to create one single large
    person_course table.  Do this by downloading each file, checking to make sure they all have the same
    fields, concatenating, and uploading back to bigquery.  This is cheaper than doing a select *, and also
    uncovers person_course files which have the wrong schema (and it works around BQ's limitation on large
    result sizes).  The result is stored in the course_report_latest dataset (if use_dataset_latest), else 
    in course_report_ORG, where ORG is the configured organization name.

    If extract_subset_tables is True, then the subset of those who viewed (ie "participants"), and the subset
    of those who enrolled for IDV, are extracted and saved as person_course_viewed, and person_course_idv.
    (those are created using a select *, for efficiency, despite the cost).
    '''

    print "="*77
    print "Concatenating person course datasets from the following courses:"
    print course_id_set
    print "-"*77

    outdir = path(outdir)
    if not outdir.exists():
        os.mkdir(outdir)
        
    ofnset = []
    cnt = 0
    for course_id in course_id_set:
        gb = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest)
        ofn = outdir / ('person_course_%s.csv.gz' % (course_id.replace('/', '__')))
        ofnset.append(ofn)

        if (nskip>0) and ofn.exists():
            print "%s already exists, not downloading" % ofn
            sys.stdout.flush()
            continue

        if ofn.exists():
            fnset = gsutil.get_gs_file_list(gb)
            local_dt = gsutil.get_local_file_mtime_in_utc(ofn)
            fnb = 'person_course.csv.gz'
            if not fnb in fnset:
                print "%s/%s missing!  skipping %s" % (gb, fnb, course_id)
                continue
            if (fnb in fnset) and (local_dt >= fnset[fnb]['date']):
                print "%s already exists with date %s (gs file date %s), not re-downloading" % (ofn, local_dt, fnset[fnb]['date'])
                sys.stdout.flush()
                continue
            else:
                print "%s already exists but has date %s (gs file date %s), so re-downloading" % (ofn, local_dt, fnset[fnb]['date'])
                sys.stdout.flush()

        cmd = 'gsutil cp %s/person_course.csv.gz %s' % (gb, ofn)
        print "Retrieving %s via %s" % (course_id, cmd)
        sys.stdout.flush()
        os.system(cmd)
        cnt += 1
        #if cnt>2:
        #    break

    org = course_id_set[0].split('/',1)[0]

    ofn = "person_course_%s_%s.csv" % (org, datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    print "="*77
    print "Combining CSV files to produce %s" % ofn
    sys.stdout.flush()

    if (nskip>1) and os.path.exists(ofn):
        print "%s already exists, not downloading" % ofn
    else:
        first = 1
        header = None
        for zfn in ofnset:
            if first:
                cmd = "zcat %s > %s" % (zfn, ofn)
                header = os.popen("zcat %s | head -1" % zfn).read().strip()
                firstfn = zfn
            else:
                cmd = "zcat %s | tail -n +2 >> %s" % (zfn, ofn)	# first row is header; don't keep when concatenating
            print cmd
            first = 0
            new_header = os.popen("zcat %s | head -1" % zfn).read().strip()
            if not header == new_header:
                print "==> Warning!  header mismatch for %s vs %s" % (zfn, firstfn)
                print "    %s has: %s" % (firstfn, header)
                print "    but %s has: %s" % (zfn, new_header)
            sys.stdout.flush()
            os.system(cmd)

    gb = gsutil.gs_path_from_course_id('course_report_%s' % org, gsbucket=output_bucket)

    print "="*77
    print "Uploading combined CSV file to google cloud storage in bucket: %s" % gb
    sys.stdout.flush()
    cmd = "TMPDIR=/var/tmp gsutil cp -z csv %s %s/" % (ofn, gb)
    print cmd
    os.system(cmd)

    gsfn = gb + '/' + ofn
    print "Combined person_course dataset CSV download link: %s" % gsutil.gs_download_link(gsfn)

    # import into BigQuery
    crname = ('course_report_%s' % org)
    if use_dataset_latest:
        crname = 'course_report_latest'
    dataset = output_dataset_id or crname
    table = ofn[:-4].replace('-','_')
    
    print "Importing into BigQuery as %s:%s.%s" % (project_id, dataset, table)
    sys.stdout.flush()
    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_person_course.json' % mypath
    the_schema = json.loads(open(SCHEMA_FILE).read())['person_course']

    bqutil.load_data_to_table(dataset, table, gsfn, the_schema, format='csv', skiprows=1, project_id=output_project_id)

    msg = ''
    msg += "Combined person-course dataset, with data from:\n"
    msg += str(course_id_set)
    msg += "\n\n"
    msg += "="*100 + "\n"
    msg += "CSV download link: %s" % gsutil.gs_download_link(gsfn)

    bqutil.add_description_to_table(dataset, table, msg, append=True, project_id=output_project_id)
    
    # copy the new table (which has a specific date in its name) to a generically named "person_course_latest"
    # so that future SQL queries can simply use this as the latest person course table
    print "-> Copying %s to %s.person_course_latest" % (table, dataset)
    bqutil.copy_bq_table(dataset, table, "person_course_latest")

    if extract_subset_tables:
        do_extract_subset_person_course_tables(dataset, table)

    print "Done"
    sys.stdout.flush()


def do_extract_subset_person_course_tables(the_dataset, pc_table):
    '''
    Extract (from the latest person_course table, specified by dataset and table), rows for which
    viewed = True (as person_course_viewed), and verified_enroll_time is not Null (for person_course_idv).
    '''
    the_sql = "SELECT * from [%s.%s] where viewed" % (the_dataset, pc_table)
    tablename = "person_course_viewed"

    try:
        ret = bqutil.create_bq_table(the_dataset, tablename, the_sql, 
                                     overwrite=True,
                                     allowLargeResults=True,
        )
    except Exception as err:
        print "ERROR! Failed on SQL="
        print the_sql
        raise
    print "  --> created %s" % tablename

    the_sql = "SELECT * from [%s.%s] where verified_enroll_time is not NULL" % (the_dataset, pc_table)
    tablename = "person_course_idv"

    try:
        ret = bqutil.create_bq_table(the_dataset, tablename, the_sql, 
                                     overwrite=True,
                                     allowLargeResults=True,
        )
    except Exception as err:
        print "ERROR! Failed on SQL="
        print the_sql
        raise
    
    print "  --> created %s" % tablename
