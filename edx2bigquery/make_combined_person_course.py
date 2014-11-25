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
               ):

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
            if local_dt >= fnset[fnb]['date']:
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

    org = course_id.split('/',1)[0]

    ofn = "person_course_%s_%s.csv" % (org, datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    print "="*77
    print "Combining CSV files to produce %s" % ofn
    sys.stdout.flush()

    if (nskip>1) and os.path.exists(ofn):
        print "%s already exists, not downloading" % ofn
    else:
        first = 1
        for zfn in ofnset:
            if first:
                cmd = "zcat %s > %s" % (zfn, ofn)
            else:
                cmd = "zcat %s | tail -n +2 >> %s" % (zfn, ofn)	# first row is header; don't keep when concatenating
            print cmd
            first = 0
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
    
    print "Done"
    sys.stdout.flush()
