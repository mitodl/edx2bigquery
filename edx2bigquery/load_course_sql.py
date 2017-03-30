#!/usr/bin/python
#
# File:   load_course_sql.py
# Date:   15-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# load course SQL files into BigQuery via Google Cloud Storage
#
# Needs gsutil to be setup and working.
# Uses bqutil to do the interface to BigQuery.
#
# Usage: 
#
#    python load_course_sql.py COURSE_ID
#
# or
#
#    python load_course_sql.py --y2
#
# Uploads SQL files to GS if requested (use --upload-to-gs flag)
#

import os
import sys
#import auth
import json
import time
import glob
import re
import datetime
import pytz
import gzip
import bqutil
import gsutil
import unicodecsv as csv
from path import path
from gsutil import get_gs_file_list

#-----------------------------------------------------------------------------

def find_course_sql_dir(course_id, basedir, datedir=None, use_dataset_latest=False, verbose=True):
    basedir = path(basedir or '')

    course_dir = course_id.replace('/','__')

    # find the directory modulo date, first
    lfp = (basedir or '.') / course_dir 
    if not os.path.exists(lfp):
        # maybe course directory uses dashes instead of __ (due to edX convention)?
        olfp = [ lfp ]
        lfp = (basedir or '.') / course_id.replace('/','-') 
        if not os.path.exists(lfp):
            # maybe course directory doesn't have the initial ORG- prefix (Harvard's local convention)
            olfp.append(lfp) 
            lfp = (basedir or '.') / course_id.split('/',1)[1].replace('/','-') 
            if not os.path.exists(lfp):
                msg = "Error!  Cannot find course SQL directory %s or %s" % (olfp , lfp)
                print msg
                raise Exception(msg)

    if use_dataset_latest:	# overrides datedir
        # find the directory with the latest date, and use that date, for any local SQL accesses
        datedirs = glob.glob(lfp / '20*-*-*')
        datedirs.sort()
        if not datedirs:
            msg = "[find_course_sql_dir] use_dateset_latest=True, but no date directories found in %s!" % (lfp)
            print msg
            raise Exception(msg)
        datedir = path(datedirs[-1]).basename()
        if verbose:
            print "[find_course_sql_dir] using latest datedir = %s for %s" % (datedir, course_id)

    if datedir is not None:
        lfp = lfp / datedir
        
    if not os.path.exists(lfp):
        msg = "Error!  Cannot find course SQL directory %s" % (lfp)
        print msg
        raise Exception(msg)

    return lfp

def get_course_sql_dirdate( course_id, lfp, datedir, use_dataset_latest ):
    '''
    Function to return the sql dirdate (latest or specified)
    lfp is the find_course_sql_dir output (e.g.: Org/Code-Term/YYYY-MM-DD)
    '''
    if use_dataset_latest or datedir is not None:
        datedir = lfp.basename()
        print "[get_course_sql_dirdate] set dirdate = %s for %s" % (datedir, course_id)
        return datedir
    else: 
        msg = "[get_course_sql_dirdate] No date directories found in %s!" % (lfp)
        print msg
        return None

#-----------------------------------------------------------------------------
def makeCourseIdRegex( org_list ):
    '''
    Function to create Regexp to search for transparent ID's
    for a given organization, based on multiple org name values
    Returns Regex pattern
    '''

    PRE = '(^'
    POST = ')'
    org_regex_mid = ''
    org_regex = ''

    # Multiple values in the org list can happen when:
    # There may be errors in edX setting the org name
    # Or, if there are joint projects between universities (VJx => MIT/Harvard)
    if len(org_list) > 1:

        # Add '|' between each org value in the list
        # (e.g.: HarvardX|VJx )
        for org in org_list:
            if org != org_list[-1]:
                org_regex_mid = org_regex_mid + str(org) + str('|')
            else:
                org_regex_mid = org_regex_mid + str(org)

    # Only one value in the list
    else:
        org_regex_mid = org_list[0]

    # Construct org regex pattern
    org_regex = str( PRE ) + str( org_regex_mid ) + str( POST )
    return org_regex

def parseCourseIdField( course, 
                        org_list,
                        course_id_type='transparent'):

    # There are 2 formats to process
    # 1) Transparent Key Format => 
    #    <dirname>/cnameprefix/cnameterm 
    #    [This is pre-March 2015, when it was introduced in Hx Logs]
    # 2) Opaque Key Identifier => "course-v1:HarvardX+cnameprefix+cnameterm"
    #    "HarvardX/cnameprefix/cnameterm"
    # By default, output course id in Transparent Key Format
    # If course_id_type = 'opaque' (by setting --course_id_type opaque in the command line
    # then convert transparent key input format to opaque key format

    # Patterns to look for detecting course id type
    import re
    org_regex = makeCourseIdRegex( org_list )
    COURSE_ID_KEY_FORMAT = [{'key_type': 'transparent', 'regex': re.compile( org_regex )},
                            {'key_type': 'opaque', 'regex': re.compile('([^:]*:)')},
                           ]

    try:

        for id_key in COURSE_ID_KEY_FORMAT:

            # Transparent Key
            m = id_key['regex'].match( course )

            if (m and id_key['key_type'] == 'transparent'):
                dirname, cnameprefix, cnameterm = course.split('/')

                # Translate Transparent => Opaque
                if course_id_type == 'opaque':
                    opaque_id = str('course-v1:') + course.replace('/', '+')
                    cname = opaque_id
                else:    
                    cname = course
                break

            # Opaque Key
            elif m and id_key['key_type'] == 'opaque':
                course_version_prefix = m.group().replace(':','')
                course_prefix, course_version = course_version_prefix.split('-') # Might need this in the future for course versioning
                opaque_id = course.replace(course_version_prefix, '')
                transparent_id = opaque_id.replace('+', '/')
                transparent_id = transparent_id.replace(':', '')
                dirname, cnameprefix, cnameterm = transparent_id.split('/')

                if course_id_type == 'opaque':
                    cname = course
                # Translate Opaque to Transparent
                else:
                    cname = transparent_id
                break
            else:
                cname = course

    except ValueError:
        cname = course
        pass

    return cname

#-----------------------------------------------------------------------------

def openfile(fn, mode='r'):
    if (not os.path.exists(fn)) and (not fn.endswith('.gz')):
        fn += ".gz"
    if mode=='r' and not os.path.exists(fn):
        return None			# failure, no file found, return None
    if fn.endswith('.gz'):
        return gzip.GzipFile(fn, mode)
    return open(fn, mode)

def tsv2csv(fn_in, fn_out):
    fp = openfile(fn_out, 'w')
    csvfp = csv.writer(fp)
    for line in openfile(fn_in):
        csvfp.writerow(line[:-1].split('\t'))
    fp.close()

#-----------------------------------------------------------------------------

def fix_opaque_keys(data, field):
    '''
    Convert opaque key field to traditional format.  For example:

    block-v1:MITx+6.01x+4T2015+type@sequential+block@813ef6a87f744d70a188abf9acbd97de -> i4x://MITx/6.01x/sequential/813ef6a87f744d70a188abf9acbd97de
    course-v1:MITx+6.01x+4T2015 -> MITx/6.01x/4T2015
    '''
    value = data[field]
    newval = value
    if value.startswith('block-v1:'):
        nfields = value.split('block-v1:',1)[1].split('+')
        newval = 'i4x://%s/%s/%s/%s' % (nfields[0], nfields[1], nfields[3].split('@',1)[1], nfields[4].split('@',1)[1])
    elif value.startswith('course-v1'):
        nfields = value.split('course-v1:',1)[1].split('+')
        newval = '%s/%s/%s' % (nfields[0], nfields[1], nfields[2])
    data[field] = newval

def rephrase_studentmodule_opaque_keys(fn_sm):
    '''
    Generate rephrased studentmodule, with opaque key entries for module_id and course_id translated
    into traditional format.
    '''
    fn_sm = path(fn_sm)
    orig_sm_fn = '%s/studentmodule_orig.csv.gz' % (fn_sm.dirname())
    cmd = 'cp %s %s' % (fn_sm, orig_sm_fn)
    print "  Running %s" % cmd
    sys.stdout.flush()
    os.system(cmd)
    ofp = openfile(fn_sm, 'w')
    smfp = openfile(orig_sm_fn)
    cdr = csv.DictReader(smfp)
    first = True
    for entry in cdr:
        if first:
            odw = csv.DictWriter(ofp, fieldnames=cdr.fieldnames)
            odw.writeheader()
            first = False
        fix_opaque_keys(entry, 'module_id')
        fix_opaque_keys(entry, 'course_id')
        odw.writerow(entry)
    ofp.close()
    print "Rephrased %s -> %s to convert opaque keys syntax to standard module_id and course_id format" % (orig_sm_fn, fn_sm)
    sys.stdout.flush()

#-----------------------------------------------------------------------------

def load_sql_for_course(course_id, gsbucket="gs://x-data", basedir="X-Year-2-data-sql", datedir="2014-09-21", 
                        do_gs_copy=False,
                        use_dataset_latest=False):
    '''
    Load SQL files into google cloud storage then import into BigQuery.

    Datasets are typically named by course_id, with "__" replacing "/", and "_" replacing "."

    If use_dataset_latest then "_latest" is appended to the dataset name.  
    Thus, the latest SQL dataset can always be put in a consistently named dataset.
    '''
    
    print "Loading SQL for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest=use_dataset_latest)

    print "Using this directory for local files: ", lfp
    sys.stdout.flush()
                          
    # convert studentmodule if necessary

    fn_sm = lfp / 'studentmodule.csv.gz'
    if not fn_sm.exists():
        fn_sm = lfp / 'studentmodule.csv'
        if not fn_sm.exists():
            fn_sm = lfp / 'studentmodule.sql.gz'
            if not fn_sm.exists():
                fn_sm = lfp / 'studentmodule.sql'
                if not fn_sm.exists():
                    print "Error!  Missing studentmodule.[sql,csv][.gz]"
            if fn_sm.exists():	# have .sql or .sql.gz version: convert to .csv
                newfn = lfp / 'studentmodule.csv.gz'
                print "--> Converting %s to %s" % (fn_sm, newfn)
                tsv2csv(fn_sm, newfn)
                fn_sm = newfn

    if fn_sm.exists():
        # rephrase studentmodule if it's using opaque keys
        fline = ''
        smfp = openfile(fn_sm)
        fline = smfp.readline()	# skip first line - it's a header
        fline = smfp.readline()
        if 'block-v1:' in fline or 'course-v1' in fline:
            rephrase_studentmodule_opaque_keys(fn_sm)

    def convert_sql(fnroot):
        if os.path.exists(fnroot + ".csv") or os.path.exists(fnroot + ".csv.gz"):
            return
        if os.path.exists(fnroot + ".sql") or os.path.exists(fnroot + ".sql.gz"):
            infn = fnroot + '.sql'
            outfn = fnroot + '.csv.gz'
            print "--> Converting %s to %s" % (infn, outfn)
            tsv2csv(infn, outfn)

    # convert sql files if necesssary
    fnset = ['users', 'certificates', 'enrollment', "profiles", 'user_id_map', 'rolecourse', 'roleforum']
    for fn in fnset:
        convert_sql(lfp / fn)

    local_files = glob.glob(lfp / '*')

    # if using latest date directory, also look for course_image.jpg one level up
    if use_dataset_latest:
        print lfp.dirname()
        ci_files = glob.glob(lfp.dirname() / 'course_image.jpg')
        if ci_files:
            local_files += list(ci_files)
            print "--> local course_image file: %s" % ci_files

    gsdir = gsutil.gs_path_from_course_id(course_id, gsbucket=gsbucket, use_dataset_latest=use_dataset_latest)

    local = pytz.timezone ("America/New_York")

    if do_gs_copy:
        try:
            fnset = get_gs_file_list(gsdir)
        except Exception as err:
            fnset = []
        
        def copy_if_newer(fn, fnset, options='-z csv,json'):
            statbuf = os.stat(fn)
            mt = datetime.datetime.fromtimestamp(statbuf.st_mtime)
            
            # do some date checking to upload files which have changed, and are newer than that on google cloud storage
            local_dt = local.localize(mt, is_dst=None)
            utc_dt = local_dt.astimezone (pytz.utc)

            fnb = os.path.basename(fn)
            if fnb in fnset and fnset[fnb]['date'] > utc_dt:
                print "...%s already copied, skipping" % fn
                sys.stdout.flush()
                return
            elif fnb in fnset:
                print "...%s already exists, but has date=%s and mtime=%s, re-uploading" % (fn, fnset[fnb]['date'], mt)

            gsutil.upload_file_to_gs(fn, gsdir / fnb, options=options, verbose=True)

        for fn in local_files:
            fnb = os.path.basename(fn)
            if fnb=='course_image.jpg':
                copy_if_newer(fn, fnset, options='-a public-read')
            if not (fnb.endswith('.csv') or fnb.endswith('.json') or fnb.endswith('.csv.gz') 
                    or fnb.endswith('.json.gz') or fnb.endswith('.mongo.gz')):
                print "...unknown file type %s, skipping" % fn
                sys.stdout.flush()
                continue
            copy_if_newer(fn, fnset)

    # load into bigquery
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    bqutil.create_dataset_if_nonexistent(dataset)
    mypath = os.path.dirname(os.path.realpath(__file__))

    # load user_info_combo
    uicfn = lfp / 'user_info_combo.json.gz'
    if uicfn.exists():
        uic_schema = json.loads(open('%s/schemas/schema_user_info_combo.json' % mypath).read())['user_info_combo']
        bqutil.load_data_to_table(dataset, 'user_info_combo', gsdir / "user_info_combo.json.gz", uic_schema, wait=False)
    else:
        print "--> File %s does not exist, not loading user_info_combo into BigQuery" % uicfn
    
    # load studentmodule
                
    if fn_sm.exists():
        schemas = json.loads(open('%s/schemas/schemas.json' % mypath).read())
        cwsm_schema = schemas['courseware_studentmodule']
        bqutil.load_data_to_table(dataset, 'studentmodule', gsdir / fn_sm.basename(), cwsm_schema, format='csv', wait=False, skiprows=1)
    else:
        print "--> Not loading studentmodule: file %s not found" % fn_sm


        
