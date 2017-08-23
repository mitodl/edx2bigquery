import json
import os
import re
import tarfile

import unicodecsv as csv
from path import path

import bqutil
import gsutil
import load_course_sql

gpfn = 'grading_policy.json'
#gpfn = 'grading_policy2.json'

def make_gp_table(course_id, basedir=None, datedir=None, 
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

    fn_to_try = ['course.xml.tar.gz',
                'course-prod-analytics.xml.tar.gz',
                'course-prod-edge-analytics.xml.tar.gz',
                'course-prod-edx-replica.xml.tar.gz',
            ]
    
    for fntt in fn_to_try:
        fn = sdir / fntt
        if os.path.exists(fn):
            break
    if not os.path.exists(fn):
        msg = "---> oops, cannot get couese content (with grading policy file) for %s, file %s (or 'course.xml.tar.gz' or 'course-prod-edge-analytics.xml.tar.gz') missing!" % (course_id, fn)
        raise Exception(msg)

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
    

def read_grading_policy_from_tar_file(tfn):
    """read the grading_policy.json file from the *.tar.gz course content file"""
    try:
        tfp = tarfile.open(tfn)
    except Exception as err:
        print "Error!  Cannot open tar file %s" % tfn
        raise 
    gpfn = None
    for fn in tfp.getnames():
        m = re.match('[^/]+/policies/[^/]+/grading_policy.json', fn)
        if m:
            gpfn = fn
            break
    if not gpfn:
        raise Exception("No grading policy file found in %s" % tfn)

    return tfp.extractfile(gpfn).read(), gpfn

def load_grading_policy(gpstr, verbose=False, gpfn=None):
    gp = json.loads(gpstr)
    # print json.dumps(gp, indent=4)
    
    # example json
    # {
    #     "GRADE_CUTOFFS": {
    #         "Passing": 0.59, 
    #         "Good": 0.7, 
    #         "Excellent": 0.85
    #     }, 
    #     "GRADER": [
    #         {
    #             "short_label": "HW1_2", 
    #             "min_count": 1, 
    #             "type": "Homework_1and2", 
    #             "drop_count": 0, 
    #             "weight": 0.046
    #         }, 

    # resulting grading_policy table has one row for each item in GRADER.
    # each item in a grader becomes a column.
    #    rename type -> assignment_type
    #    rename weight -> fraction_of_overall_grade
    # also add items in grade_cutoffs to every row.
    # 
    
    gptab = []
    grader = gp.get('GRADER')
    if not grader or not len(grader):
        raise Exception("Grading policy %s has no GRADER entries, cannot create grading_policy table" % gpfn)

    cutoffs = gp.get('GRADE_CUTOFFS')
    if not cutoffs:
        raise Exception("Grading policy %s has no GRADE_CUTOFFS, cannot create grading_policy table" % gpfn)
    the_cutoffs = {}
    for k, v in cutoffs.items():
        the_cutoffs['overall_cutoff_for_%s' % k.lower()] = v

    fields = ['assignment_type', 'name', 'fraction_of_overall_grade'] + grader[0].keys() + the_cutoffs.keys()
    fields.remove('type')
    fields.remove('weight')

    weights = []
    for g in grader:
        g['assignment_type'] = g['type']
        g.pop('type')
        weights.append(float(g['weight']))
        g['fraction_of_overall_grade'] = g['weight']
        g.pop('weight')
        g.update(the_cutoffs)
        if not 'name' in g:
            g['name'] = g['assignment_type']	# default name is same as assignment_type
        gptab.append(g)
        for key in g:
            if key not in fields:
                fields.append(key)
            
        
    print "[grading_policy] %d assignments = %s" % (len(gptab), [x['assignment_type'] for x in gptab])
    print "[grading_policy] assignment weights = ", weights
    if not abs(sum(weights)-1.0) < 0.001:
        msg = "Error!  Sum of assignment weights in grading policy = %s (should be 1.0)" % sum(weights)
        #raise Exception(msg)
        print msg

    if verbose:
        print "fields = ", fields
        print json.dumps(gptab, indent=4)

    # make schema
    string_fields = ["category", "show_only_average", "section_type", "assignment_type"]
    schema = []
    for field in fields:
        field = field.replace('.','_').replace(' ', '_')
        field = field.replace('+', '_').replace('-', '__')	# for very old, circa 2012 course grading policies
        if ('_id' in field) or ('_label' in field) or (field=='name') or (field in string_fields):
            ftype = 'STRING'
        elif ('_count' in field):
            ftype = 'INTEGER'
        elif ('hide_' in field):
            ftype = 'STRING'
        else:
            ftype = 'FLOAT'
        schema.append({'name': field,
                       'type': ftype,
                   })

    if verbose:
        print "schema = ", json.dumps(schema, indent=4)

    return fields, gptab, schema


# load_grading_policy(gpfn)

def already_exists(course_id, use_dataset_latest, table="grading_policy"):
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tables = bqutil.get_list_of_table_ids(dataset)
    return table in tables