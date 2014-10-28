#!/usr/bin/python
#
# Analyze courseware_studentmodule and tracking logs specifically for capa problems.
#
# 1. Find subset of studentmodule entries reating to problems
#    extract table ("problem_analysis") of:
#
#    course_id, user_id, problem_url_name, item: { answer_id, response, correctness} , npoints, attempts, seed, done, grade, created
#
# 2. Extract from tracking logs all problem_check attempts and fill in information in the "attempts_DAY" table inside {dataset}_pcday 
#    for each day.
#

import os, sys
import csv
import re
import json
import gsutil
import bqutil

from path import path
from collections import defaultdict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir, openfile

csv.field_size_limit(1310720)

def analyze_problems(course_id, basedir=None, datedir=None, force_recompute=False):

    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir)
    
    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_problem_analysis.json' % mypath
    the_schema = json.loads(open(SCHEMA_FILE).read())['problem_analysis']
    the_dict_schema = schema2dict(the_schema)

    smfn = lfp / 'studentmodule.csv'
    smfp = openfile(smfn)
    if smfp is None:
        print "--> [analyze_problems] oops, missing %s, cannot process course %s" % (smfn, course_id)
        return

    print "[analyze_problems] processing %s for course %s" % (smfn, course_id)
    sys.stdout.flush()

    # if table already exists, then assume we've already done analysis for this course
    if not force_recompute:
        dataset = bqutil.course_id2dataset(course_id)
        table = 'problem_analysis'
        tables = bqutil.get_list_of_table_ids(dataset)
        if table in tables:
            print "--> %s.%s already exists in BigQuery...skipping (use --force-recompute to not skip)" % (dataset, table)
            return

    data = []
    nlines = 0
    cnt = 0
    for line in csv.DictReader(smfp):
        nlines += 1
        uid = int(line['student_id'])
        if not line['module_type']=='problem':	# bug in edX platform?  too many entries are type=problem
            continue
        mid = line['module_id']
        (org, num, category, url_name) = mid.rsplit('/',3)
        if not category=='problem':		# filter based on category info in module_id
            continue
        try:
            state = json.loads(line['state'].replace('\\\\','\\'))
        except Exception as err:
            print "oops, failed to parse state in studentmodule entry, err=%s" % str(err)
            print "    %s" % repr(line)
            continue
        
        if 'correct_map' not in state:
            continue
        
        if not state['correct_map']:	# correct map = {} is not of interest
            continue

        answers = state['student_answers']

        items = []
        for aid, cm in state['correct_map'].iteritems():
            item = { 'answer_id': aid,
                     'correctness': cm['correctness'],
                     'correct_bool' : cm['correctness']=='correct',
                     'npoints': cm['npoints'],
                     'msg': cm['msg'],
                     'hint': cm['hint'],
                     'response': json.dumps(answers.get(aid, '')),
                     }
            items.append(item)

        try:
            entry = {'course_id': line['course_id'],
                     'user_id': line['student_id'],
                     'problem_url_name': url_name,
                     'item': items,
                     'attempts': int(state['attempts']),
                     'done': state['done'],
                     'grade': float(line['grade']),
                     'max_grade': float(line['max_grade']),
                     'created': line['created'],
                     }
        except Exception as err:
            print "---> [%d] Oops, error in transcribing entry, err=%s" % (cnt, str(err))
            print "     state = %s" % state
            raise

        check_schema(cnt, entry, the_ds=the_dict_schema, coerce=True)
        data.append(entry)
        cnt += 1

    print "%d problem lines extracted from %d lines in %s" % (cnt, nlines, smfn)

    if cnt==0:
        print "--> No final data: not saving or importing into BigQuery"
        return

    # write out result
    ofnb = 'problem_analysis.json.gz'
    ofn = lfp / ofnb
    ofp = openfile(ofn, 'w')
    for entry in data:
        ofp.write(json.dumps(entry) + '\n')
    ofp.close()

    # upload and import
    gsfn = gsutil.gs_path_from_course_id(course_id) / ofnb
    gsutil.upload_file_to_gs(ofn, gsfn)

    bqutil.load_data_to_table(dataset, table, gsfn, the_schema, wait=True)
        
