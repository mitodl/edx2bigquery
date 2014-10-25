#!/usr/bin/python
#
# save edx course axis data to bigquery table
#
# part of the edx2bigquery package.

import os
import json
import gsutil
import bqutil
import copy
from path import path
from check_schema_tracking_log import check_schema, schema2dict

def do_save(cid, caset_in, xbundle, datadir, log_msg):
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
    the_schema = json.loads(open('%s/schemas/schema_course_axis.json' % mypath).read())['course_axis']
    dict_schema = schema2dict(the_schema)

    caset = copy.deepcopy(caset_in)

    datadir = path(datadir)
    cafn = datadir / 'course_axis.json' 
    xbfn = datadir / ('xbundle_%s.xml' % (cid.replace('/','__')))
    fp = open(cafn, 'w')
    linecnt = 0

    for ca in caset:
        linecnt += 1
        ca['course_id'] = cid
        data = ca['data']
        if data:
            try:
                ca['data'] = json.loads(data)	# make it native, for mongo
            except Exception as err:
                print "failed to create json for %s, error=%s" % (data, err)
        ca['start'] = str(ca['start'])	# datetime to string
        if  ca['due'] is not None:
            ca['due'] = str(ca['due'])	# datetime to string
        if (ca['data'] is None) or (ca['data']==''):
            ca.pop('data')
        check_schema(linecnt, ca, the_ds=dict_schema, coerce=True)
        try:
            # db.course_axis.insert(ca)
            fp.write(json.dumps(ca)+'\n')
        except Exception as err:
            print "Failed to save!  Error=%s, data=%s" % (err, ca)
    fp.close()

    # save course xbundle (XML file without static content)
    # note this includes the grading policy
    #if xbundle is not None:
    #    db.course_axis.insert({'course_id': cid, 'xbundle': xbundle})

    # upload axis.json file and course xbundle
    gsdir = path(gsutil.gs_path_from_course_id(cid))
    if 1:
        gsutil.upload_file_to_gs(cafn, gsdir, options="-z json", verbose=False)
        gsutil.upload_file_to_gs(xbfn, gsdir, options='-z xml', verbose=False)

    # import into BigQuery
    dataset = bqutil.course_id2dataset(cid)
    table = "course_axis"
    bqutil.load_data_to_table(dataset, table, gsdir / (cafn.basename()), the_schema)

    msg = "="*100 + '\n'
    msg += "Course axis for %s\n" % (cid)
    msg += "="*100 + '\n'
    msg += '\n'.join(log_msg)
    msg = msg[:16184]		# max message length 16384
    
    bqutil.add_description_to_table(dataset, table, msg, append=True)

    print "    Done - inserted %s records into course_axis" % len(caset)
