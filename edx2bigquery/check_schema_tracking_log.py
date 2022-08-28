#!/usr/bin/python
#
# see if the schema fits specified tracking log

import sys
import json
import gzip
import copy
from collections import OrderedDict

import os
mypath = os.path.dirname(os.path.realpath(__file__))

sfn = '%s/schemas/schema_tracking_log.json' % mypath

schema = json.loads(open(sfn).read())['tracking_log']

bq2ptype = {'RECORD': dict, 'INTEGER': int, 'STRING': str, 'FLOAT': float, 
            'BOOLEAN': int,
            'TIMESTAMP': str,
            }

def schema2dict(the_schema_in):
    '''
    Convert schema from BigQuery format (list of dicts) to a dict with entries.
    Do this recursively.  Easier to check a schema when using this "dict" format.
    Ensures the original schema is unmodified by side effects.
    '''
    the_schema = copy.deepcopy(the_schema_in)	# make sure not to modify the original schema dict, by copying it
    dict_schema = OrderedDict()
    for ent in the_schema:
        dict_schema[ent['name']] = ent
        if ent['type']=='RECORD':
            ent['dict_schema'] = schema2dict(ent['fields'])
        ent['ptype'] = bq2ptype[ent['type']]
    return dict_schema

ds = schema2dict(schema)

def check_schema(linecnt, data, the_ds=None, path='', coerce=False, the_schema=None):

    global ds

    if the_schema is not None and the_ds is None:
        the_ds = schema2dict(the_schema)

    if the_ds is None:
        the_ds = ds

    for key, val in data.items():
        if val is None:
            continue
        if not key in the_ds:
            raise Exception("[check_schema] Oops! field %s is not in the schema, linecnt=%s, path=%s" % (key, linecnt, path))

        ptype = the_ds[key]['ptype']	# python type corresponding to the BigQuery schema field type

        # allow repeated nested records (see https://cloud.google.com/bigquery/preparing-data-for-bigquery?hl=ja#dataformats)
        if ptype==dict and type(val)==list and the_ds[key].get('mode')=='REPEATED':
            for item in val:
                check_schema(linecnt, item, the_ds[key]['dict_schema'], path=path + '/' + key, coerce=coerce)
            return

        if not type(val)==ptype:
            if type(val)==int and ptype==float:
                continue
            if type(val)==bool and ptype==int:
                continue
            if type(val)==str and ptype==str:
                continue
            if type(val)==str and ptype==str:
                continue

            if coerce:
                if type(val) in [str, str] and ptype==float:
                    try:
                        data[key] = float(val)
                    except Exception as err:
                        print("Error coercing data for key=%s path=%s, val=%s, err=%s" % (key, path, val, str(err)))
                        data[key] = None
                    continue
                if type(val) in [str, str] and ptype==int:
                    try:
                        data[key] = int(val)
                    except Exception as err:
                        print("Error coercing data for key=%s path=%s, val=%s, err=%s" % (key, path, val, str(err)))
                        raise
                    continue
                    
            print("[line %d] mismatch type expected %s got %s for key=%s, val=%s, path=%s" % (linecnt,
                                                                                              ptype, 
                                                                                              type(val), 
                                                                                              key, val,
                                                                                              path))
        if ptype==dict:
            check_schema(linecnt, val, the_ds[key]['dict_schema'], path + "/" + key, coerce=coerce)

#-----------------------------------------------------------------------------

def do_file(fn):
    if fn.endswith('.gz'):
        fp = gzip.GzipFile(fn)
    else:
        fp = open(fn)
    
    cnt = 0
    for line in fp:
        cnt += 1
        data = json.loads(line)
        check_schema(cnt, data, ds)

#-----------------------------------------------------------------------------

if __name__=="__main__":
    cnt = 0
    if len(sys.argv)>1:
        # arguments are filenames; process each file, append "-rephrased" to filename before suffix
        for fn in sys.argv[1:]:
            do_file(fn)

    else:
        cnt = 0
        for line in sys.stdin:
            cnt += 1
            data = json.loads(line)
            check_schema(cnt, data, ds)
        
