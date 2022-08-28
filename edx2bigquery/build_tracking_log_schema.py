#!/usr/bin/python
#
# build schema for tracking logs based on reading a bunch of tracking logs
# keep examples along the way.
#

import sys
import gzip
import json

efn = 'tracking_log_schema_examples.json'
sfn = 'schema_tracking_log.json'

def add_example(line):
    open(efn, 'a').write(line)

schema = json.loads(open(sfn).read())

def schema_names_unique(the_schema):
    names = []
    for ent in the_schema:
        if ent['name'] in names:
            print("oops!  duplicate entry %s in %s" % (ent['name'], the_schema))
            raise
        names.append(ent['name'])
        if ent['type']=='RECORD':
            schema_names_unique(ent['fields'])

# check legality
schema_names_unique(schema['tracking_log'])

def save_schema():
    open(sfn,'w').write(json.dumps(schema, indent=4))

def schema_get_key(key, the_schema):
    for ent in the_schema:
        if ent['name']==key:
            return ent
    return False

if True:
    def check_schema(line, data, cur_schema, level=0, seq=None):
        if seq is None:
            seq = []
        for key, val in data.items():
            if not type(val)==dict:
                if type(val)==int:
                    the_type = 'INTEGER'
                elif type(val)==float:
                    the_type = 'FLOAT'
                else:
                    the_type = "STRING"
                ent = schema_get_key(key, cur_schema)
                if ent:
                    if ent['type']==the_type:
                        continue
                    elif the_type=='STRING' and ent['type']=='INTEGER' and val=="":
                        continue
                    elif the_type=='STRING' and ent['type']=='INTEGER':
                        # integer ok, if string parses to integer
                        try:
                            xint = int(val)
                            continue
                        except Exception as err:
                            pass
                    elif the_type=='STRING' and ent['type']=='RECORD' and val=="":
                        continue
                    elif the_type=='STRING' and ent['type'] in ['FLOAT', 'TIMESTAMP', 'BOOLEAN']:
                        continue
                    elif the_type=='INTEGER' and ent['type'] in ['FLOAT', 'STRING']:
                        continue
                    elif the_type=='STRING' and ent['type']=='RECORD':
                        print("warning!  need %s=%s, but ent=%s" % (key, the_type, ent))
                        print("record: ", line)
                        continue
                    else:
                        print("oops!  need %s=%s, but ent=%s" % (key, the_type, ent))
                        print("record: ", line)
                        sys.exit(-1)
                newent = { 'name': key, 'type': the_type}
                cur_schema.append(newent)
                print("%s [%d] <%s> updated schema, added %s" % (' '*level*4, level,  ','.join(seq), newent))
                add_example(line)
                save_schema()
            else:
                ent = schema_get_key(key, cur_schema)
                if ent:
                    if ent['type']=='RECORD':
                        check_schema(line, val, ent['fields'], level=level+1, seq=seq+[key])
                    else:
                        print("oops!  need %s=RECORD, but ent=%s" % (key, ent))
                        print("record: ", line)
                        sys.exit(-1)
                else:
                    subschema = []
                    newent = {'name': key, 'type': 'RECORD', 'fields': subschema}
                    cur_schema.append(newent)
                    print("%s [%d] <%s> updated schema, added %s" % (' '*level*4, level, ','.join(seq), newent))
                    add_example(line)
                    save_schema()
                    check_schema(line, val, subschema, level=level+1, seq=seq+[key])
        
#=============================================================================

def process_file(fn):
    cnt = 0
    
    print("Building schema from file %s" % fn)
    
    if fn.endswith('.gz'):
        fp = gzip.GzipFile(fn)
    else:
        fp = open(fn)
    
    for k in fp:
        cnt += 1
        try:
            data = json.loads(k)
        except Exception as err:
            print("Oops, failed to parse line %d: %s" % (cnt, k))
            continue
        check_schema(k, data, schema['tracking_log'], level=0)
        # print "schema is now ", json.dumps(schema['tracking_log'], indent=4)

#-----------------------------------------------------------------------------

for fn in sys.argv[1:]:
    # print fn
    process_file(fn)
