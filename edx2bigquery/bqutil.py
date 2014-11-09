#!/usr/bin/python
#
# File:   bqutil.py
# Date:   14-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# common bigquery utility functions

import sys
import time
import json
import datetime

try:
    import getpass
except:
    pass

try:
    from edx2bigquery_config import PROJECT_ID as DEFAULT_PROJECT_ID
except:
    from local_config import PROJECT_ID as DEFAULT_PROJECT_ID

import auth
from collections import OrderedDict

service = auth.build_bq_client() 

projects = service.projects()
datasets = service.datasets()
tables = service.tables()
tabledata = service.tabledata()
jobs = service.jobs()

PROJECT_NAMES = {}				# used to cache project names, key=project_id

def get_project_name(project_id=DEFAULT_PROJECT_ID):
    if project_id in PROJECT_NAMES:		# lookup in cache, first
        return PROJECT_NAMES[project_id]
    for project in projects.list().execute()['projects']:
      if (project['id'] == project_id) or (project['numericId'] == str(project_id)):
          PROJECT_NAMES[project_id] = project['id']
          return project['id']

def course_id2dataset(course_id, dtype=None, use_dataset_latest=False):
    '''
    Generate a dataset name for a given course_id
    BigQuery disallows certain characters in table names, e.g. "-" and "."
    Use this function to keep our mapping centralized and consistent.
    '''
    dataset = course_id.replace('/','__').replace('.','_')	# dataset_id is same as course_dir but with "." -> "_"
    dataset = dataset.replace('-','_')				# also "." -> "_"
    if dtype=='logs':
        dataset += "_logs"
    elif dtype=='pcday':
        dataset += "_pcday"
    elif use_dataset_latest:	# used to store the latest SQL data
        dataset += "_latest"
    return dataset		# default dataset for SQL data

def delete_dataset(dataset, project_id=DEFAULT_PROJECT_ID, delete_contents=False):
      datasets.delete(datasetId=dataset, projectId=project_id, deleteContents=delete_contents).execute()

def create_dataset_if_nonexistent(dataset, project_id=DEFAULT_PROJECT_ID):

  if dataset not in get_list_of_datasets():

      dataset_ref = {'datasetId': dataset,
                     'projectId': project_id}
      dataset = {'datasetReference': dataset_ref}
      dataset = datasets.insert(body=dataset, projectId=project_id).execute()
      return dataset

def get_list_of_datasets(project_id=DEFAULT_PROJECT_ID):
  dataset_list = datasets.list(projectId=project_id, maxResults=1000).execute()

  if 'datasets' in dataset_list:
      dsets = {x['datasetReference']['datasetId']: x for x in dataset_list['datasets']}
      return dsets
  else:
      print "no datasets?"
  return {}

def get_projects(project_id=DEFAULT_PROJECT_ID):
    for project in projects.list().execute()['projects']:
        if (project['id'] == project_id):
            print 'Found %s: %s' % (project_id, project['friendlyName'])

def get_tables(dataset_id, project_id=DEFAULT_PROJECT_ID, verbose=False):
    table_list = tables.list(datasetId=dataset_id, projectId=project_id, maxResults=1000).execute()
    if verbose:
        for current in table_list['tables']:
            print "table: ", current
    if 'tables' not in table_list:
        print "[bqutil] get_tables: oops! dataset=%s, no table info in %s" % (dataset_id, json.dumps(table_list, indent=4))
    return table_list

def get_list_of_table_ids(dataset_id):
    tables_info = get_tables(dataset_id).get('tables', [])
    table_id_list = [ x['tableReference']['tableId'] for x in tables_info ]
    return table_id_list

def get_table_data(dataset_id, table_id, key=None, logger=None, project_id=DEFAULT_PROJECT_ID, 
                   startIndex=None, maxResults=1000000):
    '''
    Retrieve data from a specific BQ table.  Return as a dict, with

    fields      = schema fields
    field_names = name of top-level schema fields
    data        = list of data
    data_by_key = dict of data, with key being the value of the fieldname specified as the key arg

    Arguments:

    key         = dict, e.g. {'name': field_name_for_index, 'keymap': function_on_key_values}
    maxResults  = maximum number of results to return
    startIndex  = zero-based index of starting row to read; make this negative to return from 
                  end of table
    '''
    table = get_bq_table_info(dataset_id, table_id)
    nrows = int(table['numRows'])

    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    table_ref['maxResults'] = maxResults
    if startIndex is not None:
        if startIndex < 0:
            startIndex = nrows + startIndex
        table_ref['startIndex'] = startIndex

    data = tabledata.list(**table_ref).execute()

    fields = table['schema']['fields']
    field_names = [x['name'] for x in fields]

    ret = {'fields': fields,
           'field_names': field_names,
           'numRows': nrows,
           'creationTime': table['creationTime'],
           'lastModifiedTime': table['lastModifiedTime'],
           'data': [],
           'data_by_key': OrderedDict(),
           }

    rows = data.get('rows', [])
    for row in rows:
        values = OrderedDict()
        for i in xrange(0, len(fields)):
            cell = row['f'][i]
            values[field_names[i]] = cell['v']
        ret['data'].append(values)
        if key is not None:
            the_key = values[key['name']]
            if 'keymap' in key:
                the_key = key['keymap'](the_key)
            if the_key not in ret['data_by_key']:
                ret['data_by_key'][the_key] = values

    return ret

def delete_zero_size_tables(dataset_id, verbose=False):
    '''
    Delete tables which have zero rows, in the specified dataset
    '''
    for table_id in get_list_of_table_ids(dataset_id):
        if get_bq_table_size_rows(dataset_id, table_id)==0:
            if verbose:
                print "Deleting %s.%s" % (dataset_id, table_id)
                sys.stdout.flush()
            delete_bq_table(dataset_id, table_id)

def delete_bq_table(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Delete specified BQ table
    '''
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    tables.delete(**table_ref).execute()    

def get_bq_table_size_rows(dataset_id, table_id):
    '''
    Retrieve number of rows of specified BQ table
    '''
    tinfo = get_bq_table_info(dataset_id, table_id)
    if tinfo is not None:
        return int(tinfo['numRows'])
    return None

def bq_timestamp_milliseconds_to_datetime(timestamp):
    '''
    Convert a millisecond timestamp to a python datetime object
    '''
    if timestamp:
        return datetime.datetime.utcfromtimestamp(float(timestamp)/1000.0)
    return None

def get_bq_table_creation_datetime(dataset_id, table_id):
    '''
    Retrieve datetime of table creation
    '''
    tinfo = get_bq_table_info(dataset_id, table_id)
    if tinfo is not None:
        return tinfo['creationTime']
    return None

def get_bq_table_last_modified_datetime(dataset_id, table_id):
    '''
    Retrieve datetime of table last modification
    '''
    tinfo = get_bq_table_info(dataset_id, table_id)
    if tinfo is not None:
        return tinfo['lastModifiedTime']
    return None

def get_bq_table_info(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve metadata about a specific BQ table.
    '''
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    try:
        table = tables.get(**table_ref).execute()
    except Exception as err:
        if 'Not Found' in str(err):
            raise
        table = None
    table['lastModifiedTime'] = bq_timestamp_milliseconds_to_datetime(table['lastModifiedTime'])
    table['creationTime'] = bq_timestamp_milliseconds_to_datetime(table['creationTime'])
    return table

def default_logger(msg):
    print msg

def get_bq_table(dataset, tablename, sql=None, key=None, allow_create=True, force_query=False, logger=default_logger,
                 startIndex=None, maxResults=1000000):
    '''
    Retrieve data for the specified BQ table if it exists.
    If it doesn't exist, create it, using the provided SQL.
    '''
    if force_query:
        create_bq_table(dataset, tablename, sql, logger=logger)
        return get_table_data(dataset, tablename, key=key, logger=logger,
                              startIndex=startIndex, maxResults=maxResults)
    try:
        ret = get_table_data(dataset, tablename, key=key, logger=logger,
                             startIndex=startIndex, maxResults=maxResults)
    except Exception as err:
        if 'Not Found' in str(err) and allow_create and (sql is not None) and sql:
            create_bq_table(dataset, tablename, sql, logger=logger)
            return get_table_data(dataset, tablename, key=key, logger=logger,
                                  startIndex=startIndex, maxResults=maxResults)
        else:
            raise
    return ret

def create_bq_table(dataset_id, table_id, sql, verbose=False, overwrite=False, wait=True, 
                    logger=default_logger, project_id=DEFAULT_PROJECT_ID,
                    output_project_id=DEFAULT_PROJECT_ID):
    '''
    Run SQL query to create a new table.
    '''

    project_ref = dict(projectId=project_id)
    table_ref = dict(datasetId=dataset_id, projectId=output_project_id, tableId=table_id)

    if overwrite in ["append", 'APPEND']:
        wd = "WRITE_APPEND"
    elif overwrite==True:
        wd = "WRITE_TRUNCATE"
    else:
        wd = "WRITE_EMPTY"	

    config = {'query': { 'query': sql,
                         'destinationTable': table_ref,
                         'writeDisposition': wd,
                         }
              }
              
    job_id = 'create_%s_%d' % (table_id, time.time())
    job_ref = {'jobId': job_id,
               'projectId': project_id}
    
    job = {'jobReference': job_ref, 'configuration': config}

    logger("[bqutil] Creating table %s, running job %s" % (table_id, job_id))
    sys.stdout.flush()

    if verbose:
        print job

    for k in range(10):
        try:
            jobret = jobs.insert(body=job, **project_ref).execute()
            break
        except Exception as err:
            print "[bqutil] oops!  Failed to insert job=%s" % job
            if (k==9):
                raise
            if 'HttpError 500' in str(err):
                print err
                print "--> 500 error, retrying in 30 sec"
                time.sleep(30)
                continue
            raise

    if verbose:
        print "job=", json.dumps(job, indent=4)
      
    if verbose:
        job_list = jobs.list( stateFilter=['pending', 'running'], **project_ref).execute()
        print "job list: ", job_list

    if not wait:
        return

    ecnt = 0
    while job.get('status', {}).get('state', None) <> 'DONE':
        if 'status' not in job:
            ecnt += 1
            if (ecnt > 2):
                logger("[bqutil] Error!  no job status?  job ret = %s" % job)
            if (ecnt > 5):
                raise Exception('BQ Error getting job status')
        else:
            ecnt = 0
        try:
            job = jobs.get(**job_ref).execute()
        except Exception as err:
            print "[bqutil] oops!  Failed to execute jobs.get=%s" % (job_ref)

    status = job['status']
    logger( "[bqutil] job status: %s" % status )

    if 'errors' in status:
        logger( "[bqutil] ERROR!  %s" % str(status['errors']) )
        logger( "job = %s" % json.dumps(job, indent=4))
        raise Exception('BQ Error creating table')

    elif status['state']=='DONE':

        nbytes = int(job['statistics']['query']['totalBytesProcessed'])
        logger( "[bqutil] Total bytes processed (proportional to $$$ cost): %10.2f kB" % (nbytes/1024.0) )

        ctime = int(job['statistics']['creationTime'])
        etime = int(job['statistics']['endTime'])
        dt = (etime - ctime)/1000.0
        logger( "[bqutil] Job run time: %8.2f seconds" % dt)

        # Patch the table to add a description
        if not wd=='WRITE_APPEND':
            try:
                me = getpass.getuser()
            except Exception as err:
                me = "gae"
            txt = 'Computed by %s / bqutil at %s processing %s bytes in %8.2f sec\nwith this SQL: %s' % (me, datetime.datetime.now(), 
                                                                                                         nbytes,
                                                                                                         dt,
                                                                                                         sql)
            project_name = get_project_name(project_id)
            output_project_name = get_project_name(output_project_id)
    
            txt += '\n'
            txt += 'see job: https://bigquery.cloud.google.com/results/%s:%s\n' % (project_name, job_id)
            txt += 'see table: https://bigquery.cloud.google.com/table/%s:%s.%s\n' % (output_project_name, dataset_id, table_id)
            logger(txt)
    
            add_description_to_table(dataset_id, table_id, txt, project_id=output_project_id)

    return job
    

def add_description_to_table(dataset_id, table_id, description, append=False, project_id=DEFAULT_PROJECT_ID):
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)

    if append:
        table = tables.get(**table_ref).execute()
        old_description = table['description']
        description = old_description + '\n' + description

    patch = {'description': description,
             "tableReference": table_ref
             }
    try:
        table = tables.patch(body=patch, **table_ref).execute()
    except Exception as err:
        print "[bqutil] oops, failed in adding description to table, patch=%s, err=%s, table=%s" % (patch, str(err), table)
        raise
    return table

def load_data_to_table(dataset_id, table_id, gsfn, schema, wait=True, verbose=False, maxbad=None, 
                       format=None, skiprows=None,
                       project_id=DEFAULT_PROJECT_ID
                       ):
    '''
    Import data file (JSON or CSV) from Google Storage into bigquery table.
    '''

    project_ref = dict(projectId=project_id)
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)

    config = {'load': {'sourceUris': [gsfn],
                       'schema': {'fields': schema},
                       "destinationTable": table_ref,
                       'sourceFormat': "NEWLINE_DELIMITED_JSON",
                       # "maxBadRecords": 0,
                       'writeDisposition': 'WRITE_TRUNCATE',
                       }
              }
              
    if skiprows is not None:
        config['load']["skipLeadingRows"] = skiprows

    if format=='csv':
        config['load']['sourceFormat'] = 'CSV'

    if maxbad is not None:
        config['load']['maxBadRecords'] = maxbad

    job_id = 'load_%s_%d' % (table_id, time.time())
    job_ref = {'jobId': job_id,
               'projectId': project_id}
    
    job = {'jobReference': job_ref, 'configuration': config}

    print "[bqutil] loading table %s from %s, running job %s" % (table_id, gsfn, job_id)
    sys.stdout.flush()

    if verbose:
        print job

    for k in range(10):
        try:
            jobret = jobs.insert(body=job, **project_ref).execute()
            break
        except Exception as err:
            print "[bqutil] oops!  Failed to insert job=%s" % job
            if (k==9):
                raise
            if 'HttpError 500' in str(err):
                print err
                print "--> 500 error, retrying in 30 sec"
                time.sleep(30)
                continue
            raise

    job = jobret

    if verbose:
        print "job=", json.dumps(job, indent=4)
      
    if verbose:
        job_list = jobs.list( stateFilter=['pending', 'running'], **project_ref).execute()
        print "job list: ", job_list

    if not wait:
        return

    nerr = 0
    while job['status']['state'] <> 'DONE':
        try:
            job = jobs.get(**job_ref).execute()
        except Exception as err:
            if "Internal Error" in str(err):
                nerr += 1
                if nerr > 10:
                    raise
                time.sleep(10)
                continue

    status = job['status']
    print "[bqutil] job status: ", status

    if 'errors' in status:
        print "[bqutil] ERROR!  ", status['errors']
        print "job = ", json.dumps(job, indent=4)
        raise Exception('BQ Error creating table')
    else:
        me = getpass.getuser()
        project_name = get_project_name(project_id)
        txt = "Data loaded from %s by %s / bqutil on %s\n" % (gsfn, me, datetime.datetime.now())
        txt += 'see job: https://bigquery.cloud.google.com/results/%s:%s\n' % (project_name, job_id)
        txt += 'see table: https://bigquery.cloud.google.com/table/%s:%s.%s\n\n' % (project_name, dataset_id, table_id)
        add_description_to_table(dataset_id, table_id, txt, project_id=project_id)

    return job
    
def extract_table_to_gs(dataset_id, table_id, gsfn, format=None, do_gzip=False, wait=True, 
                        verbose=False,
                        project_id=DEFAULT_PROJECT_ID):
    '''
    extract BQ table to a file in google cloud storage.
    '''

    project_ref = dict(projectId=project_id)
    config = {'extract': {'sourceTable': {'projectId': project_id,
                                          'datasetId': dataset_id,
                                          'tableId': table_id
                                          },
                          'destinationUris': [ gsfn ],
                          'destinationFormat': 'NEWLINE_DELIMITED_JSON',
                          'compression': 'GZIP' if do_gzip else "NONE",
                          }
              }

    if format=='csv':
        config['extract']['destinationFormat'] = 'CSV'

    job_id = 'load_%s_%d' % (table_id, time.time())
    job_ref = {'jobId': job_id,
               'projectId': project_id}
    
    job = {'jobReference': job_ref, 'configuration': config}
    
    print "[bqutil] extracting table %s to %s, running job %s" % (table_id, gsfn, job_id)
    sys.stdout.flush()

    if verbose:
        print job

    try:
        job = jobs.insert(body=job, **project_ref).execute()
    except Exception as err:
        print "[bqutil] oops!  Failed to insert job=%s" % job
        raise

    if verbose:
        print "job=", json.dumps(job, indent=4)
      
    if verbose:
        job_list = jobs.list( stateFilter=['pending', 'running'], **project_ref).execute()
        print "job list: ", job_list

    if not wait:
        return

    nerr = 0
    while job['status']['state'] <> 'DONE':
        try:
            job = jobs.get(**job_ref).execute()
        except Exception as err:
            if "Internal Error" in str(err):
                nerr += 1
                if nerr > 10:
                    raise
                time.sleep(10)
                continue

    status = job['status']
    print "[bqutil] job status: ", status

    if 'errors' in status:
        print "[bqutil] ERROR!  ", status['errors']
        print "job = ", json.dumps(job, indent=4)
        raise Exception('BQ Error creating table')

#-----------------------------------------------------------------------------
# unit tests, using py.test
#
# assumes live credentials are available (may need GAE stubs)

def test_get_project_name():
    name = get_project_name()
    print name
    assert(name is not None and type(name)==unicode)

def test_course_id2dataset():
    dataset = course_id2dataset('the/course.123', use_dataset_latest=True)
    assert(dataset=='the__course_123_latest')
    dataset = course_id2dataset('the/course.123', use_dataset_latest=False)
    assert(dataset=='the__course_123')
    dataset = course_id2dataset('the/course.123', 'logs', use_dataset_latest=True)
    assert(dataset=='the__course_123_logs')

def test_create_dataset():
    dataset = "test_dataset"
    create_dataset_if_nonexistent(dataset)
    dlist = get_list_of_datasets()
    assert(dataset in dlist)
    delete_dataset(dataset, delete_contents=True)
    dlist = get_list_of_datasets()
    assert(dataset not in dlist)
    
def test_create_table():
    dataset = "test_dataset"
    create_dataset_if_nonexistent(dataset)

    table = "test_table"
    sql = "select word, corpus from [publicdata:samples.shakespeare]"
    data = get_bq_table(dataset, table, sql=sql, key={'name': 'corpus'})
    print 'data_by_key len: ', len(data['data_by_key'])
    print 'data len: ', len(data['data'])
    assert(type(data['creationTime'])==datetime.datetime)
    assert(len(data['data'])>0)
    assert(len(data['data_by_key'])>0 and len(data['data_by_key'])<100)	# multiple words in a single corpus

    tinfo = get_tables(dataset)
    print "tinfo = ", tinfo
    assert('tables' in tinfo)

    tables = get_list_of_table_ids(dataset)
    assert(table in tables)
    
    cdt = get_bq_table_creation_datetime(dataset, table)
    assert((cdt - datetime.datetime.now()).days < 1)

    cdt = get_bq_table_last_modified_datetime(dataset, table)
    assert((cdt - datetime.datetime.now()).days < 1)

    add_description_to_table(dataset, table, 'hello world')
    tinfo = get_bq_table_info(dataset, table)
    desc = tinfo['description']
    assert(desc.count('hello world')==1)

    add_description_to_table(dataset, table, 'hello world', append=True)
    tinfo = get_bq_table_info(dataset, table)
    desc = tinfo['description']
    assert(desc.count('hello world')==2)

    delete_bq_table(dataset, table)
    tables = get_list_of_table_ids(dataset)
    assert(table not in tables)
    
    

