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
import getpass
from edx2bigquery_config import PROJECT_ID as DEFAULT_PROJECT_ID

import auth

service = auth.build_bq_client() 
# project_id=auth.PROJECT_ID

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

# project_ref = {'projectId': project_id}
# dataset_ref = {'datasetId': dataset_id,
#                'projectId': project_id}
# table_id = 'pc_nevents'
# table_ref = {'tableId': table_id,
#              'datasetId': dataset_id,
#              'projectId': project_id}

def get_tables(dataset_id, project_id=DEFAULT_PROJECT_ID):
    table_list = tables.list(datasetId=dataset_id, projectId=project_id, maxResults=1000).execute()
    if 0:
        for current in table_list['tables']:
            print "table: ", current
    return table_list

def get_list_of_table_ids(dataset_id):
    tables_info = get_tables(dataset_id)['tables']
    table_id_list = [ x['tableReference']['tableId'] for x in tables_info ]
    return table_id_list

def get_table_data(dataset_id, table_id, key=None, logger=None, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve data from a specific BQ table.  Return as a dict, with
    fields = schema fields
    field_names = name of top-level schema fields
    data = list of data
    data_by_key = dict of data, with key being the value of the fieldname specified as the key arg

    Arguments:

      - key: dict with {'name': name_of_field_for_key}

    '''
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    table_ref['maxResults'] = 1000000
    table = get_bq_table_info(dataset_id, table_id)
    data = tabledata.list(**table_ref).execute()

    fields = table['schema']['fields']
    field_names = [x['name'] for x in fields]

    ret = {'fields': fields,
           'field_names': field_names,
           'data': [],
           'data_by_key': {}
           }

    rows = data.get('rows', [])
    for row in rows:
        values = {}
        for i in xrange(0, len(fields)):
            cell = row['f'][i]
            values[field_names[i]] = cell['v']
        ret['data'].append(values)
        if key is not None:
            the_key = values[key['name']]
            if 'keymap' in key:
                the_key = key['keymap'](the_key)
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
    return table

def default_logger(msg):
    print msg

def get_bq_table(dataset, tablename, sql, key=None, allow_create=True, force_query=False, logger=default_logger):
    '''
    Retrieve data for the specified BQ table if it exists.
    If it doesn't exist, create it, using the provided SQL.
    '''
    if force_query:
        create_bq_table(dataset, tablename, sql, logger=logger)
        return get_table_data(dataset, tablename, key=key, logger=logger)
    try:
        ret = get_table_data(dataset, tablename, key=key, logger=logger)
    except Exception as err:
        if 'Not Found' in str(err) and allow_create:
            create_bq_table(dataset, tablename, sql, logger=logger)
            return get_table_data(dataset, tablename, key=key, logger=logger)
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

    if overwrite:
        wd = "WRITE_TRUNCATE"
    elif overwrite in ["append", 'APPEND']:
        wd = "WRITE_APPEND"
    else:
        wd = "WRITE_TRUNCATE"

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

    job = jobs.insert(body=job, **project_ref).execute()

    if verbose:
        print "job=", json.dumps(job, indent=4)
      
    if verbose:
        job_list = jobs.list( stateFilter=['pending', 'running'], **project_ref).execute()
        print "job list: ", job_list

    if not wait:
        return

    while job['status']['state'] <> 'DONE':
        job = jobs.get(**job_ref).execute()

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
        me = getpass.getuser()
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
