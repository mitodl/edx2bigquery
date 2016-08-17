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

service = auth.build_bq_client(timeout=480)
#service = auth.build_bq_client() 

projects = service.projects()
datasets = service.datasets()
tables = service.tables()
tabledata = service.tabledata()
jobs = service.jobs()

PROJECT_NAMES = {}				# used to cache project names, key=project_id

def default_logger(msg):
    print msg

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

def convert_data_dict_to_csv(tdata, extra_fields=None):
    '''
    Convert dict format data from get_table_data into CSV file content, as a string.

    If extra_fields is not None, then add data from extra_fields to each row.  
    This can be used, e.g. for adding course_id to a table missing that field.
    '''
    import unicodecsv as csv
    from StringIO import StringIO

    sfp = StringIO()
    extra_fields = extra_fields or {}
    fields = extra_fields.keys()
    fields += tdata['field_names']
    dw = csv.DictWriter(sfp, fieldnames=fields)
    dw.writeheader()
    for row in tdata['data']:
        row.update(extra_fields)
        dw.writerow(row)
    return sfp.getvalue()

def get_table_data(dataset_id, table_id, key=None, logger=default_logger, 
                   project_id=DEFAULT_PROJECT_ID, 
                   return_csv=False,
                   convert_timestamps=False,
                   startIndex=None, maxResults=1000000,
		   extra_fields=None):
    '''
    Retrieve data from a specific BQ table.  Normally return this as a dict, with

    fields      = schema fields
    field_names = list of names of top-level schema fields
    data        = list of data, where each data item is a dict of field name, field value
    data_by_key = dict of data, with key being the value of the fieldname specified as the key arg

    Arguments:

    key         = dict, e.g. {'name': field_name_for_index, 'keymap': function_on_key_values}
    maxResults  = maximum number of results to return
    startIndex  = zero-based index of starting row to read; make this negative to return from 
                  end of table
    return_csv  = return data as CSV (as a big string) if True
    extra_fields = None, or dict giving extra fields which are to be added (e.g. course_id) to the 
                   output; only used when return_csv = True
    '''
    table = get_bq_table_info(dataset_id, table_id, project_id)
    if not table:
        logger('[bqutil.get_table_data] table %s:%s.%s not found!' % (project_id, dataset_id, table_id))
        return None
        
    nrows = int(table['numRows'])

    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    table_ref['maxResults'] = maxResults
    if startIndex is not None:
        if startIndex < 0:
            startIndex = nrows + startIndex
        if startIndex < 0:			# cannot be negative
            startIndex = 0
        table_ref['startIndex'] = startIndex

    data = tabledata.list(**table_ref).execute()

    if not 'rows' in data:
        logger('[bqutil.get_table_data] No rows in data!  data=%s' % data)
        return None

    dataRows = int(len(data['rows']))
    totalRows = int(data['totalRows'])
    num_rows_expected = (totalRows-(startIndex or 0))
    multiple_reads = 0

    while (dataRows < num_rows_expected):
       table_ref['startIndex'] = dataRows
       data_append = tabledata.list(**table_ref).execute()
       data['rows'] += data_append['rows']
       dataRows = int(len(data['rows']))
       multiple_reads += 1

    if multiple_reads:
        logger("[bqutil] Total Rows Retrieved: %s (%d read requests, expected %d)" % (dataRows, multiple_reads+1, num_rows_expected))

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
            # if field is a TIMESTMP then convert to datetime
            if convert_timestamps and fields[i]['type']=='TIMESTAMP':
                values[field_names[i]] = bq_timestamp_milliseconds_to_datetime(cell['v'], divisor=1)
            else:
                values[field_names[i]] = cell['v']
        ret['data'].append(values)
        if key is not None:
            the_key = values[key['name']]
            if 'keymap' in key:
                the_key = key['keymap'](the_key)
            if the_key not in ret['data_by_key']:
                ret['data_by_key'][the_key] = values

    if return_csv:
        return convert_data_dict_to_csv(ret, extra_fields=extra_fields)

    return ret

def delete_zero_size_tables(dataset_id, verbose=False, project_id=DEFAULT_PROJECT_ID):
    '''
    Delete tables which have zero rows, in the specified dataset
    '''
    for table_id in get_list_of_table_ids(dataset_id):
        if get_bq_table_size_rows(dataset_id, table_id, project_id)==0:
            if verbose:
                print "Deleting %s.%s" % (dataset_id, table_id)
                sys.stdout.flush()
            delete_bq_table(dataset_id, table_id, project_id)

def delete_bq_table(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Delete specified BQ table
    '''
    table_ref = dict(datasetId=dataset_id, projectId=project_id, tableId=table_id)
    tables.delete(**table_ref).execute()    

def copy_bq_table(dataset_id, table_id, destination_table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Copy specified BQ table, to a new table_id (in the same dataset and project).
    Destination is created if needed, and overwritten if already exists.
    This is asynchronous; no waiting for completion is done.
    '''
    jobData = {
      "projectId": project_id,
      "configuration": {
          "copy": {
              "sourceTable": {
                  "projectId": project_id,
                  "datasetId": dataset_id,
                  "tableId": table_id,
              },
              "destinationTable": {
                  "projectId": project_id,
                  "datasetId": dataset_id,
                  "tableId": destination_table_id,
              },
          "createDisposition": "CREATE_IF_NEEDED",
          "writeDisposition": "WRITE_TRUNCATE"
          }
        }
      }

    insertResponse = jobs.insert(projectId=project_id, body=jobData).execute()
    return insertResponse

def get_bq_table_size_rows(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve number of rows of specified BQ table
    '''
    tinfo = get_bq_table_info(dataset_id, table_id, project_id)
    if tinfo is not None:
        return int(tinfo['numRows'])
    return None

def get_bq_table_size_bytes(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve number of bytes of specified BQ table
    '''
    tinfo = get_bq_table_info(dataset_id, table_id, project_id)
    if tinfo is not None:
        return int(tinfo['numBytes'])
    return None

def bq_timestamp_milliseconds_to_datetime(timestamp, divisor=1000.0):
    '''
    Convert a millisecond timestamp to a python datetime object
    '''
    if timestamp:
        return datetime.datetime.utcfromtimestamp(float(timestamp)/divisor)
    return None

def get_bq_table_creation_datetime(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve datetime of table creation
    '''
    tinfo = get_bq_table_info(dataset_id, table_id, project_id)
    if tinfo is not None:
        return tinfo['creationTime']
    return None

def get_bq_table_last_modified_datetime(dataset_id, table_id, project_id=DEFAULT_PROJECT_ID):
    '''
    Retrieve datetime of table last modification
    '''
    tinfo = get_bq_table_info(dataset_id, table_id, project_id)
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
    if not table:
        return
    table['lastModifiedTime'] = bq_timestamp_milliseconds_to_datetime(table['lastModifiedTime'])
    table['creationTime'] = bq_timestamp_milliseconds_to_datetime(table['creationTime'])
    return table

def get_bq_table(dataset, tablename, sql=None, key=None, allow_create=True, force_query=False, logger=default_logger,
                 depends_on=None,
                 allowLargeResults=False,
                 newer_than=None,
                 startIndex=None, maxResults=1000000):
    '''
    Retrieve data for the specified BQ table if it exists.
    If it doesn't exist, create it, using the provided SQL.

    depends_on may be provided as a list of "dataset.table" strings, which specify which table(s)
    the desired table depends on.  If the desired table exists, but is older than any of the 
    depends_on table(s), then it is recomputed from the sql (assuming the sql was provided).

    newer_than may be provided, as a datetime, specifying that if the desired table exists,
    it must be newer than the specified datetime, else it should be recomputed.
    '''
    if (depends_on is not None) and (sql is not None) and (not force_query):

        # get the mod time of the computed table, if it exists
        try:
            table_date = get_bq_table_last_modified_datetime(dataset, tablename)
        except Exception as err:
            if 'Not Found' in str(err):
                table_date = None
            else:
                raise

        if table_date:
            # get the latest mod time of tables in depends_on:
            modtimes = [ get_bq_table_last_modified_datetime(*(x.split('.',1))) for x in depends_on]
            latest = max([x for x in modtimes if x is not None])
        
            if not latest:
                raise Exception("[get_bq_table] Cannot get last mod time for %s (got %s), needed by %s.%s" % (depends_on, modtimes, dataset, tablename))

            if table_date < latest:
                force_query = True
                logger("[get_bq_table] Forcing query recomputation of %s.%s, table_date=%s, latest=%s" % (dataset, tablename,
                                                                                                          table_date, latest))
        else:
            force_query = True

    if (not force_query) and newer_than and (sql is not None):
        # get the mod time of the computed table, if it exists
        try:
            table_date = get_bq_table_last_modified_datetime(dataset, tablename)
        except Exception as err:
            if 'Not Found' in str(err):
                table_date = None
            else:
                raise
        if table_date and (table_date < newer_than):
            force_query = True
            logger("[get_bq_table] Forcing query recomputation of %s.%s, table_date=%s, newer_than=%s" % (dataset, tablename,
                                                                                                          table_date, newer_than))
        #else:
        #    logger("[get_bq_table] table %s.%s date %s, newer than %s" % (dataset, tablename, table_date, newer_than))

    if force_query:
        create_bq_table(dataset, tablename, sql, logger=logger, overwrite=True, allowLargeResults=allowLargeResults)
        return get_table_data(dataset, tablename, key=key, logger=logger,
                              startIndex=startIndex, maxResults=maxResults)
    try:
        ret = get_table_data(dataset, tablename, key=key, logger=logger,
                             startIndex=startIndex, maxResults=maxResults)
        if ret is None:
            try:
                tsize = get_bq_table_size_rows(dataset, tablename)
            except:
                tsize = None
            if tsize is None:
                raise Exception("Table %s.%s empty or Not Found" % (dataset, tablename))	# recompute, but not if table just has zero rows
    except Exception as err:
        if 'Not Found' in str(err) and allow_create and (sql is not None) and sql:
            create_bq_table(dataset, tablename, sql, logger=logger, overwrite=True, allowLargeResults=allowLargeResults)
            return get_table_data(dataset, tablename, key=key, logger=logger,
                                  startIndex=startIndex, maxResults=maxResults)
        else:
            raise
    return ret

def create_bq_table(dataset_id, table_id, sql, verbose=False, overwrite=False, wait=True, 
                    logger=default_logger, project_id=DEFAULT_PROJECT_ID,
                    output_project_id=DEFAULT_PROJECT_ID,
                    allowLargeResults=False,
                    maximumBillingTier=None,
                    sql_for_description=None,
                    udfs=None):
    '''
    Run SQL query to create a new table.

    sql: String representation of Google BigQuery SQL expression
    udfs: String representation of Google BigQuery user-defined function (UDF).
         If multiple UDFs in a single query, udfs = list of UDF strings.
    '''

    project_ref = dict(projectId=project_id)
    table_ref = dict(datasetId=dataset_id, projectId=output_project_id, tableId=table_id)

    if overwrite in ["append", 'APPEND']:
        wd = "WRITE_APPEND"
    elif overwrite==True:
        wd = "WRITE_TRUNCATE"
    else:
        wd = "WRITE_EMPTY"	

    udfs_type_error = "[bqutil] create_bq_table parameter udfs is wrong type. Type should be str or list of str."

    if udfs is None:
        UDF_list = []
    elif type(udfs) is str:
        UDF_list = [dict(inlineCode=udfs)]
    elif type(udfs) is list:
        UDF_list = []
        for i in udfs:
            if type(i) is str:
                UDF_list.append(dict(inlineCode=i))
            else:
                logger(udfs_type_error)
                raise TypeError(udfs_type_error)
    else:
        logger(udfs_type_error)
        raise TypeError(udfs_type_error)

    config = {'query': { 'query': sql,
                         'destinationTable': table_ref,
                         'writeDisposition': wd,
                         'allowLargeResults': allowLargeResults,
                         'userDefinedFunctionResources': UDF_list,
                         }
              }
    if maximumBillingTier:
        config['query']['maximumBillingTier'] = maximumBillingTier
              
    job_id = 'create_%s_%s_%d' % (dataset_id, table_id, time.time())
    job_ref = {'jobId': job_id,
               'projectId': project_id}
    
    job = {'jobReference': job_ref, 'configuration': config}

    if 'APPEND' in wd:
        logger("[bqutil] Appending to table %s, running job %s" % (table_id, job_id))
    else:
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

    # timeoutMs = 5000
    # job_ref['timeoutMs'] = timeoutMs

    ecnt = 0
    while job.get('status', {}).get('state', None) <> 'DONE':
        if 'status' not in job:
            ecnt += 1
            if (ecnt == 10):
                logger("[bqutil] Error!  no job status?  job ret = %s" % job)
            if (ecnt > 200):
                raise Exception('BQ Error getting job status')
        else:
            ecnt = 0
        try:
            time.sleep(5)
            job = jobs.get(**job_ref).execute()
        except Exception as err:
            print "[bqutil] oops!  Failed to execute jobs.get=%s" % (job_ref)
            print "[bqutil] err=%s" % str(err)

    status = job['status']
    logger( "[bqutil] job status: %s" % status )

    if 'errors' in status:
        logger( "[bqutil] ERROR!  %s" % str(status['errors']) )
        logger( "job = %s" % json.dumps(job, indent=4))
        emsg = 'BQ Error creating table '
        emsg += status.get('errorResult', {}).get('message', '')
        raise Exception(emsg)

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
                                                                                                         sql_for_description or sql)
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

    if len(description) > 16383:
        print "[bqutil] oops, cannot add description, length=%s > 16383, truncating description" % len(description)
        description = description[:16383]
        # return

    patch = {'description': description,
             "tableReference": table_ref
             }
    try:
        table = tables.patch(body=patch, **table_ref).execute()
    except Exception as err:
        print "[bqutil] oops, failed in adding description to table, patch=%s, err=%s, table=%s" % (patch, str(err), table_id)
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

    job_id = 'load_%s__%s_%d' % (dataset_id, table_id, time.time())
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
            if 'SSL3_GET_RECORD:decryption failed' in str(err):
                print err
                print "--> SSL3 error, retrying in 10 sec"
                time.sleep(10)
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

    job_id = 'load_%s__%s_%d' % (dataset_id, table_id, time.time())
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
    
    

