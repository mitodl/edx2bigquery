
from .. import bqutil
import pandas as pd
import datetime as dt
from datetime import datetime
import os
import sys
import json
import re
import subprocess
import shutil
from difflib import SequenceMatcher

# The following script provides utility functions which are used
# for unit testing but may also be used generally as part of
# edx2bigquery functionality. Some of these functions require the Pandas
# library, but not necessarily all.

# The SQL2df and similar functions generate a Pandas dataframe directly
# from SQL. Estimated time for downloading is provided, among other useful
# features not found in bqutil.py and gsutil.py.

# Function names preceded by gs_ extend Google Storage (gs) functionality
# and may be better placed in gsutil.py when these utilities mature.

# Function names preceded by bq_ extend BigQuery (bq) functionality
# and may be better placed in bqutil.py when these utilities mature.

# Some of the following functions assume you have a Google Storage Account
# and have Gooogle Storage (gs) command line tools installed.

def get_project_id(course_id):
  '''
  Returns a string.
  Returns the project_id for a given course_id
  '''
  return 'harvardx-data' if course_id[0].lower() == 'h' else 'mitx-data'

def SQL_FROM_string_from_course_ids(course_ids, table_name=None, project_id=None, dataset_id=None, table_suffix="", table_prefix=""):
  '''
  Returns a string.
  Generates a list of tables as a string that can be inserted into
  the from clause of a SQL query.

  An example of table_suffix is "_stats_ans_coupling" or "_show_ans_before"
  '''
  if dataset_id is None and table_name is not None:
    table_strings = ["["+get_project_id(c)+":"+ \
      bqutil.course_id2dataset(c, use_dataset_latest=True)+ \
      "."+table_name+"]" for c in course_ids]
    return ",\n".join(table_strings)

  if table_suffix != "" and table_suffix[0] != "_":
    table_suffix = "_" + table_suffix

  if table_prefix != "" and table_prefix[-1] != "_":
    table_prefix = table_prefix + "_"

  table_strings = ["["+project_id+":"+ dataset_id+ "."+ \
    table_prefix+bqutil.course_id2dataset(c, use_dataset_latest=True)+table_suffix+"]" 
    for c in course_ids]

  return ",\n".join(table_strings)

def SQL_FROM_string_from_table_strings(tables):
  '''
  Returns a string.
  Generates a list of tables as a string that can be inserted into
  the from clause of a SQL query.
  '''
  table_strings = ["["+t+"]" for t in tables]
  return ",\n".join(table_strings)

def combine_sab_csv_gz_files(path, persistent=False):
  '''
  Combines all table partitions created by bqutil_download_tables
  path should end in "/" and should specify a directory, not a file_path
  '''
  if (path[-1:] != '/' and path[-1:] != '\\'):
    raise ValueError('Path is not a valid directory, please append a forward slash: ' + path) 
  
  # Find list of all the table partitions
  tables = pd.DataFrame()
  tables['name'] = [i for i in os.listdir(path)]
  tables['base'] = [i[:-19] for i in os.listdir(path)]
  tables['cnt'] = tables.groupby('base').transform('count')

  table_rows_generator = tables.iterrows()
  for i in range(len(tables.base.unique())):
    row = table_rows_generator.next()[1]
    # If only one table, just rename the file
    if row['cnt'] == 1:
      if not persistent:
        shutil.move(path + row['name'], path + row['base'] + '.csv.gz')
      else:
        shutil.copy(path + row['name'], path + row['base'] + '.csv.gz')
    else:
      df = pd.read_csv(path + row['name'], compression='gzip')
      if not persistent:
        os.remove(path + row['name'])
      for i in range(row['cnt'] - 1): # Append the rest of the row.cnt - 1 tables to df
        row = table_rows_generator.next()[1]
        df = df.append(pd.read_csv(path + row['name'], compression='gzip'))
        if not persistent:
          os.remove(path + row['name'])
      df.reset_index(drop=True).to_csv(path + row['base'] + '.csv.gz', compression='gzip', index=False)

def bqutil_download_table_via_gs(dataset_id, table_id, path, gs_folder=None, project_id='mitx-research', gs_project_id="mitx-research", persistent=False):
    '''
    Downloads large tables using Google Storage.
    dataset_id: string of dataset_id, also supports a list of dataset_id strings 
    gs_folder: Specify a folder if persistent storage in Google Storage is needed. 
    '''
    
    if gs_folder is None:
        #gs_folder = 'temp' + "_" + str(datetime.now()).replace(':',"_").replace('.',"_").replace('-',"_").replace(" ","__")
        gs_folder="temp"
        
    gsfn = 'gs://'+gs_project_id+'/' + gs_folder + '/' + dataset_id + "_" + table_id + '*.csv.gz'
    bqutil.extract_table_to_gs(dataset_id=dataset_id, table_id=table_id, gsfn=gsfn, format='csv', do_gzip=True, project_id=project_id)
   
    try:
      # Download table to path
      gsfn = 'gs://'+gs_project_id+'/' + gs_folder
      cmd = "gsutil -m cp -r " + gsfn + " '" + path + "'"
      print cmd
      print cmd
      print cmd
      os.system(cmd) 

      if not persistent:
          # If user did not ask for persitent storage in gs, remove files   
          cmd = 'gsutil -m rm -r ' + gsfn
          os.system(cmd)
    except:
      # Remove table if errors and we don't want persistence
      if not persistent:
          # If user did not ask for persitent storage in gs, remove files   
          cmd = 'gsutil -m rm -r ' + gsfn
          os.system(cmd)
      raise
      
    # Combine shard partitions of table
    combine_sab_csv_gz_files(path+gs_folder+"/", persistent = False)
    
    # Move table from gs_folder to parent path
    shutil.move(path+gs_folder + '/' + dataset_id + "_" + table_id + '.csv.gz', path)
    os.rmdir(path+gs_folder+'/')
  
def gs_SQL2local(SQL, 
              local_storage_directory, 
              output_project_id='mitx-research', 
              output_dataset='curtis_northcutt', 
              output_table=None, 
              overwrite=True,
              gs_folder=None, 
              project_id='mitx-research', 
              gs_project_id="mitx-research", 
              intermediate_persistent=False):
    '''
    USE THIS FUNCTION FOR LARGE TABLE DOWNLOADS!
    
    Executes Google BigQuery SQL and stores the results in a DataFrame.
    This method guarantees preservation of column and row orders.
    Return: A Pandas dataframe.
    
    This file supports very large SQL output downloads efficiently by
    sharding tables in google storage and downloading directly from servers then combining files.
    
    You may also choose to store intermediate files on BQ and on Google Storage by
    setting intermeidate_persistent=True.
    
    USE THIS FUNCTION FOR LARGE TABLE DOWNLOADS!
    '''
    if output_table is None:
      #output_table = "temp" + "_" + str(datetime.now()).replace(':',"_").replace('.',"_").replace('-',"_").replace(" ","__")
      output_table = "temp"
      
    bqutil.create_bq_table(output_dataset, output_table, SQL, overwrite = overwrite, output_project_id=output_project_id, allowLargeResults=True, sql_for_description='Created by Curtis G. Northcutt\n' + SQL)
    
    try:
      bqutil_download_table_via_gs(output_dataset, output_table, local_storage_directory, gs_folder=gs_folder, project_id=project_id, gs_project_id=gs_project_id, persistent=intermediate_persistent)
      if not intermediate_persistent:
        bqutil.delete_bq_table(output_dataset, output_table, output_project_id)
    except:
      # Remove table if errors and we don't want persistence
      if not intermediate_persistent:
        bqutil.delete_bq_table(output_dataset, output_table, output_project_id)
      raise

def gs_SQL2df(SQL, 
              local_storage_directory, 
              output_project_id = 'mitx-research', 
              output_dataset = 'curtis_northcutt', 
              output_table = None, 
              overwrite = True,
              gs_folder=None, 
              project_id='mitx-research', 
              gs_project_id="mitx-research", 
              intermediate_persistent=False,
              final_persistent=False):
    '''
    USE THIS FUNCTION FOR LARGE TABLES to Dataframes!
    
    Executes Google BigQuery SQL and stores the results in a DataFrame.
    This method guarantees preservation of column and row orders.
    Return: A Pandas dataframe.
    
    This file supports very large SQL output downloads efficiently by
    sharding tables in google storage and downloading directly from servers then combining files.
    
    You may choose to store the final table on your local machine by setting
    final_persistent=True.
    
    You may also choose to store intermediate files on BQ and on Google Storage by
    setting intermeidate_persistent=True.
    
    USE THIS FUNCTION FOR LARGE TABLE DOWNLOADS! (at least 100 MB to 1 GB ).
    It is inefficent for small table downloads.
    '''
    
    gs_SQL2local(SQL=SQL, local_storage_directory=local_storage_directory, output_project_id = output_project_id, 
              output_dataset = output_dataset, output_table = output_table, overwrite = overwrite,
              gs_folder=gs_folder, project_id=project_id, gs_project_id=gs_project_id, 
              intermediate_persistent=intermediate_persistent)
    
    table_file_path = local_storage_directory + output_dataset + "_" + output_table + '.csv.gz'

    df = pd.read_csv(table_file_path,compression='gzip')

    if not final_persistent:
      os.remove(table_file_path)
      
    return df.apply(lambda x: pd.to_numeric(x, errors='ignore'))

def bq_get_person_course_latest():
  '''
  Generates a string of table names for the most recent person_course tables.
  This string will likely be used in the 'FROM' clause of a SQL query.
  '''
  hx_pc = bq_get_table_ids('course_report_latest', project_id='harvardx-data', start='person_course_HarvardX')
  hx_pc.sort()
  hx_pc = hx_pc[-1:][0]

  mx_pc = bq_get_table_ids('course_report_latest', project_id='mitx-data', start='person_course_MITx')
  mx_pc.sort()
  mx_pc = mx_pc[-1:][0]
  
  return ("[harvardx-data:course_report_latest."+str(hx_pc)+"]", "[mitx-data:course_report_latest."+str(mx_pc)+"]")

def generate_all_person_course_latest_sql():
  '''
  Generates a SQL string that when run in BigQuery will produce all person course latest tables.
  '''
  return "(\n  SELECT\n   *\n  FROM\n" + ",\n".join(['    '+i for i in bq_get_person_course_latest()]) + "\n)"

def bqutil_convert_types_bq2py(df, verbose=False):
  '''
  Takes in a dataframe downloaded from Google Bigquery and converts types for Python.
  verbose: True - prints schema
  '''
  d = {'true':True, 'false':False}
  
  for i in df.columns:
    try:
      df[i] = pd.to_numeric(df[i])
      if verbose:
          print "FLOAT:", i
    except Exception as e:
      if df[i][0] in ['true', 'false']:
        df[i] = df[i].map(d) # convert to boolean 
        if verbose:
          print "BOOL:", i
      elif verbose:
        print "STRING:", i
        
  return df

def bq_get_datasets():
  '''
  Returns a Pandas Dataframe of all datasets in mitx-data and harvardx-data.
  '''
  result = pd.DataFrame(columns=['project_id', 'dataset_id'])
  for project_id in ["mitx-data", "harvardx-data"]:
    result = result.append(pd.DataFrame([{"project_id":project_id, "dataset_id":x[:-7]}
                                         for x in bq_get_dataset_ids(project_id) if 'latest' in x and 'course' not in x]))
  
  # Build map from dataset id to course_id
  c = bq_get_courses()
  c['dataset'] = c.course_id.apply(lambda x: bqutil.course_id2dataset(x))
  mapdf = dict(c[['dataset', 'course_id']].drop_duplicates().set_index('dataset')['course_id'])
  c['dataset'] = c.course_id.apply(lambda x: bqutil.course_id2dataset(x, use_dataset_latest=True))
  mapdf.update(dict(c[['dataset', 'course_id']].drop_duplicates().set_index('dataset')['course_id']))
 
  result['course_id'] = result.dataset_id.map(mapdf)
  return result.dropna().sort_values('dataset_id')

def bq_get_courses():
  '''
  Returns a Pandas Dataframe of all project_ids and course_ids in mitx-data and harvardx-data.
  '''
  dataset='course_report_latest'
  table='broad_stats_by_course'
  result = pd.DataFrame(columns=['course_id', 'project_id'])
  
  for project_id in ["harvardx-data", "mitx-data"]:
    result = result.append(pd.DataFrame([{"project_id":project_id, "course_id":x}
                                         for x in pd.DataFrame(bqutil.get_table_data(dataset, table, project_id=project_id)['data'])['course_id']]))
  return result.sort_values(["project_id", "course_id"]).drop_duplicates("course_id")

def bq_get_dataset_ids(project_id="mitx-research", max_datasets=10000):
  '''
  Returns a list of strings for all datasets in the given project_id using bq command line tools.
  '''
  dataset_string = subprocess.check_output("bq ls -a -n "+str(max_datasets)+" "+project_id+":",shell=True)
  return [string.strip() for string in dataset_string.split('\n')[2:]]

def bq_get_table_ids(dataset_id, max_tables=5000, project_id="mitx-research", end=None, start=None):
  '''
  Returns a list of strings for all table_ids in the given project_id using bq command line tools.
  '''
  if end is None:
    end = ""
  if start is None:
    start = ""
    
  table_id_string = subprocess.check_output("bq ls -a -n "+str(max_tables)+" "+project_id+":"+dataset_id,shell=True)
  return [re.search(start + r'(.*)' + end, i[:-7].strip()).group() for i in table_id_string.split("\n")[2:] if end in i and start in i]    

def bqutil_df2bq(df, schema, project_id, dataset_id, table_id):
    '''Uploads a dataframe to a bigquery table
    Example schema:
        schema = [{'type': 'STRING', 'name': 'cameo_candidate'},
              {'type': 'STRING', 'name': 'shadow_candidate'},
              {'type': 'FLOAT', 'name': 'similarity'},]
    Example usage:
        df2bq(df, schema, 'mitx-research', '0_cgn_sample', table)
    '''
    
    csv_filename = table_id + '.csv' 
    df.to_csv(csv_filename, header=True, index=False)
    schema_filename = table_id + "__schema.json"
    open(schema_filename, 'w').write(json.dumps(schema, indent=4))
    
    address = project_id + ':' + dataset_id + '.' + table_id
    #Remove table first, otherwise it will append
    try:
        bqutil.delete_bq_table(dataset_id, table_id, project_id)
        print 'Overwriting table:', address
    except:
        print 'Creating table:', address
    
    # Upload table
    cmd = "bq --project_id=mitx-research load --skip_leading_rows=1 %s %s %s" % (address, csv_filename, schema_filename)
    print cmd
    sys.stdout.flush()
    os.system(cmd)
    os.remove(csv_filename)
    os.remove(schema_filename)


def bqutil_bq2df(dataset_id, table_id, project_id="mitx-research", verbose=False):
    '''Downloads a BigQuery Table and stores it in a DataFrame.
    This method guarantees preservation of column and row orders.
    Return: A Pandas dataframe.
    '''
    info = bqutil.get_bq_table_info(dataset_id, table_id, project_id)
    megabytes = float(info['numBytes']) / 1.0e6
    print project_id + ':' + dataset_id + '.' + table_id +' is', str(megabytes) + ' Mb and', info['numRows'],'rows'
    estimated_seconds = megabytes / 0.35792536
    
    print "Downloading..."
    print "Estimated time to download table from BQ:", str(dt.timedelta(seconds = estimated_seconds))
    sys.stdout.flush()
    
    t = datetime.now()
 
    data = bqutil.get_table_data(dataset_id=dataset_id, table_id=table_id, project_id=project_id, maxResults=5000000)
    print "Download completed in", str(datetime.now() - t)
    sys.stdout.flush()
    
    if data is None:
        return pd.DataFrame()
    result = pd.DataFrame(data['data'], columns=data['field_names'])
    return bqutil_convert_types_bq2py(result)


def bqutil_SQL2df(SQL, temp_project_id='mitx-research', temp_dataset='curtis_northcutt', temp_table=None, persistent=False, udfs=None, overwrite=True):
    '''Executes Google BigQuery SQL and stores the results in a DataFrame.
    This method guarantees preservation of column and row orders.
    Return: A Pandas dataframe.
    '''
    if temp_table is None:
      temp_table = "temp" + "_" + str(datetime.now()).replace(':',"_").replace('.',"_").replace(" ","__").replace("-", "_")
    desc = 'Created by Curtis G. Northcutt\n' + (SQL if udfs is None else SQL + "\n\n\n#UDFs\n#----\n\n" + udfs)
    bqutil.create_bq_table(temp_dataset, temp_table, SQL, overwrite=overwrite, output_project_id=temp_project_id, allowLargeResults=True, sql_for_description=desc, udfs=udfs)
    try:
      df = bqutil_bq2df(dataset_id=temp_dataset, table_id=temp_table, project_id=temp_project_id)
      if not persistent:
        bqutil.delete_bq_table(temp_dataset, temp_table,temp_project_id)
    except:
      # Remove table if errors and we don't want persistence
      if not persistent:
        bqutil.delete_bq_table(temp_dataset, temp_table,temp_project_id)
        
    return df