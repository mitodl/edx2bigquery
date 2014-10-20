#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Helper class to read BigQuery tables and metadata.

This module contains code to read BigQuery tables using the
TableData.list() API and save the results to a file.
Usage:
  table_reader = TableReader(project_id=<project_id>,
                              dataset_id=<dataset_id>,
                              table_id=<table_id>)
  with FileResultHandler(<output_file_name>) as result_handler:
    table_reader.read(result_handler)
will read the BigQuery table <project_id>:<dataset_id>.<table_id>
and download the results to the file <output_file_name>.
'''

import json
import os
import sys
import threading
import time

# Imports from the Google API client:
from apiclient.errors import HttpError

# Imports from files in this directory:
import auth

READ_CHUNK_SIZE= 64 * 1024

class ResultHandler:
  '''Abstract class to handle reading TableData rows.'''

  def handle_rows(self, rows):
    '''Process one page of results.'''
    pass

class TableReader:
  '''Reads data from a BigQuery table.'''

  def __init__(self, project_id, dataset_id, table_id,
      start_index=None, read_count=None, next_page_token=None):
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.bq_service = auth.build_bq_client()
    self.next_page_token = next_page_token
    self.next_index = start_index
    self.rows_left = read_count
    self.table_id = table_id

  def get_table_info(self):
    '''Returns a tuple of (modified time, row count) for the table.'''

    table = self.bq_service.tables().get(
        projectId=self.project_id,
        datasetId=self.dataset_id,
        tableId=self.table_id).execute()
    last_modified = int(table.get('lastModifiedTime', 0))
    row_count = int(table.get('numRows', 0))
    print '%s last modified at %d' % (
      table['id'], last_modified)
    return (last_modified, row_count)

  def advance(self, rows, page_token):
    '''Called after reading a page, advances current indices.'''

    done = page_token is None
    if self.rows_left is not None:
      self.rows_left -= len(rows)
      if self.rows_left < 0: print 'Error: Read too many rows!'
      if self.rows_left <= 0: done = True

    if self.next_index is not None:
      self.next_index += len(rows)
    else:
      # Only use page tokens when we're not using
      # index-based pagination.
      self.next_page_token = page_token
    return done

  def get_table_id(self):
    if '@' in self.table_id and self.snapshot_time is not None:
      raise Exception("Table already has a snapshot time")
    if self.snapshot_time is None:
      return self.table_id
    else:
      return '%s@%d' % (self.table_id, self.snapshot_time)

  def make_read_message(self, row_count, max_results):
    '''Creates a status message for the current read operation.'''
    read_msg =  'Read %d rows' % (row_count,)
    if self.next_index is not None:
      read_msg = '%s at %s' % (read_msg, self.next_index)
    elif self.next_page_token is not None:
      read_msg = '%s at %s' % (read_msg, self.next_page_token)
    else:
      read_msg = '%s from start' % (read_msg,)
    if max_results <> row_count:
       read_msg = '%s [max %d]' % (read_msg, max_results)
    return read_msg

  def read_one_page(self, max_results=READ_CHUNK_SIZE):
    '''Reads one page from the table.'''

    while True:
      try:
        if self.rows_left is not None and self.rows_left < max_results:
          max_results = self.rows_left

        data = self.bq_service.tabledata().list(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.get_table_id(),
            startIndex=self.next_index,
            pageToken=self.next_page_token,
            maxResults=max_results).execute()
        next_page_token = data.get('pageToken', None)
        rows = data.get('rows', [])
        print self.make_read_message(len(rows), max_results)
        is_done = self.advance(rows, next_page_token)
        return (is_done, rows)
      except HttpError, err:
        # If the error is a rate limit or connection error, wait and
        # try again.
        if err.resp.status in [403, 500, 503]:
          print '%s: Retryable error %s, waiting' % (
              self.thread_id, err.resp.status,)
          time.sleep(5)
        else: raise

  def read(self, result_handler, snapshot_time=None):
    '''Reads an entire table until the end or we hit a row limit.'''
    # Read the current time and use that for the snapshot time.
    # This will prevent us from getting inconsistent results when the
    # underlying table is changing.
    if snapshot_time is None and not '@' in self.table_id:
      self.snapshot_time = int(time.time() * 1000)
    self.snapshot_time = snapshot_time
    while True:
      is_done, rows = self.read_one_page()
      if rows:
        result_handler.handle_rows(rows)
      if is_done:
        return


class FileResultHandler(ResultHandler):
  '''Result handler that saves rows to a file.'''

  def __init__(self, output_file_name):
    self.output_file_name = output_file_name
    print 'Writing results to %s' % (output_file_name,)

  def __enter__(self):
    self.make_output_dir()
    self.output_file =  open(self.output_file_name, 'w')
    return self

  def __exit__(self, type, value, traceback):
    if self.output_file:
      self.output_file.close()
      self.output_file = None

  def make_output_dir(self):
    '''Creates an output directory for the downloaded results.'''

    output_dir = os.path.dirname(self.output_file_name)
    if os.path.exists(output_dir) and os.path.isdir(output_dir):
      # Nothing to do.
      return
    os.makedirs(output_dir)

  def handle_rows(self, rows):
    if self.output_file is None:
      self.__enter()
    self.output_file.write(json.dumps(rows, indent=2))


class TableReadThread (threading.Thread):
  '''Thread that reads from a table and writes it to a file.'''
  def __init__(self, table_reader, output_file_name,
               thread_id='thread'):
    threading.Thread.__init__(self)
    self.table_reader = table_reader
    self.output_file_name = output_file_name
    self.thread_id = thread_id

  def run(self):
    print 'Reading %s' % (self.thread_id,)
    with FileResultHandler(self.output_file_name) as result_handler:
      self.table_reader.read(result_handler)


def main(argv):
  if len(argv) == 0:
     argv = ['publicdata',
             'samples',
             'shakespeare',
             '/tmp/bigquery']
  if len(argv) < 4:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0],
                 '<project_id>',
                 '<dataset_id>',
                 '<table_id>',
                 '<output_directory>']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return

  table_reader = TableReader(project_id=argv[0],
                             dataset_id=argv[1],
                             table_id=argv[2])
  # Come up with a suitably obtuse file name.
  output_file_name = os.path.join(
      argv[3],
      table_reader.project_id,
      table_reader.dataset_id,
      table_reader.table_id)
  thread = TableReadThread(table_reader, output_file_name)
  thread.start()
  thread.join()

if __name__ == "__main__":
    main(sys.argv[1:])
