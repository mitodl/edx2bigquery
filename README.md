edx2bigquery
============

edx2bigquery is a tool for importing edX SQL and log data into Google
BigQuery for research and analysis.

To get started, install the Google Cloud SDK first:

  https://cloud.google.com/sdk/
  
To install:

  python setup.py install
  
Also, setup the file edx2bigquery_config.py in your current working directory.  Example:

#-----------------------------------------------------------------------------
#
# sample edx2bigquery_config.py file
#
course_id_list = [
        "MITx/2.03x/3T2013",
]

# google cloud project access
auth_key_file = "/path/to/service-key/<keyfile.p12>"
auth_service_acct = '<project-service-account-email-address>@developer.gserviceaccount.com'

# google bigquery config
PROJECT_ID = "x-data"

# google cloud storage
GS_BUCKET = "gs://x-data"

# local file configuration
COURSE_SQL_BASE_DIR = "X-Year-1-data-sql"
COURSE_SQL_DATE_DIR = '2013-09-08'
TRACKING_LOGS_DIRECTORY = "TRACKING_LOGS"

#-----------------------------------------------------------------------------

To run:

  edx2bigquery
  
Command help message:

usage: edx2bigquery [-h] [--course_base_dir COURSE_BASE_DIR]
                    [--course_date_dir COURSE_DATE_DIR]
                    [--start_date START_DATE] [--end_date END_DATE]
                    [--tlfn TLFN] [-v] [--year2] [--force-recompute]
                    [--nskip NSKIP] [--logs-dir LOGS_DIR]
                    [--output-project-id OUTPUT_PROJECT_ID]
                    [--output-dataset-id OUTPUT_DATASET_ID]
                    [--output-bucket OUTPUT_BUCKET]
                    command [courses [courses ...]]

usage: %prog [command] [options] [arguments]

Examples of common commands:

edx2bigquery setup_sql MITx/24.00x/2013_SOND
edx2bigquery --tlfn=DAILY/mitx-edx-events-2014-10-14.log.gz  --year2 daily_logs
edx2bigquery --year2 person_course
edx2bigquery --year2 report
edx2bigquery --year2 combinepc
edx2bigquery --year2 --output-bucket="gs://harvardx-data" --nskip=2 --output-project-id='harvardx-data' combinepc >& LOG.combinepc

Examples of not-so common commands:

edx2bigquery person_day MITx/2.03x/3T2013 >& LOG.person_day
edx2bigquery --force-recompute person_course --year2 >& LOG.person_course
edx2bigquery testbq
edx2bigquery make_uic --year2
edx2bigquery logs2bq MITx/24.00x/2013_SOND
edx2bigquery person_course MITx/24.00x/2013_SOND >& LOG.person_course
edx2bigquery split DAILY/mitx-edx-events-2014-10-14.log.gz 

positional arguments:
  command               A variety of commands are available, each with different arguments:
                        
                        --- TOP LEVEL COMMANDS
                        
                        setup_sql <course_id> ...   : Do all commands (make_uic, sql2bq, load_forum) to get edX SQL data into the right format, upload to
                                                      google storage, and import into BigQuery.  See more information about each of those commands, below.
                                                      This step is idempotent - it can be re-run multiple times, and the result should not change.
                                                      Returns when all uploads and imports are completed.
                        
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        daily_logs --tlfn=<path>    : Do all commands (split, logs2gs, logs2bq) to get one day's edX tracking logs into google storage 
                                   <course_id>        and import into BigQuery.  See more information about each of those commands, below.
                                   ...                This step is idempotent - it can be re-run multiple times, and the result should not change.
                                                      Returns when all uploads and imports are completed.
                        
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        --- SQL DATA RELATED COMMANDS
                        
                        make_uic <course_id> ...    : make the "user_info_combo" file for the specified course_id, from edX's SQL dumps, and upload to google storage.
                                                      Does not import into BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        sql2bq <course_id> ...      : load specified course_id SQL files into google storage, and import the user_info_combo and studentmodule
                                                      data into BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        load_forum <course_id> ...  : Rephrase the forum.mongo data from the edX SQL dump, to fit the schema used for forum
                                                      data in the course BigQuery tables.  Saves this to google storage, and imports into BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        --- TRACKING LOG DATA RELATED COMMANDS
                        
                        split <daily_log_file> ...  : split single-day tracking log files (should be named something like mitx-edx-events-2014-10-17.log.gz),
                                                      which have aleady been decrypted, into DIR/<course>/tracklog-YYYY-MM-DD.json.gz for each course_id.
                                                      The course_id is determined by parsing each line of the tracking log.  Each line is also
                                                      rephrased such that it is consistent with the tracking log schema defined for import
                                                      into BigQuery.  For example, "event" is turned into a string, and "event_struct" is created
                                                      as a parsed JSON dict for certain event_type values.  Also, key names cannot contain
                                                      dashes or periods.  Uses --logs-dir option, or, failing that, TRACKING_LOGS_DIRECTORY in the
                                                      edx2bigquery_config file.  Employs DIR/META/* files to keep track of which log files have been
                                                      split and rephrased, such that this command's actions are idempotent.
                        
                        logs2gs <course_id> ...     : transfer compressed daily tracking log files for the specified course_id's to Google cloud storage.
                                                      Does NOT import the log data into BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        logs2bq <course_id> ...     : import daily tracking log files for the specified course_id's to BigQuery.
                                                      The import jobs are queued; this does not wait for the jobs to complete,
                                                      before exiting.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        mongo2gs <course_id> ...    : extract tracking logs from mongodb (using mongoexport) for the specified course_id and upload to google storage.
                                                      uses the --start-date and --end-date options.  Skips dates for which the correspnding file in google storage
                                                      already exists.  
                                                      Rephrases log file entries to be consistent with the schema used for tracking log file data in BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        --- REPORTING COMMANDS
                        
                        person_day <course_id> ...  : Compute the person_course_day (pcday) for the specified course_id's, based on 
                                                      processing the course's daily tracking log table data.
                                                      The compute (query) jobs are queued; this does not wait for the jobs to complete,
                                                      before exiting.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        person_course <course_id> ..: Compute the person-course table for the specified course_id's.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        report <course_id> ...      : Compute overall statistics, across all specified course_id's, based on the person_course tables.
                                                      Accepts the --nskip=XXX optional argument to determine how many report processing steps to skip.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        combinepc <course_id> ...   : Combine individual person_course tables from the specified course_id's, uploads CSV to
                                                      google storage.
                                                      Does NOT import the data into BigQuery.
                                                      Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                        
                        --- TESTING & DEBUGGING COMMANDS
                        
                        rephrase_logs               : process input tracking log lines one at a time from standard input, and rephrase them to fit the
                                                      schema used for tracking log file data in BigQuery.  Used for testing.
                        
                        testbq                      : test authentication to BigQuery, by listing accessible datasets.
                        
                        get_tables <dataset>        : dump information about the tables in the specified BigQuery dataset.
                        
                        get_table_info <dataset> <table_id>   : dump meta-data information about the specified dataset.table_id from BigQuery.
                        
                        delete_empty_tables <course_id> ...   : delete empty tables form the tracking logs dataset for the specified course_id's, from BigQuery.
                                                                Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
  courses               courses or course directories, depending on the command

optional arguments:
  -h, --help            show this help message and exit
  --course_base_dir COURSE_BASE_DIR
                        base directory where course SQL is stored, e.g. 'HarvardX-Year-2-data-sql'
  --course_date_dir COURSE_DATE_DIR
                        date sub directory where course SQL is stored, e.g. '2014-09-21'
  --start_date START_DATE
                        start date for person-course dataset generated, e.g. '2012-09-01'
  --end_date END_DATE   end date for person-course dataset generated, e.g. '2014-09-21'
  --tlfn TLFN           path to daily tracking log file to import, e.g. 'DAILY/mitx-edx-events-2014-10-14.log.gz'
  -v, --verbose         increase output verbosity
  --year2               increase output verbosity
  --force-recompute     force recomputation
  --nskip NSKIP         number of steps to skip
  --logs-dir LOGS_DIR   directory to output split tracking logs into
  --output-project-id OUTPUT_PROJECT_ID
                        project-id where the report output should go (used by the report and combinepc commands)
  --output-dataset-id OUTPUT_DATASET_ID
                        dataset-id where the report output should go (used by the report and combinepc commands)
  --output-bucket OUTPUT_BUCKET
                        gs bucket where the report output should go, e.g. gs://x-data (used by the report and combinepc commands)
