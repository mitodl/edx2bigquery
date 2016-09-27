edx2bigquery
============

edx2bigquery is a tool for importing edX SQL and log data into Google
BigQuery for research and analysis.

To get started, install the Google Cloud SDK first:

  https://cloud.google.com/sdk/

Then generate authentication tokens using "gcloud auth login". Make sure
that "bq" and "gsutil" work properly.

To install:

  python setup.py develop

Also, setup the file edx2bigquery_config.py in your current working directory.  Example:

    #-----------------------------------------------------------------------------
    #
    # sample edx2bigquery_config.py file
    #
    course_id_list = [
            "MITx/2.03x/3T2013",
    ]

    courses = {
        'year2': course_id_list,
        'all_harvardx': [
            "HarvardX/AI12.1x/2013_SOND",
        ],
    }

    # google cloud project access
    auth_key_file = "USE_GCLOUD_AUTH"
    auth_service_acct = None

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

    usage: edx2bigquery [-h] [--course-base-dir COURSE_BASE_DIR]
                        [--course-date-dir COURSE_DATE_DIR]
                        [--start-date START_DATE] [--end-date END_DATE]
                        [--tlfn TLFN] [-v] [--year2] [--clist CLIST]
                        [--force-recompute] [--dataset-latest] [--skip-geoip]
                        [--skip-if-exists] [--nskip NSKIP] [--logs-dir LOGS_DIR]
                        [--dbname DBNAME] [--table TABLE] [--org ORG]
                        [--collection COLLECTION]
                        [--output-project-id OUTPUT_PROJECT_ID]
                        [--output-dataset-id OUTPUT_DATASET_ID]
                        [--output-bucket OUTPUT_BUCKET]
                        command [courses [courses ...]]

    usage: %prog [command] [options] [arguments]

    Examples of common commands:

    edx2bigquery --clist=all_mitx logs2gs
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

                                                          Accepts the --dataset-latest flag, to use the latest directory date in the SQL data directory.
                                                          Directories should be named YYYY-MM-DD.  When this flag is used, the course SQL dataset name has
                                                          "_latest" appended to it.

                            daily_logs --tlfn=<path>    : Do all commands (split, logs2gs, logs2bq) to get one day's edX tracking logs into google storage
                                       <course_id>        and import into BigQuery.  See more information about each of those commands, below.
                                       ...                This step is idempotent - it can be re-run multiple times, and the result should not change.
                                                          Returns when all uploads and imports are completed.

                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            doall <course_id> ...       : run setup_sql, analyze_problems, logs2gs, logs2bq, axis2bq, person_day, enrollment_day,
                                                          and person_course, for each of the specified courses.  This is idempotent, and can be run
                                                          weekly when new SQL dumps come in.

                            nightly <course_id> ...     : Run sequence of commands for common nightly update (based on having new tracking logs available).
                                                          This includes logs2gs, logs2bq, person_day, enrollment_day

                            external command <cid>...   : Run external command on data from one or more course_id's.  Also uses the --extparam settings.
                                                          External commands are defined in edx2bigquery_config.  Use --skiprun to create the external script without running.  
                                                          Use --submit-condor to submit command as a condor job.

                            --- SQL DATA RELATED COMMANDS

                            waldofy <sql_data_dir>      : Apply HarvardX Jim Waldo conventions to SQL data as received from edX, which renames files to be
                                    <course_id> ...       more user friendly (e.g. courseware_studentmodule -> studentmodule) and converts the tab-separated
                                                          values form (*.sql) to comma-separated values (*.csv).  Also compresses the resulting csv files.
                                                          Does this only for the specified course's, because the edX SQL dump may contain a bunch of
                                                          uknown courses, or scratch courses from the edge site, which should not be co-mingled with
                                                          course data from the main production site.  

                                                          It is assumed that <sql_data_dir> has a name which contains a date, e.g. xorg-2014-05-11 ;
                                                          the resulting data are put into the course SQL base directory, into a subdirectory with
                                                          name given by the course_id and date, YYYY-MM-DD.

                                                          The SQL files from edX must already be decrypted (not *.gpg), before running this command.

                                                          Be sure to specify which course_id's to act upon.  Courses which are not explicitly specified
                                                          are put in a subdirectory named "UNKNOWN"

                            make_uic <course_id> ...    : make the "user_info_combo" file for the specified course_id, from edX's SQL dumps, and upload to google storage.
                                                          Does not import into BigQuery.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            sql2bq <course_id> ...      : load specified course_id SQL files into google storage, and import the user_info_combo and studentmodule
                                                          data into BigQuery.
                                                          Also upload course_image.jpg images from the course SQL directories (if image exists) to
                                                          google storage, and make them public-read.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            load_forum <course_id> ...  : Rephrase the forum.mongo data from the edX SQL dump, to fit the schema used for forum
                                                          data in the course BigQuery tables.  Saves this to google storage, and imports into BigQuery.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            makegeoip                   : Creates table of geoip information for IP addresses in person_course table missing country codes.
                                                          Accepts the --table argument to specify the person_course table to use.
                                                          Alternatively, provide the --org argument to specify the course_report_ORG dataset to look
                                                          in for the latest person_course dataset.

                            tsv2csv                     : filter, which takes lines of tab separated values and outputs lines of comma separated values.
                                                          Useful when processing the *.sql files from edX dumps.

                            analyze_problems <c_id> ... : Analyze capa problem data in studentmodule table, generating the problem_analysis table as a result.  
                                                          Uploads the result to google cloud storage and to BigQuery.
                                                          This table is necessary for the insights dashboard.
                                                          Accepts --only-step=grades | show_answer | analysis, show_answer command requires existing show_answer table
                                                          and generates show_answer_stats_by_course and show_answer_stats_by_user

                            analyze_videos <course_id>  : Analyze videos viewed and videos watched, generating tables video_axis (based on course axis),
                                                          and video_stats_day and video_stats, based on daily tracking logs, for specified course.

                            analyze_forum <course_id>   : Analyze forum events, generating the forum_events table from the daily tracking logs, for specified course.

                            analyze_ora <course_id> ... : Analyze openassessment response problem data in tracking logs, generating the ora_events table as a result.  
                                                          Uploads the result to google cloud storage and to BigQuery.

                            problem_events <course_id>  : Extract capa problem events from the tracking logs, generating the problem_events table as a result.

                            time_task <course_id> ...   : Update time_task table of data on time on task, based on daily tracking logs, for specified course.

                            item_tables <course_id> ... : Make course_item and person_item tables, used for IRT analyses.

                            staff2bq <staff.csv>        : load staff.csv file into BigQuery; put it in the "courses" dataset.

                            mongo2user_info <course_id> : dump users, profiles, enrollment, certificates CSV files from mongodb for specified course_id's.
                                                          Use this to address missing users issue in pre-early-2014 edX course dumps, which had the problem that learners
                                                          who un-enrolled were removed from the enrollment table.

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

                            logs2bq <course_id> ...     : import daily tracking log files for the specified course_id's to BigQuery, from Google cloud storage.
                                                          The import jobs are queued; this does not wait for the jobs to complete,
                                                          before exiting.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            mongo2gs <course_id> ...    : extract tracking logs from mongodb (using mongoexport) for the specified course_id and upload to google storage.
                                                          uses the --start-date and --end-date options.  Skips dates for which the correspnding file in google storage
                                                          already exists.  
                                                          Rephrases log file entries to be consistent with the schema used for tracking log file data in BigQuery.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.


                            course_key_version <cid>    : print out what version of course key (standard, v1) is being used for a given course_id.     
                                                          Needed for opaque key handling.  The "standard" course_id format is "org/course/semester".  
                                                          The opaque-key v1 format is "course-v1:org+course+semester".  
                                                          Opaque keys also mangle what is known as a the "module_id", and use something like "block-v1:MITx+8.MechCx_2+2T2015+type@problem+block@Blocks_on_Ramp_randxyzBILNKOA0".
                                                          The opaque key ID's are changed back into the standard format, in most of the parsing of the SQL and the tracking logs (see addmoduleid.py), but it's still necessary to know which kind of key is being used, e.g. do that jump_to_id will work properly when linking back to a live course page.
                                                          course_id is always stored in tradition format.  course_key is kept in either standard or v1 opaque key format.  module_id is always standard, and kept as "org/course/<module_type>/<module_id>".  
                                                          In the future, there may be a module_key, but that idea isn't currently used.  This particular command scans some tracking log entries, and if the count of "block-v1" is high, it assigns "v1" to the course; otherwise, it assigns "standard" as the course key version.

                            --- COURSE CONTENT DATA RELATED COMMANDS

                            axis2bq <course_id> ...     : construct "course axis" table, upload to gs, and generate table in BigQuery dataset for the
                                                          specified course_id's.  
                                                          Accepts the "--clist" flag, to process specified list of courses in the config file's "courses" dict.

                            grading_policy <course_id>  : construct the "grading_policy" table, upload to gs, and generate table in BigQuery dataset, for
                                                          the specified course_id's.  Uses pin dates, just as does axis2bq.  Requires course.tar.gz file, from the weekly SQL dumps.

                            recommend_pin_dates         : produce a list of recommended "pin dates" for the course axis, based on the specified --listings.  Example:
                                                          -->  edx2bigquery --clist=all --listings="course_listings.json" --course-base-dir=DATA-SQL recommend_pin_dates
                                                          These "pin dates" can be defined in edx2bigquery_config in the course_axis_pin_dates dict, to
                                                          specify the specific SQL dump dates to be used for course axis processing (by axis2bq) and for course content analysis (analyze_course).  
                                                          This is often needed when course authors change content after a course ends, e.g. to remove or hide exams, and to change grading and due dates.

                            make_cinfo listings.csv     : make the courses.listings table, which contains a listing of all the courses with metadata.
                                                          The listings.csv file should contain the columns Institution, Semester, New or Rerun, Course Number,
                                                          Short Title, Title, Instructors, Registration Open, Course Launch, Course Wrap, course_id

                            analyze_content             : construct "course_content" table, which counts the number of xmodules of different categories,
                                --listings listings.csv   and also the length of the course in weeks (based on using the listings file).
                                <course_id> ...           Use this for categorizing a course according to the kind of content it has.                               


                            --- REPORTING COMMANDS


                            pcday_ip <course_id> ...    : Compute the pcday_ip_counts table for specified course_id's, based on ingesting
                                                          the tracking logs.  This is a single table stored in the course's main table,
                                                          which is incrementally updated (by appending) when new tracking logs are found.
                                                          This table stores one line per (person, course_id, date, ip address) value from the
                                                          tracking logs.  Aggregation of these rows is then done to compute modal IP addresses,
                                                          for geolocation.
                                                          The "report" command aggregates the individual course_id's pcday_ip tables to produce
                                                          the courses.global_modal_ip table, which is the modal IP address of each username across
                                                          all the courses.

                            person_day <course_id> ...  : Compute the person_course_day (pcday) for the specified course_id's, based on
                                                          processing the course's daily tracking log table data.
                                                          The compute (query) jobs are queued; this does not wait for the jobs to complete,
                                                          before exiting.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            enrollment_day <c_id> ...   : Compute the enrollment_day (enrollday2_*) tables for the specified course_id's, based on
                                                          processing the course's daily tracking log table data.
                                                          The compute (query) jobs are queued; this does not wait for the jobs to complete,
                                                          before exiting.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            person_course <course_id> ..: Compute the person-course table for the specified course_id's.
                                                          Needs person_day tables to be created first.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                                                          Accepts the --force-recompute flag, to force recomputation of all pc_* tables in BigQuery.
                                                          Accepts the --skip-if-exists flag, to skip computation of the table already exists in the course's dataset.

                            report <course_id> ...      : Compute overall statistics, across all specified course_id's, based on the person_course tables.
                                                          Accepts the --nskip=XXX optional argument to determine how many report processing steps to skip.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
                                                          Requires show_answer_stats_by_course, time_on_task_totals, pcday_ip_counts and person_course to compute

                            combinepc <course_id> ...   : Combine individual person_course tables from the specified course_id's, uploads CSV to
                                                          google storage.
                                                          Does NOT import the data into BigQuery.
                                                          Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            problem_check <c_id> ...    : Create or update problem_check table, which has all the problem_check events from all the course's tracking logs.

                            attempts_correct <c_id> ... : Create or update stats_attempts_correct table, which records the percentages of attempts which were correct
                                                          for a given user in a specified course.

                            ip_sybils <course_id> ...   : Create or update stats_ip_pair_sybils table, which records harvester-master pairs of users for which
                                                          the IP address is the same, and the pair have meaningful disparities in perfomance.  Requires that attempts_correct be run first.

                            temporal_fingerprints <cid>  : Create or update the problem_check_temporal_fingerprint, show_answer_temporal_fingerprint, and
                                                          stats_temporal_fingerprint_correlations tables.  Part of problem analyses.  May be expensive.

                            show_answer <course_id> ... : Create or update show_answer table, which has all the show_answer events from all the course's
                                                          tracking logs.

                            enrollment_events <cid> ... : Create or update enrollment_events table, which has all the ed.course.enrollment events from all the course's
                                                          tracking logs.

                            --- TESTING & DEBUGGING COMMANDS

                            rephrase_logs               : process input tracking log lines one at a time from standard input, and rephrase them to fit the
                                                          schema used for tracking log file data in BigQuery.  Used for testing.

                            testbq                      : test authentication to BigQuery, by listing accessible datasets.

                            get_course_tables <cid>     : dump list of tables in the course_id BigQuery dataset.  Good to use as a test case for parallel execution.

                            get_tables <dataset>        : dump information about the tables in the specified BigQuery dataset.

                            get_table_data <dataset>    : dump table data as JSON text to stdout
                                           <table_id>

                            get_course_data <course_id> : retrieve course-specific table data as CSV file, saved as CID__tablename.csv, with CID being the course_id with slashes
                                   --table <table_id>     replaced by double underscore ("__").  May specify --project-id and --gzip and --combine-into <output_filename>

                            get_data p_id:d_id.t_id ... : retrieve project:dataset.table data as CSV file, saved as project__dataset__tablename.csv
                                                          May specify --gzip and --combine-into <output_filename>

                            get_table_info <dataset>    : dump meta-data information about the specified dataset.table_id from BigQuery.
                                           <table_id>

                            delete_empty_tables         : delete empty tables form the tracking logs dataset for the specified course_id's, from BigQuery.
                                        <course_id> ...   Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            delete_stats_tables         : delete stats_activity_by_day tables
                                        <course_id> ...   Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

                            check_for_duplicates        : check list of courses for duplicates
                                  --clist <cid_list>...

    optional arguments:
      -h, --help            show this help message and exit
      --course-base-dir COURSE_BASE_DIR
                            base directory where course SQL is stored, e.g. 'HarvardX-Year-2-data-sql'
      --course-date-dir COURSE_DATE_DIR
                            date sub directory where course SQL is stored, e.g. '2014-09-21'
      --start-date START_DATE
                            start date for person-course dataset generated, e.g. '2012-09-01'
      --end-date END_DATE   end date for person-course dataset generated, e.g. '2014-09-21'
      --tlfn TLFN           path to daily tracking log file to import, e.g. 'DAILY/mitx-edx-events-2014-10-14.log.gz'
      -v, --verbose         increase output verbosity
      --year2               process all courses in the config file's course_id_list
      --clist CLIST         specify name of list of courses to iterate command over
      --force-recompute     force recomputation
      --dataset-latest      use the *_latest SQL dataset
      --skip-geoip          skip geoip processing in person_course
      --skip-if-exists      skip processing in person_course if table already exists
      --nskip NSKIP         number of steps to skip
      --logs-dir LOGS_DIR   directory to output split tracking logs into
      --dbname DBNAME       mongodb db name to use for mongo2gs
      --table TABLE         bigquery table to use, specified as dataset_id.table_id
      --org ORG             organization ID to use
      --collection COLLECTION
                            mongodb collection name to use for mongo2gs
      --output-project-id OUTPUT_PROJECT_ID
                            project-id where the report output should go (used by the report and combinepc commands)
      --output-dataset-id OUTPUT_DATASET_ID
                            dataset-id where the report output should go (used by the report and combinepc commands)
      --output-bucket OUTPUT_BUCKET
                            gs bucket where the report output should go, e.g. gs://x-data (used by the report and combinepc commands)
