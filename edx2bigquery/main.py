#!/usr/bin/python
#
# edx2bigquery main entry point
#

import os
import sys
import argparse
import json
import traceback

from path import path

from argparse import RawTextHelpFormatter

CURDIR = path(os.path.abspath(os.curdir))
if os.path.exists(CURDIR / 'edx2bigquery_config.py'):
    sys.path.append(CURDIR)
    import edx2bigquery_config			# user's configuration parameters
else:
    print "WARNING: edx2bigquery needs a configuration file, ./edx2bigquery_config.py, to operate properly"

def is_valid_course_id(course_id):
    if not course_id.count('/')==2:
        return False
    return True

def get_course_ids(args, do_check=True):
    courses = get_course_ids_no_check(args)
    if do_check:
        if not all(map(is_valid_course_id, courses)):
            print "Error!  Invalid course_id:"
            for cid in courses:
                if not is_valid_course_id(cid):
                    print "  BAD --> %s " % cid
            sys.exit(-1)
    return courses

def get_course_ids_no_check(args):
    if type(args)==str:		# special case: a single course, already specified
        return [ args ]
    if args.clist:
        course_dicts = getattr(edx2bigquery_config, 'courses', None)
        if course_dicts is None:
            print "The --courses argument requires that the 'courses' dict be defined within the edx2bigquery_config.py configuraiton file"
            sys.exit(-1)
        if args.clist not in course_dicts:
            print "The --courses argument specified a course list of name '%s', but that does not exist in the courses dict in the config file" % args.clist
            print "The courses dict only has these lists defined: %s" % course_dicts.keys()
            sys.exit(-1)
        return course_dicts[args.clist]
    if args.year2:
        return edx2bigquery_config.course_id_list
    return args.courses

def CommandLine():
    help_text = """usage: %prog [command] [options] [arguments]

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

"""
    parser = argparse.ArgumentParser(description=help_text, formatter_class=RawTextHelpFormatter)

    cmd_help = """A variety of commands are available, each with different arguments:

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
                              person_course, and problem_check for each of the specified courses.  This is idempotent, and can be run
                              weekly when new SQL dumps come in.

nightly <course_id> ...     : Run sequence of commands for common nightly update (based on having new tracking logs available).
                              This includes logs2gs, logs2bq, person_day, enrollment_day, person_course (forced recompute),
                              and problem_check.

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

staff2bq <staff.csv>        : load staff.csv file into BigQuery; put it in the "courses" dataset.

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

--- COURSE CONTENT DATA RELATED COMMANDS

axis2bq <course_id> ...     : construct "course axis" table, upload to gs, and generate table in BigQuery dataset for the
                              specified course_id's.  
                              Accepts the "--clist" flag, to process specified list of courses in the config file's "courses" dict.

make_cinfo listings.csv     : make the courses.listings table, which contains a listing of all the courses with metadata.
                              The listings.csv file should contain the columns Institution, Semester, New or Rerun, Course Number,
                              Short Title, Title, Instructors, Registration Open, Course Launch, Course Wrap, course_id

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

combinepc <course_id> ...   : Combine individual person_course tables from the specified course_id's, uploads CSV to
                              google storage.
                              Also imports the data into BigQuery.
                              Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

problem_check <c_id> ...    : Create or update problem_check table, which has all the problem_check events from all the course's
                              tracking logs.

--- TESTING & DEBUGGING COMMANDS

rephrase_logs               : process input tracking log lines one at a time from standard input, and rephrase them to fit the
                              schema used for tracking log file data in BigQuery.  Used for testing.

testbq                      : test authentication to BigQuery, by listing accessible datasets.

get_tables <dataset>        : dump information about the tables in the specified BigQuery dataset.

get_table_data <dataset>    : dump table data as JSON text to stdout
               <table_id>

get_table_info <dataset>    : dump meta-data information about the specified dataset.table_id from BigQuery.
               <table_id>

delete_empty_tables         : delete empty tables form the tracking logs dataset for the specified course_id's, from BigQuery.
            <course_id> ...   Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

delete_stats_tables         : delete stats_activity_by_day tables 
            <course_id> ...   Accepts the "--year2" flag, to process all courses in the config file's course_id_list.
"""

    parser.add_argument("command", help=cmd_help)
    # parser.add_argument("-C", "--course_id", type=str, help="course ID in org/number/semester format, e.g. MITx/6.SFMx/1T2014")
    parser.add_argument("--course-base-dir", type=str, help="base directory where course SQL is stored, e.g. 'HarvardX-Year-2-data-sql'")
    parser.add_argument("--course-date-dir", type=str, help="date sub directory where course SQL is stored, e.g. '2014-09-21'")
    parser.add_argument("--start-date", type=str, help="start date for person-course dataset generated, e.g. '2012-09-01'")
    parser.add_argument("--end-date", type=str, help="end date for person-course dataset generated, e.g. '2014-09-21'")
    parser.add_argument("--tlfn", type=str, help="path to daily tracking log file to import, e.g. 'DAILY/mitx-edx-events-2014-10-14.log.gz'")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("--year2", help="increase output verbosity", action="store_true")
    parser.add_argument("--clist", type=str, help="specify name of list of courses to iterate command over")
    parser.add_argument("--force-recompute", help="force recomputation", action="store_true")
    parser.add_argument("--dataset-latest", help="use the *_latest SQL dataset", action="store_true")
    parser.add_argument("--skip-geoip", help="skip geoip processing in person_course", action="store_true")
    parser.add_argument("--skip-if-exists", help="skip processing in person_course if table already exists", action="store_true")
    parser.add_argument("--just-do-nightly", help="for person_course, just update activity stats for new logs", action="store_true")
    parser.add_argument("--nskip", type=int, help="number of steps to skip")
    parser.add_argument("--logs-dir", type=str, help="directory to output split tracking logs into")
    parser.add_argument("--dbname", type=str, help="mongodb db name to use for mongo2gs")
    parser.add_argument("--table", type=str, help="bigquery table to use, specified as dataset_id.table_id") 
    parser.add_argument("--org", type=str, help="organization ID to use")
    parser.add_argument("--collection", type=str, help="mongodb collection name to use for mongo2gs")
    parser.add_argument("--output-project-id", type=str, help="project-id where the report output should go (used by the report and combinepc commands)")
    parser.add_argument("--output-dataset-id", type=str, help="dataset-id where the report output should go (used by the report and combinepc commands)")
    parser.add_argument("--output-bucket", type=str, help="gs bucket where the report output should go, e.g. gs://x-data (used by the report and combinepc commands)")
    parser.add_argument('courses', nargs = '*', help = 'courses or course directories, depending on the command')
    
    args = parser.parse_args()
    if args.verbose:
        sys.stderr.write("command = %s\n" % args.command)
        sys.stderr.flush()

    the_basedir = args.course_base_dir or getattr(edx2bigquery_config, "COURSE_SQL_BASE_DIR", None)
    the_datedir = args.course_date_dir or getattr(edx2bigquery_config, "COURSE_SQL_DATE_DIR", None)
    use_dataset_latest = args.dataset_latest

    # default end date for person_course
    try:
        DEFAULT_END_DATE =  getattr(edx2bigquery_config, "DEFAULT_END_DATE", "2014-09-21")
    except:
        DEFAULT_END_DATE = "2014-09-21"

    def setup_sql(args, steps, course_id=None):
        sqlall = steps=='setup_sql'
        if course_id is None:
            for course_id in get_course_ids(args):
                print "="*100
                print "Processing setup_sql for %s" % course_id
                sys.stdout.flush()
                try:
                    setup_sql(args, steps, course_id)
                except Exception as err:
                    print "===> Error completing setup_sql on %s, err=%s" % (course_id, str(err))
                    traceback.print_exc()
                    sys.stdout.flush()
            return

        if sqlall or 'make_uic' in steps:
            import make_user_info_combo
            make_user_info_combo.process_file(course_id, 
                                              basedir=the_basedir,
                                              datedir=the_datedir,
                                              use_dataset_latest=use_dataset_latest,
                                              )
        if sqlall or 'sql2bq' in steps:
            import load_course_sql
            try:
                load_course_sql.load_sql_for_course(course_id, 
                                                    gsbucket=edx2bigquery_config.GS_BUCKET,
                                                    basedir=the_basedir,
                                                    datedir=the_datedir,
                                                    do_gs_copy=True,
                                                    use_dataset_latest=use_dataset_latest,
                                                    )
            except Exception as err:
                print err
            
        if sqlall or 'load_forum' in steps:
            import rephrase_forum_data
            try:
                rephrase_forum_data.rephrase_forum_json_for_course(course_id,
                                                                   gsbucket=edx2bigquery_config.GS_BUCKET,
                                                                   basedir=the_basedir,
                                                                   datedir=the_datedir,
                                                                   use_dataset_latest=use_dataset_latest,
                                                                   )
            except Exception as err:
                print err
                    
    def daily_logs(args, steps, course_id=None, verbose=True, wait=False):
        if steps=='daily_logs':
            # doing daily_logs, so run split once first, then afterwards logs2gs and logs2bq
            daily_logs(args, 'split', args.tlfn)
            for course_id in get_course_ids(args):
                daily_logs(args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose)
            return

        if course_id is None:
            do_check = not (steps=='split')
            for course_id in get_course_ids(args, do_check=do_check):
                print "---> Processing %s on course_id=%s" % (steps, course_id)
                daily_logs(args, steps, course_id)
            return

        if 'split' in steps:
            import split_and_rephrase
            tlfn = course_id		# tracking log filename
            if '*' in tlfn:
                import glob
                TODO = glob.glob(tlfn)
                TODO.sort()
            else:
                TODO = [tlfn]
            for the_tlfn in TODO:
                print "--> Splitting tracking logs in %s" % the_tlfn
                split_and_rephrase.do_file(the_tlfn, args.logs_dir or edx2bigquery_config.TRACKING_LOGS_DIRECTORY)

        if 'logs2gs' in steps:
            import transfer_logs_to_gs
            try:
                transfer_logs_to_gs.process_dir(course_id, 
                                                edx2bigquery_config.GS_BUCKET,
                                                args.logs_dir or edx2bigquery_config.TRACKING_LOGS_DIRECTORY,
                                                verbose=verbose,
                                                )
            except Exception as err:
                print err

        if 'logs2bq' in steps:
            import load_daily_tracking_logs
            try:
                load_daily_tracking_logs.load_all_daily_logs_for_course(course_id, edx2bigquery_config.GS_BUCKET,
                                                                        verbose=verbose, wait=wait,
                                                                        check_dates= (not wait),
                                                                        )
            except Exception as err:
                print err
                raise
                
    def analyze_problems(courses):
        import make_problem_analysis
        for course_id in get_course_ids(courses):
            try:
                make_problem_analysis.analyze_problems(course_id, 
                                                       basedir=the_basedir, 
                                                       datedir=the_datedir,
                                                       force_recompute=args.force_recompute,
                                                       use_dataset_latest=use_dataset_latest,
                                                       )
            except Exception as err:
                print err
                traceback.print_exc()
                sys.stdout.flush()
        
    def problem_check(courses):
        import make_problem_analysis
        for course_id in get_course_ids(courses):
            try:
                make_problem_analysis.problem_check_tables(course_id, 
                                                           force_recompute=args.force_recompute,
                                                           use_dataset_latest=use_dataset_latest,
                                                           end_date=args.end_date,
                                                           )
            except Exception as err:
                print err
                traceback.print_exc()
                sys.stdout.flush()
        
    def axis2bq(courses):
        import edx2course_axis
        import load_course_sql
        import axis2bigquery
        for course_id in get_course_ids(courses):
            if args.skip_if_exists and axis2bigquery.already_exists(course_id, use_dataset_latest=use_dataset_latest):
                print "--> course_axis for %s already exists, skipping" % course_id
                sys.stdout.flush()
                continue
            sdir = load_course_sql.find_course_sql_dir(course_id, 
                                                       basedir=the_basedir, 
                                                       datedir=the_datedir,
                                                       use_dataset_latest=use_dataset_latest,
                                                       )
            edx2course_axis.DATADIR = sdir
            edx2course_axis.VERBOSE_WARNINGS = args.verbose
            fn = sdir / 'course.xml.tar.gz'
            if not os.path.exists(fn):
                fn = sdir / 'course-prod-analytics.xml.tar.gz'
                if not os.path.exists(fn):
                    print "---> oops, cannot generate course axis for %s, file %s (or 'course.xml.tar.gz') missing!" % (course_id, fn)
                    continue
            try:
                edx2course_axis.process_xml_tar_gz_file(fn,
                                                       use_dataset_latest=use_dataset_latest)
            except Exception as err:
                print err
                # raise
            

    def person_day(courses, check_dates=True):
        import make_person_course_day
        for course_id in get_course_ids(courses):
            try:
                make_person_course_day.process_course(course_id, 
                                                      force_recompute=args.force_recompute,
                                                      use_dataset_latest=use_dataset_latest,
                                                      check_dates=check_dates,
                                                      end_date=args.end_date,
                                                      )
            except Exception as err:
                print err
                traceback.print_exc()
                sys.stdout.flush()
            

    def pcday_ip(courses):
        import make_person_course_day
        for course_id in get_course_ids(courses):
            try:
                make_person_course_day.compute_person_course_day_ip_table(course_id, force_recompute=args.force_recompute,
                                                                          use_dataset_latest=use_dataset_latest,
                                                                          end_date=args.end_date,
                                                                          )
            except Exception as err:
                print err
                traceback.print_exc()
                sys.stdout.flush()


    def enrollment_day(courses):
        import make_enrollment_day
        for course_id in get_course_ids(courses):
            try:
                make_enrollment_day.process_course(course_id, 
                                                   force_recompute=args.force_recompute,
                                                   use_dataset_latest=use_dataset_latest)
            except Exception as err:
                print err
                traceback.print_exc()
                sys.stdout.flush()
        
    def person_course(courses, just_do_nightly=False, force_recompute=False):
        import make_person_course
        print "[person_course]: for end date, using %s" % (args.end_date or DEFAULT_END_DATE)
        for course_id in get_course_ids(courses):
            try:
                make_person_course.make_person_course(course_id,
                                                      gsbucket=edx2bigquery_config.GS_BUCKET,
                                                      basedir=the_basedir,
                                                      datedir=the_datedir,
                                                      start=(args.start_date or "2012-09-05"),
                                                      end=(args.end_date or DEFAULT_END_DATE),
                                                      force_recompute=args.force_recompute or force_recompute,
                                                      nskip=(args.nskip or 0),
                                                      skip_geoip=args.skip_geoip,
                                                      skip_if_table_exists=args.skip_if_exists,
                                                      use_dataset_latest=use_dataset_latest,
                                                      just_do_nightly=just_do_nightly or args.just_do_nightly,
                                                      )
            except Exception as err:
                print err
                if ('no user_info_combo' in str(err)) or ('aborting - no dataset' in str(err)):
                    continue
                if ('Internal Error' in str(err)):
                    continue
                if ('BQ Error creating table' in str(err)):
                    continue
                raise

    #-----------------------------------------------------------------------------            

    if (args.command=='mongo2gs'):
        from extract_logs_mongo2gs import  extract_logs_mongo2gs
        for course_id in get_course_ids(args):
            extract_logs_mongo2gs(course_id, verbose=args.verbose,
                                  start=(args.start_date or "2012-09-05"),
                                  end=(args.end_date or DEFAULT_END_DATE),
                                  dbname=args.dbname,
                                  collection=args.collection or 'tracking_log',
                                  tracking_logs_directory=args.logs_dir or edx2bigquery_config.TRACKING_LOGS_DIRECTORY,
                                  )
        
    elif (args.command=='rephrase_logs'):
        if args.courses:
            # if arguments are provided, they are taken as filenames of files to be rephrased IN PLACE
            from rephrase_tracking_logs import do_rephrase_file
            files = args.courses
            for fn in files:
                do_rephrase_file(fn)
        else:
            from rephrase_tracking_logs import do_rephrase_line
            for line in sys.stdin:
                newline = do_rephrase_line(line)
                sys.stdout.write(newline)

    elif (args.command=='doall'):
        for course_id in get_course_ids(args):
            try:
                print "-"*100
                print "DOALL PROCESSING %s" % course_id
                print "-"*100
                setup_sql(course_id, 'setup_sql')
                analyze_problems(course_id)
                axis2bq(course_id)
                daily_logs(args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose, wait=True)
                pcday_ip(course_id)	# needed for modal IP
                person_day(course_id)
                enrollment_day(course_id)
                person_course(course_id)
                problem_check(course_id)
            except Exception as err:
                print "="*100
                print "ERROR: %s" % str(err)
                traceback.print_exc()

    elif (args.command=='nightly'):
        for course_id in get_course_ids(args):
            print "-"*100
            print "NIGHTLY PROCESSING %s" % course_id
            print "-"*100
            try:
                daily_logs(args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose, wait=True)
                person_day(course_id, check_dates=False)
                enrollment_day(course_id)
                pcday_ip(course_id)	# needed for modal IP
                person_course(course_id, just_do_nightly=True, force_recompute=True)
                problem_check(course_id)
            except Exception as err:
                print "="*100
                print "ERROR: %s" % str(err)
                traceback.print_exc()

    elif (args.command=='make_uic'):
        setup_sql(args, args.command)
                                              
    elif (args.command=='sql2bq'):
        setup_sql(args, args.command)

    elif (args.command=='load_forum'):
        setup_sql(args, args.command)

    elif (args.command=='setup_sql'):
        setup_sql(args, args.command)

    elif (args.command=='waldofy'):
        import do_waldofication_of_sql
        dirname = args.courses[0]		# directory of unpacked SQL data from edX
        args.courses = args.courses[1:]		# remove first element, which was dirname
        courses = get_course_ids(args)
        do_waldofication_of_sql.process_directory(dirname, courses, the_basedir)

    elif (args.command=='makegeoip'):
        import make_geoip_table
        gid = make_geoip_table.GeoIPData()
        gid.make_table(args.table,
                       org=args.org or getattr(edx2bigquery_config, 'ORG'),
                       nskip=(args.nskip or 0),
                       )

    elif (args.command=='testbq'):
        # test authentication to bigquery - list databases in project
        import bqutil
        bqutil.auth.print_creds()
        print "="*20
        print "list of datasets accessible:"
        print json.dumps(bqutil.get_list_of_datasets().keys(), indent=4)

    elif (args.command=='get_tables'):
        import bqutil
        print json.dumps(bqutil.get_tables(args.courses[0]), indent=4)

    elif (args.command=='get_table_data'):
        import bqutil
        dataset = args.courses[0].replace('/', '__').replace('.', '_')
        print json.dumps(bqutil.get_table_data(dataset, args.courses[1]), indent=4)

    elif (args.command=='get_table_info'):
        import bqutil
        print json.dumps(bqutil.get_bq_table_info(args.courses[0], args.courses[1]), indent=4)

    elif (args.command=='delete_empty_tables'):
        import bqutil
        for course_id in get_course_ids(args):
            try:
                dataset = bqutil.course_id2dataset(course_id, dtype="logs")
                bqutil.delete_zero_size_tables(dataset, verbose=True)
            except Exception as err:
                print err
                raise

    elif (args.command=='delete_stats_tables'):
        import bqutil
        for course_id in get_course_ids(args):
            try:
                dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
                bqutil.delete_bq_table(dataset, 'stats_activity_by_day')
            except Exception as err:
                print err
                raise

    elif (args.command=='daily_logs'):
        daily_logs(args, args.command)

    elif (args.command=='split'):
        daily_logs(args, args.command)

    elif (args.command=='logs2gs'):
        daily_logs(args, args.command)

    elif (args.command=='logs2bq'):
        daily_logs(args, args.command)

    elif (args.command=='tsv2csv'):
        import csv
        fp = csv.writer(sys.stdout)
        for line in sys.stdin:
            fp.writerow(line[:-1].split('\t'))

    elif (args.command=='analyze_problems'):
        analyze_problems(args)

    elif (args.command=='problem_check'):
        problem_check(args)

    elif (args.command=='axis2bq'):
        axis2bq(args)

    elif (args.command=='staff2bq'):
        import load_staff
        load_staff.do_staff_csv(args.courses[0])

    elif (args.command=='make_cinfo'):
        import make_cinfo
        make_cinfo.do_course_listings(args.courses[0])

    elif (args.command=='pcday_ip'):
        pcday_ip(args)

    elif (args.command=='person_day'):
        person_day(args)

    elif (args.command=='enrollment_day'):
        enrollment_day(args)

    elif (args.command=='person_course'):
        person_course(args)

    elif (args.command=='report'):
        import make_course_report_tables
        make_course_report_tables.CourseReport(get_course_ids(args), 
                                               nskip=(args.nskip or 0),
                                               output_project_id=args.output_project_id or edx2bigquery_config.PROJECT_ID,
                                               output_dataset_id=args.output_dataset_id,
                                               output_bucket=args.output_bucket or edx2bigquery_config.GS_BUCKET,
                                               use_dataset_latest=use_dataset_latest,
                                               )

    elif (args.command=='combinepc'):
        import make_combined_person_course
        make_combined_person_course.do_combine(get_course_ids(args),
                                               edx2bigquery_config.PROJECT_ID,
                                               nskip=(args.nskip or 0),
                                               output_project_id=args.output_project_id or edx2bigquery_config.PROJECT_ID,
                                               output_dataset_id=args.output_dataset_id,
                                               output_bucket=args.output_bucket or edx2bigquery_config.GS_BUCKET,
                                               use_dataset_latest=use_dataset_latest,
                                               )

    else:
        print "Unknown command %s!" % args.command
        sys.exit(-1)
