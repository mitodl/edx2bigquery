#!/usr/bin/python
#
# edx2bigquery main entry point
#

import os
import sys

import argparse
import json
import traceback
import datetime
import multiprocessing as mp

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
    if type(args)==list:
        return args
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

class Parameters(object):
    '''
    Handy empty class for storing option settings; needed to make analysis functions global, so they can be parallelized.
    '''
    def __init__(self):
        return

#-----------------------------------------------------------------------------
# functions for parallel execution

try:
    MAXIMUM_PARALLEL_PROCESSES = getattr(edx2bigquery_config, "MAXIMUM_PARALLEL_PROCESSES", None)
except:
    MAXIMUM_PARALLEL_PROCESSES = 5

class SubProcessStdout(object):
    def __init__(self, label):
        self.label = label
        self.output = ""

    def write(self,msg):
        msg = msg.replace('\n', '\n[%s] ' % self.label)
        self.output += msg
        sys.__stdout__.write(msg)
        self.flush()

    def flush(self):
        sys.__stdout__.flush()

def run_parallel_or_serial(function, param, courses, optargs, parallel=False, name=None):
    '''
    run function(param, course_id, args) for each course_id in the list courses.

    Do this serially if parallel=False.
    Else run in parallel, using a multiprocessing parallel processing pool.
    '''
    if not name:
        name = function.__name__

    if not parallel:
        try:
            ret = None
            for course_id in courses:
                ret = function(param, course_id, optargs)
        except Exception as err:
            print "===> Error running %s on %s, err=%s" % (name, courses, str(err))
            traceback.print_exc()
            sys.stdout.flush()
            ret = {}
        return ret
            
    pool = mp.Pool(processes=param.max_parallel or MAXIMUM_PARALLEL_PROCESSES)
    results = []
    for course_id in courses:
        runname = "%s on %s" % (name, course_id)
        runargs = (function, (param, course_id, optargs), SubProcessStdout(course_id), runname)
        results.append( pool.apply_async(run_capture_stdout, args=(runargs)) )

    output = [p.get() for p in results]
    print "="*100
    print "="*100
    print "PARALLEL RUN of %s DONE" % name
    print "="*100
    print "="*100
    def output_stats():
        for ret in output:
            ret = ret or {}
            print '    [%s] success=%s, dt=%s' % (ret.get('name', name), ret.get('success', 'unknown'), ret.get('dt', 'unknown'))
    output_stats()
    print "="*100
    for ret in output:
        runname = ret.get('name', name)
        print "="*100 + " [%s]" % runname
        if 'stdout' in ret:
            print ret['stdout'].output
    print "="*100
    output_stats()
    print "="*100


def run_capture_stdout(function, args, stdout=None, name="<run>"):
    '''
    run function(*args) and capture stdout.  Return dict with time elapsed, output, and success flag.
    '''
    start = datetime.datetime.now()
    success = False
    print "-"*100
    print "[RUN] STARTING %s at %s" % (name, start)
    print "-"*100
    if stdout:
        sys.stdout = stdout			# overload for multiprocessing, so that we can unravel output streams
    try:
        function(*args)
        errstr = None
        success = True
    except Exception as err:
        errstr = str(err)
    
    end = datetime.datetime.now()
    ret = {'start': start, 'end': end, 'dt' : end-start, 'success': success, 'name': name, 'stdout': stdout}
    print "-"*100
    print "[RUN] DONE WITH %s, success=%s, dt=%s" % (name, ret['success'], ret['dt'])
    print "-"*100
    sys.stdout.flush()
    return ret

#-----------------------------------------------------------------------------
# run external script

def run_external_single(param, course_id, args=None):
    '''
    Run a single external script
    '''
    from run_external import run_external_script
    print "-"*100
    print "[%s] Running external command script %s" % (course_id, param.extcmd)
    print "-"*100
    sys.stdout.flush()
    run_external_script(param.extcmd, param, param.ecinfo, course_id)

#-----------------------------------------------------------------------------
# main functions for performing analysis

def setup_sql(param, args, steps, course_id=None):
    sqlall = steps=='setup_sql'
    if course_id is None:
        for course_id in get_course_ids(args):
            print "="*100
            print "Processing setup_sql for %s" % course_id
            sys.stdout.flush()
            try:
                setup_sql(param, args, steps, course_id)
            except Exception as err:
                print "===> Error completing setup_sql on %s, err=%s" % (course_id, str(err))
                traceback.print_exc()
                sys.stdout.flush()
        return

    if sqlall or 'make_uic' in steps:
        import make_user_info_combo
        make_user_info_combo.process_file(course_id, 
                                          basedir=param.the_basedir,
                                          datedir=param.the_datedir,
                                          use_dataset_latest=param.use_dataset_latest,
                                          )
    if sqlall or 'sql2bq' in steps:
        import load_course_sql
        try:
            load_course_sql.load_sql_for_course(course_id, 
                                                gsbucket=edx2bigquery_config.GS_BUCKET,
                                                basedir=param.the_basedir,
                                                datedir=param.the_datedir,
                                                do_gs_copy=True,
                                                use_dataset_latest=param.use_dataset_latest,
                                                )
        except Exception as err:
            print err
        
    if sqlall or 'load_forum' in steps:
        import rephrase_forum_data
        try:
            rephrase_forum_data.rephrase_forum_json_for_course(course_id,
                                                               gsbucket=edx2bigquery_config.GS_BUCKET,
                                                               basedir=param.the_basedir,
                                                               datedir=param.the_datedir,
                                                               use_dataset_latest=param.use_dataset_latest,
                                                               )
        except Exception as err:
            print err


def setup_sql_single(param, course_id, optargs=None):
    '''
    run setup_sql on a single course

    param = (dict) run parameters
    course_id = (string) course_id of course to run on
    optargs is ignored
    '''
    print "="*100
    print "Processing setup_sql for %s" % course_id
    sys.stdout.flush()
    try:
        setup_sql(param, {}, "setup_sql", course_id)
    except Exception as err:
        print "===> Error completing setup_sql on %s, err=%s" % (course_id, str(err))
        traceback.print_exc()
        sys.stdout.flush()
        raise

def analyze_course_single(param, course_id, optargs=None):
    '''
    run analyze_course_content on a single course

    param = (dict) run parameters
    course_id = (string) course_id of course to run on
    optargs is ignored
    '''
    print "="*100
    print "Processing analyze_course_content for %s" % course_id
    sys.stdout.flush()

    import analyze_content

    try:
        course_axis_pin_dates = getattr(edx2bigquery_config, "course_axis_pin_dates", {})
    except:
        pass
    if not course_axis_pin_dates:
        course_axis_pin_dates = {}

    pin_date = course_axis_pin_dates.get(course_id)
    if pin_date:
        print "--> [analyze_course] course_axis for %s being pinned to data from dump date %s" % (course_id, pin_date)

    try:
        analyze_content.analyze_course_content(course_id, 
                                               listings_file=param.listings,
                                               basedir=param.the_basedir, 
                                               datedir=param.the_datedir,
                                               use_dataset_latest=param.use_dataset_latest,
                                               do_upload=False,
                                               pin_date=pin_date,
                                               )
    except Exception as err:
        print "===> Error completing analyze_course_content on %s, err=%s" % (course_id, str(err))
        traceback.print_exc()
        sys.stdout.flush()
        raise

def time_on_task(param, course_id, optargs=None, skip_totals=False, just_do_totals=False, suppress_errors=False):
    '''
    update time_task table based on tracking logs

    param = (dict) run parameters
    course_id = (string) course_id of course to run on
    optargs is ignored
    skip_totals typically set to True for nightly runs
    '''
    print "="*100
    print "Updating time_task table for %s" % course_id
    sys.stdout.flush()

    import make_time_on_task

    try:
        make_time_on_task.process_course_time_on_task(course_id, 
                                                      force_recompute=param.force_recompute,
                                                      use_dataset_latest=param.use_dataset_latest,
                                                      end_date=param.end_date,
                                                      start_date=param.start_date,
                                                      just_do_totals=(param.just_do_totals or just_do_totals),
                                                      limit_query_size=param.limit_query_size,
                                                      table_max_size_mb=(param.table_max_size_mb or 800),
                                                      skip_totals=skip_totals,
                                                  )
    except Exception as err:
        print "===> Error completing process_course_time_on_task on %s, err=%s" % (course_id, str(err))
        traceback.print_exc()
        sys.stdout.flush()
        if not suppress_errors:
            raise

def daily_logs(param, args, steps, course_id=None, verbose=True, wait=False):
    if steps=='daily_logs':
        # doing daily_logs, so run split once first, then afterwards logs2gs and logs2bq
        daily_logs(param, args, 'split', args.tlfn)
        for course_id in get_course_ids(args):
            daily_logs(param, args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose)
        return

    if course_id is None:
        do_check = not (steps=='split')
        for course_id in get_course_ids(args, do_check=do_check):
            print "---> Processing %s on course_id=%s" % (steps, course_id)
            daily_logs(param, args, steps, course_id)
        return

    if 'split' in steps:
        import split_and_rephrase
        import pytz
        tlfn = course_id		# tracking log filename
        if '*' in tlfn:
            import glob
            TODO = glob.glob(tlfn)
            TODO.sort()
        else:
            TODO = [tlfn]
        for the_tlfn in TODO:
            print "--> Splitting tracking logs in %s" % the_tlfn
            timezone_string = None
            timezone = None
            try:
                timezone_string = edx2bigquery_config.TIMEZONE
            except Exception as err:
                if not str(err)=="'module' object has no attribute 'TIMEZONE'":
                    print "    no timezone specified, timezone_string=%s, err=%s" % (timezone_string, err)
            if timezone_string:
                try:
                    timezone = pytz.timezone(timezone_string)
                except Exception as err:
                    print "  Error!  Cannot parse timezone '%s' err=%s" % (timezone_string, err)
                    
            split_and_rephrase.do_file(the_tlfn, 
                                       logs_dir=args.logs_dir or edx2bigquery_config.TRACKING_LOGS_DIRECTORY,
                                       dynamic_dates=args.dynamic_dates,
                                       timezone=timezone,
                                       logfn_keepdir=args.logfn_keepdir,
            )

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

def analyze_problems(param, courses, args, do_show_answer=True, do_problem_analysis=True):
    import make_problem_analysis
    for course_id in get_course_ids(courses):
        try:
            make_problem_analysis.analyze_problems(course_id, 
                                                   basedir=param.the_basedir, 
                                                   datedir=param.the_datedir,
                                                   force_recompute=args.force_recompute,
                                                   use_dataset_latest=param.use_dataset_latest,
                                                   do_show_answer=do_show_answer, 
                                                   do_problem_analysis=do_problem_analysis,
                                                   only_step=param.only_step,
                                                   )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise


def analyze_videos(param, courses, args):
    import make_video_analysis
    for course_id in get_course_ids(courses):
        try:
            make_video_analysis.analyze_videos(course_id,
                                               api_key=getattr(edx2bigquery_config, "API_KEY", None),
                                               basedir=param.the_basedir, 
                                               datedir=param.the_datedir,
                                               force_recompute=args.force_recompute,
                                               use_dataset_latest=param.use_dataset_latest,
                                               )

        except (AssertionError, Exception) as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def analyze_forum(param, courses, args):
    import make_forum_analysis
    for course_id in get_course_ids(courses):
        try:
            make_forum_analysis.AnalyzeForums(course_id,
                                              force_recompute=args.force_recompute,
                                              use_dataset_latest=param.use_dataset_latest,
                                              skip_last_day=args.skip_last_day,
                                              end_date=args.end_date,
                                          )

        except (AssertionError, Exception) as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def problem_events(param, courses, args):
    import make_problem_events
    for course_id in get_course_ids(courses):
        try:
            make_problem_events.ExtractProblemEvents(course_id,
                                                     force_recompute=args.force_recompute,
                                                     use_dataset_latest=param.use_dataset_latest,
                                                     skip_last_day=args.skip_last_day,
                                                     end_date=args.end_date,
                                                 )

        except (AssertionError, Exception) as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def course_key_version(param, courses, args):
    import check_course_key_version
    for course_id in get_course_ids(courses):
        try:
            check_course_key_version.course_key_version(course_id,
                                                        logs_dir=args.logs_dir or edx2bigquery_config.TRACKING_LOGS_DIRECTORY,
                                                    )
        except (AssertionError, Exception) as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def attempts_correct(param, courses, args):
    import make_problem_analysis
    for course_id in get_course_ids(courses):
        try:
            make_problem_analysis.attempts_correct(course_id, 
                                                   force_recompute=args.force_recompute,
                                                   use_dataset_latest=param.use_dataset_latest,
            )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def ip_sybils(param, courses, args):
    import make_problem_analysis
    for course_id in get_course_ids(courses):
        try:
            if not param.only_step or ("1" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_ip_pair_sybils(course_id, 
                                                             force_recompute=args.force_recompute,
                                                             use_dataset_latest=param.use_dataset_latest,
                                                         )
            if param.only_step and ("2" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_ip_pair_sybils2(course_id, 
                                                              force_recompute=args.force_recompute,
                                                              use_dataset_latest=param.use_dataset_latest,
                                                              uname_ip_groups_table=param.table,
                                                         )
            if param.only_step and ("3" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_ip_pair_sybils3(course_id, 
                                                              force_recompute=args.force_recompute,
                                                              use_dataset_latest=param.use_dataset_latest,
                                                              uname_ip_groups_table=param.table,
                                                         )
            if param.only_step and ("4" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_show_ans_before_high_score(course_id, 
                                                                         force_recompute=args.force_recompute,
                                                                         use_dataset_latest=param.use_dataset_latest,
                                                                     )
            if param.only_step and ("5" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_ip_pair_sybils3_unfiltered(course_id, 
                                                                         force_recompute=args.force_recompute,
                                                                         use_dataset_latest=param.use_dataset_latest,
                                                                         uname_ip_groups_table=param.table,
                                                                         course_info_table=param.listings,
                                                                     )
            if param.only_step and ("6" in (param.only_step or "").split(',')):
                make_problem_analysis.compute_cameo_demographics(course_id, 
                                                                 force_recompute=args.force_recompute,
                                                                 use_dataset_latest=param.use_dataset_latest,
                                                             )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def temporal_fingerprints(param, courses, args):
    import make_problem_analysis
    for course_id in get_course_ids(courses):
        try:
            make_problem_analysis.compute_temporal_fingerprints(course_id, 
                                                                force_recompute=args.force_recompute,
                                                                use_dataset_latest=param.use_dataset_latest,
                                                            )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
            raise

def show_answer_table(param, course_id, args=None):
    import make_problem_analysis
    try:
        make_problem_analysis.make_show_answer_table(course_id, 
                                                     force_recompute=param.force_recompute,
                                                     use_dataset_latest=param.use_dataset_latest,
                                                     )
    except Exception as err:
        print err
        traceback.print_exc()
        sys.stdout.flush()
        raise

def enrollment_events_table(param, course_id, args=None):
    import make_enrollment_day
    try:
        make_enrollment_day.make_enrollment_events(course_id, 
                                                   force_recompute=param.force_recompute,
                                                   use_dataset_latest=param.use_dataset_latest,
                                                   )
    except Exception as err:
        print err
        traceback.print_exc()
        sys.stdout.flush()
        raise

def analyze_ora(param, courses, args):
    import make_openassessment_analysis
    for course_id in get_course_ids(courses):
        try:
            make_openassessment_analysis.get_ora_events(course_id, 
                                                        force_recompute=args.force_recompute,
                                                        use_dataset_latest=param.use_dataset_latest,
                                                        end_date=args.end_date,
                                                    )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()

def item_tables(param, courses, args):
    import make_item_tables
    for course_id in get_course_ids(courses):
        try:
            make_item_tables.make_item_tables(course_id, 
                                              force_recompute=args.force_recompute,
                                              use_dataset_latest=param.use_dataset_latest,
                                          )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()

def problem_check(param, courses, args):
    import make_problem_analysis
    for course_id in get_course_ids(courses):
        try:
            make_problem_analysis.problem_check_tables(course_id, 
                                                       force_recompute=args.force_recompute,
                                                       use_dataset_latest=param.use_dataset_latest,
                                                       end_date=args.end_date,
                                                       )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
    
def axis2bq(param, courses, args):
    import make_course_axis

    try:
        course_axis_pin_dates = getattr(edx2bigquery_config, "course_axis_pin_dates", None)
    except:
        pass
    if not course_axis_pin_dates:
        course_axis_pin_dates = {}

    for course_id in get_course_ids(courses):
        if args.skip_if_exists and make_course_axis.axis2bigquery.already_exists(course_id, use_dataset_latest=param.use_dataset_latest):
            print "--> course_axis for %s already exists, skipping" % course_id
            sys.stdout.flush()
            continue
        pin_date = course_axis_pin_dates.get(course_id)
        if pin_date:
            print "--> course_axis for %s being pinned to data from dump date %s" % (course_id, pin_date)
        make_course_axis.process_course(course_id, param.the_basedir, param.the_datedir, param.use_dataset_latest, args.verbose, pin_date)
        
    
def make_grading_policy(param, courses, args):
    import make_grading_policy_table

    try:
        course_axis_pin_dates = getattr(edx2bigquery_config, "course_axis_pin_dates", None)
    except:
        pass
    if not course_axis_pin_dates:
        course_axis_pin_dates = {}

    for course_id in get_course_ids(courses):
        if args.skip_if_exists and make_grading_policy_table.already_exists(course_id, use_dataset_latest=param.use_dataset_latest):
            print "--> grading_policy for %s already exists, skipping" % course_id
            sys.stdout.flush()
            continue
        pin_date = course_axis_pin_dates.get(course_id)
        if pin_date:
            print "--> course tarfile for %s being pinned to data from dump date %s" % (course_id, pin_date)
        make_grading_policy_table.make_gp_table(course_id, param.the_basedir, param.the_datedir, param.use_dataset_latest, args.verbose, pin_date)
        

def person_day(param, courses, args, check_dates=True):
    import make_person_course_day
    for course_id in get_course_ids(courses):
        try:
            make_person_course_day.process_course(course_id, 
                                                  force_recompute=args.force_recompute,
                                                  use_dataset_latest=param.use_dataset_latest,
                                                  check_dates=check_dates,
                                                  end_date=args.end_date,
                                                  skip_last_day=args.skip_last_day,
                                                  )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()
        

def pcday_ip(param, courses, args):
    import make_person_course_day
    for course_id in get_course_ids(courses):
        try:
            make_person_course_day.compute_person_course_day_ip_table(course_id, force_recompute=args.force_recompute,
                                                                      use_dataset_latest=param.use_dataset_latest,
                                                                      end_date=args.end_date,
                                                                      )
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()


def enrollment_day(param, courses, args):
    import make_enrollment_day
    for course_id in get_course_ids(courses):
        try:
            make_enrollment_day.process_course(course_id, 
                                               force_recompute=args.force_recompute,
                                               use_dataset_latest=param.use_dataset_latest)
        except Exception as err:
            print err
            traceback.print_exc()
            sys.stdout.flush()

def person_course(param, courses, args, just_do_nightly=False, force_recompute=False):
    import make_person_course
    print "[person_course]: for end date, using %s" % (args.end_date or param.DEFAULT_END_DATE)
    for course_id in get_course_ids(courses):
        try:
            make_person_course.make_person_course(course_id,
                                                  gsbucket=edx2bigquery_config.GS_BUCKET,
                                                  basedir=param.the_basedir,
                                                  datedir=param.the_datedir,
                                                  start=(args.start_date or "2012-09-05"),
                                                  end=(args.end_date or param.DEFAULT_END_DATE),
                                                  force_recompute=args.force_recompute or force_recompute,
                                                  nskip=(args.nskip or 0),
                                                  skip_geoip=args.skip_geoip,
                                                  skip_if_table_exists=args.skip_if_exists,
                                                  use_dataset_latest=param.use_dataset_latest,
                                                  just_do_nightly=just_do_nightly or args.just_do_nightly,
                                                  just_do_geoip=args.just_do_geoip,
                                                  )
        except Exception as err:
            print err
            if ('no user_info_combo' in str(err)) or ('aborting - no dataset' in str(err)):
                continue
            if ('Internal Error' in str(err)):
                raise
                # continue
            if ('BQ Error creating table' in str(err)):
                raise
                # continue
            raise
    
def doall(param, course_id, args, stdout=None):
    start = datetime.datetime.now()
    success = False
    if stdout:
        sys.stdout = stdout			# overload for multiprocessing, so that we can unravel output streams
    try:
        print "-"*100
        print "DOALL PROCESSING %s" % course_id
        print "-"*100
        setup_sql(param, course_id, 'setup_sql')
        try:
            analyze_problems(param, course_id, args)
        except Exception as err:
            print "--> Failed in analyze_problems with err=%s" % str(err)
            print "--> continuing with doall anyway"
        axis2bq(param, course_id, args)
        daily_logs(param, args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose, wait=True)
        pcday_ip(param, course_id, args)	# needed for modal IP
        person_day(param, course_id, args)
        enrollment_day(param, course_id, args)
        person_course(param, course_id, args)
        problem_check(param, course_id, args)
        show_answer_table(param, course_id, args)
        analyze_ora(param, course_id, args)
        time_on_task(param, course_id, args, just_do_totals=True, suppress_errors=True)
        try:
            analyze_videos(param, course_id, args)
        except Exception as err:
            print "--> Failed in analyze_videos with err=%s" % str(err)
            print "--> continuing with doall anyway"
        make_grading_policy(param, course_id, args)
        item_tables(param, course_id, args)
        analyze_forum(param, course_id, args)
        
        success = True

    except Exception as err:
        print "="*100
        print "ERROR: %s" % str(err)
        traceback.print_exc()

    end = datetime.datetime.now()
    ret = {'start': start, 'end': end, 'dt' : end-start, 'success': success, 'course_id': course_id, 'stdout': stdout}
    print "-"*100
    print "DOALL DONE WITH %s, success=%s, dt=%s" % (course_id, ret['success'], ret['dt'])
    print "-"*100
    sys.stdout.flush()
    return ret


def run_nightly_single(param, course_id, args=None):
    try:
        print "-"*100
        print "NIGHTLY PROCESSING %s" % course_id
        print "-"*100
        daily_logs(param, args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose, wait=True)
        person_day(param, course_id, args, check_dates=False)
        enrollment_day(param, course_id, args)
        pcday_ip(param, course_id, args)	# needed for modal IP
        person_course(param, course_id, args, just_do_nightly=True, force_recompute=True)
        problem_check(param, course_id, args)
        show_answer_table(param, course_id, args)
        analyze_ora(param, course_id, args)
        time_on_task(param, course_id, args, skip_totals=True)
        analyze_videos(param, course_id, args)

    except Exception as err:
        print "="*100
        print "ERROR: %s" % str(err)
        traceback.print_exc()
        sys.stdout.flush()
        raise


def list_tables_in_course_db(param, courses, args):
    import bqutil
    for course_id in get_course_ids(courses):
        dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=param.use_dataset_latest)
        table = bqutil.get_list_of_table_ids(dataset)
        print "Course %s, tables=%s" % (course_id, json.dumps(table, indent=4))

#-----------------------------------------------------------------------------
# utility commands

def get_data_tables(tables, args):
    '''
    used by get_course_data and get_data
    arguments provide options for combing outputs, and for loading output back into BigQuery
    '''
    import bqutil
    import gsutil
    import gzip
    import codecs

    optargs = {}
    if args.project_id:
        optargs['project_id'] = args.project_id
        print "Using %s as the project ID" % args.project_id

    if args.output_format_json:
        out_fmt = 'json'
    else:
        out_fmt = 'csv'

    if args.combine_into:
        cofn = args.combine_into
        do_gzip = cofn.endswith(".gz")
        if args.gzip and not do_gzip:
            cofn += ".gz"
            do_gzip = True
        print "Combining outputs into a single merged %s file %s" % (out_fmt, cofn)
        if do_gzip:
            print "  output will be gzip compressed"
            cofp = gzip.GzipFile(cofn, 'w')
        else:
            # cofp = codecs.open(cofn, 'w', encoding='utf8')
            cofp = open(cofn, 'w')
        the_header = None

    if args.combine_into_table:
        # produce output file, load into google storage, then load into BigQuery
        # use schema from one of the source tables
        if not args.combine_into:
            print "Please specify a file to store the combined data into, using --combine-into <filename>"
            return
        if not args.output_bucket:
            print "Please also specify a google storage bucket using --output-bucket <bucket_path>; the data are stored there before loading into BQ"
            return
        output_table = args.combine_into_table
        gspath = "%s/%s" % (args.output_bucket, cofn)

    for table in tables:
        has_project_id = False
        dataset, tablename = table.split('.', 1)
        if ':' in dataset:
            project_id, dataset = dataset.split(':')
            optargs['project_id'] = project_id
            has_project_id = True
        if args.combine_into:
            print "Retrieving %s.%s for %s" % (dataset, tablename, cofn)
        else:
            ofn = '%s__%s.%s' % (dataset, tablename, out_fmt)
            if has_project_id:
                ofn = project_id + '__' + ofn
            if args.gzip:
                ofn += ".gz"
            print "Retrieving %s as %s" % (table, ofn)
        sys.stdout.flush()

        if args.just_get_schema:
            tinfo = bqutil.get_bq_table_info(dataset, tablename, **optargs)
            ofn = '%s__%s__schema.json' % (dataset, tablename)
            print "Saving schema file as %s" % ofn
            open(ofn, 'w').write(json.dumps(tinfo['schema']['fields'], indent=4))
            continue

        if args.only_if_newer and os.path.exists(ofn):
            mod_dt = bqutil.get_bq_table_last_modified_datetime(dataset, tablename)
            of_dt = gsutil.get_local_file_mtime_in_utc(ofn,make_tz_unaware=True)
            if (mod_dt < of_dt):
                print "--> only_if_newer specified, and table %s mt=%s, file mt=%s, so skipping" % (tablename,
                                                                                                    mod_dt,
                                                                                                    of_dt)
                continue

        try:
            bqdat = bqutil.get_table_data(dataset, tablename, 
                                          convert_timestamps=True,
                                          return_csv=(out_fmt=='csv'), **optargs)
        except Exception as err:
            if args.skip_missing and 'HttpError 404' in str(err):
                print "--> missing table [%s.%s] Skipping..." % (dataset, tablename)
                sys.stdout.flush()
                continue
            raise
        if not bqdat:
            print "--> No data for [%s.%s]!" % (dataset, tablename)
            sys.stdout.flush()
            continue
        elif args.combine_into:
            if out_fmt=='csv':
                header, data_no_header = bqdat.split('\n',1)
                if not the_header:
                    the_header = header
                    cofp.write(bqdat)
                else:
                    if not header==the_header:
                        print "--> ERROR!  Cannot combine data from %s: CSV file header is different" % table
                        print "Other courses' table file header: %s" % the_header
                        print "This courses' table file header: %s" % header
                        raise Exception("[get_course_data] Mismatched table data format")
                    cofp.write(data_no_header)
            else:
                # output JSON - just concatenate
                cofp.write('\n'.join([ json.dumps(x) for x in bqdat['data'] ]))
            
        else:
            if args.gzip:
                ofp = gzip.GzipFile(ofn, 'w')
            else:
                # ofp = codecs.open(ofn, 'w', encoding='utf8')
                ofp = open(ofn, 'w')
            if out_fmt=='csv':
                ofp.write(bqdat)
            else:
                ofp.write('\n'.join([ json.dumps(x) for x in bqdat['data'] ]))
            ofp.close()
        
    if args.combine_into:
        cofp.close()
        print "Done with output file %s" % cofn

    if args.combine_into_table:
        print "Loading into google storage, to %s" % gspath
        sys.stdout.flush()
        import gsutil
        options = ''
        if not args.gzip:
            options = '-z csv,json'
        gsutil.upload_file_to_gs(cofn, gspath, options=options)
        print "Getting table schema..."
        sys.stdout.flush()
        for table in tables:
            dataset, tablename = table.split('.', 1)
            if ':' in dataset:
                project_id, dataset = dataset.split(':')
                optargs['project_id'] = project_id
            try:
                table_info = bqutil.get_bq_table_info(dataset, tablename, **optargs)
            except Exception as err:
                if args.skip_missing and 'HttpError 404' in str(err):
                    table_info = None
                else:
                    raise
            if args.skip_missing and table_info is None:
                print "--> missing table [%s.%s] Skipping..." % (dataset, tablename)
                sys.stdout.flush()
                continue
            if table_info is not None:
                break
        schema = table_info['schema']['fields']
        print "Loading into BigQuery %s" % output_table
        sys.stdout.flush()
        out_dataset, out_table = output_table.split('.', 1)
        optargs = {}
        if ':' in out_dataset:
            project_id, out_dataset = out_dataset.split(':', 1)
            optargs['project_id'] = project_id
        if out_fmt=='csv':
            optargs['format'] = 'csv'
            optargs['skiprows'] = 1
        bqutil.load_data_to_table(out_dataset, out_table, gspath, schema, **optargs)

#-----------------------------------------------------------------------------
# command line processing

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

                              Also accepts the "--clist=XXX" option, to specify which list of courses to act upon.

                              Before running this command, make sure your SQL files are converted and formatted according to the 
                              "Waldo" convention established by Harvard.  Use the "waldofy" command (see below) for this, 
                              if necessary.

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

--external command <cid>... : Run external command on data from one or more course_id's.  Also uses the --extparam settings.
                              External commands are defined in edx2bigquery_config.  Use --skiprun to create the external script
                              without running.  Use --submit-condor to submit command as a condor job.

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
                              are put in a subdirectory named "UNKNOWN".

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
                              This table is necessary for the analytics dashboard.
                              Accepts --only-step=grades | show_answer | analysis

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
                              Use this to address missing users issue in pre-early-2014 edX course dumps, which had the problem
                              that learners who un-enrolled were removed from the enrollment table.

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

course_key_version <cid>    : print out what version of course key (standard, v1) is being used for a given course_id.  Needed for opaque key 
                              handling.  The "standard" course_id format is "org/course/semester".  The opaque-key v1 format is
                              "course-v1:org+course+semester".  Opaque keys also mangle what is known as a the "module_id", and
                              use something like "block-v1:MITx+8.MechCx_2+2T2015+type@problem+block@Blocks_on_Ramp_randxyzBILNKOA0".
                              The opaque key ID's are changed back into the standard format, in most of the parsing of the SQL
                              and the tracking logs (see addmoduleid.py), but it's still necessary to know which kind of key
                              is being used, e.g. do that jump_to_id will work properly when linking back to a live course page.
                              course_id is always stored in tradition format.  course_key is kept in either standard or v1 opaque key
                              format.  module_id is always standard, and kept as "org/course/<module_type>/<module_id>".  In
                              the future, there may be a module_key, but that idea isn't currently used.  This particular command
                              scans some tracking log entries, and if the count of "block-v1" is high, it assigns "v1"
                              to the course; otherwise, it assigns "standard" as the course key version.

--- COURSE CONTENT DATA RELATED COMMANDS

axis2bq <course_id> ...     : construct "course_axis" table, upload to gs, and generate table in BigQuery dataset for the
                              specified course_id's.  
                              Accepts the "--clist" flag, to process specified list of courses in the config file's "courses" dict.

grading_policy <course_id>  : construct the "grading_policy" table, upload to gs, and generate table in BigQuery dataset, for
                              the specified course_id's.  Uses pin dates, just as does axis2bq.  Requires course.tar.gz file,
                              from the weekly SQL dumps.

recommend_pin_dates         : produce a list of recommended "pin dates" for the course axis, based on the specified --listings.  Example:
                              -->  edx2bigquery --clist=all --listings="course_listings.json" --course-base-dir=DATA-SQL recommend_pin_dates
                              These "pin dates" can be defined in edx2bigquery_config in the course_axis_pin_dates dict, to
                              specify the specific SQL dump dates to be used for course axis processing (by axis2bq) and for course
                              content analysis (analyze_course).  This is often needed when course authors change content after
                              a course ends, e.g. to remove or hide exams, and to change grading and due dates.

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

combinepc <course_id> ...   : Combine individual person_course tables from the specified course_id's, uploads CSV to
                              google storage.
                              Also imports the data into BigQuery.
                              Accepts the "--year2" flag, to process all courses in the config file's course_id_list.

problem_check <c_id> ...    : Create or update problem_check table, which has all the problem_check events from all the course's
                              tracking logs.

attempts_correct <c_id> ... : Create or update stats_attempts_correct table, which records the percentages of attempts which were correct
                              for a given user in a specified course.

ip_sybils <course_id> ...   : Create or update stats_ip_pair_sybils table, which records harvester-master pairs of users for which
                              the IP address is the same, and the pair have meaningful disparities in perfomance.  Requires that 
                              attempts_correct be run first.

temporal_fingerprints <cid>  : Create or update the problem_check_temporal_fingerprint, show_answer_temporal_fingerprint, and
                              stats_temporal_fingerprint_correlations tables.  Part of problem analyses.  May be expensive.

show_answer <course_id> ... : Create or update show_answer table, which has all the show_answer events from all the course's
                              tracking logs.

enrollment_events <cid> ... : Create or update enrollment_events table, which has all the ed.course.enrollment events from all the course's
                              tracking logs.

--- TESTING & DEBUGGING COMMANDS

rephrase_logs               : process input tracking log lines one at a time from standard input, and rephrase them to fit the
                              schema used for tracking log file data in BigQuery.  Used for testing.

testbq                      : test authentication to BigQuery, by listing accessible datasets.  This is a good command to start with,
                              to make sure your authentication is configured properly.

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
"""

    parser.add_argument("command", help=cmd_help)
    # parser.add_argument("-C", "--course_id", type=str, help="course ID in org/number/semester format, e.g. MITx/6.SFMx/1T2014")
    parser.add_argument("--course-base-dir", type=str, help="base directory where course SQL is stored, e.g. 'HarvardX-Year-2-data-sql'")
    parser.add_argument("--course-date-dir", type=str, help="date sub directory where course SQL is stored, e.g. '2014-09-21'")
    parser.add_argument("--start-date", type=str, help="start date for person-course dataset generated, e.g. '2012-09-01'")
    parser.add_argument("--end-date", type=str, help="end date for person-course dataset generated, e.g. '2014-09-21'")
    parser.add_argument("--tlfn", type=str, help="path to daily tracking log file to import, e.g. 'DAILY/mitx-edx-events-2014-10-14.log.gz'")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("--parallel", help="run separate course_id's in parallel", action="store_true")
    parser.add_argument("--year2", help="increase output verbosity", action="store_true")
    parser.add_argument("--clist", type=str, help="specify name of list of courses to iterate command over")
    parser.add_argument("--force-recompute", help="force recomputation", action="store_true")
    parser.add_argument("--dataset-latest", help="use the *_latest SQL dataset", action="store_true")
    parser.add_argument("--skiprun", help="for external command, print, and skip running", action="store_true")
    parser.add_argument("--external", help="run specified command as being an external command", action="store_true")
    parser.add_argument("--extparam", type=str, help="configure parameter for external command, e.g. --extparam irt_type=2pl")
    parser.add_argument("--submit-condor",  help="submit external command as a condor job (must be used with --external)", action="store_true")
    parser.add_argument("--max-parallel", type=int, help="maximum number of parallel processes to run (overrides config) if --parallel is used")
    parser.add_argument("--skip-geoip", help="skip geoip (and modal IP) processing in person_course", action="store_true")
    parser.add_argument("--skip-if-exists", help="skip processing in person_course if table already exists", action="store_true")
    parser.add_argument("--just-do-nightly", help="for person_course, just update activity stats for new logs", action="store_true")
    parser.add_argument("--just-do-geoip", help="for person_course, just update geoip using local db", action="store_true")
    parser.add_argument("--just-do-totals", help="for time_task, just compute total sums", action="store_true")
    parser.add_argument("--just-get-schema", help="for get_course_data and get_data, just return the table schema as a json file", action="store_true")
    parser.add_argument("--only-if-newer", help="for get_course_data and get_data, only get if bq table newer than local file", action="store_true")
    parser.add_argument("--limit-query-size", help="for time_task, limit query size to one day at a time and use hashing for large tables", action="store_true")
    parser.add_argument("--table-max-size-mb", type=int, help="maximum log table size for query size limit processing, in MB (defaults to 800)")
    parser.add_argument("--nskip", type=int, help="number of steps to skip")
    parser.add_argument("--only-step", type=str, help="specify single step to take in processing, e.g. for report")
    parser.add_argument("--logs-dir", type=str, help="directory to output split tracking logs into")
    parser.add_argument("--listings", type=str, help="path to the course listings.csv file (or, for some commands, table name for course info listings)")
    parser.add_argument("--dbname", type=str, help="mongodb db name to use for mongo2gs")
    parser.add_argument("--project-id", type=str, help="project-id to use (overriding the default; used by get_course_data)")
    parser.add_argument("--table", type=str, help="bigquery table to use, specified as dataset_id.table_id or just as table_id (for get_course_data)")
    parser.add_argument("--org", type=str, help="organization ID to use")
    parser.add_argument("--combine-into", type=str, help="combine outputs into the specified file as output (used by get_course_data, get_data)")
    parser.add_argument("--combine-into-table", type=str, help="combine outputs into specified table (may be project:dataset.table) for get_data, get_course_data")
    parser.add_argument("--skip-missing", help="for get_data, get_course_data, skip course if missing table", action="store_true")
    parser.add_argument("--output-format-json", help="output data in JSON format instead of CSV (the default); used by get_course_data, get_data", action="store_true")
    parser.add_argument("--collection", type=str, help="mongodb collection name to use for mongo2gs")
    parser.add_argument("--output-project-id", type=str, help="project-id where the report output should go (used by the report and combinepc commands)")
    parser.add_argument("--output-dataset-id", type=str, help="dataset-id where the report output should go (used by the report and combinepc commands)")
    parser.add_argument("--output-bucket", type=str, help="gs bucket where the report output should go, e.g. gs://x-data (used by the report and combinepc commands)")
    parser.add_argument("--dynamic-dates", help="split tracking logs using dates determined by each log line entry, and not filename", action="store_true")
    parser.add_argument("--logfn-keepdir", help="keep directory name in tracking which tracking logs have been loaded already", action="store_true")
    parser.add_argument("--skip-last-day", help="skip last day of tracking log data in processing pcday, to avoid partial-day data contamination", action="store_true")
    parser.add_argument("--gzip", help="compress the output file (e.g. for get_course_data)", action="store_true")
    parser.add_argument('courses', nargs = '*', help = 'courses or course directories, depending on the command')
    
    args = parser.parse_args()
    if args.verbose:
        sys.stderr.write("command = %s\n" % args.command)
        sys.stderr.flush()

    param = Parameters()

    param.the_basedir = args.course_base_dir or getattr(edx2bigquery_config, "COURSE_SQL_BASE_DIR", None)
    param.the_datedir = args.course_date_dir or getattr(edx2bigquery_config, "COURSE_SQL_DATE_DIR", None)
    param.use_dataset_latest = args.dataset_latest
    param.listings = args.listings
    param.force_recompute = args.force_recompute
    param.end_date = args.end_date
    param.start_date = args.start_date
    param.just_do_totals = args.just_do_totals
    param.limit_query_size = args.limit_query_size
    param.table_max_size_mb = args.table_max_size_mb
    param.only_step = args.only_step
    param.table = args.table
    param.extparam = args.extparam
    param.skiprun = args.skiprun
    param.verbose = args.verbose
    param.project_id = args.output_project_id or getattr(edx2bigquery_config, "PROJECT_ID", None)
    param.max_parallel = args.max_parallel
    param.submit_condor = args.submit_condor

    # default end date for person_course
    try:
        param.DEFAULT_END_DATE = getattr(edx2bigquery_config, "DEFAULT_END_DATE", "2014-09-21")
    except:
        param.DEFAULT_END_DATE = "2014-09-21"

    try:
        param.DEFAULT_MONGO_DB = getattr(edx2bigquery_config, "DEFAULT_MONGO_DB", None)
    except:
        param.DEFAULT_MONGO_DB = None

    #-----------------------------------------------------------------------------            
    # external command?

    if args.external:
        extcmd = args.command
        try:
            eec = edx2bigquery_config.extra_external_commands	# dict of external commands, with settings
        except:
            eec = {}
        from config_external import external_commands as ecinfo
        ecinfo.update(eec)
        if not extcmd in ecinfo:
            print "Unknown external command %s (must be defined in edx2bigquery_config)" % (extcmd)
            sys.exit(0)

        courses = get_course_ids(args)
        param.extcmd = extcmd
        param.ecinfo = ecinfo
        param.parallel = args.parallel
        run_parallel_or_serial(run_external_single, param, courses, args, parallel=args.parallel)
        sys.exit(0)

    #-----------------------------------------------------------------------------            

    if (args.command=='mongo2gs'):
        from extract_logs_mongo2gs import  extract_logs_mongo2gs
        for course_id in get_course_ids(args):
            extract_logs_mongo2gs(course_id, verbose=args.verbose,
                                  start=(args.start_date or "2012-09-05"),
                                  end=(args.end_date or param.DEFAULT_END_DATE),
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
        if args.parallel:			# run multiple instances in parallel
            courses = get_course_ids(args)
            pool = mp.Pool(processes=params.max_parallel or MAXIMUM_PARALLEL_PROCESSES)
            stdoutset = {}
            results = []
            for course_id in courses:
                sq = SubProcessStdout(course_id)
                stdoutset[course_id] = sq
                results.append( pool.apply_async(doall, args=(param, course_id, args, sq)) )
            output = [p.get() for p in results]
            print "="*100
            print "="*100
            print "PARALLEL DOALL DONE"
            print "="*100
            print "="*100
            for ret in output:
                print '    [%s] success=%s, dt=%s' % (ret['course_id'], ret['success'], ret['dt'])
            print "="*100
            for ret in output:
                course_id = ret['course_id']
                print "="*100 + " [%s]" % course_id
                print ret['stdout'].output
            print "="*100
            for ret in output:	# repeat
                print '    [%s] success=%s, dt=%s' % (ret['course_id'], ret['success'], ret['dt'])
            print "="*100
        else:
            for course_id in get_course_ids(args):
                doall(param, course_id, args)

    elif (args.command=='nightly'):
        courses = get_course_ids(args)
        run_parallel_or_serial(run_nightly_single, param, courses, args, parallel=args.parallel)
        sys.exit(0)

        for course_id in get_course_ids(args):
            print "-"*100
            print "NIGHTLY PROCESSING %s" % course_id
            print "-"*100
            try:
                daily_logs(param, args, ['logs2gs', 'logs2bq'], course_id, verbose=args.verbose, wait=True)
                person_day(param, course_id, args, check_dates=False)
                enrollment_day(param, course_id, args)
                pcday_ip(param, course_id, args)	# needed for modal IP
                person_course(param, course_id, args, just_do_nightly=True, force_recompute=True)
                problem_check(param, course_id, args)
                analyze_ora(param, course_id, args)
            except Exception as err:
                print "="*100
                print "ERROR: %s" % str(err)
                traceback.print_exc()

    elif (args.command=='make_uic'):
        setup_sql(param, args, args.command)
                                              
    elif (args.command=='sql2bq'):
        setup_sql(param, args, args.command)

    elif (args.command=='load_forum'):
        setup_sql(param, args, args.command)

    elif (args.command=='setup_sql'):
        # setup_sql(param, args, args.command)
        courses = get_course_ids(args)
        run_parallel_or_serial(setup_sql_single, param, courses, args, parallel=args.parallel)

    elif (args.command=='waldofy'):
        import do_waldofication_of_sql
        dirname = args.courses[0]		# directory of unpacked SQL data from edX
        args.courses = args.courses[1:]		# remove first element, which was dirname
        courses = get_course_ids(args)
        do_waldofication_of_sql.process_directory(dirname, courses, param.the_basedir)

    elif (args.command=='analyze_course'):
        import analyze_content
        if args.force_recompute:
            analyze_content.analyze_course_content(course_id=None,
                                                   listings_file=param.listings,
                                                   basedir=param.the_basedir, 
                                                   datedir=param.the_datedir,
                                                   use_dataset_latest=param.use_dataset_latest,
                                                   do_upload=args.force_recompute,
                                                   courses=get_course_ids(args),
                                                   )
        else:
            courses = get_course_ids(args)
            run_parallel_or_serial(analyze_course_single, param, courses, args, parallel=args.parallel)

    elif (args.command=='mongo2user_info'):
        import fix_missing_user_info
        for course_id in get_course_ids(args):
            fix_missing_user_info.mongo_dump_user_info_files(course_id, 
                                                             basedir=param.the_basedir, 
                                                             datedir=param.the_datedir,
                                                             dbname=args.dbname or param.DEFAULT_MONGO_DB,
                                                             use_dataset_latest=param.use_dataset_latest,
                                                             )

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

    elif (args.command=='get_course_tables'):
        courses = get_course_ids(args)
        run_parallel_or_serial(list_tables_in_course_db, param, courses, args, parallel=args.parallel)

    elif (args.command=='check_pc_dates'):
        import bqutil
        tablename = "person_course"
        for course_id in get_course_ids(args):
            dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=param.use_dataset_latest)
            try:
                table_date = bqutil.get_bq_table_last_modified_datetime(dataset, tablename)
            except Exception as err:
                if 'Not Found' in str(err):
                    table_date = None
                else:
                    table_date = str(err)
            print "%s: %s " %  (course_id, table_date)
            sys.stdout.flush()
            
    elif (args.command=='get_tables'):
        import bqutil
        print json.dumps(bqutil.get_tables(args.courses[0]), indent=4)

    elif (args.command=='get_table_data'):
        import bqutil
        dataset = args.courses[0].replace('/', '__').replace('.', '_')
        print json.dumps(bqutil.get_table_data(dataset, 
                                               args.courses[1],
                                               convert_timestamps=True,
                                           ), indent=4)

    elif (args.command=='check_for_duplicates'):
        courses = []
        ndup = 0
        ntot = 0
        for course_id in get_course_ids(args):
            if course_id in courses:
                print "Duplicate course_id %s" % course_id
                ndup += 1
            else:
                courses.append(course_id)
            ntot += 1
        print "Summary: %d course_id's, with %d duplicates" % (ntot, ndup)

    elif (args.command=='get_course_data'):
        import bqutil
        tablename = args.table
        tables = []
        for course_id in get_course_ids(args):
            dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=param.use_dataset_latest)
            tables.append('%s.%s' % (dataset, tablename))

        return get_data_tables(tables, args)

    elif (args.command=='get_data'):
        tables = args.courses			# may specify project as well, with syntax project_id:dataset_id.table_id
        return get_data_tables(tables, args)

    elif (args.command=='get_table_info'):
        import bqutil
        print bqutil.get_bq_table_info(args.courses[0], args.courses[1])

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
                dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=param.use_dataset_latest)
                bqutil.delete_bq_table(dataset, 'stats_activity_by_day')
            except Exception as err:
                print err
                raise

    elif (args.command=='daily_logs'):
        daily_logs(param, args, args.command)

    elif (args.command=='split'):
        daily_logs(param, args, args.command)
        print "==> split done at %s" % datetime.datetime.now()

    elif (args.command=='logs2gs'):
        daily_logs(param, args, args.command)

    elif (args.command=='logs2bq'):
        daily_logs(param, args, args.command)

    elif (args.command=='course_key_version'):
        course_key_version(param, args, args)

    elif (args.command=='tsv2csv'):
        import csv
        fp = csv.writer(sys.stdout)
        for line in sys.stdin:
            fp.writerow(line[:-1].split('\t'))

    elif (args.command=='analyze_problems'):
        courses = get_course_ids(args)
        run_parallel_or_serial(analyze_problems, param, courses, args, parallel=args.parallel)
        # analyze_problems(param, args, args)

    elif (args.command=='attempts_correct'):
        courses = get_course_ids(args)
        run_parallel_or_serial(attempts_correct, param, courses, args, parallel=args.parallel)
        # attempts_correct(param, args, args)

    elif (args.command=='ip_sybils'):
        courses = get_course_ids(args)
        print "==> courses = ", courses
        run_parallel_or_serial(ip_sybils, param, courses, args, parallel=args.parallel)

    elif (args.command=='temporal_fingerprints'):
        courses = get_course_ids(args)
        print "==> courses = ", courses
        run_parallel_or_serial(temporal_fingerprints, param, courses, args, parallel=args.parallel)

    elif (args.command=='time_task'):
        courses = get_course_ids(args)
        run_parallel_or_serial(time_on_task, param, courses, args, parallel=args.parallel)

    elif (args.command=='analyze_ora'):
        analyze_ora(param, args, args)

    elif (args.command=='analyze_videos'):
        courses = get_course_ids(args)
        run_parallel_or_serial(analyze_videos, param, courses, args, parallel=args.parallel)

    elif (args.command=='problem_events'):
        courses = get_course_ids(args)
        run_parallel_or_serial(problem_events, param, courses, args, parallel=args.parallel)

    elif (args.command=='analyze_forum'):
        courses = get_course_ids(args)
        run_parallel_or_serial(analyze_forum, param, courses, args, parallel=args.parallel)

    elif (args.command=='problem_check'):
        problem_check(param, args, args)

    elif (args.command=='show_answer'):
        courses = get_course_ids(args)
        run_parallel_or_serial(show_answer_table, param, courses, args, parallel=args.parallel)

    elif (args.command=='enrollment_events'):
        courses = get_course_ids(args)
        run_parallel_or_serial(enrollment_events_table, param, courses, args, parallel=args.parallel)

    elif (args.command=='axis2bq'):
        courses = get_course_ids(args)
        run_parallel_or_serial(axis2bq, param, courses, args, parallel=args.parallel)
        # axis2bq(param, args, args)

    elif (args.command=='grading_policy'):
        courses = get_course_ids(args)
        run_parallel_or_serial(make_grading_policy, param, courses, args, parallel=args.parallel)

    elif (args.command=='item_tables'):
        courses = get_course_ids(args)
        run_parallel_or_serial(item_tables, param, courses, args, parallel=args.parallel)

    elif (args.command=='recommend_pin_dates'):
        import make_axis_pin_dates_from_listings
        courses = get_course_ids(args)
        make_axis_pin_dates_from_listings.process_courses(courses, param.the_basedir, param.listings)

    elif (args.command=='staff2bq'):
        import load_staff
        load_staff.do_staff_csv(args.courses[0])

    elif (args.command=='make_cinfo'):
        import make_cinfo
        make_cinfo.do_course_listings(args.courses[0])

    elif (args.command=='pcday_ip'):
        pcday_ip(param, args, args)

    elif (args.command=='person_day'):
        person_day(param, args, args)

    elif (args.command=='enrollment_day'):
        # enrollment_day(param, args, args)
        courses = get_course_ids(args)
        run_parallel_or_serial(enrollment_day, param, courses, args, parallel=args.parallel)

    elif (args.command=='person_course'):
        # person_course(param, args, args)
        courses = get_course_ids(args)
        run_parallel_or_serial(person_course, param, courses, args, parallel=args.parallel)

    elif (args.command=='report'):
        import make_course_report_tables
        make_course_report_tables.CourseReport(get_course_ids(args), 
                                               nskip=(args.nskip or 0),
                                               output_project_id=args.output_project_id or edx2bigquery_config.PROJECT_ID,
                                               output_dataset_id=args.output_dataset_id,
                                               output_bucket=args.output_bucket or edx2bigquery_config.GS_BUCKET,
                                               use_dataset_latest=param.use_dataset_latest,
                                               only_step=param.only_step,
                                               end_date=(param.end_date or param.DEFAULT_END_DATE),
                                               )

    elif (args.command=='combinepc'):
        import make_combined_person_course
        make_combined_person_course.do_combine(get_course_ids(args),
                                               edx2bigquery_config.PROJECT_ID,
                                               nskip=(args.nskip or 0),
                                               output_project_id=args.output_project_id or edx2bigquery_config.PROJECT_ID,
                                               output_dataset_id=args.output_dataset_id,
                                               output_bucket=args.output_bucket or edx2bigquery_config.GS_BUCKET,
                                               use_dataset_latest=param.use_dataset_latest,
                                               )

    else:
        print "Unknown command %s!" % args.command
        sys.exit(-1)
