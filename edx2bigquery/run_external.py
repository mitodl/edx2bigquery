#
# interface for edx2bigquery to run an external program on data from BQ
#

import codecs
import sys
import os
import datetime
import bqutil
import json

from path import path

def run_external_script(extcmd, param, ecinfo, course_id):
    """
    Run external script on specified course.

    extcmd = string specifying external command to run
    param = command line parameters, including extparam
    ecinfo = external command info from edx2bigquery_config
    course_id = course_id to run external command on
    """
    settings = ecinfo.get(extcmd)
    
    print settings['name']
    
    if param.verbose:
        print settings.get('description', '')

    cidns = course_id.replace('/', '__')
    cidns_nodots = course_id.replace('/', '__').replace('.', '_').replace('-', '_')

    mypath = path(os.path.realpath(__file__)).dirname().dirname()
    edx2bigquery_context = {'lib': mypath / "lib",
                            'bin': mypath / "bin",
                        }

    the_template = settings['template'].format(**edx2bigquery_context)
    fnpre = settings['filename_prefix']
    lfn = "%s-%s.log" % (fnpre, cidns)
    if settings.get('logs_dir'):
        lfn = path(settings['logs_dir']) / lfn
    ofn = settings['script_fn'].format(filename_prefix=fnpre, cidns=cidns)
    cwd = os.getcwd()

    the_date = str(datetime.datetime.now())

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=param.use_dataset_latest)
    table_prefix = dataset

    if param.force_recompute:
        param.force_recompute = 1
    else:
        param.force_recompute = 0

    context = {'course_id': course_id,
               'script_name': ofn,
               'the_date': the_date,
               'cidns': cidns,
               'cidns_nodots': cidns,
               'template_file': the_template,
               'log_file': lfn,
               'filename_prefix': fnpre,
               'working_dir': cwd,
               'table_prefix': table_prefix,
               'lib_dir': edx2bigquery_context['lib'],
               'bin_dir': edx2bigquery_context['bin'],
    }
    context.update(settings)
    context.update(param.__dict__)

    rundir = settings['run_dir'].format(**context)
    runcmd = settings['script_cmd'].format(**context)

    tem = codecs.open(the_template).read()
    tem = unicode(tem)
    try:
        script_file = tem.format(**context)
    except Exception as err:
        print "Oops, cannot properly format template %s" % the_template
        print "Error %s" % str(err)
        print "context: ", json.dumps(context, indent=4)
        raise
    codecs.open(ofn, 'w', encoding="utf8").write(script_file)
    print "Generated %s" % ofn

    # if depends_on is defined, and force_recompute is not true, then skip
    # run if output already exists and is newer than all depends_on tables.

    depends_on = settings.get('depends_on')
    output_table = settings.get('output_table')
    if depends_on and not type(depends_on)==list:
        depends_on = [ depends_on ]
    do_compute = param.force_recompute
    if (not param.force_recompute) and depends_on and output_table:
        # does output already exist?
        has_output = False
        try:
            tinfo = bqutil.get_bq_table_info(dataset, output_table)
            if tinfo:
                has_output = True
        except:
            pass
        if not has_output:
            print "Output table %s.%s doesn't exist: running" % (dataset, output_table)
            do_compute = True
        else:
            table_date = tinfo['lastModifiedTime']
            for deptab in depends_on:
                dtab_date = bqutil.get_bq_table_last_modified_datetime(dataset, deptab)
                if table_date and dtab_date > table_date:
                    do_compute = True
                    break
            if not do_compute:
                print "Output table %s.%s exists and is newer than %s, skipping" % (dataset, output_table, depends_on)
            
    if do_compute:
        os.chdir(rundir)
        print "Working directory: %s" % rundir
        print "Logging to %s" % lfn
        print runcmd
        if not param.skiprun:
            start = datetime.datetime.now()
            os.system(runcmd)

            if settings.get('type')=="stata":
                # cleanup leftover log file after stata batch run
                batch_log = ofn.split('.')[0] + ".log"
                if os.path.exists(batch_log):
                    os.unlink(batch_log)
                    print "Removed old log file %s" % batch_log

            end = datetime.datetime.now()
            has_output = False
            try:
                tinfo = bqutil.get_bq_table_info(dataset, output_table)
                if tinfo:
                    has_output = True
            except:
                pass
            success = has_output
            dt = end-start
            print "[RUN] DONE WITH %s, success=%s, dt=%s" % (extcmd, success, dt)
            sys.stdout.flush()
