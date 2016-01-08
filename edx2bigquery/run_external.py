#
# interface for edx2bigquery to run an external program on data from BQ
#

import codecs
import sys
import os
import datetime
import bqutil
import json
import re

from jinja2 import Template
from path import path

def run_external_script(extcmd, param, ecinfo, course_id):
    """
    Run external script on specified course.

    extcmd = string specifying external command to run
    param = command line parameters, including extparam
    ecinfo = external command info from edx2bigquery_config
    course_id = course_id to run external command on
    """
    # use default for base set of parameters
    ed_name = ecinfo.get('default_parameters', 'DEFAULT')
    settings = ecinfo.get(ed_name, {})
    settings.update(ecinfo.get(extcmd))
    # print "settings: ", json.dumps(settings, indent=4)
    
    print settings['name']
    
    if param.verbose:
        print settings.get('description', '')

    cidns = course_id.replace('/', '__')
    cidns_nodots = course_id.replace('/', '__').replace('.', '_').replace('-', '_')

    mypath = path(os.path.realpath(__file__)).dirname()
    edx2bigquery_context = {'lib': mypath / "lib",
                            'bin': mypath / "bin",
                        }

    the_template = settings['template'].format(**edx2bigquery_context)
    fnpre = settings['filename_prefix']
    lfn = "%s-%s.log" % (fnpre, cidns)
    if settings.get('logs_dir'):
        lfn = path(settings['logs_dir']) / lfn

    try:
        ofn = settings['script_fn'].format(filename_prefix=fnpre, cidns=cidns)
    except Exception as err:
        print "oops, errr %s" % str(err)
        print "settings=", json.dumps(settings, indent=4)
        raise
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
               'filename_prefix_cidns': "%s__%s" % (fnpre, cidns),
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
        # script_file = tem.format(**context)
        script_file = Template(tem).render(**context)
    except Exception as err:
        print "Oops, cannot properly format template %s" % the_template
        print "Error %s" % str(err)
        print "context: ", json.dumps(context, indent=4)
        raise
    fp = codecs.open(ofn, 'w', encoding="utf8")
    fp.write(script_file)
    fp.close()
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
                try:
                    dtab_date = bqutil.get_bq_table_last_modified_datetime(dataset, deptab)
                except Exception as err:
                    raise Exception("[run_external] missing dependent table %s.%s" % (dataset, deptab))
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
        sys.stdout.flush()
        if not param.skiprun:
            start = datetime.datetime.now()

            if param.submit_condor:
                condor_template_fn = settings.get('condor_job_template', '').format(**edx2bigquery_context)
                if not condor_template_fn:
                    raise Exception("[run_external] missing condor_job_template specification for %s" % (extcmd))
                condor_submit_fn = "CONDOR/{filename_prefix}-{cidns}.submit".format(**context)
                context.update({ 'MEMORY': 32768,
                                 'arguments': '{script_name}'.format(**context),
                                 'executable': context['script_cmd'],
                                 'input_file': '',
                                 'filename': condor_submit_fn,
                                 })
                condor_template = Template(open(condor_template_fn).read()).render(**context)
                dirs = ['CONDOR', 'JOBS']
                for dir in dirs:
                    if not os.path.exists(dir):
                        os.mkdir(dir)
                fp = open(condor_submit_fn, 'w')
                fp.write(condor_template)
                fp.close()
                cmd = "condor_submit %s" % condor_submit_fn
                print cmd
                jobid = None
                for k in os.popen(cmd):
                    m = re.search('submitted to cluster ([0-9]+)', k)
                    if m:
                        jobid = m.group(1)
                dt = str(datetime.datetime.now())
                jobfile = 'condor_jobs.csv'
                open(jobfile, 'a').write("%s,%s,%s,%s\n" % (course_id, dt, jobid, lfn))
                print "[%s] Submitted as condor job %s at %s" % (course_id, jobid, dt)
                # print "[run_external] submitted %s, job=%s" % (extcmd, jobnum)
                return
            else:
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
            print "[run_external] DONE WITH %s, success=%s, dt=%s" % (extcmd, success, dt)
            sys.stdout.flush()
            if param.parallel and not success:
                raise Exception("[run_external] External command %s failed on %s" % (extcmd, course_id))
