#
# interface for edx2bigquery to run an external program on data from BQ
#

import codecs
import os
import datetime
import bqutil
import json

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

    the_template = settings['template']
    fnpre = settings['filename_prefix']
    lfn = "%s-%s.log" % (fnpre, cidns)
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

    os.chdir(rundir)
    print "Working directory: %s" % rundir
    print "Logging to %s" % lfn
    print runcmd
    if not param.skiprun:
        os.system(runcmd)

