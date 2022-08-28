#!/usr/bin/python
#
# File:   rephrase_tracking_logs.py
# Date:   11-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# rephrase tracking log entries so they can be loaded into BigQuery
#
# The issue is that data have to be in a fixed schema.  However, the tracking logs
# have many free-format fields, like "event".
#
# We get around this by storing such fields as strings, but keep performance
# potential high by including parsed versions of "event" for specific kinds
# of event types, like problems.
#
# There are also a number of fields with names which have illegal characters
# as bigquery table field names.  Dashes and periods are not allowed, for
# example, so they need to be rewritten ('rephrased').
#
#
# - change POST and GET arguments to be strings
# - change problem check event from string to record with entry as key=data
# - change "_id" : { "$oid" : "5330f5e9e714bdeae575db5e" } to "mongoid": "5330f5e9e714bdeae575db5e"
#
# if module_id or course_id is missing, add them.
import datetime
import gzip
import json
import os
import sys
import string
import traceback
from math import isnan
from path import Path as path

from .addmoduleid import add_module_id
from .check_schema_tracking_log import check_schema


def do_rephrase(data, do_schema_check=True, linecnt=0):
    """
    Modify the provided data dictionary in place to rephrase
    certain pieces of data for easy loading to BigQuery

    :TODO: Move the inner functions outside this function.

    :type: dict
    :param data: A tracking log record from the edX nightly data files
    :type: bool
    :param do_schema_check: Whether or not the provided record should be checked
    against the target schema
    :type: int
    :param linecnt: Some line count value
    :rtype: None
    :return: Nothing is returned since the data parameter is modified in place
    """
    # add course_id?
    if 'course_id' not in data:
        cid = data.get('context', {}).get('course_id', '')
        if cid:
            data['course_id'] = cid
    # add module_id?
    if 'module_id' not in data:
        add_module_id(data)

    if not "event" in data:
        data['event'] = ""

    # ensure event is dict when possible
    if not 'event_js' in data:
        event = data.get('event')
        try:
            if not isinstance(event, dict):
                event = json.loads(event)
            event_js = True
        except Exception as err:
            # note - do not erase event even if it can't be loaded as JSON: see how it becomes JSONified below
            event_js = False
        data['event'] = event
        data['event_js'] = event_js

    #----------------------------------------
    # "event" is used for too many kinds of data, with colliding types for fields.
    # thus, in general, we can't store it as a fixed schema field.
    #
    # so we turn "event" into a JSON string.
    #
    # for specific kinds of events ("event_type"), we keep a parsed version of
    # "event", e.g. for problem_* event types.
    #
    # store that parsed version as "event_struct"
    event = None
    if 'event' in data:
        event = data['event']
        data['event'] = json.dumps(data['event'])

    # now the real rephrasing

    event_type = data.get('event_type', '')

    #----------------------------------------
    # types to keep

    KNOWN_TYPES = ['play_video', 'seq_goto', 'seq_next', 'seq_prev',
                   'seek_video', 'load_video',
                   'save_problem_success',
                   'save_problem_fail',
                   'reset_problem_success',
                   'reset_problem_fail',
                   'show_answer',
                   'edx.course.enrollment.activated',
                   'edx.course.enrollment.deactivated',
                   'edx.course.enrollment.mode_changed',
                   'edx.course.enrollment.upgrade.succeeded',
                   'speed_change_video',
                   'problem_check',
                   'problem_save',
                   'problem_reset'
                   ]
    if isinstance(event, dict):
        outs = ('video_embedded', 'harvardx.button', 'harvardx.')
        out_conds = not any(k in event_type for k in outs)
        in_conds = 'problem_' in event_type or event_type in KNOWN_TYPES
        if in_conds and out_conds:
            data['event_struct'] = event
        else:
            data['event_struct'] = {
                'GET': json.dumps(event.get('GET')),
                'POST': json.dumps(event.get('POST')),
                'query': event.get('query'),
            }
    else:
        if 'event_struct' in data:
            data.pop('event_struct')

    #----------------------------------------
    # special cases

    if '_id' in data:
        data['mongoid'] = data['_id']['$oid']
        data.pop('_id')

    if isinstance(event, dict) and 'POST' in event:
        post_str = json.dumps(event['POST'])
        event['POST'] = post_str

    if isinstance(event, dict) and 'GET' in event:
        get_str = json.dumps(event['GET'])
        event['GET'] = get_str

    if event_type in ['problem_check', 'problem_save', 'problem_reset'] and data['event_source']=='browser':
        if isinstance(event, str):
            event = {'data': json.dumps(event)}

    if isinstance(event, str):
        #if event and data['event_js']:
        #    sys.stderr.write('unexpected STRING event: ' + json.dumps(data, indent=4) + '\n')
        event = {'data': json.dumps(event)}

    if isinstance(event, list):
        event = {'data': json.dumps(event)}

    def make_str(key):
        if event is not None and 'state' in event:
            state = event['state']
            if key in state:
                state[key] = json.dumps(state[key])

    make_str('input_state')
    make_str('correct_map')
    make_str('student_answers')

    def make_str0(key):
        ev = event or {}
        if key in ev:
            ev[key] = json.dumps(ev[key])

    make_str0('correct_map')
    make_str0('answers')
    make_str0('submission')
    make_str0('old_state')
    make_str0('new_state')
    make_str0('permutation')
    make_str0('options_selected')
    make_str0('corrections')

    def make_str2(key):
        context = data.get('context', {})
        if key in context:
            context[key] = json.dumps(context[key])

    make_str2('course_user_tags')

    def move_unknown_fields_from_context_to_context_agent(keys):	# needed to handle new fields from mobile client
        context = data.get('context', {})
        agent = {'oldagent': context.get('agent', "")}
        for key in keys:
            if '.' in key:
                (prefix, subkey) = key.split('.', 1)
                if prefix in context:
                    subcontext = context[prefix]
                    if subkey in subcontext:
                        agent[key] = subcontext[subkey]
                        subcontext.pop(subkey)
            else:
                if key in context:
                    agent[key] = context[key]
                    context.pop(key)
        context['agent'] = json.dumps(agent)

    # 31-Jan-15: handle new "module.usage_key" field in context, e.g.:
    #
    #    "module": {
    #        "display_name": "Radiation Exposure",
    #        "usage_key": "i4x://MITx/6.00.1x_5/problem/ps03:ps03-Radiation-Exposure"
    #    },
    # 28-May-16: context.asides

    mobile_api_context_fields = ['application', 'client', 'received_at', 'component', "open_in_browser_url",
                                 "module.usage_key",
                                 "module.original_usage_version",
                                 "module.original_usage_key",
                                 "asides",
                             ]
    move_unknown_fields_from_context_to_context_agent(mobile_api_context_fields)

    #----------------------------------------
    # new fields which are not in schema get moved as JSON strings to the pre-existing "mongoid" field,
    # which is unused except in very old records
    # do this, for example, for the "referer" and "accept_language" fields

    def move_fields_to_mongoid(field_paths):
        '''
        field_path is a list of lists which gives the recursive dict keys to traverse to get to the field to move.
        Move that field, with the path intact, into the mongoid field.
        '''
        mongoid = data.get('mongoid')
        if not isinstance(mongoid, dict):
            mongoid = {'old_mongoid' : mongoid}

        def move_field_value(ddict, vdict, fp):
            '''recursively traverse dict to get and move value for specified field path'''
            key = fp[0]
            if len(fp)==1:		# base case
                if key in ddict:
                    fval = ddict.get(key)
                    vdict[key] = fval	# copy to new values dict
                    ddict.pop(key)		# remove key from current path within data dict
                    return fval
                return None
            if not key in vdict:
                vdict[key] = {}
            return move_field_value(ddict.get(key, {}), vdict[key], fp[1:])

        vdict = mongoid
        for field_path in field_paths:
            move_field_value(data, vdict, field_path)
        data['mongoid'] = json.dumps(vdict)

    # 16-Mar-15: remove event_struct.requested_skip_interval

    move_fields_to_mongoid([ ['referer'],
                             ['accept_language'],
                             ['event_struct', 'requested_skip_interval'],
                             ['event_struct', 'submitted_answer'],
                             ['event_struct', 'num_attempts'],
                             ['event_struct', 'task_id'],	# 05oct15
                             ['event_struct', 'content'],	# 11jan16
                             ['nonInteraction'], 	# 24aug15
                             ['label'],	 		# 24aug15
                             ['event_struct', 'widget_placement'],	# 08may16
                             ['event_struct', 'tab_count'],	# 08may16
                             ['event_struct', 'current_tab'],	# 08may16
                             ['event_struct', 'target_tab'],	# 08may16
                             ['event_struct', 'state', 'has_saved_answers'],	# 06dec2016
                             ['context', 'label'],	 		# 24aug15
                             ['roles'],				# 06sep2017 rp
                             ['environment'],			# 06sep2017 rp
                             ['minion_id'],			# 06sep2017 rp
                             ['event_struct', 'duration'],	# 22nov2017 ic
                             ['event_struct', 'play_medium']
                         ])

    #----------------------------------------
    # general checks

    def fix_dash(key):
        ev = event or {}
        if key in ev:
            newkey = key.replace('-', '_')
            ev[newkey] = ev[key]
            ev.pop(key)

    fix_dash('child-id')

    def check_empty(data, *keys):
        # print "--> keys=%s, data=%s" % (str(keys), data)
        key = keys[0]
        if isinstance(data, dict) and key in data:
            if len(keys) == 1:
                if data[key] in ["", '']:
                    # print "---> popped %s" % key
                    data.pop(key)
            else:
                check_empty(data[key], *keys[1:])

    check_empty(data, 'context', "user_id")

    data.pop('event_js')	# leftover from mongo import script

    #-----------------------------------------
    # check for null values in speed_change_video
    # Error encountered parsing LAC data from Oct. 2013
    # Requires that we also be able to convert the value to a float

    def string_is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    if data.get('event_type') == 'speed_change_video':
        if 'event_struct' in data and 'new_speed' in data['event_struct']:
            # First check if string is float
            if string_is_float(data['event_struct']['new_speed']):
                # Second check if value is null
                if isnan(float(data['event_struct']['new_speed'])):
                    data['event_struct'].pop('new_speed')

    # check for any funny keys, recursively
    funny_key_sections = []
    def check_for_funny_keys(entry, name='toplevel'):
        for key, val in entry.items():
            if key.startswith('i4x-') or key.startswith('xblock.'):
                sys.stderr.write(
                    "[rephrase] oops, funny key at %s in entry: %s, data=%s\n" % (name, entry, '')
                )
                funny_key_sections.append(name)
                return True
            if len(key) > 25:
                sys.stderr.write(
                    "[rephrase] suspicious key at %s in entry: %s, data=%s\n" % (name, entry, '')
                )

            if key[0].isdigit():
                sys.stderr.write(
                    "[rephrase] oops, funny key at %s in entry: %s, data=%s\n" % (name, entry, '')
                )
                funny_key_sections.append(name)
                return True

            if '-' in key or '.' in key:
                # bad key name!  rename it, chaning "-" to "_"
                newkey = key.replace('-','_').replace('.','__')
                sys.stderr.write(
                    "[rephrase] oops, bad keyname at %s in entry: %s newkey+%s\n" % (name, entry, newkey)
                )
                entry[newkey] = val
                entry.pop(key)
                key = newkey
            if isinstance(val, dict):
                ret = check_for_funny_keys(val, name + '/' + key)
                if ret is True:
                    sys.stderr.write(
                        "        coercing section %s to become a string\n" % (name+"/"+key)
                    )
                    entry[key] = json.dumps(val)
        return False

    check_for_funny_keys(data)

    try:
        check_schema(linecnt, data, coerce=True)
    except Exception as err:
        sys.stderr.write('[%d] oops, err=%s, failed in check_schema %s\n' % (linecnt, str(err), json.dumps(data, indent=4)))
        sys.stderr.write(traceback.format_exc())
        return


def do_rephrase_line(line, linecnt=0):
    try:
        data = json.loads(line)
    except Exception as err:
        sys.stderr.write('oops, bad log line %s\n' % line)
        return

    try:
        do_rephrase(data, do_schema_check=True, linecnt=linecnt)
    except Exception as err:
        sys.stderr.write('[%d] oops, err=%s, bad log line %s\n' % (linecnt, str(err), line))
        sys.stderr.write(traceback.format_exc())
        return
    return json.dumps(data)+'\n'


def do_rephrase_file(fn):
    '''
    rephrase lines in filename fn, and overwrite original file when done.
    '''

    from .load_course_sql import openfile	# only needed in this function

    fn = path(fn)

    print("Rephrasing tracking log file %s" % fn)
    sys.stdout.flush()

    ofn = fn.dirname() / ("tmp-" + fn.basename())
    ofp = openfile(ofn, 'w')

    for line in openfile(fn):
        newline = do_rephrase_line(line)
        ofp.write(newline)

    ofp.close()

    oldfilename = fn.dirname() / ("old-" + fn.basename())
    print("  --> Done; renaming %s -> %s" % (fn, oldfilename))
    os.rename(fn, oldfilename)
    print("  --> renaming %s -> %s" % (ofn, fn))
    os.rename(ofn, fn)
    sys.stdout.flush()

