#!/usr/bin/python
#
# add module_id to the log JSON string, one at a time
# also parses event string and sets event_js
# event_js = True if event is proper js; else it is False.

import os, sys
import re
import json
import traceback

#-----------------------------------------------------------------------------
# patterns to look for 

# if problem_id is given
# i4x://MITx/2.01x/problem/E6_1_1/
# "id": "i4x://MITx/DemoX/sequential/visualizations"
cidre3 = re.compile('i4x://([^/]+/[^/]+/[^/]+/[^/]+)')
cidre3a = re.compile('i4x://([^/]+/[^/]+/[^/]+/[^/]+)/goto_position')

# for video events, use event.id
# "i4x-HarvardX-PH207x-video-Solutions_to_Prevalence_Questions"
cidre4 = re.compile('i4x-([^\-]+)-([^\-]+)-video-([^ ]+)')

# for browser events, parse input_i4x
# "event_type" : "problem_check",
# "page" : "https://www.edx.org/courses/HarvardX/PH207x/2012_Fall/courseware/Week_2/Homework_2/",
# "event" : "input_i4x-HarvardX-PH207x-problem-wk2_hw2_HW02p04_2_1=0.0655&input_i4x-HarvardX-PH207x-problem-wk2_hw2_HW02p04_3_1=0.0313"
#
# input_i4x-MITx-DemoX-problem-Sample_Algebraic_Problem_2_1
#
cidre5 = re.compile('^input_i4x-([^\-]+)-([^\-]+)-problem-([^ =]+)_([0-9]+)_([0-9]+)(|_comment|_dynamath)=')

# event of going to sequence and seeing last page visited, eg:
# "event_type" : "/courses/HarvardX/CB22x/2013_Spring/courseware/69569a7536674a3c87d9675ddb48f100/b6d64278fc1e4da2a0059314c8064e28/",
cidre6 = re.compile('/courses/([^/]+/[^/]+)/[^/]+/courseware/[^/]+/([^/]+)/$')

# page_close event from browser with
# "page" : "https://www.edx.org/courses/HarvardX/CB22x/2013_Spring/courseware/69569a7536674a3c87d9675ddb48f100/d49b3020ecd8418db3854cd8fef8f33a/",
# "page": "https://www.edx.org/courses/MITx/DemoX/Demo_course/courseware/interactive_demonstrations/visualizations/"
cidre7 = re.compile('/courses/([^/]+/[^/]+)/[^/]+/courseware/[^/]+/([^/]+)/(|[#]*)$')

# opening a chapter
# "event_type" : "/courses/HarvardX/CB22x/2013_Spring/01356a17b5924b17a04b7fc2426a3798/",
cidre8 = re.compile('/courses/([^/]+/[^/]+)/[^/]+/courseware/([^/]+)/$')

# jump_to_id
# "event_type" : "/courses/MITx/16.101x/2013_SOND/jump_to_id/16101x_Assumptions"
cidre9 = re.compile('/courses/([^/]+/[^/]+)/[^/]+/jump_to_id/([^/]+)(/|)$')

#-----------------------------------------------------------------------------
# module_id for forums

# normal module_id's are of the form
#
#    {org}/{course_num}/{module_tag}/{url_name}
#
# forum events don't have an edX-defined module_id.  Instead, we
# create our own module_id for forum events, of the form
#
#    {org}/{course_num}/forum/{thread_id}
#
# but note that when a new thread is created, edX does not log the
# new thread ID (!!).  Thus, when that happens thread_id is set to
# "new".
#
# Note that forum posts are often large POST events, and thus
# lead to truncated tracking log entries, which give "event" entries
# that do not parse properly.  Thus, we don't rely on events having 
# event_js: true, for constructing module_id for forum events.
#
# example discussion forum events
#
# "event_source": "server",
# "event_type": "/courses/HarvardX/CB22x/2013_Spring/discussion/forum/i4x-HarvardX-CB22x-course-2013_Spring/threads/50ed8e41a2ecea250000000a"
#               /courses/HarvardX/PH207x/2012_Fall/discussion/forum/ph207_BioHW4_P1/threads/50982ebaafd02a1f0000007f
#
#
# often truncated:
#
# "event_source": "server",
# "event_type": "/courses/HarvardX/CB22x/2013_Spring/discussion/threads/511d113eee508c21000000e6/reply"
# "event": "{\"POST\": {\"body\": [\"I'm currently working on my PhD...
#
# example of creating new thread:
#
# "event_source": "server", 
# "event_type": "/courses/HarvardX/CB22x/2013_Spring/discussion/i4x-HarvardX-CB22x-course-2013_Spring/threads/create"
# "event": "{\"POST\": {\"body\": [\"Hello friends....\"], \"anonymous_to_peers\": [\"false\"], ...}}"

fidre5 = re.compile('^/courses/([^/]+/[^/]+)/([^/]+)/discussion/threads/([^/]+)')
fidre6 = re.compile('^/courses/([^/]+/[^/]+)/([^/]+)/discussion/forum/i4x([^/]+)/threads/([^/]+)')
fidre7 = re.compile('^/courses/([^/]+/[^/]+)/([^/]+)/discussion/i4x([^/]+)/threads/create')
fidre8 = re.compile('^/courses/([^/]+/[^/]+)/([^/]+)/discussion/forum/([^/]+)/threads/([^/]+)')

def add_module_id(data):
    #if not data['event_js']:
    #    return
    try:
        mid = guess_module_id(data)
    except Exception as err:
        sys.stderr.write("Failed to get module_id for data=" + str(data))
        sys.stderr.write('\nError=%s\n' % str(err))
        sys.stderr.write(traceback.format_exc())
        return

    if mid is not None and mid:
        data['module_id'] = mid

def guess_module_id(doc):

    event = doc['event']
    event_type = doc['event_type']

    if event_type in ['add_resource', 'delete_resource', 'recommender_upvote']:
        return None

    if type(event)==dict and ('id' in event) and not (type(event['id']) in [str, unicode]):
        return None

    if doc['event_source']=='browser':
        try:
            m = cidre3.search(event['id'])
            if m:
                # print "="*20 + "checking event id"
                # special handling for seq_goto or seq_next, so we append seq_num to sequential's module_id
                if ((event_type=='seq_goto') or (event_type=="seq_next")):
                    mid = m.group(1) + "/" + str(event['new'])
                    # sys.stderr.write("mid=" + mid + " for %s\n" % doc)
                    return mid
                return m.group(1)
        except Exception as err:
            pass

        if (event_type=='page_close'):
            rr = cidre7.search(doc['page'])
            # sys.stderr.write('checking page_close, rr=%s' % rr)
            if (rr):
                return rr.group(1) + "/sequential/" + rr.group(2) + '/'
            rr = cidre8.search(doc['page'])
            if (rr):
                return rr.group(1) + "/chapter/" + rr.group(2) + '/'
  
        try:
          rr = cidre3.search(event.problem)        # for problem_show ajax
          if (rr):
              return rr.group(1)
        except Exception as err:
            pass
  
        if (type(event)==str or type(event)==unicode):
            rr = cidre5.search(event)
            if (rr):
                return rr.group(1) + '/' + rr.group(2) + '/problem/' + rr.group(3)

        try:
            rr = cidre5.search(event[0])   # for problem_graded events from browser
            if (rr):
                return rr.group(1) + '/' + rr.group(2) + '/problem/' + rr.group(3)
        except Exception as err:
            pass
  
    # server events - ones which do not depend on event (just event_type)

    rr = fidre5.search(event_type)
    if (rr):
        return rr.group(1) + "/forum/" + rr.group(3)
  
    rr = fidre6.search(event_type)
    if (rr):
        return rr.group(1) + "/forum/" + rr.group(4)
  
    rr = fidre7.search(event_type)
    if (rr):
        return rr.group(1) + "/forum/new"
  
    rr = fidre8.search(event_type)
    if (rr):
        return rr.group(1) + "/forum/" + rr.group(4)
  
    # event of going to sequence and seeing last page visited, eg:
    rr = cidre6.search(event_type)
    if (rr):
        return rr.group(1) + "/sequential/" + rr.group(2) + '/'
  
    # event of opening a chapter
    rr = cidre8.search(event_type)
    if (rr):
        return rr.group(1) + "/chapter/" + rr.group(2)

    # event of jump_to_id
    rr = cidre9.search(event_type)
    if (rr):
        return rr.group(1) + "/jump_to_id/" + rr.group(2)

    if (type(event)==str or type(event)==unicode):	# all the rest of the patterns need event to be a dict
        return

    rr = cidre3.search(event_type)
    if (rr):
        if (cidre3a.search(event_type)):   # handle goto_position specially: append new seq position
            try:
                mid = rr.group(1) + '/' + event['POST']['position'][0]
                return mid
            except Exception as err:
                sys.stderr.write("Failed to handle goto_position for" + doc['_id'] + "\n")
        return rr.group(1)
  
    if (type(event)==dict and event.get('problem_id')): # assumes event is js, not string
        rr = cidre3.search(event['problem_id'])
        if (rr):
            return rr.group(1)
  
    if (type(event)==dict and event.get('id')): # assumes event is js, not string
        rr = cidre4.search(event['id'])
        if (rr):
            return rr.group(1) + "/" + rr.group(2) + "/video/" + rr.group(3)

    return None

#-----------------------------------------------------------------------------

def add_moduleid_line(line):
    try:
        data = json.loads(line)
    except Exception as err:
        sys.stderr.write('[addmoduleid] Oops, bad line %s' % line)
        return
    
    try:
        event = data['event']
        if not type(event)==dict:
            event = json.loads(event)
        event_js = True
    except Exception as err:
        event_js = False
        # raise
    
    data['event'] = event
    data['event_js'] = event_js
    
    # now figure out module_id
    
    add_module_id(data)
    return json.dumps(data) + '\n'

#-----------------------------------------------------------------------------

if __name__=="__main__":
    for line in sys.stdin:
        newline = add_moduleid_line(line)
        print newline,

    

