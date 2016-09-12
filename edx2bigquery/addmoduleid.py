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
# "event_type": "/courses/MITx/6.00.1x_5/1T2015/xqueue/3516/i4x://MITx/6.00.1x_5/problem/L4:L4_Problem_9/score_update"
cidre3 = re.compile('i4x://([^/]+/[^/]+/[^/]+/[^/]+)')
cidre3a = re.compile('i4x://([^/]+/[^/]+/[^/]+/[^/]+)/goto_position')
cidre3b = re.compile('i4x://(?P<org>[^/]+)/(?P<course>[^/]+)/(?P<mtype>[^/]+)/(?P<id>[^/]+)/(?P<action>[^/]+)')
cidre3c = re.compile('i4x://(?P<org>[^/]+)/(?P<course>[^/]+)/(?P<mtype>[^/]+)/(?P<id>[^/]+)')

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

#"event_type": "/courses/MITx/18.01.1x/2T2015/xblock/i4x:;_;_MITx;_18.01.1x;_problem;_ps0A-tab2-problem3/handler/xmodule_handler/input_ajax", 
# "path": "/courses/MITx/8.MReVx/2T2014/xblock/i4x:;_;_MITx;_8.MReVx;_split_test;_Quiz_Zero_randxyzLQ56GIPQ/handler/log_child_render"
cidre10 = re.compile('/courses/(?P<org>[^/]+)/(?P<course>[^/]+)/(?P<semester>[^+]+)/xblock/i4x:;_;_[^/]+;_[^/]+;_(?P<mtype>[^;]+);_(?P<id>[^/]+)/handler/.*')

# "page": "https://courses.edx.org/courses/MITx/6.00.1x_5/1T2015/courseware/d5d822451677476fbfb0a0f9a14e0501/bbc2f0aa5bc54bf8ba2f6c36391202fb/",
# "event": "input_i4x-MITx-6_00_1x_5-problem-5bada2f1e64249f996ee1a37df8db810_2_1=..."

cidre11 = re.compile('/courses/(?P<org>[^/]+)/(?P<course>[^/]+)/(?P<semester>[^+]+)/courseware/(?P<chapter>[^/]+)/(?P<sequential>[^/]+)/')
cidre11a = re.compile('input_i4x-(?P<org>[^-]+?)-(?P<course>[^-]+?)-(?P<mtype>[^-]+?)-(?P<id>.+?)_[0-9]+_[^=]+=.*')

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

#-----------------------------------------------------------------------------
# module_id for courses using opaque keys

# path or event_type: "/courses/course-v1:MITx+8.MechCx_2+2T2015/xblock/block-v1:MITx+8.MechCx_2+2T2015+type@problem+block@Blocks_on_Ramp_randxyzBILNKOA0/handler/xmodule_handler/problem_check"
# "path": "/courses/course-v1:MITx+6.00.1x_6+2T2015/xblock/block-v1:MITx+6.00.1x_6+2T2015+type@recommender+block@0d6bcca84ae54095b001d338a5b4705b/handler/handle_vote"

okre1 = re.compile('/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)/xblock/block-v1:[^+]+\+[^+]+\+[^+]+\+type@(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)/handler')

# "event_source": "browser"
# "page": "https://courses.edx.org/courses/course-v1:MITx+8.MechCx_2+2T2015/courseware/Unit_0/Quiz_Zero_randxyzLQ56GIPQ/",
# "event": "\"input_Blocks_on_Ramp_randxyzBILNKOA0_2_1=choice_3\""
# "event_type": "problem_check"

okre2 = re.compile('/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)/[bc]')
okre2a = re.compile('input_(?P<id>[^=]+)_[0-9]+_[^=]+=')

# "event_type": "/courses/course-v1:MITx+CTL.SC1x_1+2T2015/xblock/block-v1:MITx+CTL.SC1x_1+2T2015+type@sequential+block@5aff08b86e0e431e8ef29b0fbe52ecb

okre3 = re.compile('/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)/xblock/block-v1:[^+]+\+[^+]+\+[^+]+\+type@(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)')

# event "id": "block-v1:MITx+6.00.1x_6+2T2015+type@sequential+block@videosequence:Lecture_3"
okre4 = re.compile('block-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)\+type@(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)')

# page: "https://courses.edx.org/courses/course-v1:MITx+CTL.SC1x_1+2T2015/courseware/eb6a807af0324d9caaaab908133f3e7c/d05755d771d9486aa78ba888fdcc4125/"
# with event \"id\": \"i4x-MITx-3_086-2x-video-5ddc3c000e2e4e1b947615b1c92d2b8e\"
# for stop_video
okre5 = re.compile('/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)/courseware/(?P<chapter>[^/]+)/(?P<id>[^/]+)/')
okre5a = re.compile('i4x-(?P<org>[^-]+)-(?P<course>[^+]+)-(?P<mtype>[^-]+)-(?P<id>[^-]+)')

#-----------------------------------------------------------------------------

def add_module_id(data, verbose=False):
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

    else:
        if not verbose:
            return
        if 'instructor' in data['event_type']:
            return
        if not 'problem' in data['event_type']:
            return
        sys.stderr.write("Missing module_id: %s\n" % json.dumps(data, indent=4))
        pass

def guess_module_id(doc):

    event = doc['event']
    event_type = doc['event_type']
    path = doc.get('context', {}).get('path', '')
  
    # opaque keys

    rr = okre1.search(event_type)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        # sys.stderr.write("ok mid = %s\n" % mid)
        return mid
  
    rr = okre1.search(path)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        # sys.stderr.write("ok mid = %s\n" % mid)
        return mid

    if ('problem' in event_type and type(event) in [str, unicode] and event.startswith("input_")):
        page = doc.get('page', '') or ''
        # sys.stderr.write("page=%s\n" % page)
        rr = okre2.search(page)
        if (rr):
            rr2 = okre2a.search(event.split('&',1)[0])
            if rr2:
                mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), 'problem', rr2.group('id'))
                # sys.stderr.write("ok mid = %s\n" % mid)
                return mid
        rr2 = cidre11a.search(event)
        if (rr2):
            mid = "%s/%s/%s/%s" % (rr2.group('org'), rr2.group('course'), rr2.group('mtype'), rr2.group('id'))
            # sys.stderr.write("ok mid = %s\n" % mid)
            return mid
        sys.stderr.write("ok parse failed on %s" % json.dumps(doc, indent=4))
  
    if (event_type=="problem_graded" and type(event)==list and len(event)>0 and event[0].startswith("input_")):
        page = doc.get('page', '') or ''
        # sys.stderr.write("page=%s\n" % page)
        rr = okre2.search(page)
        if (rr):
            rr2 = okre2a.search(event[0])
            if rr2:
                mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), 'problem', rr2.group('id'))
                # sys.stderr.write("ok mid = %s\n" % mid)
                return mid
        rr2 = cidre11a.search(event[0])
        if (rr2):
            mid = "%s/%s/%s/%s" % (rr2.group('org'), rr2.group('course'), rr2.group('mtype'), rr2.group('id'))
            # sys.stderr.write("ok mid = %s\n" % mid)
            return mid
        sys.stderr.write("ok parse failed on %s" % json.dumps(doc, indent=4))

    rr = okre3.search(event_type)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        #sys.stderr.write("ok mid = %s\n" % mid)
        return mid
  
    rr = okre3.search(path)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        #sys.stderr.write("ok mid = %s\n" % mid)
        return mid

    if not type(event)==dict:
        event_dict = None
        try:
            event_dict = json.loads(event)
        except:
            pass
        if type(event_dict)==dict and 'id' in event_dict:
            event = event_dict

    if (type(event)==dict and 'id' in event and type(event['id']) in [str, unicode]):
        eid = event['id']
        rr = okre4.search(eid)
        if (rr):
            mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
            return mid
        rr = okre5a.search(eid)
        if (rr):
            mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
            #sys.stderr.write("ok mid = %s\n" % mid)
            return mid
        if event_type=='play_video' and '/' not in eid:
            rr = okre5.search(doc.get('page', '') or '')
            if (rr):
                mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), 'video', eid)
                return mid

    elif (type(event) in [str, unicode]):
        rr = okre4.search(event)
        if (rr):
            mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
            return mid

    # sys.stderr.write('path=%s\n' % json.dumps(path, indent=4))
    # sys.stderr.write('doc=%s\n' % json.dumps(doc, indent=4))
    # return

    # non-opaque keys

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

    # event of xblock with i4x
    rr = cidre10.match(event_type)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        # sys.stderr.write("ok mid = %s\n" % mid)
        return mid

    rr = cidre10.match(path)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        # sys.stderr.write("ok mid = %s\n" % mid)
        return mid

    if type(event) in [str, unicode] and event.startswith('input_'):
        #rr = cidre11.search(doc.get('page', ''))
        rr2 = cidre11a.search(event)
        if (rr2):
            mid = "%s/%s/%s/%s" % (rr2.group('org'), rr2.group('course'), rr2.group('mtype'), rr2.group('id'))
            # sys.stderr.write("ok mid = %s\n" % mid)
            return mid

    rr = cidre3.search(event_type)
    if (rr):
        if (cidre3a.search(event_type)):   # handle goto_position specially: append new seq position
            try:
                mid = rr.group(1) + '/' + event['POST']['position'][0]
                return mid
            except Exception as err:
                sys.stderr.write("Failed to handle goto_position for" + doc.get('_id', '<unknown>') + "\n")
                sys.stderr.write("%s\n" % json.dumps(doc, indent=4))
        return rr.group(1)
  
    rr = cidre3b.search(event_type)
    if (rr):
        mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
        return mid
  
    if type(event) in [str, unicode]:
        rr = cidre3c.search(event)
        if (rr):
            mid = "%s/%s/%s/%s" % (rr.group('org'), rr.group('course'), rr.group('mtype'), rr.group('id'))
            return mid

    if (type(event)==str or type(event)==unicode):	# all the rest of the patterns need event to be a dict
        return

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

    

