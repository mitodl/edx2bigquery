# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!/usr/bin/python
#
# File:   edx2course_axis.py
#
# From an edX xml file set, generate:
#
#   course_id, index, url_name,  category, gformat, start, due, name, path, module_id, data, chapter_mid, graded
#
# course_id = edX standard {org}/{course_num}/{semester}
# index     = integer giving temporal order of course material
# url_name  = unique key for item
# category  = (known as a "tag" in some edX docs) chapter, sequential, vertical, problem, video, html, ...
# gformat   = "grading format", ie assignment name
# start     = start date
# due       = end date
# name      = full (display) name of item
# path      = path with url_name's to this item from course root, ie chapter/sequential/position
# module_id = edX standard {org}/{course_num}/{category}/{url_name} id for an x-module
# data      = extra data for element, eg you-tube id's for videos
# chapter_mid = module_id of the chapter within which this x-module exists (empty if not within a chapter)
# graded    = string ("true" or "false") specifying if the section (ie sequential) was to be graded or not
# parent    = url_name of the parent of this item
# is_split  = boolean specifying if this item is in a split_test
# 
#
# usage:   python edx2course_axis.py COURSE_DIR
#
# or:      python edx2course_axis.py course_tar_file.xml.tar.gz

# requires BeautifulSoup and path.py to be installed

# <codecell>

import os, sys, string, re
import csv
import codecs
import json
import glob
import datetime
import xbundle
import tempfile
from collections import namedtuple, defaultdict
from lxml import etree,html
from path import path
from fix_unicode import fix_bad_unicode

DO_SAVE_TO_MONGO = False
DO_SAVE_TO_BIGQUERY = True
DATADIR = "DATA"

VERBOSE_WARNINGS = True
#FORCE_NO_HIDE = True
FORCE_NO_HIDE = False

# <codecell>

#-----------------------------------------------------------------------------

# storage class for each axis element
Axel = namedtuple('Axel', 'course_id index url_name category gformat start due name path module_id data chapter_mid graded parent is_split split_url_name')

class Policy(object):
    '''
    Handle policy.json for edX course.  Also deals with grading_policy.json if present in same directory.
    '''
    policy = None
    grading_policy = None
    InheritedSettings = ['format', 'hide_from_toc', 'start', 'due', 'graded']

    def __init__(self, pfn):
        '''
        pfn = policy file name
        '''
        self.pfn = path(pfn)
        print "loading policy file %s" % pfn
        self.policy = json.loads(open(pfn).read())
        
        gfn = self.pfn.dirname() / 'grading_policy.json'
        if os.path.exists(gfn):
            self.gfn = gfn
            self.grading_policy = json.loads(open(gfn).read())
            
    @property
    def semester(self):
        # print "semester: policy keys = %s" % self.policy.keys()
        semester = [t[1] for t in [k.split('/',1) for k in self.policy.keys()] if t[0]=='course'][0]
        return semester

    def get_metadata(self, xml, setting, default=None, parent=False):
        '''
        Retrieve policy for xml element, given the policy JSON and, for a specific setting.
        Handles inheritance of certain settings (like format and hide_from_toc)

        xml = etree
        setting = string
        '''
        if parent:
            val = xml.get(setting, None)
            if val is not None and not val=='null' and ((setting in self.InheritedSettings) and not val==""):
                return val
        
        un = xml.get('url_name', xml.get('url_name_orig', '<no_url_name>'))
        pkey = '%s/%s' % (xml.tag, un)
        
        if pkey in self.policy and setting in self.policy[pkey]:
            # print " using self.policy for %s" % setting
            return self.policy[pkey][setting]
        
        if not setting in self.InheritedSettings:
            return default

        parent = xml.getparent()	# inherited metadata: try parent
        if parent is not None:
            # print "  using parent %s for policy %s" % (parent, setting)
            return self.get_metadata(parent, setting, default, parent=True)

# <codecell>

#-----------------------------------------------------------------------------

def get_from_parent(xml, attr, default):
    '''
    get attribute from parent, recursing until end or found
    '''
    parent = xml.getparent()
    if parent is not None:
        v = parent.get(attr,None)
        if v is not None:
            return v
        return get_from_parent(parent, attr, default)
    return default
        

def date_parse(datestr, retbad=False):
    if not datestr:
        return None

    if datestr.startswith('"') and datestr.endswith('"'):
        datestr = datestr[1:-1]

    formats = ['%Y-%m-%dT%H:%M:%SZ',    	# 2013-11-13T21:00:00Z
               '%Y-%m-%dT%H:%M:%S.%f',    	# 2012-12-04T13:48:28.427430
               '%Y-%m-%dT%H:%M:%S',
               '%Y-%m-%dT%H:%M:%S+00:00',	# 2014-12-09T15:00:00+00:00 
               '%Y-%m-%dT%H:%M',		# 2013-02-12T19:00
               '%Y-%m-%dT%H:%M:%S+0000',	# 2015-06-09T12:25:21.801+0000
               '%B %d, %Y',			# February 25, 2013
               '%B %d, %H:%M, %Y', 		# December 12, 22:00, 2012
               '%B %d, %Y, %H:%M', 		# March 25, 2013, 22:00
               '%B %d %Y, %H:%M',		# January 2 2013, 22:00
               '%B %d %Y', 			# March 13 2014
               '%B %d %H:%M, %Y',		# December 24 05:00, 2012
               ]

    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(datestr,fmt)
            return dt
        except Exception as err:
            continue

    print "Date %s unparsable" % datestr
    if retbad:
        return "Bad"
    return None

# <codecell>

#-----------------------------------------------------------------------------

class CourseInfo(object):
    def __init__(self, fn, policyfn='', dir=''):
        cxml = etree.parse(fn).getroot()
        self.cxml = cxml
        self.org = cxml.get('org')
        self.course = cxml.get('course')
        self.url_name = cxml.get('url_name')
        if policyfn:
            self.load_policy(policyfn)
        else:
            pfn = dir / 'policies' / self.url_name + ".json"
            if not os.path.exists(pfn):
                pfn = dir / 'policies' / self.url_name / "policy.json"
            if os.path.exists(pfn):
                self.load_policy(pfn)
            else:
                print "==================== ERROR!  Missing policy file %s" % pfn

    def load_policy(self, pfn):
        self.policy = Policy(pfn)
        if self.url_name is None:
            self.url_name = self.policy.semester


#-----------------------------------------------------------------------------

        
def make_axis(dir):
    '''
    return dict of {course_id : { policy, xbundle, axis (as list of Axel elements) }}
    '''
    
    courses = []
    log_msg = []

    def logit(msg, nolog=False):
        if not nolog:
            log_msg.append(msg)
        print msg

    dir = path(dir)

    if os.path.exists(dir / 'roots'):	# if roots directory exists, use that for different course versions
        # get roots
        roots = glob.glob(dir / 'roots/*.xml')
        courses = [ CourseInfo(fn, '', dir) for fn in roots ]

    else:	# single course.xml file - use differnt policy files in policy directory, though

        fn = dir / 'course.xml'
    
        # get semesters
        policies = glob.glob(dir/'policies/*.json')
        assetsfn = dir / 'policies/assets.json'
        if str(assetsfn) in policies:
            policies.remove(assetsfn)
        if not policies:
            policies = glob.glob(dir/'policies/*/policy.json')
        if not policies:
            logit("Error: no policy files found!")
        
        courses = [ CourseInfo(fn, pfn) for pfn in policies ]


    logit("%d course runs found: %s" % (len(courses), [c.url_name for c in courses]))
    
    ret = {}

    # construct axis for each policy
    for cinfo in courses:
        policy = cinfo.policy
        semester = policy.semester
        org = cinfo.org
        course = cinfo.course
        cid = '%s/%s/%s' % (org, course, semester)
        logit('course_id=%s' %  cid)
    
        cfn = dir / ('course/%s.xml' % semester)
        
        # generate XBundle for course
        xml = etree.parse(cfn).getroot()
        xb = xbundle.XBundle(keep_urls=True, skip_hidden=True, keep_studio_urls=True)
        xb.policy = policy.policy
        cxml = xb.import_xml_removing_descriptor(dir, xml)

        # append metadata
        metadata = etree.Element('metadata')
        cxml.append(metadata)
        policy_xml = etree.Element('policy')
        metadata.append(policy_xml)
        policy_xml.text = json.dumps(policy.policy)
        grading_policy_xml = etree.Element('grading_policy')
        metadata.append(grading_policy_xml)
        grading_policy_xml.text = json.dumps(policy.grading_policy)
    
        bundle = etree.tostring(cxml, pretty_print=True)
        #print bundle[:500]
        index = [1]
        caxis = []
    
        def walk(x, seq_num=1, path=[], seq_type=None, parent_start=None, parent=None, chapter=None,
                 parent_url_name=None, split_url_name=None):
            '''
            Recursively traverse course tree.  
            
            x        = current etree element
            seq_num  = sequence of current element in its parent, starting from 1
            path     = list of url_name's to current element, following edX's hierarchy conventions
            seq_type = problemset, sequential, or videosequence
            parent_start = start date of parent of current etree element
            parent   = parent module
            chapter  = the last chapter module_id seen while walking through the tree
            parent_url_name = url_name of parent
            split_url_name   = url_name of split_test element if this subtree is in a split_test, otherwise None
            '''
            url_name = x.get('url_name',x.get('url_name_orig',''))
            if not url_name:
                dn = x.get('display_name')
                if dn is not None:
                    url_name = dn.strip().replace(' ','_')     # 2012 convention for converting display_name to url_name
                    url_name = url_name.replace(':','_')
                    url_name = url_name.replace('.','_')
                    url_name = url_name.replace('(','_').replace(')','_').replace('__','_')
            
            data = None
            start = None

            if not FORCE_NO_HIDE:
                hide = policy.get_metadata(x, 'hide_from_toc')
                if hide is not None and not hide=="false":
                    logit('[edx2course_axis] Skipping %s (%s), it has hide_from_toc=%s' % (x.tag, x.get('display_name','<noname>'), hide))
                    return

            if x.tag=='video':	# special: for video, let data = youtube ID(s)
                data = x.get('youtube','')
                if data:
                    # old ytid format - extract just the 1.0 part of this 
                    # 0.75:JdL1Vo0Hru0,1.0:lbaG3uiQ6IY,1.25:Lrj0G8RWHKw,1.50:54fs3-WxqLs
                    ytid = data.replace(' ','').split(',')
                    ytid = [z[1] for z in [y.split(':') for y in ytid] if z[0]=='1.0']
                    # print "   ytid: %s -> %s" % (x.get('youtube',''), ytid)
                    if ytid:
                        data = ytid
                if not data:
                    data = x.get('youtube_id_1_0', '')
                if data:
                    data = '{"ytid": "%s"}' % data

            if x.tag=="split_test":
                data = {}
                to_copy = ['group_id_to_child', 'user_partition_id']
                for tc in to_copy:
                    data[tc] = x.get(tc, None)

            if x.tag=='problem' and x.get('weight') is not None and x.get('weight'):
                try:
                    # Changed from string to dict. In next code block.
                    data = {"weight": "%f" % float(x.get('weight'))}
                except Exception as err:
                    logit("    Error converting weight %s" % x.get('weight'))

            ### Had a hard time making my code work within the try/except for weight. Happy to improve
            ### Also note, weight is typically missing in problems. So I find it weird that we throw an exception.
            if x.tag=='problem':
                # Initialize data if no weight
                if not data:
                    data = {}

                # meta will store all problem related metadata, then be used to update data
                meta = {}
                # Items is meant to help debug - an ordered list of encountered problem types with url names
                # Likely should not be pulled to Big Query 
                meta['items'] = []
                # Known Problem Types
                known_problem_types = ['multiplechoiceresponse','numericalresponse','choiceresponse',
                                       'optionresponse','stringresponse','formularesponse',
                                       'customresponse','fieldset']

                # Loop through all child nodes in a problem. If encountering a known problem type, add metadata.
                for a in x:
                    if a.tag in known_problem_types:
                        meta['items'].append({'itype':a.tag,'url_name':a.get('url_name')})

                ### Check for accompanying image
                images = x.findall('.//img')
                # meta['has_image'] = False
                
                if images and len(images)>0:
                    meta['has_image'] = True #Note, one can use a.get('src'), but needs to account for multiple images
                    # print meta['img'],len(images)

                ### Search for all solution tags in a problem
                solutions = x.findall('.//solution')
                # meta['has_solution'] = False

                if solutions and len(solutions)>0:
                    text = ''
                    for sol in solutions:
                        text = text.join(html.tostring(e, pretty_print=False) for e in sol)
                        # This if statment checks each solution. Note, many MITx problems have multiple solution tags.
                        # In 8.05x, common to put image in one solution tag, and the text in a second. So we are checking each tag.
                        # If we find one solution with > 65 char, or one solution with an image, we set meta['solution'] = True
                        if len(text) > 65 or 'img src' in text:
                            meta['has_solution'] = True

                ### If meta is empty, log all tags for debugging later. 
                if len(meta)==0:
                    logit('item type not found - here is the list of tags:['+','.join(a.tag if a else ' ' for a in x)+']')
                    # print 'problem type not found - here is the list of tags:['+','.join(a.tag for a in x)+']'

                ### Add easily accessible metadata for problems
                # num_items: number of items
                # itype: problem type - note, mixed is used when items are not of same type
                if len(meta['items']) > 0:
                    # Number of Items
                    meta['num_items'] = len(meta['items'])

                    # Problem Type
                    if all(meta['items'][0]['itype'] == item['itype'] for item in meta['items']):
                        meta['itype'] = meta['items'][0]['itype']
                        # print meta['items'][0]['itype']
                    else:
                        meta['itype'] = 'mixed'

                # Update data field
                ### ! For now, removing the items field. 
                del meta["items"]               

                data.update(meta)
                data = json.dumps(data)

            if x.tag=='html':
                iframe = x.find('.//iframe')
                if iframe is not None:
                    logit("   found iframe in html %s" % url_name)
                    src = iframe.get('src','')
                    if 'https://www.youtube.com/embed/' in src:
                        m = re.search('embed/([^"/?]+)', src)
                        if m:
                            data = '{"ytid": "%s"}' % m.group(1)
                            logit("    data=%s" % data)
                
            if url_name:              # url_name is mandatory if we are to do anything with this element
                # url_name = url_name.replace(':','_')
                dn = x.get('display_name', url_name)
                try:
                    #dn = dn.decode('utf-8')
                    dn = unicode(dn)
                    dn = fix_bad_unicode(dn)
                except Exception as err:
                    logit('unicode error, type(dn)=%s'  % type(dn))
                    raise
                pdn = policy.get_metadata(x, 'display_name')      # policy display_name - if given, let that override default
                if pdn is not None:
                    dn = pdn

                #start = date_parse(x.get('start', policy.get_metadata(x, 'start', '')))
                start = date_parse(policy.get_metadata(x, 'start', '', parent=True))
                
                if parent_start is not None and start < parent_start:
                    if VERBOSE_WARNINGS:
                        logit("    Warning: start of %s element %s happens before start %s of parent: using parent start" % (start, x.tag, parent_start), nolog=True)
                    start = parent_start
                #print "start for %s = %s" % (x, start)
                
                # drop bad due date strings
                if date_parse(x.get('due',None), retbad=True)=='Bad':
                    x.set('due', '')

                due = date_parse(policy.get_metadata(x, 'due', '', parent=True))
                if x.tag=="problem":
                    logit("    setting problem due date: for %s due=%s" % (url_name, due), nolog=True)

                gformat = x.get('format', policy.get_metadata(x, 'format', ''))
                if url_name=='hw0':
                    logit( "gformat for hw0 = %s" % gformat)

                graded = x.get('graded', policy.get_metadata(x, 'graded', ''))
                if not (type(graded) in [unicode, str]):
                    graded = str(graded)

                # compute path
                # The hierarchy goes: `course > chapter > (problemset | sequential | videosequence)`
                if x.tag=='chapter':
                    path = [url_name]
                elif x.tag in ['problemset', 'sequential', 'videosequence', 'proctor', 'randomize']:
                    seq_type = x.tag
                    path = [path[0], url_name]
                else:
                    path = path[:] + [str(seq_num)]      # note arrays are passed by reference, so copy, don't modify
                    
                # compute module_id
                if x.tag=='html':
                    module_id = '%s/%s/%s/%s' % (org, course, seq_type, '/'.join(path[1:3]))  # module_id which appears in tracking log
                else:
                    module_id = '%s/%s/%s/%s' % (org, course, x.tag, url_name)
                
                # debugging
                # print "     module %s gformat=%s" % (module_id, gformat)

                # done with getting all info for this axis element; save it
                path_str = '/' + '/'.join(path)
                ae = Axel(cid, index[0], url_name, x.tag, gformat, start, due, dn, path_str, module_id, data, chapter, graded,
                          parent_url_name,
                          not split_url_name==None,
                          split_url_name)
                caxis.append(ae)
                index[0] += 1
            else:
                if VERBOSE_WARNINGS:
                    if x.tag in ['transcript', 'wiki', 'metadata']:
                        pass
                    else:
                        logit("Missing url_name for element %s (attrib=%s, parent_tag=%s)" % (x, x.attrib, (parent.tag if parent is not None else '')))

            # chapter?
            if x.tag=='chapter':
                the_chapter = module_id
            else:
                the_chapter = chapter

            # done processing this element, now process all its children
            if (not x.tag in ['html', 'problem', 'discussion', 'customtag', 'poll_question', 'combinedopenended', 'metadata']):
                inherit_seq_num = (x.tag=='vertical' and not url_name)    # if <vertical> with no url_name then keep seq_num for children
                if not inherit_seq_num:
                    seq_num = 1
                for y in x:
                    if (not str(y).startswith('<!--')) and (not y.tag in ['discussion', 'source']):
                        if not split_url_name and x.tag=="split_test":
                            split_url_name = url_name
                                
                        walk(y, seq_num, path, seq_type, parent_start=start, parent=x, chapter=the_chapter,
                             parent_url_name=url_name,
                             split_url_name=split_url_name,
                        )
                        if not inherit_seq_num:
                            seq_num += 1
                
        walk(cxml)
        ret[cid] = dict(policy=policy.policy, 
                        bundle=bundle, 
                        axis=caxis, 
                        grading_policy=policy.grading_policy,
                        log_msg=log_msg,
                        )
    
    return ret

# <codecell>

def save_data_to_mongo(cid, cdat, caset, xbundle=None):
    '''
    Save course axis data to mongo
    
    cid = course_id
    cdat = course axis data
    caset = list of course axis data in dict format
    xbundle = XML bundle of course (everything except static files)
    '''
    import save_to_mongo
    save_to_mongo.do_save(cid, caset, xbundle)

# <codecell>

def save_data_to_bigquery(cid, cdat, caset, xbundle=None, datadir=None, log_msg=None,
                          use_dataset_latest=False):
    '''
    Save course axis data to bigquery
    
    cid = course_id
    cdat = course axis data
    caset = list of course axis data in dict format
    xbundle = XML bundle of course (everything except static files)
    '''
    import axis2bigquery
    axis2bigquery.do_save(cid, caset, xbundle, datadir, log_msg, use_dataset_latest=use_dataset_latest)

# <codecell>

#-----------------------------------------------------------------------------

def fix_duplicate_url_name_vertical(axis):
    '''
    1. Look for duplicate url_name values
    2. If a vertical has a duplicate url_name with anything else, rename that url_name
       to have a "_vert" suffix.  

    axis = list of Axel objects
    '''
    axis_by_url_name = defaultdict(list)
    for idx in range(len(axis)):
        ael = axis[idx]
        axis_by_url_name[ael.url_name].append(idx)
    
    for url_name, idxset in axis_by_url_name.items():
        if len(idxset)==1:
            continue
        print "--> Duplicate url_name %s shared by:" % url_name
        vert = None
        for idx in idxset:
            ael = axis[idx]
            print "       %s" % str(ael)
            if ael.category=='vertical':
                nun = "%s_vertical" % url_name
                print "          --> renaming url_name to become %s" % nun
                new_ael = ael._replace(url_name=nun)
                axis[idx] = new_ael

#-----------------------------------------------------------------------------

def process_course(dir, use_dataset_latest=False, force_course_id=None):
    '''
    if force_course_id is specified, then that value is used as the course_id
    '''
    ret = make_axis(dir)

    # save data as csv and txt: loop through each course (multiple policies can exist withing a given course dir)
    for default_cid, cdat in ret.iteritems():

        cid = force_course_id or default_cid

        print "================================================== (%s)" % cid

        log_msg = cdat['log_msg']

        # write out xbundle to xml file
        bfn = '%s/xbundle_%s.xml' % (DATADIR, cid.replace('/','__'))
        bundle_out = ret[default_cid]['bundle']
        bundle_out = bundle_out.replace('<!------>', '')	# workaround improper XML
        codecs.open(bfn,'w',encoding='utf8').write(bundle_out)

        print "Writing out xbundle to %s (len=%s)" % (bfn, len(bundle_out))
        
        # clean up xml file with xmllint if available
        if 1:
            if os.system('which xmllint')==0:
                xlret = os.system('xmllint --format %s > %s.new' % (bfn, bfn))
                if xlret==0:
                    os.system('mv %s.new %s' % (bfn, bfn))

        print "saving data for %s" % cid

        fix_duplicate_url_name_vertical(cdat['axis'])

        header = ("index", "url_name", "category", "gformat", "start", 'due', "name", "path", "module_id", "data", "chapter_mid", "graded",
                  "parent", "is_split", "split_url_name")
        caset = [{ x: getattr(ae,x) for x in header } for ae in cdat['axis']]

        # optional save to mongodb
        if DO_SAVE_TO_MONGO:
            save_data_to_mongo(cid, cdat, caset, ret[default_cid]['bundle'])
        
        # optional save to bigquery
        if DO_SAVE_TO_BIGQUERY:
            save_data_to_bigquery(cid, cdat, caset, ret[default_cid]['bundle'], DATADIR, log_msg, use_dataset_latest=use_dataset_latest)

        # print out to text file
        afp = codecs.open('%s/axis_%s.txt' % (DATADIR, cid.replace('/','__')),'w', encoding='utf8')
        aformat = "%8s\t%40s\t%24s\t%16s\t%16s\t%16s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
        afp.write(aformat % header)
        afp.write(aformat % tuple(["--------"] *len(aformat.split('\t'))))
        for ca in caset:
            afp.write(aformat % tuple([ca[x] for x in header]))
        afp.close()
        
        # save as csv file
        csvfn = '%s/axis_%s.csv' % (DATADIR, cid.replace('/','__'))
        csvca = '%s/course_axis.csv' % (DATADIR)
        fp = open(csvfn, 'wb')
        writer = csv.writer(fp, dialect="excel", quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        for ca in caset:
            try:
                data = [ ('%s' % ca[k]).encode('utf8') for k in header]
                writer.writerow(data)
            except Exception as err:
                print "Failed to write row %s" % data
                print "Error=%s" % err
        fp.close()
        os.system('cp %s %s' % (csvfn, csvca ) ) # Make an extra copy named course_axis.csv
        os.system('gzip -9 -f %s' % csvca )
        print "Saved course axis to %s" % csvfn
        print "Saved course axis to %s" % csvca+'.gz'

# <codecell>

def process_xml_tar_gz_file(fndir, use_dataset_latest=False, force_course_id=None):
    '''
    convert *.xml.tar.gz to course axis
    This could be improved to use the python tar & gzip libraries.
    '''
    fnabs = os.path.abspath(fndir)
    tdir = tempfile.mkdtemp()
    cmd = "cd %s; tar xzf %s" % (tdir, fnabs)
    print "running %s" % cmd
    os.system(cmd)
    newfn = glob.glob('%s/*' % tdir)[0]
    print "Using %s as the course xml directory" % newfn
    process_course(newfn, use_dataset_latest=use_dataset_latest, force_course_id=force_course_id)
    print "removing temporary files %s" % tdir
    os.system('rm -rf %s' % tdir)

# <codecell>

if __name__=='__main__':
    if sys.argv[1]=='-mongo':
        DO_SAVE_TO_MONGO = True
        print "============================================================ Enabling Save to Mongo"
        sys.argv.pop(1)

    if sys.argv[1]=='-datadir':
        sys.argv.pop(1)
        DATADIR = sys.argv[1]
        sys.argv.pop(1)
        print "==> using %s as DATADIR" % DATADIR

    if not os.path.exists(DATADIR):
        os.mkdir(DATADIR)
    for fn in sys.argv[1:]:
        if os.path.isdir(fn):
            process_course(fn)
        else:
            # not a directory - is it a tar.gz file?
            if fn.endswith('.tar.gz') or fn.endswith('.tgz'):
                process_xml_tar_gz_file(fn)
                

