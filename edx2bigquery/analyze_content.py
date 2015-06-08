#!/usr/bin/python

import os
import sys
import unicodecsv as csv
import gsutil
import bqutil
import json
import datetime

from collections import defaultdict, OrderedDict
from lxml import etree
from path import path
from load_course_sql import find_course_sql_dir
from make_geoip_table import lock_file

CCDATA = "course_content.csv"
CMINFO = "course_metainfo.csv"
CMINFO_OVERRIDES = "course_metainfo_overrides.csv"

def make_key(key):
    '''
    turn a string into a valid column name / key for BQ
    '''
    key = key.strip()
    key = key.replace(' ', '_').replace("'", "_").replace('/', '_').replace('(','').replace(')','').replace('-', '_').replace(',', '')
    return key

def get_stats_module_usage(course_id,
                           basedir="X-Year-2-data-sql", 
                           datedir="2013-09-21", 
                           use_dataset_latest=False,
                           ):
    '''
    Get data from the stats_module_usage table, if it doesn't already exist as a local file.
    Compute it if necessary.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    sql = """   SELECT 
                    module_type, module_id, count(*) as ncount 
                FROM [{dataset}.studentmodule] 
                group by module_id, module_type
                order by module_id
          """.format(dataset=dataset)

    table = 'stats_module_usage'
    course_dir = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest)
    csvfn = course_dir / (table + ".csv")

    data = {}
    if csvfn.exists():
        # read file into data structure
        for k in list(csv.DictReader(open(csvfn))):
            midfrag = tuple(k['module_id'].split('/')[-2:])
            data[midfrag] = k
    else:
        # download if it is already computed, or recompute if needed
        bqdat = bqutil.get_bq_table(dataset, table, sql=sql)
        if bqdat is None:
            bqdat = {'data': []}

        fields = [ "module_type", "module_id", "ncount" ]
        fp = open(csvfn, 'w')
        cdw = csv.DictWriter(fp, fieldnames=fields)
        cdw.writeheader()
        for k in bqdat['data']:
            midfrag = tuple(k['module_id'].split('/')[-2:])
            data[midfrag] = k
            try:
                k['module_id'] = k['module_id'].encode('utf8')
                cdw.writerow(k)
            except Exception as err:
                print "Error writing row %s, err=%s" % (k, str(err))
        fp.close()

    print "[analyze_content] got %d lines of studentmodule usage data" % len(data)
    return data


def analyze_course_content(course_id, 
                           listings_file=None,
                           basedir="X-Year-2-data-sql", 
                           datedir="2013-09-21", 
                           use_dataset_latest=False,
                           do_upload=False,
                           courses=None,
                           verbose=True,
                           ):
    '''
    Compute course_content table, which quantifies:

    - number of chapter, sequential, vertical modules
    - number of video modules
    - number of problem, *openended, mentoring modules
    - number of dicussion, annotatable, word_cloud modules

    Do this using the course "xbundle" file, produced when the course axis is computed.

    Include only modules which had nontrivial use, to rule out the staff and un-shown content. 
    Do the exclusion based on count of module appearing in the studentmodule table, based on 
    stats_module_usage for each course.

    Also, from the course listings file, compute the number of weeks the course was open.

    If do_upload (triggered by --force-recompute) then upload all accumulated data to the course report dataset 
    as the "stats_course_content" table.  Also generate a "course_summary_stats" table, stored in the
    course_report_ORG or course_report_latest dataset.  The course_summary_stats table combines
    data from many reports,, including stats_course_content, the medians report, the listings file,
    broad_stats_by_course, and time_on_task_stats_by_course.
    
    '''

    if do_upload:
        if use_dataset_latest:
            org = "latest"
        else:
            org = courses[0].split('/',1)[0]	# extract org from first course_id in courses

        crname = 'course_report_%s' % org

        gspath = gsutil.gs_path_from_course_id(crname)
        gsfnp = gspath / CCDATA
        gsutil.upload_file_to_gs(CCDATA, gsfnp)
        tableid = "stats_course_content"
        dataset = crname

        mypath = os.path.dirname(os.path.realpath(__file__))
        SCHEMA_FILE = '%s/schemas/schema_content_stats.json' % mypath

        try:
            the_schema = json.loads(open(SCHEMA_FILE).read())[tableid]
        except Exception as err:
            print "Oops!  Failed to load schema file for %s.  Error: %s" % (tableid, str(err))
            raise

        if 0:
            bqutil.load_data_to_table(dataset, tableid, gsfnp, the_schema, wait=True, verbose=False,
                                      format='csv', skiprows=1)

        table = 'course_metainfo'
        course_tables = ',\n'.join([('[%s.course_metainfo]' % bqutil.course_id2dataset(x)) for x in courses])
        sql = "select * from {course_tables}".format(course_tables=course_tables)
        print "--> Creating %s.%s using %s" % (dataset, table, sql)

        if 1:
            metainfo_dataset = bqutil.get_bq_table(dataset, table, sql=sql, 
                                          newer_than=datetime.datetime(2015, 1, 16, 3, 0),
                                          )
            # bqutil.create_bq_table(dataset, table, sql, overwrite=True)


        #-----------------------------------------------------------------------------
        # make course_summary_stats table
        #
        # This is a combination of the broad_stats_by_course table (if that exists), and course_metainfo.
        # Also use (and create if necessary) the nregistered_by_wrap table.

        # get the broad_stats_by_course data
        bsbc = bqutil.get_table_data(dataset, 'broad_stats_by_course')

        table_list = bqutil.get_list_of_table_ids(dataset)

        latest_person_course = max([ x for x in table_list if x.startswith('person_course_')])
        print "Latest person_course table in %s is %s" % (dataset, latest_person_course)
        
        sql = """
                SELECT pc.course_id as course_id, 
                    cminfo.wrap_date as wrap_date,
                    count(*) as nregistered,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) nregistered_by_wrap,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) / nregistered * 100 nregistered_by_wrap_pct,
                FROM
                    [{dataset}.{person_course}] as pc
                left join (
                 SELECT course_id,
                      TIMESTAMP(concat(wrap_year, "-", wrap_month, '-', wrap_day, ' 23:59:59')) as wrap_date,
                 FROM (
                  SELECT course_id, 
                    regexp_extract(value, r'(\d+)/\d+/\d+') as wrap_month,
                    regexp_extract(value, r'\d+/(\d+)/\d+') as wrap_day,
                    regexp_extract(value, r'\d+/\d+/(\d+)') as wrap_year,
                  FROM [{dataset}.course_metainfo]
                  where key='listings_Course Wrap'
                 )) as cminfo
                on pc.course_id = cminfo.course_id
                
                group by course_id, wrap_date
                order by course_id
        """.format(dataset=dataset, person_course=latest_person_course)

        nr_by_wrap = bqutil.get_bq_table(dataset, 'nregistered_by_wrap', sql=sql, key={'name': 'course_id'})

        # rates for registrants before and during course
        
        sql = """
                SELECT 
                    *,
                    ncertified / nregistered * 100 as pct_certified_of_reg,
                    ncertified_and_registered_before_launch / nregistered_before_launch * 100 as pct_certified_reg_before_launch,
                    ncertified_and_registered_during_course / nregistered_during_course * 100 as pct_certified_reg_during_course,
                    ncertified / nregistered_by_wrap * 100 as pct_certified_of_reg_by_wrap,
                    ncertified / nviewed * 100 as pct_certified_of_viewed,
                    ncertified / nviewed_by_wrap * 100 as pct_certified_of_viewed_by_wrap,
                    ncertified_by_ewrap / nviewed_by_ewrap * 100 as pct_certified_of_viewed_by_ewrap,
                FROM
                (
                # ------------------------
                # get aggregate data
                SELECT pc.course_id as course_id, 
                    cminfo.wrap_date as wrap_date,
                    count(*) as nregistered,
                    sum(case when pc.certified then 1 else 0 end) ncertified,
                    sum(case when (TIMESTAMP(pc.cert_created_date) < cminfo.ewrap_date) and (pc.certified and pc.viewed) then 1 else 0 end) ncertified_by_ewrap,
                    sum(case when pc.viewed then 1 else 0 end) nviewed,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) nregistered_by_wrap,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) / nregistered * 100 nregistered_by_wrap_pct,
                    sum(case when (pc.start_time < cminfo.wrap_date) and pc.viewed then 1 else 0 end) nviewed_by_wrap,
                    sum(case when (pc.start_time < cminfo.ewrap_date) and pc.viewed then 1 else 0 end) nviewed_by_ewrap,
                    sum(case when pc.start_time < cminfo.launch_date then 1 else 0 end) nregistered_before_launch,
                    sum(case when pc.start_time < cminfo.launch_date 
                              and pc.certified
                              then 1 else 0 end) ncertified_and_registered_before_launch,
                    sum(case when (pc.start_time >= cminfo.launch_date) 
                              and (pc.start_time < cminfo.wrap_date) then 1 else 0 end) nregistered_during_course,
                    sum(case when (pc.start_time >= cminfo.launch_date) 
                              and (pc.start_time < cminfo.wrap_date) 
                              and pc.certified
                              then 1 else 0 end) ncertified_and_registered_during_course,
                FROM
                    [{dataset}.{person_course}] as pc
                left join (
                
                # --------------------
                #  get course launch and wrap dates from course_metainfo

       SELECT AA.course_id as course_id, 
              AA.wrap_date as wrap_date,
              AA.launch_date as launch_date,
              BB.ewrap_date as ewrap_date,
       FROM (
               #  inner get course launch and wrap dates from course_metainfo
                SELECT A.course_id as course_id,
                  A.wrap_date as wrap_date,
                  B.launch_date as launch_date,
                from
                (
                 SELECT course_id,
                      TIMESTAMP(concat(wrap_year, "-", wrap_month, '-', wrap_day, ' 23:59:59')) as wrap_date,
                 FROM (
                  SELECT course_id, 
                    regexp_extract(value, r'(\d+)/\d+/\d+') as wrap_month,
                    regexp_extract(value, r'\d+/(\d+)/\d+') as wrap_day,
                    regexp_extract(value, r'\d+/\d+/(\d+)') as wrap_year,
                  FROM [{dataset}.course_metainfo]
                  where key='listings_Course Wrap'
                 )
                ) as A
                left outer join 
                (
                 SELECT course_id,
                      TIMESTAMP(concat(launch_year, "-", launch_month, '-', launch_day)) as launch_date,
                 FROM (
                  SELECT course_id, 
                    regexp_extract(value, r'(\d+)/\d+/\d+') as launch_month,
                    regexp_extract(value, r'\d+/(\d+)/\d+') as launch_day,
                    regexp_extract(value, r'\d+/\d+/(\d+)') as launch_year,
                  FROM [{dataset}.course_metainfo]
                  where key='listings_Course Launch'
                 )
                ) as B
                on A.course_id = B.course_id 
                # end inner course_metainfo subquery
            ) as AA
            left outer join
            (
                 SELECT course_id,
                      TIMESTAMP(concat(wrap_year, "-", wrap_month, '-', wrap_day, ' 23:59:59')) as ewrap_date,
                 FROM (
                  SELECT course_id, 
                    regexp_extract(value, r'(\d+)/\d+/\d+') as wrap_month,
                    regexp_extract(value, r'\d+/(\d+)/\d+') as wrap_day,
                    regexp_extract(value, r'\d+/\d+/(\d+)') as wrap_year,
                  FROM [{dataset}.course_metainfo]
                  where key='listings_Empirical Course Wrap'
                 )
            ) as BB
            on AA.course_id = BB.course_id

                # end course_metainfo subquery
                # --------------------
                
                ) as cminfo
                on pc.course_id = cminfo.course_id
                
                group by course_id, wrap_date
                order by course_id
                # ---- end get aggregate data
                )
                order by course_id
        """.format(dataset=dataset, person_course=latest_person_course)

        print "--> Assembling course_summary_stats from %s" % 'stats_cert_rates_by_registration'
        sys.stdout.flush()
        cert_by_reg = bqutil.get_bq_table(dataset, 'stats_cert_rates_by_registration', sql=sql, 
                                          newer_than=datetime.datetime(2015, 1, 16, 3, 0),
                                          key={'name': 'course_id'})

        # start assembling course_summary_stats

        c_sum_stats = defaultdict(OrderedDict)
        for entry in bsbc['data']:
            course_id = entry['course_id']
            cmci = c_sum_stats[course_id]
            cmci.update(entry)
            cnbw = nr_by_wrap['data_by_key'][course_id]
            nbw = int(cnbw['nregistered_by_wrap'])
            cmci['nbw_wrap_date'] = cnbw['wrap_date']
            cmci['nregistered_by_wrap'] = nbw
            cmci['nregistered_by_wrap_pct'] = cnbw['nregistered_by_wrap_pct']
            cmci['frac_female'] = float(entry['n_female_viewed']) / (float(entry['n_male_viewed']) + float(entry['n_female_viewed']))
            ncert = float(cmci['certified_sum'])
            if ncert:
                cmci['certified_of_nregistered_by_wrap_pct'] = nbw / ncert * 100.0
            else:
                cmci['certified_of_nregistered_by_wrap_pct'] = None
            cbr = cert_by_reg['data_by_key'][course_id]
            for field, value in cbr.items():
                cmci['cbr_%s' % field] = value

        # add medians for viewed, explored, and certified

        msbc_tables = {'msbc_viewed': "viewed_median_stats_by_course",
                       'msbc_explored': 'explored_median_stats_by_course',
                       'msbc_certified': 'certified_median_stats_by_course',
                       'msbc_verified': 'verified_median_stats_by_course',
                       }
        for prefix, mtab in msbc_tables.items():
            print "--> Merging median stats data from %s" % mtab
            sys.stdout.flush()
            bqdat = bqutil.get_table_data(dataset, mtab)
            for entry in bqdat['data']:
                course_id = entry['course_id']
                cmci = c_sum_stats[course_id]
                for field, value in entry.items():
                    cmci['%s_%s' % (prefix, field)] = value

        # add time on task data

        tot_table = "time_on_task_stats_by_course"
        prefix = "ToT"
        print "--> Merging time on task data from %s" % tot_table
        sys.stdout.flush()
        try:
            bqdat = bqutil.get_table_data(dataset, tot_table)
        except Exception as err:
            bqdat = {'data': {}}
        for entry in bqdat['data']:
            course_id = entry['course_id']
            cmci = c_sum_stats[course_id]
            for field, value in entry.items():
                if field=='course_id':
                    continue
                cmci['%s_%s' % (prefix, field)] = value

        # add serial time on task data

        tot_table = "time_on_task_serial_stats_by_course"
        prefix = "SToT"
        print "--> Merging serial time on task data from %s" % tot_table
        sys.stdout.flush()
        try:
            bqdat = bqutil.get_table_data(dataset, tot_table)
        except Exception as err:
            bqdat = {'data': {}}
        for entry in bqdat['data']:
            course_id = entry['course_id']
            cmci = c_sum_stats[course_id]
            for field, value in entry.items():
                if field=='course_id':
                    continue
                cmci['%s_%s' % (prefix, field)] = value

        # add show_answer stats

        tot_table = "show_answer_stats_by_course"
        prefix = "SAS"
        print "--> Merging show_answer stats data from %s" % tot_table
        sys.stdout.flush()
        try:
            bqdat = bqutil.get_table_data(dataset, tot_table)
        except Exception as err:
            bqdat = {'data': {}}
        for entry in bqdat['data']:
            course_id = entry['course_id']
            cmci = c_sum_stats[course_id]
            for field, value in entry.items():
                if field=='course_id':
                    continue
                cmci['%s_%s' % (prefix, field)] = value

        # setup list of keys, for CSV output

        css_keys = c_sum_stats.values()[0].keys()

        # retrieve course_metainfo table, pivot, add that to summary_stats

        print "--> Merging course_metainfo from %s" % table
        sys.stdout.flush()
        bqdat = bqutil.get_table_data(dataset, table)

        listings_keys = map(make_key, ["Institution", "Semester", "New or Rerun", "Andrew Recodes New/Rerun", 
                                       "Course Number", "Short Title", "Andrew's Short Titles", "Title", 
                                       "Instructors", "Registration Open", "Course Launch", "Course Wrap", "course_id",
                                       "Empirical Course Wrap", "Andrew's Order", "certifies", "MinPassGrade",
                                       '4-way Category by name', "4-way (CS, STEM, HSocSciGov, HumHistRel)"
                                       ])
        listings_keys.reverse()
        
        for lk in listings_keys:
            css_keys.insert(1, "listings_%s" % lk)

        COUNTS_TO_KEEP = ['discussion', 'problem', 'optionresponse', 'checkboxgroup', 'optioninput', 
                          'choiceresponse', 'video', 'choicegroup', 'vertical', 'choice', 'sequential', 
                          'multiplechoiceresponse', 'numericalresponse', 'chapter', 'solution', 'img', 
                          'formulaequationinput', 'responseparam', 'selfassessment', 'track', 'task', 'rubric', 
                          'stringresponse', 'combinedopenended', 'description', 'textline', 'prompt', 'category', 
                          'option', 'lti', 'annotationresponse', 
                          'annotatable', 'colgroup', 'tag_prompt', 'comment', 'annotationinput', 'image', 
                          'options', 'comment_prompt', 'conditional', 
                          'answer', 'poll_question', 'section', 'wrapper', 'map', 'area', 
                          'customtag', 'transcript', 
                          'split_test', 'word_cloud', 
                          'openended', 'openendedparam', 'answer_display', 'code', 
                          'drag_and_drop_input', 'customresponse', 'draggable', 'mentoring', 
                          'textannotation', 'imageannotation', 'videosequence', 
                          'feedbackprompt', 'assessments', 'openassessment', 'assessment', 'explanation', 'criterion']

        for entry in bqdat['data']:
            thekey = make_key(entry['key'])
            # if thekey.startswith('count_') and thekey[6:] not in COUNTS_TO_KEEP:
            #     continue
            if thekey.startswith('listings_') and thekey[9:] not in listings_keys:
                # print "dropping key=%s for course_id=%s" % (thekey, entry['course_id'])
                continue
            c_sum_stats[entry['course_id']][thekey] = entry['value']
            #if 'certifies' in thekey:
            #    print "course_id=%s, key=%s, value=%s" % (entry['course_id'], thekey, entry['value'])
            if thekey not in css_keys:
                css_keys.append(thekey)

        # compute forum_posts_per_week
        for course_id, entry in c_sum_stats.items():
            nfps = entry.get('nforum_posts_sum', 0)
            if nfps:
                fppw = int(nfps) / float(entry['nweeks'])
                entry['nforum_posts_per_week'] = fppw
                print "    course: %s, assessments_per_week=%s, forum_posts_per_week=%s" % (course_id, entry['total_assessments_per_week'], fppw)
            else:
                entry['nforum_posts_per_week'] = None
        css_keys.append('nforum_posts_per_week')

        # read in listings file and merge that in also
        if listings_file:
            if listings_file.endswith('.csv'):
                listings = csv.DictReader(open(listings_file))
            else:
                listings = [ json.loads(x) for x in open(listings_file) ]
            for entry in listings:
                course_id = entry['course_id']
                if course_id not in c_sum_stats:
                    continue
                cmci = c_sum_stats[course_id]
                for field, value in entry.items():
                    lkey = "listings_%s" % make_key(field)
                    if not (lkey in cmci) or (not cmci[lkey]):
                        cmci[lkey] = value

        print "Storing these fields: %s" % css_keys

        # get schema
        mypath = os.path.dirname(os.path.realpath(__file__))
        the_schema = json.loads(open('%s/schemas/schema_combined_course_summary_stats.json' % mypath).read())
        schema_dict = { x['name'] : x for x in the_schema }

        # write out CSV
        css_table = "course_summary_stats"
        ofn = "%s__%s.csv" % (dataset, css_table)
        ofn2 = "%s__%s.json" % (dataset, css_table)
        print "Writing data to %s and %s" % (ofn, ofn2)

        ofp = open(ofn, 'w')
        ofp2 = open(ofn2, 'w')
        dw = csv.DictWriter(ofp, fieldnames=css_keys)
        dw.writeheader()
        for cid, entry in c_sum_stats.items():
            for ek in entry:
                if ek not in schema_dict:
                    entry.pop(ek)
                # entry[ek] = str(entry[ek])	# coerce to be string
            ofp2.write(json.dumps(entry) + "\n")
            for key in css_keys:
                if key not in entry:
                    entry[key] = None
            dw.writerow(entry)
        ofp.close()
        ofp2.close()

        # upload to bigquery
        # the_schema = [ { 'type': 'STRING', 'name': x } for x in css_keys ]
        if 1:
            gsfnp = gspath / dataset / (css_table + ".json")
            gsutil.upload_file_to_gs(ofn2, gsfnp)
            # bqutil.load_data_to_table(dataset, css_table, gsfnp, the_schema, wait=True, verbose=False,
            #                           format='csv', skiprows=1)
            bqutil.load_data_to_table(dataset, css_table, gsfnp, the_schema, wait=True, verbose=False)

        return

    
    print "-"*60 + " %s" % course_id

    # get nweeks from listings
    lfn = path(listings_file)
    if not lfn.exists():
        print "[analyze_content] course listings file %s doesn't exist!" % lfn
        return

    data = None
    if listings_file.endswith('.json'):
        data_feed = map(json.loads, open(lfn))
    else:
        data_feed = csv.DictReader(open(lfn))
    for k in data_feed:
        if not 'course_id' in k:
            print "Strange course listings row, no course_id in %s" % k
            raise Exception("Missing course_id")
        if k['course_id']==course_id:
            data = k
            break

    if not data:
        print "[analyze_content] no entry for %s found in course listings file %s!" % (course_id, lfn)
        return

    def date_parse(field):
        (m, d, y) = map(int, data[field].split('/'))
        return datetime.datetime(y, m, d)

    launch = date_parse('Course Launch')
    wrap = date_parse('Course Wrap')
    ndays = (wrap - launch).days
    nweeks = ndays / 7.0

    print "Course length = %6.2f weeks (%d days)" % (nweeks, ndays)

    course_dir = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest)
    cfn = gsutil.path_from_course_id(course_id)

    xbfn = course_dir / ("xbundle_%s.xml" % cfn)
    
    if not xbfn.exists():
        print "[analyze_content] cannot find xbundle file %s for %s!" % (xbfn, course_id)
        return

    print "[analyze_content] For %s using %s" % (course_id, xbfn)
    
    # get module usage data
    mudata = get_stats_module_usage(course_id, basedir, datedir, use_dataset_latest)

    xml = etree.parse(open(xbfn)).getroot()
    
    counts = defaultdict(int)
    nexcluded = defaultdict(int)

    IGNORE = ['html', 'p', 'div', 'iframe', 'ol', 'li', 'ul', 'blockquote', 'h1', 'em', 'b', 'h2', 'h3', 'body', 'span', 'strong',
              'a', 'sub', 'strike', 'table', 'td', 'tr', 's', 'tbody', 'sup', 'sub', 'strike', 'i', 's', 'pre', 'policy', 'metadata',
              'grading_policy', 'br', 'center',  'wiki', 'course', 'font', 'tt', 'it', 'dl', 'startouttext', 'endouttext', 'h4', 
              'head', 'source', 'dt', 'hr', 'u', 'style', 'dd', 'script', 'th', 'p', 'P', 'TABLE', 'TD', 'small', 'text', 'title']

    problem_stats = defaultdict(int)

    def does_problem_have_random_script(problem):
        '''
        return 1 if problem has a script with "random." in it
        else return 0
        '''
        for elem in problem.findall('.//script'):
            if elem.text and ('random.' in elem.text):
                return 1
        return 0

    # walk through xbundle 
    def walk_tree(elem, policy=None):
        '''
        Walk XML tree recursively.
        elem = current element
        policy = dict of attributes for children to inherit, with fields like due, graded, showanswer
        '''
        policy = policy or {}
        if  type(elem.tag)==str and (elem.tag.lower() not in IGNORE):
            counts[elem.tag.lower()] += 1
        if elem.tag in ["sequential", "problem"]:
            keys = ["due", "graded", "format", "showanswer"]
            for k in keys:		# copy inheritable attributes, if they are specified
                val = elem.get(k)
                if val:
                    policy[k] = val
        if elem.tag=="problem":	# accumulate statistics about problems: how many have show_answer = [past_due, closed] ?  have random. in script?
            problem_stats['n_capa_problems'] += 1
            if policy.get('showanswer'):
                problem_stats["n_showanswer_%s" % policy.get('showanswer')] += 1
            problem_stats['n_random_script'] += does_problem_have_random_script(elem)
            
        for k in elem:
            midfrag = (k.tag, k.get('url_name_orig', None))
            if (midfrag in mudata) and int(mudata[midfrag]['ncount']) < 20:
                nexcluded[k.tag] += 1
                if verbose:
                    print "    -> excluding %s (%s), ncount=%s" % (k.get('display_name', '<no_display_name>').encode('utf8'), 
                                                                   midfrag, 
                                                                   mudata.get(midfrag, {}).get('ncount'))
                continue
            walk_tree(k, policy)

    walk_tree(xml)
    print "--> Count of individual element tags throughout XML: ", counts
    
    print "--> problem_stats:", json.dumps(problem_stats, indent=4)

    # combine some into "qual_axis" and others into "quant_axis"
    qual_axis = ['openassessment', 'optionresponse', 'multiplechoiceresponse', 
                 # 'discussion', 
                 'choiceresponse', 'word_cloud', 
                 'combinedopenended', 'choiceresponse', 'stringresponse', 'textannotation', 'openended', 'lti']
    quant_axis = ['formularesponse', 'numericalresponse', 'customresponse', 'symbolicresponse', 'coderesponse',
                  'imageresponse']

    nqual = 0
    nquant = 0
    for tag, count in counts.items():
        if tag in qual_axis:
            nqual += count
        if tag in quant_axis:
            nquant += count
    
    print "nqual=%d, nquant=%d" % (nqual, nquant)

    nqual_per_week = nqual / nweeks
    nquant_per_week = nquant / nweeks
    total_per_week = nqual_per_week + nquant_per_week

    print "per week: nqual=%6.2f, nquant=%6.2f total=%6.2f" % (nqual_per_week, nquant_per_week, total_per_week)

    # save this overall data in CCDATA
    lock_file(CCDATA)
    ccdfn = path(CCDATA)
    ccd = {}
    if ccdfn.exists():
        for k in csv.DictReader(open(ccdfn)):
            ccd[k['course_id']] = k
    
    ccd[course_id] = {'course_id': course_id,
                      'nweeks': nweeks,
                      'nqual_per_week': nqual_per_week,
                      'nquant_per_week': nquant_per_week,
                      'total_assessments_per_week' : total_per_week,
                      }

    # fields = ccd[ccd.keys()[0]].keys()
    fields = ['course_id', 'nquant_per_week', 'total_assessments_per_week', 'nqual_per_week', 'nweeks']
    cfp = open(ccdfn, 'w')
    dw = csv.DictWriter(cfp, fieldnames=fields)
    dw.writeheader()
    for cid, entry in ccd.items():
        dw.writerow(entry)
    cfp.close()
    lock_file(CCDATA, release=True)

    # store data in course_metainfo table, which has one (course_id, key, value) on each line
    # keys include nweeks, nqual, nquant, count_* for module types *

    cmfields = OrderedDict()
    cmfields['course_id'] = course_id
    cmfields['course_length_days'] = str(ndays)
    cmfields.update({ make_key('listings_%s' % key) : value for key, value in data.items() })	# from course listings
    cmfields.update(ccd[course_id].copy())

    # cmfields.update({ ('count_%s' % key) : str(value) for key, value in counts.items() })	# from content counts

    for key in sorted(counts):	# store counts in sorted order, so that the later generated CSV file can have a predictable structure
        value = counts[key]
        cmfields['count_%s' % key] =  str(value) 	# from content counts

    for key in sorted(problem_stats):	# store problem stats
        value = problem_stats[key]
        cmfields['problem_stat_%s' % key] =  str(value)

    cmfields.update({ ('nexcluded_sub_20_%s' % key) : str(value) for key, value in nexcluded.items() })	# from content counts

    course_dir = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest)
    csvfn = course_dir / CMINFO

    # manual overriding of the automatically computed fields can be done by storing course_id,key,value data
    # in the CMINFO_OVERRIDES file

    csvfn_overrides = course_dir / CMINFO_OVERRIDES
    if csvfn_overrides.exists():
        print "--> Loading manual override information from %s" % csvfn_overrides
        for ovent in csv.DictReader(open(csvfn_overrides)):
            if not ovent['course_id']==course_id:
                print "===> ERROR! override file has entry with wrong course_id: %s" % ovent
                continue
            print "    overriding key=%s with value=%s" % (ovent['key'], ovent['value'])
            cmfields[ovent['key']] = ovent['value']

    print "--> Course metainfo writing to %s" % csvfn

    fp = open(csvfn, 'w')

    cdw = csv.DictWriter(fp, fieldnames=['course_id', 'key', 'value'])
    cdw.writeheader()

    for k, v in cmfields.items():
        cdw.writerow({'course_id': course_id, 'key': k, 'value': v})
        
    fp.close()

    # build and output course_listings_and_metainfo 

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    mypath = os.path.dirname(os.path.realpath(__file__))
    clm_table = "course_listing_and_metainfo"
    clm_schema_file = '%s/schemas/schema_%s.json' % (mypath, clm_table)
    clm_schema = json.loads(open(clm_schema_file).read())

    clm = {}
    for finfo in clm_schema:
        field = finfo['name']
        clm[field] = cmfields.get(field)
    clm_fnb = clm_table + ".json"
    clm_fn = course_dir / clm_fnb
    open(clm_fn, 'w').write(json.dumps(clm))

    gsfnp = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest) / clm_fnb
    print "--> Course listing + metainfo uploading to %s then to %s.%s" % (gsfnp, dataset, clm_table)
    sys.stdout.flush()
    gsutil.upload_file_to_gs(clm_fn, gsfnp)
    bqutil.load_data_to_table(dataset, clm_table, gsfnp, clm_schema, wait=True, verbose=False)

    # output course_metainfo

    table = 'course_metainfo'
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    gsfnp = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest) / CMINFO
    print "--> Course metainfo uploading to %s then to %s.%s" % (gsfnp, dataset, table)
    sys.stdout.flush()

    gsutil.upload_file_to_gs(csvfn, gsfnp)

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_course_metainfo.json' % mypath
    the_schema = json.loads(open(SCHEMA_FILE).read())[table]

    bqutil.load_data_to_table(dataset, table, gsfnp, the_schema, wait=True, verbose=False, format='csv', skiprows=1)

