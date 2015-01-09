#!/usr/bin/python

import os
import csv
import gsutil
import bqutil
import json
import datetime

from collections import defaultdict, OrderedDict
from lxml import etree
from path import path
from load_course_sql import find_course_sql_dir

CCDATA = "course_content.csv"
CMINFO = "course_metainfo.csv"
CMINFO_OVERRIDES = "course_metainfo_overrides.csv"

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

    if do_upload then upload all accumulated data to the course report dataset as the "stats_course_content" table.
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

        if 0:
            bqutil.create_bq_table(dataset, table, sql)


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
                    ncertified / nregistered * 100 as pct_certified,
                    ncertified_and_registered_before_launch / nregistered_before_launch * 100 as pct_certified_reg_before_launch,
                    ncertified_and_registered_during_course / nregistered_during_course * 100 as pct_certified_reg_during_course,
                    ncertified / nregistered_by_wrap * 100 as pct_certified_by_wrap,
                FROM
                (
                # ------------------------
                # get aggregate data
                SELECT pc.course_id as course_id, 
                    cminfo.wrap_date as wrap_date,
                    count(*) as nregistered,
                    sum(case when pc.certified then 1 else 0 end) ncertified,
                    sum(case when pc.viewed then 1 else 0 end) nviewed,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) nregistered_by_wrap,
                    sum(case when pc.start_time < cminfo.wrap_date then 1 else 0 end) / nregistered * 100 nregistered_by_wrap_pct,
                    sum(case when (pc.start_time < cminfo.wrap_date) and pc.viewed then 1 else 0 end) nviewed_by_wrap,
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
                
                #  get course launch and wrap dates from course_metainfo
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
                # end course_metainfo subquery
                
                ) as cminfo
                on pc.course_id = cminfo.course_id
                
                group by course_id, wrap_date
                order by course_id
                # ---- end get aggregate data
)
        """.format(dataset=dataset, person_course=latest_person_course)

        cert_by_reg = bqutil.get_bq_table(dataset, 'stats_cert_rates_by_registration', sql=sql, key={'name': 'course_id'})

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
            ncert = float(cmci['certified_sum'])
            if ncert:
                cmci['certified_of_nregistered_by_wrap_pct'] = nbw / ncert * 100.0
            else:
                cmci['certified_of_nregistered_by_wrap_pct'] = None
            cbr = cert_by_reg['data_by_key'][course_id]
            for field, value in cbr.items():
                cmci['cbr_%s' % field] = value

        css_keys = c_sum_stats.values()[0].keys()

        # retrieve course_metainfo table, pivot, add that to summary_stats

        bqdat = bqutil.get_table_data(dataset, table)

        def make_key(key):
            return key.replace(' ', '_').replace("'", "_").replace('/', '_')

        listings_keys = map(make_key, ["Institution", "Semester", "New or Rerun", "Andrew Recodes New/Rerun", 
                                       "Course Number", "Short Title", "Andrew's Short Titles", "Title", 
                                       "Instructors", "Registration Open", "Course Launch", "Course Wrap", "course_id"])
        listings_keys.reverse()
        
        for lk in listings_keys:
            css_keys.insert(1, "listings_%s" % lk)

        for entry in bqdat['data']:
            thekey = make_key(entry['key'])
            c_sum_stats[entry['course_id']][thekey] = entry['value']
            if thekey not in css_keys:
                css_keys.append(thekey)

        # compute forum_posts_per_week
        for course_id, entry in c_sum_stats.items():
            nfps = entry['nforum_posts_sum']
            if nfps:
                fppw = int(nfps) / float(entry['nweeks'])
                entry['nforum_posts_per_week'] = fppw
                print "    course: %s, assessments_per_week=%s, forum_posts_per_week=%s" % (course_id, entry['total_assessments_per_week'], fppw)
            else:
                entry['nforum_posts_per_week'] = None
        css_keys.append('nforum_posts_per_week')

        print "Storing these fields: %s" % css_keys

        # write out CSV
        css_table = "course_summary_stats"
        ofn = "%s__%s.csv" % (dataset, css_table)
        print "Writing data to %s" % ofn

        ofp = open(ofn, 'w')
        dw = csv.DictWriter(ofp, fieldnames=css_keys)
        dw.writeheader()
        for cid, entry in c_sum_stats.items():
            for key in css_keys:
                if key not in entry:
                    entry[key] = None
            dw.writerow(entry)
        ofp.close()

        # upload to bigquery
        the_schema = [ { 'type': 'STRING', 'name': x } for x in css_keys ]
        if 1:
            gsfnp = gspath / dataset / (css_table + ".csv")
            gsutil.upload_file_to_gs(ofn, gsfnp)
            bqutil.load_data_to_table(dataset, css_table, gsfnp, the_schema, wait=True, verbose=False,
                                      format='csv', skiprows=1)

        return

    
    print "-"*60 + " %s" % course_id

    # get nweeks from listings
    lfn = path(listings_file)
    if not lfn.exists():
        print "[analyze_content] course listings file %s doesn't exist!" % lfn
        return

    data = None
    for k in csv.DictReader(open(lfn)):
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

    def walk_tree(elem):
        if  type(elem.tag)==str and (elem.tag.lower() not in IGNORE):
            counts[elem.tag.lower()] += 1
        for k in elem:
            midfrag = (k.tag, k.get('url_name_orig', None))
            if (midfrag in mudata) and int(mudata[midfrag]['ncount']) < 20:
                nexcluded[k.tag] += 1
                if verbose:
                    print "    -> excluding %s (%s), ncount=%s" % (k.get('display_name', '<no_display_name>').encode('utf8'), 
                                                                   midfrag, 
                                                                   mudata.get(midfrag, {}).get('ncount'))
                continue
            walk_tree(k)

    walk_tree(xml)
    print counts

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

    # store data in course_metainfo table, which has one (course_id, key, value) on each line
    # keys include nweeks, nqual, nquant, count_* for module types *

    cmfields = OrderedDict()
    cmfields['course_id'] = course_id
    cmfields['course_length_days'] = str(ndays)
    cmfields.update({ ('listings_%s' % key) : value for key, value in data.items() })	# from course listings
    cmfields.update(ccd[course_id].copy())
    cmfields.update({ ('count_%s' % key) : str(value) for key, value in counts.items() })	# from content counts
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

    table = 'course_metainfo'
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    gsfnp = gsutil.gs_path_from_course_id(course_id, use_dataset_latest=use_dataset_latest) / CMINFO
    print "--> Course metainfo uploading to %s then to %s.%s" % (gsfnp, dataset, table)

    gsutil.upload_file_to_gs(csvfn, gsfnp)

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_course_metainfo.json' % mypath
    the_schema = json.loads(open(SCHEMA_FILE).read())[table]

    bqutil.load_data_to_table(dataset, table, gsfnp, the_schema, wait=True, verbose=False, format='csv', skiprows=1)

