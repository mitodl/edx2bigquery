#!/usr/bin/python
#
# File:   make_course_report_tables.py
# Date:   18-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# compute tables for annual course report

import sys
import bqutil
import gsutil
import datetime
import json

class CourseReport(object):
    def __init__(self, course_id_set, output_project_id=None, nskip=0, 
                 output_dataset_id=None, 
                 output_bucket=None,
                 use_dataset_latest=False,
                 ):
        
        org = course_id_set[0].split('/',1)[0]	# extract org from first course_id

        self.output_project_id = output_project_id

        crname = ('course_report_%s' % org)
        if use_dataset_latest:
            crname = 'course_report_latest'
        self.dataset = output_dataset_id or crname

        self.gsbucket = gsutil.gs_path_from_course_id(crname, gsbucket=output_bucket)

        course_datasets = [ bqutil.course_id2dataset(x, use_dataset_latest=use_dataset_latest) for x in course_id_set]

        # check to see which datasets have person_course tables
        datasets_with_pc = []
        self.all_pc_tables = {}
        self.all_pcday_ip_counts_tables = {}
        for cd in course_datasets:
            try:
                table = bqutil.get_bq_table_info(cd, 'person_course')
            except Exception as err:
                print "[make-course_report_tables] Err: %s" % str(err)
                continue
            if table is None:
                continue
            self.all_pc_tables[cd] = table
            datasets_with_pc.append(cd)

            try:
                table = bqutil.get_bq_table_info(cd, 'pcday_ip_counts')
            except Exception as err:
                continue
            if table is None:
                continue
            self.all_pcday_ip_counts_tables[cd] = table

        pc_tables = ',\n'.join(['[%s.person_course]' % x for x in datasets_with_pc])
        pcday_ip_counts_tables = ',\n'.join(['[%s.pcday_ip_counts]' % x for x in self.all_pcday_ip_counts_tables])

        self.parameters = {'dataset': self.dataset,
                           'pc_tables': pc_tables,
                           'pcday_ip_counts_tables': pcday_ip_counts_tables,
                           }
        print "[make_course_report_tables] ==> Using these datasets (with person_course tables): %s" % datasets_with_pc

        self.course_datasets = course_datasets
    
        print "="*100
        print "Generating course report tables -> dataset=%s, project=%s" % (self.dataset, self.output_project_id)
        sys.stdout.flush()

        bqutil.create_dataset_if_nonexistent(self.dataset, project_id=output_project_id)

        self.nskip = nskip
        self.make_global_modal_ip_table()
        self.make_enrollment_by_day()
        self.make_totals_by_course()
        self.make_total_populations_by_course()
        self.make_table_of_n_courses_registered()
        self.make_geographic_distributions()
        self.make_overall_totals()
    
        print "="*100
        print "Done with course report tables"
        sys.stdout.flush()

    def do_table(self, the_sql, tablename, the_dataset=None):
        if self.nskip:
            self.nskip += -1
            print "Skipping %s" % tablename
            return

        if the_dataset is None:
            the_dataset = self.dataset

        print("Computing %s in BigQuery" % tablename)
        ret = bqutil.create_bq_table(the_dataset, tablename, the_sql, 
                                     overwrite=True,
                                     output_project_id=self.output_project_id)
        gsfn = "%s/%s.csv" % (self.gsbucket, tablename)
        bqutil.extract_table_to_gs(the_dataset, tablename, gsfn, 
                                   format='csv', 
                                   do_gzip=False,
                                   wait=False)

        msg = "CSV download link: %s" % gsutil.gs_download_link(gsfn)
        print msg
        bqutil.add_description_to_table(the_dataset, tablename, msg, append=True, project_id=self.output_project_id)


    def make_totals_by_course(self):

        the_sql = '''
            SELECT course_id,
                   count(*) as registered_sum,
                   sum(case when viewed then 1 else 0 end) as viewed_sum,
                   sum(case when explored then 1 else 0 end) as explored_sum,
                   sum(case when certified then 1 else 0 end) as certified_sum,
    
                   sum(case when gender='m' then 1 else 0 end) as n_male,
                   sum(case when gender='f' then 1 else 0 end) as n_female,
    
                   sum(case when mode="verified" then 1 else 0 end) as n_verified_id,
                   sum(case when (viewed and mode="verified") then 1 else 0 end) as verified_viewed,
                   sum(case when (explored and mode="verified") then 1 else 0 end) as verified_explored,
                   sum(case when (certified and mode="verified") then 1 else 0 end) as verified_certified,
                   avg(case when (mode="verified") then grade else null end) as verified_avg_grade,
    
                   sum(case when (gender='m' and mode="verified") then 1 else 0 end) as verified_n_male,
                   sum(case when (gender='f' and mode="verified") then 1 else 0 end) as verified_n_female,
    
                   sum(nplay_video) as nplay_video_sum,
                   avg(nchapters) as nchapters_avg,
                   sum(ndays_act) as ndays_act_sum,
                   sum(nevents) as nevents_sum,
                   sum(nforum_posts) as nforum_posts_sum,
                   min(case when certified then grade else null end) as min_gade_certified,
                   min(start_time) as min_start_time,
                   max(last_event) as max_last_event,
                   max(nchapters) as max_nchapters,
                   sum(nforum_votes) as nforum_votes_sum,
                   sum(nforum_endorsed) as nforum_endorsed_sum,
                   sum(nforum_threads) as nforum_threads_sum,
                   sum(nforum_comments) as nforum_commments_sum,
                   sum(nforum_pinned) as nforum_pinned_sum,
    
                   avg(nprogcheck) as nprogcheck_avg,
                   avg(case when certified then nprogcheck else null end) as certified_nprogcheck,
                   avg(case when (mode="verified") then nprogcheck else null end) as verified_nprogcheck,
    
                   sum(nshow_answer) as nshow_answer_sum,
                   sum(nseq_goto) as nseq_goto_sum,
                   sum(npause_video) as npause_video_sum,
                   avg(avg_dt) as avg_of_avg_dt,
                   avg(sum_dt) as avg_of_sum_dt,
                   avg(case when certified then avg_dt else null end) as certified_avg_dt,
                   avg(case when certified then sum_dt else null end) as certified_sum_dt,
                   sum(case when (ip is not null) then 1 else 0 end) as n_have_ip,
                   sum(case when ((ip is not null) and (cc_by_ip is null)) then 1 else 0 end) as n_missing_cc,
            FROM 
                {pc_tables}
            group by course_id
            order by course_id;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'broad_stats_by_course')

    def make_enrollment_by_day(self):

        # check creation dates on all the person course tables
        recent_pc_tables = []
        old_pc_tables = []
        for cd, table in self.all_pc_tables.items():
            try:
                cdt = table['lastModifiedTime']
            except Exception as err:
                print "Error getting %s person_course table creation time, table info = %s" % (cd, table)
                raise
            # print "--> %s: %s" % (cd, cdt)
            if cdt > datetime.datetime(2014, 11, 8):
                recent_pc_tables.append('[%s.person_course]' % cd)
            else:
                old_pc_tables.append('[%s.person_course]' % cd)

        if not recent_pc_tables:
            msg = "[make_course_report_tables] ==> enrollday_sql ERROR! no recent person_course tables found!"
            print msg
            raise Exception(msg)
            
        the_pc_tables = ',\n'.join(recent_pc_tables)
        print "[make_course_report_tables] ==> enrollday_sql being computed on these tables: %s" % the_pc_tables
        print "==> old person_course tables which should be updated: %s" % json.dumps(old_pc_tables, indent=4)

        the_sql = '''
            SELECT course_id,
                 date, 
                 SUM(registered) as nregistered_ever,
                 SUM(un_registered) as n_unregistered,
                 -SUM(un_registered) + nregistered_ever as nregistered_net,
                 SUM(nregistered_ever) over (partition by course_id order by date) as nregistered_ever_cum,
                 SUM(nregistered_net) over (partition by course_id order by date) as nregistered_net_cum,
               
                 SUM(verified) as nverified_ever,
                 SUM(verified_un_registered) as nverified_un_registered,
                 -SUM(verified_un_registered) + nverified_ever as nverified_net,
                 SUM(nverified_ever) over (partition by course_id order by date) as nverified_ever_cum,
                 SUM(nverified_net) over (partition by course_id order by date) as nverified_net_cum,
               
               FROM (
                   SELECT  
                     course_id,
                     date(last_event) as date,
                     INTEGER(0) as registered,
                     INTEGER(count(*)) un_registered,
                     INTEGER(0) as verified,
                     INTEGER(sum(case when mode = "verified" then 1 else 0 end)) as verified_un_registered,
                   FROM {pc_tables}
                   where is_active = 0
                   and  last_event is not null
                   group by date, course_id
                   order by date, course_id
                 ),(
                   SELECT  
                     course_id,
                     date(start_time) as date,
                     INTEGER(count(*)) registered,
                     INTEGER(0) as un_registered,
                     INTEGER(sum(case when mode = "verified" then 1 else 0 end)) as verified,
                     INTEGER(0) as verified_un_registered,
                   FROM {pc_tables}
                   where start_time is not null
                   group by date, course_id
                   order by date, course_id
                 )
               group by date, course_id
               order by date, course_id;
        '''.format(pc_tables = the_pc_tables)
        
        self.do_table(the_sql, 'enrollday_sql')
    
    def make_table_of_n_courses_registered(self):
        the_sql = '''
            SELECT user_id, count(*) as n_courses_registered
            FROM 
                {pc_tables}
            group by user_id
            order by n_courses_registered desc;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'multi_registrations')
    
    def make_geographic_distributions(self):
        the_sql = '''
            SELECT cc_by_ip as cc,
                countryLabel,
                sum(case when certified then 1 else 0 end) as ncertified,
                sum(case when registered then 1 else 0 end) as nregistered,
                sum(case when viewed then 1 else 0 end) as nviewed,
                sum(case when explored then 1 else 0 end) as nexplored,
                sum(case when (mode="verified") then 1 else 0 end) as nverified,
                sum(case when (mode="verified") and certified then 1 else 0 end) as ncert_verified,
                avg(case when certified then avg_dt else null end) as avg_certified_dt,
            FROM {pc_tables}
            group by cc, countryLabel
            #order by countryLabel
            order by nverified desc
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'geographic_distributions')
        
    def make_global_modal_ip_table(self):
        '''
        Make table of global modal ip addresses, based on each course's IP address counts, found in 
        individual course's pcday_ip_counts tables.
        '''
        the_sql = '''
              SELECT username, IP as modal_ip, ip_count, n_different_ip,
              FROM
                  ( SELECT username, ip, ip_count,
                          RANK() over (partition by username order by ip_count ASC) n_different_ip,
                          RANK() over (partition by username order by ip_count DESC) rank,
                    from ( select username, ip, sum(ipcount) as ip_count
                           from {pcday_ip_counts_tables}
                           GROUP BY username, ip
                    )
                  )
                  where rank=1
                  order by username
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'global_modal_ip', the_dataset='courses')

    def make_overall_totals(self):
        the_sql = '''

            select * from (
                SELECT sum(registered_sum) as registered,
                               sum(viewed_sum) as viewed,
                               sum(explored_sum) as explored,
                               sum(certified_sum) as certified,
                
                               sum(n_verified_id) as verified_id,
                               sum(verified_viewed) as verified_viewed,
                               sum(verified_explored) as verified_explored,
                               sum(verified_certified) as verified_certified,

                               sum(verified_certified)/sum(n_verified_id)*100.0 as verified_certification_pct,
                
                               avg(certified_nprogcheck / nprogcheck_avg) as avg_nprogcheck_ratio_cert_vs_all,
                      
                               sum(n_missing_cc) as n_missing_cc,
                        FROM 
                            [{dataset}.broad_stats_by_course]
            ) total cross join (
                        select count(*) as n_unique_registrants, 
                               sum(n_courses_registered) as course_registrations
                        from [{dataset}.multi_registrations]
            ) multi
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'overall_stats')
        
    def make_total_populations_by_course(self):
        the_sql = '''
            SELECT course_id,
                   count(*) as registered_sum,
                   sum(case when viewed then 1 else 0 end) as viewed_sum,
                   sum(case when explored then 1 else 0 end) as explored_sum,
                   sum(case when certified then 1 else 0 end) as certified_sum,
                   sum(case when (ip is not null) then 1 else 0 end) as n_have_ip,
                   sum(case when ((ip is not null) and (cc_by_ip is null)) then 1 else 0 end) as n_missing_cc,
            FROM 
                {pc_tables}
            group by course_id
            order by course_id;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'populations_stats_by_course')
        
