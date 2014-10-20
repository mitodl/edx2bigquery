#!/usr/bin/python
#
# File:   make_course_report_tables.py
# Date:   18-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# compute tables for annual course report

import bqutil
import gsutil

class CourseReport(object):
    def __init__(self, course_id_set, output_project_id=None, nskip=0, output_dataset_id=None, output_bucket=None):
        
        org = course_id_set[0].split('/',1)[0]	# extract org from first course_id

        self.gsbucket = gsutil.gs_path_from_course_id('course_report_%s' % org, gsbucket=output_bucket)
        self.output_project_id = output_project_id

        self.dataset = output_dataset_id or ('course_report_%s' % org)
        course_datasets = [ bqutil.course_id2dataset(x) for x in course_id_set]
        pc_tables = ',\n'.join(['[%s.person_course]' % x for x in course_datasets])

        self.parameters = {'dataset': self.dataset,
                           'pc_tables': pc_tables,
                           }
    
        print "="*77
        print "Generating course report tables -> dataset=%s, project=%s" % (self.dataset, self.output_project_id)

        bqutil.create_dataset_if_nonexistent(self.dataset, project_id=output_project_id)

        self.nskip = nskip
        self.make_totals_by_course()
        self.make_total_populations_by_course()
        self.make_table_of_n_courses_registered()
        self.make_overall_totals()
    
    def do_table(self, the_sql, tablename):
        if self.nskip:
            self.nskip += -1
            print "Skipping %s" % tablename
            return

        print("Computing %s in BigQuery" % tablename)
        ret = bqutil.create_bq_table(self.dataset, tablename, the_sql, output_project_id=self.output_project_id)
        gsfn = "%s/%s.csv" % (self.gsbucket, tablename)
        bqutil.extract_table_to_gs(self.dataset, tablename, gsfn, 
                                   format='csv', 
                                   do_gzip=False,
                                   wait=False)

        msg = "CSV download link: %s" % gsutil.gs_download_link(gsfn)
        print msg
        bqutil.add_description_to_table(self.dataset, tablename, msg, append=True, project_id=self.output_project_id)


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
    
    def make_table_of_n_courses_registered(self):
        the_sql = '''
            SELECT user_id, count(*) as n_courses_registered
            FROM 
                {pc_tables}
            group by user_id
            order by n_courses_registered desc;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'multi_registrations')
        
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
        
