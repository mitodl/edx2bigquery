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
from collections import OrderedDict
from collections import defaultdict

class CourseReport(object):
    def __init__(self, course_id_set, output_project_id=None, nskip=0, 
                 output_dataset_id=None, 
                 output_bucket=None,
                 use_dataset_latest=False,
                 only_step=None,
                 end_date=None,
                 ):
        '''
        Compute course report tables, based on combination of all person_course and other individual course tables.

        only_step: specify a single course report step to be executed; runs all reports, if None
        '''
        
        if only_step and ',' in only_step:
            only_step = only_step.split(',')
        self.only_step = only_step

        self.end_date = end_date;

        if not course_id_set:
            print "ERROR! Must specify list of course_id's for report.  Aborting."
            return

        org = course_id_set[0].split('/',1)[0]	# extract org from first course_id
        self.org = org

        self.output_project_id = output_project_id

        crname = ('course_report_%s' % org)
        if use_dataset_latest:
            crname = 'course_report_latest'
        self.dataset = output_dataset_id or crname

        self.gsbucket = gsutil.gs_path_from_course_id(crname, gsbucket=output_bucket)
        self.course_id_set = course_id_set

        course_datasets = [ bqutil.course_id2dataset(x, use_dataset_latest=use_dataset_latest) for x in course_id_set]

        # check to see which datasets have person_course tables
        datasets_with_pc = []
        self.all_pc_tables = OrderedDict()
        self.all_pcday_ip_counts_tables = OrderedDict()
        self.all_pcday_trlang_counts_tables = OrderedDict()
        self.all_uic_tables = OrderedDict()
        self.all_ca_tables = OrderedDict()
        self.all_va_tables = OrderedDict()
        self.all_tott_tables = OrderedDict()
        for cd in course_datasets:
            try:
                table = bqutil.get_bq_table_info(cd, 'person_course')
            except Exception as err:
                print "[make-course_report_tables] Err: %s" % str(err)
                table = None
            if table is not None:
                self.all_pc_tables[cd] = table
                datasets_with_pc.append(cd)

            try:
                table = bqutil.get_bq_table_info(cd, 'course_axis')
            except Exception as err:
                print "[make_course_axis_tables] Err: %s" % str(err)
                table = None
            if table is not None:
                self.all_ca_tables[cd] = table

            try:
                table = bqutil.get_bq_table_info(cd, 'video_axis')
            except Exception as err:
                print "[make_video_axis_tables] Err: %s" % str(err)
                table = None
            if table is not None:
                self.all_va_tables[cd] = table

            try:
                table = bqutil.get_bq_table_info(cd, 'pcday_ip_counts')
            except Exception as err:
                table = None
            if table is not None:
                self.all_pcday_ip_counts_tables[cd] = table

            try:
                table = bqutil.get_bq_table_info(cd, 'pcday_trlang_counts')
            except Exception as err:
                table = None
            if table is not None:
                self.all_pcday_trlang_counts_tables[cd] = table

            try:
                table = bqutil.get_bq_table_info(cd, 'user_info_combo')
            except Exception as err:
                table = None
            if table is not None:
                self.all_uic_tables[cd] = table

            try:
                table = bqutil.get_bq_table_info(cd, 'time_on_task_totals')
            except Exception as err:
                print "[make-course_report_tables] Err: %s" % str(err)
                table = None
            if table is not None:
                self.all_tott_tables[cd] = table

        pc_tables = ',\n'.join(['[%s.person_course]' % x for x in datasets_with_pc])
        pcday_tables = ',\n'.join(['[%s.person_course_day]' % x for x in datasets_with_pc])
        pcday_ip_counts_tables = ',\n'.join(['[%s.pcday_ip_counts]' % x for x in self.all_pcday_ip_counts_tables])
        pcday_trlang_counts_tables = ',\n'.join(['[%s.pcday_trlang_counts]' % x for x in self.all_pcday_trlang_counts_tables])
        uic_tables = ',\n'.join(['[%s.user_info_combo]' % x for x in self.all_uic_tables])
        ca_tables = ',\n'.join(['[%s.course_axis]' % x for x in self.all_ca_tables])
        va_tables = ',\n'.join(['[%s.video_axis]' % x for x in self.all_va_tables])
        tott_tables = ',\n'.join(['[%s.time_on_task_totals]' % x for x in self.all_tott_tables])

        print "%d time_on_task tables: %s" % (len(self.all_tott_tables), tott_tables)
        sys.stdout.flush()

        # find latest combined person_course table
        cpc_tables = [ x for x in bqutil.get_list_of_table_ids(self.dataset) if (x.startswith("person_course_") and not x.endswith("allcourses"))]
        if cpc_tables:
            the_cpc_table = "[%s.%s]" % (self.dataset, max(cpc_tables))
        else:
            the_cpc_table = None
        print "[make_course_report_tables] ==> Using %s as the latest combined person_course table" % the_cpc_table

        self.parameters = {'dataset': self.dataset,
                           'pc_tables': pc_tables,
                           'pcday_tables': pcday_tables,
                           'uic_tables': uic_tables,
                           'ca_tables': ca_tables,
                           'va_tables': va_tables,
                           'tott_tables': tott_tables,
                           'pcday_ip_counts_tables': pcday_ip_counts_tables,
                           'pcday_trlang_counts_tables': pcday_trlang_counts_tables,
                           'combined_person_course': the_cpc_table,
                           }
        print "[make_course_report_tables] ==> Using these datasets (with person_course tables): %s" % datasets_with_pc

        self.course_datasets = course_datasets
    
        print "="*100
        print "Generating course report tables -> dataset=%s, project=%s" % (self.dataset, self.output_project_id)
        sys.stdout.flush()

        bqutil.create_dataset_if_nonexistent(self.dataset, project_id=output_project_id)

        self.nskip = nskip
        if 1:
            self.combine_show_answer_stats_by_course()
            self.make_totals_by_course()
            self.make_course_axis_table()
            self.make_video_axis_table()
            self.make_person_course_day_table() 
            self.make_medians_by_course()
            self.make_table_of_email_addresses()
            self.make_table_of_email_addresses_by_institution()
            self.make_global_modal_ip_table()
            self.make_global_modal_lang_table()
            self.make_enrollment_by_day()
            self.make_time_on_task_stats_by_course()
            self.make_total_populations_by_course()
            self.make_table_of_n_courses_registered()
            self.make_geographic_distributions()
            ## self.count_tracking_log_events()
            self.make_overall_totals()
    
        print "="*100
        print "Done with course report tables"
        sys.stdout.flush()

    def skip_or_do_step(self, tablename):
        if self.nskip:
            self.nskip += -1
            print "Skipping %s" % tablename
            return -1

        if self.only_step:
            if type(self.only_step)==list:
                if tablename not in self.only_step:
                    print "Skipping %s because not specified in only_step" % tablename
                    return -1
            else:
                if not self.only_step == tablename:
                    print "Skipping %s because not specified in only_step" % tablename
                    return -1
        return 0

    def do_table(self, the_sql, tablename, the_dataset=None, sql_for_description=None, check_skip=True, 
                 allowLargeResults=False, maximumBillingTier=None):

        if check_skip:
            if self.skip_or_do_step(tablename) < 0:
                return	# skip step

        if the_dataset is None:
            the_dataset = self.dataset

        print("Computing %s in BigQuery" % tablename)
        sys.stdout.flush()
        try:
            ret = bqutil.create_bq_table(the_dataset, tablename, the_sql, 
                                         overwrite=True,
                                         output_project_id=self.output_project_id,
                                         sql_for_description=sql_for_description or the_sql,
                                         allowLargeResults=allowLargeResults,
                                         maximumBillingTier=maximumBillingTier,
                                     )
        except Exception as err:
            print "ERROR! Failed on SQL="
            print the_sql
            raise

        gsfn = "%s/%s.csv" % (self.gsbucket, tablename)
        bqutil.extract_table_to_gs(the_dataset, tablename, gsfn, 
                                   format='csv', 
                                   do_gzip=False,
                                   wait=False)

        msg = "CSV download link: %s" % gsutil.gs_download_link(gsfn)
        print msg
        bqutil.add_description_to_table(the_dataset, tablename, msg, append=True, project_id=self.output_project_id)


    def make_medians_by_course(self):

        outer_sql = '''
            select course_id, 
                max(nplay_video_median) as nplay_video_median,
                max(ndays_act_median) as ndays_act_median,
                max(nchapters_median) as nchapters_median,
                max(nevents_median) as nevents_median,
                max(nforum_posts_median) as nforum_posts_median,
                max(grade_median) as grade_median,
                max(nforum_votes_median) as nforum_votes_median,
                max(nforum_endorsed_median) as nforum_endorsed_median,
                max(nforum_threads_median) as nforum_threads_median,
                max(nforum_comments_median) as nforum_comments_median,
                max(nprogcheck_median) as nprogcheck_median,
                max(nshow_answer_median) as nshow_answer_median,
                max(npause_video_median) as npause_video_median,
                max(avg_dt_median) as avg_dt_median,
                max(sum_dt_median) as sum_dt_median,
                max(age_in_2014_median) as age_in_2014_median,
                sum(case when (nplay_video is not null) and (nplay_video > 0) then 1 else 0 end) / count(*) as frac_nonzero_play_video,
                sum(case when (nplay_video is not null) and (nplay_video > 0) then 1 else 0 end) as sum_n_nonzero_play_video,
                sum(case when (grade is not null) and (grade > 0) then 1 else 0 end) / count(*) as frac_nonzero_grade,
                sum(case when (grade is not null) and (grade > 0) then 1 else 0 end) as sum_n_nonzero_grade,
                sum(case when (nforum_posts is not null) and (nforum_posts > 0) then 1 else 0 end) / count(*) as frac_nonzero_forum_posts,
                sum(case when (nforum_posts is not null) and (nforum_posts > 0) then 1 else 0 end) as sum_n_nonzeo_nforum_posts,
                # sum(case when nforum_posts is not null then 1 else 0 end) as n_non_null_nforum_posts,
                count(*) as n_records,
             FROM
                    {sub_sql}
              group by course_id
              order by course_id;
        '''

        sub_sql = """
                   SELECT course_id,
                          (case when nplay_video is not null then course_id end) as cid_nplay_video,
                          (case when ndays_act is not null then course_id end) as cid_ndays_act,
                          (case when nchapters is not null then course_id end) as cid_nchapters,
                          (case when nevents is not null then course_id end) as cid_nevents,
                          (case when nforum_posts is not null then course_id end) as cid_nforum_posts,
                          (case when grade is not null then course_id end) as cid_grade,
                          (case when nforum_votes is not null then course_id end) as cid_nforum_votes,
                          (case when nforum_endorsed is not null then course_id end) as cid_nforum_endorsed,
                          (case when nforum_threads is not null then course_id end) as cid_nforum_threads,
                          (case when nforum_comments is not null then course_id end) as cid_nforum_comments,
                          (case when nprogcheck is not null then course_id end) as cid_nprogcheck,
                          (case when nshow_answer is not null then course_id end) as cid_nshow_answer,
                          (case when npause_video is not null then course_id end) as cid_npause_video,
                          (case when avg_dt is not null then course_id end) as cid_avg_dt,
                          (case when sum_dt is not null then course_id end) as cid_sum_dt,
                          (case when age is not null then course_id end) as cid_age,
                          nforum_posts,
                          grade,
                          nplay_video,

                          PERCENTILE_DISC(0.5) over (partition by cid_nplay_video order by nplay_video) as nplay_video_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_ndays_act order by ndays_act) as ndays_act_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nchapters order by nchapters) as nchapters_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nevents order by nevents) as nevents_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nforum_posts order by nforum_posts) as nforum_posts_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_grade order by grade) as grade_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nforum_votes order by nforum_votes) as nforum_votes_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nforum_endorsed order by nforum_endorsed) as nforum_endorsed_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nforum_threads order by nforum_threads) as nforum_threads_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nforum_comments order by nforum_comments) as nforum_comments_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nprogcheck order by nprogcheck) as nprogcheck_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_nshow_answer order by nshow_answer) as nshow_answer_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_npause_video order by npause_video) as npause_video_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_avg_dt order by avg_dt) as avg_dt_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_sum_dt order by sum_dt) as sum_dt_median,
                          PERCENTILE_DISC(0.5) over (partition by cid_age order by age) as age_in_2014_median,
                    FROM 
                         (
                            select course_id,
                                   viewed,
                                   explored,
                                   certified,
                                   mode,
                                   nplay_video,
                                   ndays_act,
                                   nchapters,
                                   nevents,
                                   nforum_posts,
                                   grade,
                                   nforum_votes,
                                   nforum_endorsed,
                                   nforum_threads,
                                   nforum_comments,
                                   nprogcheck,
                                   nshow_answer,
                                   npause_video,
                                   avg_dt,
                                   sum_dt,
                                   (2014-YoB) as age, 
                                   forumRoles_isStudent,
                            FROM 
                                {pc_tables}
                         )
                    WHERE {constraint} and ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
                           AND ABS(HASH(course_id)) %% 4 = %d
        """

        sub_sql_all = ','.join(["(%s)" % (sub_sql % k) for k in range(4)])
        the_sql = outer_sql.format(sub_sql=sub_sql_all)

        sql_for_description = "outer SQL:\n%s\n\nInner SQL:\n%s" % (outer_sql, sub_sql)

        v_sql = the_sql.format(constraint="viewed", **self.parameters)
        self.do_table(v_sql, 'viewed_median_stats_by_course', sql_for_description=sql_for_description)

        ev_sql = the_sql.format(constraint="explored and viewed", **self.parameters)
        self.do_table(ev_sql, 'explored_median_stats_by_course', sql_for_description=sql_for_description)

        cv_sql = the_sql.format(constraint="certified and viewed", **self.parameters)
        self.do_table(cv_sql, 'certified_median_stats_by_course', sql_for_description=sql_for_description)

        cv_sql = the_sql.format(constraint="(mode='verified') and viewed", **self.parameters)
        self.do_table(cv_sql, 'verified_median_stats_by_course', sql_for_description=sql_for_description)

    def make_time_on_task_stats_by_course(self):

        if not self.parameters['combined_person_course']:
            print "Need combined person_course table for make_time_on_task_stats_by_course, skipping..."
            return

        if self.skip_or_do_step("time_on_task_stats_by_course") < 0:
            return	# skip step

        outer_sql = '''
select course_id, 

sum(total_hours) as sum_total_{hours},
sum(problem_hours) as sum_problem_{hours},
sum(video_hours) as sum_video_{hours},
sum(forum_hours) as sum_forum_{hours},

sum(case when viewed then total_hours end) as sum_total_{hours}_viewed,
sum(case when viewed then problem_hours end) as sum_problem_{hours}_viewed,
sum(case when viewed then video_hours end) as sum_video_{hours}_viewed,
sum(case when viewed then forum_hours end) as sum_forum_{hours}_viewed,

sum(case when certified then total_hours end) as sum_total_{hours}_certified,
sum(case when certified then problem_hours end) as sum_problem_{hours}_certified,
sum(case when certified then video_hours end) as sum_video_{hours}_certified,
sum(case when certified then forum_hours end) as sum_forum_{hours}_certified,

sum(case when verified then total_hours end) as sum_total_{hours}_verified,
sum(case when verified then problem_hours end) as sum_problem_{hours}_verified,
sum(case when verified then video_hours end) as sum_video_{hours}_verified,
sum(case when verified then forum_hours end) as sum_forum_{hours}_verified,

max(median_total_hours) as median_total_{hours},
max(median_problem_hours) as median_problem_{hours},
max(median_video_hours) as median_video_{hours},
max(median_forum_hours) as median_forum_{hours},

max(case when viewed then median_total_hours_viewed end) as median_total_{hours}_viewed,
max(case when viewed then median_problem_hours_viewed end) as median_problem_{hours}_viewed,
max(case when viewed then median_video_hours_viewed end) as median_video_{hours}_viewed,
max(case when viewed then median_forum_hours_viewed end) as median_forum_{hours}_viewed,

max(case when certified then median_total_hours_certified end) as median_total_{hours}_certified,
max(case when certified then median_problem_hours_certified end) as median_problem_{hours}_certified,
max(case when certified then median_video_hours_certified end) as median_video_{hours}_certified,
max(case when certified then median_forum_hours_certified end) as median_forum_{hours}_certified,

max(case when verified then median_total_hours_verified end) as median_total_{hours}_verified,
max(case when verified then median_problem_hours_verified end) as median_problem_{hours}_verified,
max(case when verified then median_video_hours_verified end) as median_video_{hours}_verified,
max(case when verified then median_forum_hours_verified end) as median_forum_{hours}_verified,

FROM {sub_sql}

where course_id is not null
group by course_id
order by course_id;
        '''
        
        sub_sql = '''
 SELECT   
          course_id, total_hours, video_hours, problem_hours, forum_hours, viewed, certified, verified,
          (case when total_hours is not null then course_id end) as cid_total_hours,
          (case when problem_hours is not null then course_id end) as cid_problem_hours,
          (case when video_hours is not null then course_id end) as cid_video_hours,
          (case when forum_hours is not null then course_id end) as cid_forum_hours,

          (case when viewed and (total_hours is not null) then course_id end) as viewed_cid_total_hours,
          (case when viewed and (problem_hours is not null) then course_id end) as viewed_cid_problem_hours,
          (case when viewed and (video_hours is not null) then course_id end) as viewed_cid_video_hours,
          (case when viewed and (forum_hours is not null) then course_id end) as viewed_cid_forum_hours,

          (case when certified and (total_hours is not null) then course_id end) as certified_cid_total_hours,
          (case when certified and (problem_hours is not null) then course_id end) as certified_cid_problem_hours,
          (case when certified and (video_hours is not null) then course_id end) as certified_cid_video_hours,
          (case when certified and (forum_hours is not null) then course_id end) as certified_cid_forum_hours,

          (case when verified and (total_hours is not null) then course_id end) as verified_cid_total_hours,
          (case when verified and (problem_hours is not null) then course_id end) as verified_cid_problem_hours,
          (case when verified and (video_hours is not null) then course_id end) as verified_cid_video_hours,
          (case when verified and (forum_hours is not null) then course_id end) as verified_cid_forum_hours,

         # (case when viewed then course_id end) as viewed_course_id,
         # (case when certified then course_id end) as cert_course_id,

           PERCENTILE_DISC(0.5) over (partition by cid_total_hours order by total_hours) as median_total_hours,
           PERCENTILE_DISC(0.5) over (partition by cid_problem_hours order by problem_hours) as median_problem_hours,
           PERCENTILE_DISC(0.5) over (partition by cid_video_hours order by video_hours) as median_video_hours,
           PERCENTILE_DISC(0.5) over (partition by cid_forum_hours order by forum_hours) as median_forum_hours,

           PERCENTILE_DISC(0.5) over (partition by viewed_cid_total_hours order by total_hours) as median_total_hours_viewed,
           PERCENTILE_DISC(0.5) over (partition by viewed_cid_problem_hours order by problem_hours) as median_problem_hours_viewed,
           PERCENTILE_DISC(0.5) over (partition by viewed_cid_video_hours order by video_hours) as median_video_hours_viewed,
           PERCENTILE_DISC(0.5) over (partition by viewed_cid_forum_hours order by forum_hours) as median_forum_hours_viewed,

           PERCENTILE_DISC(0.5) over (partition by certified_cid_total_hours order by total_hours) as median_total_hours_certified,
           PERCENTILE_DISC(0.5) over (partition by certified_cid_problem_hours order by problem_hours) as median_problem_hours_certified,
           PERCENTILE_DISC(0.5) over (partition by certified_cid_video_hours order by video_hours) as median_video_hours_certified,
           PERCENTILE_DISC(0.5) over (partition by certified_cid_forum_hours order by forum_hours) as median_forum_hours_certified,

           PERCENTILE_DISC(0.5) over (partition by verified_cid_total_hours order by total_hours) as median_total_hours_verified,
           PERCENTILE_DISC(0.5) over (partition by verified_cid_problem_hours order by problem_hours) as median_problem_hours_verified,
           PERCENTILE_DISC(0.5) over (partition by verified_cid_video_hours order by video_hours) as median_video_hours_verified,
           PERCENTILE_DISC(0.5) over (partition by verified_cid_forum_hours order by forum_hours) as median_forum_hours_verified,
 FROM
  (
    SELECT TT.course_id as course_id,
           (TT.total_time_30)/60.0/60 as total_hours,
           (TT.{total}_video_time_30)/60.0/60 as video_hours,
           (TT.{total}_problem_time_30)/60.0/60 as problem_hours,
           (TT.{total}_forum_time_30)/60.0/60 as forum_hours,

           # (TT.serial_video_time_30)/60.0/60 as serial_video_hours,
           # (TT.serial_problem_time_30)/60.0/60 as serial_problem_hours,
           # (TT.serial_forum_time_30)/60.0/60 as serial_forum_hours,

           PC.viewed as viewed,
           PC.certified as certified,
           (case when PC.mode="verified" then True end) as verified,

      FROM (
            SELECT * FROM {tt_set}
      ) as TT
      JOIN EACH {combined_person_course} as PC
      ON TT.course_id = PC.course_id and TT.username=PC.username
      WHERE
                ((PC.forumRoles_isStudent = 1) or (PC.forumRoles_isStudent is null))
      order by course_id
   )
'''

        psets = [{'total': 'total', 'hours': 'hours', 'table': 'time_on_task_stats_by_course'},	# parallel ToT
                 {'total': 'serial', 'hours': 'serial_hours', 'table': 'time_on_task_serial_stats_by_course'}	# serial ToT
                 ]

        for pset in psets:
    
            tott_tables = self.parameters['tott_tables'].split(',\n')

            sub_sql_list = []
            nmax = 6
    
            # break up query to use only at most nmax tables at a time
            # elses BQ may run out of resources, due to the window functions
            
            while len(tott_tables):
                tt_set = ',\n'.join(tott_tables[:nmax])
                tott_tables = tott_tables[nmax:]
                sub_sql_list.append(sub_sql.format(tt_set=tt_set, total=pset['total'], hours=pset['hours'], **self.parameters))
                
            sub_sql_all = ','.join(["(%s)" % x for x in sub_sql_list])
            the_sql = outer_sql.format(sub_sql=sub_sql_all, total=pset['total'], hours=pset['hours'])
            
            sql_for_description = "outer SQL:\n%s\n\nInner SQL:\n%stt_set=%s" % (outer_sql, sub_sql, self.parameters['tott_tables'])
    
            self.do_table(the_sql, pset['table'], sql_for_description=sql_for_description, check_skip=False, maximumBillingTier=4)

    def make_course_axis_table(self):

        the_sql = '''
            SELECT *
            FROM {ca_tables}
        '''.format(**self.parameters)
        self.do_table(the_sql, 'course_axis')

    def make_video_axis_table(self):

        the_sql = '''
            SELECT *
            FROM {va_tables}
        '''.format(**self.parameters)
        self.do_table(the_sql, 'video_axis')

    def make_person_course_day_table(self):

        the_sql = '''
            SELECT *
            FROM 
                {pcday_tables}
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'person_course_day_allcourses', allowLargeResults=True)

    def make_totals_by_course(self):

        the_sql = '''
            SELECT course_id,
                   count(*) as registered_sum,
                   sum(case when viewed then 1 else 0 end) as viewed_sum,
                   sum(case when explored and viewed then 1 else 0 end) as explored_sum,
                   sum(case when certified and viewed then 1 else 0 end) as certified_sum,
                   sum(case when certified and explored and viewed then 1 else 0 end) as certified_and_explored_sum,
                   sum(case when (certified or explored) and viewed then 1 else 0 end) as certified_or_explored_sum,
    
                   sum(case when gender='m' then 1 else 0 end) as n_male,
                   sum(case when gender='f' then 1 else 0 end) as n_female,
    
                   sum(case when cc_by_ip="US" and viewed then 1 else 0 end) as n_usa_and_viewed,

                   sum(case when ((LoE="b") or (LoE="m") or (LoE="p") or (LoE="p_oth") or (LoE="p_se")) then 1 else 0 end) as n_bachplus_in_reg,
                   sum(case when (viewed and ((LoE="b") or (LoE="m") or (LoE="p") or (LoE="p_oth") or (LoE="p_se"))) then 1 else 0 end) as n_bachplus_in_viewed,

                   sum(case when ((LoE="a") or (LoE="el") or (LoE="hs") or (LoE="jhs") or (LoE="none")) then 1 else 0 end) as n_not_bachplus_in_reg,
                   sum(case when (viewed and ((LoE="a") or (LoE="el") or (LoE="hs") or (LoE="jhs") or (LoE="none"))) then 1 else 0 end) as n_not_bachplus_in_viewed,

                   sum(case when (viewed and gender='m') then 1 else 0 end) as n_male_viewed,
                   sum(case when (viewed and gender='f') then 1 else 0 end) as n_female_viewed,

                   sum(case when mode="verified" then 1 else 0 end) as n_verified_id,
                   sum(case when (viewed and mode="verified") then 1 else 0 end) as verified_viewed,
                   sum(case when (explored and mode="verified") then 1 else 0 end) as verified_explored,
                   sum(case when (certified and mode="verified") then 1 else 0 end) as verified_certified,
                   avg(case when (mode="verified") then grade else null end) as verified_avg_grade,
    
                   sum(case when (gender='m' and mode="verified") then 1 else 0 end) as verified_n_male,
                   sum(case when (gender='f' and mode="verified") then 1 else 0 end) as verified_n_female,
    
                   sum(case when (grade > 0) then 1 else 0 end) as n_reg_nonzero_grade,
                   sum(case when ((grade > 0) and viewed) then 1 else 0 end) as n_viewed_nonzero_grade,
                   sum(case when ((grade > 0) and viewed and explored) then 1 else 0 end) as n_explored_nonzero_grade,

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
    
                   avg(case when viewed then nprogcheck else null end) as nprogcheck_avg,
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
            WHERE
                ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
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
                   and ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
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
                   and ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
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
    
    def make_table_of_email_addresses(self):
        the_sql = '''
            SELECT username, email, count (*) as ncourses
            FROM 
                {uic_tables}
            WHERE username is not null
            group by username, email
            order by username;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'email_addresses')
    
    def make_table_of_email_addresses_by_institution(self):
        '''
        Make separate tables of email addresses by institution (for cases when usernames are in separate name spaces)
        '''
        uic_by_inst = defaultdict(list)
        for cid in self.all_uic_tables:
            inst = cid.split("__", 1)[0]
            uic_by_inst[inst].append('[%s.user_info_combo]' % cid)

        for inst, uicset in uic_by_inst.items():
            the_sql = '''
                SELECT username, email, count (*) as ncourses
                FROM 
                    {uic_tables}
                WHERE username is not null
                group by username, email
                order by username;
            '''.format(uic_tables=',\n'.join(uicset))
        
            self.do_table(the_sql, 'email_addresses_%s' % inst)

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
            WHERE
                 ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
            group by cc, countryLabel
            #order by countryLabel
            order by nverified desc
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'geographic_distributions')

    def make_global_modal_lang_table(self):
        '''
        Make table of global language, based on each course's transcript language  counts, found in 
        individual course's pcday_trlang_counts tables.
        '''
 
        the_sql = '''
           SELECT
             username,
             resource_event_data AS modal_language,
             langcount,
             n_different_lang,
           FROM (
             SELECT
               username,
               resource_event_data,
               langcount,
               RANK() OVER (PARTITION BY username ORDER BY langcount ASC) n_different_lang,
               RANK() OVER (PARTITION BY username ORDER BY langcount DESC) rank,
             FROM (
               SELECT
                 username,
                 resource_event_data,
                 SUM(langcount) AS langcount
               FROM {pcday_trlang_counts_tables}
               GROUP EACH BY
                 username,
                 resource_event_data ) )
               WHERE rank=1
               ORDER BY username
        '''.format(**self.parameters)

        self.do_table(the_sql, 'global_modal_lang', the_dataset='courses')

        
    def make_global_modal_ip_table(self):
        '''
        Make table of global modal ip addresses, based on each course's IP address counts, found in 
        individual course's pcday_ip_counts tables.
        '''
        ntables = len(self.all_pcday_ip_counts_tables)
        if (ntables > 30):
            print "--> Too many pcday_ip_counts tables (%d): using hash for computing global modal_ip" % ntables
            sys.stdout.flush()
            return self.make_global_modal_ip_table_with_hash()

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

    def make_global_modal_ip_table_with_hash(self):
        '''
        Use hashing to reduce number of usernames which need to partitioned, in computing
        global modal ip.
        '''
        inner_sql = '''
                  SELECT username, ip, ip_count,
                          RANK() over (partition by username order by ip_count ASC) n_different_ip,
                          RANK() over (partition by username order by ip_count DESC) rank,
                    from ( select username, ip, sum(ipcount) as ip_count
                           from {pcday_ip_counts_tables}
                           WHERE {hash_limit}
                           GROUP EACH BY username, ip
                    )
        '''
        
        outer_sql = '''
              SELECT username, IP as modal_ip, ip_count, n_different_ip,
              FROM
                 {inner_sql_set}
                  where rank=1
                  order by username
        '''

        ntables = len(self.all_pcday_ip_counts_tables)
        # dn = 30
        dn = 50
        nhash = ntables / dn
        if nhash * dn < ntables:
            nhash += 1
        iss = []
        for k in range(nhash):
            hash_limit = "ABS(HASH(username)) %% %d = %d" % (nhash, k)
            iss.append(inner_sql.format(hash_limit=hash_limit, **self.parameters))

        iss_str = ','.join(['( %s )\n' % x for x in iss])
        the_sql = outer_sql.format(inner_sql_set=iss_str, **self.parameters)

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
            WHERE
                 ((forumRoles_isStudent = 1) or (forumRoles_isStudent is null))
            group by course_id
            order by course_id;
        '''.format(**self.parameters)
        
        self.do_table(the_sql, 'populations_stats_by_course')
        
    def count_tracking_log_events(self):
        '''
        Loop over all tracking logs up to cutoff date, and sum up number of entries, by
        doing table info lookups, with no SQL queries.
        '''
        if self.skip_or_do_step("count_events") < 0:
            return	# skip step

        tlend = self.end_date.replace('-', '')	# end_date normally specified as YYYY-MM-DD

        log_event_counts = {}

        # iterate over each course, one at a time
        for course_id in self.course_id_set:
            log_dataset = bqutil.course_id2dataset(course_id, dtype="logs")
            # get list of all tracking log files for this course
            log_tables = [x for x in bqutil.get_list_of_table_ids(log_dataset) if x.startswith('tracklog_20')]

            log_tables_todo = [x for x in log_tables if x[9:] <= tlend]
            log_tables_todo.sort()
            print "[count_tracking_log_events] for course %s using %d tracking log tables, from %s to %s" % (course_id, 
                                                                                                             len(log_tables_todo),
                                                                                                             log_tables_todo[0], 
                                                                                                             log_tables_todo[-1])
            sys.stdout.flush()

            # go through all log files and get size on each
            row_sizes = [ bqutil.get_bq_table_size_rows(log_dataset, x) for x in log_tables_todo ]
            
            log_event_counts[course_id] = sum(row_sizes)
            print "                         For %s found %d total tracking log events" % (course_id, log_event_counts[course_id])
            sys.stdout.flush()

        self.log_event_counts = log_event_counts
        
        self.total_events = sum(log_event_counts.values())
        print "--> Total number of events for %s = %d" % (self.org, self.total_events)
        
    def combine_show_answer_stats_by_course(self):
        '''
        combine show_answer_stats_by_course over all courses, into one table,
        stored in the course_report dataset.
        '''
        tablename = "show_answer_stats_by_course"
        if self.skip_or_do_step(tablename) < 0:
            return	# skip step

        # which datasets have stats_by_course?

        datasets_with_sasbc = []
        for cd in self.course_datasets:
            try:
                table = bqutil.get_bq_table_info(cd, tablename)
            except Exception as err:
                print "[make-course_report_tables] Err: %s" % str(err)
                continue
            if table is None:
                continue
            datasets_with_sasbc.append(cd)
        
        if not datasets_with_sasbc:
            print '[make_course_report_tables] combine_show_answer_stats_by_course: no datasets have show_answer_stats_by_course!'
            print '--> Aborting creation of %s' %  show_answer_stats_by_course
            print '--> This may cause problems with report creation.  Run analyze_problems on at least one course to resolve'

        sasbc_tables = ',\n'.join(['[%s.%s]' % (x, tablename) for x in datasets_with_sasbc])

        SQL = """
              SELECT * from {tables}
              """.format(tables=sasbc_tables)
        
        self.do_table(SQL, tablename, check_skip=False)
        
