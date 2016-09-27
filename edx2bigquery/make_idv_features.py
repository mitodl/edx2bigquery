'''
Analyze engagement of IDV enrollees (and non-IDV enrollees) before end of the course, including
forum, video, and problem activity, up to the point of IDV enrollment (or last ever IDV enrollment
in the course, for non-IDVers).  Also include context about prior activity in other courses,
if course_report_latest.person_course_viewed is available.  Produces idv_analysis table, in each course.
'''

import sys
import bqutil

def AnalyzeIDV(course_id, force_recompute=False, use_dataset_latest=False):

    tablename = "idv_analysis"
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)

    print "="*77
    print "Creating %s.%s table for %s" % (dataset, tablename, course_id)
    print "-"*77

    org = course_id.split('/',1)[0]
    dataset_cr = ('course_report_%s' % org)
    if use_dataset_latest:
        dataset_cr = 'course_report_latest'

    pcv = "person_course_viewed"
    try:
        tinfo = bqutil.get_bq_table_info(dataset_cr, "person_course_viewed")
        assert tinfo is not None
    except Exception as err:
        print " --> missing %s.%s ;  using dummy instead" % (dataset_cr, pcv)
        sys.stdout.flush()
        dataset_cr = dataset
        pcv = "person_course"

    the_sql = """
# IDV and non-IDV enrollee engagement at point of last IDV enrollment, before course end, including # IDV and certificates in other courses
SELECT
    "{course_id}" as course_id,    
    OC.user_id as user_id,
    OC.username as username,
    (OC.verified_enroll_time is not NULL) as is_idv,
    sum(case when PCI.verified_enroll_time is not NULL then 1 else 0 end) as n_other_idv,
    sum(case when PCI.verified_enroll_time is not NULL and PCI.start_time > OC.start_time then 1 else 0 end) as n_previous_idv,
    sum(case when PCI.certified and PCI.start_time > OC.start_time then 1 else 0 end) as n_previous_certified,
    sum(case when PCI.viewed and PCI.start_time > OC.start_time then 1 else 0 end) as n_previous_participated,
    sum(case when (PCI.verified_enroll_time is not NULL and PCI.start_time > OC.start_time and PCI.certified) then 1 else 0 end) as n_previous_idv_certified,
    first(gender) as gender,
    first(YoB) as YoB,
    first(LoE) as LoE,
    OC.n_problem_records as n_problem_records,
    OC.n_correct as n_correct,
    OC.n_incorrect as n_incorrect,
    OC.total_problem_points as total_problem_points,
    OC.verified_enroll_time as verified_enroll_time,
    OC.verified_unenroll_time as verified_unenroll_time,
    OC.verified_enroll_date as verified_enroll_date,
    OC.verified_unenroll_date as verified_unenroll_date,
    OC.nforum_pinned as nforum_pinned,
    OC.is_forum_moderator as is_forum_moderator,
    OC.final_course_grade as final_course_grade,
    OC.earned_certificate as earned_certificate,
    OC.n_show_answer as n_show_answer,
    OC.nprogcheck as nprogcheck,
    OC.nvideo as nvideo,
    OC.nforum_reads as nforum_reads,
    OC.nforum_posts as nforum_posts,
    OC.hours_on_system as hours_on_system,
    OC.countryLabel as countryLabel,
    OC.start_time as start_time,
FROM
(
    # engagement stats for NON verified ID versus verified ID, as of the date of the last IDV signup
    SELECT *
    FROM
    (
        # stats for NON verified ID, as of the date of the last IDV signup
        SELECT
        PAC.user_id as user_id,
            PAC.username as username,
            PAC.n_problem_records as n_problem_records,
            PAC.n_correct as n_correct,
            PAC.n_incorrect as n_incorrect,
            PAC.total_problem_points as total_problem_points,
            PAC.verified_enroll_time as verified_enroll_time,
            PAC.verified_unenroll_time as verified_unenroll_time,
            DATE(PAC.verified_enroll_time) as verified_enroll_date,
            DATE(PAC.verified_unenroll_time) as verified_unenroll_date,
            PAC.nforum_pinned as nforum_pinned,
            PAC.is_forum_moderator as is_forum_moderator,
            PAC.final_course_grade as final_course_grade,
            PAC.earned_certificate as earned_certificate,
            PAC.countryLabel as countryLabel,
            PAC.start_time as start_time,
            sum(PCD.nshow_answer) as n_show_answer,
            sum(PCD.nprogcheck) as nprogcheck,
            sum(PCD.nvideo) as nvideo,
            sum(PCD.nforum_reads) as nforum_reads,
            sum(PCD.nforum_posts) as nforum_posts,
            sum(PCD.sum_dt / 60 / 60) as hours_on_system,
        FROM
        (
            # get problem grade and activity counts up to date of verified ID enrollment
            SELECT PA.user_id as user_id,
                PC.username as username,
                count(*) as n_problem_records,
                sum(case when PA.item.correct_bool then 1 else 0 end) as n_correct,
                sum(case when PA.item.correct_bool==False then 1 else 0 end) as n_incorrect,
                sum(PA.grade) as total_problem_points,
                PC.verified_enroll_time as verified_enroll_time,
                PC.verified_unenroll_time as verified_unenroll_time,
                PC.nforum_pinned as nforum_pinned,
                PC.forumRoles_isModerator as is_forum_moderator,
                PC.grade as final_course_grade,
                PC.certified as earned_certificate,
                PC.countryLabel as countryLabel,
                PC.start_time as start_time,
                max_verified_enroll_time,
            FROM [{dataset}.problem_analysis] PA
            JOIN
            (
                SELECT user_id, username, verified_enroll_time, verified_unenroll_time, nforum_pinned,
                    forumRoles_isModerator, grade, certified, max_verified_enroll_time, countryLabel, start_time
                      FROM [{dataset}.person_course] PC
                CROSS JOIN
                (
                    SELECT max(verified_enroll_time) as max_verified_enroll_time
                            FROM [{dataset}.person_course]
                ) VET
                where viewed
            ) PC
            ON PA.user_id = PC.user_id
            where PA.created <= PC.max_verified_enroll_time
                and PC.verified_enroll_time is null
            group by user_id, username, verified_enroll_time, nforum_pinned, is_forum_moderator, final_course_grade, earned_certificate,
                verified_unenroll_time, max_verified_enroll_time, countryLabel, start_time
            order by user_id
        ) PAC
        JOIN [{dataset}.person_course_day] PCD
            ON PAC.username = PCD.username
        WHERE PCD.date < DATE(max_verified_enroll_time)
        group by user_id, username, verified_enroll_time, nforum_pinned, is_forum_moderator, final_course_grade, earned_certificate,
                 verified_unenroll_time, n_problem_records, n_correct, n_incorrect, total_problem_points, nforum_pinned, is_forum_moderator,
                 verified_enroll_date, verified_unenroll_date, countryLabel, start_time
        order by user_id
    ),
    (
        # stats for those who DID enroll verified ID, as of the date of their IDV enrollment
        # include nprogcheck, nshow_answer, nproblem_check, nvideo, hours_on_system
        SELECT
        PAC.user_id as user_id,
            PAC.username as username,
            PAC.n_problem_records as n_problem_records,
            PAC.n_correct as n_correct,
            PAC.n_incorrect as n_incorrect,
            PAC.total_problem_points as total_problem_points,
            PAC.verified_enroll_time as verified_enroll_time,
            PAC.verified_unenroll_time as verified_unenroll_time,
            DATE(PAC.verified_enroll_time) as verified_enroll_date,
            DATE(PAC.verified_unenroll_time) as verified_unenroll_date,
            PAC.nforum_pinned as nforum_pinned,
            PAC.is_forum_moderator as is_forum_moderator,
            PAC.final_course_grade as final_course_grade,
            PAC.earned_certificate as earned_certificate,
            PAC.countryLabel as countryLabel,
            PAC.start_time as start_time,
            sum(PCD.nshow_answer) as n_show_answer,
            sum(PCD.nprogcheck) as nprogcheck,
            sum(PCD.nvideo) as nvideo,
            sum(PCD.nforum_reads) as nforum_reads,
            sum(PCD.nforum_posts) as nforum_posts,
            sum(PCD.sum_dt / 60 / 60) as hours_on_system,
        FROM
        (
            # get problem grade and activity counts up to date of verified ID enrollment
            SELECT PA.user_id as user_id,
                PC.username as username,
                count(*) as n_problem_records,
                sum(case when PA.item.correct_bool then 1 else 0 end) as n_correct,
                sum(case when PA.item.correct_bool==False then 1 else 0 end) as n_incorrect,
                sum(PA.grade) as total_problem_points,
                PC.verified_enroll_time as verified_enroll_time,
                PC.verified_unenroll_time as verified_unenroll_time,
                PC.nforum_pinned as nforum_pinned,
                PC.forumRoles_isModerator as is_forum_moderator,
                PC.grade as final_course_grade,
                PC.certified as earned_certificate,
                PC.countryLabel as countryLabel,
                PC.start_time as start_time,
            FROM [{dataset}.problem_analysis] PA
            JOIN [{dataset}.person_course] PC
               ON PA.user_id = PC.user_id
            where PA.created <= PC.verified_enroll_time
            group by user_id, username, verified_enroll_time, nforum_pinned, is_forum_moderator, final_course_grade,
                     earned_certificate, verified_unenroll_time, countryLabel, start_time
            order by user_id
        ) PAC
        JOIN [{dataset}.person_course_day] PCD
            ON PAC.username = PCD.username
        WHERE PCD.date < DATE(PAC.verified_enroll_time)
        group by user_id, username, verified_enroll_time, nforum_pinned, is_forum_moderator, final_course_grade, earned_certificate,
                 verified_unenroll_time, n_problem_records, n_correct, n_incorrect, total_problem_points, nforum_pinned, is_forum_moderator,
                 verified_enroll_date, verified_unenroll_date, countryLabel, start_time
        order by user_id
    )
    order by verified_enroll_date, user_id
) OC
LEFT JOIN [{dataset_cr}.{pcv}] PCI
on OC.user_id = PCI.user_id
#where (PCI.verified_enroll_time is null) or (PCI.verified_enroll_time <= OC.verified_enroll_time)
group by user_id, username, verified_enroll_time, nforum_pinned, is_forum_moderator, final_course_grade, earned_certificate,
         verified_unenroll_time, n_problem_records, n_correct, n_incorrect, total_problem_points, nforum_pinned, is_forum_moderator,
         verified_enroll_date, verified_unenroll_date,
         n_show_answer, nprogcheck, nvideo, nforum_reads, nforum_posts, hours_on_system, countryLabel, start_time, is_idv
order by verified_enroll_date, user_id
"""

    the_sql = the_sql.format(dataset=dataset, dataset_cr=dataset_cr, pcv=pcv, course_id=course_id)

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, force_query=force_recompute,
                                    depends_on=["%s.problem_course" % dataset, "%s.person_course_day" % dataset, "%s.problem_analysis" % dataset],
                                    allowLargeResults=True,
                                    startIndex=-2)
    except Exception as err:
        print "ERROR! Failed on SQL="
        print the_sql
        raise
    
    print "  --> created %s.%s" % (dataset, tablename)
    sys.stdout.flush()
