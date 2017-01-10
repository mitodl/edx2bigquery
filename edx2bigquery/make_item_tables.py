#!/usr/bin/python
'''
Make tables for IRT analyses.

1. course-item table, with one row per assessment item.

   Currently only includes capa problems (LTI and other xmodules / xblocks not included)

2. person-item table, with one row per item & person.  Only includes items in the course-item table.

'''

import sys
import bqutil
import datetime

def make_item_tables(course_id, force_recompute=False, use_dataset_latest=False):
    create_course_item_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)
    create_person_item_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)
    create_person_problem_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)
    create_course_problem_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)


def create_course_item_table(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    the course_item dataset has these columns:

    Field Name                              Type    Example         Description
    item_id                                 string  i4x-MITx-8_MReV-problem-CheckPoint_1_Newton_s_First_Law_2_1     
                                                                    Unique ID for an assessment item (constructed using the problem module_id, and linked to problem_analysis table keys)
    problem_id                              string  CheckPoint_1_Newton_s_First_Law 
                                                                    Unique ID for an assessment problem (constructed using problem url_name)
    problem_nid                             integer 27              unique problem numerical id (equal to the sequential count of problems up to this one)
    assignment_short_id                     string  HW_4            Unique short ID for assignment, using assignment short name + "_" + assignment_seq_num (should be same as what shows up in user's edX platform progress page)
    item_weight                             float   6.59E-05        Fraction of overall grade (between 0 and 1) contributed by this item
    n_user_responses                        integer 4868            Number of users who provided a response to this assessment item
    problem_name                            string  CheckPoint 1: Newton's First Law        
                                                                    Name of problem within which this item exists
    chapter_name                            string  Chapter 1       Name of chapter within which the problem exists
    section_name                            string  Section 1       Name of section (aka sequential) within which the problem exists
    assignment_id                           string  Checkpoint_ch3  Unique ID for the assignment within which the problem exists
    n_problems_in_assignment                integer 23              Number of problems within the assignment
    assignment_type                         string  Checkpoint      The assignment type within which the assignment exists
    assignment_type_weight                  float   0.1             Fraction of the overall grade contributed by the assignment type
    n_assignments_of_type                   integer 11              Number of assignments of this type
    assignment_seq_num                      integer 3               Sequential number of the assignment_type within the course
    chapter_number                          integer 3               Number of the chapter within which the problem exists
    section_number                          integer 3               Number of the section (aka sequential) within which the problem exists
    content_index                           integer 141             Index number of the problem within the content course axis
    problem_weight                          integer 1               Weight of the problem within the assignment
    item_points_possible                    float   1               Always 1 (used for debugging - number of points assigned to an item)
    problem_points_possible                 integer 6               Always equal to the number of items in the assignment (used for debugging)
    emperical_item_points_possible          integer 1               Emperical value of point value of item, based on user data in problem_analysis table (for debugging)
    emperical_problem_points_possible       integer 6               Emperical value of maximum number of points possible for problem based on problem_analysis (for debugging)
    item_number                             integer 1               Number of the item, within the problem (in order of presentation, starting from 1)
    n_items                                 integer 6               Number of items within the problem
    start_date                              date    2013-06-01 00:01:00 UTC 
                                                                    Date when problem was issued
    due_date                                date    2013-06-23 23:59:00 UTC 
                                                                    Date when problem was due
    problem_path                            string  /Unit_1/Newtons_First_Law/2/1   
                                                                    Path of problem within course content, specifying chapter and sequential
    problem_short_id                        string  HW_7__3         short (and unique) problem ID, made using assignment short ID + "__" + problem number
    item_short_id                           string  HW_7__3_1       short (and unique) item ID, made using problem short ID + "_" + item number
    item_nid                                integer 41              unique item numerical id (equal to the row number of this entry in the course_itm table)
    cumulative_item_weight                  float   6.59E-05        Cumulative fraction of item weights (for debugging: should increase to 1.0 by the end of table)
    is_split                                boolean False           Boolean flag indicating if this item was within an A/B split_test or not
    split_name                              string  CircMotionAB    Name of the split_test within which this item is placed, if is_split is True

    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "course_item"

    # determine if grading_policy exists or not
    GP_TABLE = "grading_policy"
    have_grading_policy = False
    try:
        tinfo = bqutil.get_bq_table_info(dataset, GP_TABLE)
        assert tinfo is not None, "%s.%s does not exist" % ( dataset, GP_TABLE )
        if tinfo is not None:
            have_grading_policy = True
    except Exception as err:
        pass

    # change SQL if grading_policy doesn't exist
    if have_grading_policy:
        disable_gformat = ""
        alternate_gp = ""
    else:
        print "Warning - grading_policy doest NOT exist, using a dummy grading policy instead, and allowing gformat=null"
        sys.stdout.flush()
        disable_gformat = "#"
        alternate_gp = '( SELECT "" as assignment_type, 1.0 as fraction_of_overall_grade, "none" as short_label ) GP'

    the_sql = """
SELECT 
    # '{course_id}' as course_id,
    *,
    CONCAT(assignment_short_id, "__", STRING(problem_number)) as problem_short_id,
    CONCAT(assignment_short_id, "__", STRING(problem_number), "_", STRING(item_number)) as item_short_id,
    row_number() over (order by content_index, item_number) as item_nid,
    sum(item_weight) over (order by content_index, item_number) cumulative_item_weight
FROM
(
    # items with additional data about fraction_of_overall_grade from grading_policy
    SELECT item_id, 
        problem_id,
        max(if(item_number=1, x_item_nid, null)) over (partition by problem_id) as problem_nid,
        CONCAT((case when GP.short_label is null then "" else GP.short_label end),
               "_", STRING(assignment_seq_num)) as assignment_short_id,
        (problem_weight * (case when GP.fraction_of_overall_grade is null then 1.0 else GP.fraction_of_overall_grade end)
             / n_items / sum_problem_weight_in_assignment / n_assignments_of_type) as item_weight,
        n_user_responses,
        chapter_name,
        section_name,
        vertical_name,
        problem_name,
        CI.assignment_id as assignment_id,
        n_problems_in_assignment,
        CI.assignment_type as assignment_type,
        (case when GP.fraction_of_overall_grade is null then 1.0 else GP.fraction_of_overall_grade end) as assignment_type_weight,
        n_assignments_of_type,
        assignment_seq_num,
        chapter_number,
        content_index,
        section_number,
        problem_number,
        problem_weight,
        item_points_possible,
        problem_points_possible,
        emperical_item_points_possible,
        emperical_problem_points_possible,
        item_number,
        n_items,
        start_date,
        due_date,
        is_split,
        split_name,
        problem_path,
    FROM
    (
        # items with number of problems per assignment
        SELECT item_id, item_number,
            n_items,
            problem_id,
            row_number() over (partition by item_number order by content_index) as x_item_nid,
            n_user_responses,
            chapter_name,
            section_name,
            vertical_name,
            problem_name,
            assignment_id,
            sum(if(assignment_id is not null and item_number=1, 1, 0)) over (partition by assignment_id) n_problems_in_assignment,
            sum(if(assignment_id is not null and item_number=1, problem_weight, 0)) 
                over (partition by assignment_id) sum_problem_weight_in_assignment,
            assignment_type,
            n_assignments_of_type,
            assignment_seq_num,
            chapter_number,
            section_number,
            problem_number,
            problem_path,
            content_index,
            start_date,
            due_date,
            is_split,
            split_name,
            problem_weight,
            item_points_possible,
            problem_points_possible,
            emperical_item_points_possible,
            emperical_problem_points_possible,
        FROM
        (
            # items from problem_analysis with metadata from course_axis
            SELECT item_id, item_number,
                n_items,
                problem_id,
                n_user_responses,
                CA.name as problem_name,
                chapter_name,
                section_name,
                vertical_name,
                assignment_id,
                assignment_type,
                n_assignments_of_type,
                CA.assignment_seq_num as assignment_seq_num,
                CA.chapter_number as chapter_number,
                CA.section_number as section_number,
                CA.problem_number as problem_number,
                CA.path as problem_path,
                CA.index as content_index,
                CA.start as start_date,
                CA.due as due_date,
                CA.is_split as is_split,
                CA.split_name as split_name,
                if(CA.weight is null, 1.0, CA.weight) as problem_weight,
                item_points_possible,
                problem_points_possible,
                emperical_item_points_possible,
                emperical_problem_points_possible,
            FROM
            (
                # get items with item metadata from problem_analysis table
                SELECT item_id, item_number,
                    n_items,
                    problem_id,
                    n_user_responses,
                    1.0 as item_points_possible,
                    1.0 * n_items as problem_points_possible,
                    problem_points_possible / n_items as emperical_item_points_possible,
                    problem_points_possible as emperical_problem_points_possible,
                FROM
                (
                    SELECT item_id, item_number,
                        max(item_number) over (partition by problem_id) n_items,
                        problem_id,
                        problem_points_possible,
                        n_user_responses,
                    FROM
                    (
                        SELECT item_id,
                            row_number() over (partition by problem_id order by item_id) item_number,
                            problem_id,
                            problem_points_possible,
                            n_user_responses,
                        FROM
                        (
                            SELECT item.answer_id as item_id,
                                problem_url_name as problem_id,
                                max_grade as problem_points_possible,
                                count(*) as n_user_responses,
                            FROM [{dataset}.problem_analysis]
                            group by item_id, problem_id, problem_points_possible
                            having n_user_responses > 5   # minimum cutoff for an item to be included
                        )
                    )
                )
                order by item_id, item_number
            ) as PA
            JOIN 
            (
                # -------------------------------------------------- graded problems from course axis
                # master table of graded problems from course_axis, with assignment metadata
                SELECT module_id,
                    url_name,
                    index,
                    weight,
                    assignment_type,
                    MAX(IF(problem_number=1, x_assignment_seq_num, null)) over (partition by assignment_id) as assignment_seq_num,
                    problem_number,
                    assignment_id,
                    n_assignments_of_type,
                    chapter_name,
                    section_name,
                    vertical_name,
                    name,
                    path,
                    start,
                    due,
                    is_split,
                    split_name,
                    chapter_number,
                    section_number,
                FROM
                (
                    # course_axis with chapter number and number of assignments of type
                    SELECT *,  # add column with number of assignments of type
                        SUM(IF(problem_number=1, 1, 0)) over (partition by assignment_type) n_assignments_of_type,
                        row_number() over (partition by assignment_type, problem_number order by index) as x_assignment_seq_num,
                    FROM
                    (
                        # ---------------------------------------- course axis with vertical name
                        SELECT module_id,
                            url_name,
                            index,
                            weight,
                            assignment_type,
                            chapter_number,
                            section_number,
                            assignment_id,  
                            chapter_name,
                            section_name,
                            vertical_name,
                            name,
                            path,
                            start,
                            due,
                            is_split,
                            split_name,
                            # add column with problem number within assignment_id
                            row_number() over (partition by assignment_id order by index) problem_number,
                        FROM
                        (
                            # course axis of problems which have non-null grading_format, including chapter number
                            # and section (aka sequential) number (within the chapter)
                            SELECT CAI.module_id as module_id,
                                CAI.url_name as url_name,
                                index,
                                weight,
                                assignment_type,
                                chapter_number,
                                section_number,
                                #  assignment_id = assignment_type + ch_chapter_number + sec_section_number
                                CONCAT(assignment_type, "_ch", STRING(chapter_number), "_sec", STRING(section_number)) as assignment_id,  
                                chapter_name,
                                section_name,
                                name,
                                path,
                                start,
                                due,
                                is_split,
                                split_name,
                                parent,
                            FROM 
                            (
                                # course axis entries of things which have non-null grading format, with section_mid from path
                                SELECT module_id,
                                    url_name,
                                    index,
                                    If(data.weight is null, 1.0, data.weight) as weight,
                                    (case when gformat is null then "" else gformat end) as assignment_type,
                                    chapter_mid as chapter_mid,
                                    REGEXP_EXTRACT(path, '^/[^/]+/([^/]+)') as section_mid,
                                    name,
                                    path,
                                    start,
                                    due,
                                    is_split,
                                    split_url_name as split_name,
                                    parent,
                                FROM [{dataset}.course_axis] CAI
                                where 
                                #{disable_gformat} gformat is not null and
                                category = "problem"
                                order by index
                            ) CAI
                            LEFT JOIN  # join course_axis with itself to get chapter_number and section_number
                            (   
                                # get chapters and sections (aka sequentials) with module_id, chapter_number, and section_number
                                # each assignment is identified by assignment_type + chapter_number + section_number
                                # note in some previous calculations, the section_number was left out by mistake
                                # see https://github.com/edx/edx-platform/blob/master/common/lib/xmodule/xmodule/course_module.py#L1305
                                SELECT module_id, url_name, name as section_name,
                                    max(if(category="chapter", x_chapter_number, null)) over (partition by chapter_mid order by index) as chapter_number,
                                    section_number,
                                    chapter_name,
                                FROM
                                (
                                    SELECT module_id, url_name,
                                        row_number() over (partition by category order by index) as x_chapter_number,
                                        row_number() over (partition by chapter_mid, category order by index) as section_number,
                                        FIRST_VALUE(name) over (partition by chapter_mid order by index) as chapter_name,
                                        index,
                                        category,
                                        name,
                                        if(category="chapter", module_id, chapter_mid) as chapter_mid,
                                    FROM  [{dataset}.course_axis] 
                                    where category = "chapter" or category = "sequential" or category = "videosequence"
                                    order by index
                                )
                                order by index
                            ) CHN
                            # ON CAI.chapter_mid = CHN.chapter_mid  # old, for assignments by chapter
                            ON CAI.section_mid = CHN.url_name     # correct way, for assignments by section (aka sequential)
                            # where gformat is not null
                        ) CAPN
                        LEFT JOIN # join with course_axis to get names of verticals in which problems reside
                        (
                            # get verticals
                            SELECT url_name as vertical_url_name, 
                                name as vertical_name,
                            FROM  [{dataset}.course_axis] 
                            where category = "vertical"
                        ) CAV
                        ON CAPN.parent = CAV.vertical_url_name
                        # ---------------------------------------- END course axis with vertical_name
                    )
                )
                order by index
                # -------------------------------------------------- END graded problems from course axis
            ) CA
            ON PA.problem_id = CA.url_name
        )
    ) CI
    LEFT JOIN 
    {disable_gformat} [{dataset}.grading_policy] GP
    {alternate_gp}
    ON CI.assignment_type = GP.assignment_type
    order by content_index, item_number
)
order by content_index, item_number
    """.format(dataset=dataset, course_id=course_id, disable_gformat=disable_gformat, alternate_gp=alternate_gp)

    depends_on = [ "%s.course_axis" % dataset,
                   "%s.grading_policy" % dataset,
                   "%s.problem_analysis" % dataset
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
                                    newer_than=datetime.datetime(2015, 10, 31, 17, 00),
                                    depends_on=depends_on,
                                    force_query=force_recompute)
    except Exception as err:
        print "[make_course_item_table] ERR! failed in creating %s.%s using this sql:" % (dataset, tablename)
        print the_sql
        raise

    if not bqdat:
        nfound = 0
    else:
        nfound = len(bqdat['data'])
    print "--> Done with %s for %s, %d entries found" % (tablename, course_id, nfound)
    sys.stdout.flush()


def create_person_item_table(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Generate person_item table, with one row per (user_id, item_id), giving grade points earned, attempts,
    and datestamp.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "person_item"

    the_sql = """
# compute person-item table

SELECT user_id, 
       course_id,
    # PA.item_id as item_id,
    CI.item_short_id as item_short_id,
    CI.item_nid as item_nid,
    item_grade,
    grade,
    n_attempts,
    date
FROM
(
    SELECT user_id,
           course_id,
        item.answer_id as item_id,
        if(item.correct_bool, 1, 0) as item_grade,
	grade,
        attempts as n_attempts,
        max(created) as date,
    FROM [{dataset}.problem_analysis]
    group by user_id, 
             course_id, 
             item_id, 
             item_grade, 
	     grade,
             n_attempts  # force (user_id, item_id) to be unique (it should always be, even w/o this)
) PA
JOIN [{dataset}.course_item] CI
on PA.item_id = CI.item_id
order by user_id, CI.content_index, CI.item_number
    """.format(dataset=dataset, course_id=course_id)

    depends_on = [ "%s.course_item" % dataset,
                   "%s.problem_analysis" % dataset
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
                                    depends_on=depends_on,
                                    force_query=force_recompute,
                                    startIndex=-2)
    except Exception as err:
        print "[make_person_item_table] ERR! failed in creating %s.%s using this sql:" % (dataset, tablename)
        print the_sql
        raise

    if not bqdat:
        nfound = 0
    else:
        nfound = bqutil.get_bq_table_size_rows(dataset, tablename)
    print "--> Done with %s for %s, %d entries found" % (tablename, course_id, nfound)
    sys.stdout.flush()
    

def create_person_problem_table(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Generate person_problem table, with one row per (user_id, problem_id), giving problem raw_score earned, attempts,
    and datestamp.

    Computed by aggregating over person_item, and joining with course_item
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "person_problem"

    the_sql = """
# compute person-problem table for {course_id}

SELECT user_id,
       course_id,
    CI.problem_nid as problem_nid,
    sum(item_grade) as problem_raw_score,
    sum(item_grade) / sum(CI.item_points_possible) * 100 as problem_pct_score,
    max(PI.grade) as grade,
    max(n_attempts) as n_attempts,
    max(date) as date,
    
FROM [{dataset}.person_item] PI
JOIN [{dataset}.course_item] CI
    
on PI.item_nid = CI.item_nid
group by user_id, course_id, problem_nid
order by user_id, course_id, problem_nid
    """.format(dataset=dataset, course_id=course_id)

    depends_on = [ "%s.course_item" % dataset,
                   "%s.person_item" % dataset
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
                                    depends_on=depends_on,
                                    force_query=force_recompute,
                                    startIndex=-2)
    except Exception as err:
        print "[make_person_problem_table] ERR! failed in creating %s.%s using this sql:" % (dataset, tablename)
        print the_sql
        raise

    if not bqdat:
        nfound = 0
    else:
        nfound = bqutil.get_bq_table_size_rows(dataset, tablename)
    print "--> Done with %s for %s, %d entries found" % (tablename, course_id, nfound)
    sys.stdout.flush()
    

def create_course_problem_table(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Generate course_problem table, with one row per (problem_id), giving average points, standard deviation on points,
    number of unique users attempted, max points possible.

    Uses person_item and course_item.
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "course_problem"

    the_sql = """
# compute course_problem table for {course_id}
SELECT course_id, problem_nid, problem_id, problem_short_id, 
  avg(problem_grade) as avg_problem_raw_score,
  stddev(problem_grade) as sdv_problem_raw_score,
  # max(problem_grade) as max_problem_raw_score,
  max(possible_raw_score) as max_possible_raw_score,
  avg(problem_grade / possible_raw_score * 100) as avg_problem_pct_score,
  count(unique(user_id)) as n_unique_users_attempted,
  problem_name,
  is_split,
  split_name,
FROM
(
    SELECT course_id, problem_nid, problem_id, problem_short_id, sum(item_grade) as problem_grade, user_id,
        sum(CI.item_points_possible) as possible_raw_score, problem_name, is_split, split_name,
    FROM [{dataset}.person_item] PI
    JOIN [{dataset}.course_item] CI
    on PI.item_nid = CI.item_nid
    group by course_id, problem_nid, problem_short_id, problem_id, user_id, problem_name, is_split, split_name
)
group by course_id, problem_nid, problem_id, problem_short_id, problem_name, is_split, split_name
# order by problem_short_id
order by avg_problem_pct_score desc
    """.format(dataset=dataset, course_id=course_id)

    depends_on = [ "%s.course_item" % dataset,
                   "%s.person_item" % dataset
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
                                    depends_on=depends_on,
                                    force_query=force_recompute,
                                    startIndex=-2)
    except Exception as err:
        print "[make_course_problem_table] ERR! failed in creating %s.%s using this sql:" % (dataset, tablename)
        print the_sql
        raise

    if not bqdat:
        nfound = 0
    else:
        nfound = bqutil.get_bq_table_size_rows(dataset, tablename)
    print "--> Done with %s for %s, %d entries found" % (tablename, course_id, nfound)
    sys.stdout.flush()
    
