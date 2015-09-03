#!/usr/bin/python
'''
Make tables for IRT analyses.

1. course-item table, with one row per assessment item.

   Currently only includes capa problems (LTI and other xmodules / xblocks not included)

2. person-item table, with one row per item & person.  Only includes items in the course-item table.

'''

import sys
import bqutil

def make_item_tables(course_id, force_recompute=False, use_dataset_latest=False):
    create_course_item_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)
    create_person_item_table(course_id, force_recompute=force_recompute, use_dataset_latest=use_dataset_latest)


def create_course_item_table(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    the course_item dataset has these columns:

    Field Name                              Type    Example         Description
    item_id                                 string  i4x-MITx-8_MReV-problem-CheckPoint_1_Newton_s_First_Law_2_1     
                                                                    Unique ID for an assessment item (constructed using the problem module_id, and linked to problem_analysis table keys)
    problem_id                              string  CheckPoint_1_Newton_s_First_Law 
                                                                    Unique ID for an assessment problem (constructed using problem url_name)
    item_weight                             float   6.59E-05        Fraction of overall grade (between 0 and 1) contributed by this item
    n_user_responses                        integer 4868            Number of users who provided a response to this assessment item
    problem_name                            string  CheckPoint 1: Newton's First Law        
                                                                    Name of problem within which this item exists
    assignment_id                           string  Checkpoint_ch3  Unique ID for the assignment within which the problem exists
    n_problems_in_assignment                integer 23              Number of problems within the assignment
    assignment_type                         string  Checkpoint      The assignment type within which the assignment exists
    assignment_type_weight                  float   0.1             Fraction of the overall grade contributed by the assignment type
    n_assignments_of_type                   integer 11              Number of assignments of this type
    chapter_number                          integer 3               Number of the chapter within which the problem exists
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
    cumulative_item_weight                  float   6.59E-05        Cumulative fraction of item weights (for debugging: should increase to 1.0 by the end of table)


    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "course_item"

    the_sql = """
# make course-item dataset
# items with weight and cumulative weight column (for double-checking - should end with 1.0)
SELECT 
    # '{course_id}' as course_id,
    *,
    sum(item_weight) over (order by content_index, item_number) cumulative_item_weight
FROM
(
    # items with additional data about fraction_of_overall_grade from grading_policy
    SELECT item_id, 
        problem_id,
        (problem_weight * GP.fraction_of_overall_grade / n_items / sum_problem_weight_in_assignment / n_assignments_of_type) as item_weight,
        n_user_responses,
        problem_name,
        CI.assignment_id as assignment_id,
        n_problems_in_assignment,
        CI.assignment_type as assignment_type,
        GP.fraction_of_overall_grade as assignment_type_weight,
        n_assignments_of_type,
        chapter_number,
        content_index,
        problem_weight,
        item_points_possible,
        problem_points_possible,
        emperical_item_points_possible,
        emperical_problem_points_possible,
        item_number,
        n_items,
        start_date,
        due_date,
        problem_path,
    FROM
    (
        # items with number of problems per assignment
        SELECT item_id, item_number,
            n_items,
            problem_id,
            n_user_responses,
            problem_name,
            assignment_id,
            sum(if(assignment_id is not null and item_number=1, 1, 0)) over (partition by assignment_id) n_problems_in_assignment,
            sum(if(assignment_id is not null and item_number=1, problem_weight, 0)) 
                over (partition by assignment_id) sum_problem_weight_in_assignment,
            assignment_type,
            n_assignments_of_type,
            chapter_number,
            problem_path,
            content_index,
            start_date,
            due_date,
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
                assignment_id,
                assignment_type,
                n_assignments_of_type,
                CA.chapter_number as chapter_number,
                CA.path as problem_path,
                CA.index as content_index,
                CA.start as start_date,
                CA.due as due_date,
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
                # course_axis with chapter number and number of assignments of type
                SELECT *,  # add column with number of assignments of type
                    SUM(IF(problem_number=1, 1, 0)) over (partition by assignment_type) n_assignments_of_type,
                FROM
                (
                    SELECT *,  # add column with problem number within assignment_id
                        row_number() over (partition by assignment_id order by index) problem_number,
                    FROM
                    (
                        SELECT module_id,
                            url_name,
                            index,
                            If(data.weight is null, 1.0, data.weight) as weight,
                            gformat as assignment_type,
                            chapter_number,
                            CONCAT(gformat, "_ch", STRING(chapter_number)) as assignment_id,  #  assignment_id = assignment_type + ch_chapter_number
                            name,
                            path,
                            start,
                            due,
                        FROM [{dataset}.course_axis] CAI
                        JOIN
                        (   # join course_axis with itself to get chapter_number
                            SELECT module_id as chapter_mid, 
                                row_number() over (partition by category order by index) as chapter_number
                            FROM  [{dataset}.course_axis] 
                            where category = "chapter"
                            order by index
                        ) CHN
                        ON CAI.chapter_mid = CHN.chapter_mid
                        where gformat is not null
                    )
                )
                order by index
            ) CA
            ON PA.problem_id = CA.url_name
        )
    ) CI
    LEFT JOIN [{dataset}.grading_policy] GP
    ON CI.assignment_type = GP.assignment_type
    order by content_index, item_number
)
order by content_index, item_number
    """.format(dataset=dataset, course_id=course_id)

    depends_on = [ "%s.course_axis" % dataset,
                   "%s.grading_policy" % dataset,
                   "%s.problem_analysis" % dataset
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
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
    PA.item_id as item_id,
    item_grade,
    n_attempts,
    date
FROM
(
    SELECT user_id,
        item.answer_id as item_id,
        if(item.correct_bool, 1, 0) as item_grade,
        attempts as n_attempts,
        max(created) as date,
    FROM [{dataset}.problem_analysis]
    group by user_id, item_id, item_grade, n_attempts  # force (user_id, item_id) to be unique (it should always be, even w/o this)
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
    
