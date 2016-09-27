#!/usr/bin/python
'''
Make item_response_theory_report table.

This extracts data from item_irt_grm[_R], course_item,
course_problem, and item_reliabilities.  This table is used in the XAnalytics reporting on IRT.
Note that item_reliabilities and item_irt_grm[_R] are computed using external commands, using
Stata and/or R.  If item_irt_grm (produced by Stata) is available, that is used, in preference to
item_irt_grm_R (produced by mirt in R).  This report summarizes, in one place, statistics about
problem difficulty and discrimination, Cronbach's alpha, item-test and item-rest correlations,
average problem raw scores, average problem percent scores, number of unique users attempted.
Standard errors are also provided for difficulty and discrimination.  Requires the IRT tables to
already have been computed.  

'''

import sys
import bqutil
import datetime
from collections import OrderedDict

def make_irt_report(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    '''
    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    tablename = "course_item"

    the_sql = """
# item_response_theory_report for {course_id}
#
# problem_nid,problem_short_id,chapter,assignment_type,problem_label,problem_id,IRT item number,avg_problem_raw_score,avg_problem_pct_score,
# n_unique_users_attempted,item_test,item_rest,alpha ,Discrimination,Difficulty

SELECT 
    "{course_id}" as course_id,
    IG.problem_nid as problem_nid,
    CP.problem_short_id as problem_short_id,
    CI.chapter_name as chapter,
    assignment_type,
    CONCAT("[", STRING(IG.problem_nid), "] ", CI.chapter_name, " / ", CI.section_name, " / ", CP.problem_name) as problem_label,
    CP.problem_id,
    CONCAT(STRING(CP.problem_nid), "/", STRING(cutnum)) as IRT_item_number,
    CP.avg_problem_raw_score avg_problem_raw_score,
    CP.avg_problem_pct_score avg_problem_pct_score,
    CP.n_unique_users_attempted n_unique_users_attempted,
    IR.itemtestcorr as item_test,
    IR.itemrestcorr as item_rest,
    IR.alpha as alpha,
    irt_diff as Difficulty,
    irt_disc as Discrimination,
    diff_se as Difficulty_SE,
    disc_se as Discrimination_SE,
    "{irt_method}" as irt_method,

FROM [{dataset}.{item_irt_grm}] IG
JOIN [{dataset}.course_item] CI
on IG.problem_nid = CI.problem_nid
JOIN 
(
    SELECT *, CONCAT("y", STRING(problem_nid)) as problem_yid,
    FROM [{dataset}.course_problem]
) CP
on IG.problem_nid = CP.problem_nid
JOIN [{dataset}.item_reliabilities] IR
on IR.item = CP.problem_yid
where CI.item_number = 1
    """

    tablename = "item_response_theory_report"
    IRT_TABLES = OrderedDict([ ("item_irt_grm", "STATA GRM"),
                               ("item_irt_grm_R", "R mirt GRM"),
                           ])
    
    irt_table_to_use = None
    for irt_tablename in IRT_TABLES:
        try:
            tinfo = bqutil.get_bq_table_info(dataset, irt_tablename )
            assert tinfo is not None, "%s.%s does not exist" % ( dataset, irt_tablename )
            irt_table_to_use = irt_tablename
            break
        except Exception as err:
            pass
    
    if not irt_table_to_use:
        raise Exception("[make_irt_report] Cannot generate IRT report; requires one of %s" % (','.join(IRT_TABLES.keys())))

    the_sql = the_sql.format(dataset=dataset, course_id=course_id, item_irt_grm=irt_table_to_use, 
                             irt_method=IRT_TABLES[irt_table_to_use])

    depends_on = [ "%s.course_item" % dataset,
                   "%s.course_problem" % dataset,
                   "%s.%s" % (dataset, irt_table_to_use),
                   "%s.item_reliabilities" % dataset,
               ]

    try:
        bqdat = bqutil.get_bq_table(dataset, tablename, the_sql, 
                                    depends_on=depends_on,
                                    force_query=force_recompute,
                                    startIndex=-2)
    except Exception as err:
        print "[make_irt_report] ERR! failed in creating %s.%s using this sql:" % (dataset, tablename)
        print the_sql
        raise

    if not bqdat:
        nfound = 0
    else:
        nfound = bqutil.get_bq_table_size_rows(dataset, tablename)
    print "--> Done with %s for %s, %d problem items found" % (tablename, course_id, nfound)
    sys.stdout.flush()
