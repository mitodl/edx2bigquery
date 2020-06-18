import util
from .. import bqutil

NUM_TEST_COURSES = 5

def fetch_test_course_ids():
  '''
  Returns a Pandas Series of course_id strings.
  These course_ids will be used as the test courses for testing analysis functions.
  '''
  SQL = "SELECT course_id FROM [mitx-research:unit_testing.test_courses]"
  return util.bqutil_SQL2df(SQL).course_id

def ans_coupling_test1 (
	dataset_id, 
	project_id='mitx-research', 
	test_course_ids=None, 
	what_changed=None,
  output_project='mitx-research', 
  output_dataset='unit_testing', 
  output_table="ans_coupling_test1__avg_change_v1",
):
  '''
  Compares average feature values of previous ans_coupling tables
  with the latest tables.

  Returns a dataframe with the comparitive results of the test. 
  The result can also be viewed as a table in Google BigQuery.
  '''

  if what_changed is None:
    what_changed = "No update message provided."

  if test_course_ids is None:
    test_course_ids = fetch_test_course_ids()

  # Check if this is the first iteration of this test
  table_exists=bqutil.get_bq_table_size_rows(output_dataset, output_table, output_project) is not None

  tables_string = util.SQL_FROM_string_from_course_ids(test_course_ids, table_name=None,project_id=project_id, dataset_id=dataset_id, table_suffix="_stats_ans_coupling")

  previous_test = '''
	  (
	    # Fetch last test, only the last N rows, where N is the number of test courses.
	    SELECT
	      test_time,
	      what_changed,
	      course_id,
	      cnt,
	      avg_cheating_likelihood,
	      min_cheating_likelihood,
	      max_cheating_likelihood,
	      avg_wilsons_interval_cheating_score,
	      min_wilsons_interval_cheating_score,
	      max_wilsons_interval_cheating_score,
	      avg_x,
	      avg_n,
	      avg_dt_p50,
	      avg_dt_p90,
	      avg_nsame_ip,
	      avg_ncorrect,
	      avg_ncameo_both,
	      avg_percent_same_ip_given_sab,
	      avg_ha_ma_dt_correlation,
	      avg_x05m_same_ip,
	      avg_x15s,
	      avg_dt_dt_p80
	    FROM 
	    (
	      SELECT *, COUNT(1) OVER () AS nrows, ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num
	      FROM [{output_table}]
	    )
	    WHERE row_num > nrows - {ntestcourses} 
	  ),'''.format(output_table=output_project+':'+output_dataset+'.'+output_table, ntestcourses=NUM_TEST_COURSES)

  SQL = '''
  SELECT 
	  test_time,
	  what_changed,
	  course_id,
	  cnt / cnt_lag - 1 AS cnt_change,
	  FLOAT(avg_cheating_likelihood / avg_cheating_likelihood_lag - 1) AS avg_cheating_likelihood_change,
	  FLOAT(min_cheating_likelihood / min_cheating_likelihood_lag - 1) AS min_cheating_likelihood_change,
	  FLOAT(max_cheating_likelihood / max_cheating_likelihood_lag - 1) AS max_cheating_likelihood_change,
	  FLOAT(avg_wilsons_interval_cheating_score / avg_wilsons_interval_cheating_score_lag - 1) AS avg_wilsons_interval_cheating_score_change,
	  FLOAT(min_wilsons_interval_cheating_score / min_wilsons_interval_cheating_score_lag - 1) AS min_wilsons_interval_cheating_score_change,
	  FLOAT(max_wilsons_interval_cheating_score / max_wilsons_interval_cheating_score_lag - 1) AS max_wilsons_interval_cheating_score_change,
	  avg_x / avg_x_lag - 1 AS avg_x_change,
	  avg_n / avg_n_lag - 1 AS avg_n_change,
	  avg_dt_p50 / avg_dt_p50_lag - 1 AS avg_dt_p50_change,
	  avg_dt_p90 / avg_dt_p90_lag - 1 AS avg_dt_p90_change,
	  avg_nsame_ip / avg_nsame_ip_lag - 1 AS avg_nsame_ip_change,
	  avg_ncorrect / avg_ncorrect_lag - 1 AS avg_ncorrect_change,
	  avg_ncameo_both / avg_ncameo_both_lag - 1 AS avg_ncameo_both_change,
	  avg_percent_same_ip_given_sab / avg_percent_same_ip_given_sab_lag - 1 AS avg_percent_same_ip_given_sab_change,
	  avg_ha_ma_dt_correlation / avg_ha_ma_dt_correlation_lag - 1 AS avg_ha_ma_dt_correlation_change,
	  avg_x05m_same_ip / avg_x05m_same_ip_lag - 1 AS avg_x05m_same_ip_change,
	  avg_x15s / avg_x15s_lag - 1AS avg_x15s_change,
	  avg_dt_dt_p80 / avg_dt_dt_p80_lag - 1 AS avg_dt_dt_p80_change,
	  cnt,
	  FLOAT(avg_cheating_likelihood) AS avg_cheating_likelihood,
	  FLOAT(min_cheating_likelihood) AS min_cheating_likelihood,
	  FLOAT(max_cheating_likelihood) AS max_cheating_likelihood,
	  FLOAT(avg_wilsons_interval_cheating_score) AS avg_wilsons_interval_cheating_score,
	  FLOAT(min_wilsons_interval_cheating_score) AS min_wilsons_interval_cheating_score,
	  FLOAT(max_wilsons_interval_cheating_score) AS max_wilsons_interval_cheating_score,
	  avg_x,
	  avg_n,
	  avg_dt_p50,
	  avg_dt_p90,
	  avg_nsame_ip,
	  avg_ncorrect,
	  avg_ncameo_both,
	  avg_percent_same_ip_given_sab,
	  avg_ha_ma_dt_correlation,
	  avg_x05m_same_ip,
	  avg_x15s,
	  avg_dt_dt_p80
	FROM
	(
	  SELECT
	    *,
	    COUNT(1) OVER () AS nrows,
	    ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num,
	    LAG(cnt, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS cnt_lag,
	    LAG(avg_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_cheating_likelihood_lag,
	    LAG(min_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS min_cheating_likelihood_lag,
	    LAG(max_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS max_cheating_likelihood_lag,
	    LAG(avg_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_wilsons_interval_cheating_score_lag,
	    LAG(min_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS min_wilsons_interval_cheating_score_lag,
	    LAG(max_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS max_wilsons_interval_cheating_score_lag,
	    LAG(avg_x, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x_lag,
	    LAG(avg_n, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_n_lag,
	    LAG(avg_dt_p50, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p50_lag,
	    LAG(avg_dt_p90, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p90_lag,
	    LAG(avg_nsame_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_nsame_ip_lag,
	    LAG(avg_ncorrect, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncorrect_lag,
	    LAG(avg_ncameo_both, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncameo_both_lag,
	    LAG(avg_percent_same_ip_given_sab, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_percent_same_ip_given_sab_lag,
	    LAG(avg_ha_ma_dt_correlation, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ha_ma_dt_correlation_lag,
	    LAG(avg_x05m_same_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x05m_same_ip_lag,
	    LAG(avg_x15s, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x15s_lag,
	    LAG(avg_dt_dt_p80, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_dt_p80_lag
	  FROM {previous_test}
	  (  
	    SELECT 
	      CURRENT_TIMESTAMP() as test_time,
		    "{what_changed}" as what_changed,
		    course_id,
	      CAST(COUNT(1) AS INTEGER) AS cnt,
	      AVG(cheating_likelihood) AS avg_cheating_likelihood,
	      MIN(cheating_likelihood) AS min_cheating_likelihood,
	      MAX(cheating_likelihood) AS max_cheating_likelihood,
	      AVG(wilsons_interval_cheating_score) AS avg_wilsons_interval_cheating_score,
	      MIN(wilsons_interval_cheating_score) AS min_wilsons_interval_cheating_score,
	      MAX(wilsons_interval_cheating_score) AS max_wilsons_interval_cheating_score,
	      AVG(show_ans_before) AS avg_x,
	      AVG(prob_in_common) as avg_n,
	      AVG(median_max_dt_seconds) as avg_dt_p50,
	      AVG(percentile90_dt_seconds) as avg_dt_p90,
	      AVG(nsame_ip) as avg_nsame_ip,
	      AVG(ncorrect) as avg_ncorrect,
	      AVG(ncameo_both) as avg_ncameo_both,
	      AVG(percent_same_ip_given_sab) as avg_percent_same_ip_given_sab,
	      AVG(ha_ma_dt_correlation) as avg_ha_ma_dt_correlation,
	      AVG(x05m_same_ip) as avg_x05m_same_ip,
	      AVG(x15s) as avg_x15s,
	      AVG(dt_dt_p80) as avg_dt_dt_p80
	    FROM 
	      {tables}
	    GROUP BY course_id
	    ORDER BY course_id
	  )
	) # We will append to previous tests, so only take the last NUM_TEST_COURSES records.
	WHERE row_num > nrows - {ntestcourses}
  '''.format(
  	tables=tables_string,
  	what_changed=what_changed if table_exists else "Initial baseline test.",
  	previous_test=previous_test if table_exists else "",
  	ntestcourses=NUM_TEST_COURSES,
  )

  return util.bqutil_SQL2df(
    SQL,
    temp_project_id=output_project, 
    temp_dataset=output_dataset, 
    temp_table=output_table, 
    persistent=True, 
    overwrite='append' if table_exists else True,
  )
    
def cameo_test1 (
	dataset_id, 
	project_id='mitx-research', 
	test_course_ids=None, 
	what_changed=None,
  output_project='mitx-research', 
  output_dataset='unit_testing', 
  output_table="cameo_test1__avg_change_v1",
):
  '''
  Compares average feature values of previous cameo tables
  with the latest tables.

  Returns a dataframe with the comparitive results of the test. 
  The result can also be viewed as a table in Google BigQuery.
  '''

  if what_changed is None:
    what_changed = "No update message provided."

  if test_course_ids is None:
    test_course_ids = fetch_test_course_ids()

  # Check if this is the first iteration of this test
  table_exists=bqutil.get_bq_table_size_rows(output_dataset, output_table, output_project) is not None

  tables_string = util.SQL_FROM_string_from_course_ids(test_course_ids, table_name=None,project_id=project_id, dataset_id=dataset_id, table_prefix="cameo2_")

  previous_test = '''
	  (
	    # Fetch last test, only the last N rows, where N is the number of test courses.
	    SELECT
	      test_time,
	      what_changed,
	      course_id,
        cnt,
        avg_x,
        avg_n,
        avg_dt_p50,
        avg_dt_p90,
        avg_nsame_ip,
        avg_ncorrect,
        avg_ncameo_both,
        avg_percent_same_ip_given_sab,
        avg_sa_ca_dt_correlation,
        avg_x05m_same_ip,
        avg_x15s,
        avg_dt_dt_p80
	    FROM 
	    (
	      SELECT *, COUNT(1) OVER () AS nrows, ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num
	      FROM [{output_table}]
	    )
	    WHERE row_num > nrows - {ntestcourses} 
	  ),'''.format(output_table=output_project+':'+output_dataset+'.'+output_table, ntestcourses=NUM_TEST_COURSES)

  SQL = '''
  SELECT 
	  test_time,
	  what_changed,
	  course_id,
	  cnt / cnt_lag - 1 AS cnt_change,
	  avg_x / avg_x_lag - 1 AS avg_x_change,
	  avg_n / avg_n_lag - 1 AS avg_n_change,
	  avg_dt_p50 / avg_dt_p50_lag - 1 AS avg_dt_p50_change,
	  avg_dt_p90 / avg_dt_p90_lag - 1 AS avg_dt_p90_change,
	  avg_nsame_ip / avg_nsame_ip_lag - 1 AS avg_nsame_ip_change,
	  avg_ncorrect / avg_ncorrect_lag - 1 AS avg_ncorrect_change,
	  avg_ncameo_both / avg_ncameo_both_lag - 1 AS avg_ncameo_both_change,
	  avg_percent_same_ip_given_sab / avg_percent_same_ip_given_sab_lag - 1 AS avg_percent_same_ip_given_sab_change,
	  avg_sa_ca_dt_correlation / avg_sa_ca_dt_correlation_lag - 1 AS avg_sa_ca_dt_correlation_change,
	  avg_x05m_same_ip / avg_x05m_same_ip_lag - 1 AS avg_x05m_same_ip_change,
	  avg_x15s / avg_x15s_lag - 1AS avg_x15s_change,
	  avg_dt_dt_p80 / avg_dt_dt_p80_lag - 1 AS avg_dt_dt_p80_change,
	  cnt,
	  avg_x,
	  avg_n,
	  avg_dt_p50,
	  avg_dt_p90,
	  avg_nsame_ip,
	  avg_ncorrect,
	  avg_ncameo_both,
	  avg_percent_same_ip_given_sab,
	  avg_sa_ca_dt_correlation,
	  avg_x05m_same_ip,
	  avg_x15s,
	  avg_dt_dt_p80
	FROM
	(
	  SELECT
	    *,
	    COUNT(1) OVER () AS nrows,
	    ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num,
	    LAG(cnt, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS cnt_lag,
	    LAG(avg_x, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x_lag,
	    LAG(avg_n, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_n_lag,
	    LAG(avg_dt_p50, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p50_lag,
	    LAG(avg_dt_p90, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p90_lag,
	    LAG(avg_nsame_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_nsame_ip_lag,
	    LAG(avg_ncorrect, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncorrect_lag,
	    LAG(avg_ncameo_both, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncameo_both_lag,
	    LAG(avg_percent_same_ip_given_sab, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_percent_same_ip_given_sab_lag,
	    LAG(avg_sa_ca_dt_correlation, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_sa_ca_dt_correlation_lag,
	    LAG(avg_x05m_same_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x05m_same_ip_lag,
	    LAG(avg_x15s, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x15s_lag,
	    LAG(avg_dt_dt_p80, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_dt_p80_lag
	  FROM {previous_test}
	  (  
	    SELECT
		    CURRENT_TIMESTAMP() as test_time,
		    "{what_changed}" as what_changed,
		    course_id,
			  CAST(COUNT(1) AS INTEGER) AS cnt,
		    AVG(X) AS avg_x,
		    AVG(N) as avg_n,
		    AVG(percentile50_dt_seconds) as avg_dt_p50,
		    AVG(percentile90_dt_seconds) as avg_dt_p90,
		    AVG(nsame_ip) as avg_nsame_ip,
		    avg(ncorrect) as avg_ncorrect,
		    avg(ncameo_both) as avg_ncameo_both,
		    avg(percent_same_ip_given_sab) as avg_percent_same_ip_given_sab,
		    avg(sa_ca_dt_correlation) as avg_sa_ca_dt_correlation,
		    avg(x05m_same_ip) as avg_x05m_same_ip,
		    avg(x15s) as avg_x15s,
		    avg(dt_dt_p80) as avg_dt_dt_p80
		  FROM {tables}
		  GROUP BY course_id
	    ORDER BY course_id
	  )
	) # We will append to previous tests, so only take the last NUM_TEST_COURSES records.
	WHERE row_num > nrows - {ntestcourses}
  '''.format(
  	tables=tables_string,
  	what_changed=what_changed if table_exists else "Initial baseline test.",
  	previous_test=previous_test if table_exists else "",
  	ntestcourses=NUM_TEST_COURSES,
  )

  return util.bqutil_SQL2df(
    SQL,
    temp_project_id=output_project, 
    temp_dataset=output_dataset, 
    temp_table=output_table, 
    persistent=True, 
    overwrite='append' if table_exists else True,
  )
    
def sab_test1 (
	dataset_id, 
	project_id='mitx-research', 
	test_course_ids=None, 
	what_changed=None,
  output_project='mitx-research', 
  output_dataset='unit_testing', 
  output_table="sab_test1__avg_change_v1",
):
  '''
  Compares average feature values of previous show_ans_before tables
  with the latest tables.

  Returns a dataframe with the comparitive results of the test. 
  The result can also be viewed as a table in Google BigQuery.
  '''

  if what_changed is None:
    what_changed = "No update message provided."

  if test_course_ids is None:
    test_course_ids = fetch_test_course_ids()

  # Check if this is the first iteration of this test
  table_exists=bqutil.get_bq_table_size_rows(output_dataset, output_table, output_project) is not None

  tables_string = util.SQL_FROM_string_from_course_ids(test_course_ids, table_name=None,project_id=project_id, dataset_id=dataset_id, table_suffix="_stats_show_ans_before")

  previous_test = '''
	  (
	    # Fetch last test, only the last N rows, where N is the number of test courses.
	    SELECT
	      test_time,
	      what_changed,
	      course_id,
        INTEGER(cnt) AS cnt,
        FLOAT(avg_cheating_likelihood) AS avg_cheating_likelihood,
        FLOAT(min_cheating_likelihood) AS min_cheating_likelihood,
        FLOAT(max_cheating_likelihood) AS max_cheating_likelihood,
        FLOAT(avg_wilsons_interval_cheating_score) AS avg_wilsons_interval_cheating_score,
        FLOAT(min_wilsons_interval_cheating_score) AS min_wilsons_interval_cheating_score,
        FLOAT(max_wilsons_interval_cheating_score) AS max_wilsons_interval_cheating_score,
        FLOAT(avg_x) AS avg_x,
        FLOAT(avg_n) AS avg_n,
        FLOAT(avg_dt_p50) AS avg_dt_p50,
        FLOAT(avg_dt_p90) AS avg_dt_p90,
        FLOAT(avg_nsame_ip) AS avg_nsame_ip,
        FLOAT(avg_ncorrect) AS avg_ncorrect,
        FLOAT(avg_ncameo_both) AS avg_ncameo_both,
        FLOAT(avg_percent_same_ip_given_sab) AS avg_percent_same_ip_given_sab,
        FLOAT(avg_sa_ca_dt_correlation) AS avg_sa_ca_dt_correlation,
        FLOAT(avg_x05m_same_ip) AS avg_x05m_same_ip,
        FLOAT(avg_x15s) AS avg_x15s,
        FLOAT(avg_dt_dt_p80) AS avg_dt_dt_p80
	    FROM 
	    (
	      SELECT *, COUNT(1) OVER () AS nrows, ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num
	      FROM [{output_table}]
	    )
	    WHERE row_num > nrows - {ntestcourses} 
	  ),'''.format(output_table=output_project+':'+output_dataset+'.'+output_table, ntestcourses=NUM_TEST_COURSES)

  SQL = '''
  SELECT 
	  test_time,
	  what_changed,
	  course_id,
	  FLOAT(cnt / cnt_lag - 1) AS cnt_change,
	  FLOAT(avg_cheating_likelihood / avg_cheating_likelihood_lag - 1) AS avg_cheating_likelihood_change,
	  FLOAT(min_cheating_likelihood / min_cheating_likelihood_lag - 1) AS min_cheating_likelihood_change,
	  FLOAT(max_cheating_likelihood / max_cheating_likelihood_lag - 1) AS max_cheating_likelihood_change,
	  FLOAT(avg_wilsons_interval_cheating_score / avg_wilsons_interval_cheating_score_lag - 1) AS avg_wilsons_interval_cheating_score_change,
	  FLOAT(min_wilsons_interval_cheating_score / min_wilsons_interval_cheating_score_lag - 1) AS min_wilsons_interval_cheating_score_change,
	  FLOAT(max_wilsons_interval_cheating_score / max_wilsons_interval_cheating_score_lag - 1) AS max_wilsons_interval_cheating_score_change,
	  FLOAT(avg_x / avg_x_lag - 1) AS avg_x_change,
	  FLOAT(avg_n / avg_n_lag - 1) AS avg_n_change,
	  FLOAT(avg_dt_p50 / avg_dt_p50_lag - 1) AS avg_dt_p50_change,
	  FLOAT(avg_dt_p90 / avg_dt_p90_lag - 1) AS avg_dt_p90_change,
	  FLOAT(avg_nsame_ip / avg_nsame_ip_lag - 1) AS avg_nsame_ip_change,
	  FLOAT(avg_ncorrect / avg_ncorrect_lag - 1) AS avg_ncorrect_change,
	  FLOAT(avg_ncameo_both / avg_ncameo_both_lag - 1) AS avg_ncameo_both_change,
	  FLOAT(avg_percent_same_ip_given_sab / avg_percent_same_ip_given_sab_lag - 1) AS avg_percent_same_ip_given_sab_change,
	  FLOAT(avg_sa_ca_dt_correlation / avg_sa_ca_dt_correlation_lag - 1) AS avg_sa_ca_dt_correlation_change,
	  FLOAT(avg_x05m_same_ip / avg_x05m_same_ip_lag - 1) AS avg_x05m_same_ip_change,
	  FLOAT(avg_x15s / avg_x15s_lag - 1) AS avg_x15s_change,
	  FLOAT(avg_dt_dt_p80 / avg_dt_dt_p80_lag - 1) AS avg_dt_dt_p80_change,
	  cnt,
	  FLOAT(avg_cheating_likelihood) AS avg_cheating_likelihood,
	  FLOAT(min_cheating_likelihood) AS min_cheating_likelihood,
	  FLOAT(max_cheating_likelihood) AS max_cheating_likelihood,
	  FLOAT(avg_wilsons_interval_cheating_score) AS avg_wilsons_interval_cheating_score,
	  FLOAT(min_wilsons_interval_cheating_score) AS min_wilsons_interval_cheating_score,
	  FLOAT(max_wilsons_interval_cheating_score) AS max_wilsons_interval_cheating_score,
	  FLOAT(avg_x) AS avg_x,
	  FLOAT(avg_n) AS avg_n,
	  FLOAT(avg_dt_p50) AS avg_dt_p50,
	  FLOAT(avg_dt_p90) AS avg_dt_p90,
	  FLOAT(avg_nsame_ip) AS avg_nsame_ip,
	  FLOAT(avg_ncorrect) AS avg_ncorrect,
	  FLOAT(avg_ncameo_both) AS avg_ncameo_both,
	  FLOAT(avg_percent_same_ip_given_sab) AS avg_percent_same_ip_given_sab,
	  FLOAT(avg_sa_ca_dt_correlation) AS avg_sa_ca_dt_correlation,
	  FLOAT(avg_x05m_same_ip) AS avg_x05m_same_ip,
	  FLOAT(avg_x15s) AS avg_x15s,
	  FLOAT(avg_dt_dt_p80) AS avg_dt_dt_p80
	FROM
	(
	  SELECT
	    *,
	    COUNT(1) OVER () AS nrows,
	    ROW_NUMBER() over (ORDER BY test_time, course_id) AS row_num,
	    LAG(cnt, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS cnt_lag,
	    LAG(avg_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_cheating_likelihood_lag,
	    LAG(min_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS min_cheating_likelihood_lag,
	    LAG(max_cheating_likelihood, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS max_cheating_likelihood_lag,
	    LAG(avg_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_wilsons_interval_cheating_score_lag,
	    LAG(min_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS min_wilsons_interval_cheating_score_lag,
	    LAG(max_wilsons_interval_cheating_score, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS max_wilsons_interval_cheating_score_lag,
	    LAG(avg_x, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x_lag,
	    LAG(avg_n, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_n_lag,
	    LAG(avg_dt_p50, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p50_lag,
	    LAG(avg_dt_p90, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_p90_lag,
	    LAG(avg_nsame_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_nsame_ip_lag,
	    LAG(avg_ncorrect, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncorrect_lag,
	    LAG(avg_ncameo_both, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_ncameo_both_lag,
	    LAG(avg_percent_same_ip_given_sab, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_percent_same_ip_given_sab_lag,
	    LAG(avg_sa_ca_dt_correlation, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_sa_ca_dt_correlation_lag,
	    LAG(avg_x05m_same_ip, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x05m_same_ip_lag,
	    LAG(avg_x15s, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_x15s_lag,
	    LAG(avg_dt_dt_p80, {ntestcourses}) OVER (ORDER BY test_time, course_id) AS avg_dt_dt_p80_lag
	  FROM {previous_test}
	  (  
	    SELECT 
	      CURRENT_TIMESTAMP() as test_time,
		    "{what_changed}" as what_changed,
		    course_id,
	      CAST(COUNT(1) AS INTEGER) AS cnt,
	      AVG(cheating_likelihood) AS avg_cheating_likelihood,
	      MIN(cheating_likelihood) AS min_cheating_likelihood,
	      MAX(cheating_likelihood) AS max_cheating_likelihood,
	      AVG(wilsons_interval_cheating_score) AS avg_wilsons_interval_cheating_score,
	      MIN(wilsons_interval_cheating_score) AS min_wilsons_interval_cheating_score,
	      MAX(wilsons_interval_cheating_score) AS max_wilsons_interval_cheating_score,
	      AVG(show_ans_before) AS avg_x,
	      AVG(prob_in_common) as avg_n,
	      AVG(median_max_dt_seconds) as avg_dt_p50,
	      AVG(percentile90_dt_seconds) as avg_dt_p90,
	      AVG(nsame_ip) as avg_nsame_ip,
	      AVG(ncorrect) as avg_ncorrect,
	      AVG(ncameo_both) as avg_ncameo_both,
	      AVG(percent_same_ip_given_sab) as avg_percent_same_ip_given_sab,
	      AVG(sa_ca_dt_correlation) as avg_sa_ca_dt_correlation,
	      AVG(x05m_same_ip) as avg_x05m_same_ip,
	      AVG(x15s) as avg_x15s,
	      AVG(dt_dt_p80) as avg_dt_dt_p80
	    FROM 
	      {tables}
	    GROUP BY course_id
	    ORDER BY course_id
	  )
	) # We will append to previous tests, so only take the last NUM_TEST_COURSES records.
	WHERE row_num > nrows - {ntestcourses} 
  '''.format(
  	tables=tables_string,
  	what_changed=what_changed if table_exists else "Initial baseline test.",
  	previous_test=previous_test if table_exists else "",
  	ntestcourses=NUM_TEST_COURSES,
  )

  return util.bqutil_SQL2df(
    SQL,
    temp_project_id=output_project, 
    temp_dataset=output_dataset, 
    temp_table=output_table, 
    persistent=True, 
    overwrite='append' if table_exists else True,
  )
    
