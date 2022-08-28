'''
Make table of time spent on a given asset (anything with a unique module_id aka url_name)
'''

import os
import sys
import json
from . import bqutil
import datetime
from . import process_tracking_logs

        
#-----------------------------------------------------------------------------

def process_course_time_on_asset(course_id, force_recompute=False, use_dataset_latest=False, end_date=None,
                                just_do_totals=False, limit_query_size=False, table_max_size_mb=800,
                                skip_totals=False, start_date=None, config_parameter_overrides=None):
    '''
    Create the time_on_asset_daily table, containing module_id, username, date, 
    and time on asset stats.  This table has separate rows for each date, 
    and is ordered in time.  To update it, a new day's logs are
    processed, then the results appended to this table.

    If the table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.

    Compute totals and store in time_on_asset_totals, by summing over all dates, 
    grouped by module_id.

    How are time_on_asset numbers computed?

    See discussion in make_time_on_task.py

    The time_one_asset_daily table has these columns:

    - date: gives day for the data
    - username
    - module_id
    - time_umid5: total time on module (by module_id) in seconds, with a 5-minute timeout
    - time_umid30: total time on module (by module_id) in seconds, with a 30-minute timeout

    '''


    if just_do_totals:
        return process_time_on_task_totals(course_id, force_recompute=force_recompute, 
                                           use_dataset_latest=use_dataset_latest)

    SQL = """
            SELECT
                    "{course_id}" as course_id,
                    date(time) as date,
                    username,
                    module_id,
                    # time_umid5 = total time on module (by module_id) in seconds
                    # time_mid5 has 5 minute timeout, time_mid30 has 30 min timeout
                    SUM( case when dt_umid < 5*60 then dt_umid end ) as time_umid5,
                    SUM( case when dt_umid < 30*60 then dt_umid end ) as time_umid30,
            FROM (
              SELECT time,
                username,
                module_id,
                (time - last_time)/1.0E6 as dt,         # dt is in seconds
                (time - last_time_umid)/1.0E6 as dt_umid,   # dt for (user, module_id) in seconds
                last_time_umid,
              FROM
                (
                SELECT time,
                    username,
                    last_username,
                    module_id,
                    USEC_TO_TIMESTAMP(last_time) as last_time,
                    USEC_TO_TIMESTAMP(last_time_umid) as last_time_umid,                    
                FROM (
                  SELECT time,
                    username,
                    module_id,
                    lag(time, 1) over (partition by username order by time) last_time,
                    lag(username, 1) over (partition by username order by time) last_username,
                    lag(time, 1) over (partition by username, module_id order by time) last_time_umid,
                  FROM
                    (SELECT time, 
                      username,
                      (case when REGEXP_MATCH(module_id, r'.*\"\}}$') then REGEXP_EXTRACT(module_id, r'(.*)\"\}}$')
                            when REGEXP_MATCH(module_id, r'.*\"\]\}}\}}$') then REGEXP_EXTRACT(module_id, r'(.*)\"\]\}}\}}$')
                           else module_id end) as module_id,	# fix some errors in module_id names
                    FROM {DATASETS}
                  )
         
                    WHERE       
                                     module_id is not null
                                     AND username is not null
                                     AND username != ""
                                     and time > TIMESTAMP("{last_date}")
                                     
                    )
                )
              )
              WHERE module_id is not null
                    AND NOT module_id CONTAINS '"'
              GROUP BY date, module_id, username
              ORDER BY date, module_id, username
          """

    table = 'time_on_asset_daily'

    def gdf(row):
        return datetime.datetime.strptime(row['date'], '%Y-%m-%d')

    process_tracking_logs.run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute,
                                                     use_dataset_latest=use_dataset_latest,
                                                     end_date=end_date,
                                                     start_date=start_date,
                                                     get_date_function=gdf,
                                                     days_delta=0,
                                                     has_hash_limit=True,
                                                     newer_than=datetime.datetime(2015,3,15),	# schema change
                                                     table_max_size_mb=table_max_size_mb,
                                                     limit_query_size=limit_query_size)

    if not skip_totals:
        return process_time_on_asset_totals(course_id, force_recompute=force_recompute, 
                                           use_dataset_latest=use_dataset_latest)

    return


def process_time_on_asset_totals(course_id, force_recompute=False, use_dataset_latest=False):
    '''
    Compute total time on asset values, across various subpopulations of users, for different
    assets (labeled by their module id's).  This requires time_on_asset_daily.

    The time_on_asset_totals table has these columns:

    - course_id
    - module_id: ID for the asset (including video, problem, text, vertical, sequential - see course axis)
    - n_unique_users: number of unique users who accessed the asset
    - n_unique_certified: number of unique users who accessed the asset and also earned a certificate
    - mean_tmid5: mean time spent on asset [sec], for the given module_id, with a 5-minute timeout
    - cert_mean_tmid5: mean time spent on asset by certified users [sec], for the given module_id, with a 5-minute timeout
    - mean_tmid30: mean time spent on asset [sec], for the given module_id, with a 30-minute timeout
    - cert_mean_tmid30: mean time spent on asset by certified users [sec], for the given module_id, with a 30-minute timeout
    - median_tmid5: median time spent on asset [sec], for the given module_id, with a 5-minute timeout
    - cert_median_tmid5: median time spent on asset by certified users [sec], for the given module_id, with a 5-minute timeout
    - median_tmid30: median time spent on asset [sec], for the given module_id, with a 30-minute timeout
    - cert_median_tmid30: median time spent on asset by certified users [sec], for the given module_id, with a 30-minute timeout
    - total_tmid5: total time spent on given module_id, in seconds, with a 5-minute timeout
    - cert_total_tmid5: total time spent on given module_id, in seconds, with a 5-minute timeout, by certified users
    - total_tmid30: total time spent on given module_id, in seconds, with a 30-minute timeout
    - cert_total_tmid30: total time spent on given module_id, in seconds, with a 30-minute timeout, by certified users
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    SQL = """
      SELECT 
          "{course_id}" as course_id,
          module_id, 
          EXACT_COUNT_DISTINCT(username) as n_unique_users,
          EXACT_COUNT_DISTINCT (certified_username) as n_unique_certified,
          AVG(time_umid5) as mean_tmid5,
          AVG(cert_time_umid5) as cert_mean_tmid5,
          AVG(time_umid30) as mean_tmid30,
          AVG(cert_time_umid30) as cert_mean_tmid30,
          NTH(26, QUANTILES(time_umid5, 50)) as median_tmid5,
          NTH(26, QUANTILES(cert_time_umid5, 50)) as cert_median_tmid5,
          NTH(26, QUANTILES(time_umid30, 50)) as median_tmid30,
          NTH(26, QUANTILES(cert_time_umid30, 50)) as cert_median_tmid30,
          sum(time_umid5) as total_tmid5,   # total time on module (by module_id) in seconds
          sum(cert_time_umid5) as cert_total_tmid5,
          sum(time_umid30) as total_tmid30, # mid5 has 5 minute timeout, mid30 has 30 min timeout
          sum(cert_time_umid30) as cert_total_tmid30,
      FROM (
          SELECT
            TL.module_id as module_id,
            TL.username as username,
            (case when certified then TL.username else null end) as certified_username,
            sum(time_umid5) as time_umid5,
            sum(time_umid30) as time_umid30,
            sum(case when certified then time_umid5 else null end) as cert_time_umid5,
            sum(case when certified then time_umid30 else null end) as cert_time_umid30,

          FROM [{dataset}.time_on_asset_daily] TL
          JOIN [{dataset}.person_course] PC	# join to know who certified or attempted a problem
          ON TL.username = PC.username
          WHERE TL.time_umid5 is not null
                AND PC.nproblem_check > 0	# limit to users who attempted at least one problem
          GROUP BY module_id, username, certified_username
          ORDER BY module_id, username
        )
        GROUP BY module_id
        order by module_id
         """

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)
    the_sql = SQL.format(dataset=dataset, course_id=course_id)

    tablename = 'time_on_asset_totals'

    print("Computing %s for %s" % (tablename, dataset))
    sys.stdout.flush()

    bqdat = bqutil.get_bq_table(dataset, tablename, the_sql,
                                force_query=force_recompute,
                                depends_on=[ '%s.time_on_asset_daily' % dataset ],
                                )

    return bqdat
                  
