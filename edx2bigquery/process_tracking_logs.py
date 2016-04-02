#!/usr/bin/python
#
# common procedures to process all tracking logs, and update table based on new tracking logs

import sys
import datetime
import bqutil
import math

def run_query_on_tracking_logs(SQL, table, course_id, force_recompute=False, use_dataset_latest=False, 
                               start_date=None,
                               end_date=None, 
                               get_date_function=None,
                               existing=None,
                               log_dates=None,
                               days_delta=1,
                               skip_last_day=False,
                               has_hash_limit=False,
                               newer_than=None,
                               table_max_size_mb=800,
                               limit_query_size=False):
    '''
    make a certain table (with SQL given) for specified course_id.

    The master table holds all the data for a course.  It isn't split into separate
    days.  It is ordered in time, however.  To update it, a new day's logs
    are processed, then the results appended to this table.

    If the table doesn't exist, then run it once on all
    the existing tracking logs.  

    If it already exists, then run a query on it to see what dates have
    already been done.  Then do all tracking logs except those which
    have already been done.  Append the results to the existing table.

    If the query fails because of "Resources exceeded during query execution"
    then try setting the end_date, to do part at a time.

    NOTE: the SQL must produce a result which is ordered by date, in increasing order.

    days_delta = integer number of days to increase each time; specify 0 for one day overlap,
                 but make sure the SQL query only selects for time > TIMESTAMP("{last_date}")

    If skip_last_day is True then do not include the last day of tracking log data
    in the processing.  This is done to avoid processing partial data, e.g. when
    tracking log data are incrementally loaded with a delta of less than one day.

    start_date = optional argument, giving min start date for logs to process, in YYYY-MM-DD format.

    newer_than = if specified, as datetime, then any existing destination table must
    be newer than this datetime, else force_recompute is made True
    '''

    dataset = bqutil.course_id2dataset(course_id, use_dataset_latest=use_dataset_latest)	# destination
    log_dataset = bqutil.course_id2dataset(course_id, dtype="logs")

    if existing is None:
        existing = bqutil.get_list_of_table_ids(dataset)
        print "[run_query_on_tracking_logs] got %s existing tables in dataset %s" % (len(existing or []), dataset)

    if log_dates is None:
        log_tables = [x for x in bqutil.get_list_of_table_ids(log_dataset) if x.startswith('tracklog_20')]
        log_dates = [x[9:] for x in log_tables]
        log_dates.sort()

    if len(log_dates)==0:
        print "--> no tracking logs in %s aborting!" % (log_dataset)
        return

    if skip_last_day:
        old_max_date = max(log_dates)
        log_dates.remove(max(log_dates))	# remove the last day of data from consideration
        max_date = max(log_dates)
        print "         --> skip_last_day is True: dropping %s, new max_date=%s" % (old_max_date, max_date)
        sys.stdout.flush()

    min_date = min(log_dates)
    max_date = max(log_dates)

    if start_date:
        start_date = start_date.replace('-','')
        if min_date < start_date:
            print "        --> logs start at %s, but that is before start_date, so using min_date=start_date=%s" % (min_date, start_date)
            min_date = start_date

    if end_date is not None:
        print "[run_query_on_tracking_logs] %s: min_date=%s, max_date=%s, using end_date=%s for max_date cutoff" % (table, min_date, max_date, end_date)
        sys.stdout.flush()
        the_end_date = end_date.replace('-','')	# end_date should be YYYY-MM-DD
        if the_end_date < max_date:
            max_date = the_end_date

    if (table in existing) and newer_than:
        # get date on existing table
        table_datetime = bqutil.get_bq_table_last_modified_datetime(dataset, table)
        if (table_datetime < newer_than):
            print "[run_query_on_tracking_logs] existing %s.%s table last modified on %s, older than newer_than=%s, forcing recompute!" % (dataset, table, table_datetime, newer_than)
            force_recompute = True

    if force_recompute:
        overwrite = True
    else:
        overwrite = False

    last_date = "2010-10-01 01:02:03"    	# default last date

    if (not overwrite) and table in existing:
        # find out what the end date is of the current table
        pc_last = bqutil.get_table_data(dataset, table, startIndex=-10, maxResults=100)
        if (pc_last is None):
            print "--> no data in table %s.%s, starting from scratch!" % (dataset, table)
            overwrite = True
        else:
            last_dates = []
            for x in pc_last['data']:
                try:
                    add_date = get_date_function(x)
                    last_dates.append( add_date )
                except Exception as err:
                    print "Error with get_date_function occurred. %s" % str( err )
                    continue
            last_date = max(last_dates)
            table_max_date = last_date.strftime('%Y%m%d')
            if max_date <= table_max_date:
                print '--> %s already exists, max_date=%s, but tracking log data min=%s, max=%s, nothing new!' % (table, 
                                                                                                                  table_max_date,
                                                                                                                  min_date,
                                                                                                                  max_date)
                sys.stdout.flush()
                return
            min_date = (last_date + datetime.timedelta(days=days_delta)).strftime('%Y%m%d')
            print '--> %s already exists, max_date=%s, adding tracking log data from %s to max=%s' % (table, 
                                                                                                      table_max_date,
                                                                                                      min_date,
                                                                                                      max_date)
            sys.stdout.flush()
            overwrite = 'append'
    
    if overwrite=='append':
        print "Appending to %s table for course %s (start=%s, end=%s, last_date=%s) [%s]"  % (table, course_id, min_date, max_date, last_date, datetime.datetime.now())
    else:
        print "Making new %s table for course %s (start=%s, end=%s) [%s]"  % (table, course_id, min_date, max_date, datetime.datetime.now())
    sys.stdout.flush()

    if limit_query_size:
        # do only one day's tracking logs, and force use of hash if table is too large
        print '--> limiting query size, so doing only one day at a time, and checking tracking log table size as we go (max=%s MB)' % table_max_size_mb
        the_max_date = max_date	# save max_date information

        while min_date not in log_dates:
            print "  tracklog_%s does not exist!" % min_date
            for ld in log_dates:
                if ld < min_date:
                    continue
                if (ld > min_date) and (ld <= max_date):
                    min_date = ld
                    break
        if min_date not in log_dates:
            print "--> ERROR! Cannot find tracking log file for %s, aborting!" % min_date
            raise Exception("[process_tracking_logs] missing tracking log")

        max_date = min_date
        print '    min_date = max_date = %s' % min_date
        tablename = 'tracklog_%s' % min_date.replace('-', '')
        tablesize_mb = bqutil.get_bq_table_size_bytes(log_dataset, tablename) / (1024.0*1024)
        nhashes =  int(math.ceil(tablesize_mb / table_max_size_mb))
        from_datasets = "[%s.%s]" % (log_dataset, tablename)
        print "--> table %s.%s size %s MB > max=%s MB, using %d hashes" % (log_dataset, tablename, tablesize_mb, table_max_size_mb, nhashes)
        sys.stdout.flush()

        if nhashes:
            if not has_hash_limit:
                print "--> ERROR! table %s.%s size %s MB > max=%s MB, but no hash_limit in SQL available" % (log_dataset, tablename, 
                                                                                                             tablesize_mb, table_max_size_mb)
                print "SQL: ", SQL
                raise Exception("[process_tracking_logs] table too large")

            for k in range(nhashes):
                hash_limit = "AND ABS(HASH(username)) %% %d = %d" % (nhashes, k)
                the_sql = SQL.format(course_id=course_id, DATASETS=from_datasets, last_date=last_date, hash_limit=hash_limit)
                print "   Hash %d" % k
                sys.stdout.flush()
                try:
                    bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite, allowLargeResults=True)
                except Exception as err:
                    print the_sql
                    raise
                overwrite = "append"
        else:
            the_sql = SQL.format(course_id=course_id, DATASETS=from_datasets, last_date=last_date, hash_limit="")
            try:
                bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite, allowLargeResults=True)
            except Exception as err:
                print the_sql
                raise

        txt = '[%s] added tracking log data from %s' % (datetime.datetime.now(), tablename)
        bqutil.add_description_to_table(dataset, table, txt, append=True)

        print "----> Done with day %s" % max_date

        if the_max_date > max_date:
            # more days still to be done
            print "--> Moving on to another day (max_date=%s)" % the_max_date
            run_query_on_tracking_logs(SQL, table, course_id, force_recompute=False, 
                                       use_dataset_latest=use_dataset_latest,
                                       end_date=end_date, 
                                       get_date_function=get_date_function,
                                       existing=existing,
                                       log_dates=log_dates,
                                       has_hash_limit=has_hash_limit,
                                       table_max_size_mb=table_max_size_mb,
                                       limit_query_size=limit_query_size,
                                   )
        return

    from_datasets = """(
                  TABLE_QUERY({dataset},
                       "integer(regexp_extract(table_id, r'tracklog_([0-9]+)')) BETWEEN {start} and {end}"
                     )
                  )
         """.format(dataset=log_dataset, start=min_date, end=max_date)

    def get_ym(x):
        return int(x[0:4]), int(x[4:6]), int(x[6:])
    (mx_year, mx_month, mx_day) = get_ym(max_date)
    (mn_year, mn_month, mn_day) = get_ym(min_date)
    max_date_end = "%04d-%02d-%02d 23:59:59" % (mx_year, mx_month, mx_day)
    min_date_start = "%04d-%02d-%02d 00:00:00" % (mn_year, mn_month, mn_day)

    the_sql = SQL.format(course_id=course_id, DATASETS=from_datasets, last_date=last_date, min_date_start=min_date_start, max_date_end=max_date_end, hash_limit="")

    try:
        bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite, allowLargeResults=True)
    except Exception as err:
        if ( ('Response too large to return.' in str(err) or 'Too many tables for query' in str(err) ) and has_hash_limit ):
            # try using hash limit on username
            # e.g. WHERE ABS(HASH(username)) % 4 = 0

            for k in range(4):
                hash_limit = "AND ABS(HASH(username)) %% 4 = %d" % k
                the_sql = SQL.format(course_id=course_id, DATASETS=from_datasets, last_date=last_date, hash_limit=hash_limit)
                bqutil.create_bq_table(dataset, table, the_sql, wait=True, overwrite=overwrite, allowLargeResults=True)
                overwrite = "append"

        elif ('Resources exceeded during query execution' in str(err) or 'Too many tables for query' in str(err)):
            if True:
                # figure out time interval in days, and split that in half
                start_date = datetime.datetime.strptime(min_date, '%Y%m%d')
                end_date = datetime.datetime.strptime(max_date, '%Y%m%d')
                ndays = (end_date - start_date).days
                if (ndays < 1) and has_hash_limit:
                    print "----> ndays=%d; retrying with limit_query_size" % ndays
                    sys.stdout.flush()
                    return run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute, 
                                                      use_dataset_latest=use_dataset_latest,
                                                      end_date=max_date, 
                                                      get_date_function=get_date_function,
                                                      has_hash_limit=has_hash_limit,
                                                      # existing=existing,
                                                      log_dates=log_dates,
                                                      limit_query_size=True,
                                                  )
                elif (ndays < 1):
                    print "====> ERROR with resources exceeded during query execution; ndays=%d, cannot split -- ABORTING!" % ndays
                    raise
                
                nd1 = int(ndays/2)
                nd2 = ndays - nd1
                #if nd2 > nd1:
                #    nd1 = nd2
                #    nd2 = ndays - nd1
                print "====> ERROR with resources exceeded during query execution; re-trying based on splitting %d days into %d + %d days" % (ndays, nd1, nd2)
                sys.stdout.flush()

                end_date = (start_date + datetime.timedelta(days=nd1)).strftime('%Y%m%d')
                print "--> part 1 with %d days (end_date=%s)" % (nd1, end_date)
                sys.stdout.flush()
                run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute, 
                                           use_dataset_latest=use_dataset_latest,
                                           end_date=end_date, 
                                           get_date_function=get_date_function,
                                           has_hash_limit=has_hash_limit,
                                           # existing=existing,
                                           log_dates=log_dates)

                end_date = max_date
                print "--> part 2 with %d days (end_date=%s)" % (nd2, end_date)
                sys.stdout.flush()
                run_query_on_tracking_logs(SQL, table, course_id, force_recompute=False, 
                                           use_dataset_latest=use_dataset_latest,
                                           end_date=end_date, 
                                           get_date_function=get_date_function,
                                           has_hash_limit=has_hash_limit,
                                           # existing=existing,
                                           log_dates=log_dates)
                print "--> Done with %d + %d days!" % (nd1, nd2)
                return


            if False:
                def get_ym(x):
                    return int(x[0:4]), int(x[4:6]), int(x[6:])
                (min_year, min_month, min_day) = get_ym(min_date)
                (max_year, max_month, max_day) = get_ym(max_date)
                nmonths = max_month - min_month + 12 * (max_year - min_year)
                print "====> ERROR with resources exceeded during query execution; re-trying based on one month's data at a time"
                sys.stdout.flush()
                (end_year, end_month) = (min_year, min_month)
                for dm in range(nmonths):
                    end_month += 1
                    if end_month > 12:
                        end_month = 1
                        end_year += 1
                    end_date = "%04d-%02d-%02d" % (end_year, end_month, min_day)
                    print "--> with end_date=%s" % end_date
                    sys.stdout.flush()
                    run_query_on_tracking_logs(SQL, table, course_id, force_recompute=force_recompute, 
                                               use_dataset_latest=use_dataset_latest,
                                               end_date=end_date, 
                                               get_date_function=get_date_function,
                                               has_hash_limit=has_hash_limit,
                                               # existing=existing,
                                               log_dates=log_dates)
                    force_recompute = False		# after first, don't force recompute
                return
        else:
            print the_sql
            raise

    if overwrite=='append':
        txt = '[%s] added tracking log data from %s to %s' % (datetime.datetime.now(), min_date, max_date)
        bqutil.add_description_to_table(dataset, table, txt, append=True)
    
    print "Done with course %s (end %s)"  % (course_id, datetime.datetime.now())
    print "="*77
    sys.stdout.flush()
    
