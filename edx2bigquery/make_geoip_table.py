#!/usr/bin/python
#
# make geoip lookup table for IP addreses missing geoip information in specified person-course table
# 

import os
import sys
import re
import json
import codecs
import time
import datetime
from . import gsutil
from . import bqutil
import random
import pygeoip
import geoip2.database
from collections import OrderedDict

GIPDATASET = 'geocode'
GIPTABLE = 'extra_geoip'

def lock_file(fn, release=False):
    lockfn = '%s.lock' % fn

    def release_lock(verbose=False):
        try:
            os.rmdir(lockfn)
        except Exception as err:
            print("[lock_file] in releasing lock %s, err=%s" % (lockfn, err))
            return False
        if verbose:
            print("[lock_file] released lock %s" % (lockfn))
            sys.stdout.flush()
        return True

    if release:
        return release_lock(verbose=True)

    cnt = 0
    have_lock = False
    start = datetime.datetime.now()
    timeout = random.uniform(3*60, 8*60)
    while not have_lock:
        if not os.path.exists(lockfn):
            try:
                os.mkdir(lockfn)
                if cnt:
                    dt = datetime.datetime.now() - start
                    print("[lock_file] acquired lock %s after cnt=%s, dt=%s" % (lockfn, cnt, dt))
                    sys.stdout.flush()
                return True
            except Exception as err:
                print("[lock_file] in acquiring lock %s, err=%s" % (lockfn, err))
        cnt += 1
        if (cnt > 2000):
            print("[lock_file] Aborting!")
            break
        if (cnt > 18):
            dt = datetime.datetime.now() - start
            if (dt.seconds > timeout):
                print("[lock_file] timeout after %s seconds waiting for lock" % timeout)
                sys.stdout.flush()
                release_lock(verbose=True)
        print("[lock_file] waiting for %s, cnt=%d..." % (lockfn, cnt))
        time.sleep(10)
    if not have_lock:
        msg = "[lock_file] Aborting - could not acquire %s" % lockfn
        raise Exception(msg)
    return False

class GeoIPData(object):
    def __init__(self, gipdataset=GIPDATASET, giptable=GIPTABLE, geoipdat='GeoIP2-City.mmdb', version=2):

        # geoip version 1: GeoIPCity.dat
        # geoip version 2: GeoIP2-City.mmdb

        if not os.path.exists(geoipdat):
            raise Exception("---> [make_geoip_table] Error! Missing file %s for local geoip" % geoipdat)
        self.gipdataset = gipdataset
        self.giptable = giptable
        self.gipfn = self.giptable + ".json"
        self.version = version
        if version==1:
            self.gi = pygeoip.GeoIP(geoipdat, pygeoip.MEMORY_CACHE)
        else:
            self.gi = geoip2.database.Reader(geoipdat)
        self.nchanged = 0

    def load_geoip(self):
        if os.path.exists(self.gipfn):
            print("Retrieving existing geoipdat from local file %s" % (self.gipfn))
            geoipdat = OrderedDict()
            lock_file(self.gipfn)
            for k in open(self.gipfn):
                try:
                    data = json.loads(k)
                except Exception as err:
                    print("[GeoIpData] in loading '%s' from %s err %s" % (k, self.gipfn, err))
                geoipdat[data['ip']] = data
            lock_file(self.gipfn, release=True)
        else:
            try:
                print("--> Trying to retrieve geoipdat from BigQuery %s.%s" % (self.gipdataset, self.giptable))
                geoipdat_table_data = bqutil.get_table_data(self.gipdataset, self.giptable, key={'name': 'ip'})
                self.geoipdat = geoipdat_table_data['data_by_key']
                print("    Retrieved %d entries" % len(self.geoipdat)) 
            except Exception as err:
                print("--> Failed to retrieve existing geoipdat from BigQuery, %s.%s" % (self.gipdataset, self.giptable))
                print(err)
                geoipdat = OrderedDict()
        self.geoipdat = geoipdat
        return
    
    def lookup_ip(self, ip):
        if (ip in self.geoipdat) and ('region' in self.geoipdat[ip]):
            return self.geoipdat[ip]
        try:
            if self.version==1:
                rec = self.gi.record_by_addr(ip)
            else:
                rec = self.gi.city(ip)
        except Exception as err:
            print("--> Cannot get geoip for %s, skipping" % ip)
            return
    
        if (rec is None) or (not rec):
            print("--> Cannot get geoip for %s, skipping" % ip)
            return
    
        try:
            if self.version==1:
                self.geoipdat[ip] = {'ip': ip, 
                                     'city': rec['city'].decode('latin1'),	# maxmind geoip data city encoded as latin1
                                     'countryLabel': rec['country_name'],
                                     'country': rec['country_code'],
                                     'latitude': rec['latitude'],
                                     'longitude': rec['longitude'],
                                     'postalCode': rec.get('postal_code'),	# JUST A GUESS - CHECK THIS
                                     'continent': rec.get('continent'),		# JUST A GUESS - CHECK THIS
                                     'subdivision': rec.get('subdivision_name'),	# JUST A GUESS - CHECK THIS
                                     'region': rec.get('region'),	# JUST A GUESS - CHECK THIS
                                     }
            else:
                self.geoipdat[ip] = {'ip': ip, 
                                     'city': rec.city.names.get('en', ''),	# already unicode
                                     'countryLabel': rec.country.names.get('en', ''),
                                     'country': rec.country.iso_code,
                                     'latitude': rec.location.latitude,
                                     'longitude': rec.location.longitude,
                                     'postalCode': rec.postal.code,		# in the US, this is the zip code
                                     'continent': rec.continent.name,
                                     'subdivision': rec.subdivisions.most_specific.name,
                                     'region': rec.subdivisions.most_specific.iso_code,	# in the US, this is the state
                                     }
        except Exception as err:
            print("Oops, bad geoip record for ip=%s, error=%s, got rec=%s" % (ip, str(err), rec))
            return None
    
        # In [3]: gi.record_by_addr('217.212.231.57')
        # Out[3]: 
        # {'area_code': 0,
        #  'city': '',
        #  'continent': 'EU',
        #  'country_code': 'GB',
        #  'country_code3': 'GBR',
        #  'country_name': 'United Kingdom',
        #  'dma_code': 0,
        #  'latitude': 51.5,
        #  'longitude': -0.12999999999999545,
        #  'metro_code': None,
        #  'postal_code': '',
        #  'region_name': '',
        #  'time_zone': 'Europe/London'}
    
        self.nchanged += 1
        return self.geoipdat[ip]
        
    def write_geoip_table(self):
        '''
        Write out the geoipdat table if nchanged > 0
        '''
        if not self.nchanged:
            return

        ofn = 'tmp_geoip_%08d.json' % random.uniform(0,100000000)
        print("--> new entries added to geoipdat, writing to %s" % (ofn))
        sys.stdout.flush()

        ofp = codecs.open(ofn, 'w', encoding='utf8')
        for key, val in self.geoipdat.items():
            try:
                ofp.write(json.dumps(val)+'\n')
            except Exception as err:
                print("Error!  %s" % err)
                sys.stdout.write(repr(val))
                raise
        ofp.close()

        lock_file(self.gipfn)
        try:
            print("--> renaming %s to %s" % (ofn, self.gipfn))
            sys.stdout.flush()
            os.rename(ofn, self.gipfn)
        except Exception as err:
            print("Error %s in renaming gipfn" % str(err))
        lock_file(self.gipfn, release=True)
        
        mypath = os.path.dirname(os.path.realpath(__file__))
        the_schema = json.loads(open('%s/schemas/schema_extra_geoip.json' % mypath).read())['extra_geoip']

        gsp = gsutil.gs_path_from_course_id(self.gipdataset) / self.gipfn
        print("--> Uploading %s to %s" % (self.gipfn, gsp))
        sys.stdout.flush()
        gsutil.upload_file_to_gs(self.gipfn, gsp, '-z json')
        
        print("--> Importing %s to %s" % (gsp, self.giptable))
        sys.stdout.flush()
        try:
            bqutil.create_dataset_if_nonexistent(self.gipdataset)
        except Exception as err:
            print("--> Warning: failed to create %s, err=%s" % (gsp, err))
        try:
            bqutil.load_data_to_table(self.gipdataset, self.giptable, gsp, the_schema)
        except Exception as err:
            print("---> ERROR: failed to load %s into BigQuery %s.%s, err=%s" % (gsp, self.gipdataset, self.giptable, err))
            print("---> Continuing anyway")
            sys.stdout.flush()

    
    def make_table(self, pcds_table=None, org=None, nskip=0):
        '''
        Get the specified person_course table.  Find IP addresses for which the country code is missing.
        Get country codes for each of those IP addreses using local copy of the maxmind geoip database.
        Store result in bigquery geoip table.
    
        Does not overwrite existing bigquery geoip table - adds new entries.
        '''
        
        if pcds_table is None:
            if org is None:
                print("Error!  Must specify either --table or --org")
                return
            dataset = 'course_report_' + org
            
            pctables = []
            for table in bqutil.get_list_of_table_ids(dataset):
                m = re.match('person_course_%s_([\d_]+)$' % org, table)
                if m:
                    pctables.append(table)
            pctables.sort()
            if not pctables:
                print("Error!  No person_course_%s_* tables found in dataset %s!" % (org, dataset))
                return
            pctable = pctables[-1]
        else:
            (dataset, pctable) = pcd_table.split('.',1)
    
        print("[make_geoip_table] Using person course from %s.%s" % (dataset, pctable))
    
        if nskip <= 0:
            pimc_table = "%s_ip_no_cc" % pctable
            sql = """SELECT ip, count(*) as n
                     FROM [{dataset}.{table}] 
                     where cc_by_ip is Null
                     group by ip
                  """.format(dataset=dataset, table=pctable)
        
            noips = bqutil.get_bq_table(dataset, pimc_table, sql)
            
            print("%d IP addresses missing geoip information in %s" % (len(noips['data']), pimc_table))
            # print noips['data'][:10]
            sys.stdout.flush()
        
            self.load_geoip()
        
            for entry in noips['data']:
                ip = entry['ip']
                if ip is None:
                    continue
                if ip in self.geoipdat:
                    # print "--> Already have geoip for %s, skipping" % ip
                    continue
    
                self.lookup_ip(ip)
    
                if (self.nchanged%100==0):
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    break
        
            print("Added %d new geoip entries" % self.nchanged)
            sys.stdout.flush()
        else:
            nskip -= 1
        self.write_geoip_table()
    
        print("--> Done") 
        sys.stdout.flush()
    
