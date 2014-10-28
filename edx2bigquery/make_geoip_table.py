#!/usr/bin/python
#
# make geoip lookup table for IP addreses missing geoip information in specified person-course table
# 

import os
import sys
import re
import json
import codecs
import gsutil
import bqutil
import pygeoip
from collections import OrderedDict

GIPDATASET = 'geocode'
GIPTABLE = 'extra_geoip'

class GeoIPData(object):
    def __init__(self, gipdataset=GIPDATASET, giptable=GIPTABLE, geoipdat='GeoIPCity.dat'):

        if not os.path.exists(geoipdat):
            raise Exception("---> [make_geoip_table] Error! Missing file %s for local geoip" % geoipdat)
        self.gipdataset = gipdataset
        self.giptable = giptable
        self.gipfn = self.giptable + ".json"
        self.gi = pygeoip.GeoIP(geoipdat, pygeoip.MEMORY_CACHE)
        self.nchanged = 0

    def load_geoip(self):
        if os.path.exists(self.gipfn):
            print "Retrieving existing geoipdat from local file %s" % (self.gipfn)
            geoipdat = OrderedDict()
            for k in open(self.gipfn):
                data = json.loads(k)
                geoipdat[data['ip']] = data
        else:
            try:
                print "--> Trying to retrieve geoipdat from BigQuery %s.%s" % (self.gipdataset, self.giptable)
                geoipdat_table_data = bqutil.get_table_data(self.gipdataset, self.giptable, key={'name': 'ip'})
                self.geoipdat = geoipdat_table_data['data_by_key']
                print "    Retrieved %d entries" % len(self.geoipdat) 
            except Exception as err:
                print "--> Failed to retrieve existing geoipdat from BigQuery, %s.%s" % (self.gipdataset, self.giptable)
                print err
                geoipdat = OrderedDict()
        self.geoipdat = geoipdat
        return
    
    def lookup_ip(self, ip):
        if ip in self.geoipdat:
            return self.geoipdat[ip]
        try:
            rec = self.gi.record_by_addr(ip)
        except Exception as err:
            print "--> Cannot get geoip for %s, skipping" % ip
            return
    
        if (rec is None) or (not rec):
            print "--> Cannot get geoip for %s, skipping" % ip
            return
    
        try:
            self.geoipdat[ip] = {'ip': ip, 
                                 'city': rec['city'].decode('latin1'),	# maxmind geoip data city encoded as latin1
                                 'countryLabel': rec['country_name'],
                                 'country': rec['country_code'],
                                 'latitude': rec['latitude'],
                                 'longitude': rec['longitude'],
                                 }
        except Exception as err:
            print "Oops, bad geoip record for ip=%s, got rec=%s" % (ip, rec)
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

        ofn = 'tmp_geoip.json'
        print "--> new entries added to geoipdat, writing to %s" % (ofn)
        sys.stdout.flush()

        ofp = codecs.open(ofn, 'w', encoding='utf8')
        for key, val in self.geoipdat.iteritems():
            try:
                ofp.write(json.dumps(val)+'\n')
            except Exception as err:
                print "Error!  %s" % err
                sys.stdout.write(repr(val))
                raise
        ofp.close()

        print "--> renaming %s to %s" % (ofn, self.gipfn)
        sys.stdout.flush()
        os.rename(ofn, self.gipfn)
        
        mypath = os.path.dirname(os.path.realpath(__file__))
        the_schema = json.loads(open('%s/schemas/schema_extra_geoip.json' % mypath).read())['extra_geoip']

        gsp = gsutil.gs_path_from_course_id(self.gipdataset) / self.gipfn
        print "--> Uploading %s to %s" % (self.gipfn, gsp)
        sys.stdout.flush()
        gsutil.upload_file_to_gs(self.gipfn, gsp, '-z json')
        
        print "--> Importing %s to %s" % (gsp, self.giptable)
        sys.stdout.flush()
        bqutil.create_dataset_if_nonexistent(self.gipdataset)
        bqutil.load_data_to_table(self.gipdataset, self.giptable, gsp, the_schema)

    
    def make_table(self, pcds_table=None, org=None, nskip=0):
        '''
        Get the specified person_course table.  Find IP addresses for which the country code is missing.
        Get country codes for each of those IP addreses using local copy of the maxmind geoip database.
        Store result in bigquery geoip table.
    
        Does not overwrite existing bigquery geoip table - adds new entries.
        '''
        
        if pcds_table is None:
            if org is None:
                print "Error!  Must specify either --table or --org"
                return
            dataset = 'course_report_' + org
            
            pctables = []
            for table in bqutil.get_list_of_table_ids(dataset):
                m = re.match('person_course_%s_([\d_]+)$' % org, table)
                if m:
                    pctables.append(table)
            pctables.sort()
            if not pctables:
                print "Error!  No person_course_%s_* tables found in dataset %s!" % (org, dataset)
                return
            pctable = pctables[-1]
        else:
            (dataset, pctable) = pcd_table.split('.',1)
    
        print "[make_geoip_table] Using person course from %s.%s" % (dataset, pctable)
    
        if nskip <= 0:
            pimc_table = "%s_ip_no_cc" % pctable
            sql = """SELECT ip, count(*) as n
                     FROM [{dataset}.{table}] 
                     where cc_by_ip is Null
                     group by ip
                  """.format(dataset=dataset, table=pctable)
        
            noips = bqutil.get_bq_table(dataset, pimc_table, sql)
            
            print "%d IP addresses missing geoip information in %s" % (len(noips['data']), pimc_table)
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
        
            print "Added %d new geoip entries" % self.nchanged
            sys.stdout.flush()
        else:
            nskip -= 1
        self.write_geoip_table()
    
        print "--> Done" 
        sys.stdout.flush()
    
