#!/usr/bin/python
#
# recommend pin dates for course axes computation, based on course wrap date.
# take the nearest SQL dataset date with tar.gz file, closest to wrap date
# as given in listings file

import json
import glob
from .load_course_sql import find_course_sql_dir

def process_courses(clist, basedir, listings):

    data = { y['course_id']: y for y in [ json.loads(x) for x in open(listings) ] }

    for course_id in clist:
        wrap = data[course_id].get('Course Wrap')
        try:
            (month, day, year) = list(map(int, wrap.split('/')))
        except Exception as err:
            print("Cannot parse date %s for course %s" % (wrap, course_id))
            continue
    
        wrap_dn = "%04d-%02d-%02d" % (year, month, day)
    
        datedir = ""
        use_dataset_latest = True
        course_dir = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest, verbose=False)
    
        sql_dir = course_dir.dirname()
    
        dirs_available = list(glob.glob( sql_dir / '20*' / "course*.tar.gz" ))
        dirs_available.sort()
        # print "course %s, wrap %s, sql_dir=%s, dirs_available=%s" % (course_id, wrap_dn, sql_dir, dirs_available)
    
        chosen_dir = None
        just_chose = False
        for dn in dirs_available:
            ddate = dn.rsplit('/', 2)[1]
            if ddate < wrap_dn:
                chosen_dir = dn
                just_chose = True
            elif just_chose:
                chosen_dir = dn	# shift to the date just after wrap
                just_chose = False
    
        if not chosen_dir and dirs_available:
            chosen_dir = dirs_available[0]
    
        # print "course %s, wrap %s, suggestd pin dir=%s" % (course_id, wrap_dn, chosen_dir)
        
        if chosen_dir:
            print("    '%s': '%s'," % (course_id, chosen_dir.rsplit('/',2)[1]))
        else:
            print("--> course %s, wrap %s, suggestd pin dir NOT FOUND" % (course_id, wrap_dn))
    
        
