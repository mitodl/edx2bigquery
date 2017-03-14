#!/usr/bin/python
#
# File:   make_user_info_combo.py
# Date:   23-Jan-17
# Author: G. Lopez
#
# make single JSON file containing edX SQL information from:
#
#    rolesaccess.csv ( courseaccessrole )
#    rolesdisc.csv ( client_role_users )
#
# one line is generated for each user.
#
# This will be used to generate roles
# needed for the person course dataset.
#
# Usage:  
#
#    python make_roles.py <course_directory>
#
# files are assumed to already be in CSV format, with the HarvardX
# sql data file naming conventions.
#
# Fields included are:
#
# from rolesaccess.csv:  org, course_id, user_id, role
# from rolesdisc.csv: course_id, user_id, name
#
# these are all joined on user_id
#
# Each record's schema is checked for validity afterwards.

import os, sys
import csv
import gzip
import json
import gsutil
import pandas as pd

from path import path
from collections import defaultdict, OrderedDict
from check_schema_tracking_log import schema2dict, check_schema
from load_course_sql import find_course_sql_dir

#csv.field_size_limit(sys.maxsize)
#csv.field_size_limit(1310720)
csv.field_size_limit(13107200)

ROLE_COURSE_ACCESS = 'rolecourse.csv.gz'#'rolesaccess.csv.gz'
ROLE_FORUM_ACCESS = 'roleforum.csv.gz'#'rolesdisc.csv.gz'

def process_file(course_id, basedir=None, datedir=None, use_dataset_latest=False):

    basedir = path(basedir or '')
    course_dir = course_id.replace('/','__')
    lfp = find_course_sql_dir(course_id, basedir, datedir, use_dataset_latest=use_dataset_latest)

    cdir = lfp
    print "Processing %s role data in %s" % (course_id, cdir)
    sys.stdout.flush()

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA_FILE = '%s/schemas/schema_roles.json' % mypath
    the_dict_schema = schema2dict(json.loads(open(SCHEMA_FILE).read())['staff']) 
    
    role_data = defaultdict(dict)		# dict with key = user_id, and val = dict to be written out as JSON line
    
    def convertLongToWide( longData, unique_id, column_header, values, columns ):
        import numpy as np
        print "    Currently %d roles assigned... Dropping duplicates" % len(longData)
        sys.stdout.flush()
    
        ld2 = longData.drop_duplicates(subset=[unique_id, column_header])#, keep='last')
    
        print "    %d unique roles assigned" % len(ld2)
        print "    Processing role data with these known roles: %s" % columns
        sys.stdout.flush()
        wideData = ld2.pivot( index=unique_id, columns=column_header, values=values )
        wideData = pd.DataFrame( wideData , index=wideData.index ).reset_index()
        
        # Ensuring all specified columns exist
        cols = wideData.columns.tolist()
        for c in columns:
            if c not in cols:
                wideData[ c ] = pd.Series([np.nan])
        return wideData
    
    def createUniqueId( longData ):
        import numpy as np
        if pd.notnull(longData['user_id']) and pd.notnull(['course_id']):
            uid = str(longData['user_id']) + str('__') + str(longData['course_id'])
        else:
            uid = 'NULL'
        return uid
    
    def splitUniqueId( wideData ):
        import numpy as np
        wideData['user_id'] = pd.Series([np.nan])
        wideData['course_id'] = pd.Series([np.nan])
        wideData[['user_id', 'course_id']] = wideData['uid'].apply( lambda x: pd.Series(x.split('__') ) )
        wideData['course_id'] = wideData['course_id'].apply( forceTransparentCourseId )
        return wideData
    
    def forceTransparentCourseId( course_id ):
        
        #if key=='course_id' and src[key].startswith('course-v1:'):
        if 'course-v1' in course_id:
            # special handling for mangled "opaque keys" version of course_id, e.g. course-v1:MITx+6.00.2x_3+1T2015
            #src[key] = src[key].split(':',1)[1].replace('+','/')
            return course_id.split(':',1)[1].replace('+','/')
        else:
            return course_id
    
    def openfile(fn_in, mode='r', add_dir=True, pandas_df=False):
        if pandas_df:
            return pd.read_csv( gzip.GzipFile( cdir / fn_in ), sep=',' )
        else:
            if add_dir:
                fn = cdir / fn_in
	    else:
                fn = fn_in
            if fn.endswith('.gz'):
                return gzip.GzipFile(fn, mode)
            return open(fn, mode)

    def createRoleVar( wideData, all_roles ):

	if wideData[ all_roles ].notnull().values.any():
             return str("Staff")
        else:
             return str("Student")
           
    def cleanRoles( longData, unique_id, column_header, values, columns, allfields ):
        
        longData[ values ] = int(1)
        longData['uid'] = longData.apply(createUniqueId, axis=1 )
        wideData = convertLongToWide( longData, unique_id=unique_id, column_header=column_header, values=values, columns=columns )
        wideData = splitUniqueId( wideData )[ allfields ]    
        return wideData
    
    def copy_elements(src, dest, fields, prefix="", skip_empty=False):
        for key in fields:
            if skip_empty and (not key in src):
                src[key] = None
            if src[key]=='NULL':
                continue
            if key=='course_id' and src[key].startswith('course-v1:'):
                # special handling for mangled "opaque keys" version of course_id, e.g. course-v1:MITx+6.00.2x_3+1T2015
                src[key] = src[key].split(':',1)[1].replace('+','/')

            # Ensure integer for floats, or null
            if pd.isnull(src[key]):
                copyKey = None
            elif type(src[key]) == float:
                copyKey = int(float(src[key]))
            else:
                copyKey = src[key]

            dest[prefix + key] = copyKey

    #roledata = pd.read_csv( cdir / ROLE_COURSE_ACCESS, sep=',')
    #rolediscdata = pd.read_csv( cdir / ROLE_FORUM_ACCESS, sep=',')
    roledata = openfile( fn_in=ROLE_COURSE_ACCESS, mode='w', pandas_df=True)
    rolediscdata = openfile( fn_in=ROLE_FORUM_ACCESS, mode='w', pandas_df=True)
    
    # Process Course Access Roles
    known_roles_course = OrderedDict([
                          ('beta_testers', 'roles_isBetaTester'), 
                          ('instructor', 'roles_isInstructor'), 
                          ('staff', 'roles_isStaff'),
                          ('ccx_coach', 'roles_isCCX'),
                          ('finance_admin', 'roles_isFinance'),
                          ('library_user', 'roles_isLibrary'),
                          ('sales_admin', 'roles_isSales')
                          ])
    base_fields = ['user_id', 'course_id']
    fields = base_fields + known_roles_course.keys()
    print "  Cleaning %s" % ROLE_COURSE_ACCESS
    wide_roledata = cleanRoles( roledata, 'uid', 'role', 'value', known_roles_course.keys(), fields )
    
    # Process Forum Discussion Roles
    known_roles_disc = OrderedDict([
                          ('Administrator','forumRoles_isAdmin'),
                          ('CommunityTA','forumRoles_isCommunityTA'),
                          ('Moderator', 'forumRoles_isModerator'), 
                          ('Student', 'forumRoles_isStudent')
                           ])
    base_fields = ['user_id', 'course_id']
    extra_fields = ['roles'] # To be used to create custom mapping based on existing course and discussion forum roles
    fields = base_fields + known_roles_disc.keys()
    print "  Cleaning %s" % ROLE_FORUM_ACCESS
    wide_rolediscdata = cleanRoles( rolediscdata, 'uid', 'name', 'value', known_roles_disc.keys(), fields )
    
    # Compile
    fields_all = base_fields + known_roles_course.keys() + known_roles_disc.keys() + extra_fields
    rename_dict = dict(known_roles_course, **known_roles_disc)
    wideData = pd.merge( wide_roledata, wide_rolediscdata, how='outer', on=["user_id", "course_id"], suffixes=['', '_disc'] )

    # Create Roles var
    all_roles = known_roles_disc.keys() + known_roles_course.keys()
    all_roles.remove('Student')
    wideData['roles'] = wideData.apply( createRoleVar, args=[all_roles], axis=1 )

    # Rename columns
    wideData = wideData[ fields_all ]
    wideData.rename( columns=rename_dict, inplace=True )

    finalFields = wideData.columns.tolist()

    # Write out
    fieldnames = the_dict_schema.keys()
    ofp = openfile('roles.json.gz', 'w')
    ocsv = csv.DictWriter(openfile('roles.csv', 'w'), fieldnames=finalFields)
    ocsv.writeheader()
    
    wideData_dict = wideData.to_dict(orient='record')
    
    for line in wideData_dict:
        uid = int(line['user_id'])
        copy_elements(line, role_data[uid], finalFields, skip_empty=False )
        role_data[uid]['user_id'] = uid
        data = role_data[uid]   
    
        ofp.write(json.dumps(data) + '\n')
        try:
            ocsv.writerow(data)
        except Exception as err:
            print "failed to write data=%s" % data
            raise
    
    print "Done with make_roles for %s" % course_id
    sys.stdout.flush()

