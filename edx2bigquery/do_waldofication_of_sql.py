import os, sys, glob
import re
import gzip

from path import path

#-----------------------------------------------------------------------------

filename2collection = {'certificates': 'certificates_generatedcertificate',
                       'enrollment': 'student_courseenrollment',
                       'profiles': 'auth_userprofile',
                       'studentmodule': 'courseware_studentmodule',
                       'users': 'auth_user',
                       'rolecourse': 'student_courseaccessrole',
                       'roleforum': 'django_comment_client_role_users',
                       }

collection2filename = {v:k for k, v in filename2collection.items()}

#-----------------------------------------------------------------------------

def guess_course_id(name, known_course_ids):
    '''
    given a filename, guess the course id.  
    this is needed because edX is mashing course_id together, instead of cannoncally separating
    org, num, semester with the same separator.  
    
    For example, name ="MITx-6.00.1-x-1T2014..." should be turned into "MITx/6.00.1-x/1T2014"
    '''
    for course in known_course_ids:
        cstr = course.replace('/','-')
        if name.startswith(cstr):
            # print course
            return course, name[len(cstr)+1:]
    return None, None


dtreset = [ [ '2012_Fall', re.compile('^2012-[0-9][0-9]-') ],
            [ '2012_Fall', re.compile('^2013-01-') ],
            [ '2013_Spring', re.compile('^2013-0[2-7]-') ],
            [ '2013_Fall', re.compile('^2013-[1-7][0-9]-') ],
            [ '2013_Fall', re.compile('^2013-0[8-9]-') ],
          ]

def fix_course_id(short_cid, date):
    """
    Try to fix a short course id based on date.
    Use date to guess semester.
    """
    
    if short_cid.endswith('/'):
        short_cid = short_cid[:-1]
      
    if short_cid.count('-')==2 and short_cid.count('/')==0:
        short_cid = short_cid.replace('-','/')

    #if not short_cid.startswith(org):
    #    short_cid = '%s/%s' % (org, short_cid)

    try:
        short_cid = str(short_cid)
    except Exception as err:
        print "Cannot make short cid - unicode error"
        return ""

    if short_cid.count('/')==0 and '-' in short_cid:
        # maybe it has dashes instead of slashes?
        m = re.match('([^\-]+)-([^\-]+)-(.*)', short_cid)
        if m:
            short_cid = "%s/%s/%s" % (m.group(1), m.group(2), m.group(3))

    if short_cid.count('/')==2:
        # already long enough?
        return short_cid

    # guess the semester based on the date of the entry

    semester = None
    for semkey, dtre in dtreset:
        m = dtre.search(date)
        if m:
            semester = semkey
            break
    if semester is None:
        print "fix_short_cid failed on scid=%s time=%s" % (short_cid, date)
        return ''  # no guess
    
    cid = "%s/%s" % (short_cid, semester)

    return cid

#-----------------------------------------------------------------------------

def getCourseDir(cid, dtstr, basedir='', known_course_ids=None, is_edge=False):
    cdir = path(basedir)
    if is_edge:
        cdir = cdir / "EDGE"
        if not cdir.exists():
            os.mkdir(cdir)
    else:
        if not cid in known_course_ids or []:
            cdir = cdir / "UNKNOWN"
            if not cdir.exists():
                os.mkdir(cdir)
        
    cdir = cdir / (cid.replace('/','-'))
    if not cdir.exists():
        os.mkdir(cdir)
    cdir2 = cdir / dtstr
    if not cdir2.exists():
        os.mkdir(cdir2)
    return cdir2

def tsv2csv(fn_in, fn_out):
    import csv
    fp = gzip.GzipFile(fn_out, 'w')
    csvfp = csv.writer(fp)
    for line in open(fn_in):
        csvfp.writerow(line[:-1].split('\t'))
    fp.close()

#-----------------------------------------------------------------------------
        
def copyFile(fn, cdir, ofn=''):
    # copy file to course_id directory
    if not ofn:
        ofn = fn
    if ofn.endswith('.gz'):
        os.system('cp "%s" "%s"' % (fn, cdir / ofn))
    else:
        os.system('cat "%s" | gzip -9 > "%s"' % (fn, cdir / (ofn+'.gz')))
    #os.rename(fn, cdir / ofn)

def processFile(fn, dtstr, basedir, courses):
    '''
    Process an edX class data file, of path fn.
    dtstr = date-time string eg 2013-02-15 of the data dump
    
    The file will be placed in the directory basedir/class_id/dtstr/
    '''
    # skip xml, csv, and gpg files
    skipset = ['.xml', '.csv', '.gpg']
    for suffix in skipset:
        if fn.endswith(suffix):
            # print "Skipping %s" % fn
            return
    print ("  File %s" % fn.basename()), 
    sys.stdout.flush()
        
    # get course ID
    suffix = fn.basename().rsplit('.',1)[1]
    if suffix in ('mongo', 'json') or fn.endswith('.xml.tar.gz'):
        cid, datafn = guess_course_id(fn.basename(), courses)
        if cid is None:
            cfnpre = fn.basename().split('-course_structure')[0].split('-course-prod')[0]
            cfnpre = cfnpre.split('-edge.mongo')[0]
            cfnpre = cfnpre.split('-prod.mongo')[0]
            if cfnpre.count('-')==2:
                cid = '/'.join(cfnpre.split('-'))
                datafn = fn.basename().split(cfnpre,1)[1][1:]
            elif cfnpre.count('-')>2:
                cid = '/'.join(cfnpre.rsplit('-',2))
                datafn = fn.basename().split(cfnpre,1)[1][1:]
            else:
                print "could not guess course_id from %s [%s]" % (fn, cfnpre)
                return
                cid_org, cid_num, cid_sem, datafn = fn.basename().split('-',3)
                cid = '%s/%s/%s' % (cid_org, cid_num, cid_sem)
                cid = fix_course_id(cid, dtstr)
        #cid = fn.basename().split('.mongo')[0]
        cdir = getCourseDir(cid, dtstr, basedir, courses)

        if fn.endswith('.mongo'):
            ofn = 'forum.mongo'
        else:
            ofn = datafn

        print "   --> %s" % (cdir / ofn)
        copyFile(fn, cdir, ofn)
        return

    elif fn.endswith('.sql'):

        cid, sqlfn = guess_course_id(fn.basename(), courses)
        if cid is None:
            if fn.basename().count('-')<3:
                cid, sqlfn = fn.basename().rsplit('-',1)
                sqlfn = sqlfn[:-4]
            else:
                cid_org, cid_num, cid_sem, sqlfn = fn.basename().split('-',3)
                cid = '%s/%s/%s' % (cid_org, cid_num, cid_sem)
                if '-' in sqlfn:
                    sqlfn = sqlfn.split('-',1)[0]	# 27oct13: mitx-2013-10-27/MITx-00.00-TestClass-auth_user-prod-edge-analytics.sql
                if sqlfn.endswith('.sql'):
                    sqlfn = sqlfn[:-4]
        else:
            if '-' in sqlfn:
                sqlfn = sqlfn.split('-',1)[0]	# 27oct13: mitx-2013-10-27/MITx-00.00-TestClass-auth_user-prod-edge-analytics.sql
            if sqlfn.endswith('.sql'):
                sqlfn = sqlfn[:-4]

        cid = fix_course_id(cid, dtstr)
        cdir = getCourseDir(cid, dtstr, basedir, courses)
        sqlfn = collection2filename.get(sqlfn, sqlfn)
        csvfn = cdir / (sqlfn + '.csv.gz')
        print "   --> %s" % csvfn
        if not csvfn.exists():
            tsv2csv(fn, csvfn)
            pass
        return

#-----------------------------------------------------------------------------

def process_directory(dirname, courses, basedir):
    '''
    dirname = directory where unencrypted edX SQL files are stored.

    basedir = directory where transformed SQL files are to be stored, into a subdirectory
              with name given by the course_id and date, YYYY-MM-DD.
    '''

    # dirname must have date in it

    m = re.search('.*-(\d\d\d\d-\d\d-\d\d)$', dirname)
    
    if not m:
        print "="*10 + " Unknown directory name format %s -- skipping" % dirname
        return
    
    dtstr = m.group(1)

    print "="*100
    print "Doing Waldofication of SQL data from edX in %s -> %s/%s" % (dirname, basedir, dtstr)
    sys.stdout.flush()

    files = glob.glob('%s/*.*' % dirname)
    files.sort()
    # print "files to process: ", files

    for fn in files:
        processFile(path(fn), dtstr, basedir, courses)
        sys.stdout.flush()

