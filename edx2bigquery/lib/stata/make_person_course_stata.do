****************************************
* make standard STATA version of person-course
*
* usage:
*
*     run make_person_course_stata.do <course_id_name_short> <table_prefix> <output_fn>
*
* requires globals:
*
*     force_recompute - 1 if data should always be downloaded from bq
*     lib_dir         - path to edx2bigquery library

args cidns table_prefix output_fn

local pcdta = "`output_fn'"
di "pcdta = `pcdta'"

if (1) {

	get_bq_data_csv "`table_prefix'.person_course"  "`table_prefix'__person_course.csv"  "DATA/DATA-`cidns'__person_course.dta"

	* person_course fields
	* course_id,user_id,username,registered,viewed,explored,certified,ip,cc_by_ip,countryLabel,continent,city,region,subdivision,postalCode,un_major_region,un_economic_group,un_developing_nation,un_special_region,latitude,longitude,LoE,YoB,gender,grade,start_time,last_event,nevents,ndays_act,nplay_video,nchapters,nforum_posts,nforum_votes,nforum_endorsed,nforum_threads,nforum_comments,nforum_pinned,roles,nprogcheck,nproblem_check,nforum_events,mode,is_active,cert_created_date,cert_modified_date,cert_status,profile_country,y1_anomalous,email_domain,ntranscript,nshow_answer,nvideo,nseq_goto,nseek_video,npause_video,avg_dt,sdv_dt,max_dt,n_dt,sum_dt,roles_isBetaTester,roles_isInstructor,roles_isStaff,forumRoles_isAdmin,forumRoles_isCommunityTA,forumRoles_isModerator,forumRoles_isStudent

	drop course_id username ip city region subdivision latitude longitude start_time last_event nevents ndays_act
	drop cert_created_date cert_modified_date cert_status profile_country 
	drop y1_anomalous email_domain ntranscript
	* drop nseek_video npause_video 
	* drop avg_dt sdv_dt max_dt n_dt sum_dt 
	drop roles_isbetatester roles_isinstructor roles_isstaff forumroles_isadmin 
	drop forumroles_iscommunityta forumroles_ismoderator forumroles_isstudent
	drop roles

	drop if nforum_pinned > 0 & !missing(nforum_pinned)	// drop staff (those who have pinned posts)

* make binary from true/false

	gen b_registered = (lower(registered)=="true")
	drop registered
	rename b_registered registered

	gen b_viewed = (lower(viewed)=="true")
	drop viewed
	rename b_viewed viewed

	gen b_explored = (lower(explored)=="true")
	drop explored
	rename b_explored explored

	gen b_certified = (lower(certified)=="true")
	drop certified
	rename b_certified certified

* drop unregistered (resididual residential observations?)
	drop if !registered

* Mutually exclusive registrant categories (Viewed may now include certified students!)
	gen o_registered = registered & !viewed & !explored
	gen o_viewed = viewed & !explored
	gen o_explored = explored
	
* Female (not male, other missing)

	gen female = .
	capture replace female = 1 if gender == "f"
	capture replace female = 0 if gender == "m"

* Bachelor's Plus (Education)

	gen bachplus = .
	capture replace bachplus = 1 if loe=="b" | loe=="m" | loe=="p" | loe=="p_oth" | loe=="p_se"
	capture replace bachplus = 0 if loe=="a" | loe=="el" | loe=="hs" | loe=="jhs" | loe=="none"

* Age
	replace yob = . if yob==513 | yob==3116
	gen age2015 = 2015-yob
	
* USA
	capture gen usa = cc_by_ip == "US" if cc_by_ip != ""

* Verified
	capture gen verified = mode=="verified"	

* Nonzero counts of
	capture gen played_video = nplay_video > 0 & nplay_video < .
	capture gen posted = nforum_posts > 0 & nforum_posts < .
	capture gen right_answer = grade > 0 & grade < .
	capture gen checked_problem = nproblem_check > 0 & nproblem_check < .

* some helpful labels

	label var played_video "played at least one video"
	label var posted "posted in forum at least once"
	label var right_answer "submitted at least one right answer"
	label var checked_problem "checked at least one problem"
	capture label var usa "located in USA"
	label var mode "certificate mode, e.g. honor or verified"
	label var sum_dt "estimate of total time spent on edx (probably in seconds)"
	label var n_dt "number of distinct time segments spent on edx"
	label var grade "end-of-course grade assigned by edX"

	* save
	save "`pcdta'", replace
}
