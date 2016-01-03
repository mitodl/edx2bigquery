****************************************
* make table of items: one per problem
*
* usage:
*
*     run make_person_problem_wide.do <course_id_name_short> <table_prefix> <output_fn>
*
* requires globals:
*
*     force_recompute - 1 if data should always be downloaded from bq
*     lib_dir         - path to edx2bigquery library

args cidns table_prefix output_fn

local pitemfn = "`output_fn'"

****************************************
* need unique item listings

local ulfn = "DATA/DATA-`cidns'-item-listings-unique.dta"
di "ulfn = `ulfn'"
capture confirm file "`ulfn'"
if ((_rc > 0) | 0 | $force_recompute) {

	do $lib_dir/stata/make_table_of_items.do `cidns' `table_prefix' `ulfn'
}
else{
	use "`ulfn'", replace
}

****************************************
* now make person problem wide

	* start with long table, direct from bigquery
	get_bq_data_csv "`table_prefix'.person_problem"  "`table_prefix'__person_problem.csv"  "DATA/DATA-`cidns'__person_problem.dta"

	* drop raw_score column
	drop problem_ra~e  
	
	* rename pct_score to be "item" 
	rename problem_pc~e pct_score 

	* now the columns are user_id, problem_nid, pct_score
	* make wide version
	rename pct_score y
	keep user_id problem_nid y
	reshape wide y, i(user_id) j(problem_nid)

	summarize

	* add labels to variables
	foreach x of varlist y* {
		local inum = substr("`x'", 2, length("`x'"))
		preserve
		use "`ulfn'", replace
		local xlabel = subinstr(plabel[`inum'], char(34), "", 100)
		* di `"`xlabel'"'
		* local xlabel = plabel[`inum']
		restore
		quietly label var `x' `"`xlabel'"'
	}

	fsum, label

	save "`pitemfn'", replace
