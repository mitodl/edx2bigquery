****************************************
* make table of items: one per problem
*
* usage:
*
*     run make_table_of_items.do <course_id_name_short> <table_prefix> <output_fn>
*
* Note this also saves DATA/DATA-`cidns'-item-listings.dta - which may not have items being unique

args cidns table_prefix output_fn

****************************************

local ulfn = "`output_fn'"

if(1) {
	get_bq_data_csv "`table_prefix'.course_item"  "`table_prefix'__course_item.csv"  "DATA/DATA-`cidns'__course_item.dta"
	* get_dataset_gzip_ok DATA/DATA-`cidns'__course_item.dta
	
	drop if !(item_number==1)
	drop item_id
	drop item_weight item_number
	drop item_short
	drop item*
	rename problem_id problem_url
	* gen plabel = "[" + string(problem_nid) + "] " + problem_url + " " + chapter_name + "/" + section_name + "/" + problem_name

	capture gen plabel = "[" + string(problem_nid) + "] " + problem_short_id + " " + chapter_name + "/" + section_name + "/" + problem_name

	if (_rc > 0){
		* some courses don't use chapter and section names
		gen plabel = "[" + string(problem_nid) + "] " + problem_short_id + "/" + problem_name
	}
	
	save DATA/DATA-`cidns'-item-listings.dta, replace

	* make version with problem_url guaranteed unique
	collapse (min) problem_nid (firstnm) plabel, by(problem_url)
	sort problem_nid
	save "`ulfn'", replace
}
