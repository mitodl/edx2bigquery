*****************************************************************************
* utility stata programs for edx2bigquery interface
*
* requires global variables:
*
* project_id    - string specifying bigquery project
* working_dir   - string specifying path of working directory
* bin_dir       - string specfying path of edx2bigquery script binaries

*****************************************************************************
* install fsum package, if not already available

preserve
clear
set obs 1
gen test = 1
capture fsum
if (_rc > 0){
	* net install fsum.pkg
	ssc install fsum
}
restore

*****************************************************************************
* program to get dataset which may be gzipped

capture program drop get_dataset_gzip_ok
program define get_dataset_gzip_ok, rclass
	args dtafn
	di "===> loading data file `dtafn'"
	capture confirm file "`dtafn'"
	if (_rc == 0) {
		use "`dtafn'", replace
	}
	else{
	    capture confirm file "`dtafn'.gz"
		if (_rc == 0) {
			di "    file `dtafn'.gz exists, reading"
			local used_zip = 1
			!gzip -dc `dtafn'.gz > `dtafn'
			use "`dtafn'", replace
			!rm `dtafn'	
		}
	}
end

*****************************************************************************
* program to get data from bigquery

capture program drop get_bq_data_csv
program define get_bq_data_csv, rclass
	args tablename csvfn dtafn

	local used_zip = 0
	di "===> getting BQ data file for `dtafn'"
	capture confirm file "`dtafn'"
	if ((_rc > 0)|$force_recompute) {

	    capture confirm file "`dtafn'.gz"
	    if ((_rc > 0)|$force_recompute) {

	       local cmd = "$bin_dir/GET_DATA_FROM_BQ $project_id `tablename' $working_dir `csvfn'"
	       di "Running `cmd'"
	       !`cmd'
	       capture confirm file "`csvfn'"

	       if !(_rc==0) {
		   di "============================================================================="
		   di "ERROR! Failed to get required file `csvfn' !  ABORTING!!!"
		   exit, clear
	       }
	       import delimited "`csvfn'", encoding(ISO-8859-1)clear
	       save "`dtafn'", replace
	       !gzip -9f `dtafn'
	       !rm `csvfn'
	       !rm `dtafn'
	       local used_zip = 1
	   }
	   else {
	       di "    file `dtafn'.gz exists, reading"
	       local used_zip = 1
	       !gzip -dc `dtafn'.gz > `dtafn'
	       use "`dtafn'", replace
	       !rm `dtafn'
	   }
	}
	else{
		use "`dtafn'", replace
	}
	return scalar used_zip = `used_zip'
end

*****************************************************************************
* program to make a simple text-format schema string of variablename:variabletype,...
* for use in uploading data to bigquery

capture program drop make_bq_text_schema
program define make_bq_text_schema, rclass
	preserve
	describe, replace clear
	* make bq schema text string from variable descriptions
	* this is name:type, where type is string, integer, float
	
	local N = _N
	local stext = ""
	forvalues k = 1/`N' {
		local sname = name[`k']
		if (type[`k']=="int"){
			local stype = "integer"
		}
		else{
			if (isnumeric[`k']){
				local stype = "float"
			}
			else{
				local stype = "string"
			}
		}
		if (`k'>1){
			local stext = "`stext',"
		}
		local stext = "`stext'`sname':`stype'"
	}
	di "schema string = `stext'"
	return local stext = "`stext'"
	restore
end

*****************************************************************************
* program to make a json-format schema file with type, name, description
* for use in uploading data to bigquery.  description = label;
* type = STRING, FLOAT, INTEGER; name = variable_name

capture program drop make_bq_json_schema
program define make_bq_json_schema, rclass
	args jsonfn
	preserve
	describe, replace clear
	* make bq schema json file from variable descriptions
	
	file open jfp using "`jsonfn'", write text replace
	file write jfp "[" _n

	local N = _N
	forvalues k = 1/`N' {
		local sname = name[`k']
		local slabel = varlab[`k']
		if (type[`k']=="int" | type[`k']=="long"){
			local stype = "integer"
		}
		else{
			if (isnumeric[`k']){
				local stype = "float"
			}
			else{
				local stype = "string"
			}
		}
		file write jfp ("{ " +char(34)+ "name" +char(34)+ ": " +char(34)+ "`sname'" +char(34)+ ", ")
		file write jfp (char(34)+ "type" +char(34)+ ": " +char(34)+ "`stype'" +char(34)+ ", ")
		file write jfp (char(34)+ "description" +char(34)+ ": " +char(34)+ "`slabel'" +char(34)+ " }" )
		if (`k' < `N'){
			file write jfp ","
		}
		file write jfp _n
	}
	file write jfp "]" _n
	file close jfp
	di "schema json written to file `jsonfn'"
	restore
end

*****************************************************************************
* program to upload current in-memory data to bigquery, complete with schema.
* variable labels are used for column field descriptions.
* requires helper script: UPLOAD_DATA_TO_BQ

capture program drop upload_data_to_bq
program define upload_data_to_bq, rclass
	args tablename csvfn savecsv description

	if (`savecsv'){
		* store current data as CSV file
		outsheet * using "`csvfn'", comma replace
	}

	* make schema string for bigquery
	* make_bq_text_schema
	* local stext = r(stext)

	* use json schema file: includes labels as descriptions
	local jsonfn = substr("`csvfn'", 1, length("`csvfn'")-4) + ".json"
	make_bq_json_schema "`jsonfn'"
	local stext = "`jsonfn'"

	local cmd = "$bin_dir/UPLOAD_DATA_TO_BQ $project_id `csvfn' `tablename' `stext' $working_dir " ///
			+ "'`description''"
	di "------------------ Uploading `csvfn' to bigquery"
	di "`cmd'"
	! `cmd'
end

