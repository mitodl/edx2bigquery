#--------------------------------------------------------------------------------------------
# utility functions

std_file_name <- function(subdir, tabname){
	fn <- sprintf("%s/%s/DATA-%s%s", working_dir, subdir, cidns, tabname)
	return(fn)
}

dta_name <- function(tabname){
	return(std_file_name("DATA", tabname))
}

plot_name <- function(tabname){
	fn <- sprintf("%s/%s/PLOT-%s%s", working_dir, "PLOTS", cidns, tabname)
	return(fn)
}

get_bq_data_csv <- function(tablename, csvfn){
	# get data from bigquery (or existing data file, if cached locally)
	if (force_recompute || (!file.exists(csvfn))){
		# get data from bq
	       	cmd = sprintf("%s/GET_DATA_FROM_BQ %s %s.%s %s %s", bin_dir, project_id, table_prefix, tablename, working_dir, csvfn)
	       	print(sprintf("Running command '%s'", cmd))
	       	system(cmd)
	}
	if (!file.exists(csvfn)){
	        print(sprintf("ERROR!  Download of %s.%s failed, no file %s", table_prefix, tablename, csvfn))
		quit(status=-1)
	}
	print(sprintf("Loading data from %s", csvfn))
	return(read.csv(csvfn))
}

make_bq_json_schema <- function(df.in, schema.fn){
	# create file with JSON format schema for data frame, suitable for use with BQ
	dtypes <- sapply(df.in, class)
	dnames <- colnames(df.in)
	fp <- file(schema.fn, 'w')
	cat("[\n", file=fp)
	count = 0
	for (name in dnames){
		if (dtypes[name]=="integer"){ 
			stype = "integer"
		} else {
			if (dtypes[name]=="numeric"){ 
				stype = "float" 
			} else {
				stype = "string"
			}
		}
		count = count + 1
		if(count==length(dnames)){ eol = "" }else{ eol="," }
		cat(sprintf('{ "name": "%s", "type": "%s" }%s\n', name, stype, eol), file=fp)
	}
	cat("]\n", file=fp)
}

upload_data_to_bq <- function(df.in, tablename, csvfn, description){
	json.fn = str_replace(csvfn, '.csv', '_schema.json')
	make_bq_json_schema(df.in, json.fn)
	the_table = sprintf("%s.%s", table_prefix, tablename)
	cmd = sprintf("%s/UPLOAD_DATA_TO_BQ %s %s %s %s %s '%s'", bin_dir, project_id, csvfn, the_table, json.fn, working_dir, description)
	print(sprintf("Running %s", cmd))
	system(cmd)
}

upload_file_to_gs <- function(fn, cidns, gsfn){
	# upload file to google storage, using edx2bigquery convention for directory names (as cidns)
	the_gsfn = sprintf("%s/DIST/%s", cidns, gsfn)
	cmd = sprintf("%s/UPLOAD_FILE_TO_GS %s %s %s", bin_dir, project_id, fn, the_gsfn)
	print(sprintf("Running %s", cmd))
	system(cmd)
}
