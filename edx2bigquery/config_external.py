#
# File:   config_external.py
# 
# Define (built-in) external commands available as scripts for edx2bigquery, to run on a specified course.
#
# Additional external commands can be defined via edx2bigquery_config.py

import platform

if platform.system()=='Darwin':
    stata = "/Applications/Stata/StataSE.app/Contents/MacOS/stata-se -b {script_name}"
elif platform.system()=='Linux':
    stata = "/usr/local/bin/stata -b {script_name}"
else:
    stata = None

external_commands = {
    'DEFAULT': {},
    'reliability': {
        'name': "Classical test theory (CTT) reliability",
        'description': "Compute Cronbach's alpha for all items in course",
        'type': "stata",
        'template': "{lib}/stata/compute_ctt_alpha.do.template",
        'filename_prefix': "compute_ctt_alpha",
        'script_cmd': stata,
        'script_fn': "{filename_prefix}-{cidns}.do",
        'run_dir': "{working_dir}",
        'logs_dir': "LOGS",
        'output_table': 'item_reliabilities',
        'depends_on': ['course_item', 'person_problem'],
    }
}
