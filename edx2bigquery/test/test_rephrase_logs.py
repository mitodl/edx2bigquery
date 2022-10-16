#!/usr/bin/python

import os
import sys
import json
import unittest

import edx2bigquery.main
import edx2bigquery.rephrase_tracking_logs as rephrase_tracking_logs
from edx2bigquery.check_schema_tracking_log import KeyNotInSchema

class Config:
    TIMEZONE = "America/New_York"
    TRACKING_LOGS_DIRECTORY = "/tmp"
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)
        return

class TestRephraseLogs(unittest.TestCase):

    EVENTS = r"""{"name": "play_video", "context": {"user_id": 99364143, "path": "/event", "course_id": "course-v1:MITx+15.455x+3T2022", "org_id": "MITx", "enterprise_uuid": "", "new_key": "foo", "newfield2": 123, "newfield3": {"a": 3, "c": 5}}, "username": "quantum", "session": "426fa344070cba479b52ce359af7d955", "ip": "2a01:cb04:3f:b900:a1ba:4da3:a5ba:e72b", "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0", "host": "courses.edx.org", "referer": "https://courses.edx.org/xblock/block-v1:MITx+15.455x+3T2022+type@vertical+block@0c7d4200ce9c4fc187b5f0e154fecc14?show_title=0&show_bookmark_button=0&recheck_access=1&view=student_view", "accept_language": "en-US,en;q=0.5", "event": "{\"id\": \"2f02b380423f4f74a7f69a6ab2a6a917\", \"code\": \"hls\", \"duration\": 1303.808, \"currentTime\": 0}", "time": "2022-10-10T14:49:45.625763+00:00", "event_type": "play_video", "event_source": "browser", "page": "https://courses.edx.org/xblock/block-v1:MITx+15.455x+3T2022+type@vertical+block@0c7d4200ce9c4fc187b5f0e154fecc14?show_title=0&show_bookmark_button=0&recheck_access=1&view=student_view"}
{"name": "edx.video.language_menu.hidden", "context": {"user_id": 38697191, "path": "/event", "course_id": "course-v1:MITx+15.671.1x+3T2022", "org_id": "MITx", "enterprise_uuid": ""}, "username": "atester", "session": "f96a6aa2f5f2cb547a52e05a53804f9d", "ip": "46.114.150.130", "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15", "host": "courses.edx.org", "referer": "https://courses.edx.org/xblock/block-v1:MITx+15.671.1x+3T2022+type@vertical+block@819239938da84a20a9ac26e9007044c4?show_title=0&show_bookmark_button=0&recheck_access=1&view=student_view", "accept_language": "de-DE,de;q=0.9", "event": "{\"id\": \"5771c0b99bed4f6495490b15a3986eb4\", \"code\": \"hls\", \"duration\": 78.11, \"language\": \"en\"}", "time": "2022-10-10T15:16:50.802890+00:00", "event_type": "video_hide_cc_menu", "event_source": "browser", "page": "https://courses.edx.org/xblock/block-v1:MITx+15.671.1x+3T2022+type@vertical+block@819239938da84a20a9ac26e9007044c4?show_title=0&show_bookmark_button=0&recheck_access=1&view=student_view"}
{"name": "/courses/course-v1:MITx+18.6501x+2T2022/xblock/block-v1:MITx+18.6501x+2T2022+type@problem+block@u03s05_methodestimation-tab10-problem5/handler/xmodule_handler/input_ajax", "context": {"course_id": "course-v1:MITx+18.6501x+2T2022", "course_user_tags": {}, "user_id": 15066491, "path": "/courses/course-v1:MITx+18.6501x+2T2022/xblock/block-v1:MITx+18.6501x+2T2022+type@problem+block@u03s05_methodestimation-tab10-problem5/handler/xmodule_handler/input_ajax", "org_id": "MITx", "enterprise_uuid": ""}, "username": "tester", "session": "e573564c2ff12f96cebd30ab783d16ea", "ip": "2607:fb90:a487:1500:f573:d277:281d:fdc2", "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36", "host": "courses.edx.org", "referer": "https://courses.edx.org/xblock/block-v1:MITx+18.6501x+2T2022+type@vertical+block@u03s05_methodestimation-tab10?show_title=0&show_bookmark_button=0&recheck_access=1&view=student_view&format=Exercises", "accept_language": "en-US,en;q=0.9", "event": "{\"GET\": {}, \"POST\": {\"formula\": [\"1/L\"], \"request_start\": [\"1665414524745\"], \"dispatch\": [\"preview_formcalc\"], \"input_id\": [\"u03s05_methodestimation-tab10-problem5_2_1\"]}}", "time": "2022-10-10T15:08:44.972388+00:00", "event_type": "/courses/course-v1:MITx+18.6501x+2T2022/xblock/block-v1:MITx+18.6501x+2T2022+type@problem+block@u03s05_methodestimation-tab10-problem5/handler/xmodule_handler/input_ajax", "event_source": "server", "page": null}
"""

    def x_test_rephrase1(self):

        rephrase_tracking_logs.DYNAMIC_TRACKING_LOGS_REPHRASING_CONFIG_FN = "/tmp/edx2bigquery_tracking_logs_rephrasing.json"
        os.unlink(rephrase_tracking_logs.DYNAMIC_TRACKING_LOGS_REPHRASING_CONFIG_FN)
        drc = rephrase_tracking_logs.DynamicRephraseConfig()
        assert drc.dfn==rephrase_tracking_logs.DYNAMIC_TRACKING_LOGS_REPHRASING_CONFIG_FN
        rephrasing_list = drc.get_list()
        assert len(rephrasing_list) == 0

        okcnt = 0
        elist = [x for x in self.EVENTS.split('\n') if len(x.strip())]
        for newline in rephrase_tracking_logs.do_rephrase_lines(elist):
            if newline:
                newline = newline.strip()
            if newline:
                okcnt += 1
            print(newline)

        assert okcnt==len(elist)

        rephrasing_list = drc.get_list()
        assert len(rephrasing_list) == 3


    def x_test_rephrase2(self):
        '''
        Test via main
        '''
        tfn = "/tmp/edx2bigquery_test_data.json"
        dtrcfn = "/tmp/edx2bigquery_tracking_logs_rephrasing.json"
        if os.path.exists(dtrcfn):
            os.unlink(dtrcfn)
        ec = Config(COURSE_SQL_BASE_DIR="/tmp",
                    DYNAMIC_TRACKING_LOGS_REPHRASING_CONFIG_FN=dtrcfn,
        )
        edx2bigquery.main.edx2bigquery_config = ec
            
        with open(tfn, 'w') as ofp:
            ofp.write(self.EVENTS)
        argv = f"rephrase_logs {tfn}".split(" ")
        edx2bigquery.main.CommandLine(argv)

    def test_rephrase3(self):
        '''
        Test via main - split_and_rephrase
        '''
        tfn = "/tmp/edx2bigquery_test_data.json"
        dtrcfn = "/tmp/edx2bigquery_tracking_logs_rephrasing.json"
        if os.path.exists(dtrcfn):
            os.unlink(dtrcfn)
        dfn = "/tmp/META/edx2bigquery_test_data"
        if os.path.exists(dfn):
            os.unlink(dfn)

        ec = Config(COURSE_SQL_BASE_DIR="/tmp",
                    DYNAMIC_TRACKING_LOGS_REPHRASING_CONFIG_FN=dtrcfn,
        )
        edx2bigquery.main.edx2bigquery_config = ec
            
        with open(tfn, 'w') as ofp:
            ofp.write(self.EVENTS)
        argv = f"split {tfn}".split(" ")
        print(f"running edx2bigquery {argv}")
        edx2bigquery.main.CommandLine(argv)

if __name__ == '__main__':
    unittest.main()

