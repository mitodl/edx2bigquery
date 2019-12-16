#!/usr/bin/python

import unittest

from edx2bigquery import addmoduleid


class TestAddModuleID(unittest.TestCase):

    def setUp(self):
        '''declaring inputs for test cases
        '''
        self.okre1_et_input = {'event': '',
                               'event_type': '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                               }
        self.okre1_path_input = {'event': '',
                                 'event_type': '',
                                 'context': {'path': '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                                             }
                                 }
        self.okre2_page_input = {'event': 'input_foo_0_0=',
                                 'event_type': 'problem',
                                 'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
                                 }
        self.cidre11a_event_input = {'event': 'input_i4x-ORGx-Course1x-foo-bar_0_a=',
                                     'event_type': 'problem'
                                     }
        self.okre2_page_pg_input = {'event': ['input_foo_0_0='],
                                    'event_type': 'problem_graded',
                                    'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
                                    }
        self.cidre11a_event_input_pg = {'event': ['input_i4x-ORGx-Course1x-foo-bar_0_a='],
                                        'event_type': 'problem_graded'
                                        }
        self.okre3_event_type_input = {'event': '',
                                       'event_type': '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:a+a+a+type@foo+block@bar'
                                       }
        self.okre3_path_input = {'event': '',
                                 'event_type': '',
                                 'context': {'path': '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:a+a+a+type@foo+block@bar'
                                             }
                                 }
        self.okre4_event_id_input = {'event': {'id': 'block-v1:ORGx+Course1x+4T2099+type@foo+block@bar'
                                               },
                                     'event_type': ''
                                     }
        self.okre5a_event_id_input = {'event': {'id': 'i4x-ORGx-Course1x-foo-bar'
                                                },
                                      'event_type': ''
                                      }
        self.okre5_page_input = {'event': {'id': 'foo'
                                           },
                                 'event_type': 'play_video',
                                 'page': '/courses/course-v1:ORGx+Course1x+4T2099/courseware/chapter0/bar/'
                                 }
        self.okre4_event_input = {'event': 'block-v1:ORGx+Course1x+4T2099+type@foo+block@bar',
                                  'event_type': ''
                                  }
        self.undesired_event_type_1_input = {'event': '',
                                             'event_type': 'add_resource'
                                             }
        self.undesired_event_type_2_input = {'event': '',
                                             'event_type': 'delete_resource'
                                             }
        self.undesired_event_type_3_input = {'event': '',
                                             'event_type': 'recommender_upvote'
                                             }
        self.undesired_event_id_input = {'event': {'id': None
                                                   },
                                         'event_type': ''
                                         }
        self.cidre3_event_id_input = {'event': {'id': 'i4x://ORGx/Course1x/foo/bar'
                                                },
                                      'event_type': '',
                                      'event_source': 'browser'
                                      }
        self.cidre3_event_id_seq_goto_input = {'event': {'id': 'i4x://ORGx/Course1x/foo/bar',
                                                         'new': 'baz'
                                                         },
                                               'event_type': 'seq_goto',
                                               'event_source': 'browser'
                                               }
        self.cidre3_event_id_seq_next_input = {'event': {'id': 'i4x://ORGx/Course1x/foo/bar',
                                                         'new': 'baz'
                                                         },
                                               'event_type': 'seq_next',
                                               'event_source': 'browser'
                                               }
        self.cidre7_page_input = {'event': '',
                                  'event_type': 'page_close',
                                  'event_source': 'browser',
                                  'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/bar/'
                                  }
        self.cidre8_page_input = {'event': '',
                                  'event_type': 'page_close',
                                  'event_source': 'browser',
                                  'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/'
                                  }
        self.cidre5_event_input = {'event': 'input_i4x-ORGx-Course1x-problem-foo_0_0=',
                                   'event_type': '',
                                   'event_source': 'browser',
                                   }
        self.cidre5_event_list_input = {'event': ['input_i4x-ORGx-Course1x-problem-foo_0_0='],
                                        'event_type': '',
                                        'event_source': 'browser',
                                        }
        self.fidre5_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/discussion/threads/foo',
                                        'event_source': ''
                                        }
        self.fidre6_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/discussion/forum/i4xfoo/threads/bar',
                                        'event_source': ''
                                        }
        self.fidre7_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/discussion/i4xfoo/threads/create',
                                        'event_source': ''
                                        }
        self.fidre8_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/discussion/forum/foo/threads/bar',
                                        'event_source': ''
                                        }
        self.cidre6_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/courseware/foo/bar/',
                                        'event_source': ''
                                        }
        self.cidre8_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/courseware/foo/',
                                        'event_source': ''
                                        }
        self.cidre9_event_type_input = {'event': '',
                                        'event_type': '/courses/ORGx/Course1x/4T2019/jump_to_id/foo',
                                        'event_source': ''
                                        }
        self.cidre10_event_type_input = {'event': '',
                                         'event_type': '/courses/ORGx/Course1x/4T2019/xblock/i4x:;_;_a;_a;_foo;_bar/handler/',
                                         'event_source': ''
                                         }
        self.cidre10_path_input = {'event': '',
                                   'event_type': '',
                                   'event_source': '',
                                   'context': {'path': '/courses/ORGx/Course1x/4T2019/xblock/i4x:;_;_a;_a;_foo;_bar/handler/'
                                               }
                                   }
        self.cidre11a_event_no_problem_input = {'event': 'input_i4x-ORGx-Course1x-foo-bar_0_a=',
                                                'event_type': '',
                                                'event_source': ''
                                                }
        self.cidre3_event_type_input = {'event': '',
                                        'event_type': 'i4x://ORGx/Course1x/foo/bar',
                                        'event_source': ''
                                        }
        self.cidre3a_event_type_input = {'event': {'POST': {'position': ['baz']
                                                            }
                                                   },
                                         'event_type': 'i4x://ORGx/Course1x/foo/bar/goto_position',
                                         'event_source': ''
                                         }
        self.cidre3b_event_type_input = {'event': '',
                                         'event_type': 'i4x://ORGx/Course1x/foo/bar/baz',
                                         'event_source': ''
                                         }
        self.cidre3c_event_input = {'event': 'i4x://ORGx/Course1x/foo/bar',
                                    'event_type': '',
                                    'event_source': ''
                                    }
        self.string_event_input = {'event': '',
                                   'event_type': '',
                                   'event_source': ''
                                   }
        self.cidre3_problem_id_input = {'event': {'problem_id': 'i4x://ORGx/Course1x/foo/bar'
                                                  },
                                        'event_type': '',
                                        'event_source': ''
                                        }
        self.cidre4_event_id_input = {'event': {'id': 'i4x-ORGx-Course1x-video-foo'
                                                },
                                      'event_type': '',
                                      'event_source': ''
                                      }
        self.meets_no_criteria_input = {'event': dict(),
                                        'event_type': '',
                                        'event_source': ''
                                        }

    def test_empty_json(self):
        '''tests when an empty dictionary (result of json.loads()) is passed
        '''
        with self.assertRaises(KeyError):
            addmoduleid.guess_module_id(dict())

    def test_okre1_event_type(self):
        '''tests when the okre1 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre1_et_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre1_path(self):
        '''tests when the okre1 regexp searches context > path
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre1_path_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre2_page(self):
        '''tests when the okre2 regexp searches page, event_type is 'problem'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre2_page_input),
                         'ORGx/Course1x/problem/foo')

    def test_cidre11a_event(self):
        '''tests when the cidre11a regexp searches event,
        event_type is 'problem'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre11a_event_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre2_page_pg(self):
        '''tests when the okre2 regexp searches page,
        event_type is 'problem_graded'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre2_page_pg_input),
                         'ORGx/Course1x/problem/foo')

    def test_cidre11a_event_pg(self):
        '''tests when the cidre11a regexp searches event,
        event_type is 'problem_graded'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre11a_event_input_pg),
                         'ORGx/Course1x/foo/bar')

    def test_okre3_event_type(self):
        '''tests when the okre3 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre3_event_type_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre3_path(self):
        '''tests when the okre3 regexp searches context > path
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre3_path_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre4_event_id(self):
        '''tests when the okre4 regexp searches event > id
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre4_event_id_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre5a_event_id(self):
        '''tests when the okre5a regexp searches event > id
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre5a_event_id_input),
                         'ORGx/Course1x/foo/bar')

    def test_okre5_page(self):
        '''tests when the okre5 regexp searches page,
        event_type is 'play_video'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre5_page_input),
                         'ORGx/Course1x/video/foo')

    def test_okre4_event(self):
        '''tests when the okre4 regexp searches event
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.okre4_event_input),
                         'ORGx/Course1x/foo/bar')

    def test_undesired_event_type_1(self):
        '''tests that nothing is returned when event_type is 'add_resource'
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.undesired_event_type_1_input))

    def test_undesired_event_type_2(self):
        '''tests that nothing is returned when event_type is 'delete_resource'
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.undesired_event_type_2_input))

    def test_undesired_event_type_3(self):
        '''tests that nothing is returned when event_type is 'recommender_upvote'
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.undesired_event_type_3_input))

    def test_undesired_event_id(self):
        '''tests that nothing is returned when event > id is not string
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.undesired_event_id_input))

    def test_cidre3_event_id(self):
        '''tests when the cidre3 regexp searches event > id,
        event_source is 'browser'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3_event_id_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre3_event_id_seq_goto(self):
        '''tests when the cidre3 regexp searches event > id,
        event_source is 'browser',
        event_type is 'seq_goto'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3_event_id_seq_goto_input),
                         "ORGx/Course1x/foo/bar/baz")

    def test_cidre3_event_id_seq_next(self):
        '''tests when the cidre3 regexp searches event > id,
        event_source is 'browser',
        event_type is 'seq_next'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3_event_id_seq_next_input),
                         "ORGx/Course1x/foo/bar/baz")

    def test_cidre7_page(self):
        '''tests when the cidre7 regexp searches page,
        event_source is 'browser',
        event_type is 'page_close'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre7_page_input),
                         "ORGx/Course1x/sequential/bar/")

    def test_cidre8_page(self):
        '''tests when the cidre8 regexp searches page,
        event_source is 'browser',
        event_type is 'page_close'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre8_page_input),
                         "ORGx/Course1x/chapter/foo/")

    def test_cidre5_event(self):
        '''tests when the cidre5 regexp searches event,
        event_source is 'browser'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre5_event_input),
                         "ORGx/Course1x/problem/foo")

    def test_cidre5_event_list(self):
        '''tests when the cidre5 regexp searches first element of event list,
        event_source is 'browser'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre5_event_list_input),
                         "ORGx/Course1x/problem/foo")

    def test_fidre5_event_type(self):
        '''tests when the fidre5 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.fidre5_event_type_input),
                         "ORGx/Course1x/forum/foo")

    def test_fidre6_event_type(self):
        '''tests when the fidre6 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.fidre6_event_type_input),
                         "ORGx/Course1x/forum/bar")

    def test_fidre7_event_type(self):
        '''tests when the fidre7 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.fidre7_event_type_input),
                         "ORGx/Course1x/forum/new")

    def test_fidre8_event_type(self):
        '''tests when the fidre8 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.fidre8_event_type_input),
                         "ORGx/Course1x/forum/bar")

    def test_cidre6_event_type(self):
        '''tests when the cidre6 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre6_event_type_input),
                         "ORGx/Course1x/sequential/bar/")

    def test_cidre8_event_type(self):
        '''tests when the cidre8 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre8_event_type_input),
                         "ORGx/Course1x/chapter/foo")

    def test_cidre9_event_type(self):
        '''tests when the cidre9 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre9_event_type_input),
                         "ORGx/Course1x/jump_to_id/foo")

    def test_cidre10_event_type(self):
        '''tests when the cidre10 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre10_event_type_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre10_path(self):
        '''tests when the cidre10 regexp searches context > path
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre10_path_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre11a_event_no_problem(self):
        '''tests when the cidre11a regexp searches event,
        event_type does not contain 'problem'
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre11a_event_no_problem_input),
                         'ORGx/Course1x/foo/bar')

    def test_cidre3_event_type(self):
        '''tests when the cidre3 regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3_event_type_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre3a_event_type(self):
        '''tests when the cidre3a regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3a_event_type_input),
                         "ORGx/Course1x/foo/bar/baz")

    def test_cidre3b_event_type(self):
        '''tests when the cidre3b regexp searches event_type
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3b_event_type_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre3c_event(self):
        '''tests when the cidre3c regexp searches event
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3c_event_input),
                         "ORGx/Course1x/foo/bar")

    def test_str_event(self):
        '''tests when none of the previous criteria have been met,
        event is a string
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.string_event_input))

    def test_cidre3_problem_id(self):
        '''tests when the cidre3 regexp searches event > problem_id
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre3_problem_id_input),
                         "ORGx/Course1x/foo/bar")

    def test_cidre4_event_id(self):
        '''tests when the cidre4 regexp searches event > problem_id
        '''
        self.assertEqual(addmoduleid.guess_module_id(self.cidre4_event_id_input),
                         "ORGx/Course1x/video/foo")

    def test_meets_no_criteria(self):
        '''tests when none of the other criteria have been met
        '''
        self.assertIsNone(addmoduleid.guess_module_id(self.meets_no_criteria_input))


if __name__ == '__main__':
    unittest.main()
