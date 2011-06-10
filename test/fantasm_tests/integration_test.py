""" Integration tests for testing the Task execution order etc. """
import logging

import random # pylint: disable-msg=W0611
from fantasm.lock import ReadWriteLock
from fantasm import config # pylint: disable-msg=W0611
from fantasm.fsm import FSM
from fantasm.models import _FantasmFanIn, _FantasmInstance, _FantasmLog
from fantasm_tests.helpers import runQueuedTasks
from fantasm_tests.helpers import overrideFails
from fantasm_tests.helpers import setUpByFilename
from fantasm_tests.helpers import getCounts
from fantasm_tests.fixtures import AppEngineTestCase
from fantasm_tests.fsm_test import TestModel
from fantasm_tests.fsm_test import getLoggingDouble
from fantasm_tests.actions import ContextRecorder, CountExecuteCallsFanIn, TestFileContinuationFSMAction, \
                                  DoubleContinuation1, DoubleContinuation2, ResultModel, CustomImpl
from minimock import mock, restore
from google.appengine.api import datastore_types

# pylint: disable-msg=C0111, W0212, W0612, W0613, C0301
# - docstrings not reqd in unit tests
# - unit tests need access to protected members
# - lots of unused args in unit tests
# - lots of long lines for clarity

class RunTasksBaseTest(AppEngineTestCase):
    
    FILENAME = None
    MACHINE_NAME = None
    METHOD = 'GET'
    
    def setUp(self):
        super(RunTasksBaseTest, self).setUp()
        for i in range(10):
            TestModel(key_name=str(i)).put()
        self.setUpByFilename(self.FILENAME, instanceName='instanceName', machineName=self.MACHINE_NAME, 
                             method=self.METHOD)
        
    def tearDown(self):
        super(RunTasksBaseTest, self).tearDown()
        restore()
        
    def setUpMock(self):
        mock('config.currentConfiguration', returns=self.currentConfig, tracker=None)
        mock('random.randint', returns=1, tracker=None)
        
    def setUpByFilename(self, filename, instanceName=None, taskRetryLimitOverrides=None, machineName=None, 
                        method='GET'):
        setUpByFilename(self, 
                        filename, 
                        instanceName=instanceName, 
                        taskRetryLimitOverrides=taskRetryLimitOverrides, 
                        machineName=machineName,
                        method=method)
        self.setUpMock()
        
        
class LoggingTests( RunTasksBaseTest ):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    
    def setUp(self):
        super(LoggingTests, self).setUp()
        self.context.initialize()
        self.context.logger.persistentLogging = True
        
    def test_FantasmInstance(self):
        self.assertEqual(1, _FantasmInstance.all().count())
        
    def _test_FantasmLog(self, level, logger):
        self.assertEqual(1, _FantasmInstance.all().count())
        logger("message: %s", "foo", exc_info=True)
        runQueuedTasks(queueName=self.context.queueName)
        query = _FantasmLog.all().filter("level =", level)
        self.assertEqual(1, query.count())
        self.assertEqual("message: foo", query.get().message)
        
    def test_FantasmLog_DEBUG(self):
        self._test_FantasmLog(logging.DEBUG, self.context.logger.debug)

    def test_FantasmLog_INFO(self):
        self._test_FantasmLog(logging.INFO, self.context.logger.info)
        
    def test_FantasmLog_WARNING(self):
        self._test_FantasmLog(logging.WARNING, self.context.logger.warning)
        
    def test_FantasmLog_ERROR(self):
        self._test_FantasmLog(logging.ERROR, self.context.logger.error)
        
    def test_FantasmLog_CRITICAL(self):
        self._test_FantasmLog(logging.CRITICAL, self.context.logger.critical)
        
    def test_FantasmInstance_stack(self):
        self.context.logger.critical("message", exc_info=1)
        runQueuedTasks(queueName=self.context.queueName)
        log = _FantasmLog.all().get()
        self.assertEqual("message", log.message)
        self.assertEqual("None\n", log.stack)
        
    def test_FantasmInstance_stack_error(self):
        try:
            list()[0]
        except Exception:
            self.context.logger.critical("message", exc_info=1)
        runQueuedTasks(queueName=self.context.queueName)
        log = _FantasmLog.all().get()
        self.assertEqual("message", log.message)
        self.assertTrue("Traceback" in log.stack and "IndexError" in log.stack)
        
class ParamsTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'ContextRecorder'
    
    def setUp(self):
        super(ParamsTests, self).setUp()
        ContextRecorder.CONTEXTS = []
                
    def tearDown(self):
        super(ParamsTests, self).tearDown()
        ContextRecorder.CONTEXTS = []
        
    def _test_not_a_list(self, method):
        self.context.method = method
        self.context['foo'] = 'bar'
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--state-initial--next-event--state-final--step-1'], ran)
        self.assertEqual([{'foo': 'bar', 
                           '__step__': 1}], ContextRecorder.CONTEXTS)
        
    def _test_list(self, method):
        self.context.method = method
        self.context['foo'] = ['bar1', 'bar2', 'bar3']
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--state-initial--next-event--state-final--step-1'], ran)
        self.assertEqual([{'foo': ['bar1', 'bar2', 'bar3'], 
                           '__step__': 1}], ContextRecorder.CONTEXTS)
        
    def test_GET_not_a_list(self):
        self._test_not_a_list('GET')
        
    def test_POST_not_a_list(self):
        self._test_not_a_list('POST')
        
    def test_GET_list(self):
        self._test_list('GET')
        
    def test_POST_list(self):
        self._test_list('POST')
        
    def _test_lots_of_different_data_types(self, method):
        self.context.method = method
        models = list(TestModel.all())
        
        self.context['db_Key'] = models[0].key()
        self.context['db_Key_defined_in_context_types'] = models[0].key()
        self.context['char'] = 'a'
        self.context['bool1'] = False
        self.context['bool2'] = True
        self.context['str'] = 'abc'
        self.context['str_as_unicode'] = 'abc'
        self.context['unicode'] = u'\xe8' # MUST be context_types
        self.context['list_of_str'] = ['a', 'b', 'c']
        self.context['list_of_str_len_1'] = ['a']
        self.context['list_of_db_Key'] = [models[0].key(), models[1].key(), models[2].key()]
        self.context['list_of_db_Key_len_1'] = [models[0].key()]
        self.context['list_of_mixed'] = ['a', 1, 'b', 2]
        self.context['list_of_unicode'] = [u'\xe8', u'\xe9'] # MUST be context_types
        self.context['dict_int_keys'] = {1: 1, 2: 2} # BAD!!!! not defined in context_types
        self.context['dict_int_keys_defined_in_context_types'] = {1: 1, 2: 2}
        self.context['dict_str_keys'] = {'a': 1, 'b': 2} # BAD!!!! not defined in context_types
        self.context['dict_str_keys_defined_in_context_types'] = {'a': 1, 'b': 2}
        self.context['dict_db_Key'] = {'a': models[1].key()} # BAD!!!! not defined in context_types
        self.context['dict_db_Key_defined_in_context_types'] = {'a': models[1].key()}
        self.context['unicode2'] = "  Mik\xe9 ,  Br\xe9\xe9 ,  Michael.Bree-1@gmail.com ,  Montr\xe9al  ".decode('iso-8859-1')
        self.context['custom'] = [CustomImpl(a='A', b='B')]
        
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--state-initial--next-event--state-final--step-1'], ran)
        
        self.assertEqual([{'custom': [CustomImpl(a="A", b="B")],
                           'db_Key': 'agdmYW50YXNtchALEglUZXN0TW9kZWwiATAM',
                           'db_Key_defined_in_context_types': datastore_types.Key.from_path(u'TestModel', u'0', _app=u'fantasm'),
                           'char': 'a',
                           'unicode': u'\xe8',
                           'bool1': False,
                           'bool2': True,
                           'str': 'abc',
                           'str_as_unicode': u'abc',
                           'list_of_str': ['a', 'b', 'c'],
                           'list_of_str_len_1': ['a'],
                           'list_of_db_Key': [datastore_types.Key.from_path(u'TestModel', u'0', _app=u'fantasm'),
                                              datastore_types.Key.from_path(u'TestModel', u'1', _app=u'fantasm'),
                                              datastore_types.Key.from_path(u'TestModel', u'2', _app=u'fantasm')],
                           'list_of_db_Key_len_1': [datastore_types.Key.from_path(u'TestModel', u'0', _app=u'fantasm')],
                           'list_of_unicode': [u'\xe8', u'\xe9'],
                           'list_of_mixed': ['a', '1', 'b', '2'], 
                           'dict_int_keys': '{"1": 1, "2": 2}',
                           'dict_int_keys_defined_in_context_types': {"1": 1, "2": 2},
                           'dict_str_keys': '{"a": 1, "b": 2}',
                           'dict_str_keys_defined_in_context_types': {"a": 1, "b": 2},
                           'dict_db_Key': '{"a": {"__db.Key__": true, "key": "agdmYW50YXNtchALEglUZXN0TW9kZWwiATEM"}}',
                           'dict_db_Key_defined_in_context_types': {'a': datastore_types.Key.from_path(u'TestModel', u'1', _app=u'fantasm')},
                           'unicode2': u"  Mik\xe9 ,  Br\xe9\xe9 ,  Michael.Bree-1@gmail.com ,  Montr\xe9al  ",
                           '__step__': 1}], ContextRecorder.CONTEXTS)
        
    def test_GET_lots_of_different_data_types(self):
        self._test_lots_of_different_data_types('GET')
        
    def test_POST_lots_of_different_data_types(self):
        self._test_lots_of_different_data_types('POST')
        
class HeadersTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'TaskQueueFSMTests'
    
    def setUp(self):
        super(HeadersTests, self).setUp()
        ContextRecorder.CONTEXTS = []
                
    def tearDown(self):
        super(HeadersTests, self).tearDown()
        ContextRecorder.CONTEXTS = []
        
    def test_headers(self):
        self.context.headers = {'X-Fantasm-Header': 'abc'}
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        
    def test_headers_multi_valued(self):
        self.context.headers = {'X-Fantasm-Header': ['abc', '123']}
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        
class RunTasksTests_TaskQueueFSMTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'TaskQueueFSMTests'

    def test_TaskQueueFSMTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0}, 
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}},
                         getCounts(self.machineConfig))
        
class RunTasksTests_TaskQueueFSMTests_POST(RunTasksTests_TaskQueueFSMTests):
    METHOD = 'POST'
    
class RunTasksTests_TaskQueueFSMTestsFinalExit(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'TaskQueueFSMTestsFinalExit'

    def test_TaskQueueFSMTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2',
                          'instanceName--state-final--pseudo-final--pseudo-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 1}, 
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}},
                         getCounts(self.machineConfig))
        
class RunTasksTests_TaskQueueFSMTestsFinalExit_POST(RunTasksTests_TaskQueueFSMTestsFinalExit):
    METHOD = 'POST'

class RunTasksTests_SelfTransitionTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'SelfTransitionTests'

    def test_SelfTransitionTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state1--step-0', 
                          'instanceName--state1--next-event1--state1--step-1', 
                          'instanceName--state1--next-event1--state1--step-2', 
                          'instanceName--state1--next-event1--state1--step-3', 
                          'instanceName--state1--next-event1--state1--step-4', 
                          'instanceName--state1--next-event1--state1--step-5', 
                          'instanceName--state1--next-event2--state2--step-6'], ran)
        self.assertEqual({'state1': {'entry': 6, 'action': 6, 'exit': 6},
                          'state2': {'entry': 1, 'action': 1, 'exit': 0},
                          'state1--next-event1': {'action': 5},
                          'state1--next-event2': {'action': 1}},
                         getCounts(self.machineConfig))
        
class RunTasksTests_SelfTransitionTests_POST(RunTasksTests_SelfTransitionTests):
    METHOD = 'POST'
        
class RunTasksTests_DatastoreFSMContinuationTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationTests'
        
    def test_DatastoreFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationTests_POST(RunTasksTests_DatastoreFSMContinuationTests):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationQueueTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationQueueTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationQueueTests, self).setUp()
        
        import google.appengine.api.taskqueue.taskqueue_stub as taskqueue_stub
        import google.appengine.api.apiproxy_stub_map as apiproxy_stub_map
        self.__taskqueue = taskqueue_stub.TaskQueueServiceStub(root_path='./test/fantasm_tests/yaml/')
        apiproxy_stub_map.apiproxy._APIProxyStubMap__stub_map.pop('taskqueue')
        apiproxy_stub_map.apiproxy.RegisterStub('taskqueue', self.__taskqueue)
        
    def test_DatastoreFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        
        ran1 = runQueuedTasks(queueName='queue1')
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0'], ran1)
        
        ran2 = runQueuedTasks(queueName='queue2')
        self.assertEqual(['instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1',
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1'], ran2)
        
        ran3 = runQueuedTasks(queueName='queue3')
        self.assertEqual(['instanceName--state-continuation--next-event--state-final--step-2',  
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2',  
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran3)
        
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationQueueTests_POST(RunTasksTests_DatastoreFSMContinuationQueueTests):
    METHOD = 'POST'
    
class RunTasksTests_FileFSMContinuationTests(RunTasksBaseTest):
    
    FILENAME = 'test-FileFSMContinuationTests.yaml'
    MACHINE_NAME = 'FileFSMContinuationTests'
    
    def setUp(self):
        super(RunTasksTests_FileFSMContinuationTests, self).setUp()
        TestFileContinuationFSMAction.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_FileFSMContinuationTests, self).tearDown()
        TestFileContinuationFSMAction.CONTEXTS = []
        
    def test_FileFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-2--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-3--state-continuation--next-event--state-final--step-1'], 
                         ran)
        self.assertEqual({'state-continuation': {'entry': 4, 'action': 4, 'continuation': 4, 'exit': 0},
                          'state-final': {'entry': 4, 'action': 4, 'exit': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        self.assertEqual([{'result': 'a', '__step__': 1}, 
                          {'__step__': 1, 'result': 'b', '__ge__': {'0': 1}}, 
                          {'__step__': 1, 'result': 'c', '__ge__': {'0': 2}}, 
                          {'__step__': 1, 'result': 'd', '__ge__': {'0': 3}}], 
                         TestFileContinuationFSMAction.CONTEXTS)
        
        
class RunTasksTests_FileFSMContinuationTests_POST(RunTasksTests_FileFSMContinuationTests):
    METHOD = 'POST'


class RunTasksTests_DoubleContinuationTests(RunTasksBaseTest):
    
    FILENAME = 'test-DoubleContinuationTests.yaml'
    MACHINE_NAME = 'DoubleContinuationTests'
    
    def setUp(self):
        super(RunTasksTests_DoubleContinuationTests, self).setUp()
        DoubleContinuation1.CONTEXTS = []
        DoubleContinuation2.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_DoubleContinuationTests, self).tearDown()
        DoubleContinuation1.CONTEXTS = []
        DoubleContinuation2.CONTEXTS = []
        
    def test_FileFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        
        # Shawn:
        #
        # I'm suggesting some task names here, but am fine with other stuff.
        # You can see I added a new final state (StateFinal) to show how the continuation
        # markers would carry forward into subsequent states.
        #
        # Also, since I've never run this, my values in the counts and contexts may not be correct.
        
        self.assertEqual(
            ['instanceName--pseudo-init--pseudo-init--DoubleContinuation1--step-0', 
             'instanceName--continuation-0-1--pseudo-init--pseudo-init--DoubleContinuation1--step-0', 
             'instanceName--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-0-2--pseudo-init--pseudo-init--DoubleContinuation1--step-0', 
             'instanceName--continuation-0-1--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-1-1--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-0-2--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-1-1--continuation-0-1--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-0-1--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-1-1--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-1--continuation-0-2--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-0-2--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--continuation-0-1--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-1-1--continuation-0-1--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--continuation-0-2--DoubleContinuation1--ok--DoubleContinuation2--step-1', 
             'instanceName--continuation-1-1--continuation-0-2--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--continuation-0-1--DoubleContinuation2--okfinal--StateFinal--step-2', 
             'instanceName--continuation-1-2--continuation-0-2--DoubleContinuation2--okfinal--StateFinal--step-2'],
            ran)
        self.assertEqual({
            'DoubleContinuation1': {
                'entry': 3,
                'action': 3,
                'continuation': 3,
                'exit': 0
            },
            'DoubleContinuation2': {
                'entry': 9,
                'action': 9,
                'continuation': 9,
                'exit': 0
            },
            'StateFinal': {
                'entry': 9,
                'action': 9,
                'exit': 0
            },
            'DoubleContinuation1--ok': {
                'action': 0
            },
            'DoubleContinuation2--okfinal': {
                'action': 0
            },
        }, getCounts(self.machineConfig))
        self.assertEqual([
            {'c1': '1', '__step__': 1},
            {'c1': '2', '__step__': 1, '__ge__': {'0': 1}},
            {'c1': '3', '__step__': 1, '__ge__': {'0': 2}}
            ], DoubleContinuation1.CONTEXTS
        )
        self.assertEqual([
            {'c2': 'a', u'c1': u'1', u'__step__': 2}, 
            {'c2': 'a', u'c1': u'2', u'__ge__': {u'0': 1}, u'__step__': 2}, 
            {'c2': 'b', u'c1': u'1', u'__ge__': {u'1': 1}, u'__step__': 2}, 
            {'c2': 'a', u'c1': u'3', u'__ge__': {u'0': 2}, u'__step__': 2}, 
            {'c2': 'b', u'c1': u'2', u'__ge__': {u'1': 1, u'0': 1}, u'__step__': 2}, 
            {'c2': 'c', u'c1': u'1', u'__ge__': {u'1': 2}, u'__step__': 2}, 
            {'c2': 'b', u'c1': u'3', u'__ge__': {u'1': 1, u'0': 2}, u'__step__': 2}, 
            {'c2': 'c', u'c1': u'2', u'__ge__': {u'1': 2, u'0': 1}, u'__step__': 2}, 
            {'c2': 'c', u'c1': u'3', u'__ge__': {u'1': 2, u'0': 2}, u'__step__': 2}
            ], DoubleContinuation2.CONTEXTS
        )
        
class RunTasksTests_DoubleContinuationTests_POST(RunTasksTests_DoubleContinuationTests):
    METHOD = 'POST'

class RunTasksTests_DatastoreFSMContinuationAndForkTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationAndForkTests'
        
    def test_DatastoreFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(
            ['instanceName--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--fork--1--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-1--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-1--fork--1--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-2--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-2--fork--1--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-3--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-3--fork--1--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-4--state-continuation-and-fork--next-event--state-final--step-1', 
             'instanceName--continuation-0-4--fork--1--state-continuation-and-fork--next-event--state-final--step-1'],ran)
        self.assertEqual({'state-continuation-and-fork': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 10, 'action': 10, 'exit': 0},
                          'state-continuation-and-fork--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationAndForkTests_POST(RunTasksTests_DatastoreFSMContinuationAndForkTests):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationFanInAndForkTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInAndForkTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInAndForkTests, self).setUp()
        CountExecuteCallsFanIn.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInAndForkTests, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        
    def test_DatastoreFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(
            ['instanceName--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation-and-fork--step-0', 
             'instanceName--state-continuation-and-fork--next-event--state-fan-in--step-1-1', 
             'instanceName--work-index-1--state-fan-in--next-event--state-final--step-2'],ran)
        self.assertEqual({'state-continuation-and-fork': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 10, 'fan-in-action': 10, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation-and-fork--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        self.assertEqual(0, _FantasmFanIn.all().count())
        # pylint: disable-msg=C0301
        # - long lines are much clearer in htis case
        self.assertEqual([{u'__count__': 2, u'key': datastore_types.Key.from_path(u'TestModel', '3', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 2, u'key': datastore_types.Key.from_path(u'TestModel', '2', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 3, u'key': datastore_types.Key.from_path(u'TestModel', '5', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 3, u'key': datastore_types.Key.from_path(u'TestModel', '4', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 4, u'key': datastore_types.Key.from_path(u'TestModel', '7', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 4, u'key': datastore_types.Key.from_path(u'TestModel', '6', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 5, u'key': datastore_types.Key.from_path(u'TestModel', '9', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 5, u'key': datastore_types.Key.from_path(u'TestModel', '8', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}, 
                          {u'__count__': 1, u'key': datastore_types.Key.from_path(u'TestModel', '1', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1},
                          {u'__count__': 1, u'key': datastore_types.Key.from_path(u'TestModel','0', _app=u'fantasm'), 'data': {'a': 'b'}, u'__step__': 1, u'__ix__': 1}], CountExecuteCallsFanIn.CONTEXTS)
        
class RunTasksTests_DatastoreFSMContinuationFanInAndForkTests_POST(
                                                            RunTasksTests_DatastoreFSMContinuationFanInAndForkTests):
    METHOD = 'POST'
        
class RunTasksTests_DatastoreFSMContinuationTestsInitCont(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationTestsInitCont'
        
    def test_DatastoreFSMContinuationTestsInitCont(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-2--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-3--state-continuation--next-event--state-final--step-1', 
                          'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-4--state-continuation--next-event--state-final--step-1'], ran)
        self.assertEqual({'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationTestsInitCont_POST(RunTasksTests_DatastoreFSMContinuationTestsInitCont):
    method = 'POST'
        
class RunTasksTests_DatastoreFSMContinuationFanInTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests, self).setUp()
        CountExecuteCallsFanIn.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        # pylint: disable-msg=C0301
        self.assertEqual([{u'__ix__': 1, u'__count__': 2, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'2', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'3', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 3, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'4', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'5', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 4, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'6', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'7', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 5, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'8', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'9', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 1, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'0', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'1', _app=u'fantasm')]}], CountExecuteCallsFanIn.CONTEXTS)
        self.assertEqual(0, _FantasmFanIn.all().count())
        self.assertEqual(10, ResultModel.get_by_key_name(self.context.instanceName).total)
        
class RunTasksTests_DatastoreFSMContinuationFanInTests_POST(RunTasksTests_DatastoreFSMContinuationFanInTests):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationFanInGroupDefaultTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInGroupDefaultTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInGroupDefaultTests, self).setUp()
        CountExecuteCallsFanIn.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInGroupDefaultTests, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationFanInGroupDefaultTests_POST(
                                                        RunTasksTests_DatastoreFSMContinuationFanInGroupDefaultTests):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationFanInGroupTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInGroupTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInGroupTests, self).setUp()
        CountExecuteCallsFanIn.CONTEXTS = []
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInGroupTests, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2--group-0-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3--group-0', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2--group-2-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3--group-2', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2--group-4-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3--group-4', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2--group-6-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3--group-6', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2--group-8-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3--group-8'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 5, 'action': 5, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationFanInGroupTests_POST(RunTasksTests_DatastoreFSMContinuationFanInGroupTests):
    METHOD = 'POST'
    
    
class RunTasksTests_DatastoreFSMContinuationFanInTests_memcache_problems(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests_memcache_problems, self).setUp()
        self.loggingDouble = getLoggingDouble()
        class BreakReadLock( object ):
            def execute(self, context, obj):
                from google.appengine.api import memcache
                lockKey = 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'
                memcache.set(lockKey, 2**64)
        self.context.currentState.getTransition(FSM.PSEUDO_INIT) \
                    .target.getTransition('next-event') \
                    .target.exitAction = BreakReadLock()
        CountExecuteCallsFanIn.CONTEXTS = []
        ReadWriteLock._BUSY_WAIT_ITER_SECS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITER_SECS = 0
        ReadWriteLock._BUSY_WAIT_ITERS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITERS = 2
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests_memcache_problems, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        ReadWriteLock.BUSY_WAIT_ITER_SECS = ReadWriteLock._BUSY_WAIT_ITER_SECS
        ReadWriteLock.BUSY_WAIT_ITERS = ReadWriteLock._BUSY_WAIT_ITERS
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-continuation--pseudo-final--pseudo-final--step-2', #???
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        # pylint: disable-msg=C0301
        self.assertEqual(["Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'.", 
                          "Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'.", 
                          "Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'.", 
                          "Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'.", 
                          "Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'.", 
                          "Gave up waiting for all fan-in work items with read lock 'instanceName--state-continuation--next-event--state-fan-in--step-2-lock-1'."], self.loggingDouble.messages['critical'])
        # pylint: disable-msg=C0301
        self.assertEqual([{u'__ix__': 1, u'__count__': 2, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'2', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'3', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 3, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'4', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'5', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 4, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'6', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'7', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 5, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'8', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'9', _app=u'fantasm')]}, 
                          {u'__ix__': 1, u'__count__': 1, u'__step__': 2, 'fan-me-in': [datastore_types.Key.from_path(u'TestModel', u'0', _app=u'fantasm'), datastore_types.Key.from_path(u'TestModel', u'1', _app=u'fantasm')]}], CountExecuteCallsFanIn.CONTEXTS)
        self.assertEqual(0, _FantasmFanIn.all().count())
        self.assertEqual(10, ResultModel.get_by_key_name(self.context.instanceName).total)
        
class RunTasksTests_DatastoreFSMContinuationFanInTests__memcache_problems_POST(
                                                    RunTasksTests_DatastoreFSMContinuationFanInTests_memcache_problems):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationFanInTests_fail_post_fan_in(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'
    
    def setUp(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests_fail_post_fan_in, self).setUp()
        CountExecuteCallsFanIn.CONTEXTS = []
        ReadWriteLock._BUSY_WAIT_ITER_SECS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITER_SECS = 0
        ReadWriteLock._BUSY_WAIT_ITERS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITERS = 2
        self.context['UNITTEST_RAISE_AFTER_FAN_IN'] = True
                
    def tearDown(self):
        super(RunTasksTests_DatastoreFSMContinuationFanInTests_fail_post_fan_in, self).tearDown()
        CountExecuteCallsFanIn.CONTEXTS = []
        ReadWriteLock.BUSY_WAIT_ITER_SECS = ReadWriteLock._BUSY_WAIT_ITER_SECS
        ReadWriteLock.BUSY_WAIT_ITERS = ReadWriteLock._BUSY_WAIT_ITERS
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1',
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 2, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        self.assertEqual(10, ResultModel.get_by_key_name(self.context.instanceName).total)
        
class RunTasksTests_DatastoreFSMContinuationFanInTests_fail_post_fan_in_POST(
                                                    RunTasksTests_DatastoreFSMContinuationFanInTests_fail_post_fan_in):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationFanInDiamondTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInDiamondTests'
        
    def test_DatastoreFSMContinuationFanInDiamondTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--state-continuation--happy--state-happy--step-1', 
                          'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--state-continuation--sad--state-sad--step-1', 
                          'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-2--state-continuation--happy--state-happy--step-1', 
                          'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-3--state-continuation--sad--state-sad--step-1', 
                          'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-4--state-continuation--happy--state-happy--step-1', 
                          'instanceName--state-happy--next-event--state-fan-in--step-2-1', 
                          'instanceName--state-sad--next-event--state-fan-in--step-2-1'], ran)
        self.assertEqual({'state-continuation': {'entry': 6, 'continuation': 6, 'action': 5, 'exit': 0},
                          'state-happy': {'entry': 3, 'action': 3, 'exit': 0},
                          'state-sad': {'entry': 2, 'action': 2, 'exit': 0},
                          'state-fan-in': {'entry': 2, 'action': 2, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-continuation--happy': {'action': 0},
                          'state-continuation--sad': {'action': 0},
                          'state-happy--next-event': {'action': 0},
                          'state-sad--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationFanInDiamondTests_POST(
                                                            RunTasksTests_DatastoreFSMContinuationFanInDiamondTests):
    METHOD = 'POST'
        
class RunTasksTests_DatastoreFSMContinuationFanInTestsInitCont(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTestsInitCont'
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-1-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationFanInTestsInitCont_POST(
                                                            RunTasksTests_DatastoreFSMContinuationFanInTestsInitCont):
    METHOD = 'POST'
    
class RunTasksTests_DatastoreFSMContinuationNoFinalAction(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationNoFinalAction'
        
    def test_DatastoreFSMContinuationFanInTests(self):
        # FIXME: this test is non-deterministic based on time.time in _queueDispatchFanIn
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-1--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-2--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-3--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-4--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--continuation-0-5--pseudo-init--pseudo-init--state-continuation--step-0', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-1-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-2',
                          'instanceName--work-index-1--state-final--pseudo-final--pseudo-final--step-3'], ran)
        self.assertEqual({'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 0, 'exit': 1},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        
class RunTasksTests_DatastoreFSMContinuationFanInTestsInitContNoFinalAction_POST(
                                                                 RunTasksTests_DatastoreFSMContinuationNoFinalAction):
    METHOD = 'POST'
        
class RunTasksWithFailuresTests_TaskQueueFSMTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'TaskQueueFSMTests'
    
    def test_TaskQueueFSMTests_initial_state_entry(self):
        overrideFails(self.machineConfig, [('state-initial', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 2, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0}, 
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}},
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_initial_state_do(self):
        overrideFails(self.machineConfig, [('state-initial', 'action', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 2, 'action': 2, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}}, 
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_initial_state_exit(self):
        overrideFails(self.machineConfig, [('state-initial', 'exit', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}}, 
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_normal_state_entry(self):
        overrideFails(self.machineConfig, [('state-normal', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-normal': {'entry': 2, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 2},
                          'state-normal--next-event': {'action': 1}}, 
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_normal_state_do(self):
        overrideFails(self.machineConfig, [('state-normal', 'action', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-normal': {'entry': 2, 'action': 2, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 2},
                          'state-normal--next-event': {'action': 1}}, 
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_normal_state_exit(self):
        overrideFails(self.machineConfig, [('state-normal', 'exit', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1',
                          'instanceName--state-normal--next-event--state-final--step-2', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 1}},  
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_final_state_entry(self):
        overrideFails(self.machineConfig, [('state-final', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1',
                          'instanceName--state-normal--next-event--state-final--step-2', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-final': {'entry': 2, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 2}},  
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_final_state_do(self):
        overrideFails(self.machineConfig, [('state-final', 'action', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1',
                          'instanceName--state-normal--next-event--state-final--step-2', 
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-final': {'entry': 2, 'action': 2, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 2}},  
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_initial_state_next_event(self):
        overrideFails(self.machineConfig, [], [('state-initial--next-event', 1)])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1',
                          'instanceName--state-initial--next-event--state-normal--step-1',
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 2},
                          'state-normal--next-event': {'action': 1}},  
                         getCounts(self.machineConfig))
        
    def test_TaskQueueFSMTests_normal_state_next_event(self):
        overrideFails(self.machineConfig, [], [('state-normal--next-event', 1)])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-normal--step-1', 
                          'instanceName--state-normal--next-event--state-final--step-2',
                          'instanceName--state-normal--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 2},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 2}},  
                         getCounts(self.machineConfig))
        
class RunTasksWithFailuresTests_TaskQueueFSMTests_POST(RunTasksWithFailuresTests_TaskQueueFSMTests):
    METHOD = 'POST'
        
class RunTasksWithFailuresTests_DatastoreFSMContinuationTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationTests'
    
    def test_DatastoreFSMContinuationTests_initial_state_entry(self):
        overrideFails(self.machineConfig, [('state-initial', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 2, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
    def test_DatastoreFSMContinuationTests_initial_state_do(self):
        overrideFails(self.machineConfig, [('state-initial', 'action', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0',
                          'instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 2, 'action': 2, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
    def test_DatastoreFSMContinuationTests_continuation_state_entry(self):
        overrideFails(self.machineConfig, [('state-continuation', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1',
                          'instanceName--state-initial--next-event--state-continuation--step-1',
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 7, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
    def test_DatastoreFSMContinuationTests_continuation_state_continuation_2(self):
        overrideFails(self.machineConfig, [('state-continuation', 'action', (0, 0, 2))], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1',
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 7, 'action': 5, 'continuation': 7, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0}, 
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}},
                         getCounts(self.machineConfig))
        
    def test_DatastoreFSMContinuationTests_continuation_state_do_2(self):
        overrideFails(self.machineConfig, [('state-continuation', 'action', (0, 2, 0))], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1',
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-final--step-2',  
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-continuation--next-event--state-final--step-2', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-continuation--next-event--state-final--step-2'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 7, 'action': 6, 'continuation': 7, 'exit': 0},
                          'state-final': {'entry': 5, 'action': 5, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))
        
class RunTasksWithFailuresTests_DatastoreFSMContinuationTests_POST(
                                                            RunTasksWithFailuresTests_DatastoreFSMContinuationTests):
    METHOD = 'POST'
        
class RunTasksWithFailuresTests_DatastoreFSMContinuationFanInTests(RunTasksBaseTest):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'
    
    def test_DatastoreFSMContinuationTests_initial_state_entry(self):
        overrideFails(self.machineConfig, [('state-initial', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 2, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 1, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 5, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        self.assertEqual(10, TestModel.all().count())
        self.assertEqual(1, ResultModel.all().count())
        self.assertEqual(self.context.instanceName, ResultModel.all().get().key().name())
        self.assertEqual(10, ResultModel.get_by_key_name(self.context.instanceName).total)
        
    def test_DatastoreFSMContinuationTests_fan_in_state_entry(self):
        overrideFails(self.machineConfig, [('state-fan-in', 'entry', 1)], [])
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(['instanceName--pseudo-init--pseudo-init--state-initial--step-0', 
                          'instanceName--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-2--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-3--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-4--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--continuation-1-5--state-initial--next-event--state-continuation--step-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1', 
                          'instanceName--state-continuation--next-event--state-fan-in--step-2-1', 
                          'instanceName--work-index-1--state-fan-in--next-event--state-final--step-3'], ran)
        self.assertEqual({'state-initial': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-continuation': {'entry': 6, 'action': 5, 'continuation': 6, 'exit': 0},
                          'state-fan-in': {'entry': 2, 'action': 1, 'exit': 0, 
                                           'fan-in-entry': 10, 'fan-in-action': 5, 'fan-in-exit': 0},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-continuation--next-event': {'action': 0},
                          'state-fan-in--next-event': {'action': 0}}, 
                 getCounts(self.machineConfig))
        self.assertEqual(0, _FantasmFanIn.all().count())
        self.assertEqual(10, ResultModel.get_by_key_name(self.context.instanceName).total)
        
class RunTasksWithFailuresTests_DatastoreFSMContinuationFanInTests_POST(
                                                        RunTasksWithFailuresTests_DatastoreFSMContinuationFanInTests):
    METHOD = 'POST'

class FinalStateCanEmitEventTests(RunTasksBaseTest):
    
    FILENAME = 'test-TaskQueueFSMTests.yaml'
    MACHINE_NAME = 'FinalStateCanEmitEvent'
        
    def test_DatastoreFSMContinuationTests(self):
        self.context.initialize() # queues the first task
        ran = runQueuedTasks(queueName=self.context.queueName)
        counts = getCounts(self.machineConfig)
        self.assertEquals(1, counts['InitialState']['action'])
        self.assertEquals(1, counts['OptionalFinalState']['action'])
        self.assertEquals(1, counts['FinalState']['action'])

class SpawnMachinesTests(RunTasksBaseTest):
    
    FILENAME = 'test-SpawnTests.yaml'
    MACHINE_NAME = 'SpawnTests'
    
    def test_spawnKicksOffMachines(self):
        self.context.initialize()
        ran = runQueuedTasks(queueName=self.context.queueName)
        counts = getCounts(self.machineConfig)
        self.assertEqual({'SpawnTests-InitialState': {'entry': 1, 'action': 1, 'exit': 1}}, 
                 getCounts(self.machineConfig))
        self.assertTrue('instanceName--pseudo-init--pseudo-init--SpawnTests-InitialState--step-0' in ran)
        self.assertTrue('instanceName--pseudo-init--pseudo-init--SpawnTests-InitialState--step-0--startStateMachine-0' in ran)
        self.assertTrue('instanceName--pseudo-init--pseudo-init--SpawnTests-InitialState--step-0--startStateMachine-1' in ran)
        self.assertTrue('instanceName--SpawnTests-InitialState--pseudo-final--pseudo-final--step-1' in ran)
        self.assertEquals({'action': 1, 'entry': 1, 'exit': 1}, counts['SpawnTests-InitialState'])

        # FIXME: counts only considers one machine and needs to be extended
        # FIXME: spawned machines don't have an instrumented instanceName, so the tasks are difficult to compare
        # u'MachineToSpawn-20101020035109-W579H8--MachineToSpawn-InitialState--pseudo-final--pseudo-final--step-1', 
        # u'MachineToSpawn-20101020035109-GKQD74--MachineToSpawn-InitialState--pseudo-final--pseudo-final--step-1'
        # self.assertEquals({'action': 1, 'entry': 1, 'exit': 1}, counts['MachineToSpawn-InitialState'])
