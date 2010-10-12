""" Tests for fantasm.fsm """

import unittest
import urllib
import datetime
from django.utils import simplejson
import random # pylint: disable-msg=W0611
from google.appengine.api.labs.taskqueue.taskqueue import Queue, Task # pylint: disable-msg=W0611
from google.appengine.api import memcache # pylint: disable-msg=W0611
from google.appengine.ext import db
from fantasm import config
from fantasm.handlers import TemporaryStateObject
from fantasm.fsm import FSMContext, FSM, spawn
from fantasm.transition import Transition
from fantasm.exceptions import UnknownMachineError, UnknownStateError, UnknownEventError, \
                               FanInWriteLockFailureRuntimeError, FanInReadLockFailureRuntimeError, \
                               YamlFileCircularImportError
from fantasm.state import State
from fantasm.models import _FantasmFanIn
from fantasm.constants import STATE_PARAM, EVENT_PARAM, INSTANCE_NAME_PARAM, STEPS_PARAM, MACHINE_STATES_ATTRIBUTE, \
                              CONTINUATION_PARAM, INDEX_PARAM, GEN_PARAM, FORKED_CONTEXTS_PARAM, \
                              FORK_PARAM
from fantasm_tests.fixtures import AppEngineTestCase
from fantasm_tests.actions import RaiseExceptionAction, RaiseExceptionContinuationAction
from fantasm_tests.helpers import TaskQueueDouble, getLoggingDouble
from fantasm_tests.helpers import ConfigurationMock
from fantasm_tests.helpers import getFSMFactoryByFilename
from fantasm_tests.helpers import getMachineNameByFilename
from fantasm_tests.helpers import setUpByFilename
from fantasm_tests.helpers import getCounts
from fantasm_tests.actions import CountExecuteCalls
from fantasm_tests.actions import CountExecuteCallsWithFork

from minimock import mock, restore, mocked

# pylint: disable-msg=C0111, W0212, W0612, W0613
# - docstrings not reqd in unit tests
# - unit tests need access to protected members
# - lots of unused args in unit tests

class FSMTests(unittest.TestCase):
    
    def test(self):
        machineName = 'foo'
        machineConfig = config._MachineConfig({'name':machineName})
        machineConfig.addState({'name':'foo', 'initial': True, 'action': 'fantasm_tests.actions.CountExecuteCalls'})
        currentConfig = ConfigurationMock(machines=[machineConfig])
        factory = FSM(currentConfig=currentConfig)
        context = factory.createFSMInstance('foo')

    def test_TaskQueueFSMRetryTests(self):
        setUpByFilename(self, 'test-TaskQueueFSMRetryTests.yaml')
        
    def test_TaskQueueFSMTests(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        
    def test_createFSMInstance_raises_UnknownMachineError(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        self.assertRaises(UnknownMachineError, self.factory.createFSMInstance, 'foo')

    def test_createFSMInstance_raises_UnknownStateError_for_currentState(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        self.assertRaises(UnknownStateError, self.factory.createFSMInstance, 'TaskQueueFSMTests', 
                          currentStateName='foo')
        
    def test_TestYamlFileLocation(self):
        setUpByFilename(self, 'test-TestYamlFileLocation.yaml', machineName='MyMachine')
        
    def test_transitionRetryPolicyOverridesMachineLevelPolicy(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        transInitialToNormal = self.initialState._eventToTransition['next-event']
        self.assertNotEquals(self.machineConfig.maxRetries, transInitialToNormal.maxRetries)
        
    def test_createFSMInstance_no_initial_data(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        context = self.factory.createFSMInstance('TaskQueueFSMTests')
        self.assertEqual({}, context)
        
    def test_createFSMInstance_initial_data(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        context = self.factory.createFSMInstance('TaskQueueFSMTests', data={'a' : 'b'})
        self.assertEqual({'a' : 'b'}, context)
        
class FSMContextTests(unittest.TestCase):
    
    def setUp(self):
        super(FSMContextTests, self).setUp()
        self.state = State('foo', None, CountExecuteCalls(), None)
        self.state2 = State('foo2', None, CountExecuteCallsWithFork(), None)
        self.state.addTransition(Transition('t1', self.state2), 'event')
        self.state3 = State('foo3', None, None, None)
        self.state2.addTransition(Transition('t2', self.state3), 'event')
        self.context = FSMContext(self.state, machineName='machineName')
        self.machineName = self.context.machineName

    def test_contextValueSet(self):
        self.context['foo'] = 'bar'
        self.assertEquals(self.context.get('foo'), 'bar')
        
    def test_contextValuePop(self):
        self.context['foo'] = 'bar'
        self.assertEquals(self.context.pop('foo'), 'bar')
        self.assertEquals(self.context.pop('foo', None), None)
        
    def test_contextValueOverridden(self):
        self.context['foo'] = 'bar'
        self.context['foo'] = 'bar2'
        self.assertEquals(self.context.get('foo'), 'bar2')
        
    def test_contextKeyMissingReturnsNone(self):
        self.assertEqual(None, self.context.get('unknown-key'))
        
    def test_contextGeneratesInstanceName(self):
        self.assertTrue(self.context.instanceName.startswith(self.machineName))
        
    def test_generatedContextNameIsUnique(self):
        instanceName1 = self.context.instanceName
        context2 = FSMContext({self.state.name : self.state}, self.state)
        instanceName2 = context2.instanceName
        self.assertNotEquals(instanceName1, instanceName2)
        
    def test_clone(self):
        self.context['foo'] = 'bar'
        clone = self.context.clone()
        self.assertEqual({'foo': 'bar'}, self.context)
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertEqual(self.context.instanceName, clone.instanceName)
        
        self.context['bar'] = 'foo'
        self.assertEqual({'foo': 'bar', 'bar': 'foo'}, self.context)
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertEqual(self.context.instanceName, clone.instanceName)
        
    def test_clone_data(self):
        self.context['foo'] = 'bar'
        clone = self.context.clone(data={'abc': '123'})
        self.assertEqual({'foo': 'bar', 'abc': '123'}, clone)
        
    def test_clone_instanceName(self):
        self.context['foo'] = 'bar'
        clone = self.context.clone(instanceName='foo')
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertNotEqual(self.context.instanceName, clone.instanceName)
        
    def test_fork(self):
        obj = {}
        self.context.fork(obj)
        self.assertEqual({FORKED_CONTEXTS_PARAM: [{FORK_PARAM: 0}]}, obj)
        self.context.fork(obj)
        self.assertEqual({FORKED_CONTEXTS_PARAM: [{FORK_PARAM: 0}, {FORK_PARAM: 1}]}, obj)
        
class FSMContextMergeJoinTests(AppEngineTestCase):
    
    def setUp(self):
        super(FSMContextMergeJoinTests, self).setUp()
        self.state = State('foo', None, CountExecuteCalls(), None)
        self.state2 = State('foo2', None, CountExecuteCallsWithFork(), None)
        self.state.addTransition(Transition('t1', self.state2), 'event')
        self.context = FSMContext(self.state, 
                                  currentState=self.state, 
                                  machineName='machineName', 
                                  instanceName='instanceName')
        self.context[INDEX_PARAM] = 1
        self.context[STEPS_PARAM] = 0
        
    def test_mergeJoinDispatch_1_context(self):
        _FantasmFanIn(workIndex='instanceName--foo--event--foo2--step-0-2654435761').put()
        self.assertEqual(1, _FantasmFanIn.all().count())
        contexts = self.context.mergeJoinDispatch('event', {})
        self.assertEqual([{'__ix__': 1, '__step__': 0}], contexts)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
    def test_mergeJoinDispatch_1234_contexts(self):
        for i in xrange(1234):
            _FantasmFanIn(workIndex='instanceName--foo--event--foo2--step-0-2654435761').put()
        self.assertEqual(1000, _FantasmFanIn.all().count()) # can't get them all with .count()
        contexts = self.context.mergeJoinDispatch('event', {})
        self.assertEqual(1234, len(contexts))
        self.assertEqual(0, _FantasmFanIn.all().count())
        


class TaskQueueFSMTests(AppEngineTestCase):
    
    def setUp(self):
        super(TaskQueueFSMTests, self).setUp()
        filename = 'test-TaskQueueFSMTests.yaml'
        setUpByFilename(self, filename)
        machineName = getMachineNameByFilename(filename)
        
        self.stateInitial = self.factory.machines[machineName][MACHINE_STATES_ATTRIBUTE]['state-initial']
        self.stateNormal = self.factory.machines[machineName][MACHINE_STATES_ATTRIBUTE]['state-normal']
        self.stateFinal = self.factory.machines[machineName][MACHINE_STATES_ATTRIBUTE]['state-final']
        self.transInitialToNormal = self.stateInitial._eventToTransition['next-event']
        self.transNormalToFinal = self.stateNormal._eventToTransition['next-event']
        
    def tearDown(self):
        super(TaskQueueFSMTests, self).tearDown()
        restore()
        
    def assertTaskUrlHasStateAndEvent(self, task, expectedState, expectedEvent):
        # '/fantasm/fsm/TaskQueueFSMTests/?__st__=state-normal&__ev__=next-event&arg1=val1&arg2=val2'
        stateParams = '%s=%s' % (STATE_PARAM, expectedState)
        eventParams = '%s=%s' % (EVENT_PARAM, expectedEvent)
        self.assertTrue(stateParams in task.url)
        self.assertTrue(eventParams in task.url)
        
    def assertTaskUrlHasInstanceName(self, task, instanceName):
        instanceParams = '%s=%s' % (INSTANCE_NAME_PARAM, instanceName)
        self.assertTrue(instanceParams in task.url)

    def test_initialialize_counts(self):
        event = self.context.initialize()
        self.assertEqual('pseudo-init', event)
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 0}, 
                          'state-normal': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-final': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-normal--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))

    def test_initialDispatchEmitsEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        event = self.context.initialize()
        self.assertEquals(len(mockQueue.tasks), 1)
        
        self.context.dispatch(event, {})

        self.assertEquals(len(mockQueue.tasks), 2)
        (task, transactional) = mockQueue.tasks[1]
        
        # state-initial is the state we're transitioning FROM
        self.assertTaskUrlHasStateAndEvent(task, 'state-initial', 'next-event') 

    def test_initialDispatch_counts(self):
        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 1}, 
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-final': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 0}}, 
                         getCounts(self.machineConfig))

    def test_normalStateDispatchWithEventEmitsEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        self.assertEquals(len(mockQueue.tasks), 1)
        (task, transactional) = mockQueue.tasks[0]
        # state-normal is the state we're transitioning FROM
        self.assertTaskUrlHasStateAndEvent(task, 'state-normal', 'next-event') 

    def test_normalStateDispatchWithEventRespectsCountdown(self):
        import time
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        (task, transactional) = mockQueue.tasks[0]
        self.assertTrue(time.time()+20 - getattr(task, '_Task__eta_posix') < 0.01)
        
    def test_finalStateDispatch_counts(self):
        self.context.currentState = self.stateNormal
        self.context.dispatch('next-event', {})
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 0}, 
                          'state-normal': {'entry': 0, 'action': 0, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-normal--next-event': {'action': 1}}, 
                         getCounts(self.machineConfig))
        
    def test_finalStateDispatchWithEventEmitsNoEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateNormal
        self.context.dispatch('next-event', {})

        self.assertEquals(len(mockQueue.tasks), 0)
        
    def test_unknownEventLogsCriticalEvent(self):
        loggingDouble = getLoggingDouble()
        
        self.context.currentState = self.stateInitial
        self.assertRaises(UnknownEventError, self.context.dispatch, 'bad-event', {})
        # The "bad-event" message is logged twice: once when first looking it up (which raises exception),
        # then again when handling the exception (which uses the event to find the transition to find the retry policy)
        self.assertEquals(loggingDouble.count['critical'], 2)
        
    def test_nonFinalStateEmittingNoEventLogsCriticalEvent(self):
        
        def executeReturnsNoEvent(context, obj):
            return None
            
        mock(name='CountExecuteCalls.execute', returns_func=executeReturnsNoEvent, tracker=None)
        loggingDouble = getLoggingDouble()
        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})
        self.assertEquals(loggingDouble.count['critical'], 1)
        
    def test_instanceNameIsPropagated(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        event = self.context.initialize()
        self.context.dispatch(event, {})

        self.assertEquals(len(mockQueue.tasks), 2)
        (task, transactional) = mockQueue.tasks[0]
        self.assertTaskUrlHasInstanceName(task, self.context.instanceName)

    def test_getTaskName(self):
        self.context.dispatch(self.context.initialize(), {})
        self.context.instanceName = 'instanceName'
        self.assertEqual('instanceName--state-initial--next-event--state-normal--step-1', 
                         self.context.getTaskName('next-event'))
        
    def test_getTaskName_multiple_steps(self):
        self.context.dispatch(self.context.initialize(), {})
        self.context.instanceName = 'instanceName'
        self.context[STEPS_PARAM] = '123'
        self.assertEqual('instanceName--state-initial--next-event--state-normal--step-123', 
                         self.context.getTaskName('next-event'))
        
    def test_taskQueueOnQueueSpecifiedAtMachineLevel(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.queueName = 'fantasm-queue'
        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        self.assertEquals(mockQueue.name, 'fantasm-queue')
        
    
    # These tests are not raising as expected. The mock object is not being called. TODO sort this out     
    # def test_nextEventNotStringRaisesException(self):
    #     def executeReturnsNoEvent(context, object):
    #         return None
    #         
    #     mock(name='CountExecuteCalls.execute', returns_func=executeReturnsNoEvent, tracker=None)
    #     self.context.currentState = self.stateNormal
    #     self.transNormalToFinal.maxRetries = 10
    #     self.context.maxRetries = 10
    #     self.assertRaises(InvalidEventNameRuntimeError, self.context.dispatch, 'next-event', None)
    #     
    # def test_invalidNextEventRaisesException(self):
    #     def executeReturnsBadEvent(context, object):
    #         return '*%&#)%&'
    #         
    #     mock(name='CountExecuteCalls.execute', returns_func=executeReturnsBadEvent, tracker=None)
    #     self.context.currentState = self.stateNormal
    #     self.transNormalToFinal.maxRetries = 10
    #     self.context.maxRetries = 10
    #     self.assertRaises(InvalidEventNameRuntimeError, self.context.dispatch, 'next-event', None)

# some bits borrowed from the taskqueue implementation
class _UTCTimeZone(datetime.tzinfo):
    """UTC timezone."""
    ZERO = datetime.timedelta(0)
    def utcoffset(self, dt):
        return self.ZERO
    def dst(self, dt):
        return self.ZERO
    def tzname(self, dt):
        return 'UTC'
_UTC = _UTCTimeZone()

class TaskQueueFSMRetryTests(AppEngineTestCase):
    
    def setUp(self):
        super(TaskQueueFSMRetryTests, self).setUp()
        filename = 'test-TaskQueueFSMRetryTests.yaml'
        machineName = getMachineNameByFilename(filename)
        self.factory = getFSMFactoryByFilename(filename)
        self.context = self.factory.createFSMInstance(machineName)
        
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()
        
    def tearDown(self):
        super(TaskQueueFSMRetryTests, self).tearDown()
        restore()
        
    def parseRetryFromName(self, task):
        name = task.name
        retry = int(name.split('--')[4])
        return retry

    def assertApproximates(self, first, second, tolerance, msg=None):
        """Assert that first is near second by at most tolerance."""
        if abs(first - second) > tolerance:
            raise self.failureException(msg or "abs(%r - %r) > %r" % (first, second, tolerance))
            
    def getDatetimeForCountdown(self, countdown):
        """ Baesd on some code from taskqueue used to compute an eta from a countdown. """
        return datetime.datetime.now().replace(tzinfo=_UTC).astimezone(_UTC) + datetime.timedelta(seconds=countdown)
                                    
    def test_maximumRetriesDoesNotRequeue(self):
        pass # TODO
        
    def test_maximumRetriesEmitsCriticalLog(self):
        pass # TODO

class TestModel(db.Model):
    prop1 = db.StringProperty()

class DatastoreFSMContinuationBaseTests(AppEngineTestCase):
    
    FILENAME = None
    MACHINE_NAME = None
    
    def setUp(self):
        super(DatastoreFSMContinuationBaseTests, self).setUp()
        setUpByFilename(self, self.FILENAME, instanceName='instanceName', machineName=self.MACHINE_NAME)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()
        self.modelKeys = []
        for i in range(10):
            modelKey = TestModel().put()
            self.modelKeys.append(modelKey)
            
    def tearDown(self):
        super(DatastoreFSMContinuationBaseTests, self).tearDown()
        restore()
        
class DatastoreFSMContinuationTests(DatastoreFSMContinuationBaseTests):
    
    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationTests'
        
    def test_DatastoreFSMContinuation_smoke_test(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)
        
    def test_DatastoreFSMContinuation_continuation_param_is_popped_from_context(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        
        # test that continuation pops the continuation param out for current machine
        obj = TemporaryStateObject()
        query = db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
        query.fetch(5)
        cursor = query.cursor()
        self.context[CONTINUATION_PARAM] = cursor
        event = self.context.dispatch(event, obj)
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM)) # continuation param is popped out
        self.assertEqual(self.modelKeys[5:7], [m.key() for m in obj.results])
        
        # and check that the expected cursor is in the continuation task
        query.with_cursor(cursor) # unexpected - i would have though the previous fetch() would leave the cursor
        query.fetch(2)
        self.assertTrue(urllib.quote(query.cursor()) in self.mockQueue.tasks[-2][0].url)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)
        
    def test_DatastoreFSMContinuation_queues_a_continuation_task(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                         self.mockQueue.tasks[-2][0].name)
        self.assertEqual(4, len(self.mockQueue.tasks))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))
        
        self.assertEqual(None, event)
        
    def test_DatastoreFSMContinuation_queue_continuation_fails_if_already_queued(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))
        
        # patch a failing do action
        originalAction = self.context.currentState.getTransition(event).target.doAction
        self.context.currentState.getTransition(event).target.doAction = RaiseExceptionContinuationAction()
        self.assertRaises(Exception, self.context.dispatch, event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))
        self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1', 
                         self.mockQueue.tasks[-2][0].name)
        self.context.currentState.getTransition(event).target.doAction = originalAction # patch it back
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('Unable to queue continuation Task as it already exists. ' +
                         '(Machine DatastoreFSMContinuationTests, State state-continuation)', 
                         self.loggingDouble.messages['info'][-1])
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(5, len(self.mockQueue.tasks))
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(5, len(self.mockQueue.tasks))
        
        self.assertEqual(None, event)
        
class DatastoreFSMContinuationFanInTests(DatastoreFSMContinuationBaseTests):
    
    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'
        
    def test_DatastoreFSMContinuationFanIn_smoke_test(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-fan-in', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
    def test_DatastoreFSMContinuationFanInTests_write_lock_error(self):
        obj = TemporaryStateObject()
        
        event = self.context.initialize() # queues the first task
        self.assertEqual('pseudo-init', self.context.currentState.name)
        
        event = self.context.dispatch(event, obj)
        self.assertEqual('state-initial', self.context.currentState.name)
        
        mock('memcache.incr', returns=1, tracker=None)
        
        # the .dispatch() takes the machine into "state-continuation" and the queueDispatch() looks
        # ahead to determine that _queueDispatchFanIn() is required, which raises the expected error
        # due to lock acquisition problems
        self.assertRaises(FanInWriteLockFailureRuntimeError, self.context.dispatch, event, obj)
        
    def test_DatastoreFSMContinuationFanIn_race_on_work_delete(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all().count())
        
        writeLock = '%s-lock-%d' % (self.context.getTaskName(event, fanIn=True), self.context.get(INDEX_PARAM))
        readLock = '%s-readlock-%d' % (self.context.getTaskName(event, fanIn=True), self.context.get(INDEX_PARAM))
        def memcacheget(arg):
            if arg == readLock:
                return 'not-me'
            return mocked[1][0](arg)
        mock('memcache.get', returns_func=memcacheget, tracker=None)
        
        self.assertRaises(FanInReadLockFailureRuntimeError, self.context.dispatch, event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        
    def test_DatastoreFSMContinuationFanIn_work_packages_restored_on_exception(self):
        
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all().count())
        
        # override the action of the transition raise an exception
        originalAction = self.context.currentState.getTransition(event).action
        self.context.currentState.getTransition(event).action = RaiseExceptionAction()
        self.assertRaises(Exception, self.context.dispatch, event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all().count()) # the work packages are restored on exception
        self.context.currentState.getTransition(event).action = originalAction # and restore
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-fan-in', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())
        
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all().count())

class ContextTypesCoercionTests(unittest.TestCase):
    
    def setUp(self):
        super(ContextTypesCoercionTests, self).setUp()
        setUpByFilename(self, 'test-TypeCoercionTests.yaml')
        
    def test_incomingItemsArePlacedIntoContextAsCorrectDatatype(self):
        self.context.putTypedValue('counter', '123')
        self.context.putTypedValue('batch-key', 'agxmYW50YXNtLXRlc3RyEAsSCkVtYWlsQmF0Y2gYUAw')
        self.context.putTypedValue('data', simplejson.dumps({'a': 'a'}))
        self.context.putTypedValue('start-date', '1283823070')
        self.assertEquals(self.context['counter'], 123)
        self.assertTrue(isinstance(self.context['batch-key'], db.Key))
        self.assertEqual({'a': 'a'}, self.context['data'])
        self.assertEqual(datetime.datetime(2010, 9, 7, 1, 31, 10), self.context['start-date'])
        
    def test_internalParametersArePlacedIntoContextAsCorrectDatatype(self):
        self.context.putTypedValue(STEPS_PARAM, '123')
        self.assertEquals(self.context[STEPS_PARAM], 123)
        
        self.context.putTypedValue(GEN_PARAM, '{"123": 123}')
        self.assertEquals(self.context[GEN_PARAM], {'123': 123})
        
        self.context.putTypedValue(INDEX_PARAM, '123')
        self.assertEquals(self.context[INDEX_PARAM], 123)
        
class ContextYamlImportTests(unittest.TestCase):
    
    def setUp(self):
        super(ContextYamlImportTests, self).setUp()
        
    def test_imports_only(self):
        setUpByFilename(self, 'test-YamlImportOnly.yaml', machineName='TypeCoercionTests')
        self.assertEqual(['MyMachine', 'TypeCoercionTests'], [machine for machine in self.currentConfig.machines])
       
    def test_imports_and_machines(self):
        setUpByFilename(self, 'test-YamlImport.yaml', machineName='Foo')
        self.assertEqual(['MyMachine', 'TypeCoercionTests', 'Foo'], [machine for machine in self.currentConfig.machines])
        
    def test_import_circular_fails(self):
        self.assertRaises(YamlFileCircularImportError, setUpByFilename, self, 'test-YamlImportCircular.yaml', machineName='Foo')

class SpawnTests(unittest.TestCase):

    def setUp(self):
        super(SpawnTests, self).setUp()
        filename = 'test-TaskQueueFSMTests.yaml'
        setUpByFilename(self, filename)
        self.machineName = getMachineNameByFilename(filename)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        
    def tearDown(self):
        super(SpawnTests, self).tearDown()
        restore()
    
    def test_spawnWithNoContextDoesNotQueueAnything(self):
        spawn(self.machineName, None, currentConfig=self.currentConfig)
        self.assertEquals(len(self.mockQueue.tasks), 0)
        
    def test_spawnWithOneContextQueuesOne(self):
        spawn(self.machineName, {'a': '1'}, currentConfig=self.currentConfig)
        self.assertEquals(len(self.mockQueue.tasks), 1)
        
    def test_spawnWithTwoContextsQueuesTwo(self):
        spawn(self.machineName, [{'a': '1'}, {'b': '2'}], currentConfig=self.currentConfig)
        self.assertEquals(len(self.mockQueue.tasks), 2)
        
    def test_spawnUsesCorrectUrl(self):
        spawn(self.machineName, [{'a': '1'}, {'b': '2'}], currentConfig=self.currentConfig)
        self.assertTrue(self.mockQueue.tasks[0][0].url.startswith('/fantasm/fsm/%s' % self.machineName))
        self.assertTrue(self.mockQueue.tasks[1][0].url.startswith('/fantasm/fsm/%s' % self.machineName))
        
    def test_contextAreIncludedInTasks(self):
        spawn(self.machineName, [{'a': '1'}, {'b': '2'}], currentConfig=self.currentConfig)
        self.assertTrue('a=1' in self.mockQueue.tasks[0][0].url)
        self.assertTrue('b=2' in self.mockQueue.tasks[1][0].url)

    def test_countdownIsIncludedInTask(self):
        # having trouble mocking Task, so I'll dip into a private attribute right on task
        import time
        spawn(self.machineName, {'a': '1'}, countdown=20, currentConfig=self.currentConfig)
        self.assertTrue(time.time()+20 - getattr(self.mockQueue.tasks[0][0], '_Task__eta_posix') < 0.01)
