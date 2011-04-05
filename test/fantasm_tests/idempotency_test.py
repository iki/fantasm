""" A variety of test of idempotency of framework code. """
import logging
import time
import random
import copy

import google.appengine.api.apiproxy_stub_map as apiproxy_stub_map
from google.appengine.api import memcache
from google.appengine.ext import db

from fantasm.handlers import TemporaryStateObject
from fantasm_tests.fixtures import AppEngineTestCase
from fantasm_tests.helpers import setUpByString
from fantasm_tests.helpers import runQueuedTasks
from fantasm_tests.actions import ResultModel
from fantasm.models import _FantasmTaskSemaphore
from fantasm import config # pylint: disable-msg=W0611
from fantasm import constants
from fantasm.action import DatastoreContinuationFSMAction
from fantasm.lock import ReadWriteLock
from fantasm.models import _FantasmFanIn

from minimock import mock, restore

# pylint: disable-msg=C0111,W0613

SIMPLE_MACHINE = """
state_machines:
  
  - name: SimpleMachine
    namespace: fantasm_tests.idempotency_test
  
    states:
      
    - name: SimpleState
      initial: True
      final: True
      action: SimpleFinalAction
"""

FAN_IN_MACHINE = """
state_machines:
  
  - name: FanInMachine
    namespace: fantasm_tests.idempotency_test
  
    states:
      
    - name: InitialState
      initial: True
      continuation: True
      final: True
      action: ContinuationAction
      transitions:
        - event: 'ok'
          to: FanInState
          
    - name: FanInState
      final: True
      fan_in: 1
      action: FanInAction
"""

class SimpleModel( db.Model ):
    pass

class SimpleFinalAction( object ):
    def execute(self, context, obj):
        SimpleModel().put()
        
class SimpleAction( object ):
    def execute(self, context, obj):
        SimpleModel().put()
        return 'ok'
    
class ContinuationAction( DatastoreContinuationFSMAction ):
    def getQuery(self, context, obj):
        return SimpleModel.all()
    def execute(self, context, obj):
        if obj['results']:
            context['keys'] = [e.key() for e in obj['results']]
            time.sleep(random.uniform(0.0, 1.0))
            return 'ok'
        
class FanInAction( object ):
    CALLS = 0
    def execute(self, contexts, obj):
        keys = []
        for ctx in contexts:
            keys.extend(ctx.get('keys', []))
        def txn():
            FanInAction.CALLS += 1
            result = ResultModel.get_by_key_name('test')
            if not result:
                result = ResultModel(key_name='test', total=0)
            result.total += len(keys)
            result.put()
            if FanInAction.CALLS % 2:
                raise db.Error()
        if keys:
            db.run_in_transaction(txn)

class TaskDoubleExecutionTest( AppEngineTestCase ):
    """
    App Engine Tasks occasionally run multiple times. This tests that 
    the framework successfully handle this.
    """
    def setUp(self):
        super(TaskDoubleExecutionTest, self).setUp()
        setUpByString(self, SIMPLE_MACHINE, machineName='SimpleMachine')
        mock('config.currentConfiguration', returns=self.currentConfig, tracker=None)
        
    def tearDown(self):
        super(TaskDoubleExecutionTest, self).tearDown()
        restore()
    
    def test(self):
        self.context.initialize() # queues the first task
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        self.assertEqual(0, SimpleModel.all().count())
        tq = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
        tasks = tq.GetTasks('default')
        runQueuedTasks(tasksOverride=tasks)
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual(1, SimpleModel.all().count())
        runQueuedTasks(tasksOverride=tasks)
        logging.info([e.key().name() for e in _FantasmTaskSemaphore.all().fetch(100)])
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual(1, SimpleModel.all().count())
        
class FanInTxnException( AppEngineTestCase ):
    """
    App Engine Tasks occasionally run multiple times. This tests that 
    the framework successfully handle this.
    """
    def setUp(self):
        super(FanInTxnException, self).setUp()
        setUpByString(self, FAN_IN_MACHINE, machineName='FanInMachine')
        mock('config.currentConfiguration', returns=self.currentConfig, tracker=None)
        for i in range(20):
            SimpleModel(key_name='%d' % i).put()
        FanInAction.CALLS = 0
        
    def tearDown(self):
        super(FanInTxnException, self).tearDown()
        restore()
        FanInAction.CALLS = 0
    
    def test(self):
        self.context.initialize() # queues the first task
        self.assertEqual(20, SimpleModel.all().count())
        runQueuedTasks(speedup=False)
        result = ResultModel.get_by_key_name('test')
        self.assertEqual(20, result.total)
        
        
class FanInDispatch( AppEngineTestCase ):
    
    def setUp(self):
        super(FanInDispatch, self).setUp()
        setUpByString(self, FAN_IN_MACHINE, machineName='FanInMachine', instanceName='foo')
        mock('config.currentConfiguration', returns=self.currentConfig, tracker=None)
        for i in range(20):
            SimpleModel(key_name='%d' % i).put()
        FanInAction.CALLS = 0
        self.setUpContext()
        
    def setUpContext(self, retryCount=0):
        self.context = self.factory.createFSMInstance(self.machineConfig.name, instanceName='foo')
        self.context[constants.STEPS_PARAM] = 1
        self.obj = TemporaryStateObject()
        self.obj[constants.TASK_NAME_PARAM] = 'taskName'
        self.obj[constants.RETRY_COUNT_PARAM] = retryCount
        random.seed(0) # last step
        
    def tearDown(self):
        restore()
        super(FanInDispatch, self).tearDown()
        
    def test_run_twice(self):
        self.setUpContext()
        self.context.dispatch('pseudo-init', self.obj)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual('foo--InitialState--ok--FanInState--step-2-2957927341', 
                         _FantasmFanIn.all().get().workIndex)
        self.assertEqual('65536', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        
        self.setUpContext()
        self.context.dispatch('pseudo-init', self.obj)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual('foo--InitialState--ok--FanInState--step-2-2957927341', 
                         _FantasmFanIn.all().get().workIndex)
        self.assertEqual('65536', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        
        
    def test_fail_at_currentIndex(self):
        self.setUpContext()
        mock('ReadWriteLock.currentIndex', raises=Exception, tracker=None)
        self.assertRaises(Exception, self.context.dispatch, 'pseudo-init', self.obj)
        self.assertEqual(0, _FantasmFanIn.all().count())
        self.assertEqual(None, memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        restore()
        
        self.setUpContext(retryCount=1)
        self.context.dispatch('pseudo-init', self.obj)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual('foo--InitialState--ok--FanInState--step-2-2957927341', 
                         _FantasmFanIn.all().get().workIndex)
        self.assertEqual('65536', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        
    def test_fail_at_acquireWriteLock(self):
        self.setUpContext()
        mock('ReadWriteLock.acquireWriteLock', raises=Exception, tracker=None)
        self.assertRaises(Exception, self.context.dispatch, 'pseudo-init', self.obj)
        self.assertEqual(0, _FantasmFanIn.all().count())
        self.assertEqual(None, memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        restore()
        
        self.setUpContext(retryCount=1)
        self.context.dispatch('pseudo-init', self.obj)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual('foo--InitialState--ok--FanInState--step-2-2957927341', 
                         _FantasmFanIn.all().get().workIndex)
        self.assertEqual('65536', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        
    def test_fail_at_put(self):
        self.setUpContext()
        mock('db.put', raises=Exception, tracker=None)
        self.assertRaises(Exception, self.context.dispatch, 'pseudo-init', copy.copy(self.obj))
        self.assertEqual(0, _FantasmFanIn.all().count())
        # notice the +1 extra on the lock
        self.assertEqual('65537', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        restore()
        
        self.setUpContext(retryCount=1)
        self.context.dispatch('pseudo-init', self.obj)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual('foo--InitialState--ok--FanInState--step-2-2957927341', 
                         _FantasmFanIn.all().get().workIndex)
        self.assertEqual('65537', memcache.get('foo--InitialState--ok--FanInState--step-2-lock-3255389373'))
        
        