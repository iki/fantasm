""" Some tests around specific continuation/fan-in issues. """

from google.appengine.ext import db 
from fantasm.action import DatastoreContinuationFSMAction
from fantasm.action import ListContinuationFSMAction
from fantasm_tests.fixtures import AppEngineTestCase
from fantasm_tests.helpers import runQueuedTasks
from fantasm_tests.helpers import setUpByString
from fantasm.models import _FantasmFanIn
from fantasm import config # pylint: disable-msg=W0611
from fantasm.handlers import FSMFanInCleanupHandler # pylint: disable-msg=W0611
from minimock import mock, restore

# pylint: disable-msg=C0111,W0613
# - docstrings not reqd in unit tests
# - arguments 'obj' are often unused in tets

class ContinuationFanInModel( db.Model ):
    value = db.IntegerProperty()

class ContinuationFanInResult( db.Model ):
    values = db.ListProperty(int)

class DatastoreContinuation( DatastoreContinuationFSMAction ):
    def getQuery(self, context, obj):
        return ContinuationFanInModel.all()
    def getBatchSize(self, context, obj):
        return context.get('batchsize', 1)
    def execute(self, context, obj):
        pass
    
class ListContinuation( ListContinuationFSMAction ):
    def getList(self, context, obj):
        return context.get('items', [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    def getBatchSize(self, context, obj):
        return context.get('batchsize', 1)
    def execute(self, context, obj):
        pass
    
class InsideDatastoreContinuationAction( DatastoreContinuation ):
    def execute(self, context, obj):
        if obj['results']:
            context['data'] = [e.value for e in obj['results']]
            return context.get('event', 'ok')
        
class OutsideDatastoreContinuationAction( DatastoreContinuation ):
    def execute(self, context, obj):
        if obj['results']:
            context['data'] = [e.value for e in obj['results']]
        return context.get('event', 'ok') # bad!!! should be inside if
    
class InsideListContinuationAction( ListContinuation ):
    def execute(self, context, obj):
        if obj['results']:
            context['data'] = obj['results']
            return context.get('event', 'ok')
        
class OutsideListContinuationAction( ListContinuation ):
    def execute(self, context, obj):
        if obj['results']:
            context['data'] = obj['results']
        return context.get('event', 'ok') # bad!!! should be inside if
    
class MiddleAction( object ):
    def execute(self, context, obj):
        return 'ok'
        
class FanInAction( object ):
    def execute(self, contexts, obj):
        for context in contexts:
            result = ContinuationFanInResult.get_or_insert('test')
            result.values += context.get('data', [-999])
            result.put()
    
FAN_IN_MACHINE = """
state_machines:
  
  - name: InsideDatastoreFanInMachine
    namespace: fantasm_tests.continuation_fan_in_test
    task_retry_limit: 0
    context_types:
      data: int
      batchsize: int
      items: int
      event: str
  
    states:
      
    - name: InitialState
      initial: True
      final: True
      continuation: True
      action: InsideDatastoreContinuationAction
      transitions:
        - event: 'ok'
          to: MiddleState
        - event: 'fan'
          to: FanInState
          
    - name: MiddleState
      action: MiddleAction
      transitions:
        - event: 'ok'
          to: FanInState
          
    - name: FanInState
      final: True
      fan_in: 1
      action: FanInAction
      
  - name: InsideListFanInMachine
    namespace: fantasm_tests.continuation_fan_in_test
    task_retry_limit: 0
    context_types:
      data: int
      batchsize: int
      items: int
      event: str
  
    states:
      
    - name: InitialState
      initial: True
      final: True
      continuation: True
      action: InsideListContinuationAction
      transitions:
        - event: 'ok'
          to: MiddleState
        - event: 'fan'
          to: FanInState
          
    - name: MiddleState
      action: MiddleAction
      transitions:
        - event: 'ok'
          to: FanInState
          
    - name: FanInState
      final: True
      fan_in: 1
      action: FanInAction
      
  - name: OutsideDatastoreFanInMachine
    namespace: fantasm_tests.continuation_fan_in_test
    task_retry_limit: 0
    context_types:
      data: int
      batchsize: int
      items: int
      event: str
      
    states:
      
    - name: InitialState
      initial: True
      continuation: True
      action: OutsideDatastoreContinuationAction
      transitions:
        - event: 'ok'
          to: MiddleState
        - event: 'fan'
          to: FanInState
          
    - name: MiddleState
      action: MiddleAction
      transitions:
        - event: 'ok'
          to: FanInState
          
    - name: FanInState
      final: True
      fan_in: 1
      action: FanInAction
      
  - name: OutsideListFanInMachine
    namespace: fantasm_tests.continuation_fan_in_test
    task_retry_limit: 0
    context_types:
      data: int
      batchsize: int
      items: int
      event: str
  
    states:
      
    - name: InitialState
      initial: True
      continuation: True
      action: OutsideListContinuationAction
      transitions:
        - event: 'ok'
          to: MiddleState
        - event: 'fan'
          to: FanInState
          
    - name: MiddleState
      action: MiddleAction
      transitions:
        - event: 'ok'
          to: FanInState
          
    - name: FanInState
      final: True
      fan_in: 1
      action: FanInAction
"""

class BaseTest( AppEngineTestCase ):
    """
    Common setUp and tearDown methods
    """
    MACHINE_NAME = None
    EXPECTED_COUNT = None
    EXPECTED_VALUES = None
    EVENT = None
    
    def setUp(self):
        super(BaseTest, self).setUp()
        setUpByString(self, FAN_IN_MACHINE, machineName=self.MACHINE_NAME)
        mock('config.currentConfiguration', returns=self.currentConfig, tracker=None)
        mock('FSMFanInCleanupHandler.post', returns=None, tracker=None)
        for i in range(10):
            ContinuationFanInModel(value=i).put()
        self.context['event'] = self.EVENT
        
    def tearDown(self):
        super(BaseTest, self).tearDown()
        restore()
    
    
class InsideTest( BaseTest ):
    """
    Tests good continuation execute() method
    """
    
    MACHINE_NAME = 'InsideDatastoreFanInMachine'
    EXPECTED_VALUES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    EVENT = 'ok'
    
    def test_batchsize_1(self):
        self.context['batchsize'] = 1
        self.context.initialize() # queues the first task
        runQueuedTasks()
        self.assertEqual(10, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
        
    def test_batchsize_3(self):
        self.context['batchsize'] = 3
        self.context.initialize() # queues the first task
        runQueuedTasks()
        self.assertEqual(4, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
        
    def test_batchsize_10(self):
        self.context['batchsize'] = 10
        self.context.initialize() # queues the first task
        runQueuedTasks()
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
        
    def test_batchsize_11(self):
        self.context['batchsize'] = 11
        self.context.initialize() # queues the first task
        runQueuedTasks()
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
        
class InsideFanTest( InsideTest ):
    EVENT = 'fan'
    
class InsideListTest( InsideTest ):
    MACHINE_NAME = 'InsideListFanInMachine'

class InsideFanListTest( InsideListTest ):
    EVENT = 'fan'
        
class OutsideTest( BaseTest ):
    """
    Tests bad continuation execute() method
    """
    MACHINE_NAME = 'OutsideDatastoreFanInMachine'
    EXPECTED_VALUES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    EVENT = 'ok'
    EXTRA_COUNT = 1
    EXTRA_VALUES = [-999]
    
    def test_batchsize_1(self):
        self.context['batchsize'] = 1
        self.context.initialize() # queues the first task
        runQueuedTasks(maxRetries=0)
        self.assertEqual(10 + self.EXTRA_COUNT, _FantasmFanIn.all().count())
        self.assertEqual(self.EXTRA_VALUES + self.EXPECTED_VALUES, 
                         sorted(ContinuationFanInResult.get_by_key_name('test').values))
            
    def test_batchsize_3(self):
        self.context['batchsize'] = 3
        self.context.initialize() # queues the first task
        runQueuedTasks(maxRetries=0)
        self.assertEqual(4, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
            
    def test_batchsize_10(self):
        self.context['batchsize'] = 10
        self.context.initialize() # queues the first task
        runQueuedTasks(maxRetries=0)
        self.assertEqual(1 + self.EXTRA_COUNT, _FantasmFanIn.all().count())
        self.assertEqual(self.EXTRA_VALUES + self.EXPECTED_VALUES, 
                         sorted(ContinuationFanInResult.get_by_key_name('test').values))
            
    def test_batchsize_11(self):
        self.context['batchsize'] = 11
        self.context.initialize() # queues the first task
        runQueuedTasks(maxRetries=0)
        self.assertEqual(1, _FantasmFanIn.all().count())
        self.assertEqual(self.EXPECTED_VALUES, sorted(ContinuationFanInResult.get_by_key_name('test').values))
        
class OutsideFanTest( OutsideTest ):
    EVENT = 'fan'
        
class OutsideListTest( OutsideTest ):
    MACHINE_NAME = 'OutsideListFanInMachine'
    EXTRA_COUNT = 0
    EXTRA_VALUES = []
    
class OutsideFanListTest( OutsideListTest ):
    EVENT = 'fan'