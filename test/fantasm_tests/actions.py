""" FSMActions used in unit tests """

import logging
from fantasm.action import DatastoreContinuationFSMAction, ContinuationFSMAction
from google.appengine.ext import db
from fantasm.constants import FORK_PARAM

# pylint: disable-msg=C0111, W0613
# - docstrings not reqd in unit tests
# - these actions do not use arguments

class ContextRecorder(object):
    CONTEXTS = []
    def execute(self, context, obj):
        self.CONTEXTS.append(context)

class CountExecuteCalls(object):
    def __init__(self):
        self.count = 0
        self.fails = 0
    def execute(self, context, obj):
        self.count += 1
        if self.fails:
            self.fails -= 1
            raise Exception()
        return self.event
    @property
    def event(self):
        return 'next-event'
        
class CountExecuteCallsWithSpawn(CountExecuteCalls):
    def execute(self, context, obj):
        context.spawn('MachineToSpawn', [{'a': '1'}, {'b': '2'}])
        super(CountExecuteCallsWithSpawn, self).execute(context, obj)
        return None
        
class CountExecuteCallsWithFork(object):
    def __init__(self):
        self.count = 0
        self.fails = 0
        self.numFork = 0
    def execute(self, context, obj):
        self.count += 1
        if self.fails:
            self.fails -= 1
            raise Exception()
        context.fork()
        context.fork()
        return self.event
    @property
    def event(self):
        return 'next-event'
    
class CountExecuteAndContinuationCalls(object):
    def __init__(self):
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def continuation(self, context, obj, token=None):
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        return None
    def execute(self, context, obj):
        self.count += 1
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        return self.event
    @property
    def event(self):
        return 'next-event'
    
class CountExecuteCallsFanInEntry(object):
    def __init__(self):
        self.count = 0
        self.fcount = 0
        self.fails = 0
    def execute(self, context, obj):
        if not isinstance(context, list):
            context = [context]
        self.count += 1
        self.fcount += len(context)
        if self.fails:
            self.fails -= 1
            raise Exception()
        return self.event
    @property
    def event(self):
        return None
    
class CountExecuteCallsFanIn(CountExecuteCallsFanInEntry):
    CONTEXTS = []
    def execute(self, context, obj):
        CountExecuteCallsFanIn.CONTEXTS.extend(context)
        return super(CountExecuteCallsFanIn, self).execute(context, obj)
    @property
    def event(self):
        return 'next-event'
    
class CountExecuteCallsFanInFinal(CountExecuteCallsFanIn):
    @property
    def event(self):
        return None

class CountExecuteCallsFinal(CountExecuteCalls):
    @property
    def event(self):
        return None
    
class CountExecuteCallsSelfTransition(object):
    def __init__(self):
        self.count = 0
        self.fails = 0
    def execute(self, context, obj):
        self.count += 1
        if self.fails:
            self.fails -= 1
            raise Exception()
        if self.count <= 5:
            return 'next-event1'
        else:
            return 'next-event2'
    
class RaiseExceptionAction(object):
    def execute(self, context, obj):
        raise Exception('instrumented exception')
    
class RaiseExceptionContinuationAction(object):
    def continuation(self, context, obj, token=None):
        return "token"
    def execute(self, context, obj):
        raise Exception('instrumented exception')
        
class TestDatastoreContinuationFSMAction(DatastoreContinuationFSMAction):
    def __init__(self):
        super(TestDatastoreContinuationFSMAction, self).__init__()
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def getQuery(self, context, obj):
        return db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
    def getBatchSize(self, context, obj):
        return 2
    def continuation(self, context, obj, token=None):
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        return super(TestDatastoreContinuationFSMAction, self).continuation(context, obj, token=token)
    def execute(self, context, obj):
        if not obj.results:
            return None
        self.count += 1
        context['__count__'] = self.count
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        return 'next-event'
    
class HappySadContinuationFSMAction(TestDatastoreContinuationFSMAction):
    def execute(self, context, obj):
        if not obj.results:
            return None
        self.count += 1
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        if self.count % 2:
            return 'happy'
        else:
            return 'sad'
        
class TestFileContinuationFSMAction(ContinuationFSMAction):
    CONTEXTS = []
    ENTRIES = ['a', 'b', 'c', 'd']
    def __init__(self):
        super(TestFileContinuationFSMAction, self).__init__()
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def continuation(self, context, obj, token=None):
        token = int(token or 0) # awkward
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        obj.results = [TestFileContinuationFSMAction.ENTRIES[token]]
        nextToken = token + 1
        if nextToken >= len(TestFileContinuationFSMAction.ENTRIES):
            return None
        return nextToken
    def execute(self, context, obj):
        self.count += 1
        context['result'] = obj.results[0]
        TestFileContinuationFSMAction.CONTEXTS.append(context)
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        return 'next-event'
    
class TestContinuationAndForkFSMAction(DatastoreContinuationFSMAction):
    def __init__(self):
        super(TestContinuationAndForkFSMAction, self).__init__()
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def getQuery(self, context, obj):
        return db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
    def getBatchSize(self, context, obj):
        return 2
    def continuation(self, context, obj, token=None):
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        return super(TestContinuationAndForkFSMAction, self).continuation(context, obj, token=token)
    def execute(self, context, obj):
        if not obj.results:
            return None
        self.count += 1
        context['__count__'] = self.count
        context['data'] = {'a': 'b'}
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        
        # FIXME: this pattern is a bit awkward, or is it?
        # FIXME: how can we drive this into yaml?
        # FIXME: maybe just another provided base class like DatastoreContinuationFSMAction?
        
        # fork a machine to deal with all but one of the continuation dataset
        for result in obj.results[1:]:
            context.fork(data={'key': result.key()})
            
        # and deal with the leftover data item
        context['key'] = obj.result.key()
        context[FORK_PARAM] = -1
        
        # this event will be dispatched to this machine an all the forked contexts
        return 'next-event'

class DoubleContinuation1(ContinuationFSMAction):
    CONTEXTS = []
    ENTRIES = ['1', '2', '3']
    def __init__(self):
        super(DoubleContinuation1, self).__init__()
        self.count = 0
        self.ccount = 0
    def continuation(self, context, obj, token=None):
        token = int(token or 0) # awkward
        self.ccount += 1
        obj.results = [DoubleContinuation1.ENTRIES[token]]
        nextToken = token + 1
        if nextToken >= len(DoubleContinuation1.ENTRIES):
            return None
        return nextToken
    def execute(self, context, obj):
        self.count += 1
        context['c1'] = obj.results[0]
        #logging.critical('%s' % (context['c1']))
        DoubleContinuation1.CONTEXTS.append(context)
        return 'ok'

class DoubleContinuation2(object):
    CONTEXTS = []
    ENTRIES = ['a', 'b', 'c']
    def __init__(self):
        super(DoubleContinuation2, self).__init__()
        self.count = 0
        self.ccount = 0
    def continuation(self, context, obj, token=None):
        token = int(token or 0) # awkward
        self.ccount += 1
        obj.results = [DoubleContinuation2.ENTRIES[token]]
        nextToken = token + 1
        if nextToken >= len(DoubleContinuation2.ENTRIES):
            return None
        return nextToken
    def execute(self, context, obj):
        self.count += 1
        context['c2'] = obj.results[0]
        #logging.critical('%s-%s' % (context['c1'], context['c2']))
        DoubleContinuation2.CONTEXTS.append(context)
        return 'okfinal'

class FSCEE_InitialState(object):
    def __init__(self):
        self.count = 0
    def execute(self, context, obj):
        self.count += 1
        return 'ok'
        
class FSCEE_OptionalFinalState(object):
    def __init__(self):
        self.count = 0
    def execute(self, context, obj):
        self.count += 1
        # a final state should be able to emit an event
        return 'ok'
        
class FSCEE_FinalState(object):
    def __init__(self):
        self.count = 0
    def execute(self, context, obj):
        self.count += 1
