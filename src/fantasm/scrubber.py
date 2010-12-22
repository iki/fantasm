""" Code for the Fantasm Scrubber. """

import time
import datetime
from google.appengine.ext import db
from fantasm.action import DatastoreContinuationFSMAction
from fantasm.models import _FantasmInstance, _FantasmLog, _FantasmTaskSemaphore, _FantasmFanIn
from fantasm.fsm import startStateMachine

# W0613: Unused argument 'obj'
# implementing interfaces
# pylint: disable-msg=W0613

class InitalizeScrubber(object):
    """ Use current time to set up task names. """
    def execute(self, context, obj):
        """ Set the current time on the context. """
        age = context.pop('age', 90)
        context['before'] = datetime.datetime.utcnow() - datetime.timedelta(days=age)
        context['startTime'] = int(time.time())
        return 'next'
        
class StartScrubberMachines(object):
    """ Kick off the parallel scrubbers. """
    def execute(self, context, obj):
        """ Start the other scrubber machines. """
        startTime = context['startTime']
        before = context['before']
        
        spawnContext = { 'before': before }
        taskName = 'fantasm-scrubber-FantasmInstance-%s' % startTime
        startStateMachine('fantasm-scrubber-FantasmInstance', spawnContext, taskName=taskName)
        
        taskName = 'fantasm-scrubber-FantasmLog-%s' % startTime
        startStateMachine('fantasm-scrubber-FantasmLog', spawnContext, taskName=taskName)
        
        taskName = 'fantasm-scrubber-FantasmTaskSemaphore-%s' % startTime
        startStateMachine('fantasm-scrubber-FantasmTaskSemaphore', spawnContext, taskName=taskName)
        
        taskName = 'fantasm-scrubber-FantasmFanIn-%s' % startTime
        startStateMachine('fantasm-scrubber-FantasmFanIn', spawnContext, taskName=taskName)
     
# W0223: Method 'getQuery' is abstract in class 'DatastoreContinuationFSMAction' but is not overridden
# this too is an abstract
class ScrubberContinuation(DatastoreContinuationFSMAction): # pylint: disable-msg=W0223
    """ Parent class for scrubbers. """
    def getBatchSize(self, context, obj):
        """ Batch size. """
        return 500
    def execute(self, context, obj):
        """ Delete the rows. """
        if obj['results']:
            db.delete(obj['results'])
                    
class Scrub_FantasmInstance(ScrubberContinuation):
    """ Iterate over old _FantasmInstance entities. """
    def getQuery(self, context, obj):
        """ Return query. """
        before = context['before']
        return _FantasmInstance.all(keys_only=True).filter('createdTime <', before)

class Scrub_FantasmLog(ScrubberContinuation):
    """ Iterate over old _FantasmLog entities. """
    def getQuery(self, context, obj):
        """ Return query. """
        before = context['before']
        return _FantasmLog.all(keys_only=True).filter('time <', before)

class Scrub_FantasmTaskSemaphore(ScrubberContinuation):
    """ Iterate over old _FantasmTaskSemaphore entities. """
    def getQuery(self, context, obj):
        """ Return query. """
        before = context['before']
        return _FantasmTaskSemaphore.all(keys_only=True).filter('createdTime <', before)

class Scrub_FantasmFanIn(ScrubberContinuation):
    """ Iterate over old _FantasmFanIn entities. """
    def getQuery(self, context, obj):
        """ Return query. """
        before = context['before']
        return _FantasmFanIn.all(keys_only=True).filter('createdTime <', before)
    