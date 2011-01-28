""" Fantasm: A taskqueue-based Finite State Machine for App Engine Python

Docs and examples: http://code.google.com/p/fantasm/

Copyright 2010 VendAsta Technologies Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import logging
import datetime
import traceback
import StringIO
from google.appengine.ext import deferred
from fantasm.models import _FantasmLog
from google.appengine.api.taskqueue import taskqueue

LOG_URL = '/fantasm/log/'
LOG_ERROR_MESSAGE = 'Exception constructing log message. Please adjust your usage of context.logger.'

def _log(instanceName, 
         machineName, stateName, actionName, transitionName,
         level, namespace, tags, message, stack, time, 
         *args, **kwargs): # pylint: disable-msg=W0613
    """ Creates a _FantasmLog that can be used for debugging 
    
    @param instanceName:
    @param machineName:
    @param stateName:
    @param actionName:
    @param transitionName: 
    @param level:
    @param namespace: 
    @param tags: 
    @param message:
    @param time:
    @param args:
    @param kwargs:
    """
    _FantasmLog(instanceName=instanceName, 
                machineName=machineName,
                stateName=stateName,
                actionName=actionName,
                transitionName=transitionName,
                level=level,
                namespace=namespace,
                tags=list(set(tags)) or [],
                message=message % args, 
                stack=stack,
                time=time).put()

class Logger( object ):
    """ A object that allows an FSMContext to have methods debug, info etc. similar to logging.debug/info etc. """
    
    _LOGGING_MAP = {
        logging.CRITICAL: logging.critical,
        logging.ERROR: logging.error,
        logging.WARNING: logging.warning,
        logging.INFO: logging.info,
        logging.DEBUG: logging.debug
    }
    
    def __init__(self, context, persistentLogging=False):
        """ Constructor """
        self.context = context
        self.level = logging.DEBUG
        self.maxLevel = logging.CRITICAL
        self.tags = []
        self.persistentLogging = persistentLogging
        
    def getLoggingMap(self):
        """ One layer of indirection to fetch self._LOGGING_MAP (required for minimock to work) """
        return self._LOGGING_MAP
    
    def _log(self, level, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        
        @param level:
        @param message:
        @param args:
        @param kwargs:   
        
        NOTE: we are not not using deferred module to reduce dependencies, but we are re-using the helper
              functions .serialize() and .run() - see handler.py
        """
        if not (self.level <= level <= self.maxLevel):
            return
        
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        
        self.getLoggingMap()[level](message, *args, **kwargs)
        
        if not self.persistentLogging:
            return
        
        stack = None
        if 'exc_info' in kwargs:
            f = StringIO.StringIO()
            traceback.print_exc(25, f)
            stack = f.getvalue()
            
        # this _log method requires everything to be serializable, which is not the case for the logging
        # module. if message is not a basestring, then we simply cast it to a string to allow _something_
        # to be logged in the deferred task
        if not isinstance(message, basestring):
            try:
                message = str(message)
            except Exception:
                message = LOG_ERROR_MESSAGE
                if args:
                    args = []
                logging.warning(message, exc_info=True)
                
        stateName = None
        if self.context.currentState:
            stateName = self.context.currentState.name
            
        transitionName = None
        if self.context.startingState and self.context.startingEvent:
            transitionName = self.context.startingState.getTransition(self.context.startingEvent).name
            
        actionName = None
        if self.context.currentAction:
            actionName = self.context.currentAction.__class__.__name__
                
        serialized = deferred.serialize(_log,
                                        self.context.instanceName,
                                        self.context.machineName,
                                        stateName,
                                        actionName,
                                        transitionName,
                                        level,
                                        namespace,
                                        (self.tags or []) + (tags or []),
                                        message,
                                        stack,
                                        datetime.datetime.now(), # FIXME: called .utcnow() instead?
                                        *args,
                                        **kwargs)
        
        try:
            task = taskqueue.Task(url=LOG_URL, payload=serialized, 
                                  retry_options=taskqueue.TaskRetryOptions(task_retry_limit=20))
            # FIXME: a batch add may be more optimal, but there are quite a few more corners to deal with
            taskqueue.Queue().add(task)
            
        except taskqueue.TaskTooLargeError:
            logging.warning("fantasm log message too large - skipping persistent storage")
        
    def setLevel(self, level):
        """ Sets the minimum logging level to log 
        
        @param level: a log level (ie. logging.CRITICAL)
        """
        self.level = level
        
    def setMaxLevel(self, maxLevel):
        """ Sets the maximum logging level to log 
        
        @param maxLevel: a max log level (ie. logging.CRITICAL)
        """
        self.maxLevel = maxLevel
        
    def debug(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.DEBUG
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.DEBUG, message, *args, **kwargs)
        
    def info(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.INFO
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.INFO, message, *args, **kwargs)
        
    def warning(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.WARNING
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.WARNING, message, *args, **kwargs)
        
    warn = warning
        
    def error(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.ERROR
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.ERROR, message, *args, **kwargs)
        
    def critical(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.CRITICAL
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.CRITICAL, message, *args, **kwargs)
        
    # pylint: disable-msg=W0613
    # - kwargs is overridden in this case, and never used
    def exception(self, message, *args, **kwargs):
        """ Logs the message + stack dump to the normal logging module and also queues a Task to create an 
        _FantasmLog at level logging.ERROR
        
        @param message:
        @param args:
        @param kwargs:   
        """
        self._log(logging.ERROR, message, *args, **{'exc_info': True})
        