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

def _log(instanceName, level, namespace, tags, message, stack, time, *args, **kwargs): # pylint: disable-msg=W0613
    """ Creates a _FantasmLog that can be used for debugging 
    
    @param instanceName:
    @param level:
    @param namespace: 
    @param tags: 
    @param message:
    @param time:
    @param args:
    @param kwargs:
    """
    _FantasmLog(instanceName=instanceName, 
                        level=level,
                        namespace=namespace,
                        tags=tags or [],
                        message=message % args, 
                        stack=stack,
                        time=time).put()

class Logger( object ):
    """ A object that allows an FSMContext to have methods debug, info etc. similar to logging.debug/info etc. """
    
    def __init__(self, context):
        """ Constructor """
        self.context = context
        self.level = logging.DEBUG
        self.maxLevel = logging.CRITICAL
    
    def _log(self, level, namespace, tags, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        
        @param level:
        @param message:
        @param args:
        @param kwargs:   
        
        NOTE: we are not not using deferred module to reduce dependencies, but we are re-using the helper
              functions .serialize() and .run() - see handler.py
        """
        
        stack = None
        if 'exc_info' in kwargs:
            f = StringIO.StringIO()
            traceback.print_exc(25, f)
            stack = f.getvalue()
        serialized = deferred.serialize(_log,
                                        self.context.instanceName,
                                        level,
                                        namespace,
                                        tags,
                                        message,
                                        stack,
                                        datetime.datetime.now(),
                                        *args,
                                        **kwargs)
        
        try:
            task = taskqueue.Task(url=LOG_URL, payload=serialized)
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
        if not (self.level <= logging.DEBUG <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None) # this is not expected in the logging.debug kwargs
        tags = kwargs.pop('tags', None) # this is not expected in the logging.debug kwargs
        logging.debug(message, *args, **kwargs)
        self._log(logging.DEBUG, namespace, tags, message, *args, **kwargs)
        
    def info(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.INFO
        
        @param message:
        @param args:
        @param kwargs:   
        """
        if not (self.level <= logging.INFO <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        logging.info(message, *args, **kwargs)
        self._log(logging.INFO, namespace, tags, message, *args, **kwargs)
        
    def warning(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.WARNING
        
        @param message:
        @param args:
        @param kwargs:   
        """
        if not (self.level <= logging.WARNING <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        logging.warning(message, *args, **kwargs)
        self._log(logging.WARNING, namespace, tags, message, *args, **kwargs)
        
    def error(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.ERROR
        
        @param message:
        @param args:
        @param kwargs:   
        """
        if not (self.level <= logging.ERROR <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        logging.error(message, *args, **kwargs)
        self._log(logging.ERROR, namespace, tags, message, *args, **kwargs)
        
    def critical(self, message, *args, **kwargs):
        """ Logs the message to the normal logging module and also queues a Task to create an _FantasmLog
        at level logging.CRITICAL
        
        @param message:
        @param args:
        @param kwargs:   
        """
        if not (self.level <= logging.CRITICAL <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        logging.critical(message, *args, **kwargs)
        self._log(logging.CRITICAL, namespace, tags, message, *args, **kwargs)
        
    def exception(self, message, *args, **kwargs):
        """ Logs the message + stack dump to the normal logging module and also queues a Task to create an 
        _FantasmLog at level logging.ERROR
        
        @param message:
        @param args:
        @param kwargs:   
        """
        if not (self.level <= logging.ERROR <= self.maxLevel):
            return
        namespace = kwargs.pop('namespace', None)
        tags = kwargs.pop('tags', None)
        logging.exception(message, *args, **kwargs)
        self._log(logging.ERROR, namespace, tags, message, *args, **{'exc_info': True})
        