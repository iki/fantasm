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

import time
import logging
from google.appengine.ext import deferred, webapp, db
from google.appengine.api.capabilities import CapabilitySet
from fantasm import config, constants
from fantasm.fsm import FSM
from fantasm.constants import NON_CONTEXT_PARAMS, STATE_PARAM, EVENT_PARAM, INSTANCE_NAME_PARAM, TASK_NAME_PARAM, \
                              RETRY_COUNT_PARAM, STARTED_AT_PARAM, URL_SEPARATOR
from fantasm.exceptions import UnknownMachineError, RequiredServicesUnavailableRuntimeError
from fantasm.models import _FantasmTaskSemaphore

REQUIRED_SERVICES = ('memcache', 'datastore_v3', 'taskqueue')

class TemporaryStateObject(dict):
    """ A simple object that is passed throughout a machine dispatch that can hold temporary
        in-flight data.
    """
    pass

def getMachineConfig(request):
    """ Returns the machine configuration specified by a URI in a HttpReuest
    
    @param request: an HttpRequest
    @return: a config._machineConfig instance
    """ 
    
    # parse out the machine-name from the path {mount-point}/fsm/{machine-name}/f/startState/event/endState/
    # NOTE: /f/startState/event/endState/ is optional
    path = request.path
    parts = [part for part in path.split('/') if part]
    if len(parts) >= 5 and parts[-4] == URL_SEPARATOR:
        machineName = parts[-5]
    else:
        machineName = parts[-1]
    
    # load the configuration, lookup the machine-specific configuration
    # FIXME: sort out a module level cache for the configuration - it must be sensitive to YAML file changes
    # for developer-time experience
    currentConfig = config.currentConfiguration()
    try:
        machineConfig = currentConfig.machines[machineName]
        return machineConfig
    except KeyError:
        raise UnknownMachineError(machineName)

class FSMLogHandler(webapp.RequestHandler):
    """ The handler used for logging """
    def post(self):
        """ Runs the serialized function """
        deferred.run(self.request.body)

class FSMGraphvizHandler(webapp.RequestHandler):
    """ The hander to output graphviz diagram of the finite state machine. """
    def get(self):
        """ Handles the GET request. """
        from fantasm.utils import outputMachineConfig
        machineConfig = getMachineConfig(self.request)
        content = outputMachineConfig(machineConfig)
        if self.request.GET.get('type', 'png') == 'png':
            self.response.out.write(
"""
<html>
<head></head>
<body onload="javascript:document.forms.chartform.submit();">
<form id='chartform' action='http://chart.apis.google.com/chart' method='POST'>
  <input type="hidden" name="cht" value="gv:dot"  />
  <input type="hidden" name="chl" value='%(chl)s'  />
  <input type="submit" value="Generate GraphViz .png" />
</form>
</body>
""" % {'chl': content.replace('\n', ' ')})
        else:
            self.response.out.write(content)
            

class FSMHandler(webapp.RequestHandler):
    """ The main worker handler, used to process queued machine events. """

    def get(self):
        """ Handles the GET request. """
        self.get_or_post(method='GET')
        
    def post(self):
        """ Handles the POST request. """
        self.get_or_post(method='POST')
        
    def get_or_post(self, method='POST'):
        """ Handles the GET/POST request. """
        
        # ensure that we have our services for the next 30s (length of a single request)
        unavailable = set()
        for service in REQUIRED_SERVICES:
            if not CapabilitySet(service).will_remain_enabled_for(constants.REQUEST_LENGTH):
                unavailable.add(service)
        if unavailable:
            raise RequiredServicesUnavailableRuntimeError(unavailable)
        
        # the case of headers is inconsistent on dev_appserver and appengine
        # ie 'X-AppEngine-TaskRetryCount' vs. 'X-AppEngine-Taskretrycount'
        lowerCaseHeaders = dict([(key.lower(), value) for key, value in self.request.headers.items()])

        taskName = lowerCaseHeaders.get('x-appengine-taskname')
        retryCount = int(lowerCaseHeaders.get('x-appengine-taskretrycount', 0))
        
        # Taskqueue can invoke multiple tasks of the same name occassionally. Here, we'll use
        # a datastore transaction as a semaphore to determine if we should actually execute this or not.
        if taskName:
            firstExecution = db.run_in_transaction(self.__isFirstExecution, taskName, retryCount)
            if not firstExecution:
                # we can simply return here, this is a duplicate fired task
                logging.info('A duplicate task "%s" has been queued by taskqueue infrastructure. Ignoring.', taskName)
                self.response.status_code = 200
                return

        requestData = {'POST': self.request.POST, 'GET': self.request.GET}[method]
        method = requestData.get('method') or method
        
        currentConfig = config.currentConfiguration()
        machineConfig = getMachineConfig(self.request)
        
        # get the incoming instance name, if any
        instanceName = requestData.get(INSTANCE_NAME_PARAM)
        
        # get the incoming state, if any
        fsmState = requestData.get(STATE_PARAM)
        
        # get the incoming event, if any
        fsmEvent = requestData.get(EVENT_PARAM)
        
        assert (fsmState and instanceName) or True # if we have a state, we should have an instanceName
        assert (fsmState and fsmEvent) or True # if we have a state, we should have an event
        
        # make a copy, add the data
        # FIXME: sort out a module level cache for the FSM instance so that we actually
        #        have singletons
        fsm = FSM(currentConfig=currentConfig).createFSMInstance(machineConfig.name, 
                                                                 currentStateName=fsmState, 
                                                                 instanceName=instanceName,
                                                                 method=method)
        
        # pull all the data off the url and stuff into the context
        for key, value in requestData.items():
            if key in NON_CONTEXT_PARAMS:
                continue # these are special, don't put them in the data
            
            # deal with ...a=1&a=2&a=3...
            value = requestData.get(key)
            valueList = requestData.getall(key)
            if len(valueList) > 1:
                value = valueList
                
            if key.endswith('[]'):
                key = key[:-2]
                value = [value]
                
            if key in fsm.contextTypes.keys():
                fsm.putTypedValue(key, value)
            else:
                fsm[key] = value
        
        if not (fsmState or fsmEvent):
            
            # just queue up a task to run the initial state transition using retries
            fsm[STARTED_AT_PARAM] = time.time()
            fsm.initialize()
            
        else:
            
            obj = TemporaryStateObject()
            
            # add the retry counter into the machine context from the header
            obj[RETRY_COUNT_PARAM] = retryCount
            
            # add the actual task name to the context
            obj[TASK_NAME_PARAM] = taskName
            
            # dispatch
            fsm.dispatch(fsmEvent, obj)

    def __isFirstExecution(self, taskName, retryCount):
        """ Ensures that the task has not been executed before. Meant to be run in a transaction. """
        firstExecution = False
        keyName = '%s--%s' % (taskName, retryCount)
        existingTaskName = _FantasmTaskSemaphore.get_by_key_name(keyName)
        if not existingTaskName:
            _FantasmTaskSemaphore(key_name=keyName).put()
            firstExecution = True
        return firstExecution
