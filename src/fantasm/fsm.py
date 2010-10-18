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



The FSM implementation is inspired by the paper:

[1] J. van Gurp, J. Bosch, "On the Implementation of Finite State Machines", in Proceedings of the 3rd Annual IASTED
    International Conference Software Engineering and Applications,IASTED/Acta Press, Anaheim, CA, pp. 172-178, 1999.
    (www.jillesvangurp.com/static/fsm-sea99.pdf)

The Fan-out / Fan-in implementation is modeled after the presentation:
    
[2] B. Slatkin, "Building high-throughput data pipelines with Google App Engine", Google IO 2010.
    http://code.google.com/events/io/2010/sessions/high-throughput-data-pipelines-appengine.html
"""

import logging
import datetime
import random
import copy
import time
import types
import uuid
from django.utils import simplejson
from google.appengine.api.labs.taskqueue.taskqueue import Task, Queue, TaskAlreadyExistsError, TombstonedTaskError
from google.appengine.ext import db
from google.appengine.api import memcache
from fantasm import constants, config
from fantasm.log import Logger
from fantasm.state import State
from fantasm.transition import Transition
from fantasm.exceptions import UnknownEventError, UnknownStateError, UnknownMachineError, \
                               FanInWriteLockFailureRuntimeError, FanInReadLockFailureRuntimeError
from fantasm.models import _FantasmFanIn, _FantasmInstance
from fantasm import models
from fantasm.utils import knuthHash

class FSM(object):
    """ An FSMContext creation factory. This is primarily responsible for translating machine
    configuration information (config.currentConfiguration()) into singleton States and Transitions as per [1]
    """
    
    PSEUDO_INIT = 'pseudo-init'
    PSEUDO_FINAL = 'pseudo-final'
    
    _CURRENT_CONFIG = None
    _MACHINES = None
    _PSEUDO_INITS = None
    _PSEUDO_FINALS = None
    
    def __init__(self, currentConfig=None):
        """ Constructor which either initializes the module/class-level cache, or simply uses it 
        
        @param currentConfig: a config._Configuration instance (dependency injection). if None, 
            then the factory uses config.currentConfiguration()
        """
        currentConfig = currentConfig or config.currentConfiguration()
        
        # if the FSM is not using the currentConfig (.yaml was edited etc.)
        if not (FSM._CURRENT_CONFIG is currentConfig):
            self._init(currentConfig=currentConfig)
            FSM._CURRENT_CONFIG = self.config
            FSM._MACHINES = self.machines
            FSM._PSEUDO_INITS = self.pseudoInits
            FSM._PSEUDO_FINALS = self.pseudoFinals
            
        # otherwise simply use the cached currentConfig etc.
        else:
            self.config = FSM._CURRENT_CONFIG
            self.machines = FSM._MACHINES
            self.pseudoInits = FSM._PSEUDO_INITS
            self.pseudoFinals = FSM._PSEUDO_FINALS
    
    def _init(self, currentConfig=None):
        """ Constructs a group of singleton States and Transitions from the machineConfig 
        
        @param currentConfig: a config._Configuration instance (dependency injection). if None, 
            then the factory uses config.currentConfiguration()
        """
        logging.info("Initializing FSM factory.")
        
        self.config = currentConfig or config.currentConfiguration()
        self.machines = {}
        self.pseudoInits, self.pseudoFinals = {}, {}
        for machineConfig in self.config.machines.values():
            self.machines[machineConfig.name] = {constants.MACHINE_STATES_ATTRIBUTE: {}, 
                                                 constants.MACHINE_TRANSITIONS_ATTRIBUTE: {}}
            machine = self.machines[machineConfig.name]
            
            # create a pseudo-init state for each machine that transitions to the initialState
            pseudoInit = State(FSM.PSEUDO_INIT, None, None, None)
            self.pseudoInits[machineConfig.name] = pseudoInit
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_INIT] = pseudoInit
            
            # create a pseudo-final state for each machine that transitions from the finalState(s)
            pseudoFinal = State(FSM.PSEUDO_FINAL, None, None, None, isFinalState=True)
            self.pseudoFinals[machineConfig.name] = pseudoFinal
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_FINAL] = pseudoFinal
            
            for stateConfig in machineConfig.states.values():
                state = self._getState(machineConfig, stateConfig)
                
                # add the transition from pseudo-init to initialState
                if state.isInitialState:
                    transition = Transition(FSM.PSEUDO_INIT, state)
                    transition.maxRetries = machineConfig.maxRetries
                    self.pseudoInits[machineConfig.name].addTransition(transition, FSM.PSEUDO_INIT)
                    
                # add the transition from finalState to pseudo-final
                if state.isFinalState:
                    transition = Transition(FSM.PSEUDO_FINAL, pseudoFinal)
                    transition.maxRetries = machineConfig.maxRetries
                    state.addTransition(transition, FSM.PSEUDO_FINAL)
                    
                machine[constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name] = state
                
            for transitionConfig in machineConfig.transitions.values():
                source = machine[constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.fromState.name]
                transition = self._getTransition(machineConfig, transitionConfig)
                machine[constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name] = transition
                event = transitionConfig.event
                source.addTransition(transition, event)
                
    def _getState(self, machineConfig, stateConfig):
        """ Returns a State instance based on the machineConfig/stateConfig 
        
        @param machineConfig: a config._MachineConfig instance
        @param stateConfig: a config._StateConfig instance  
        @return: a State instance which is a singleton wrt. the FSM instance
        """
        
        if machineConfig.name in self.machines and \
           stateConfig.name in self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name]
        
        name = stateConfig.name
        entryAction = stateConfig.entry
        doAction = stateConfig.action
        exitAction = stateConfig.exit
        isInitialState = stateConfig.initial
        isFinalState = stateConfig.final
        isContinuation = stateConfig.continuation
        fanInPeriod = stateConfig.fanInPeriod
        
        return State(name, 
                     entryAction, 
                     doAction, 
                     exitAction, 
                     machineName=machineConfig.name,
                     isInitialState=isInitialState,
                     isFinalState=isFinalState,
                     isContinuation=isContinuation,
                     fanInPeriod=fanInPeriod)
            
    def _getTransition(self, machineConfig, transitionConfig):
        """ Returns a Transition instance based on the machineConfig/transitionConfig 
        
        @param machineConfig: a config._MachineConfig instance
        @param transitionConfig: a config._TransitionConfig instance  
        @return: a Transition instance which is a singleton wrt. the FSM instance
        """
        if machineConfig.name in self.machines and \
           transitionConfig.name in self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name]
        
        target = self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.toState.name]
        maxRetries = transitionConfig.maxRetries
        countdown = transitionConfig.countdown
        
        return Transition(transitionConfig.name, target, action=transitionConfig.action, maxRetries=maxRetries,
                          countdown=countdown)
        
    def createFSMInstance(self, machineName, currentStateName=None, instanceName=None, data=None, method='GET'):
        """ Creates an FSMContext instance with non-initialized data 
        
        @param machineName: the name of FSMContext to instantiate, as defined in fsm.yaml 
        @param currentStateName: the name of the state to place the FSMContext into
        @param instanceName: the name of the current instance
        @raise UnknownMachineError: if machineName is unknown
        @raise UnknownStateError: is currentState name is not None and unknown in machine with name machineName
        @return: an FSMContext instance
        """
        
        try:
            machineConfig = self.config.machines[machineName]
        except KeyError:
            raise UnknownMachineError(machineName)
        
        initialState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][machineConfig.initialState.name]
        
        try:
            currentState = self.pseudoInits[machineName]
            if currentStateName:
                currentState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][currentStateName]
        except KeyError:
            raise UnknownStateError(machineName, currentStateName)
                
        maxRetries = machineConfig.maxRetries
        url = machineConfig.url
        queueName = machineConfig.queueName
        
        return FSMContext(initialState, currentState=currentState, 
                          machineName=machineName, instanceName=instanceName,
                          maxRetries=maxRetries, url=url, queueName=queueName,
                          data=data, contextTypes=machineConfig.contextTypes,
                          method=method)

class FSMContext(dict):
    """ A finite state machine context instance. """
    
    def __init__(self, initialState, currentState=None, machineName=None, instanceName=None,
                 maxRetries=None, url=None, queueName=None, data=None, contextTypes=None,
                 method='GET'):
        """ Constructor
        
        @param initialState: a State instance 
        @param currentState: a State instance
        @param machineName: the name of the fsm
        @param instanceName: the instance name of the fsm
        @param maxRetries: the maximum number of times to retry running the fsm
        @param url: the url of the fsm  
        @param queueName: the name of the appengine task queue 
        """
        super(FSMContext, self).__init__(data or {})
        self.initialState = initialState
        self.currentState = currentState
        self.machineName = machineName
        self.instanceName = instanceName or self._generateUniqueInstanceName()
        self.queueName = queueName
        self.maxRetries = maxRetries
        self.url = url
        self.method = method
        self.startingEvent = None
        self.startingState = None
        self.contextTypes = constants.PARAM_TYPES.copy()
        if contextTypes:
            self.contextTypes.update(contextTypes)
        self.logger = Logger(self)
        self.__obj = None
        
    def _generateUniqueInstanceName(self):
        """ Generates a unique instance name for this machine. 
        
        @return: a FSMContext instanceName that is (pretty darn likely to be) unique
        """
        utcnow = datetime.datetime.utcnow()
        dateStr = utcnow.strftime('%Y%m%d%H%M%S')
        randomStr = ''.join(random.sample(constants.CHARS_FOR_RANDOM, 6))
        return '%s-%s-%s' % (self.machineName, dateStr, randomStr)
        
    def putTypedValue(self, key, value):
        """ Sets a value on context[key], but casts the value according to self.contextTypes. """

        # cast the value to the appropriate type TODO: should this be in FSMContext?
        cast = self.contextTypes[key]
        kwargs = {}
        if cast is simplejson.loads:
            kwargs = {'object_hook': models.decode}
        if isinstance(value, list):
            value = [cast(v, **kwargs) for v in value]
        else:
            value = cast(value, **kwargs)

        # update the context
        self[key] = value
        
    def generateInitializationTask(self, countdown=0, taskName=None):
        """ Generates a task for initializing the machine. """
        assert self.currentState.name == FSM.PSEUDO_INIT
        
        url = self.buildUrl(self.currentState.name, FSM.PSEUDO_INIT)
        params = self.buildParams(self.currentState.name, FSM.PSEUDO_INIT)
        taskName = taskName or self.getTaskName(FSM.PSEUDO_INIT)
        task = Task(name=taskName, method=self.method, url=url, params=params, countdown=countdown)
        return task
    
    def fork(self, data=None):
        """ Forks the FSMContext. 
        
        When an FSMContext is forked, an identical copy of the finite state machine is generated
        that will have the same event dispatched to it as the machine that called .fork(). The data
        parameter is useful for allowing each forked instance to operate on a different bit of data.
        
        @param data: an option mapping of data to apply to the forked FSMContext 
        """
        obj = self.__obj
        if obj.get(constants.FORKED_CONTEXTS_PARAM) is None:
            obj[constants.FORKED_CONTEXTS_PARAM] = []
        forkedContexts = obj.get(constants.FORKED_CONTEXTS_PARAM)
        data = copy.copy(data) or {}
        data[constants.FORK_PARAM] = len(forkedContexts)
        forkedContexts.append(self.clone(data=data))
    
    def spawn(self, machineName, contexts, countdown=0, method='POST', 
              _currentConfig=None):
        """ Spawns new machines.
        
        @param machineName the machine to spawn
        @param contexts a list of contexts (dictionaries) to spawn the new machine(s) with; multiple contexts will spawn
                        multiple machines
        @param countdown the countdown (in seconds) to wait before spawning machines
        @param method the method ('GET' or 'POST') to invoke the machine with (default: POST)
        
        @param _currentConfig test injection for configuration
        """
        # using the current task name as a root to startStateMachine will make this idempotent
        taskName = self.__obj[constants.TASK_NAME_PARAM]
        startStateMachine(machineName, contexts, taskName=taskName, method=method, countdown=countdown, 
                          _currentConfig=_currentConfig)
    
    def initialize(self):
        """ Initializes the FSMContext. Queues a Task (so that we can benefit from auto-retry) to dispatch
        an event and take the machine from 'pseudo-init' into the state machine's initial state, as 
        defined in the fsm.yaml file.
        
        @param data: a dict of initial key, value pairs to stuff into the FSMContext
        @return: an event string to dispatch to the FSMContext to put it into the initialState 
        """
        self[constants.STEPS_PARAM] = 0
        task = self.generateInitializationTask()
        Queue(name=self.queueName).add(task)
        _FantasmInstance(key_name=self.instanceName, instanceName=self.instanceName).put()
        
        return FSM.PSEUDO_INIT
        
    def dispatch(self, event, obj):
        """ The main entry point to move the machine according to an event. 
        
        @param event: a string event to dispatch to the FSMContext
        @param obj: an object that the FSMContext can operate on  
        @return: an event string to dispatch to the FSMContext
        """
        
        self.__obj = obj # hold the obj object for use during this context

        # store the starting state and event for the handleEvent() method
        self.startingState = self.currentState
        self.startingEvent = event

        nextEvent = None
        try:
            nextEvent = self.currentState.dispatch(self, event, obj)
            
            if obj.get(constants.FORKED_CONTEXTS_PARAM):
                # pylint: disable-msg=W0212
                # - accessing the protected method is fine here, since it is an instance of the same class
                tasks = []
                for context in obj[constants.FORKED_CONTEXTS_PARAM]:
                    context[constants.STEPS_PARAM] = int(context.get(constants.STEPS_PARAM, '0')) + 1
                    task = context.queueDispatch(nextEvent, queue=False)
                    if task: # fan-in magic
                        if not task.was_enqueued: # fan-in always queues
                            tasks.append(task)
                
                try:
                    if tasks:
                        Queue(name=self.queueName).add(tasks)
                
                except (TaskAlreadyExistsError, TombstonedTaskError):
                    # unlike a similar block in self.continutation, this should NOT happen
                    logging.critical('Unable to queue fork Tasks %s as it/they already exists. (Machine %s, State %s)',
                                     [task.name for task in tasks if not task.was_enqueued],
                                     self.machineName, 
                                     self.currentState.name)
                
            if nextEvent:
                self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1
                self.queueDispatch(nextEvent)
            else:
                # if we're not in a final state, emit a log message
                # FIXME - somehow we should avoid this message if we're in the "last" step of a continuation...
                if not self.currentState.isFinalState and not obj.get(constants.TERMINATED_PARAM):
                    logging.critical('Non-final state did not emit an event. Machine has terminated in an ' +
                                     'unknown state. (Machine %s, State %s)' %
                                     (self.machineName, self.currentState.name))
                # if it is a final state, then dispatch the pseudo-final event to finalize the state machine
                elif self.currentState.isFinalState and self.currentState.exitAction:
                    self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1
                    self.queueDispatch(FSM.PSEUDO_FINAL)
                    
        except Exception:
            self.logger.exception("FSMContext.dispatch is handling the following exception:")
            self._handleException(event, obj)
            
        return nextEvent
    
    def continuation(self, nextToken):
        """ Performs a continuation be re-queueing an FSMContext Task with a slightly modified continuation
        token. self.startingState and self.startingEvent are used in the re-queue, so this can be seen as a
        'fork' of the current context.
        
        @param nextToken: the next continuation token
        """
        assert not self.get(constants.INDEX_PARAM) # fan-out after fan-in is not allowed
        step = str(self[constants.STEPS_PARAM]) # needs to be a str key into a json dict
        
        # make a copy and set the currentState to the startingState of this context
        context = self.clone()
        context.currentState = self.startingState
        
        # update the generation and continuation params
        gen = context.get(constants.GEN_PARAM, {})
        gen[step] = gen.get(step, 0) + 1
        context[constants.GEN_PARAM] = gen
        context[constants.CONTINUATION_PARAM] = nextToken
        
        try:
            # pylint: disable-msg=W0212
            # - accessing the protected method is fine here, since it is an instance of the same class
            context._queueDispatchNormal(self.startingEvent, queue=True)
            
        except (TaskAlreadyExistsError, TombstonedTaskError):
            # this can happen when currentState.dispatch() previously succeeded in queueing the continuation
            # Task, but failed with the doAction.execute() call in a _previous_ execution of this Task.
            logging.info('Unable to queue continuation Task as it already exists. (Machine %s, State %s)',
                          self.machineName, 
                          self.currentState.name)
    
    def queueDispatch(self, nextEvent, queue=True):
        """ Queues a .dispatch(nextEvent) call in the appengine Task queue. 
        
        @param nextEvent: a string event 
        @param queue: a boolean indicating whether or not to queue a Task, or leave it to the caller 
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None
        
        # self.currentState is already transitioned away from self.startingState
        transition = self.currentState.getTransition(nextEvent)
        if transition.target.isFanIn:
            task = self._queueDispatchFanIn(nextEvent, fanInPeriod=transition.target.fanInPeriod)
        else:
            task = self._queueDispatchNormal(nextEvent, queue=queue, countdown=transition.countdown)
            
        return task
        
    def _queueDispatchNormal(self, nextEvent, queue=True, countdown=0):
        """ Queues a call to .dispatch(nextEvent) in the appengine Task queue. 
        
        @param nextEvent: a string event 
        @param queue: a boolean indicating whether or not to queue a Task, or leave it to the caller 
        @param countdown: the number of seconds to countdown before the queued task fires
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None
        
        url = self.buildUrl(self.currentState.name, nextEvent)
        params = self.buildParams(self.currentState.name, nextEvent)
        taskName = self.getTaskName(nextEvent)
        
        task = Task(name=taskName, method=self.method, url=url, params=params, countdown=countdown)
        if queue:
            Queue(name=self.queueName).add(task)
        
        return task
    
    def _queueDispatchFanIn(self, nextEvent, fanInPeriod=0):
        """ Queues a call to .dispatch(nextEvent) in the task queue, or saves the context to the 
        datastore for processing by the queued .dispatch(nextEvent)
        
        @param nextEvent: a string event 
        @param fanInPeriod: the period of time between fan in Tasks 
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None
        assert not self.get(constants.INDEX_PARAM) # fan-in after fan-in is not allowed
        
        # we pop this off here because we do not want the fan-out/continuation param as part of the
        # task name, otherwise we loose the fan-in - each fan-in gets one work unit.
        self.pop(constants.GEN_PARAM, None)
        self.pop(constants.FORK_PARAM, None)
        
        taskNameBase = self.getTaskName(nextEvent, fanIn=True)
        index = memcache.get('index-' + taskNameBase)
        if index is None:
            # using 'random.randint' here instead of '1' helps when the index is ejected from memcache
            # instead of restarting at the same counter, we jump (likely) far way from existing task job
            # names. 
            memcache.add('index-' + taskNameBase, random.randint(1, 2**32))
            index = memcache.get('index-' + taskNameBase)
            
        # grab the lock
        lock = '%s-lock-%d' % (taskNameBase, index)
        writers = memcache.incr(lock, initial_value=2**16)
        if writers < 2**16:
            memcache.decr(lock)
            # this will escape as a 500 error and the Task will be re-tried by appengine
            raise FanInWriteLockFailureRuntimeError(nextEvent, 
                                                    self.machineName, 
                                                    self.currentState.name, 
                                                    self.instanceName)
        
        # insert the work package, which is simply a serialized FSMContext
        workIndex = '%s-%d' % (taskNameBase, knuthHash(index))
        work = _FantasmFanIn(context=self, workIndex=workIndex)
        work.put()
        
        # insert a task to run in the future and process a bunch of work packages
        now = time.time()
        try:
            self[constants.INDEX_PARAM] = index
            url = self.buildUrl(self.currentState.name, nextEvent)
            params = self.buildParams(self.currentState.name, nextEvent)
            # int(now / (fanInPeriod - 1 + 30)) included because it was in [2], but is less needed now that
            # we use random.randint in seeding memcache. for long fan in periods, and the case where random.randint
            # hits the same value twice, this may cause problems for up to fanInPeriod + 30s.
            # see: http://www.mail-archive.com/google-appengine@googlegroups.com/msg30408.html
            task = Task(name='%s-%d-%d' % (taskNameBase, int(now / (fanInPeriod - 1 + 30)), index),
                        method=self.method,
                        url=url,
                        params=params,
                        eta=datetime.datetime.utcfromtimestamp(now) + datetime.timedelta(seconds=fanInPeriod))
            Queue(name=self.queueName).add(task)
            return task
        except TaskAlreadyExistsError:
            pass # Fan-in magic
        finally:
            memcache.decr(lock)
            
    def mergeJoinDispatch(self, event, obj):
        """ Performs a merge join on the pending fan-in dispatches.
        
        @param event: an event that is being merge joined (destination state must be a fan in) 
        @return: a list (possibly empty) of FSMContext instances
        """
        # this assertion comes from _queueDispatchFanIn - we never want fan-out info in a fan-in context
        assert not self.get(constants.GEN_PARAM)
        assert not self.get(constants.FORK_PARAM)
        
        # the work package index is stored in the url of the Task/FSMContext
        index = self.get(constants.INDEX_PARAM)
        taskNameBase = self.getTaskName(event, fanIn=True)
        
        # tell writers to use another index
        memcache.incr('index-' + taskNameBase)
        
        lock = '%s-lock-%d' % (taskNameBase, index)
        memcache.decr(lock, 2**15) # tell writers they missed the boat
        
        # 20 iterations * 0.25s = 5s total wait time
        busyWaitIters = 20
        busyWaitIterSecs = 0.250
        
        # busy wait for writers
        for i in xrange(busyWaitIters):
            counter = memcache.get(lock)
            # counter is None --> ejected from memcache
            # int(counter) <= 2**15 --> writers have all called memcache.decr
            if counter is None or int(counter) <= 2**15:
                break
            time.sleep(busyWaitIterSecs)
            logging.debug("Tried to acquire lock '%s' %d times...", lock, i + 1)
        
        # FIXME: is there anything else that can be done? will work packages be lost? maybe queue another task
        #        to sweep up later?
        if i >= (busyWaitIters - 1): # pylint: disable-msg=W0631
            logging.error("Gave up waiting for all fan-in work items.")
        
        # at this point we could have two tasks trying to process the same work packages. in the
        # happy path this will not likely happen because the tasks are sent off with different ETAs,
        # however in the unhappy path, it is possible for multiple tasks to be executing (retry on
        # 500 etc.). we solve this with a read lock using memcache.
        #
        # FIXME: would using a transaction on db.delete work if using ancestors? one task would win the
        #        race to delete the the work based on a transaction error?
        readlock = '%s-readlock-%d' % (taskNameBase, index)
        haveReadLock = False
        try:
            # put the actual name of the winning task into to lock
            actualTaskName = self.get(constants.TASK_NAME_PARAM)
            added = memcache.add(readlock, actualTaskName, time=30) # FIXME: is 30s appropriate?
            lockValue = memcache.get(readlock)
            
            # if the lock value is not equal to the added value, it means this task lost the race
            if not added or lockValue != actualTaskName:
                raise FanInReadLockFailureRuntimeError(event, 
                                                       self.machineName, 
                                                       self.currentState.name, 
                                                       self.instanceName)
            
            # flag used in finally block to decide whether or not to log an error message
            haveReadLock = True
                
            # fetch all the work packages in the current group for processing
            workIndex = '%s-%d' % (taskNameBase, knuthHash(index))
            query = _FantasmFanIn.all() \
                                 .filter('workIndex =', workIndex) \
                                 .order('__key__')
                                 
            # iterate over the query to fetch results - this is done in 'small batches'
            fanInResults = list(query)
            obj[constants.FAN_IN_RESULTS_PARAM] = fanInResults
            
            # construct a list of FSMContexts
            contexts = [self.clone(data=r.context) for r in fanInResults]
            
            # and delete the work packages - bearing in mind appengine limits
            maxDeleteSize = 250 # appengine does not like to delete > 500 models at a time, 250 is a nice safe number
            if len(fanInResults) > maxDeleteSize:
                logging.warning("%d contexts in the current batch. Consider decreasing fan-in.", len(fanInResults))
            i = 0
            while fanInResults[i:i+maxDeleteSize]:
                db.delete(fanInResults[i:i+maxDeleteSize])
                i += maxDeleteSize
                
            # and return the FSMContexts list
            class FSMContextList(list):
                """ A list that supports .logger.info(), .logger.warning() etc.for fan-in actions """
                def __init__(self, context, contexts):
                    """ setup a self.logger for fan-in actions """
                    super(FSMContextList, self).__init__(contexts)
                    self.logger = Logger(context)
                
            return FSMContextList(self, contexts)
        
        finally:
            deleted = memcache.delete(readlock)
            
            # FIXME: is there anything else that can be done? 
            if haveReadLock and deleted == memcache.DELETE_NETWORK_FAILURE:
                logging.error("Unable to release the fan in read lock.")
                
            
    def _handleException(self, event, obj):
        """ Method for child classes to override to handle exceptions. 
        
        @param event: 
        @param obj: 
        """
        retryCount = obj.get(constants.RETRY_COUNT_PARAM, 0)
        
        # get max_retries configuration
        try:
            transition = self.startingState.getTransition(self.startingEvent)
            maxRetries = transition.maxRetries
        except UnknownEventError:
            # can't find the transition, use the machine-level default
            maxRetries = self.maxRetries
            
        if retryCount >= maxRetries:
            # need to permanently fail
            logging.critical('Max-requeues reached. Machine has terminated in an unknown state. ' +
                             '(Machine %s, State %s, Event %s)',
                             self.machineName, self.startingState.name, event, exc_info=True)
            # eat the exception so that a 200-series response is given
        else:
            # re-raise the exception
            logging.warning('Exception occurred processing event. Task will be retried. ' +
                            '(Machine %s, State %s)',
                            self.machineName, self.startingState.name, exc_info=True)
            
            # re-put fan-in work packages
            if obj.get(constants.FAN_IN_RESULTS_PARAM):
                try:
                    fanInResults = obj[constants.FAN_IN_RESULTS_PARAM]
                    maxPutSize = 250 # put in chunks, rather than the entire list which could be large
                    i = 0
                    while(fanInResults[i:i+maxPutSize]):
                        db.put(fanInResults[i:i+maxPutSize])
                        i += maxPutSize
                except Exception:
                    logging.critical("Unable to re.put() for workIndex = %s", self.fanInResults[0].workIndex)
                    raise
                
            # this line really just allows unit tests to work - the request is really dead at this point
            self.currentState = self.startingState
            
            raise
    
    def buildUrl(self, stateName, eventName):
        """ Builds the taskqueue url. 
        
        @param stateName: the name of the State to dispatch to
        @param eventName: the event to dispatch
        @return: a url that can be used to build a taskqueue.Task instance to .dispatch(eventName)  
        """
        assert stateName and eventName
        return self.url
    
    def buildParams(self, stateName, eventName):
        """ Builds the taskqueue params. 
        
        @param stateName: the name of the State to dispatch to
        @param eventName: the event to dispatch
        @return: a dict suitable to use in constructing a url (GET) or using as params (POST)
        """
        assert stateName and eventName
        params = {constants.STATE_PARAM: stateName, 
                  constants.EVENT_PARAM: eventName,
                  constants.INSTANCE_NAME_PARAM: self.instanceName}
        for key, value in self.items():
            if key not in constants.NON_CONTEXT_PARAMS:
                if self.contextTypes.get(key) is simplejson.loads:
                    value = simplejson.dumps(value, cls=models.Encoder)
                if isinstance(value, datetime.datetime):
                    value = str(int(time.mktime(value.utctimetuple())))
                if isinstance(value, dict):
                    # FIXME: should we issue a warning that they should update fsm.yaml?
                    value = simplejson.dumps(value, cls=models.Encoder)
                if isinstance(value, list) and len(value) == 1:
                    key = key + '[]' # used to preserve lists of length=1 - see handler.py for inverse
                params[key] = value
        return params

    def getTaskName(self, nextEvent, instanceName=None, fanIn=False):
        """ Returns a task name that is unique for a specific dispatch 
        
        @param nextEvent: the event to dispatch
        @return: a task name that can be used to build a taskqueue.Task instance to .dispatch(nextEvent)
        """
        transition = self.currentState.getTransition(nextEvent)
        parts = []
        parts.append(instanceName or self.instanceName)
        
        if self.get(constants.GEN_PARAM):
            for (step, gen) in self[constants.GEN_PARAM].items():
                parts.append('continuation-%s-%s' % (step, gen))
        if self.get(constants.FORK_PARAM):
            parts.append('fork-' + str(self[constants.FORK_PARAM]))
        # post-fan-in we need to store the workIndex in the task name to avoid duplicates, since
        # we popped the generation off during fan-in
        # FIXME: maybe not pop the generation in fan-in?
        # FIXME: maybe store this in the instanceName?
        # FIXME: i wish this was easier to get right :-)
        if (not fanIn) and self.get(constants.INDEX_PARAM):
            parts.append('work-index-' + str(self[constants.INDEX_PARAM]))
        parts.append(self.currentState.name)
        parts.append(nextEvent)
        parts.append(transition.target.name)
        parts.append('step-' + str(self[constants.STEPS_PARAM]))
        return '--'.join(parts)
    
    def clone(self, instanceName=None, data=None):
        """ Returns a copy of the FSMContext.
        
        @param instanceName: the instance name to optionally apply to the clone
        @param data: a dict/mapping of data to optionally apply (.update()) to the clone
        @return: a new FSMContext instance
        """
        context = copy.deepcopy(self)
        if instanceName:
            context.instanceName = instanceName
        if data:
            context.update(data)
        return context

def startStateMachine(machineName, contexts, taskName=None, method='POST', countdown=0,
                      _currentConfig=None):
    """ Starts a new machine(s), by simply queuing a task. 
    
    @param machineName the name of the machine in the FSM to start
    @param contexts a list of contexts to start the machine with; a machine will be started for each context
    @param taskName used for idempotency; will become the root of the task name for the actual task queued
    @param method the HTTP methld (GET/POST) to run the machine with (default 'POST')
    @param countdown the number of seconds into the future to start the machine (default 0 - immediately)
    
    @param _currentConfig used for test injection (default None - use fsm.yaml definitions)
    """
    if not contexts:
        return
    if not isinstance(contexts, list):
        contexts = [contexts]
        
    # FIXME: I shouldn't have to do this.
    for context in contexts:
        context[constants.STEPS_PARAM] = 0
        
    fsm = FSM(currentConfig=_currentConfig) # loads the FSM definition
    
    instances = [fsm.createFSMInstance(machineName, data=context, method=method) for context in contexts]
    
    tasks = []
    for i, instance in enumerate(instances):
        if taskName:
            tname = '%s--startStateMachine-%d' % (taskName, i)
        else:
            tname = str(uuid.uuid4()) # generate some random name if none provided
        task = instance.generateInitializationTask(countdown=countdown, taskName=tname)
        tasks.append(task)

    queueName = instances[0].queueName # same machineName, same queues
    try:
        Queue(name=queueName).add(tasks)
    except (TaskAlreadyExistsError, TombstonedTaskError):
        # FIXME: what happens if _some_ of the tasks were previously enqueued?
        # normal result for idempotency
        logging.info('Unable to queue new machine %s with taskName %s as it has been previously enqueued.',
                      machineName, taskName)
