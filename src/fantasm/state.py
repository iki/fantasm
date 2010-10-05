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
from fantasm import constants
from fantasm.transition import Transition
from fantasm.exceptions import UnknownEventError, InvalidEventNameRuntimeError, InvalidFinalEventRuntimeError

class State(object):
    """ A state object for a machine. """
    
    def __init__(self, name, entryAction, doAction, exitAction, machineName=None, 
                 isFinalState=False, isInitialState=False, isContinuation=False, fanInPeriod=constants.NO_FAN_IN):
        """
        @param name: the name of the State instance
        @param entryAction: an FSMAction instance
        @param doAction: an FSMAction instance
        @param exitAction: an FSMAction instance
        @param machineName: the name of the machine this State is associated with 
        @param isFinalState: a boolean indicating this is a terminal state
        @param isInitialState: a boolean indicating this is a starting state
        @param isContinuation: a boolean indicating this is a continuation State 
        @param fanInPeriod: integer (seconds) representing how long these states should collect before dispatching
        """
        assert not (exitAction and isContinuation) # TODO: revisit this with jcollins, we want to get it right
        assert not (exitAction and fanInPeriod > constants.NO_FAN_IN) # TODO: revisit this with jcollins
        
        self.name = name
        self.entryAction = entryAction
        self.doAction = doAction
        self.exitAction = exitAction
        self.machineName = machineName # is this really necessary? it is only for logging.
        self.isInitialState = isInitialState
        self.isFinalState = isFinalState
        self.isContinuation = isContinuation
        self.isFanIn = fanInPeriod != constants.NO_FAN_IN
        self.fanInPeriod = fanInPeriod
        self._eventToTransition = {}
        
    def addTransition(self, transition, event):
        """ Adds a transition for an event. 
        
        @param transition: a Transition instance
        @param event: a string event that results in the associated Transition to execute  
        """
        assert isinstance(transition, Transition)
        assert isinstance(event, basestring)
        
        assert not (self.exitAction and transition.target.isContinuation) # TODO: revisit this with jcollins
        assert not (self.exitAction and transition.target.isFanIn) # TODO: revisit
        
        self._eventToTransition[event] = transition
        
    def getTransition(self, event):
        """ Gets the Transition for a given event. 
        
        @param event: a string event
        @return: a Transition instance associated with the event
        @raise an UnknownEventError if event is unknown (i.e., no transition is bound to it).
        """
        try:
            return self._eventToTransition[event]
        except KeyError:
            logging.critical('Cannot find transition for event "%s". (Machine %s, State %s)',
                             event, self.machineName, self.name)
            raise UnknownEventError(event, self.machineName, self.name)
        
    def dispatch(self, context, event, obj):
        """ Fires the transition and executes the next States's entry, do and exit actions.
            
        @param context: an FSMContext instance
        @param event: a string event to dispatch to the State
        @param obj: an object that the Transition can operate on  
        @return: the event returned from the next state's main action.
        """
        transition = self.getTransition(event)
        
        if context.currentState.exitAction:
            try:
                context.currentState.exitAction.execute(context, obj)
            except Exception:
                logging.error('Error processing entry action for state. (Machine %s, State %s, exitAction %s)',
                              context.machineName, 
                              context.currentState.name, 
                              context.currentState.exitAction.__class__)
                raise
        
        # join the contexts of a fan-in
        contextOrContexts = context
        if transition.target.isFanIn:
            contextOrContexts = context.mergeJoinDispatch(event, obj)
            if not contextOrContexts:
                logging.info('Fan-in resulted in 0 contexts. Terminating machine. (Machine %s, State %s)',
                             context.machineName, 
                             context.currentState.name)
                obj[constants.TERMINATED_PARAM] = True
                
        transition.execute(context, obj)
        
        if context.currentState.entryAction:
            try:
                context.currentState.entryAction.execute(contextOrContexts, obj)
            except Exception:
                logging.error('Error processing entry action for state. (Machine %s, State %s, entryAction %s)',
                              context.machineName, 
                              context.currentState.name, 
                              context.currentState.entryAction.__class__)
                raise
            
        if context.currentState.isContinuation:
            try:
                token = context.get(constants.CONTINUATION_PARAM, None)
                nextToken = context.currentState.doAction.continuation(contextOrContexts, obj, token=token)
                
                # In the case of a datastore continuation, you may not know that you are done until you go
                # "one past" the end. In this special case, we'll do special work to avoid calling the execute
                # handler with no results.
                if hasattr(obj, 'results') and not obj.results:
                    logging.info('No continuation results. Terminating machine. (Machine %s, State %s)',
                                 context.machineName, 
                                 context.currentState.name)
                    obj[constants.TERMINATED_PARAM] = True
                if nextToken:
                    context.continuation(nextToken)
                context.pop(constants.CONTINUATION_PARAM, None) # pop this off because it is really long
                
            except Exception:
                logging.error('Error processing continuation for state. (Machine %s, State %s, continuation %s)',
                              context.machineName, 
                              context.currentState.name, 
                              context.currentState.doAction.__class__)
                raise
            
        # either a fan-in resulted in no contexts, or a continuation was completed
        if obj.get(constants.TERMINATED_PARAM):
            return None
            
        nextEvent = None
        if context.currentState.doAction:
            try:
                nextEvent = context.currentState.doAction.execute(contextOrContexts, obj)
            except Exception:
                logging.error('Error processing action for state. (Machine %s, State %s, Action %s)',
                              context.machineName, 
                              context.currentState.name, 
                              context.currentState.doAction.__class__)
                raise
                
        if nextEvent:
            if not isinstance(nextEvent, str) or not constants.NAME_RE.match(nextEvent):
                raise InvalidEventNameRuntimeError(nextEvent, context.machineName, context.currentState.name,
                                                   context.instanceName)
            if context.currentState.isFinalState:
                raise InvalidFinalEventRuntimeError(nextEvent, context.machineName, context.currentState.name,
                                                    context.instanceName)
            
        return nextEvent
