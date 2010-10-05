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
from fantasm import constants

def knuthHash(number):
    """A decent hash function for integers."""
    return (number * 2654435761) % 2**32

def boolConverter(boolStr):
    """ A converter that maps some common bool string to True """
    return {'1': True, 'True': True, 'true': True}.get(boolStr, False)

def outputAction(action):
    """ Outputs the name of the action 
    
    @param action: an FSMAction instance 
    """
    if action:
        return str(action.__class__.__name__).split('.')[-1]

def outputTransitionConfig(transitionConfig):
    """ Outputs a GraphViz directed graph node
    
    @param transitionConfig: a config._TransitionConfig instance
    @return: a string
    """
    label = transitionConfig.event
    if transitionConfig.action:
        label += '/ ' + outputAction(transitionConfig.action)
    return '"%s" -> "%s" [label="%s"];' % \
            (transitionConfig.fromState.name, 
             transitionConfig.toState.name, 
             label)
            
def outputStateConfig(stateConfig):
    """ Outputs a GraphViz directed graph node
    
    @param stateConfig: a config._StateConfig instance
    @return: a string
    """
    actions = []
    if stateConfig.entry:
        actions.append('entry/ %s' % outputAction(stateConfig.entry))
    if stateConfig.action:
        actions.append('do/ %s' % outputAction(stateConfig.action))
    if stateConfig.exit:
        actions.append('exit/ %s' % outputAction(stateConfig.exit))
    label = '%s|%s' % (stateConfig.name, '\\l'.join(actions))
    if stateConfig.continuation:
        label += '|continuation = True'
    if stateConfig.fanInPeriod != constants.NO_FAN_IN:
        label += '|fan in period = %ds' % stateConfig.fanInPeriod
    shape = 'Mrecord'
    return '"%s" [shape=%s,label="{%s}"];' % \
           (stateConfig.name,
            shape,
            label)

def outputMachineConfig(machineConfig):
    """ Outputs a GraphViz directed graph of the state machine 
    
    @param machineConfig: a config._MachineConfig instance
    @return: a string
    """
    lines = []
    lines.append('digraph G {')
    lines.append('label="%s"' % machineConfig.name)
    lines.append('labelloc="t"')
    lines.append('"start" [shape=circle,style=filled,fillcolor=black,fontcolor=white];')
    lines.append('"end" [shape=doublecircle,style=filled,fillcolor=black,fontcolor=white];')
    for stateConfig in machineConfig.states.values():
        lines.append(outputStateConfig(stateConfig))
        if stateConfig.initial:
            lines.append('"start" -> "%s"' % stateConfig.name)
        if stateConfig.final:
            lines.append('"%s" -> "end"' % stateConfig.name)
    for transitionConfig in machineConfig.transitions.values():
        lines.append(outputTransitionConfig(transitionConfig))
    lines.append('}')
    return '\n'.join(lines) 