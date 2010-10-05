""" Simple machine actions """
import logging
import random
from fantasm.action import FSMAction

# pylint: disable-msg=C0111
# - docstring not reqd

class EntryAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction1.execute()')
        context['foo'] = 'bar'
        if 'failure' in context and random.random() < 0.25:
            raise Exception('failure')

class EntryAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction2.execute()')

class EntryAction3(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction3.execute()')

class ExitAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction1.execute()')
        
class ExitAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction2.execute()')
        if 'failure' in context and random.random() < 0.4:
            raise Exception('failure')

class ExitAction3(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction3.execute()')
        
class DoAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction1.execute()')
        context['unicode'] = u'\xe8'
        return 'event1'

class DoAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction2.execute()')
        logging.info(context['unicode'])
        return 'event2'
    
class DoAction3(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction3.execute()')
        if 'failure' in context and random.random() < 0.4:
            raise Exception('failure')

class TransitionAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('TransitionAction1.execute()')
        if 'failure' in context and random.random() < 0.4:
            raise Exception('failure')

class TransitionAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('TransitionAction2.execute()')
        if 'failure' in context and random.random() < 0.4:
            raise Exception('failure')
