""" Complex machine actions """
import logging
import random
import time
from fantasm.action import FSMAction, DatastoreContinuationFSMAction
from google.appengine.ext import db

# pylint: disable-msg=C0111
# - docstring not reqd

class TestModel(db.Model):
    prop1 = db.StringProperty()

class MyDatastoreContinuationFSMAction(DatastoreContinuationFSMAction):
    def getQuery(self, context, obj):
        return db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
    def getBatchSize(self, context, obj):
        return 2
    def execute(self, context, obj):
        if not obj['results']:
            return None
        logging.info('MyDatastoreFSMContinuationAction.execute(): %s', [o.key().id() for o in obj.results])
        context['key'] = [r.key() for r in obj.results] # would be nice for this casting on .put()
        time.sleep(5.0 * random.random())
        return 'event2'

class EntryAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction1.execute()')
        context['foo'] = 'bar'
        if 'failure' in context  and random.random() < 0.25:
            raise Exception('failure')

class EntryAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction2.execute()')
        if 'failure' in context  and random.random() < 0.4:
            raise Exception('failure')

class EntryAction3(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction3.execute()')

class EntryAction4(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction4.execute()')
        
class EntryAction5(FSMAction):
    def execute(self, context, obj):
        logging.info('EntryAction5.execute()')

class ExitAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction1.execute()')
        
class ExitAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction2.execute()')

class ExitAction3(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction3.execute()')
        
class ExitAction4(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction4.execute()')
        
class ExitAction5(FSMAction):
    def execute(self, context, obj):
        logging.info('ExitAction5.execute()')

class DoAction1(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction1.execute()')
        return 'event1'

class DoAction2(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction2.execute()')
        return 'event2'
    
class DoAction3(FSMAction):
    def execute(self, context, obj):
        keys = []
        for ctx in context:
            keys.extend(ctx.get('key', []))
        logging.info('DoAction3.execute(): %d unique keys', len(set(keys)))
        logging.info('DoAction3.execute(): %d keys', len(keys))
        return 'event3'
    
class DoAction4(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction4.execute()')
        return 'event4'
        
class FinalAction5(FSMAction):
    def execute(self, context, obj):
        logging.info('DoAction5.execute()')
