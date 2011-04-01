""" Complex machine actions """

import random
import time
import os
import pickle
from fantasm.action import FSMAction, DatastoreContinuationFSMAction
from fantasm import constants
from google.appengine.ext import db

# pylint: disable-msg=C0111
# - docstring not reqd

class TestModel(db.Model):
    prop1 = db.StringProperty()
    
class ResultsModel(db.Model):
    createdTime = db.DateTimeProperty(auto_now_add=True)
    total = db.IntegerProperty()
    version = db.StringProperty()
    data = db.BlobProperty(indexed=False)

class MyDatastoreContinuationFSMAction(DatastoreContinuationFSMAction):
    def getQuery(self, context, obj):
        return db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
    def getBatchSize(self, context, obj):
        return 2
    def execute(self, context, obj):
        if not obj['results']:
            return None
#        context.logger.info('MyDatastoreFSMContinuationAction.execute(): %s', 
#                            [o.key().id() for o in obj['results']],
#                            tags=[str(o.key()) for o in obj['results']])
        context['key'] = [r.key() for r in obj['results']] # would be nice for this casting on .put()
        time.sleep(5.0 * random.random())
        return 'event2'

class EntryAction1(FSMAction):
    def execute(self, context, obj):
#        context.logger.info('EntryAction1.execute()')
        context['foo'] = 'bar'
        context['unicode'] = u'\xe8'
        if 'failure' in context  and random.random() < 0.25:
            raise Exception('failure')

class EntryAction2(FSMAction):
    def execute(self, context, obj):
#        context.logger.info('EntryAction2.execute()')
        if 'failure' in context  and random.random() < 0.4:
            raise Exception('failure')

class EntryAction3(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('EntryAction3.execute()')

class EntryAction4(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('EntryAction4.execute()')
        
class EntryAction5(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('EntryAction5.execute()')

class ExitAction1(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('ExitAction1.execute()')
        
class ExitAction2(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('ExitAction2.execute()')

class ExitAction3(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('ExitAction3.execute()')
        
class ExitAction4(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('ExitAction4.execute()')
        
class ExitAction5(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('ExitAction5.execute()')

class DoAction1(FSMAction):
    def execute(self, context, obj):
#        context.logger.info('DoAction1.execute()')
        return 'event1'

class DoAction2(FSMAction):
    def execute(self, context, obj):
#        context.logger.info('DoAction2.execute()')
        return 'event2'
    
class DoAction3(FSMAction):
    def execute(self, context, obj):
        keys, keysTasksIndices = [], []
        for ctx in context:
            keys.extend(ctx.get('key', []))
            keysTasksIndices.append( (ctx.get('key', []), 
                                      ctx.get(constants.TASK_NAME_PARAM, 'unknown'),
                                      ctx.get('workIndex', 'unknown')) )
        def txn():
            results = ResultsModel.get_by_key_name(context.instanceName)
            if not results:
                results = ResultsModel(key_name=context.instanceName, 
                                       total=0, 
                                       data=pickle.dumps([]),
                                       version=os.environ['CURRENT_VERSION_ID'].split('.')[0])
            results.total += len(keys)
            data = pickle.loads(results.data)
            data += (keysTasksIndices, obj[constants.TASK_NAME_PARAM])
            results.data = pickle.dumps(data)
            results.put()
        if keys:
            db.run_in_transaction(txn)
#        context.logger.info('DoAction3.execute(): %d unique keys', len(set(keys)))
#        context.logger.info('DoAction3.execute(): %d keys', len(keys))
        return 'event3'
    
class DoAction4(FSMAction):
    def execute(self, context, obj):
#        context.logger.info('DoAction4.execute()')
        return 'event4'
        
class FinalAction5(FSMAction):
    def execute(self, context, obj):
        pass
#        context.logger.info('DoAction5.execute()')
