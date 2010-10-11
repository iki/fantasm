""" Code for the backup example. """

import logging
import datetime
import random
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.ext import webapp
from google.appengine.api import memcache
from fantasm.action import FSMAction, DatastoreContinuationFSMAction
from fantasm.fsm import spawn

FULL_BACKUP_ON_DAY_OF_WEEK = 2 # 0 = Monday, 1 = Tuesday, 2 = Wednesday, ...
DELETE_BACKUPS_OLDER_THAN_DAYS = 90 # set to -1 to disable
DELETE_BACKUP_DELAY_IN_SECONDS = 60*60*4 # the amount of time to delay the backup deletion so that it doesn't slow
                                         # down the main backup. 0 means no delay.

class _Backup(db.Model):
    """ Tracks backup meta-information. """
    backupId = db.StringProperty(required=True)
    model = db.StringProperty(required=True)
    lastBackup = db.DateTimeProperty()

class Account(db.Model):
    accountID = db.StringProperty(required=True)
    firstName = db.StringProperty(required=True)
    lastName = db.StringProperty(required=True)
    createdDate = db.DateTimeProperty(auto_now_add=True)
    modifiedDate = db.DateTimeProperty(auto_now=True)
    
class Company(db.Model):
    companyID = db.StringProperty(required=True)
    companyName = db.StringProperty(required=True)
    accountKeyName = db.StringProperty(required=True)
    createdDate = db.DateTimeProperty(auto_now_add=True)
    modifiedDate = db.DateTimeProperty(auto_now=True)

# Just add any new models to this tuple. 
# Use a db.DateTimeProperty(auto_now=True) in the middle column above to get incremental 
# semantics (without one, you'll get a full backup of the model nightly, which is a bit wasteful). 
# The backup batch size is an appropriate size for a batch db.put(), 
# i.e., choose a size that can fit in a single proto buffer safely. If in doubt, just use a size of 1.
BACKUP_CONFIG = (
     # MODEL                          MODIFIED DATE      BACKUP BATCH SIZE
    (Account,                         'modifiedDate',      1),
    (Company,                         'modifiedDate',     10),
    # add more models here
)
BACKUP_CLASS = dict( (m[0].__name__, m[0]) for m in BACKUP_CONFIG )
BACKUP_INCREMENTAL_PROPERTY = dict( (m[0].__name__, m[1]) for m in BACKUP_CONFIG )
BACKUP_BATCH_SIZE = dict( (m[0].__name__, m[2]) for m in BACKUP_CONFIG )
BACKUP_MODELS = sorted(BACKUP_CLASS.keys())


### BACKUP ACTIONS

class SelectBackup(FSMAction):
    """ Using utcnow, selects the current backup. """
    
    def execute(self, context, obj):
        """ Selects the most recent weekday, assigns the backupId to the context. """
        
        utcnow = datetime.datetime.utcnow()
        fullBackupDay = utcnow - datetime.timedelta(days=utcnow.weekday()) \
                        + datetime.timedelta(days=FULL_BACKUP_ON_DAY_OF_WEEK)
        # above date might be in the future, if so, we need to back it up a week
        if fullBackupDay > utcnow:
            fullBackupDay = fullBackupDay - datetime.timedelta(days=7)
        context['backupId'] = 'backup-' + fullBackupDay.strftime('%Y-%m-%d')
        
        # spawn the DeleteBackup machine
        if DELETE_BACKUPS_OLDER_THAN_DAYS > 0:
            spawn('DeleteBackup', 
                  {'daysOld': DELETE_BACKUPS_OLDER_THAN_DAYS}, 
                  method='POST', 
                  countdown=DELETE_BACKUP_DELAY_IN_SECONDS)
        
        return 'ok'
        
class EnumerateBackupModels(FSMAction):
    """ Continues through the backup models, setting new backup dates for each. """
    
    def continuation(self, context, obj, token=None):
        """ Returns each model to be operated on. """
        if not token:
            obj['model'] = BACKUP_MODELS[0]
            return BACKUP_MODELS[1] if len(BACKUP_MODELS) > 1 else None
        else:
            # find next in list
            for i in range(0, len(BACKUP_MODELS)):
                if BACKUP_MODELS[i] == token:
                    obj['model'] = BACKUP_MODELS[i]
                    return BACKUP_MODELS[i+1] if i < len(BACKUP_MODELS)-1 else None
        return None # this occurs if a token passed in is not found in list - shouldn't happen
    
    def execute(self, context, obj):
        """ Updates the backup meta information. """
        backupId = context['backupId']
        model = obj['model']
        
        def tx():
            """ Gets the backup meta information for this model, creating if necessary. """
            keyName = '%s:%s' % (backupId, model)
            entry = _Backup.get_by_key_name(keyName)
            
            if not entry:
                entry = _Backup(key_name=keyName, backupId=backupId, model=model, lastBackup=None)
            else:
                context['lastBackup'] = entry.lastBackup # get the lastBackup time
            
            entry.lastBackup = datetime.datetime.utcnow() # update to now
            entry.put()

        db.run_in_transaction(tx)
        context['model'] = model
        return 'ok'
        
class BackupEntity(DatastoreContinuationFSMAction):
    """ Continues over all the entities, backing up one at a time. """
    
    def getQuery(self, context, obj):
        """ Returns the query to iterate over the model. """
        model = context['model']
        query = 'SELECT * FROM %s' % model
        
        lastBackup = context.get('lastBackup')
        if lastBackup and BACKUP_INCREMENTAL_PROPERTY[model]:
            query = query + ' WHERE %s >= :1' % BACKUP_INCREMENTAL_PROPERTY[model]
            return db.GqlQuery(query, lastBackup)
        else:
            return db.GqlQuery(query)
            
    def getBatchSize(self, context, obj):
        """ Returns the batch size for the current model. """
        model = context['model']
        batchSize = BACKUP_BATCH_SIZE[model]
        return batchSize
    
    def execute(self, context, obj):
        """ Copies all information to a BackupAccount entity. """
        
        if not obj['results']:
            # query may return no results
            return None
        
        model = context['model']
        backupId = context['backupId']
        entities = obj.results

        backupEntities = []
        for originalEntity in entities:

            # build a key with same path, but in different namespace
            originalKeyPath = originalEntity.key().to_path()
            newKey = db.Key.from_path(*originalKeyPath, **{'namespace': backupId})

            # copy over the property values
            kwargs = {}
            for prop in originalEntity.properties().values():
                if isinstance(prop, (db.ReferenceProperty, blobstore.BlobReferenceProperty)):
                    # avoid the dereference/auto-lookup
                    datastoreValue = prop.get_value_for_datastore(originalEntity)
                else:
                    datastoreValue = getattr(originalEntity, prop.name, None)
                kwargs[prop.name] = datastoreValue
                
            logging.info('Backing up %s (Path %s Key %s) to %s', model, originalKeyPath, 
                                                              originalEntity.key(), backupId)
            backupModelClass = BACKUP_CLASS[model]
            backupEntity = backupModelClass(key=newKey, **kwargs)
            backupEntities.append(backupEntity)

        db.put(backupEntities)
        
        
### DELETE BACKUP ACTIONS
        
class ComputeDate(FSMAction):
    """ Compute a stable backup date to use across continuations. """
    def execute(self, context, obj):
        """ Using daysOld param, compute an absolute date. """
        daysOld = context['daysOld']
        context['backupDate'] = datetime.datetime.utcnow() - datetime.timedelta(days=daysOld)
        return 'ok'

class SelectBackupToDelete(DatastoreContinuationFSMAction):
    """ Select a backup based on the passed in argument. """
    
    def getQuery(self, context, obj):
        """ Searches the _Backup model for backups that are older than the number of days provided. """
        return _Backup.all().filter('lastBackup <', context['backupDate'])
        
    def execute(self, context, obj):
        """ Adds the backup_id and model to the context. """
        if not obj['results']:
            return None
        backupEntity = obj.result
        context['model'] = backupEntity.model
        context['backupId'] = backupEntity.backupId
        db.delete(backupEntity)
        return 'ok'
        
class DeleteBackupEntity(DatastoreContinuationFSMAction):
    """ Walks through a model and deletes rows that belong to a given backupId. """
    
    def getQuery(self, context, obj):
        """ Queries for everything in a backup model for a given backupId. """
        model = context['model']
        backupId = context['backupId']
        backupModelClass = BACKUP_CLASS[model]
        return backupModelClass.all(keys_only=True, namespace=backupId)
        
    def getBatchSize(self, context, obj):
        """ Returns the delete batch size. """
        return 100
        
    def execute(self, context, obj):
        """ Actually delete the keys. """
        if obj['results']:
            db.delete(obj['results'])


FIRST_NAMES = ['Abe', 'Bob', 'Carol', 'Dale', 'Ewan', 'Fred', 'Georgina', 'Hanna']
LAST_NAMES = ['Fletcher', 'Bower', 'Read', 'Bree', 'Collins', 'Rusaw']
COMPANY_NAMES = ['Widgets', 'Technologies', 'Printers', 'Landscaping', 'Plumbing', 'Electricians']

class PopulateBackupExample(webapp.RequestHandler):
    
    def get(self):
        """ Create (or update) some rows in the example models Account and Company. """

        self.response.headers['Content-type'] = 'text/plain'
        validAccountKeyNames = set()
        for i in range(0, random.randint(3, 20)):
            
            kwargs = {
                'accountID': 'a' + str(random.randint(10, 50)),
                'firstName': random.choice(FIRST_NAMES),
                'lastName': random.choice(LAST_NAMES)
            }
            keyName = kwargs['accountID']
            validAccountKeyNames.add(keyName)
            Account(key_name=keyName, **kwargs).put()
            self.response.out.write('Account %s\n' % kwargs)
            
        for i in range(0, random.randint(2, 10)):
            
            kwargs = {
                'companyID': 'c' + str(random.randint(60, 90)),
                'companyName': random.choice(LAST_NAMES) + ' ' + random.choice(COMPANY_NAMES),
                'accountKeyName': random.choice(list(validAccountKeyNames))
            }
            keyName = kwargs['companyID']
            Company(key_name=keyName, **kwargs).put()
            self.response.out.write('Company %s\n' % kwargs)
