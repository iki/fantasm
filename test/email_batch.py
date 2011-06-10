""" Email batch machine actions """
import logging
from datetime import datetime
from google.appengine.ext import db
from google.appengine.ext import webapp
from fantasm.action import DatastoreContinuationFSMAction

# pylint: disable-msg=C0111, W0613, C0103
# - docstring not reqd
# - non camel-case vars are ok

VALIDATION_COUNTDOWN = 4*60*60
BATCH_SUCCESS_RATE = 0.95

class StartBatch(object):
    
    def execute(self, context, obj):

        # query to get subscriber count
        expected = Subscriber.all().count()
        
        # add batch entry to datastore
        batch = EmailBatch(start_date=datetime.utcnow(), expected=expected)
        batch.put()
        context['batch-key'] = batch.key()
        logging.info('Added a new email batch. ID %d, expected %d.', batch.key().id(), expected)
        
        # kick off the validation machine (in the future)
        context.spawn('ValidateEmailBatch', {'batch-key': context['batch-key']}, countdown=30*60)
        
        return 'next'
        
class SendEmail(DatastoreContinuationFSMAction):
    
    def getQuery(self, context, obj):
        return Subscriber.all()
        
    def execute(self, context, obj):
        if not obj['result']:
            return None # datastore continuations can go "one past" the end of the results; we need to handle this here
        subscriber = obj['result']

        # TODO: here's where the actual email would be send with Mail API
        # from google.appengine.api import mail
        # mail.send_mail(sender="sender@example.com",
        #                to=subscriber.email, ...)

        logging.info('Sending email to %s', subscriber.email)

        return 'next'
        
class UpdateCounter(object):
    
    def execute(self, contexts, obj): # contexts (plural) because this is a fan_in
        
        logging.info('NUMBER OF CONTEXTS FANNED IN: %d', len(contexts))
        
        batch_key = contexts[0]['batch-key'] # all the batch-key's are the same, or Fantasm has a bug
        emails_sent = len(contexts) # the number of contexts represents the number of emails successfully sent
        
        def txn():
            batch = db.get(batch_key)
            batch.actual += emails_sent
            batch.put()
            
        db.run_in_transaction(txn)
        
class ValidateBatch(object):
    
    def execute(self, context, obj):
        
        batch_key = context['batch-key']
        batch = db.get(batch_key)
        if batch.actual < batch.expected * BATCH_SUCCESS_RATE:
            # TODO: send an ops email
            logging.critical('Less than %f success on batch. Sent %d, expected %d.',
                             BATCH_SUCCESS_RATE, batch.actual, batch.expected)
        else:
            logging.info('Batch complete. Sent %d, expected %d.', batch.actual, batch.expected)
        
class EmailBatch(db.Model):
    
    start_date = db.DateTimeProperty(required=True)
    expected = db.IntegerProperty(required=True)
    actual = db.IntegerProperty(required=True, default=0)

class Subscriber(db.Model):
    
    email = db.EmailProperty(required=True)
    
class CreateSubscribers(webapp.RequestHandler):
    """ Just a test harness to stock some data. """
    
    def get(self):
        emails = [
            'ted@example.com',
            'sue@example.com',
            'bob@example.com'
        ]
        for email in emails:
            Subscriber(email=email).put()
            logging.info('Added new subscriber %s', email)
