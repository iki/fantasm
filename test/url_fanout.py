""" Url fanout actions """
from django.utils import simplejson
from google.appengine.ext import db
from google.appengine.api import urlfetch

# pylint: disable-msg=C0111, W0613
# - docstring not reqd
# - some unused arguments

class TwitterSearch(object):
    """ Performs a Twitter search """
    
    def continuation(self, context, obj, token=None):
        """
        The token represents the current twitter page being queried. Page is 
        stored on the obj object so the execute knows which page to fetch.
        This method must return the next token, for when this method is called again (which will happen
        in a clone of this machine).
        """
    
        page = int(token or 1)
        obj.page = page

        if page >= 5:
            return None # stop the insanity!
            
        return page + 1
        
    def execute(self, context, obj):
        """ Performs a twitter geo search and batch stores the Twitter search results for the current page. """

        page = obj.page
        resultsPerPage = 100
        geocode = '52.15,-106.66,5mi' # Saskatoon!

        url = 'http://search.twitter.com/search.json?rpp=%s&page=%d&geocode=%s' % (resultsPerPage, page, geocode)
        response = urlfetch.fetch(url)

        if response.status_code != 200:
            raise Exception('Non-200 from Twitter. Terminating. status_code: %s' % response.status_code)
        
        data = simplejson.loads(response.content)
        results = data['results']

        statuses = []
        for result in results:
            status = TwitterStatus(key_name=str(result['id']), tweet=result['text'], 
                                   fromUserName=result['from_user'], createdDate=result['created_at'])
            statuses.append(status)
        db.put(statuses)

class TwitterStatus(db.Model):
    
    tweet = db.StringProperty(required=True, multiline=True)
    fromUserName = db.StringProperty(required=True)
    createdDate = db.StringProperty(required=True)
    fromUserID = db.IntegerProperty()
