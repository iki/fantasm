""" Installs mock middleware - simulates a TERRIBLE App Engine environment

FIXME: work in progress, currently disabled
"""

import random
import google.appengine.ext.db

from fantasm.models import _FantasmFanIn
from fantasm.models import _FantasmTaskSemaphore
from fantasm.models import _FantasmInstance
from fantasm.models import _FantasmLog

GET = google.appengine.ext.db.get
GET_THRESHOLD = 0.1

MODELS = [_FantasmFanIn, _FantasmTaskSemaphore, _FantasmInstance, _FantasmLog]

GETS = [m.get for m in MODELS]
GET_BY_KEY_NAMES = [m.get_by_key_name for m in MODELS]
GET_BY_IDS = [m.get_by_id for m in MODELS]
GET_OR_INSERT = [m.get_or_insert for m in MODELS]

GET_BY_KEY_NAME_THRESHOLD = 0.1

def get(*args, **kwargs):
    """ A mock of google.appengine.ext.db """
    if random.uniform(0.0, 1.0) < GET_THRESHOLD:
        raise google.appengine.ext.db.Error()
    return GET(*args, **kwargs)

def get_by_key_name(cls, *args, **kwargs):
    """ A mock of google.appengine.ext.db """
    if random.uniform(0.0, 1.0) < GET_BY_KEY_NAME_THRESHOLD:
        raise google.appengine.ext.db.Error()
    return GET_BY_KEY_NAMES[MODELS.index(cls)](*args, **kwargs)
    
def mock():
    """ sets up exception throwing mocks """
    google.appengine.ext.db.get = get
    for model in MODELS:
        model.get_by_key_name = classmethod(get_by_key_name)

def restore():
    """ tears down exception throwing mocks """
    google.appengine.ext.db.get = GET
    for i, model in enumerate(MODELS):
        model.get_by_key_name = GET_BY_KEY_NAMES[i]

def exceptionGeneratingWsgiMiddleware(app):
    """
    Exception generating WSGI middleware. 
    
    NOTE: see google.appengine.ext.appstats.recording.appstats_wsgi_middleware
    """
    def exceptionGeneratingWsgiWrapper(environ, startResponse):
        """ 
        see WSGI middleware specs 
        """
        mock()
        def exceptionGeneratingStartResponse(status, headers, exc_info=None):
            """ 
            see WSGI middleware specs 
            """
            return startResponse(status, headers, exc_info)
        try:
            result = app(environ, exceptionGeneratingStartResponse)
        except Exception:
            restore()
            raise
        if result is not None:
            for value in result:
                yield value
        restore()
    return exceptionGeneratingWsgiWrapper

def __webapp_add_wsgi_middleware(app):
    """ installs webapp middleware """
    app = exceptionGeneratingWsgiMiddleware(app)
    return app