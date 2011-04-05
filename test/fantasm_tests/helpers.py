""" Unittest helper methods """
import os
import random
import logging
import time
import datetime
import base64
import tempfile
from collections import defaultdict
from minimock import mock
import google.appengine.api.apiproxy_stub_map as apiproxy_stub_map
import fantasm
from fantasm import constants
from fantasm import config
from fantasm.fsm import FSM
from fantasm.handlers import FSMLogHandler
from fantasm.handlers import FSMHandler
from fantasm.handlers import FSMFanInCleanupHandler
from fantasm.log import Logger
from google.appengine.ext import webapp
from google.appengine.api.taskqueue.taskqueue import TaskAlreadyExistsError

# pylint: disable-msg=C0111, C0103, W0613, W0612
# - docstrings not reqd in unit tests
# - mock interfaces need to inherit args with '_' in them

os.environ['APPLICATION_ID'] = 'fantasm'
APP_ID = os.environ['APPLICATION_ID']

class TaskDouble(object):
    """ TaskDouble is a mock for google.appengine.api.taskqueue.Task """
    def __init__(self, url, params=None, name=None, transactional=False, method='POST', countdown=0):
        """ Initialize MockTask """
        self.url = url
        self.params = params
        self.name = name or 'task-%s' % random.randint(100000000, 999999999)
        self.transactional = transactional
        self.method = method
        self.countdown = countdown

    def add(self, queue_name='default', transactional=False):
        """Adds this Task to a queue. See Queue.add."""
        return TaskQueueDouble(queue_name).add(self, transactional=transactional)

class TaskQueueDouble(object):
    """ TaskQueueDouble is a mock for google.appengine.api.lab.taskqueue.Queue """

    def __init__(self, name='default'):
        """ Initialize TaskQueueDouble object """
        self.tasknames = set([])
        self.tasks = []
        self.name = name
        
    def add(self, task_or_tasks, transactional=False):
        """ mock for google.appengine.api.taskqueue.add """
        if isinstance(task_or_tasks, list):
            tasks = task_or_tasks
            for task in tasks:
                if task.name in self.tasknames:
                    raise TaskAlreadyExistsError()
                if task.url != constants.DEFAULT_LOG_URL: # avoid fragile unit tests
                    self.tasknames.add(task.name)
                    self.tasks.append((task, transactional))
        else:
            task = task_or_tasks
            if task.name in self.tasknames:
                raise TaskAlreadyExistsError()
            if task.url != constants.DEFAULT_LOG_URL: # avoid fragile unit tests
                self.tasknames.add(task.name)
                self.tasks.append((task, transactional))

    def purge(self):
        """ purge all tasks in queue """
        self.tasks = []

class LoggingDouble(object):

    def __init__(self):
        self.count = defaultdict(int)
        self.messages = defaultdict(list)
        
    def _log(self, level, message, *args, **kwargs):
        self.count[level] += 1
        if not isinstance(message, basestring):
            try:
                message = str(message)
            except Exception, e:
                message = 'logging error'
                args = ()
        try:
            self.messages[level].append(message % args)
        except TypeError:
            self.messages[level].append(message)

    def debug(self, message, *args, **kwargs):
        self._log('debug', message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self._log('info', message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self._log('warning', message, *args, **kwargs)
        
    warn = warning

    def error(self, message, *args, **kwargs):
        self._log('error', message, *args, **kwargs)
        
    def critical(self, message, *args, **kwargs):
        self._log('critical', message, *args, **kwargs)

def getLoggingDouble():
    """ Creates a logging double and wires it up with minimock. 
    
    You are responsible for restoring the minimock environment (ususally in your tearDown).
    
    @param printMessages A list of 'debug', 'info', 'warning', 'error', 'critical' indicating which
                         error levels to dump to STDERR.
    """
    loggingDouble = LoggingDouble()
    mock(name='logging.debug', returns_func=loggingDouble.debug, tracker=None)
    mock(name='logging.info', returns_func=loggingDouble.info, tracker=None)
    mock(name='logging.warning', returns_func=loggingDouble.warning, tracker=None)
    mock(name='logging.error', returns_func=loggingDouble.error, tracker=None)
    mock(name='logging.critical', returns_func=loggingDouble.critical, tracker=None)
    def getLoggingMap():
        return { logging.CRITICAL: logging.critical,
                 logging.ERROR: logging.error,
                 logging.WARNING: logging.warning,
                 logging.INFO: logging.info,
                 logging.DEBUG: logging.debug }
    mock(name='Logger.getLoggingMap', returns_func=getLoggingMap, tracker=None)
    return loggingDouble

def runQueuedTasks(queueName='default', assertTasks=True, tasksOverride=None, speedup=True):
    """ Ability to run Tasks from unit/integration tests """
    # pylint: disable-msg=W0212
    #         allow access to protected member _IsValidQueue
    tq = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
    if assertTasks:
        assert tq.GetTasks(queueName)
        
    retries = {}
    runList = []
    alreadyRun = []
    runAgain = True
    while runAgain:
        
        runAgain = False
        tasks = tasksOverride or tq.GetTasks(queueName)
        lastRunList = list(runList)
        
        for task in tasks:
            
            if task['name'] in alreadyRun:
                continue
            
            if task.has_key('eta'):
                
                UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
                now = datetime.datetime.utcfromtimestamp(time.time())
                eta = datetime.datetime.strptime(task['eta'], "%Y/%m/%d %H:%M:%S") - UTC_OFFSET_TIMEDELTA
                
                if speedup and (runList == lastRunList):
                    # nothing ran list loop around, just force this task to speedup the tests
                    pass
                elif eta > now:
                    runAgain = True
                    continue
                
            record = True
            if task['url'] == constants.DEFAULT_CLEANUP_URL:
                record = False
                handler = FSMFanInCleanupHandler()
            elif task['url'] == constants.DEFAULT_LOG_URL:
                record = False
                handler = FSMLogHandler()
            else:
                handler = FSMHandler()
            parts = task['url'].split('?')
            assert 1 <= len(parts) <= 2
            
            environ = {'PATH_INFO': parts[0]}
            if len(parts) == 2:
                environ['QUERY_STRING'] = parts[1]
            if task['method'] == 'POST':
                environ['CONTENT_TYPE'] = 'application/x-www-form-urlencoded'
            environ['REQUEST_METHOD'] = task['method']
            
            handler.request = webapp.Request(environ)
            handler.response = webapp.Response()
            
            if task['method'] == 'POST':
                handler.request.body = base64.decodestring(task['body'])
            
            handler.request.headers[random.choice(['X-AppEngine-TaskName', 
                                                   'X-Appengine-Taskname'])] = task['name']
            if retries.get(task['name']):
                handler.request.headers[random.choice(['X-AppEngine-TaskRetryCount', 
                                                       'X-Appengine-Taskretrycount'])] = retries[task['name']]
            
            try:
                {'GET': handler.get, 'POST': handler.post}[task['method']]() # call the correct dispatch
                runAgain = True
                alreadyRun.append(task['name'])
                if record:
                    runList.append(task['name'])
                
            except Exception:
                logging.debug("Error running Task. This would be a 500 error.", exc_info=1)
                runAgain = True
                if record:
                    runList.append(task['name'])
                retries[task['name']] = retries.get(task['name'], 0) + 1
            
    return runList

class ConfigurationMock(object):
    """ A mock object that looks like a config._Configuration instance """
    def __init__(self, machines):
        self.machines = dict([(m.name, m) for m in machines])
        
def getFSMFactoryByFilename(filename):
    """ Returns an FSM instance 
    
    @param filename: a filename like 'test-Foo.yaml'
    @return: an FSM instance 
    """
    machineName = getMachineNameByFilename(filename)
    filename = os.path.join(os.path.dirname(__file__), 'yaml', filename)
    currentConfig = config.loadYaml(filename=filename)
    factory = FSM(currentConfig=currentConfig)
    return factory
    
def getMachineNameByFilename(filename):
    """ Returns a fsm name based on the input filename 
    
    @param filename: a filename like 'test-Foo.yaml'
    @return: a machine name like 'Foo'
    """
    return filename.replace('test-', '').replace('.yaml', '')
    
def setUpByFilename(obj, filename, machineName=None, instanceName=None, taskRetryLimitOverrides=None, method='GET'):
    """ Configures obj (a unittest.TestCase instance) with obj.context 
    
    @param obj: a unittest.TestCase instance
    @param filename: a filename like 'test-Foo.yaml'
    @param machineName: a machine name define in filename
    @param instanceName: an fsm instance name   
    @param taskRetryLimitOverrides: a dict of {'transitionName' : taskRetryLimit} use to override values in .yaml 
    """
    obj.machineName = machineName or filename.replace('test-', '').replace('.yaml', '')
    if not filename.startswith('/'):
        filename = os.path.join(os.path.dirname(__file__), 'yaml', filename)
    obj.currentConfig = config.loadYaml(filename=filename)
    obj.machineConfig = obj.currentConfig.machines[obj.machineName]
    if taskRetryLimitOverrides:
        overrideTaskRetryLimitRetries(obj.machineConfig, taskRetryLimitOverrides)
    obj.factory = FSM(currentConfig=obj.currentConfig)
    obj.context = obj.factory.createFSMInstance(obj.machineConfig.name, instanceName=instanceName, method=method)
    obj.initialState = obj.context.initialState
    
def setUpByString(obj, yaml, machineName=None, instanceName=None):
    """ Configures obj (a unittest.TestCase instance) with obj.context 
    
    @param obj: a unittest.TestCase instance
    @param yaml: a yaml string 
    """
    f = tempfile.NamedTemporaryFile()
    f.write(yaml)
    f.flush()
    setUpByFilename(obj, f.name, machineName=machineName, instanceName=instanceName)
    f.close()

class ZeroCountMock(object):
    count = 0
    fcount = 0
    ccount = 0
    
def getCounts(machineConfig):
    """ Returns the count values from all the states' entry/action/exit FSMActions 
    
    @param machineConfig: a config._MachineConfig instance
    @return: a dict of { 'stateName' : {'entry' : count, 'action' : count, 'exit' : count } } 
    
    NOTE: relies on the config._StateConfig and FSMState instances sharing the FSMActions
    """
    counts = {}
    for stateName, state in machineConfig.states.items():
        if state.continuation:
            counts[state.name] = {'entry': (state.entry or ZeroCountMock).count, 
                                  'continuation': (state.action or ZeroCountMock).ccount,
                                  'action': (state.action or ZeroCountMock).count, 
                                  'exit': (state.exit or ZeroCountMock).count}
            
        elif state.fanInPeriod > 0:
            counts[state.name] = {'entry': (state.entry or ZeroCountMock).count, 
                                  'action': (state.action or ZeroCountMock).count, 
                                  'exit': (state.exit or ZeroCountMock).count,
                                  'fan-in-entry': (state.entry or ZeroCountMock).fcount, 
                                  'fan-in-action': (state.action or ZeroCountMock).fcount, 
                                  'fan-in-exit': (state.exit or ZeroCountMock).fcount}
            
        else:
            counts[state.name] = {'entry': (state.entry or ZeroCountMock).count, 
                                  'action': (state.action or ZeroCountMock).count, 
                                  'exit': (state.exit or ZeroCountMock).count}
                                  
    for transition in machineConfig.transitions.values():
        counts['%s--%s' % (transition.fromState.name, transition.event)] = {
            'action': (transition.action or ZeroCountMock).count
        }
        
    return counts

def overrideFails(machineConfig, fails, tranFails):
    """ Configures all the .fail parameters on the actions defined in fails
    
    @param machineConfig: a config._MachineConfig instance
    @param fails: a list of ('stateName', 'actionName', #failures)
        ie. [('state-initial', 'entry', 2), ('state-final', 'entry', 2)] 
    @param tranFails: a list of ('transitionName', #failures)
        ie. [('state-event', 1), ('state2-event2', 3)]
    
    NOTE: relies on the config._StateConfig and FSMState instances sharing the FSMActions
    """
    for (stateName, action, fail) in fails:
        state = machineConfig.states[stateName]
        if isinstance(fail, tuple):
            getattr(state, action).fails = fail[0]
            getattr(state, action).failat = fail[1]
            getattr(state, action).cfailat = fail[2]
        else:
            getattr(state, action).fails = fail
            
    for (tranName, fail) in tranFails:
        transition = machineConfig.transitions[tranName]
        transition.action.fails = fail
            
        
def overrideTaskRetryLimit(machineConfig, overrides):
    """ Configures all the .taskRetryLimit parameters on all the Transitions 
    
    @param machineConfig: a config._MachineConfig instance
    @param overrides: a dict of {'transitionName' : task_retry_limit} to override
    """
    for (transitionName, taskRetryLimit) in overrides.items():
        transition = machineConfig.transitions[transitionName]
        transition.taskRetryLimit = taskRetryLimit

def buildRequest(method='GET', get_args=None, post_args=None, referrer=None, 
                  path=None, cookies=None, host=None, port=None):
    """ Builds a request suitable for view.initialize(). """

    if not get_args:
        get_args = {}
        
    if not post_args:
        post_args = {}

    wsgi = {
            'REQUEST_METHOD': method,
            'wsgi.url_scheme': 'http',
            'SERVER_NAME': 'localhost',
            'SERVER_PORT' : '80'
    }
    
    if get_args:
        wsgi['QUERY_STRING'] = urlencode(get_args)
        
    if referrer:
        wsgi['HTTP_REFERER'] = referrer
        
    if path:
        wsgi['PATH_INFO'] = path
        
    if host:
        wsgi['SERVER_NAME'] = host
        
    if port:
        wsgi['SERVER_PORT'] = str(port)
        
    if cookies:
        if not isinstance(cookies, BaseCookie):
            raise Exception('cookies, if set, must be a BaseCookie or subclass.')
            
        # HACK the replace('"', '') below is super-weird. For some reason, cookies.output is
        # creating a string like this:
        #
        #    wallet="ABC"
        #
        # when it should be simply
        #
        #    wallet=ABC
        #
        # I'm sure this hack will eventually break and I sincerely apologize to whomever this
        # affects.
        wsgi['HTTP_COOKIE'] = cookies.output(header='', sep=';').strip().replace('"', '')
        
    request = webapp.Request(wsgi)

    if post_args:
        assert method == 'POST', 'method must be POST for post_args'
        post_body = urlencode(post_args)
        wsgi['wsgi.input'] = StringIO(post_body)
        wsgi['CONTENT_LENGTH'] = len(post_body)
    
    return request
