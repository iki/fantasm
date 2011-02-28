""" Tests for fantasm.log """

from fantasm_tests.fixtures import AppEngineTestCase
from fantasm.fsm import FSM
from fantasm.models import _FantasmLog
from fantasm_tests.helpers import setUpByFilename
from fantasm_tests.helpers import runQueuedTasks
from fantasm_tests.helpers import getLoggingDouble
from fantasm.log import LOG_ERROR_MESSAGE

import logging

class LoggerTestPersistent(AppEngineTestCase):
    
    PERSISTENT_LOGGING = True
    
    def setUp(self):
        super(LoggerTestPersistent, self).setUp()
        filename = 'test-FSMContextTests.yaml'
        setUpByFilename(self, filename)
        self.context.logger.persistentLogging = self.PERSISTENT_LOGGING
        self.loggingDouble = getLoggingDouble()
    
    def test(self):
        self.assertEqual(0, _FantasmLog.all().count())
        self.assertEqual(0, sum(self.loggingDouble.count.values()))
        self.context.logger.info('a')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=self.PERSISTENT_LOGGING)
        self.assertEqual({True: 1, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(1, sum(self.loggingDouble.count.values()))
        
    def test_empty_tags(self):
        self.assertEqual(0, _FantasmLog.all().count())
        self.assertEqual(0, sum(self.loggingDouble.count.values()))
        self.context.logger.info('a', tags=[])
        runQueuedTasks(queueName=self.context.queueName, assertTasks=self.PERSISTENT_LOGGING)
        self.assertEqual({True: 1, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(1, sum(self.loggingDouble.count.values()))
    
    def test_TaskTooLargeError(self):
        self.assertEqual(0, _FantasmLog.all().count())
        self.assertEqual(0, sum(self.loggingDouble.count.values()))
        self.context.logger.info('a' * 100000)
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual({True: 0, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual({True: 2, False: 1}[self.PERSISTENT_LOGGING], sum(self.loggingDouble.count.values()))
        
    def test_level_OFF(self):
        self.context.logger.setLevel(logging.CRITICAL+1)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual({True: 0, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(0, sum(self.loggingDouble.count.values()))
        
    def test_level_WARNING(self):
        self.context.logger.setLevel(logging.WARNING)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=self.PERSISTENT_LOGGING)
        self.assertEqual({True: 3, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(3, sum(self.loggingDouble.count.values()))
        
    def test_maxLevel_OFF(self):
        self.context.logger.setMaxLevel(logging.DEBUG-1)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual({True: 0, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(0, sum(self.loggingDouble.count.values()))
        
    def test_maxLevel_WARNING(self):
        self.context.logger.setMaxLevel(logging.WARNING)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=self.PERSISTENT_LOGGING)
        self.assertEqual({True: 3, False: 0}[self.PERSISTENT_LOGGING], _FantasmLog.all().count())
        self.assertEqual(3, sum(self.loggingDouble.count.values()))
        
    def test_logging_object(self):
        self.context.logger.info({'a': 'b'})
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            self.assertEqual("{'a': 'b'}", _FantasmLog.all().get().message)
        else:
            self.assertEqual("{'a': 'b'}", self.loggingDouble.messages['info'][0])
        
    def test_logging_bad_object(self):
        class StrRaises(object):
            def __str__(self):
                raise Exception()
        self.context.logger.info(StrRaises())
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            self.assertEqual(LOG_ERROR_MESSAGE, _FantasmLog.all().get().message)
            self.assertEqual(logging.INFO, _FantasmLog.all().get().level)
        else:
            self.assertEqual('logging error', self.loggingDouble.messages['info'][0])
            
    def test_logging_TypeError(self):
        self.context.logger.info('%s')
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            self.assertEqual('%s', _FantasmLog.all().get().message)
            self.assertEqual(logging.INFO, _FantasmLog.all().get().level)
            
    def test_machineName(self):
        self.context.logger.info('info')
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            log = _FantasmLog.all().get()
            self.assertEqual('FSMContextTests', log.machineName)
            
    def test_stateName(self):
        self.context.logger.info('info')
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            log = _FantasmLog.all().get()
            self.assertEqual('pseudo-init', log.stateName)
            
    def test_actionName(self):
        self.context.logger.info('info')
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            log = _FantasmLog.all().get()
            self.assertEqual(None, log.actionName)
            
    def test_transitionName(self):
        self.context.logger.info('info')
        if self.PERSISTENT_LOGGING:
            runQueuedTasks(queueName=self.context.queueName)
            log = _FantasmLog.all().get()
            self.assertEqual(None, log.transitionName)
        
class LoggerTestNotPersistent(LoggerTestPersistent):
    
    PERSISTENT_LOGGING = False