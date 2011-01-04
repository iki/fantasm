""" Tests for fantasm.log """

from fantasm_tests.fixtures import AppEngineTestCase
from fantasm.fsm import FSM
from fantasm.models import _FantasmLog
from fantasm_tests.helpers import setUpByFilename
from fantasm_tests.helpers import runQueuedTasks
from fantasm.log import LOG_ERROR_MESSAGE

import logging

class LoggerTest(AppEngineTestCase):
    
    def setUp(self):
        super(LoggerTest, self).setUp()
        filename = 'test-FSMContextTests.yaml'
        setUpByFilename(self, filename)
    
    def test(self):
        self.assertEqual(0, _FantasmLog.all().count())
        self.context.logger.info('a')
        runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(1, _FantasmLog.all().count())
    
    def test_TaskTooLargeError(self):
        self.assertEqual(0, _FantasmLog.all().count())
        self.context.logger.info('a' * 100000)
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual(0, _FantasmLog.all().count())
        
    def test_level_OFF(self):
        self.context.logger.setLevel(logging.CRITICAL+1)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual(0, _FantasmLog.all().count())
        
    def test_level_WARNING(self):
        self.context.logger.setLevel(logging.WARNING)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(3, _FantasmLog.all().count())
        
    def test_maxLevel_OFF(self):
        self.context.logger.setMaxLevel(logging.DEBUG-1)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName, assertTasks=False)
        self.assertEqual(0, _FantasmLog.all().count())
        
    def test_maxLevel_WARNING(self):
        self.context.logger.setMaxLevel(logging.WARNING)
        self.context.logger.critical('critical')
        self.context.logger.error('error')
        self.context.logger.warning('warning')
        self.context.logger.info('info')
        self.context.logger.debug('debug')
        runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(3, _FantasmLog.all().count())
        
    def test_logging_object(self):
        self.context.logger.info({'a': 'b'})
        runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual("{'a': 'b'}", _FantasmLog.all().get().message)
        
    def test_logging_bad_object(self):
        class StrRaises(object):
            def __str__(self):
                raise Exception()
        self.context.logger.info(StrRaises())
        runQueuedTasks(queueName=self.context.queueName)
        self.assertEqual(LOG_ERROR_MESSAGE, _FantasmLog.all().get().message)