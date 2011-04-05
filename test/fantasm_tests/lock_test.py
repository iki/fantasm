""" Tests for fantasm.log """

# pylint: disable-msg=C0111

import random
import time # pylint: disable-msg=W0611

from fantasm_tests.fixtures import AppEngineTestCase
from fantasm.lock import ReadWriteLock, RunOnceSemaphore
from fantasm.fsm import FSMContext
from fantasm.state import State
from fantasm.exceptions import FanInWriteLockFailureRuntimeError
from fantasm.models import _FantasmTaskSemaphore

from fantasm_tests.helpers import getLoggingDouble

from google.appengine.api import memcache

from minimock import mock, restore

class ReadWriteLockTest(AppEngineTestCase):
    
    def setUp(self):
        super(ReadWriteLockTest, self).setUp()
        self.loggingDouble = getLoggingDouble()
        self.state = State('name', None, None, None)
        self.context = FSMContext(self.state, queueName='default')
        self.context.currentState = self.state
        ReadWriteLock._BUSY_WAIT_ITER_SECS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITER_SECS = 0
        ReadWriteLock._BUSY_WAIT_ITERS = ReadWriteLock.BUSY_WAIT_ITERS
        ReadWriteLock.BUSY_WAIT_ITERS = 2
        random.seed(0) # last step
        
    def tearDown(self):
        restore()
        # pylint: disable-msg=W0212
        ReadWriteLock.BUSY_WAIT_ITER_SECS = ReadWriteLock._BUSY_WAIT_ITER_SECS
        ReadWriteLock.BUSY_WAIT_ITERS = ReadWriteLock._BUSY_WAIT_ITERS
        super(ReadWriteLockTest, self).tearDown()
        
    def test_indexKey(self):
        lock = ReadWriteLock('foo', self.context)
        self.assertEqual('index-foo', lock.indexKey())

    def test_lockKey(self):
        lock = ReadWriteLock('foo', self.context)
        self.assertEqual('foo-lock-999', lock.lockKey(999))
        
    def test_currentIndex(self):
        lock = ReadWriteLock('foo', self.context)
        self.assertEqual(3626764237, lock.currentIndex())
        self.assertEqual(3626764237, lock.currentIndex())
        
    def test_currentIndex_index_changed(self):
        lock = ReadWriteLock('foo', self.context)
        self.assertEqual(3626764237, lock.currentIndex())
        memcache.incr(lock.indexKey())
        self.assertEqual(3626764238, lock.currentIndex())
        
    def test_currentIndex_index_expired(self):
        lock = ReadWriteLock('foo', self.context)
        self.assertEqual(3626764237, lock.currentIndex())
        random.seed(1)
        memcache.delete(lock.indexKey())
        self.assertEqual(577090035, lock.currentIndex())
        
    def test_acquireWriteLock(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index)
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        
    def test_acquireWriteLock_failure(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index) # need to call before acquireReadLock
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        lock.acquireReadLock(index)
        self.assertEqual('32769', memcache.get(lock.lockKey(index)))
        self.assertRaises(FanInWriteLockFailureRuntimeError, lock.acquireWriteLock, index)
        
    def test_releaseWriteLock(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index)
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        lock.releaseWriteLock(index)
        self.assertEqual('65536', memcache.get(lock.lockKey(index)))
        
    def test_acquireReadLock_before_acquireWriteLock(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireReadLock(index)
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        self.assertEqual([], self.loggingDouble.messages['debug'])
        self.assertEqual([], self.loggingDouble.messages['critical'])
        
    def test_acquireReadLock_gave_up(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index)
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        lock.acquireReadLock(index)
        self.assertEqual('32769', memcache.get(lock.lockKey(index)))
        self.assertEqual(["Tried to acquire lock 'foo-lock-3626764237' 1 times...",
                          "Tried to acquire lock 'foo-lock-3626764237' 2 times..."], 
                          self.loggingDouble.messages['debug'])
        self.assertEqual(["Gave up waiting for all fan-in work items."], 
                         self.loggingDouble.messages['critical'])
        
    def test_acquireReadLock(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index)
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        lock.releaseWriteLock(index)
        self.assertEqual('65536', memcache.get(lock.lockKey(index)))
        lock.acquireReadLock(index)
        self.assertEqual('32768', memcache.get(lock.lockKey(index)))
        self.assertEqual([], self.loggingDouble.messages['debug'])
        self.assertEqual([], self.loggingDouble.messages['critical'])
        
    def test_acquireReadLock_one_wait_iter(self):
        lock = ReadWriteLock('foo', self.context)
        index = lock.currentIndex()
        self.assertEqual(None, memcache.get(lock.lockKey(index)))
        lock.acquireWriteLock(index)
        self.assertEqual('65537', memcache.get(lock.lockKey(index)))
        def sleepAndRelease(seconds): # pylint: disable-msg=W0613
            lock.releaseWriteLock(index)
        mock('time.sleep', returns_func=sleepAndRelease, tracker=None)
        lock.acquireReadLock(index)
        self.assertEqual('32768', memcache.get(lock.lockKey(index)))
        self.assertEqual(["Tried to acquire lock 'foo-lock-3626764237' 1 times..."], 
                         self.loggingDouble.messages['debug'])
        self.assertEqual(["Gave up waiting for all fan-in work items."], 
                         self.loggingDouble.messages['critical'])

class RunOnceSemaphoreTest(AppEngineTestCase):
    
    TRANSACTIONAL = True
    
    def setUp(self):
        super(RunOnceSemaphoreTest, self).setUp()
        self.loggingDouble = getLoggingDouble()
        random.seed(0) # last step
        
    def tearDown(self):
        restore()
        super(RunOnceSemaphoreTest, self).tearDown()
        
    def test_writeRunOnceSemaphore(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, memcache.get('foo'))
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertTrue(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)
        
    def test_writeRunOnceSemaphore_second_time_False(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, memcache.get('foo'))
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertTrue(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertFalse(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)

    def test_writeRunOnceSemaphore_second_time_False_memcache_expired(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, memcache.get('foo'))
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertTrue(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)
        memcache.delete('foo')
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertFalse(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)

    def test_writeRunOnceSemaphore_second_time_wrong_payload_memcache(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, memcache.get('foo'))
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertTrue(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)
        memcache.set('foo', 'bar')
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertEqual(["Run-once semaphore memcache payload write error."], 
                         self.loggingDouble.messages['critical'])
        self.assertFalse(success)
        self.assertEqual('bar', payload)
        self.assertEqual('bar', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)

    def test_writeRunOnceSemaphore_second_time_wrong_payload_datastore(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, memcache.get('foo'))
        self.assertEqual(0, _FantasmTaskSemaphore.all().count())
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertTrue(success)
        self.assertEqual('payload', payload)
        self.assertEqual('payload', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('payload', _FantasmTaskSemaphore.all().get().payload)
        e = _FantasmTaskSemaphore.all().get()
        e.payload = 'bar'
        e.put()
        memcache.delete('foo')
        success, payload = sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertEqual(["Run-once semaphore datastore payload write error."], 
                         self.loggingDouble.messages['critical'])
        self.assertFalse(success)
        self.assertEqual('bar', payload)
        self.assertEqual('bar', memcache.get('foo'))
        self.assertEqual(1, _FantasmTaskSemaphore.all().count())
        self.assertEqual('bar', _FantasmTaskSemaphore.all().get().payload)

    def test_readRunOnceSemaphore_not_written(self):
        sem = RunOnceSemaphore('foo', None)
        self.assertEqual(None, sem.readRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL))

    def test_readRunOnceSemaphore(self):
        sem = RunOnceSemaphore('foo', None)
        sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        payload = sem.readRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertEqual('payload', payload)
        
    def test_readRunOnceSemaphore_memcache_expired(self):
        sem = RunOnceSemaphore('foo', None)
        sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        memcache.delete('foo')
        payload = sem.readRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        self.assertEqual('payload', payload)

    def test_readRunOnceSemaphore_payload_error(self):
        sem = RunOnceSemaphore('foo', None)
        sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        payload = sem.readRunOnceSemaphore('bar', transactional=self.TRANSACTIONAL)
        self.assertEqual('payload', payload)
        self.assertEqual(["Run-once semaphore memcache payload read error."], self.loggingDouble.messages['critical'])

    def test_readRunOnceSemaphore_payload_error_memcache_expired(self):
        sem = RunOnceSemaphore('foo', None)
        sem.writeRunOnceSemaphore('payload', transactional=self.TRANSACTIONAL)
        payload = sem.readRunOnceSemaphore('bar', transactional=self.TRANSACTIONAL)
        self.assertEqual('payload', payload)
        self.assertEqual(["Run-once semaphore memcache payload read error."], self.loggingDouble.messages['critical'])

        
class RunOnceSemaphoreTest_NOT_TRANSACTIONAL(AppEngineTestCase):
    
    TRANSACTIONAL = False