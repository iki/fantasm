""" Tests for fantasm.models """

# pylint: disable-msg=C0111
# - docstrings not reqd in unit tests

import datetime
from fantasm.models import _FantasmFanIn
from fantasm_tests.fixtures import AppEngineTestCase
from google.appengine.ext import db

class TestModel(db.Model):
    prop1 = db.StringProperty()

class FastasmFanInTest(AppEngineTestCase):
    
    def setUp(self):
        super(FastasmFanInTest, self).setUp()
        self.testModel = TestModel()
        self.testModel.put()
    
    def test_db_Key(self):
        model = _FantasmFanIn()
        model.context = {'a': self.testModel.key()}
        model.put()
        model = db.get(model.key())
        self.assertEqual({'a': self.testModel.key()}, model.context)
        
    def test_db_Key_list(self):
        model = _FantasmFanIn()
        model.context = {'a': [self.testModel.key()]}
        model.put()
        model = db.get(model.key())
        self.assertEqual({'a': [self.testModel.key()]}, model.context)
        
    def test_datetime(self):
        model = _FantasmFanIn()
        now = datetime.datetime.now()
        model.context = {'a': now}
        model.put()
        model = db.get(model.key())
        self.assertEqual({'a': now}, model.context)
        
    def test_datetime_list(self):
        model = _FantasmFanIn()
        nows = [datetime.datetime.now(), datetime.datetime.now()]
        model.context = {'a': nows}
        model.put()
        model = db.get(model.key())
        self.assertEqual({'a': nows}, model.context)
