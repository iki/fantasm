""" Tests for fantasm.handlers """

# pylint: disable-msg=C0111
# - docstrings not reqd in tests

import unittest
from minimock import mock, restore
from fantasm_tests.helpers import buildRequest
from fantasm.handlers import getMachineNameFromRequest
from fantasm import config # pylint: disable-msg=W0611
                           # - actually used by minimock

class MockConfigRootUrl(object):
    """ Simple mock config. """
    def __init__(self, rootUrl):
        """ Initialize. """
        self.rootUrl = rootUrl

class GetMachineNameFromRequestTests(unittest.TestCase):
    """ Tests for getMachineNameFromRequest """
    
    def tearDown(self):
        super(GetMachineNameFromRequestTests, self).tearDown()
        restore()
    
    def test_defaultMountPointNoExtraPathInfo(self):
        url = '/fantasm/fsm/MyMachine/'
        request = buildRequest(path=url)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_defaultMountPointExtraPathInfo(self):
        url = '/fantasm/fsm/MyMachine/state1/to/state2/'
        request = buildRequest(path=url)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_graphvizMapping(self):
        url = '/fantasm/graphviz/MyMachine/'
        request = buildRequest(path=url)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_singleLevelMountPointNoExtraPathInfo(self):
        url = '/o/fsm/MyMachine/'
        request = buildRequest(path=url)
        mock('config.currentConfiguration', returns=MockConfigRootUrl('/o/'), tracker=None)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_singleLevelMountPointExtraPathInfo(self):
        url = '/o/fsm/MyMachine/state1/to/state2'
        request = buildRequest(path=url)
        mock('config.currentConfiguration', returns=MockConfigRootUrl('/o/'), tracker=None)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_multipleLevelMountPointNoExtraPathInfo(self):
        url = '/other/mount/point/fsm/MyMachine/'
        request = buildRequest(path=url)
        mock('config.currentConfiguration', returns=MockConfigRootUrl('/other/mount/point/'), tracker=None)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
        
    def test_multipleLevelMountPointExtraPathInfo(self):
        url = '/other/mount/point/fsm/MyMachine/state1/to/state2'
        request = buildRequest(path=url)
        mock('config.currentConfiguration', returns=MockConfigRootUrl('/other/mount/point/'), tracker=None)
        name = getMachineNameFromRequest(request)
        self.assertEquals(name, 'MyMachine')
