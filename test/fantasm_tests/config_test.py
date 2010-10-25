""" Tests for fantasm.config """

import unittest
from google.appengine.ext import db
from fantasm import config, exceptions, constants
import fantasm_tests
from fantasm_tests.helpers import getLoggingDouble

from minimock import restore

# pylint: disable-msg=C0111, W0212
# - docstrings not reqd in unit tests
# - accessing protected config members a lot in these tests

# the following classes are used for namespace and interface testing
class MockAction(object):
    def execute(self, context, obj):
        pass
class MockEntry(object):
    def execute(self, context, obj):
        pass
class MockExit(object):
    def execute(self, context, obj):
        pass
class MockActionWithContinuation(object):
    def continuation(self, context, obj, token):
        pass
    def execute(self, context, obj):
        pass
class MockActionNoExecute(object):
    pass
class MockEntryNoExecute(object):
    pass
class MockExitNoExecute(object):
    pass

class TestMachineDictionaryProcessing(unittest.TestCase):
    
    def setUp(self):
        super(TestMachineDictionaryProcessing, self).setUp()
        self.machineName = 'MyMachine'
        self.machineDict = {constants.MACHINE_NAME_ATTRIBUTE: self.machineName}
    
    def test_nameParsed(self):
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.name, self.machineName)
        
    def test_nameRequired(self):
        self.assertRaises(exceptions.MachineNameRequiredError, config._MachineConfig, {})
        
    def test_nameFollowsNamingConvention(self):
        machineName = 'My_Bad_Machine_Name'
        self.assertRaises(exceptions.InvalidMachineNameError, config._MachineConfig, 
                          {constants.MACHINE_NAME_ATTRIBUTE: machineName})
        
    def test_nameFollowsLengthRestriction(self):
        machineName = 'a'*(constants.MAX_NAME_LENGTH+1)
        self.assertRaises(exceptions.InvalidMachineNameError, config._MachineConfig, 
                          {constants.MACHINE_NAME_ATTRIBUTE: machineName})
                          
    def test_queueParsed(self):
        queueName = 'SomeQueue'
        self.machineDict[constants.MACHINE_QUEUE_NAME_ATTRIBUTE] = queueName
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.queueName, queueName)
        
    def test_queueHasDefaultValue(self):
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.queueName, constants.DEFAULT_QUEUE_NAME)
        
    def test_noNamespaceYieldNoneAttribute(self):
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.namespace, None)
        
    def test_namespaceParsed(self):
        namespace = 'MyNamespace'
        self.machineDict[constants.NAMESPACE_ATTRIBUTE] = namespace
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.namespace, namespace)
        
    def test_maxRetriesInvalidRaisesException(self):
        self.machineDict[constants.MAX_RETRIES_ATTRIBUTE] = 'abc'
        self.assertRaises(exceptions.InvalidMaxRetriesError, config._MachineConfig, self.machineDict)
        
    def test_maxRetriesParsed(self):
        self.machineDict[constants.MAX_RETRIES_ATTRIBUTE] = '3'
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.maxRetries, 3)
        
    def test_maxRetriesDefault(self):
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.maxRetries, constants.DEFAULT_MAX_RETRIES)
        
    def test_invalidAttributeRaisesException(self):
        self.machineDict['bad_attribute'] = 'something'
        self.assertRaises(exceptions.InvalidMachineAttributeError, config._MachineConfig, self.machineDict)
        
    def test_contextTypesDefault(self):
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(fsm.contextTypes, {})
        
    def test_contextTypesParsed(self):
        self.machineDict[constants.MACHINE_CONTEXT_TYPES_ATTRIBUTE] = {
            'counter': 'types.IntType'
        }
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(1, len(fsm.contextTypes))
        self.assertTrue('counter' in fsm.contextTypes)
        
    def test_contextTypesValueResolved(self):
        self.machineDict[constants.MACHINE_CONTEXT_TYPES_ATTRIBUTE] = {
            'counter': 'types.IntType',
            'batch-key': 'google.appengine.ext.db.Key'
        }
        fsm = config._MachineConfig(self.machineDict)
        self.assertEquals(int, fsm.contextTypes['counter'])
        self.assertEquals(db.Key, fsm.contextTypes['batch-key'])
        
    def test_contextTypesBadValueRaisesException(self):
        self.machineDict[constants.NAMESPACE_ATTRIBUTE] = 'namespace'
        self.machineDict[constants.MACHINE_CONTEXT_TYPES_ATTRIBUTE] = {
            'counter': 'NeverHeardOfIt'
        }
        self.assertRaises(exceptions.UnknownModuleError, config._MachineConfig, self.machineDict)

class TestStateDictionaryProcessing(unittest.TestCase):
    
    def setUp(self):
        super(TestStateDictionaryProcessing, self).setUp()
        self.machineName = 'MyFsm'
        self.fsm = config._MachineConfig({constants.MACHINE_NAME_ATTRIBUTE: self.machineName})
        self.stateDict = {constants.STATE_NAME_ATTRIBUTE: 'MyState', 
                          constants.STATE_ACTION_ATTRIBUTE: 'MockAction', 
                          constants.NAMESPACE_ATTRIBUTE: 'fantasm_tests.config_test'}
        
    def tearDown(self):
        super(TestStateDictionaryProcessing, self).tearDown()
        restore()

    def test_nameParsed(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.name, self.stateDict[constants.STATE_NAME_ATTRIBUTE])
        
    def test_nameRequired(self):
        self.stateDict.pop(constants.STATE_NAME_ATTRIBUTE)
        self.assertRaises(exceptions.StateNameRequiredError, self.fsm.addState, self.stateDict)
        
    def test_nameFollowsNamingConvention(self):
        self.stateDict[constants.STATE_NAME_ATTRIBUTE] = 'bad_name'
        self.assertRaises(exceptions.InvalidStateNameError, self.fsm.addState, self.stateDict)
        
    def test_nameFollowsLengthRestriction(self):
        self.stateDict[constants.STATE_NAME_ATTRIBUTE] = 'a'*(constants.MAX_NAME_LENGTH+1)
        self.assertRaises(exceptions.InvalidStateNameError, self.fsm.addState, self.stateDict)
        
    def test_nameIsUnique(self):
        self.fsm.addState(self.stateDict)
        self.assertRaises(exceptions.StateNameNotUniqueError, self.fsm.addState, self.stateDict)

    def test_actionRequired(self):
        self.stateDict.pop(constants.STATE_ACTION_ATTRIBUTE)
        self.assertRaises(exceptions.StateActionRequired, self.fsm.addState, self.stateDict)

    def test_initialDefaultsToFalse(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.initial, False)
    
    def test_initialParsed(self):
        self.stateDict[constants.STATE_INITIAL_ATTRIBUTE] = True
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.initial, True)
        
    def test_finalDefaultsToFalse(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.final, False)
        
    def test_finalParsed(self):
        self.stateDict[constants.STATE_FINAL_ATTRIBUTE] = True
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.final, True)
        
    def test_continuationDefaultsToFalse(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.continuation, False)
        
    def test_continuationParsed(self):
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockActionWithContinuation'
        self.stateDict[constants.STATE_CONTINUATION_ATTRIBUTE] = True
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.continuation, True)
        
    def test_entryResolvedUsingDefaultNamespace(self):
        self.stateDict[constants.STATE_ENTRY_ATTRIBUTE] = 'MockEntry'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.config_test.MockEntry, state.entry.__class__)
        
    def test_exitResolvedUsingDefaultNamespace(self):
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'MockExit'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.config_test.MockExit, state.exit.__class__)
        
    def test_actionResolvedUsingDefaultNamespace(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.config_test.MockAction, state.action.__class__)
        
    def test_entryResolvedUsingFullyQualified(self):
        self.stateDict[constants.STATE_ENTRY_ATTRIBUTE] = 'fantasm_tests.MockEntry2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockEntry2, state.entry.__class__)
        
    def test_exitResolvedUsingFullyQualified(self):
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'fantasm_tests.MockExit2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockExit2, state.exit.__class__)
        
    def test_actionResolvedUsingFullyQualified(self):
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'fantasm_tests.MockAction2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockAction2, state.action.__class__)
        
    def test_entryResolvedUsingStateOverride(self):
        self.stateDict[constants.NAMESPACE_ATTRIBUTE] = 'fantasm_tests'
        self.stateDict[constants.STATE_ENTRY_ATTRIBUTE] = 'MockEntry2'
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockAction2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockEntry2, state.entry.__class__)
        
    def test_exitResolvedUsingStateOverride(self):
        self.stateDict[constants.NAMESPACE_ATTRIBUTE] = 'fantasm_tests'
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'MockExit2'
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockAction2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockExit2, state.exit.__class__)

    def test_actionResolvedUsingStateOverride(self):
        self.stateDict[constants.NAMESPACE_ATTRIBUTE] = 'fantasm_tests'
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockAction2'
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(fantasm_tests.MockAction2, state.action.__class__)
        
    def test_noEntryYieldsNoneEntry(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.entry, None)
        
    def test_noExitYieldsNoneExit(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.exit, None)
        
    def test_unresolvedEntryYieldsException(self):
        self.stateDict[constants.STATE_ENTRY_ATTRIBUTE] = 'VeryBadClass'
        self.assertRaises(exceptions.UnknownClassError, self.fsm.addState, self.stateDict)
        
    def test_unresolvedExitYieldsException(self):
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'VeryBadClass'
        self.assertRaises(exceptions.UnknownClassError, self.fsm.addState, self.stateDict)
        
    def test_unresolvedActionYieldsException(self):
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'VeryBadClass'
        self.assertRaises(exceptions.UnknownClassError, self.fsm.addState, self.stateDict)
        
    def test_unresolvedActionBadClassYieldsException(self):
        self.stateDict[constants.NAMESPACE_ATTRIBUTE] = 'bad.namespace'
        self.assertRaises(exceptions.UnknownModuleError, self.fsm.addState, self.stateDict)
        
    def test_invalidAttributeRaisesException(self):
        self.stateDict['bad_attribute'] = 'something'
        self.assertRaises(exceptions.InvalidStateAttributeError, self.fsm.addState, self.stateDict)

    def test_continuationSpecifiedButNoContinuationMethodRaisesException(self):
        self.stateDict[constants.STATE_CONTINUATION_ATTRIBUTE] = True
        self.assertRaises(exceptions.InvalidContinuationInterfaceError, self.fsm.addState, self.stateDict)

    def test_continuationNotSpecifiedButHasContinuationMethodLogsWarning(self):
        loggingDouble = getLoggingDouble()
        self.stateDict[constants.STATE_CONTINUATION_ATTRIBUTE] = False
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockActionWithContinuation'
        self.fsm.addState(self.stateDict)
        self.assertEquals(loggingDouble.count['warning'], 1)
        
    def test_actionSpecifiedButNoExecuteMethodRaisesException(self):
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockActionNoExecute'
        self.assertRaises(exceptions.InvalidActionInterfaceError, self.fsm.addState, self.stateDict)
        
    def test_entrySpecifiedButNoExecuteMethodRaisesException(self):
        self.stateDict[constants.STATE_ENTRY_ATTRIBUTE] = 'MockEntryNoExecute'
        self.assertRaises(exceptions.InvalidEntryInterfaceError, self.fsm.addState, self.stateDict)
        
    def test_exitSpecifiedButNoExecuteMethodRaisesException(self):
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'MockExitNoExecute'
        self.assertRaises(exceptions.InvalidExitInterfaceError, self.fsm.addState, self.stateDict)
        
    def test_faninDefaultToNoFanIn(self):
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.fanInPeriod, constants.NO_FAN_IN)
        
    def test_faninMustBeAnInteger(self):
        self.stateDict[constants.STATE_FAN_IN_ATTRIBUTE] = 'abc'
        self.assertRaises(exceptions.InvalidFanInError, self.fsm.addState, self.stateDict)
        
    def test_faninParsed(self):
        self.stateDict[constants.STATE_FAN_IN_ATTRIBUTE] = 10
        state = self.fsm.addState(self.stateDict)
        self.assertEquals(state.fanInPeriod, 10)
    
    def test_faninCombinedWithContinuationRaisesException(self):
        self.stateDict[constants.STATE_FAN_IN_ATTRIBUTE] = 10
        self.stateDict[constants.STATE_CONTINUATION_ATTRIBUTE] = True
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockActionWithContinuation'
        self.assertRaises(exceptions.FanInContinuationNotSupportedError, self.fsm.addState, self.stateDict)
        
    def test_exitActionOnContinuationRaisesException(self):
        self.stateDict[constants.STATE_CONTINUATION_ATTRIBUTE] = True
        self.stateDict[constants.STATE_ACTION_ATTRIBUTE] = 'MockActionWithContinuation'
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'MockExit'
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addState, self.stateDict)
        
    def test_exitActionOnFanInRaisesException(self):
        self.stateDict[constants.STATE_FAN_IN_ATTRIBUTE] = 10
        self.stateDict[constants.STATE_EXIT_ATTRIBUTE] = 'MockExit'
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addState, self.stateDict)

class TestMachineUrlConstruction(unittest.TestCase):
    
    def test_urlIncludesHostName(self):
        fsm = config._MachineConfig({constants.MACHINE_NAME_ATTRIBUTE: 'MyMachine'})
        self.assertEquals(fsm.url, '%sfsm/MyMachine/' % constants.DEFAULT_ROOT_URL)
        
    def test_rootUrlOverridesDefault(self):
        fsm = config._MachineConfig({constants.MACHINE_NAME_ATTRIBUTE: 'MyMachine'}, rootUrl='/myfsm')
        self.assertEquals(fsm.url, '/myfsm/fsm/MyMachine/')
        
class TestTransitionDictionaryProcessing(unittest.TestCase):
    
    def setUp(self):
        super(TestTransitionDictionaryProcessing, self).setUp()
        self.transDict = {
            constants.TRANS_EVENT_ATTRIBUTE: 'MyEvent',
            constants.TRANS_TO_ATTRIBUTE: 'GoodState'
        }
        self.fsm = config._MachineConfig({constants.MACHINE_NAME_ATTRIBUTE: 'MyMachine', 
                                          constants.NAMESPACE_ATTRIBUTE: 'fantasm_tests.config_test',
                                          constants.MAX_RETRIES_ATTRIBUTE: 100})
        self.goodState = self.fsm.addState({constants.STATE_NAME_ATTRIBUTE: 'GoodState', 
                                            constants.STATE_ACTION_ATTRIBUTE: 'MockAction'})
    
    def test_nameGenerated(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.name, 'GoodState--MyEvent')
        
    def test_eventRequired(self):
        self.transDict.pop(constants.TRANS_EVENT_ATTRIBUTE)
        self.assertRaises(exceptions.TransitionEventRequiredError, self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_eventFollowingNamingConvention(self):
        self.transDict[constants.TRANS_EVENT_ATTRIBUTE] = 'bad_name'
        self.assertRaises(exceptions.InvalidTransitionEventNameError, 
                          self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_eventFollowsLengthRestriction(self):
        self.transDict[constants.TRANS_EVENT_ATTRIBUTE] = 'a'*(constants.MAX_NAME_LENGTH+1)
        self.assertRaises(exceptions.InvalidTransitionEventNameError, 
                          self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_eventParsed(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.event, self.transDict[constants.TRANS_EVENT_ATTRIBUTE])
        
    def test_toRequired(self):
        self.transDict.pop(constants.TRANS_TO_ATTRIBUTE)
        self.assertRaises(exceptions.TransitionToRequiredError, self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_toUnknownRaisesException(self):
        self.transDict[constants.TRANS_TO_ATTRIBUTE] = 'UnknownState'
        self.assertRaises(exceptions.TransitionUnknownToStateError, self.fsm.addTransition, self.transDict, 'GoodState')

    def test_toParsed(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.toState, self.goodState)

    def test_noActionYieldsNoneAttribute(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.action, None)

    def test_actionResolvedUsingDefaultNamespace(self):
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction'
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(fantasm_tests.config_test.MockAction, transition.action.__class__)

    def test_actionResolvedUsingFullyQualified(self):
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'fantasm_tests.MockAction2'
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(fantasm_tests.MockAction2, transition.action.__class__)

    def test_actionResolvedUsingStateOverride(self):
        self.transDict[constants.NAMESPACE_ATTRIBUTE] = 'fantasm_tests'
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction2'
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(fantasm_tests.MockAction2, transition.action.__class__)

    def test_unresolvedActionYieldsException(self):
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'VeryBadClass'
        self.assertRaises(exceptions.UnknownClassError, self.fsm.addTransition, self.transDict, 'GoodState')

    def test_unresolvedActionBadClassYieldsException(self):
        self.transDict[constants.NAMESPACE_ATTRIBUTE] = 'bad.namespace'
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'VeryBadClass'
        self.assertRaises(exceptions.UnknownModuleError, self.fsm.addTransition, self.transDict, 'GoodState')

    def test_invalidAttributeRaisesException(self):
        self.transDict['bad_attribute'] = 'something'
        self.assertRaises(exceptions.InvalidTransitionAttributeError, 
                          self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_retryPolicyInheritedFromMachine(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.maxRetries, 100)
        
    def test_retryPolicyOverridesMachineRetryPolicy(self):
        self.transDict[constants.MAX_RETRIES_ATTRIBUTE] = 99
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.maxRetries, 99)

    def test_transitionActionOnContinuationRaisesException(self):
        self.goodState.continuation = True
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction'
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'GoodState')

    def test_transitionActionOnFanInRaisesException(self):
        self.goodState.fanInPeriod = 10
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction'
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'GoodState')

    def test_countdownDefaultIsZero(self):
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.countdown, 0)
        
    def test_countdownMustBeAnInteger(self):
        self.transDict[constants.TRANS_COUNTDOWN_ATTRIBUTE] = 'abc'
        self.assertRaises(exceptions.InvalidCountdownError, self.fsm.addTransition, self.transDict, 'GoodState')
        
    def test_countdownParsed(self):
        self.transDict[constants.TRANS_COUNTDOWN_ATTRIBUTE] = 10
        transition = self.fsm.addTransition(self.transDict, 'GoodState')
        self.assertEquals(transition.countdown, 10)
        
class TestAdvancedTransitionDictionaryProcessing(unittest.TestCase):
    
    def setUp(self):
        super(TestAdvancedTransitionDictionaryProcessing, self).setUp()
        self.transDict = {
            constants.TRANS_EVENT_ATTRIBUTE: 'MyEvent',
            constants.TRANS_TO_ATTRIBUTE: 'state2'
        }
        self.fsm = config._MachineConfig({constants.MACHINE_NAME_ATTRIBUTE: 'MyMachine', 
                                          constants.NAMESPACE_ATTRIBUTE: 'fantasm_tests.config_test',
                                          constants.MAX_RETRIES_ATTRIBUTE: 100})
        self.state1 = self.fsm.addState({constants.STATE_NAME_ATTRIBUTE: 'state1', 
                                         constants.STATE_ACTION_ATTRIBUTE: 'MockAction'})
        self.state2 = self.fsm.addState({constants.STATE_NAME_ATTRIBUTE: 'state2', 
                                         constants.STATE_ACTION_ATTRIBUTE: 'MockAction'})

    def test_exitActionOnStatesWithTransitionToContinuationRaisesException(self):
        self.state1.exit = 'something-non-null'
        self.state2.continuation = True
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'state1')
        
    def test_exitActionOnStatesWithTransitionToFanInRaisesException(self):
        self.state1.exit = 'something-non-null'
        self.state2.fanInPeriod = 10
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'state1')
        
    def test_transitionActionOnStatesWithTransitionToContinuationRaisesExeption(self):
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction'
        self.state2.continuation = True
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'state1')
        
    def test_transitionActionOnStatesWithTransitionToFanInRaisesException(self):
        self.transDict[constants.TRANS_ACTION_ATTRIBUTE] = 'MockAction'
        self.state2.fanInPeriod = 10
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'state1')

    def test_countdownOnTransitionToFanInStateRaisesError(self):
        self.transDict[constants.TRANS_COUNTDOWN_ATTRIBUTE] = 20
        self.state2.fanInPeriod = 10
        self.assertRaises(exceptions.UnsupportedConfigurationError, self.fsm.addTransition, self.transDict, 'state1')

class TestConfigDictionaryProcessing(unittest.TestCase):
    
    def setUp(self):
        super(TestConfigDictionaryProcessing, self).setUp()
        self.rootUrl = '/foo/'
        self.machineName = 'MyMachine'
        self.initialStateName = 'MyInitialState'
        self.finalStateName = 'MyState'
        self.baseDict = {
            constants.ROOT_URL_ATTRIBUTE: self.rootUrl,
            constants.STATE_MACHINES_ATTRIBUTE: [
                {
                    constants.MACHINE_NAME_ATTRIBUTE: self.machineName,
                    constants.MACHINE_STATES_ATTRIBUTE: [
                        {
                            constants.STATE_NAME_ATTRIBUTE: self.initialStateName,
                            constants.STATE_ACTION_ATTRIBUTE: 'fantasm_tests.MockAction2',
                            constants.STATE_INITIAL_ATTRIBUTE: True,
                            constants.STATE_FINAL_ATTRIBUTE: False,
                            constants.STATE_TRANSITIONS_ATTRIBUTE: [
                                {
                                    constants.TRANS_EVENT_ATTRIBUTE: 'event1',
                                    constants.TRANS_TO_ATTRIBUTE: self.finalStateName
                                },
                                {
                                    constants.TRANS_EVENT_ATTRIBUTE: 'event2',
                                    constants.TRANS_TO_ATTRIBUTE: self.finalStateName
                                }
                            ]
                        },
                        {
                            constants.STATE_NAME_ATTRIBUTE: self.finalStateName,
                            constants.STATE_ACTION_ATTRIBUTE: 'fantasm_tests.MockAction2',
                            constants.STATE_INITIAL_ATTRIBUTE: False,
                            constants.STATE_FINAL_ATTRIBUTE: True
                        }
                    ]
                }
            ]
        }
        self.myMachineStates = self.baseDict[constants.STATE_MACHINES_ATTRIBUTE][0][constants.MACHINE_STATES_ATTRIBUTE]
    
    def test_rootUrlPassedToMachines(self):
        configuration = config.Configuration(self.baseDict)
        self.assertEquals(configuration.rootUrl, self.rootUrl)
        self.assertTrue(configuration.machines[self.machineName].url.startswith(self.rootUrl))
        
    def test_rootUrlHasDefault(self):
        self.baseDict.pop(constants.ROOT_URL_ATTRIBUTE)
        configuration = config.Configuration(self.baseDict)
        self.assertEquals(configuration.rootUrl, constants.DEFAULT_ROOT_URL)
        
    def test_machinesMustHaveUniqueNames(self):
        self.baseDict[constants.STATE_MACHINES_ATTRIBUTE].append(
            {
                constants.MACHINE_NAME_ATTRIBUTE: self.machineName,
                constants.MACHINE_STATES_ATTRIBUTE: [
                    {
                        constants.STATE_NAME_ATTRIBUTE: self.initialStateName,
                        constants.STATE_ACTION_ATTRIBUTE: 'fantasm_tests.MockAction2',
                        constants.STATE_INITIAL_ATTRIBUTE: True,
                        constants.STATE_FINAL_ATTRIBUTE: True
                    }
                ]
            }
        )
        self.assertRaises(exceptions.MachineNameNotUniqueError, config.Configuration, self.baseDict)
        
    def test_multipleMachinesParsed(self):
        otherMachineName = 'OtherMachine'
        self.baseDict[constants.STATE_MACHINES_ATTRIBUTE].append(
            {
                constants.MACHINE_NAME_ATTRIBUTE: otherMachineName,
                constants.MACHINE_STATES_ATTRIBUTE: [
                    {
                        constants.STATE_NAME_ATTRIBUTE: self.initialStateName,
                        constants.STATE_ACTION_ATTRIBUTE: 'fantasm_tests.MockAction2',
                        constants.STATE_INITIAL_ATTRIBUTE: True,
                        constants.STATE_FINAL_ATTRIBUTE: True
                    }
                ]
            }
        )
        configuration = config.Configuration(self.baseDict)
        self.assertEquals(len(configuration.machines), 2)
        self.assertEquals(configuration.machines[self.machineName].name, self.machineName)
        self.assertEquals(configuration.machines[otherMachineName].name, otherMachineName)
        
    def test_statesAddedToMachines(self):
        configuration = config.Configuration(self.baseDict)
        myMachine = configuration.machines[self.machineName]
        self.assertEquals(len(myMachine.states), 2)
        self.assertEquals(myMachine.states[self.initialStateName].name, self.initialStateName)
        self.assertEquals(myMachine.states[self.finalStateName].name, self.finalStateName)
        
    def test_machineWithNoInitialRaisesException(self):
        # this is hairy, but I'm going to clear the initial flag on the state
        self.myMachineStates[0][constants.STATE_INITIAL_ATTRIBUTE] = False # updates self.baseDict
        self.assertRaises(exceptions.MachineHasNoInitialStateError, config.Configuration, self.baseDict)
        
    def test_machineWithMultipleInitialRaisesException(self):
        # here, I'm going to set the initial flag on the second, non-initial state
        self.myMachineStates[1][constants.STATE_INITIAL_ATTRIBUTE] = True # updates self.baseDict
        self.assertRaises(exceptions.MachineHasMultipleInitialStatesError, 
                          config.Configuration, self.baseDict)
        
    def test_machineWithNoFinalRaisesException(self):
        # here, I'm going to clear the final flag on the second state, which is the only final state
        self.myMachineStates[1][constants.STATE_FINAL_ATTRIBUTE] = False # updates self.baseDict
        self.assertRaises(exceptions.MachineHasNoFinalStateError, config.Configuration, self.baseDict)
        
    def test_transitionsAddedToMachines(self):
        configuration = config.Configuration(self.baseDict)
        myMachine = configuration.machines[self.machineName]
        self.assertEquals(len(myMachine.transitions), 2)
        trans1Name = self.initialStateName + '--event1'
        trans2Name = self.initialStateName + '--event2'
        self.assertEquals(myMachine.transitions[trans1Name].name, trans1Name)
        self.assertEquals(myMachine.transitions[trans2Name].name, trans2Name)
        
    def test_stateMachineAttributeRequired(self):
        self.assertRaises(exceptions.StateMachinesAttributeRequiredError, config.Configuration, {})
        
class TestYamlFileLocation(unittest.TestCase):
    
    def test_noYamlFileRaisesException(self):
        self.assertRaises(exceptions.YamlFileNotFoundError, config.loadYaml, filename='nofile.yaml')
        
    def test_yamlFileFound(self):
        import os
        filename = os.path.join(os.path.dirname(__file__), 'yaml', 'test-TestYamlFileLocation.yaml')
        configuration = config.loadYaml(filename=filename)
        self.assertEquals(len(configuration.machines), 1)
        
        
class TestStatesWithAndWithoutDoActions(unittest.TestCase):
    
    def _test(self, yamlString):
        """ just tests that it can be built """
        import StringIO, yaml
        yamlFile = StringIO.StringIO()
        yamlFile.write(yamlString)
        yamlFile.seek(0)
        configDict = yaml.load(yamlFile.read())
        configuration = config.Configuration(configDict)
    
    def test_finalStateWithDoAction(self):
        self._test(
"""
state_machines:
- name: machineName
  namespace: fantasm_tests.fsm_test
  states:
    - name: state1
      entry: CountExecuteCalls
      action: CountExecuteCalls
      exit: CountExecuteCalls
      initial: True
      final: True
""")
        
    def test_finalStateWithOutDoAction(self):
        self._test(
"""
state_machines:
- name: machineName
  namespace: fantasm_tests.fsm_test
  states:
    - name: state1
      entry: CountExecuteCalls
      exit: CountExecuteCalls
      initial: True
      final: True
""")
        
    def test_nonFinalStateWithOutDoAction(self):
        self.assertRaises(exceptions.StateActionRequired, self._test,
"""
state_machines:
- name: machineName
  namespace: fantasm_tests.fsm_test
  states:
    - name: state1
      entry: CountExecuteCalls
      exit: CountExecuteCalls
      initial: True
""")
        
# class TestMachineConfigRetrieval(unittest.TestCase):
#     
#     def test_ensureMachineConfigIsCachedStatically(self):
#         pass # TODO
# 
#     def test_ensureYamlFileUpdateClearsStaticCache(self):
#         pass # TODO
