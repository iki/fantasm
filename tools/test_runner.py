"""Module to identify all unit tests that need to run and return a TestSuite object to be run"""

import os
import sys
import site
import unittest
import logging
import re
import types
import teamcity
from teamcity.unittestpy import TeamcityTestRunner

pt = os.getcwd()
root_dir = pt

loglevel = int(os.environ.get('UNITTEST_LOGLEVEL', logging.ERROR))
logging.getLogger().setLevel(loglevel) # DEBUG, LOG, WARN(?) are too chatty

TEST_MODULES = [
    'test'
]

COVERAGE_OMIT_PATHS = [
    'test', '/usr/local/google_appengine/'
]

def convert_module_path(module):
    """Helper method to convert a module in dot notation to
    a proper file path"""
    path_bits = module.split(".")
    return os.path.sep.join(path_bits)

def suite():
    """Iterate across all of the modules in TEST_MODULES, find all of the
    tests within and return a TestSuite which will run them all"""
    try:
        test_suite = unittest.TestSuite()

        TEST_RE = r"^.*_tests?\.py$"

        # Search through every file inside this package.
        test_names = []

        for module in TEST_MODULES:
            test_dir = os.path.join(root_dir, convert_module_path(module))

            for filename in os.listdir(test_dir):
                
                if os.path.isdir(os.path.join(test_dir, filename)) and not filename.startswith("."):
                    TEST_MODULES.append(module + "." + filename)
                    continue

                if not re.match(TEST_RE, filename):
                    continue
                    
                # Import the test file and find all TestClass clases inside it.
                module_name = '%s.%s' % (module, filename[:-3])
                # logging.critical('adding %s' % module_name)
                # test_suite.addTest(unittest.TestLoader().loadTestsFromName(module_name))
                test_module = __import__(module_name, {}, {}, filename[:-3])
                test_suite.addTest(unittest.TestLoader().loadTestsFromModule(test_module))

        return test_suite

    except Exception:
        logging.critical("Error loading tests.", exc_info=1)
        raise SystemExit("Error loading tests.")

if __name__ == "__main__":

    sys.path.insert(0, os.path.join(os.environ['APPENGINEDIR'], 'lib', 'django'))
    os.environ['SERVER_SOFTWARE'] = 'Development/unittest' # to ensure correct reloading of config/FSM

    import sys
    if len(sys.argv) > 1:
        # tests specified
        test_suite = unittest.TestLoader().loadTestsFromNames(sys.argv[1:])
    else:
        test_suite = suite()
    if teamcity.underTeamcity():
        TeamcityTestRunner().run(test_suite)
    else:
        unittest.TextTestRunner(verbosity=int(os.environ.get('UNITTEST_VERBOSITY', 1))).run(test_suite)
