"""This module provideas a convienance method to run all of the tests for PVGeo.
Each suite within PVGeo has its own ``*_test.py`` file for all unittest
implemenations to live and each file is executable on its own.
"""

__all__ = [
    'test',
]

import unittest
import glob
import os
import sys


def lookUpSuite(name):
    """Looks up the suites test files by name"""
    if name is None:
        return None
    SUITES = {
                'filters': ['filters_test.py'],
                'grids': ['grids_test.py'],
                'gslib': ['gslib_test.py'],
                'helpers': ['helpers_test.py'],
                'model_build': ['model_build_test.py'],
                'readers': ['readers_test.py'],
                'ubc': ['ubc_test.py'],
              }
    if isinstance(name, str):
        return SUITES[name]
    else:
        raise RuntimeError('Suite name `%s` not understood.' % name)


def test(close=False, suite=None):
    """This is a convienance method to run all of the tests in ``PVGeo`` while
    in an active python environment.

    Args:
        close (bool): exit the python environment with error code if errors or failures occur
        suite (str): the suite to test
    """
    try:
        from colour_runner.runner import ColourTextTestRunner as TextTestRunner
    except ImportError:
        from unittest import TextTestRunner
    os.chdir(os.path.dirname(__file__))
    test_file_strings = lookUpSuite(suite)
    if test_file_strings is None:
        test_file_strings = glob.glob('*_test.py')
    module_strings = [str[0:len(str)-3] for str in test_file_strings]
    suites = [unittest.defaultTestLoader.loadTestsFromName(str) for str
              in module_strings]
    testSuite = unittest.TestSuite(suites)

    #unittest.TextTestRunner(verbosity=2).run(testSuite)
    run = TextTestRunner(verbosity=2).run(testSuite)
    if close:
        exit(len(run.failures) > 0 or len(run.errors) > 0)
    return run


if __name__ == '__main__':
    close = False
    suite = None
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.lower() == 'close':
            close = True
        else:
            raise RuntimeError('Unknown argument: %s' % arg)
        if len(sys.argv) == 3:
            suite = sys.argv[2]
    test(close=close, suite=suite)
