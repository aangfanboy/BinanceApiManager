from tests import DataFetchersTest

import unittest
import logging

def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_tests():
    suite = unittest.TestLoader().loadTestsFromModule(DataFetchersTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
    

if __name__ == '__main__':
    setup_logging()
    run_tests()

# Path: runTests.py