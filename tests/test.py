import sys
sys.path.append('..')
import unittest

from dependencies.spark import start_spark


class SparkETLTests(unittest.TestCase):
    
    def setUp(self):
        self.spark, self.log = start_spark('test')
        
    def tearDown(self):
        self.spark.stop()
        
    def test_setup(self):
        self.assertIsNotNone(self.spark)
        self.assertIsNotNone(self.log)
        
        
if __name__ == '__main__':
    unittest.main()