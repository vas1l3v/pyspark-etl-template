import unittest

class etlTest(unittest.TestCase):

    def test_etl(self):
        for i in range(0, 5):
            self.assertEqual(i, i)

if __name__ == '__main__':
    unittest.main()