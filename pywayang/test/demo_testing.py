import unittest


class MyTestCase(unittest.TestCase):

    def test_something(self):
        self.assertEqual(True, False)

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')


if __name__ == '__main__':
    unittest.main()
