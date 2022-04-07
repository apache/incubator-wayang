import unittest

from pywy.core.platform import Platform


class TestUnitCorePlatform(unittest.TestCase):
    def setUp(self):
        pass

    def test_create(self):
        name = "platform"
        p = Platform(name)
        self.assertEqual(type(p), Platform)
        self.assertEqual(p.name, name)
