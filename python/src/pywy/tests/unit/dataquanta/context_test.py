import unittest
from unittest.mock import Mock

from pywy.dataquanta import WayangContext
from pywy.dataquanta import DataQuanta
from pywy.operators.source import TextFileSource


class TestUnitDataquantaContext(unittest.TestCase):

    def test_create(self):
        context = WayangContext()
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 0)

    def test_set_pluggin(self):
        pluggin = Mock()
        context = WayangContext().register(pluggin)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 1)

    def test_register_bulk_pluggins(self):
        pluggin = Mock()
        pluggin2 = Mock()
        context = WayangContext().register(pluggin, pluggin2)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 2)

    def test_register_one_by_one_pluggins(self):
        pluggin = Mock()
        pluggin2 = Mock()
        context = WayangContext().register(pluggin).register(pluggin2)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 2)

    def test_register_one_two_times_pluggins(self):
        pluggin = Mock()
        context = WayangContext().register(pluggin).register(pluggin)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 1)

    def test_unregister_pluggins(self):
        pluggin = Mock()
        context = WayangContext().register(pluggin)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 1)

        context = context.unregister(pluggin)

        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 0)

    def test_unregister_in_bulk_pluggins(self):
        pluggin = Mock()
        pluggin2 = Mock()
        pluggin3 = Mock()
        context = WayangContext().register(pluggin, pluggin2, pluggin3)
        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 3)

        context = context.unregister(pluggin,pluggin3)

        self.assertIsInstance(context, WayangContext)
        self.assertIsInstance(context.plugins, set)
        self.assertEqual(len(context.plugins), 1)
        self.assertEqual(context.plugins.pop(), pluggin2)

    def test_textfile_withoutPlugin(self):
        path = Mock()
        context = WayangContext()
        self.assertIsInstance(context, WayangContext)
        self.assertEqual(len(context.plugins), 0)

        dataQuanta = context.textfile(path)

        self.assertIsInstance(dataQuanta, DataQuanta)
        self.assertIsInstance(dataQuanta.operator, TextFileSource)
        self.assertEqual(context, dataQuanta.context)