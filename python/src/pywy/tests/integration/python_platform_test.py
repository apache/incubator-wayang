import os
import unittest
import tempfile
from os import fdopen
from typing import List

from pywy.config import RC_TEST_DIR as ROOT
from pywy.dataquanta import WayangContext
from pywy.plugins import PYTHON


class TestIntegrationPythonPlatform(unittest.TestCase):

    file_10e0: str

    def setUp(self):
        self.file_10e0 = "{}/10e0MB.input".format(ROOT)
        pass

    def test_grep(self):
        def pre(a: str) -> bool:
            return 'six' in a

        fd, path_tmp = tempfile.mkstemp()

        WayangContext() \
            .register(PYTHON) \
            .textfile(self.file_10e0) \
            .filter(pre) \
            .store_textfile(path_tmp)

        lines_filter: List[str]
        with open(self.file_10e0, 'r') as f:
            lines_filter = list(filter(pre, f.readlines()))
            selectivity = len(list(lines_filter))

        lines_platform: List[str]
        with open(path_tmp, 'r') as fp:
            lines_platform = fp.readlines()
            elements = len(lines_platform)
        os.remove(path_tmp)

        self.assertEqual(selectivity, elements)
        self.assertEqual(lines_filter, lines_platform)
