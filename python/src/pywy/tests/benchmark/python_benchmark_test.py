import os
import tempfile
import unittest
import time
import logging

from typing import List
from pywy.dataquanta import WayangContext
from pywy.plugins import PYTHON
from pywy.config import RC_TEST_DIR as ROOT
from pywy.config import RC_BENCHMARK_SIZE

logger = logging.getLogger(__name__)


class TestBenchmarkPythonGrep(unittest.TestCase):
    file_grep: List[str]

    def setUp(self):
        full_list = [
            "{}/10e0MB.input".format(ROOT),
            "{}/10e1MB.input".format(ROOT),
            "{}/10e2MB.input".format(ROOT),
            "{}/10e3MB.input".format(ROOT),
            "{}/10e4MB.input".format(ROOT),
            "{}/10e5MB.input".format(ROOT),
        ]
        self.file_grep = full_list[:RC_BENCHMARK_SIZE]


    @staticmethod
    def grep_python(path):
        def pre(a: str) -> bool:
            return 'six' in a

        fd, path_tmp = tempfile.mkstemp()

        tic = time.perf_counter()
        WayangContext() \
            .register(PYTHON) \
            .textfile(path) \
            .filter(pre) \
            .store_textfile(path_tmp)
        toc = time.perf_counter()

        os.remove(path_tmp)
        return tic, toc

    @staticmethod
    def grep_so(path):
        fd, path_tmp = tempfile.mkstemp()

        tic = time.perf_counter()
        os.system('grep "six" {} >> {}'.format(path, path_tmp))
        toc = time.perf_counter()

        os.remove(path_tmp)
        return tic, toc

    def test_grep(self):
        for path in self.file_grep:
            (tic, toc) = self.grep_python(path)
            logger.info(f"Python-Platform time for the file {path} {toc - tic:0.4f} seconds")
            (tic, toc) = self.grep_so(path)
            logger.info(f"Operative System time for the file {path} {toc - tic:0.4f} seconds")
