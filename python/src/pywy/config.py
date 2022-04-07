import logging
import os
import pkg_resources

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logger = logging.getLogger(__name__)


def if_environ(key: str, default_value: str) -> str:
    return os.environ[key] if key in os.environ else os.path.abspath(default_value)


BASE_DIR = pkg_resources.resource_filename("pywy", "")
RC_DIR = if_environ("PYWY_RC_HOME", os.path.expanduser("~/.pywy"))
RC_TEST_DIR = if_environ("PYWY_RC_TEST_HOME", "{}/tests/resources".format(BASE_DIR))
#RC_TEST_OUT_DIR = if_environ("PYWY_RC_TEST_OUT_HOME", "{}/../../output".format(BASE_DIR))

logger.info(" Environment variables")
logger.info(" ############################")
logger.info(f" ## {BASE_DIR=}")
logger.info(f" ## {RC_DIR=}")
logger.info(f" ## {RC_TEST_DIR=}")
#logger.info(f" ## {RC_TEST_OUT_DIR=}")
logger.info(" ############################")
# print(HOME_DIR)
