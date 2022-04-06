from typing import Set

from pywy.platforms.basic.executor import Executor
from pywy.platforms.basic.platform import Platform
from pywy.platforms.basic.mapping import Mapping

class Plugin:
    """
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms: Set[Platform]
    mappings: Mapping

    def __init__(self, platforms:Set[Platform], mappings: Mapping = Mapping()):
        self.platforms = platforms
        self.mappings = mappings

    def get_mappings(self) -> Mapping:
        return self.mappings

    def get_executor(self) -> Executor:
        pass

    def __str__(self):
        return "Platforms: {}, Mappings: {}".format(str(self.platforms), str(self.mappings))

    def __repr__(self):
        return self.__str__()


