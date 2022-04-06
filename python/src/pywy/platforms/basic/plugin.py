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

    platforms = []
    mappings: Mapping

    def __init__(self, *platform:Platform, mappings: Mapping = Mapping()):
        self.platforms = list(platform)
        self.mappings = mappings

    def get_mappings(self) -> Mapping:
        return self.mappings

    def __str__(self):
        return "Platforms: {}".format(str(self.platforms))

    def __repr__(self):
        return self.__str__()


