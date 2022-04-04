from pywayang.platform import Platform

class Plugin:
    """
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms = []

    def __init__(self, *platform:Platform):
        self.platforms = list(platform)

    def __str__(self):
        return "Platforms: {}".format(str(self.platforms))

    def __repr__(self):
        return self.__str__()


# define the basic plugins that can be used
java = Plugin(Platform('java'))
spark = Plugin(Platform('spark'))
flink = Plugin(Platform('flink'))
