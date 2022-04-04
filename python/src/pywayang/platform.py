
class Platform:
    """
    A platform describes an execution engine that is used for execute the
    wayang plan

    Parameters
    ----------
    name: str
      platform name, it uses as identification
    """

    name : str
    #configuration : dict[str, str]

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "name: {}".format(self.name)

    def __repr__(self):
        return self.__str__()

