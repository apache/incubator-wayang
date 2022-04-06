from pywy.platforms.basic.plugin import Plugin
from pywy.dataquanta import DataQuanta
from pywy.wayangplan.source import TextFileSource

class WayangContext:
  """
  This is the entry point for users to work with Wayang.
  """
  def __init__(self):
    self.plugins = set()

  """
  add a :class:`Plugin` to the :class:`Context`
  """
  def register(self, *p: Plugin):
    self.plugins.add(p)
    return self

  """
  remove a :class:`Plugin` from the :class:`Context`
  """
  def unregister(self, p: Plugin):
    self.plugins.remove(p)
    return self

  def textFile(self, file_path: str) -> DataQuanta[str]:
    return DataQuanta(TextFileSource(file_path))


  def __str__(self):
    return "Plugins: {} \n".format(str(self.plugins))

  def __repr__(self):
    return self.__str__()

