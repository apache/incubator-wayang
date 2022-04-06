from pywy.platforms.basic.plugin import Plugin
from pywy.wayangplan.wayang import PywyPlan
from pywy.platforms.basic.mapping import Mapping

class Translator:

    def __init__(self, plugin: Plugin, plan: PywyPlan):
        self.plugin = plugin
        self.plan = plan

    def translate(self):
        mappings:Mapping = self.plugin.get_mappings()
