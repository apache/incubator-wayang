from typing import Iterable

from pywy.wayangplan.sink import SinkOperator
from pywy.platforms.basic.platform import Platform


class WayangPlan:

    def __init__(self, platforms: Iterable[Platform], sinks: Iterable[SinkOperator]):
        self.platforms = platforms
        self.sinks = sinks
