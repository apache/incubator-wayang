#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from pywy.core import Mapping
from pywy.platforms.python.operator import *


PYWY_OPERATOR_MAPPINGS = Mapping()

PYWY_OPERATOR_MAPPINGS.add_mapping(PyFilterOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyTextFileSourceOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyTextFileSinkOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyMapOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyFlatmapOperator())

