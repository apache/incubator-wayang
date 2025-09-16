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
from pywy.basic.model.ops import Op


class Model:
    pass


class DLModel(Model):
    def __init__(self, out: Op):
        self.out = out

    def get_out(self):
        return self.out

class LogisticRegression(Op):
    def __init__(self, name=None):
        super().__init__(Op.DType.FLOAT32, name)


class DecisionTreeRegression(Op):
    def __init__(self, max_depth: int = 5, min_instances: int = 2, name=None):
        super().__init__(Op.DType.FLOAT32, name)
        self.max_depth = max_depth
        self.min_instances = min_instances


class LinearSVC(Op):
    def __init__(self, max_iter: int = 10, reg_param: float = 0.1, name=None):
        super().__init__(Op.DType.FLOAT32, name)
        self.max_iter = max_iter
        self.reg_param = reg_param

