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


from itertools import count

class Optimizer:
    _CNT = count(0)

    def __init__(self, learningRate, name=None):
        self.learningRate = learningRate
        if name is not None:
            self.name = name
        else:
            self.name = self.__class__.__name__

    def get_name(self):
        return self.name

    def get_learning_rate(self):
        return self.learningRate

    def to_dict(self):
        return { \
            "name": self.name, \
            "learningRate": self.learningRate, \
        }


class Adam(Optimizer):
    def __init__(self, learningRate, betaOne=0.9, betaTwo=0.999, epsilon=1e-8, name=None):
        super().__init__(learningRate, name)
        self.betaOne = betaOne
        self.betaTwo = betaTwo
        self.epsilon = epsilon

    def get_beta_one(self):
        return self.betaOne

    def get_beta_two(self):
        return self.betaTwo

    def get_epsilon(self):
        return self.epsilon


class GradientDescent(Optimizer):
    def __init__(self, learningRate, name=None):
        super().__init__(learningRate, name)
