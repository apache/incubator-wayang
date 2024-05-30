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


class Option:
    def __init__(self, criterion, optimizer, batch_size, epoch):
        self.criterion = criterion
        self.optimizer = optimizer
        self.batch_size = batch_size
        self.epoch = epoch
        self.accuracy_calculation = None

    def set_accuracy_calculation(self, accuracy_calculation):
        self.accuracy_calculation = accuracy_calculation

    def get_criterion(self):
        return self.criterion

    def get_optimizer(self):
        return self.optimizer

    def get_batch_size(self):
        return self.batch_size

    def get_epoch(self):
        return self.epoch

    def get_accuracy_calculation(self):
        return self.accuracy_calculation

    def to_dict(self):
        result = { \
            "optimizer": self.get_optimizer().to_dict(), \
            "criterion": self.get_criterion().to_dict(), \
            "batchSize": self.get_batch_size(), \
            "epoch": self.get_epoch() }

        if self.accuracy_calculation is not None:
            result["accuracy_calculation"] = self.get_accuracy_calculation().to_dict()

        return result
