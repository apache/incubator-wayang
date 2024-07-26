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

from typing import List
from itertools import count


class Op:
    CNT = count(0)

    class DType:
        ANY = 'ANY'
        INT32 = 'INT32'
        INT64 = 'INT64'
        FLOAT32 = 'FLOAT32'
        FLOAT64 = 'FLOAT64'
        BYTE = 'BYTE'
        INT16 = 'INT16'
        BOOL = 'BOOL'

    def __init__(self, dType: DType, name=None, opType=None):
        if name is None:
            self.name = self.__class__.__name__
        else:
            self.name = name
        self.fromList: List[Op] = []
        self.dType = dType
        self.opType = opType

    def get_name(self):
        return self.name

    def get_dType(self):
        return self.dType

    def get_fromList(self):
        return self.fromList

    def with_ops(self, *ops):
        assert not self.fromList
        assert len(ops) == self.inputs_required()
        for op in ops:
            assert self.name != op.name
        self.fromList.extend(ops)
        return self

    def inputs_required(self):
        pass

    def to_dict(self):
        output = {}
        output['op'] = self.name
        output['opType'] = self.opType
        output['dType'] = self.dType
        output['fromList'] = list(map(lambda child: child.to_dict(),self.fromList))
        output["dim"] = None
        output["labels"] = None
        output["inFeatures"] = None
        output["outFeatures"] = None
        output["bias"] = None

        if hasattr(self, "dim"):
            output["dim"] = self.dim

        if hasattr(self, "labels"):
            output["labels"] = self.labels

        if hasattr(self, "inFeatures"):
            output["inFeatures"] = self.inFeatures

        if hasattr(self, "outFeatures"):
            output["outFeatures"] = self.outFeatures

        if hasattr(self, "bias"):
            output["bias"] = self.bias

        return output



class ArgMax(Op):
    def __init__(self, dim, name=None):
        super().__init__(Op.DType.INT32, name)
        self.dim = dim

    def get_dim(self):
        return self.dim

    def inputs_required(self):
        return 1


class Cast(Op):
    def __init__(self, dType, name=None):
        super().__init__(dType, name)

    def inputs_required(self):
        return 1


class Eq(Op):
    def __init__(self, name=None):
        super().__init__(Op.DType.BOOL, name)

    def inputs_required(self):
        return 2


class Input(Op):
    class Type:
        FEATURES = "..FEATURES.."
        LABEL = "..LABEL.."
        PREDICTED = "..PREDICTED.."

        def __init__(self, name):
            self.name = name

        def get_name(self):
            return self.name

    def __init__(self, opType=None, dType=Op.DType.FLOAT32, name=None):
        if opType is not None:
            super().__init__(dType=dType, opType=opType)
        else:
            super().__init__(dType=dType, name=name)

    def inputs_required(self):
        return 0


class Mean(Op):
    def __init__(self, dim, name=None):
        super().__init__(Op.DType.FLOAT32, name)
        self.dim = dim

    def get_dim(self):
        return self.dim

    def get_dType(self):
        if self.fromList and self.fromList[0].get_dType() == Op.DType.FLOAT64:
            return Op.DType.FLOAT64
        return Op.DType.FLOAT32

    def inputs_required(self):
        return 1


class CrossEntropyLoss(Op):
    def __init__(self, labels, name=None):
        super().__init__(Op.DType.FLOAT32, name)
        self.labels = labels

    def get_labels(self):
        return self.labels

    def get_dType(self):
        if self.fromList and self.fromList[0].get_dType() == Op.DType.FLOAT64:
            return Op.DType.FLOAT64
        return Op.DType.FLOAT32

    def inputs_required(self):
        return 2


class Linear(Op):
    def __init__(self, inFeatures, outFeatures, bias, name=None, dType=Op.DType.FLOAT32):
        super().__init__(dType, name)
        self.inFeatures = inFeatures
        self.outFeatures = outFeatures
        self.bias = bias

    def get_in_features(self):
        return self.inFeatures

    def get_out_features(self):
        return self.outFeatures

    def get_bias(self):
        return self.bias

    def inputs_required(self):
        return 1


class ReLU(Op):
    def __init__(self, name=None):
        super().__init__(Op.DType.FLOAT32, name)

    def get_dType(self):
        if self.fromList:
            return self.fromList[0].get_dType()
        return Op.DType.FLOAT32

    def inputs_required(self):
        return 1


class Sigmoid(Op):
    def __init__(self, name=None):
        super().__init__(Op.DType.FLOAT32, name)

    def get_dType(self):
        if self.fromList and self.fromList[0].get_dType() == Op.DType.FLOAT64:
            return Op.DType.FLOAT64
        return Op.DType.FLOAT32

    def inputs_required(self):
        return 1


class Softmax(Op):
    def __init__(self, name=None):
        super().__init__(Op.DType.FLOAT32, name)

    def get_dType(self):
        if self.fromList and self.fromList[0].get_dType() == Op.DType.FLOAT64:
            return Op.DType.FLOAT64
        return Op.DType.FLOAT32

    def inputs_required(self):
        return 1
