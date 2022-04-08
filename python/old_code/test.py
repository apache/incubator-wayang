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

from pywy.dataquanta import WayangContext
from pywy.plugins import PYTHON

# p = Platform("nana")
# print("LALA "+str(p))
# pt = type(p)
# print(pt)
# p2 = pt("chao")
# print(p2)
# print(type(p2))
#
#
# print(str(WayangContext().register(java, spark)))

#
# predicate : Predicate = lambda x: x % 2 == 0
# getTypePredicate(predicate)
import time
def pre(a:str):
    return "six" in a
#
# def func(s:str) -> int:
#     return len(s)
#
# def fmfunc(i:int) -> str:
#     for x in range(i):
#         yield str(x)

for index in range(0, 1):
    print(index)
    tic = time.perf_counter()
    fileop = WayangContext()\
                .register(PYTHON)\
                .textfile("/Users/bertty/databloom/blossom/python/resources/tmp" + str(index))\
                .filter(pre)\
                .store_textfile("/Users/bertty/databloom/blossom/python/resources/out" + str(index))
    toc = time.perf_counter()
    print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")

# filterop: FilterOperator = fileop.filter(pre).getOperator()
# #fop_pre = filterop.getWrapper()
# #fop_pre_res = fop_pre(["la", "lala"])
# #for i in fop_pre_res:
# #    print(i)
#
#
# mapop: MapOperator = fileop.map(func).getOperator()
# mop_func = mapop.getWrapper()
# mop_func_res = mop_func(["la", "lala"])
# #for i in mop_func_res:
# #    print(i)
#
#
# fmop: FlatmapOperator = fileop.flatmap(fmfunc).getOperator()
# fmop_func = fmop.getWrapper()
# fmop_func_res = fmop_func([2, 3])
# #for i in fmop_func_res:
# #    print(i)
#
# def concatenate(function_a, function_b):
#     def executable(iterable):
#         return function_b(function_a(iterable))
#     return executable
#
# #res = concatenate(concatenate(fop_pre, mop_func), fmop_func)
# #res_pro = res(["la", "lala"])
# #for i in res_pro:
# #    print(i)
#
# from pywy.platforms.python.mappings import PywyOperatorMappings
# from pywy.platforms.python.operators import *
#
# print(PywyOperatorMappings)
#
# pyF = PyFilterOperator()
# print(pyF)
# print(pyF.get_input_channeldescriptors())
# print(type(pyF.get_input_channeldescriptors().pop().create_instance()))
#
# qq : Channel = pyF.get_input_channeldescriptors().pop().create_instance()
# print(qq)
# print(type(qq))
# print("ads")
#
#
# def pre_lala(a:str):
#     print("executed")
#     return len(a) > 3
#
# ou1 = filter(pre_lala, ["la", "lala"])
# print(ou1)
#
# for i in ou1:
#     print(i)
#
# pyFM = PywyOperatorMappings.get_instanceof(filterop)
# print(pyFM)
# print(type(pyFM))