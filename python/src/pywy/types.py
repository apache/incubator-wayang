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

from typing import (Generic, TypeVar, Callable, Hashable, Iterable, Type, Union, Tuple, get_args, get_origin, List, Dict, Any)
from inspect import signature
from numpy import int32, int64, float32, float64, ndarray
import re

from pywy.exception import PywyException

T = TypeVar("T")      # Type
In = TypeVar("In")    # Input Type number 1
In2 = TypeVar("In2")  # Input Type number 2
Out = TypeVar("Out")  # Output Type

IterableT = Iterable[T]      # Iterable of type 'T'
IterableOut = Iterable[Out]  # Iterable of type 'O'
IterableIn = Iterable[In]    # Iterable of type 'O'
ListT = List[T]              # List of type T

T_co = TypeVar("T_co", covariant=True)
U_co = TypeVar("U_co", covariant=True)
K = TypeVar("K", bound=Hashable)

GenericTco = Generic[T_co]
GenericUco = Generic[U_co]

PrimitiveType = Union[bool, float, int, str]

NumberOrArray = TypeVar(
    "NumberOrArray", float, int, complex, int32, int64, float32, float64, ndarray
)

ConstrainedOperatorType = Union[PrimitiveType, NumberOrArray, IterableT, ListT]

Predicate = Callable[[ConstrainedOperatorType], bool]
Function = Callable[[ConstrainedOperatorType], ConstrainedOperatorType]
BiFunction = Callable[[ConstrainedOperatorType, ConstrainedOperatorType], ConstrainedOperatorType]

FlatmapFunction = Callable[[ConstrainedOperatorType], Iterable[ConstrainedOperatorType]]

"""
    List[List[int]]
    [[1,2,3],[4,5,6]]
    {origin: int, depth: 2}

    List[int]
    [1,2,3]
    {origin: int, depth: 1}

    Tuple[List[int]]
    ([1,2,3], [1,2,3])
    {origin: int, depth: 2}

    int
    1
    {origin: int, depth: 0}
"""
class NDimArray:
    origin: Type
    depth: int

    def __init__(self, origin: Type, depth: int):
        self.origin = origin
        self.depth = depth

    def __str__(self) -> str:
        return f"NDimArray: \n\t- origin: {self.origin.__name__}\n\t- depth: {self.depth}"

    def to_json(self) -> dict:
        return {"origin": get_java_type(self.origin), "depth": self.depth}

def ndim_from_type(py_type: ConstrainedOperatorType, depth: int = 0) -> NDimArray:
    # Handle basic types and direct typing module classes
    if hasattr(py_type, '__name__'):
        return NDimArray(py_type, depth)

    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle generic types
    if origin:
        if origin is tuple and args:
            return NDimArray(tuple, depth + 1)
        if args:
            return ndim_from_type(args[0], depth + 1)
        return NDimArray(py_type, depth + 1)

    return NDimArray(py_type, depth)

#Define the mappings
type_mappings: Dict[Type, str] = {
    'int': 'Integer',
    'float': 'Float',
    'str': 'String',
    'bool': 'Boolean',
    'list': 'Array',
    'List': 'Array',
    'dict': 'Map',
    'Dict': 'Map',
    'tuple': 'Tuple',
    'Tuple': 'Tuple',
    'Any': 'Object'
}

def get_type_predicate(call: Predicate) -> type:
    sig = signature(call)
    if len(sig.parameters) != 1:
        raise PywyException(
            "the parameters for the Predicate are distinct than one, {}".format(
                str(sig.parameters)
            )
        )

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation


def get_type_function(call: Function) -> (type, type):
    sig = signature(call)
    if len(sig.parameters) != 1:
        raise PywyException(
            "the parameters for the Function are distinct than one, {}".format(
                str(sig.parameters)
            )
        )

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.return_annotation


def get_type_bifunction(call: BiFunction) -> (type, type, type):
    sig = signature(call)
    if len(sig.parameters) != 2:
        raise PywyException(
            "the parameters for the BiFunction are distinct than two, {}".format(
                str(sig.parameters)
            )
        )

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.parameters[keys[1]].annotation, sig.return_annotation


def get_type_flatmap_function(call: FlatmapFunction) -> (type, type):
    sig = signature(call)
    print(sig.parameters)
    if len(sig.parameters) != 1:
        raise PywyException(
            "the parameters for the FlatmapFunction are distinct than one, {}".format(
                str(sig.parameters)
            )
        )

    if type(sig.return_annotation) != type(Iterable):
        raise PywyException(
            "the return for the FlatmapFunction is not Iterable, {}".format(
                str(sig.return_annotation)
            )
        )

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.return_annotation.__args__[0]

def typecheck(input_type: Type[ConstrainedOperatorType]):
    allowed_types = get_args(ConstrainedOperatorType)
    print(allowed_types)
    print(input_type)
    if input_type in allowed_types or input_type is None:
        return

    origin = get_origin(input_type)
    args = get_args(input_type)

    print(origin)
    print(args)

    if isinstance(input_type, List) and args:
        typecheck(args[0])
    elif isinstance(input_type, Tuple):
        if all(arg in allowed_types for arg in args):
            return
        else:
            raise TypeError(f"Unsupported Operator type: {input_type}")
    else:
    #print(get_args(ConstrainedOperatorType))
    #if candT not in get_args(ConstrainedOperatorType) and candT is not None:
    #if candT is not PrimitiveType and candT is not IterableT and candT is not NumberOrArray and candT is not None:
        raise TypeError(f"Unsupported Operator type: {input_type}, {args}, {isinstance(input_type, Tuple)}")

def get_java_type(input_type: ConstrainedOperatorType) -> str:
    str_type = get_type_str(input_type)

    py_type = str_type.replace("typing.", "")
    return convert_type(py_type)

def convert_type(py_type: str) -> str:
    # Regex to find generic types like List[float], Dict[str, int], etc.
    generic_type_pattern = re.compile(r'(\w+)\[(.+)\]')

    match = generic_type_pattern.match(py_type)
    if match:
        base_type, inner_types = match.groups()
        # Convert inner types (e.g., float in List[float])
        converted_inner_types = ', '.join([convert_type(inner.strip()) for inner in inner_types.split(',')])
        return f"{type_mappings.get(base_type, base_type)}[{converted_inner_types}]"
    else:
        return type_mappings.get(py_type, py_type)

def get_type_str(py_type: Any) -> str:
    # Handle basic types and direct typing module classes
    if hasattr(py_type, '__name__'):
        return py_type.__name__

    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle generic types
    if origin:
        origin_str = get_type_str(origin)
        if args:
            args_str = ', '.join(get_type_str(arg) for arg in args)
            return f"{origin_str}[{args_str}]"
        return origin_str

    return str(py_type)
