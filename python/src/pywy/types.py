from typing import (Generic, TypeVar, Callable, Hashable, Iterable)
from inspect import signature
from pywy.exception import PywyException

T = TypeVar("T")      # Type
In = TypeVar("In")    # Input Type number 1
In2 = TypeVar("In2")  # Input Type number 2
Out = TypeVar("Out")  # Output Type

IterableT = Iterable[T]      # Iterable of type 'T'
IterableOut = Iterable[Out]  # Iterable of type 'O'
IterableIn = Iterable[In]    # Iterable of type 'O'

T_co = TypeVar("T_co", covariant=True)
U_co = TypeVar("U_co", covariant=True)
K = TypeVar("K", bound=Hashable)

GenericTco = Generic[T_co]
GenericUco = Generic[U_co]

Predicate = Callable[[T], bool]
Function = Callable[[In], Out]
BiFunction = Callable[[In, In2], Out]

FlatmapFunction = Callable[[T], IterableOut]


def get_type_predicate(call: Predicate) -> type:
    sig = signature(call)
    if len(sig.parameters) != 1:
        raise PywyException("the parameters for the Predicate are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation


def get_type_function(call: Function) -> (type, type):
    sig = signature(call)
    if len(sig.parameters) != 1:
        raise PywyException("the parameters for the Function are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.return_annotation


def get_type_bifunction(call: BiFunction) -> (type, type, type):
    sig = signature(call)
    if len(sig.parameters) != 2:
        raise PywyException("the parameters for the BiFunction are distinct than two, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.parameters[keys[1]].annotation, sig.return_annotation


def get_type_flatmap_function(call: FlatmapFunction) -> (type, type):
    sig = signature(call)
    if len(sig.parameters) != 1:
        raise PywyException("the parameters for the FlatmapFunction are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation, sig.return_annotation
