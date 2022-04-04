from typing import ( Generic, TypeVar, Callable, Hashable, Iterable)
from inspect import signature

T = TypeVar("T")   # Type
I = TypeVar("I")   # Input Type number 1
I2 = TypeVar("I2") # Input Type number 2
O = TypeVar("O")   # Output Type

IterableT = Iterable[T] # Iterable of type 'T'
IterableO = Iterable[O] # Iterable of type 'O'

T_co = TypeVar("T_co", covariant=True)
U_co = TypeVar("U_co", covariant=True)
K = TypeVar("K", bound=Hashable)

GenericTco = Generic[T_co]
GenericUco = Generic[U_co]

Predicate = Callable[[T], bool]
Function = Callable[[I], O]
BiFunction = Callable[[I, I2], O]
Function = Callable[[I], O]

FlatmapFunction = Callable[[T], IterableO]


def getTypePredicate(callable: Predicate) -> Generic :
    sig = signature(callable)
    if(len(sig.parameters) != 1):
        raise Exception("the parameters for the Predicate are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return sig.parameters[keys[0]].annotation

def getTypeFunction(callable: Function) -> Generic :
    sig = signature(callable)
    if(len(sig.parameters) != 1):
        raise Exception("the parameters for the Function are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return (sig.parameters[keys[0]].annotation, sig.return_annotation)

def getTypeBiFunction(callable: BiFunction) -> (Generic, Generic, Generic) :
    sig = signature(callable)
    if(len(sig.parameters) != 2):
        raise Exception("the parameters for the BiFunction are distinct than two, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return (sig.parameters[keys[0]].annotation, sig.parameters[keys[1]].annotation, sig.return_annotation)

def getTypeFlatmapFunction(callable: FlatmapFunction) -> (Generic, Generic) :
    sig = signature(callable)
    if(len(sig.parameters) != 1):
        raise Exception("the parameters for the FlatmapFunction are distinct than one, {}".format(str(sig.parameters)))

    keys = list(sig.parameters.keys())
    return (sig.parameters[keys[0]].annotation, sig.return_annotation)