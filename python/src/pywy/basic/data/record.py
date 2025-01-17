from typing import List, cast

from python.src.pywy.types import GenericTco, ListT, T


class Record:
    """
    A Type that represents a record with a schema.
    """
    values: ListT

    def __init__(self, values: ListT) -> None:
        self.values = values

    def copy(self) -> 'Record':
        return Record(self.values.copy())

    def equals(self, o: T) -> bool:
        if self == 0:
            return True
        if o is None or type(self) != type(o):
            return False

        record_2 = cast(Record, o)
        return self.values == o.values

    def hash_code(self) -> int:
        return hash(self.values)

    def get_field(self, index: int) -> T:
        return self.values[index]

    def get_double(self, index: int) -> float:
        return float(self.values[index])

    def get_int(self, index: int) -> int:
        return int(self.values[index])

    def get_string(self, index: int) -> str:
        return str(self.values[index])

    def set_field(self, index: int, field: T):
        self.values[index] = field

    def addField(self, field: T):
        self.values.append(field)

    def size(self) -> int:
        return len(self.values)

    def __str__(self):
        return "Record" + str(self.values)

    def __repr__(self):
        return self.__str__()
