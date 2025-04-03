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

from typing import Any, List


class Record:
    """
    A Type that represents a record with a schema.
    """
    values: List[Any]

    def __init__(self, values: List[Any]) -> None:
        self.values = values

    def copy(self) -> 'Record':
        return Record(self.values.copy())

    def equals(self, o: Any) -> bool:
        if o is None or type(self) != type(o):
            return False

        return self.values == o.values

    def hash_code(self) -> int:
        return hash(self.values)

    def get_field(self, index: int) -> Any:
        return self.values[index]

    def get_double(self, index: int) -> float:
        return float(self.values[index])

    def get_int(self, index: int) -> int:
        return int(self.values[index])

    def get_string(self, index: int) -> str:
        return str(self.values[index])

    def set_field(self, index: int, field: Any) -> None:
        self.values[index] = field

    def add_field(self, field: Any) -> None:
        self.values.append(field)

    def size(self) -> int:
        return len(self.values)

    def __str__(self):
        return str(self.values).replace("[", "").replace("]", "").replace(" ", "")

    def __repr__(self):
        return self.__str__()
