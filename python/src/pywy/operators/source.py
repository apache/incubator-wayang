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

from typing import List, Optional

from pywy.operators.base import PywyOperator


class SourceUnaryOperator(PywyOperator):

    def __init__(self, name: str):
        super(SourceUnaryOperator, self).__init__(
            name=name,
            input_type=None,
            output_type=str,
            cat="input",
            input_length=0,
            output_length=1
        )

    def postfix(self) -> str:
        return 'Source'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class TextFileSource(SourceUnaryOperator):
    path: str
    json_name: str

    def __init__(self, path: str):
        super(TextFileSource, self).__init__('TextFile')
        self.path = path
        self.json_name = "textFileInput"

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class ParquetSource(SourceUnaryOperator):
    path: str
    projection: Optional[List[str]]
    column_names: Optional[List[str]]
    json_name: str

    def __init__(self, path: str, projection: Optional[List[str]] = None, column_names: Optional[List[str]] = None):
        super(ParquetSource, self).__init__('Parquet')
        self.path = path
        self.projection = projection
        self.column_names = column_names
        self.json_name = "parquetInput"

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
