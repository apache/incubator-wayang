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

from typing import Dict, Type
from pywy.operators.base import PywyOperator


class Mapping:
    """Mapping between :py:class:`pywy.operators.base.PywyOperator` and the executable version in a platform

    Mapping is the structure that keep the conversion between :py:class:`pywy.operators.base.PywyOperator`
    and the executable version of the same operator in a different platforms

    Attributes
    ----------
    mappings : :obj:`dict`
       Mapping using the name as key to retrieve the executable operator

    """
    mappings: Dict[str, Type]

    def __init__(self):
        """
        Just instance of the :obj:`dict` to store the mappings
        """
        self.mappings = {}

    def add_mapping(self, operator: PywyOperator):
        """create the mapping for the instance of :py:class:`pywy.operators.base.PywyOperator`

        Parameters
        ----------
        operator : :py:class:`pywy.operators.base.PywyOperator`
            instance of :py:class:`pywy.operators.base.PywyOperator` that will be used to extract the
            properties required to create the mapping
        """
        self.mappings[operator.name_basic()] = type(operator)

    def get_instanceof(self, operator: PywyOperator, **kwargs):
        """Instance the executable version of :py:class:`pywy.operators.base.PywyOperator`

        Parameters
        ----------
        operator : :py:class:`pywy.operators.base.PywyOperator`
            instance of the :py:class:`pywy.operators.base.PywyOperator` that needs to be
            converted to the executable version
        Returns
        -------
            executable version of :py:class:`pywy.operators.base.PywyOperator` in the
            platform that the mapping is holding
        """
        template = self.mappings[operator.name_basic()]
        if template is None:
            raise Exception(
                "the operator {} does not have valid mapping".format(
                    operator.name
                )
            )
        return template(operator, **kwargs)

    def __str__(self):
        return str(self.mappings)

    def __repr__(self):
        return self.__str__()
