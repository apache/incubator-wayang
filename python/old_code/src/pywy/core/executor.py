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

class Executor:
    """ Executor is the responsible for execute the plan

    Because in each platform the execution it will be different the plan
    need to be executed in the different modes, and the Executor is
    the responsible for execute in the given platform
    """
    def __init__(self):
        pass

    def execute(self, plan):
        """ execute is the method called for execute the givin plan

        Returns
        -------
        does not return anything, but it will differ in some platforms
        """
        pass
