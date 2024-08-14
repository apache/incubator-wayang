#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import configparser
import os

path = '/var/www/html/pywayang/src/pywy'
config_path = path + '/config/pywayang_config.ini'

def get_boundary_types():
    print(os.getcwd())
    config = configparser.ConfigParser()
    config.sections()
    config.read(config_path)
    #config.read('../config/pywayang_config.ini')
    boundary_types = dict(config.items('BOUNDARY_TYPES'))
    boundary_types.pop("variable_to_access")
    return boundary_types.values()


def get_source_types():
    config = configparser.ConfigParser()
    #print("path: ", os.getcwd())
    config.read(config_path)
    source_types = dict(config.items('SOURCE_TYPES'))
    source_types.pop("variable_to_access")
    return source_types.values()
    #sections_list = config.sections()
    #for section in sections_list:
    #    print(section)
    #print("source_types")
    #for x in source_types.values():
    #    print(x)

def get_sink_types():
    config = configparser.ConfigParser()
    #print("path: ", os.getcwd())
    config.read(config_path)
    sink_types = dict(config.items('SINK_TYPES'))
    sink_types.pop("variable_to_access")
    return sink_types.values()
