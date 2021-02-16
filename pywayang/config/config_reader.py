import configparser
import os


def get_boundary_types():
    config = configparser.ConfigParser()
    config.sections()
    config.read('../config/pywayang_config.ini')
    boundary_types = dict(config.items('BOUNDARY_TYPES'))
    boundary_types.pop("variable_to_access")
    return boundary_types.values()


def get_source_types():
    config = configparser.ConfigParser()
    #print("path: ", os.getcwd())
    config.read("../config/pywayang_config.ini")
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
    config.read("../config/pywayang_config.ini")
    sink_types = dict(config.items('SINK_TYPES'))
    sink_types.pop("variable_to_access")
    return sink_types.values()