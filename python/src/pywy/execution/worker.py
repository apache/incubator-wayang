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

import os
import socket
import struct
import base64
import re
import sys
import ast
import copy
import pickle
import cloudpickle


class SpecialLengths(object):
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    res = struct.unpack("!i", length)[0]
    return res


class UTF8Deserializer:
    """
    Deserializes streams written by String.getBytes.
    """

    def __init__(self, use_unicode=True):
        self.use_unicode = use_unicode

    def loads(self, stream):
        length = read_int(stream)
        if length == SpecialLengths.END_OF_DATA_SECTION:
            raise EOFError
        elif length == SpecialLengths.NULL:
            return None
        s = stream.read(length)
        return s.decode("utf-8") if self.use_unicode else s

    def load_stream(self, stream):
        try:
            while True:
                yield self.loads(stream)
        except struct.error:
            return
        except EOFError:
            return

    def __repr__(self):
        return "UTF8Deserializer(%s)" % self.use_unicode


def write_int(p, outfile):
    outfile.write(struct.pack("!i", p))


def write_with_length(obj, stream):
    serialized = obj.encode('utf-8')
    if serialized is None:
        raise ValueError("serialized value should not be None")
    if len(serialized) > (1 << 31):
        raise ValueError("can not serialize object larger than 2G")
    write_int(len(serialized), stream)
    stream.write(serialized)


def dump_stream(iterator, stream):
    for obj in iterator:
        if type(obj) is str:
            write_with_length(obj, stream)
        else:
            write_with_length(str(obj), stream)
        ## elif type(obj) is list:
        ##    write_with_length(obj, stream)
    write_int(SpecialLengths.END_OF_DATA_SECTION, stream)

def m1_func(iterator):
    return map(lambda x: int(x), iterator)

def m2_func(iterator):
    return map(lambda x: int(x) + 1, iterator)

def filter_func(iterator):
    return filter(lambda x: int(x) < 12, iterator)

def process(infile, outfile):
    udf_length = read_int(infile)
    serialized_udf = infile.read(udf_length)
    decoded_udf = base64.b64decode(serialized_udf)
    func = pickle.loads(decoded_udf)
    """
    bfunc = cloudpickle.dumps(lambda x: int(x) < 12)
    decoded_func = base64.b64encode(bfunc)
    print(decoded_func)
    decoded_udf = base64.b64decode(decoded_func)
    func = pickle.loads(decoded_udf)
    """
    iterator = UTF8Deserializer().load_stream(infile)
    print(iterator)
    out_iter = func(iterator)
    dump_stream(iterator=out_iter, stream=outfile)

def local_connect(port):
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo("127.0.0.1", port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            # sock.settimeout(int(os.environ.get("SPARK_AUTH_SOCKET_TIMEOUT", 15)))
            sock.settimeout(30)
            sock.connect(sa)
            # sockfile = sock.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
            sockfile = sock.makefile("rwb", 65536)
            # _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = str(e)
            errors.append("tried to connect to %s, but an error occurred: %s" % (sa, emsg))
            sock.close()
            sock = None
    raise Exception("could not open socket: %s" % errors)


if __name__ == '__main__':
    print("Python version")
    print(sys.version)
    """
    bfunc = cloudpickle.dumps(m1_func)
    decoded_func = base64.b64encode(bfunc)
    print(decoded_func)
    bfunc = cloudpickle.dumps(m2_func)
    decoded_func = base64.b64encode(bfunc)
    print(decoded_func)
    bfunc = cloudpickle.dumps(filter_func)
    decoded_func = base64.b64encode(bfunc)
    print(decoded_func)
    """
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    sock_file, sock = local_connect(java_port)
    process(sock_file, sock_file)
    sock_file.flush()
    exit()
