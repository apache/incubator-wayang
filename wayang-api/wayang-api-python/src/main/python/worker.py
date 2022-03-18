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
import pickle
import base64
import re
import sys


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
        ## elif type(obj) is list:
        ##    write_with_length(obj, stream)
    print("Termine")
    write_int(SpecialLengths.END_OF_DATA_SECTION, stream)
    print("Escribi Fin")


def process(infile, outfile):
    """udf64 = os.environ["UDF"]
    print("udf64")
    print(udf64)
    #serialized_udf = binascii.a2b_base64(udf64)
    #serialized_udf = base64.b64decode(udf64)
    serialized_udf = bytearray(udf64, encoding='utf-16')
    # NOT VALID TO BE UTF8  serialized_udf = bytes(udf64, 'UTF-8')
    print("serialized_udf")
    print(serialized_udf)
    # input to be ast.literal_eval(serialized_udf)
    func = pickle.loads(serialized_udf, encoding="bytes")
    print ("func")
    print (func)
    print(func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
    # func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])"""



    # TODO First we must receive the operator + UDF
    """udf = lambda elem: elem.lower()

    def func(it):
        return sorted(it, key=udf)"""
    udf_length = read_int(infile)
    print("udf_length")
    print(udf_length)
    serialized_udf = infile.read(udf_length)
    print("serialized_udf")
    print(serialized_udf)
    #base64_message = base64.b64decode(serialized_udf + "===")
    #print("base64_message")
    #print(base64_message)
    func = pickle.loads(serialized_udf)
    #func = ori.lala(serialized_udf)
    #print (func)
    #for x in func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]): print(x)

    """print("example")
    for x in func("2344|234|efrf|$#|ffrf"): print(x)"""
    # TODO Here we are temporarily assuming that the user is exclusively sending UTF8. User has several types
    iterator = UTF8Deserializer().load_stream(infile)
    # out_iter = sorted(iterator, key=lambda elem: elem.lower())
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
    print (sys.version)
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    sock_file, sock = local_connect(java_port)
    process(sock_file, sock_file)
    sock_file.flush()
    exit()
