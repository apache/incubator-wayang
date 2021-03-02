import os
import socket
import struct
import binascii
import pickle
import ast
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


def decode_base64(data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', data)  # normalize
    missing_padding = len(data) % 4
    if missing_padding:
        data += b'='* (4 - missing_padding)
    return base64.b64decode(data, altchars)


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
        write_with_length(obj, stream)
    write_int(SpecialLengths.END_OF_DATA_SECTION, stream)


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
    #func = pickle.loads(b'\x80\x04\x955\x04\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\r_builtin_type\x94\x93\x94\x8c\nLambdaType\x94\x85\x94R\x94(h\x02\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x03K\x13C\nt\x00\x88\x00|\x00\x83\x02S\x00\x94N\x85\x94\x8c\x06filter\x94\x85\x94\x8c\x08iterator\x94\x85\x94\x8cS/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/dataquanta.py\x94\x8c\x04func\x94K%C\x02\x00\x01\x94\x8c\x03udf\x94\x85\x94)t\x94R\x94}\x94(\x8c\x0b__package__\x94\x8c\x0corchestrator\x94\x8c\x08__name__\x94\x8c\x17orchestrator.dataquanta\x94\x8c\x08__file__\x94\x8cS/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/dataquanta.py\x94uNNh\x00\x8c\x10_make_empty_cell\x94\x93\x94)R\x94\x85\x94t\x94R\x94\x8c\x1ccloudpickle.cloudpickle_fast\x94\x8c\x12_function_setstate\x94\x93\x94h"}\x94}\x94(h\x19h\x10\x8c\x0c__qualname__\x94\x8c\x1fDataQuanta.filter.<locals>.func\x94\x8c\x0f__annotations__\x94}\x94\x8c\x0e__kwdefaults__\x94N\x8c\x0c__defaults__\x94N\x8c\n__module__\x94h\x1a\x8c\x07__doc__\x94N\x8c\x0b__closure__\x94h\x00\x8c\n_make_cell\x94\x93\x94h\x05(h\x08(K\x01K\x00K\x01K\x02KSC\x10t\x00|\x00\x83\x01d\x01\x16\x00d\x02k\x03S\x00\x94NK\x02K\x00\x87\x94\x8c\x03int\x94\x85\x94\x8c\x04elem\x94\x85\x94\x8cM/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/main.py\x94\x8c\x08<lambda>\x94K\x18C\x00\x94))t\x94R\x94}\x94(h\x17Nh\x19\x8c\x08__main__\x94h\x1b\x8cM/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/main.py\x94uNNNt\x94R\x94h%hB}\x94}\x94(h\x19h:h(\x8c\x1dplan_filter.<locals>.<lambda>\x94h*}\x94h,Nh-Nh.h?h/Nh0N\x8c\x17_cloudpickle_submodules\x94]\x94\x8c\x0b__globals__\x94}\x94u\x86\x94\x86R0\x85\x94R\x94\x85\x94hG]\x94hI}\x94u\x86\x94\x86R0.')
    func = pickle.loads(serialized_udf)
    #func = ori.lala(serialized_udf)
    #print (func)
    #for x in func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]): print(x)

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
