import os
import socket
import struct
import binascii
import pickle


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
        write_with_length(obj, stream)
    write_int(SpecialLengths.END_OF_DATA_SECTION, stream)


def process(infile, outfile):
    udf64 = os.environ["UDF"]
    serialized_udf = binascii.a2b_base64(udf64)
    func = pickle.loads(serialized_udf)
    # func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # TODO First we must receive the operator + UDF
    """udf = lambda elem: elem.lower()

    def func(it):
        return sorted(it, key=udf)"""

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
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    sock_file, sock = local_connect(java_port)
    process(sock_file, sock_file)
    sock_file.flush()
    exit()
