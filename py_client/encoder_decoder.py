from pipe_pb2 import Route

class LengthFieldProtoEncoder:
    def __init__(self):
        pass

    def convert_int_to_bytes_string(self, n):
        res = ""
        for i in range(0, 4):
            x = n % 256
            n /= 256
            res = str(unichr(x)) + res
        return res

    def encode(self, route):
        msg = route.SerializeToString()
        print(msg)
        msg = self.convert_int_to_bytes_string(len(msg)) + msg
        return msg


class LengthFieldProtoDncoder:
    def __init__(self):
        pass

    def decode(self, msg):
        pass


