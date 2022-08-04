from io import BytesIO

from common.exception import DisConnect, CommandError, Error


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_interger,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict
        }

    def handle_request(self, socket_file):
        # parse a request from the client into it`s component parts.
        first_byte = socket_file.read(1)

        if not first_byte:
            raise DisConnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def write_response(self, socket_file, data):
        # serialize the response data and send it to the client.
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_interger(self, socket_file):
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            msg = f'${len(data)}\r\n'.encode() + data + b'\r\n'
            buf.write(msg)
        elif isinstance(data, int):
            msg = f':{data}\r\n'.encode()
            buf.write(msg)
        elif isinstance(data, Error):
            msg = f'-{data.message}\r\n'.encode()
            buf.write(msg)
        elif isinstance(data, (list, tuple)):
            msg = f'*{len(data)}\r\n'.encode()
            buf.write(msg)
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            msg = f'%{len(data)}\r\n'.encode()
            buf.write(msg)
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            msg = '$-1\r\n'.encode()
            buf.write(msg)
        else:
            raise CommandError(f'unrecognized type: {type(data)}')
