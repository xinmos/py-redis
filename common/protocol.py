from io import BytesIO

from common.exception import DisConnect, CommandError, Error


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_interger,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
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

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))

    def handle_interger(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(f'${len(data)}\r\n{data}\r\n')
        elif isinstance(data, int):
            buf.write(f':{data}\r\n')
        elif isinstance(data, Error):
            buf.write(f'-{data.message}\r\n')
        elif isinstance(data, (list, tuple)):
            buf.write(f'*{len(data)}')
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f'%%{len(data)}')
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n')
        else:
            raise CommandError(f'unrecognized type: {type(data)}')
