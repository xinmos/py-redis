from gevent.pool import Pool
from gevent.server import StreamServer

from common.exception import DisConnect, CommandError, Error
from common.protocol import ProtocolHandler


class Server(object):
    def __init__(self, host="127.0.0.1", port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handler(self, conn, address):
        # Convert "conn" (a socket object) into a file-like object
        socket_file = conn.makefile('rwb')

        # process client requests until client disconnects
        while True:
            try:
                data = self._protocol.handle_request((socket_file))
            except DisConnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        # here we`ll actually upack the data sent by the client,
        # execute the command they specified, and pass back the return value
        pass

    def run(self):
        self._server.serve_forever()
