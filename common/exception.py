from collections import namedtuple


class CommandError(Exception):
    pass


class DisConnect(Exception):
    pass


Error = namedtuple("Error", ("message",))
