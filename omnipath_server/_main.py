from .. import _connection

__all__ = [
    'Runner',
]

class Runner:


    def __init__(
            self,
            con_param: dict | None = None,
            # TODO: separate load vs. run
            # TODO: multiple loaders/runners, each with its own config
            legacy_files: str | dict | None = None,
    ):

        self.con = _connection.Connection(param = con_param)

    def connect(self):

        self.connection.connect()
