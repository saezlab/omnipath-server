from .. import _connection
from ..schema import _legacy

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

        self.con_param = con_param
        self.legacy_files = legacy_files


    def connect(self, reconnect: bool = False) -> None:

        if reconnect or not isinstance(self.con, Connection):

            self.con = Connection(**self.con_param)
            self.con.connect()


    def create(self):

        _legacy.Base.metadata.create_all(self.con.engine)
