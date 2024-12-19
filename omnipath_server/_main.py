from collections.abc import Generator

from . import _connection
from .schema import _legacy

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
        self.con = None


    def connect(self, reconnect: bool = False) -> None:

        if reconnect or not isinstance(self.con, _connection.Connection):

            self.con = _connection.Connection(**self.con_param)
            self.con.connect()


    def create(self):

        _legacy.Base.metadata.create_all(self.con.engine)


    def load(self):

        for tbl, path in self.legacy_files.items():


    def _open_tsv(self, path: str) -> Generator[tuple, None, None]:

        with open(path, 'r') as fp:

            _ = next(fp)

            for row in fp:

                yield tuple(f.strip() for f in row.split('\t'))
