from collections.abc import Generator
import csv

from sqlalchemy.orm import decl_api

from .. import _connection
from ..schema import _legacy as _schema


class Loader:

    _all_tables: list[str] = [
        'interactions',
        'enz_sub',
        'complexes',
        'intercell',
        'annotations',
    ]

    def __init__(
            self,
            path: str | None = None,
            tables: dict[str, dict] | None = None,
            exclude: list[str] | None = None,
            con: _connection.Connection | dict | None = None,
        ):
        """
        Args:
            config:
                Configuration for loading the database for this service. Under
                "con_param" connection parameters can be provided, and
        """

        self.path = path
        self.table = table

    def create(self):

        _schema.Base.metadata.create_all(self.con.engine)


    def load(self):

        for tbl, path in self.legacy_files.items():


    def _open_tsv(self, path: str) -> Generator[tuple, None, None]:

        with open(path, 'r') as fp:

            _ = next(fp)

            for row in fp:

                yield tuple(f.strip() for f in row.split('\t'))


class TableLoader:

    def __init__(
            self,
            path: str,
            table: decl_api.DeclarativeMeta,
            con: _connection.Connection,
        ):
        """
        Args:
            path:
                Path to a TSV file with the data to be loaded.
            table:
                The SQLAlchemy table where we load the data.
        """

        self.path = path
        self.table = table
        self.con = con


    def load(self) -> None:
        """
        Load data from the TSV file into the table.
        """

        cols = [col.name for col in self.table.columns]
        query = f'INSERT INTO {self.table.name} ({', '.join(cols)}) VALUES %s'

        self.con.execute_values(query, self._read())


    def _read(self) -> Generator[tuple, None, None]:
        """
        Read TSV and process fields according to their types.
        """

        with open(self.path, 'r') as fp:

            reader = csv.DictReader(fp, delimiter = '\t')

            for row in reader:

                for col, typ in self.table.columns.items():

                    if typ.type.python_type is dict:  # JSONB

                        row[col] = json.loads(row[col]) if row[col] else None

                    elif typ.type.python_type is list:  # Array

                        row[col] = row[col].split(';') if row[col] else []

                    elif typ.type.python_type is bool:  # Boolean

                        row[col] = row[col].lower() in ('true', '1', 'yes')

                    elif typ.type.python_type in (int, float):  # Numeric

                        row[col] = typ.type.python_type(row[col]) if row[col] else None

                yield tuple(row[column.name] for column in self.table.columns)
