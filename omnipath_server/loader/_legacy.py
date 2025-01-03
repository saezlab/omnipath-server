#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

from collections.abc import Generator
import csv
import json
import pathlib as pl

from pypath_common import _misc
from sqlalchemy.orm import decl_api

from .. import _log, _connection
from ..schema import _legacy as _schema

__all__ = [
    'Loader',
    'TableLoader',
]


class Loader:

    _all_tables: list[str] = [
        'interactions',
        'enz_sub',
        'complexes',
        'intercell',
        'annotations',
    ]
    _fname = 'omnipath_webservice_%s.tsv.gz'


    def __init__(
            self,
            path: str | None = None,
            tables: dict[str, dict] | None = None,
            exclude: list[str] | None = None,
            con: _connection.Connection | dict | None = None,
    ):
        """
        Populate the legacy database from TSV files.

        Args:
            config:
                Configuration for loading the database for this service. Under
                "con_param" connection parameters can be provided, and
        """

        self.path = pl.Path(path or '.')
        self.tables = tables
        self.exclude = exclude
        self.con = _connection.ensure_con(con)


    def create(self):
        """
        Create the tables defined in the legacy schema.
        """

        _log('Creating tables in legacy database...')
        _schema.Base.metadata.create_all(self.con.engine)
        _log('Finished creating tables in legacy database...')


    def load(self):
        """
        Load all tables from TSV files into Postgres.
        """

        _log('Populating legacy database...')

        for tbl in set(self._all_tables) - _misc.to_set(self.exclude):

            self._load_table(tbl)

        _log('Finished populating legacy database.')


    def _load_table(self, tbl: str):
        """
        Load one table from a TSV file into Postgres.

        Args:
            tbl:
                Name of the table to load.
        """

        param = self.tables.get(tbl, {})
        path = self.path / param.get('path', self._fname % tbl)

        if not path.exists():

            _log(f'File not found: `{path}`; skipping table `{tbl}`.')
            return

        schema = getattr(_schema, tbl.capitalize())

        _log(f'Loading table `{tbl}` from `{path}`...')
        TableLoader(path, schema, self.con).load()


class TableLoader:

    def __init__(
            self,
            path: str,
            table: decl_api.DeclarativeMeta,
            con: _connection.Connection,
    ):
        """
        Load data from a TSV file into a Postgres table.

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
        _log(f'Insert query: {query}')

        _log(f'Inserting data into table `{self.table.name}`...')
        self.con.execute_values(query, self._read())
        _log(f'Finished inserting data into table `{self.table.name}`.')


    def _read(self) -> Generator[tuple, None, None]:
        """
        Read TSV and process fields according to their types.
        """

        with open(self.path) as fp:

            _log(f'Opened `{self.path}` for reading.')

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
