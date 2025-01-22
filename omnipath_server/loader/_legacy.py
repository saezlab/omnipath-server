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
import re
import bz2
import csv
import gzip
import lzma
import pathlib as pl

from pypath_common import _misc
from sqlalchemy.orm import decl_api

from .. import _log, _connection
from ..schema import _legacy as _schema

__all__ = [
    'Loader',
    'TableLoader',
]


# TODO: Prevent accidental wiping of DB
class Loader:

    _all_tables: list[str] = [
        'interactions',
        'enz_sub',
        'complexes',
        'intercell',
        'annotations',
    ]
    _compr = {
        '': (open, {}),
        '.gz': (gzip.open, {'mode': 'rt'}),
        '.bz2': (bz2.open, {'mode': 'rt'}),
        '.xz': (lzma.open, {'mode': 'rt'}),
    }
    _fname = 'omnipath_webservice_%s.tsv'


    def __init__(
            self,
            path: str | pl.Path | None = None,
            tables: dict[str, dict] | None = None,
            exclude: list[str] | None = None,
            con: _connection.Connection | dict | None = None,
            wipe: bool = False,
    ):
        """
        Populate the legacy database from TSV files.

        Args:
            config:
                Configuration for loading the database for this service. Under
                "con_param" connection parameters can be provided, and
        """

        self.path = pl.Path(path or '.')
        self.table_param = tables or {}
        self.exclude = exclude
        self.con = _connection.ensure_con(con)
        self.wipe = wipe

        self.con.connect()


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

        for tbl in self.tables:

            self._load_table(tbl)

        _log('Finished populating legacy database.')


    @property
    def tables(self) -> set[str]:

        return set(self._all_tables) - _misc.to_set(self.exclude)


    def _load_table(self, tbl: str):
        """
        Load one table from a TSV file into Postgres.

        Args:
            tbl:
                Name of the table to load.
        """

        param = self.table_param.get(tbl, {})
        path = self.path / param.get('path', self._fname % tbl)
        schema_name = tbl.capitalize().replace('_', '')

        if not (schema := getattr(_schema, schema_name, None)):

            _log(f'No schema found for table `{tbl}`; skipping.')
            return

        for ext in self._compr:

            if (compr_path := path.with_name(path.name + ext)).exists():

                _log(f'Loading table `{tbl}` from `{compr_path}`...')

                return TableLoader(
                    compr_path,
                    schema,
                    self.con,
                    wipe = self.wipe
                ).load()

        _log(
            f'File not found: `{path.name}[{"|".join(self._compr)}]`; '
            f'skipping table `{tbl}`.',
        )

    def _ensure_tables(self) -> bool:
        """
        Verifies whether the tables existing in the database are the same as in
        the schema.
        """

        if self.tables - self.con.tables:

            self.create()


class TableLoader:

    def __init__(
            self,
            path: str | pl.Path,
            table: decl_api.DeclarativeMeta,
            con: _connection.Connection,
            wipe: bool = False,
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
        self.wipe = wipe


    def load(self) -> None:
        """
        Load data from the TSV file into the table.
        """

        if self.wipe:
            self.table.__table__.drop(bind=self.con.engine)
            self.table.__table__.create(bind=self.con.engine)

        cols = [f'"{col.name}"' for col in self.columns if col.name != 'id']
        query = f'INSERT INTO {self.tablename} ({", ".join(cols)}) VALUES %s'
        _log(f'Insert query: {query}')

        _log(f'Inserting data into table `{self.tablename}`...')
        self.con.execute_values(query, self._read())
        _log(f'Finished inserting data into table `{self.tablename}`.')



    @property
    def columns(self) -> 'ReadOnlyColumnCollection':

        return self.table.__table__.columns


    @property
    def tablename(self) -> str:

        return self.table.__table__.name


    def _read(self) -> Generator[tuple, None, None]:
        """
        Read TSV and process fields according to their types.
        """

        compr = ''

        if m := re.search(r'\.(gz|bz2|xz)$', self.path.name):

            compr = m.group()

        opener, args = Loader._compr[compr]

        _log(
            f'Opening `{self.path}` by '
            f'`{opener.__module__}.{opener.__name__}'
            f'(... {_misc.dict_str(args)})`...',
        )

        with opener(self.path, **args) as fp:

            _log(f'Opened `{self.path}` for reading.')

            reader = csv.DictReader(fp, delimiter = '\t')

            for row in reader:

                for col, typ in self.columns.items():

                    if col not in row:

                        continue

                    elif typ.type.python_type is list:  # Array

                        row[col] = row[col].split(';') if row[col] else []

                    elif typ.type.python_type is bool:  # Boolean

                        row[col] = row[col].lower() in ('true', '1', 'yes')

                    elif typ.type.python_type in (int, float):  # Numeric

                        row[col] = (
                            typ.type.python_type(row[col])
                            if row[col] else None
                        )

                yield tuple(
                    row[column.name]
                    for column in self.columns
                    if column.name in row
                )
