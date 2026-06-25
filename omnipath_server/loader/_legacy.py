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
import sys
import bz2
import csv
import gzip
import lzma
import pathlib as pl

from pypath_common import _misc
from sqlalchemy import inspect as sqla_inspect
from sqlalchemy.orm import decl_api
from sqlalchemy.sql.base import ReadOnlyColumnCollection

#  Some `extra_attrs`/`evidences` JSON fields exceed the default CSV field-size
#  limit (e.g. CollecTRI2's verbose per-source evidence); raise it as high as the
#  platform allows.
_csv_field_limit = sys.maxsize

while True:

    try:

        csv.field_size_limit(_csv_field_limit)

        break

    except OverflowError:

        _csv_field_limit //= 10

from .. import _log, _connection
from ..schema import _legacy as _schema

__all__ = [
    'Loader',
    'TableLoader',
]


class Loader:

    # All Legacy table names
    _all_tables: list[str] = [
        'interactions',
        'enzsub',
        'complexes',
        'intercell',
        'annotations',
        'licenses',
    ]
    # Compressed file methods
    _compr = {
        '': (open, {}),
        '.gz': (gzip.open, {'mode': 'rt'}),
        '.bz2': (bz2.open, {'mode': 'rt'}),
        '.xz': (lzma.open, {'mode': 'rt'}),
    }
    # File name template (table name -> file name)
    _fname = 'omnipath_webservice_%s.tsv'
    # Tables whose export file name does not follow the template above: the
    # enzyme-substrate table is `enzsub` in the schema but exported as `enz_sub`,
    # and licenses are exported by the ResourceController as a bare `licenses.tsv`.
    _fname_override = {
        'enzsub': 'omnipath_webservice_enz_sub.tsv',
        'licenses': 'licenses.tsv',
    }


    def __init__(
            self,
            path: str | pl.Path | None = None,
            tables: dict[str, dict] | None = None,
            exclude: list[str] | None = None,
            con: _connection.Connection | dict | None = None,
            wipe: bool = False,
    ):
        """
        Loader class that populates the legacy database from TSV files.

        Args:
            path:
                Path where the TSV files can be found.
            tables:
                Dictionary containing keys as table names to be loaded and
                values are dictionaries whose key/value pairs are configuration
                options for loading the tables.
            exclude:
                List of tables to exclude.
            con:
                Connection instance to the SQL database.
            wipe:
                Whether to wipe the database contents (if any) prior to loading
                the tables.

        Attrs:
            path:
                Same as `path` argument. Otherwise, defaults to current current
                path.
            table_param:
                Same as `tables` argumen. Otherwise, defaults to empty dict.
            exclude:
                Same as `exclude` argument.
            con:
                `Connection` instance to the SQL database.
            wipe:
                Same as `wipe` argument.
            tables:
                Set of table names to be loaded (i.e. all tables except those
                specified in the `exclude` parameter).
        """

        self.path = pl.Path(path or '.')
        self.table_param = tables or {}
        self.exclude = exclude
        self.con = _connection.ensure_con(con)
        self.wipe = wipe

        self.con.init()


    def create(self):
        """
        Method that creates the tables as defined in the legacy schema. Note
        that this method just creates the tables and does not populate them.

        Tables that already exist but whose columns no longer match the schema
        (e.g. a newly added dataset boolean column such as `collectri2`) are
        dropped first so they are recreated with the current columns; their
        data is repopulated by `load()`. `create_all` alone never alters an
        existing table.
        """

        _log('Creating tables in legacy database...')
        self._sync_schema()
        _schema.Base.metadata.create_all(self.con.engine)
        _log('Finished creating tables in legacy database...')


    def _sync_schema(self):
        """
        Drops any existing table whose column set differs from the ORM schema,
        so that `create_all()` recreates it with the current columns. Used to
        pick up schema changes (e.g. a new dataset column) on an in-place
        update without a full manual migration.
        """

        insp = sqla_inspect(self.con.engine)

        for table in _schema.Base.metadata.sorted_tables:

            if not insp.has_table(table.name):

                continue

            db_cols = {c['name'] for c in insp.get_columns(table.name)}
            schema_cols = {c.name for c in table.columns}

            if db_cols != schema_cols:

                _log(
                    f'Table `{table.name}` differs from the schema '
                    f'(missing: {schema_cols - db_cols or "-"}, '
                    f'extra: {db_cols - schema_cols or "-"}); '
                    f'dropping to recreate.',
                )
                table.drop(self.con.engine)


    def load(self):
        """
        Populates all tables from TSV files into the SQL database.
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
        Loads a given table from a TSV file into the SQL database.

        Args:
            tbl:
                Name of the table to load.

        Returns:
            None. If schema is found for a given table, the `TableLoader.load()`
            method will be called, otherwise it will be skipped.
        """

        param = self.table_param.get(tbl, {})
        path = self.path / param.get(
            'path',
            self._fname_override.get(tbl, self._fname % tbl),
        )
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
                    wipe = self.wipe,
                ).load()

        _log(
            f'File not found: `{path.name}[{"|".join(self._compr)}]`; '
            f'skipping table `{tbl}`.',
        )

    def _ensure_tables(self) -> bool: # NOTE: Method not used anywhere?
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
        Loader class for loading the data from a single TSV file into a single
        table on the SQL database.

        Args:
            path:
                Path to the TSV file with the data to be loaded.
            table:
                The SQLAlchemy table where we load the data.
            con:
                Connection instance to the SQL database.
            wipe:
                Whether to wipe the table contents (if any) prior to loading
                the data from the table file.

        Attrs:
            path:
                Same as `path` argument.
            table:
                Same as `table` argument.
            con:
                Same as `con` argument.
            wipe:
                Same as `wipe` argument.
            columns:
                SQLAlchemy `ReadOnlyColumnCollection` instance containing the
                columns in the SQL database table.
            tablename:
                Table name as in the SQL database.
        """

        self.path = path
        self.table = table
        self.con = con
        self.wipe = wipe


    def load(self) -> None:
        """
        Loads the data from the TSV file and populates the corresponding table
        on the SQL database.
        """

        #  Secondary (non-PK) indexes are created AFTER the bulk insert: building
        #  them up front would make every row insert maintain them (very slow for
        #  the ~2M-row interactions GIN). The PK is a constraint, not in
        #  `table.indexes`, so it is still created with the table.
        table = self.table.__table__
        indexes = list(table.indexes)

        if self.wipe:

            table.drop(bind=self.con.engine, checkfirst = True)

            for index in indexes:

                table.indexes.discard(index)

            table.create(bind=self.con.engine)

            for index in indexes:

                table.indexes.add(index)

        #  Insert only the columns present in BOTH the schema and the TSV header,
        #  in schema order. This keeps the INSERT column list in sync with the
        #  value tuples produced by `_read` (which also skips columns absent from
        #  the file), so a TSV missing a schema column (e.g. an older export
        #  without the `collectri2` column) loads with that column left NULL
        #  instead of failing.
        self._file_cols = self._file_columns()
        cols = [
            f'"{col.name}"'
            for col in self.columns
            if col.name != 'id' and col.name in self._file_cols
        ]
        query = f'INSERT INTO {self.tablename} ({", ".join(cols)}) VALUES %s'
        _log(f'Insert query: {query}')

        _log(f'Inserting data into table `{self.tablename}`...')
        self.con.execute_values(query, self._read())
        _log(f'Finished inserting data into table `{self.tablename}`.')

        if self.wipe and indexes:

            _log(
                f'Building {len(indexes)} index(es) on `{self.tablename}`...',
            )

            for index in indexes:

                index.create(bind=self.con.engine)

            _log(f'Finished building indexes on `{self.tablename}`.')


    def _file_columns(self) -> set[str]:
        """
        Column names present in the TSV header.
        """

        compr = ''

        if m := re.search(r'\.(gz|bz2|xz)$', self.path.name):

            compr = m.group()

        opener, args = Loader._compr[compr]

        with opener(self.path, **args) as fp:

            return set(csv.DictReader(fp, delimiter = '\t').fieldnames or [])



    @property
    def columns(self) -> ReadOnlyColumnCollection:

        return self.table.__table__.columns


    @property
    def tablename(self) -> str:

        return self.table.__table__.name


    def _read(self) -> Generator[tuple, None, None]:
        """
        Reads the TSV file and processes the fields according to their types.

        Returns:
            Generator of entries in the table file that are to be passed to the
            SQL connection `execute_values` method.
        """

        # Asserting file compression type and corresponding method for opening
        compr = ''

        if m := re.search(r'\.(gz|bz2|xz)$', self.path.name):

            compr = m.group()

        opener, args = Loader._compr[compr]

        _log(
            f'Opening `{self.path}` by '
            f'`{opener.__module__}.{opener.__name__}'
            f'(... {_misc.dict_str(args)})`...',
        )

        # Opening and processing the file
        with opener(self.path, **args) as fp:

            _log(f'Opened `{self.path}` for reading.')

            reader = csv.DictReader(fp, delimiter = '\t')

            # Iterating over entries in the table (rows)
            for row in reader:

                # Iterating over columns in entry and ensuring types
                for col, typ in self.columns.items():

                    if col not in row:

                        continue

                    elif typ.type.python_type is list:  # Array

                        sep = getattr(
                            self.table,
                            '_array_sep',
                            {},
                        ).get(col, ';')

                        row[col] = row[col].split(sep) if row[col] else []

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
