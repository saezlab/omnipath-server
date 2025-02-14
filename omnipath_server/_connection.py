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

from contextlib import closing, contextmanager
from collections.abc import Generator
import os

from sqlalchemy import MetaData, inspect, create_engine
from sqlalchemy.orm import Query, sessionmaker
import yaml
import psycopg2.extras

from . import _log

__all__ = [
    'Connection',
    'DEFAULTS',
    'ensure_con',
]

DEFAULTS = {
    'user': 'omnipath',
    'password': 'omnipath',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}


class Connection:


    def __init__(
            self,
            param: str | dict | None = None,
            chunk_size: int = 1000,
            **kwargs,
    ):
        """
        Manage an SQLAlchemy+psycopg2 Postgres connection.

        Args:
            param, kwargs:
                Connection parameters. If a string is provided, it is assumed
                to be a path to a YAML file with the connection parameters. The
                parameters include the host, port, database, user and password.
        """

        self._param = param or kwargs
        self.chunk_size = chunk_size
        self._parse_param()
        self.init()


    def _parse_param(self) -> None:

        self._from_file()

        if isinstance(self._param, dict):

            self._param = {**DEFAULTS, **self._param}


    def _from_file(self) -> None:
        """
        Read connection parameters from file (if the file exists).
        """

        if isinstance(self._param, str) and os.path.exists(self._param):

            with closing(open(self._param)) as fp:

                self._param = yaml.load(fp, Loader = yaml.FullLoader)

        else:

            self._param = self._param or {}


    @property
    def _uri(self) -> str:
        """
        Connection URI string as used in SQLAlchemy.
        """

        return (
            'postgresql://{user}:{password}@'
            '{host}:{port}/{database}'.format(**self._param)
        )

    @property
    def tables(self) -> set[str]:

        return set(inspect(self.engine).get_table_names())

    def init(self):
        """
        Initialize the SQLAlchemy session.
        """

        uri = self._uri

        _log(f'Connecting to `{uri}`...')

        self.engine = create_engine(uri)
        Session = sessionmaker(bind = self.engine)
        self.session = Session()

        _log(f'Connected to `{uri}`.')


    def __del__(self):

        if hasattr(self, 'session'):

            self.session.close()

        if hasattr(self, 'engine'):

            self.engine.dispose()


    def execute_values(
            self,
            query: str,
            values: Generator[tuple, None, None],
    ) -> None:
        """
        Insert by psycopg2.extras.execute_values.

        Args:
            query:
                An SQL INSERT query.
            values:
                Values to insert.
        """

        with closing(self.engine.raw_connection()) as conn:

            with closing(conn.cursor()) as cur:

                try:

                    _log(f'Executing query: {query}')
                    psycopg2.extras.execute_values(cur, query, values)
                    conn.commit()

                except Exception as e:

                    conn.rollback()
                    raise e


    def execute(self, query: str | Query) -> Generator[tuple, None, None]:
        """
        Execute an arbitrary SQL query.

        This execute uses the connection's cursor, bypasses SQLAlchemy's ORM.
        It submits the SQL query as text, and uses a server side cursor,
        consuming the result in chunks.

        Args:
            query:
                An SQL query to execute (Query object or string).
        """

        query = getattr(query, 'statement', query)

        _log(f'Executing query: {query}')

        with self.connect() as con:

            result = con.execute(query)

            while chunk := result.fetchmany(self.chunk_size):

                yield from chunk


    @contextmanager
    def connect(self):
        """
        Context manager for connection management.
        """

        _log('New connection...')
        con = self.engine.connect()

        try:

            yield con

        finally:

            con.close()


    def wipe(self) -> None:
        """
        Wipe the database.
        """

        _log('Wiping database')
        metadata = MetaData()
        metadata.reflect(bind = self.engine)
        metadata.drop_all(bind = self.engine)


def ensure_con(
        con: Connection | dict | str,
        reconnect: bool = False,
) -> Connection:
    """
    Ensure that the provided connection is an instance of Connection.

    Args:
        con:
            Connection object or connection parameters.
        reconnect:
            Create new connection even if an existing Connection is provided.
    """

    if isinstance(con, Connection):

        if reconnect:

            con = con._param.copy()

        else:

            return con

    con = Connection(con)
    con.init()

    return con
