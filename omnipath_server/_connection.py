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

from contextlib import closing
from collections.abc import Generator
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml
import psycopg2.extras

from . import _log

__all__ = [
    'Connection',
    'ensure_con',
]


class Connection:


    def __init__(
            self,
            param: str | dict | None = None,
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
        self._parse_param()


    def _parse_param(self) -> None:

        self._from_file()


    def _from_file(self) -> None:
        """
        Read connection parameters from file (if the file exists).
        """

        if isinstance(self._param, str) and os.path.exists(self._param):

            with closing(open(self._param)) as fp:

                self._param = yaml.load(fp, Loader = yaml.FullLoader)

    @property
    def _uri(self) -> str:
        """
        Connection URI string as used in SQLAlchemy.
        """

        return (
            'postgresql://{user}:{password}@'
            '{host}:{port}/{database}'.format(**self._param)
        )

    def connect(self):
        """
        Connect to the database server.
        """

        uri = self._uri

        _log(f'Connecting to `{uri}`...')

        self.engine = create_engine(uri)
        Session = sessionmaker(bind = self.engine)
        self.session = Session()

        _log(f'Connected to `{uri}`.')


    def __del__(self):

        self.session.close()
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

                    psycopg2.extras.execute_values(cur, query, values)
                    conn.commit()

                except Exception as e:

                    conn.rollback()
                    raise e


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

    return Connection(con)
