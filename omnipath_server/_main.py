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

import psycopg2

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
        self.headers = dict()


    def connect(self, reconnect: bool = False) -> None:

        if reconnect or not isinstance(self.con, _connection.Connection):

            self.con = _connection.Connection(**self.con_param)
            self.con.connect()


    def create(self):

        _legacy.Base.metadata.create_all(self.con.engine)


    def load(self):

        raw_con = self.con.engine.raw_connection()

        for tbl, path in self.legacy_files.items():
            result = self._open_tsv(tbl, path)

            with raw_con.cursor() as cursor:

                query = f"""
                    INSERT INTO {tbl} ({','.join(self.headers[tbl])}) VALUES %s;
                    """
                
                #_log("loading insert statments for structures table")

                psycopg2.extras.execute_values(cursor, query, result, page_size = 1000)


    def _open_tsv(self, tbl, path: str) -> Generator[tuple, None, None]:

        with open(path, 'r') as fp:

            # TODO: make separate function for split+strip
            self.headers[tbl] = [i.strip() for i in next(fp).split('\t')]

            for row in fp:

                yield tuple(f.strip() for f in row.split('\t'))
