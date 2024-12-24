from contextlib import closing
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml
import psycopg2.extras

__all__ = [
    'Connection',
]


class Connection:
    def __init__(
            self,
            param: str | dict | None = None,
            **kwargs,
    ):

        self._param = param or kwargs
        self._parse_param()


    def _parse_param(self) -> None:

        self._from_file()


    def _from_file(self) -> None:

        if isinstance(self._param, str) and os.path.exists(self._param):

            with closing(open(self._param)) as fp:

                self._param = yaml.load(fp, Loader = yaml.FullLoader)

    @property
    def _uri(self) -> str:

        return (
            'postgresql://{user}:{password}@'
            '{host}:{port}/{database}'.format(**self._param)
        )

    def connect(self):

        self.engine = create_engine(self._uri)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()


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
