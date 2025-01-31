# omnipath_server

Here a brief description of the package contents:

- `loader/`: The data loading classes and methods that populate the database.
- `schema/`: Table definitions (i.e. table columns and associated data types).
- `server/`: Methods related to the actual server.
- `service/`: Classes and methods that manage the different queries to the
different services.
- `_connection.py`: Classes and methods related to the connection to the SQL
database.
- `_main.py`: Main runner for the server.
- `_metadata.py`: For package metadata (author, version, etc.).
- `_session.py`: Session and corresponding log

A basic schema of the information flow can be seen below:

![](./pkg_schema.svg)