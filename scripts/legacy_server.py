#!/bin/env python

from omnipath_server.server import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

app = _legacy.create_server(con = con_param)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=44444, dev=True)
