from omnipath_server.server import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

_legacy.create_server(con = con_param)
