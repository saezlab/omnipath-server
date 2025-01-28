from omnipath_server.server import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

app = _legacy.legacy_server
#app.run(host="0.0.0.0", port=8000)
