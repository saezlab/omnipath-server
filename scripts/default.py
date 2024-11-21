from omnipath_server import _main

d = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}
db = _main.Runner(d)
db.connect()
db.create()
