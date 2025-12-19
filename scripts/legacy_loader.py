from omnipath_server.loader import _legacy as legacy_loader

sample_dir = './test/data/legacy/'
con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

loader = legacy_loader.Loader(
    path = sample_dir,
    con = con_param,
    wipe = True,
)
loader.create()
loader.load()
