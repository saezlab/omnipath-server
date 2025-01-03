from omnipath_server.loader import _legacy as legacy_loader

sample_dir = './tests/data/legacy'
con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

loader = legacy_loader.Loader(
    path = sample_dir,
    exclude = ['complexes', 'enz_sub', 'intercell', 'annotations'],
    con = con_param,
)
loader.create()
loader.load()
