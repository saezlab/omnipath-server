from omnipath_server.service import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}


service = _legacy.LegacyService(con = con_param)

req = service.enzsub(
    enzymes = 'P06239',
    substrates = 'O14543',
    limit = 10,
    format = 'query',
)

list(req)

# enz-sub
req = service.enzsub(enzymes = 'P06239', substrates = 'O14543', limit = 10, format = 'raw')

list(req)

req = service.enzsub(enzymes=['P06241', 'P12931'], limit = 10, format = 'raw')

list(req)

query, bad_req = service._query({'limit': 10}, 'complexes')

# lines
req = service._request({'limit': 10}, 'complexes')

list(req)

# tuples
req = service._request({'limit': 10}, 'complexes', format = 'raw')

list(req)

# JSON
req = service._request({'limit': 10}, 'complexes', format = 'json')

list(req)
