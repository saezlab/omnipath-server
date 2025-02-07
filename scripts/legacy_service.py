from omnipath_server.service import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}


service = _legacy.LegacyService(con = con_param)


# enz-sub
req = service.enzsub(limit = 10, format = 'raw')

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
