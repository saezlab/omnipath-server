from omnipath_server.service import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

service = _legacy.LegacyService(con = con_param)

# annotations
req = service.annotations(limit=10, format='raw')

list(req)

req = service.annotations(proteins='FST', limit=10, format='raw')

list(req)
req = service.annotations(proteins=['FST', 'TGFB1'], format='raw')
list(req)
req = service.annotations(resources=['UniProt_tissue', 'KEGG'], format='raw')
list(req)
req = service.annotations(entity_types='potato', limit=10, format='raw')
list(req)
req = service.annotations(entity_types='complex', limit=10, format='raw')
list(req)

# enz-sub

req = service.enzsub(
    enzymes = 'P06239',
    substrates = 'O14543',
    limit = 10,
    format = 'query',
)

list(req)

req = service.enzsub(enzymes = 'P06239', substrates = 'O14543', limit = 10, format = 'raw')

list(req)

service.query_str(
    'enzsub',
    enzymes = ['P06241', 'P12931'],
    limit = 10,
).split('WHERE')[1].strip()
service.query_str(
    'enzsub',
    enzymes = 'P06239',
    substrates = 'O14543',
    enzyme_substrate = 'AND',
    limit = 10,
).split('WHERE')[1].strip()
service.query_str(
    'enzsub',
    organisms = 10090,
).split('WHERE')[1].strip()
service.query_str(
    'enzsub',
    types = 'phosphorylation',
).split('WHERE')[1].strip()
service.query_str(
    'enzsub',
    types = ['phosphorylation', 'acetylation'],
).split('WHERE')[1].strip()


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
