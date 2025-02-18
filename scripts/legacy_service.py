from omnipath_server.service import _legacy

con_param = {
    'user': 'omnipath',
    'password': 'omnipath123',
    'host': 'localhost',
    'port': '5432',
    'database': 'omnipath',
}

service = _legacy.LegacyService(con = con_param)

# intercell
req = service.intercell(limit=10, format='raw')
list(req)
req = service.intercell(resources='CellPhoneDB', limit=10, format='raw')
list(req)
req = service.intercell(resources=['CellPhoneDB', 'UniProt_location'], format='raw')
req = service.intercell(proteins='EGFR', format='raw')
req = service.intercell(proteins=['EGFR', 'TGFB1'], format='raw')
req = service.intercell(entity_types='protein', format='raw')
req = service.intercell(aspect='functional', format='raw')
req = service.intercell(scope='generic', format='raw')
req = service.intercell(source='composite', format='raw')
req = service.intercell(categories='receptor', format='raw')
req = service.intercell(categories=['receptor', 'ligand'], format='raw')
req = service.intercell(parent='receptor', format='raw')
req = service.intercell(transmitter=True, format='raw')
req = service.intercell(transmitter=0, format='raw')
req = service.intercell(receiver='false', format='raw')
req = service.intercell(receiver='false', transmitter='true', format='raw')
req = service.intercell(secreted='false', format='raw')
req = service.intercell(plasma_membrane_transmembrane=1, format='raw')
req = service.intercell(pmtm=1, format='raw')
req = service.intercell(pmp=1, format='raw')
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
