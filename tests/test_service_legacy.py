
import pytest

from omnipath_server.service import _legacy


service = _legacy.LegacyService(con = con_param)


# enz-sub
req = service.enzsub(enzymes = 'P06239', substrates = 'O14543', limit = 10, format = 'raw')