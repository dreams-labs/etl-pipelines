import os
import json
import sys
import dreams_core as dc
from dextools_python import DextoolsAPIV2


apikey_dextools = dc.get_secret('apikey_dextools')
dextools = DextoolsAPIV2(apikey_dextools,plan='trial')


# blockchains = dextools.get_blockchains()
# print(blockchains)
