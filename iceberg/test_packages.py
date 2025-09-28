import os
from pyiceberg.catalog import load_catalog

os.environ['PYICEBERG_HOME'] = os.getcwd()

catalog = load_catalog(name='local')

print(catalog)