"""
Root conftest.py — adds the connector directory to sys.path so that
`import connector` works from any test module without file-path hacks.

Also initialises the fivetran_connector_sdk Logging.LOG_LEVEL so that
log.info / log.warning / log.severe calls do not crash with TypeError
when tests run outside the SDK harness (LOG_LEVEL is None by default).
"""
import sys
import os

# The connector.py lives one level above tests/
CONNECTOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if CONNECTOR_DIR not in sys.path:
    sys.path.insert(0, CONNECTOR_DIR)

# Initialise SDK logging so log.info / warning / severe work in tests
from fivetran_connector_sdk import Logging
if Logging.LOG_LEVEL is None:
    Logging.LOG_LEVEL = Logging.Level.WARNING  # suppress INFO noise, allow warnings
