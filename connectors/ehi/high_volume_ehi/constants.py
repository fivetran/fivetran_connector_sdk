"""
Constants for EHI connector.
Modify these values to tune performance and behaviour for your environment.
"""

BATCH_SIZE = 1_000  # Rows fetched per SQL FETCH page
CHECKPOINT_INTERVAL = 25_000  # Rows processed between state checkpoints
MAX_WORKERS = 4  # Parallel worker threads (one per table)

MAX_RETRIES = 5  # Maximum retry attempts on transient SQL errors
BASE_RETRY_DELAY = 5.0  # Initial backoff delay in seconds (doubles each attempt)
MAX_RETRY_DELAY = 300.0  # Ceiling for exponential backoff (5 minutes)
CONNECTION_TIMEOUT_HOURS = 8  # Hours before proactively cycling a connection

# SQLSTATE codes that indicate a transient error safe to retry.
# Using SQLSTATE codes is more reliable than substring-matching error messages:
# codes are standardised, locale-independent, and never accidentally match
# unrelated errors whose text happens to contain "timeout" or "deadlock".
RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",  # Serialization failure / deadlock victim
        "HYT00",  # Query timeout expired
        "HYT01",  # Connection timeout expired
        "08S01",  # Communication link failure (TCP-level error)
        "08001",  # Client unable to establish connection
        "08003",  # Connection not open
        "08007",  # Connection failure during transaction
    }
)

# SQL Server native error 1222 = lock request time out period exceeded.
# It arrives with SQLSTATE HY000 (general database error), so we need an
# extra check on the native error number embedded in the message string.
LOCK_TIMEOUT_NATIVE_ERROR = "1222"

DEFAULT_TABLE_LIST = ""  # Empty = sync every table in the schema
DEFAULT_TABLE_EXCLUSION_LIST = ""  # Empty = exclude nothing
DEFAULT_USE_PK_TIEBREAK = False  # Set True for tables with duplicate replication key values
DEFAULT_FORCE_FULL_RESYNC = False  # Set True to discard state and re-sync from scratch

KNOWN_REPLICATION_KEY_PATTERNS = [
    "_LastUpdatedInstant",
    "_lastupdatedinstant",
    "UpdatedAt",
    "updated_at",
    "UpdatedDate",
    "updated_date",
    "ModifiedDate",
    "modified_date",
    "ModifiedAt",
    "modified_at",
    "LastModified",
    "last_modified",
    "DateModified",
    "date_modified",
    "LastUpdated",
    "last_updated",
]
