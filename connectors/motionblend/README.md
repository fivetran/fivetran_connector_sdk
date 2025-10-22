# Rydlr MotionBlend Connector Example

## Connector overview
The MotionBlend connector ingests motion-capture (MoCap) metadata from Google Cloud Storage (GCS) and delivers it to BigQuery for downstream transformation and analytics. It scans folders containing `.bvh` and `.fbx` files, extracts key metadata (such as motion type, frame rate, skeleton ID, and timestamps), and loads this structured information into BigQuery.

This connector supports three motion categories – `seed_motions`, `build_motions`, and `blend_motions` – and is designed for AI and animation pipelines that analyze or generate blended human motion sequences.

Typical use cases include:
- Motion analytics for animation and gaming studios
- Behavior modeling for robotics and simulation
- Training data pipelines for large motion-blend models (e.g., MotionBlendAI)
- Centralizing motion capture assets from distributed storage into a data warehouse
- Supporting downstream dbt transformations for motion blend analytics

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64/x86_64)
- Google Cloud Platform account with:
  - GCS bucket access (Storage Object Viewer role)
  - BigQuery dataset write access (BigQuery Data Editor role)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for SDK installation and environment configuration.

## Features
- Lists and ingests motion-capture files from GCS
- Normalizes metadata into three logical streams: `seed_motions`, `build_motions`, `blend_motions`
- Incremental sync using the `updated_at` timestamp for efficient refreshes
- Deterministic record IDs (SHA-1 hash of file path) for idempotent loads
- Retry and circuit-breaker logic for resilient ingestion
- Structured JSON logging with correlation IDs for observability
- Automatic table creation with daily partitioning on `created_at`
- Configurable batch sizes (default: 25 records) for optimal performance

## Configuration file
`configuration.json` defines the connector parameters uploaded to Fivetran.

```json
{
  "gcp_project": "your-project-id",
  "gcs_bucket": "motionblend-mocap",
  "gcs_prefixes": "mocap/seed/,mocap/build/,mocap/blend/",
  "bigquery_dataset": "RAW",
  "batch_limit": 25,
  "include_exts": ".bvh,.fbx",
  "state_path": "state.json"
}
```

Configuration keys:
- `gcp_project` – GCP project ID for BigQuery
- `gcs_bucket` – GCS bucket name containing motion files
- `gcs_prefixes` – Comma-separated list of prefixes to scan
- `bigquery_dataset` – BigQuery dataset name (e.g., RAW, RAW_DEV)
- `batch_limit` – Records per batch for BigQuery insert
- `include_exts` – File extensions to process (comma-separated)
- `state_path` – Path to state file for incremental sync

Note: Do not check this file into version control, as it may contain credentials.

## Requirements file
`requirements.txt` lists third-party Python dependencies used by the connector.

Example content:
```
google-cloud-storage>=2.0.0
google-cloud-bigquery>=3.0.0
structlog>=23.0.0
tenacity>=8.0.0
```

Key dependencies:
- `google-cloud-storage` – GCS client for blob iteration
- `google-cloud-bigquery` – BigQuery client for data loading
- `structlog` – Structured logging with JSON output
- `tenacity` – Retry library for exponential backoff

Note: `fivetran_connector_sdk` and `requests` are already available in the SDK runtime; do not redeclare them.

## Authentication
This connector uses service account authentication for both GCS and BigQuery access.

1. Create a Google Cloud service account with Storage Object Viewer and BigQuery Data Editor roles.

2. Download its JSON key file and reference it in your environment as `GOOGLE_APPLICATION_CREDENTIALS`.

3. Ensure the same credentials grant read access to the bucket and write access to the dataset.

Example setup:
```bash
gcloud iam service-accounts create motionblend-connector \
  --display-name="MotionBlend Connector"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:motionblend-connector@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:motionblend-connector@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud iam service-accounts keys create sa-key.json \
  --iam-account=motionblend-connector@YOUR_PROJECT.iam.gserviceaccount.com

export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

## Pagination
Not applicable. The connector uses a streaming approach, iterating through GCS object listings with a configurable limit (`batch_limit`). See `extract.py` function `list_blobs()` (lines 18-65).

The connector implements:
- Lazy iteration using `storage.Client().bucket().list_blobs(prefix=prefix)` iterator
- No pagination tokens required; processes blobs as they are discovered
- Configurable `limit` parameter for testing (processes first N files)
- Filters out directories (names ending with `/`) and non-BVH/FBX files

For incremental sync, the connector tracks cursors in `state.py` (refer to `update_cursor()` function, lines 54-79).

## Data handling
Files are discovered via GCS API, normalized, and streamed to BigQuery in JSON rows. Each stream (seed/build/blend) maps to its own BigQuery table (refer to `transform.py`, lines 1-130).

Schemas are generated in `discover.py` through the `discover()` function and correspond to the table definitions below. Date fields are UTC ISO-8601 strings; numeric metrics (frames, fps) are integers.

**Data Transformation Pipeline:**
1. **Extract** (`extract.py`) – List blobs from GCS, filter by file extension, yield metadata
2. **Transform** (`transform.py`) – Normalize records based on category:
   - Generate deterministic ID using SHA-1 hash of file URI
   - Add default values for skeleton type, fps, and joint count
   - Convert timestamps to ISO 8601 format
3. **Load** (`load.py`) – Insert records to BigQuery:
   - Create table if not exists (with daily partitioning on `created_at`)
   - Use `insert_rows_json()` for batch inserts
   - Handle partial failures (log errors, continue processing)

**Type Conversions:**
- GCS blob `updated_at` (datetime) → ISO 8601 string
- Unix timestamps (int) → BigQuery TIMESTAMP
- File sizes (bytes) → INTEGER

## Error handling
Refer to `retry_util.py` and `run_once.py` (lines 95-150).

The connector implements:
- Exponential backoff (0.5s → 30s)
- Circuit breaker (opens when > 50% of a batch fails for 2 minutes)
- Structured logging of all exceptions with correlation IDs

Transient errors (HTTP 5xx, timeouts) are retried; persistent ones are logged to a local DLQ file.

**Retry Logic** (refer to `retry_util.py`, `retry_with_backoff()` decorator, lines 60-150):
- Exponential backoff with jitter (10% variation to prevent thundering herd)
- Max attempts: 7 retries before final failure
- Retryable errors: Transient GCS/BigQuery failures (5xx, timeout, connection)
- Non-retryable: Authentication errors (403), invalid requests (400)

**Circuit Breaker** (refer to `retry_util.py`, `CircuitBreaker` class, lines 20-58):
- Failure threshold: 50% of requests in 5-minute window
- Open state: Reject requests immediately for 120 seconds
- Half-open state: Allow one test request to check recovery

**Structured Logging** (refer to `logging_util.py`, `StructuredLogger` class, lines 15-100):
- Correlation IDs (UUID4) for request tracing across services
- Timing decorators for automatic duration measurement
- Context enrichment to add metadata to all log entries

## Tables created

The MotionBlend schema produces three tables in BigQuery:

| Table | Primary Key | Incremental Field | Description |
|-------|-------------|-------------------|-------------|
| `seed_motions` | `id` | `updated_at` | Base motion files providing raw sequences |
| `build_motions` | `id` | `updated_at` | Derived motions built from seeds |
| `blend_motions` | `id` | `updated_at` | Blended motion pairs with transition metadata |

Example schema snippet (`blend_motions`):
```json
{
  "id": "string",
  "left_motion_id": "string",
  "right_motion_id": "string",
  "blend_ratio": "float",
  "transition_start_frame": "integer",
  "transition_end_frame": "integer",
  "file_uri": "string",
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

All tables use daily partitioning on `created_at` field for query performance and cost optimization.

## Additional files
- `discover.py` – Defines table schemas for discovery
- `extract.py` – Interfaces with GCS to list and fetch object metadata
- `transform.py` – Normalizes raw GCS metadata into BigQuery schemas
- `load.py` – Inserts JSON rows into BigQuery
- `state.py` – Manages incremental cursors between syncs
- `run_once.py` – Entry point orchestrating discover, extract, transform, and load phases
- `logging_util.py` – Handles structured logging with correlation IDs
- `retry_util.py` – Implements exponential backoff and circuit breaker
- `config.yaml` – Configuration file for source/destination settings

## Additional considerations
The MotionBlend connector example demonstrates a production-grade ingestion workflow for AI and animation datasets. It can be adapted for other file-based sources that use GCS metadata as the discovery mechanism.

Key considerations:
- Use least-privilege IAM roles and rotate credentials regularly
- Batch size (default: 25) can be tuned based on record size and network latency
- Partitioning reduces query costs and improves performance for date-range filters
- BigQuery inserts are charged per API call; batch sizes balance latency vs cost
- Schema evolution requires updating `discover.py`, `transform.py`, and `load.py`

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For assistance or questions, contact the Fivetran Support team or open an issue in the example repository.
