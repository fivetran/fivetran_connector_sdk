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
  "include_exts": ".bvh,.fbx"
}
```

Configuration keys:
- `gcp_project` – GCP project ID for BigQuery
- `gcs_bucket` – GCS bucket name containing motion files
- `gcs_prefixes` – Comma-separated list of prefixes to scan
- `bigquery_dataset` – BigQuery dataset name (e.g., RAW, RAW_DEV)
- `batch_limit` – Records per batch for BigQuery insert
- `include_exts` – File extensions to process (comma-separated)

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
Not applicable. The connector uses a streaming approach, iterating through GCS object listings with a configurable limit (`batch_limit`). See `list_gcs_files()` function in `connector.py` (lines 127-214).

The connector implements:
- Lazy iteration using `storage.Client().bucket().list_blobs(prefix=prefix)` iterator
- No pagination tokens required; processes blobs as they are discovered
- Configurable `limit` parameter for testing (processes first N files)
- Filters out directories (names ending with `/`) and non-BVH/FBX files

For incremental sync, the connector tracks cursors in the `update()` function (refer to line 430: `state[f"last_sync_{prefix}"] = datetime.now(timezone.utc).isoformat()`).

## Data handling
Files are discovered via GCS API, normalized, and streamed to the destination via Fivetran operations. Each stream (seed/build/blend) maps to its own table (refer to `transform_seed_record()`, `transform_build_record()`, and `transform_blend_record()` functions in `connector.py`, lines 253-370).

Schemas are defined in the `schema()` function (lines 49-125) and correspond to the table definitions below. Date fields are UTC ISO-8601 strings; numeric metrics (frames, fps) are integers.

Data Transformation Pipeline:
1. Extract (`list_gcs_files()` function, lines 127-214) – List blobs from GCS, filter by file extension, yield metadata
2. Transform (transform functions, lines 253-370) – Normalize records based on category:
   - Generate deterministic ID using SHA-1 hash of file URI (`generate_record_id()`, lines 240-250)
   - Add default values for skeleton type, fps, and joint count
   - Convert timestamps to ISO 8601 format
3. Load (`update()` function, lines 373-451) – Upsert records to destination:
   - Tables are automatically created by Fivetran based on schema definition
   - Uses `op.upsert()` operation for inserting/updating records
   - Checkpoints state after each prefix to enable incremental sync

Type Conversions:
- File sizes (bytes) → INTEGER

## Error handling
Refer to the `list_gcs_files()` function (lines 127-214) and `update()` function (lines 373-451) in `connector.py`.

The connector implements:
- Exponential backoff with retry logic for transient GCS failures
- Specific exception handling for permanent vs. transient errors
- Comprehensive logging using the Fivetran SDK logging module

Retry Logic (refer to `list_gcs_files()` function, lines 145-180):
- Exponential backoff: delays of 1s, 2s, 4s (capped at 60s)
- Max attempts: 3 retries before raising RuntimeError
- Retryable errors: Transient GCS/network failures (GoogleAPIError, RetryError, ServerError, ConnectionError, Timeout, HTTPError)
- Non-retryable: Authentication errors (PermissionDenied, Unauthenticated), invalid requests (NotFound, ValueError)

Error Categories:
1. Transient errors (lines 150-169) – Retried with exponential backoff:
   - `google_exceptions.GoogleAPIError`
   - `google_exceptions.RetryError`
   - `google_exceptions.ServerError`
   - `requests_exceptions.ConnectionError`
   - `requests_exceptions.Timeout`
   - `requests_exceptions.HTTPError`

2. Permanent errors (lines 170-176) – Fail immediately:
   - `google_exceptions.PermissionDenied`
   - `google_exceptions.Unauthenticated`
   - `google_exceptions.NotFound`
   - `ValueError`

Logging (using Fivetran SDK `log` module throughout):
- `log.info()` – Progress updates and operation completion
- `log.warning()` – Retry attempts and recoverable issues
- `log.severe()` – Fatal errors and exceptions
- `log.fine()` – Detailed debugging information

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

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
