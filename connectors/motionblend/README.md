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
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Google Cloud Platform account with GCS bucket access (Storage Object Viewer role)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Lists and ingests motion-capture files from GCS
- Normalizes metadata into three logical streams: `seed_motions`, `build_motions`, `blend_motions`
- Incremental sync using blob `updated` timestamps to process only new or modified files
- Deterministic record IDs (SHA-1 hash of file path) for idempotent loads
- Exponential backoff retry logic for transient GCS failures
- Checkpointing every 100 records for fault tolerance on large datasets
- SDK-based structured logging for operational visibility
- Configurable batch sizes (default: 25 records) for optimal performance

## Configuration file
`configuration.json` defines the connector parameters uploaded to Fivetran.

```json
{
  "google_cloud_storage_bucket": "<YOUR_GCS_BUCKET_NAME>",
  "google_cloud_storage_prefixes": "<COMMA_SEPARATED_GCS_PREFIXES_EXAMPLE_mocap/seed/,mocap/build/>",
  "batch_limit": "<BATCH_LIMIT_NUMBER_DEFAULT_25>",
  "include_extensions": "<FILE_EXTENSIONS_DEFAULT_.bvh,.fbx>"
}
```

Configuration keys:
- `google_cloud_storage_bucket` – GCS bucket name containing motion files
- `google_cloud_storage_prefixes` – Comma-separated list of prefixes to scan
- `batch_limit` – Maximum number of files to process per sync (for testing)
- `include_extensions` – File extensions to process (comma-separated)

Note: Do not check this file into version control, as it may contain credentials.

## Requirements file
`requirements.txt` lists third-party Python dependencies used by the connector.

Example content:
```
google-cloud-storage==2.18.2
```

Key dependencies:
- `google-cloud-storage` – GCS client for blob iteration and file discovery

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector uses Google Cloud Application Default Credentials (ADC) for GCS authentication. The connector initializes the GCS client as `storage.Client()` without explicit credentials, relying on the runtime environment to provide authentication.

### Local development
Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account JSON key:

```bash
# Create service account with required permissions
gcloud iam service-accounts create motionblend-connector \
  --display-name="MotionBlend Connector"

# Grant Storage Object Viewer role for GCS read access
gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:motionblend-connector@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# Download service account key
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=motionblend-connector@YOUR_PROJECT.iam.gserviceaccount.com

# Set environment variable for local testing
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

### Fivetran deployment
When deploying to Fivetran, credentials must be configured through the Fivetran platform:

1. In the Fivetran connector setup UI, provide the service account JSON key content in the designated credentials field
2. Fivetran will inject these credentials into the runtime environment as ADC
3. The connector will automatically discover and use these credentials via `storage.Client()`

Note: The connector does not accept credentials via `configuration.json`. All authentication is handled through Fivetran's secure credential management system.

### Required permissions
The service account needs the following IAM role:
- `roles/storage.objectViewer` - Read access to GCS buckets and objects

## Pagination
Not applicable. The connector uses a streaming approach, iterating through GCS object listings with a configurable limit (`batch_limit`). See `list_gcs_files()` function in `connector.py` (lines 134-225).

The connector implements:
- Lazy iteration using `storage.Client().bucket().list_blobs(prefix=prefix)` iterator
- No pagination tokens required; processes blobs as they are discovered
- Configurable `limit` parameter for testing (processes first N files)
- Filters out directories (names ending with `/`) and non-BVH/FBX files

For incremental sync, the connector tracks cursors in the `update()` function (refer to line 455: `state[f"last_sync_{prefix}"] = datetime.now(timezone.utc).isoformat()`).

## Data handling
Files are discovered via GCS API, normalized, and streamed to the destination via Fivetran operations. Each stream (seed/build/blend) maps to its own table (refer to `transform_seed_record()`, `transform_build_record()`, and `transform_blend_record()` functions in `connector.py`, lines 268-376).

Schemas are defined in the `schema()` function (lines 46-131) and correspond to the table definitions below. Date fields are UTC ISO-8601 strings; numeric metrics (frames, fps) are integers.

Data Transformation Pipeline:
1. Extract (`list_gcs_files()` function, lines 134-225) – List blobs from GCS, filter by file extension, yield metadata
2. Transform (transform functions, lines 268-376) – Normalize records based on category:
   - Generate deterministic ID using SHA-1 hash of file URI (`generate_record_id()`, lines 252-265)
   - Add default values for skeleton type, fps, and joint count
   - Convert timestamps to ISO 8601 format
3. Load (`update()` function, lines 378-472) – Upsert records to destination:
   - Tables are automatically created by Fivetran based on schema definition
   - Uses `op.upsert()` operation for inserting/updating records
   - Checkpoints state after each prefix to enable incremental sync

Type Conversions:
- File sizes (bytes) → INTEGER

## Error handling
Refer to the `list_gcs_files()` function (lines 134-225) and `update()` function (lines 378-472) in `connector.py`.

The connector implements:
- Exponential backoff with retry logic for transient GCS failures
- Specific exception handling for permanent vs. transient errors
- Comprehensive logging using the Fivetran SDK logging module

Retry Logic (refer to `list_gcs_files()` function, lines 158-193):
- Exponential backoff: delays of 1s, 2s, 4s (capped at 60s)
- Max attempts: 3 retries before raising RuntimeError
- Retryable errors: Transient GCS/network failures (GoogleAPIError, RetryError, ServerError, ConnectionError, Timeout, HTTPError)
- Non-retryable: Authentication errors (PermissionDenied, Unauthenticated), invalid requests (NotFound, ValueError)

Error Categories:
1. Transient errors (lines 165-184) – Retried with exponential backoff:
   - `google_exceptions.GoogleAPIError`
   - `google_exceptions.RetryError`
   - `google_exceptions.ServerError`
   - `requests_exceptions.ConnectionError`
   - `requests_exceptions.Timeout`
   - `requests_exceptions.HTTPError`

2. Permanent errors (lines 186-193) – Fail immediately:
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

For optimal query performance and cost optimization, we recommend configuring daily partitioning on the `created_at` field in BigQuery. This must be set up in your BigQuery destination; the connector delivers the `created_at` field but does not configure partitioning automatically.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
