# Rydlr MotionBlend Connector Example

## Connector overview
The MotionBlend connector catalogs motion-capture (MoCap) file inventory from Google Cloud Storage (GCS) and delivers it to BigQuery for downstream transformation and analytics. It scans folders containing `.bvh` and `.fbx` files, extracts GCS blob metadata (file URIs, update timestamps, and file sizes), and loads this file catalog into BigQuery.

This connector supports three motion categories – `seed_motions`, `build_motions`, and `blend_motions` – and is designed for AI and animation pipelines that analyze or generate blended human motion sequences.

Current Limitations:
- Motion-specific metadata (frame count, FPS, joint counts, skeleton IDs) use static placeholder values
- Quality metrics in blend records are set to NULL and require post-processing
- File contents are NOT parsed; only GCS blob metadata (URI, timestamps, size) is extracted
- Suitable for building a file catalog and tracking file locations; full metadata extraction requires additional processing steps

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
- Lists and catalogs motion-capture files from GCS
- Normalizes file inventory into three logical streams: `seed_motions`, `build_motions`, `blend_motions`
- Incremental sync using blob `updated` timestamps to process only new or modified files
- Stable record IDs using GCS object identifiers (persists across file renames/moves)
- Exponential backoff retry logic for transient GCS failures
- Checkpointing every 100 records for fault tolerance on large datasets
- SDK-based structured logging for operational visibility
- Configurable batch limit for testing (processes N files per sync, resumes in next sync)

## Configuration file
`configuration.json` defines the connector parameters uploaded to Fivetran.

```json
{
  "google_cloud_storage_bucket": "<YOUR_GCS_BUCKET_NAME>",
  "google_cloud_storage_prefixes": "<COMMA_SEPARATED_GCS_PREFIXES>",
  "batch_limit": "<BATCH_LIMIT>",
  "include_extensions": "<FILE_EXTENSIONS>"
}
```

Configuration keys:
- `google_cloud_storage_bucket` – GCS bucket name containing motion files
- `google_cloud_storage_prefixes` – Comma-separated list of prefixes to scan (e.g., `mocap/seed/,mocap/build/`)
- `batch_limit` – Maximum number of files to process per prefix per sync (default: 25, TESTING ONLY - state is not updated when limit is reached to prevent data loss; remove for production use)
- `include_extensions` – File extensions to process (comma-separated, default: `.bvh,.fbx`)

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
Not applicable. The connector uses a streaming approach, iterating through GCS object listings with a configurable limit (`batch_limit`). See `list_gcs_files()` function in `connector.py` (lines 138-235).

The connector implements:
- Lazy iteration using `storage.Client().bucket().list_blobs(prefix=prefix)` iterator
- No pagination tokens required; processes blobs as they are discovered
- Configurable `limit` parameter for testing (processes first N files per prefix)
- Filters out directories (names ending with `/`) and non-BVH/FBX files

Important - Testing Only: The `batch_limit` parameter limits how many files are processed per prefix per sync. When the limit is reached:
- State is updated with the last processed file's timestamp
- Next sync will resume from that point and process the next batch
- This is ONLY for testing/development to avoid processing large datasets during development
- Production use: Remove `batch_limit` entirely or set to a very high value
- Files are processed in chronological order (sorted by `updated_at`) to ensure no files are skipped

For incremental sync, the connector tracks cursors in the `update()` function by recording the `updated_at` timestamp from each successfully processed file (lines 544-550).

## Data handling
Files are discovered via GCS API, cataloged with blob metadata, and streamed to the destination via Fivetran operations. Each stream (seed/build/blend) maps to its own table (refer to `transform_seed_record()`, `transform_build_record()`, and `transform_blend_record()` functions in `connector.py`, lines 299-432).

Schemas are defined in the `schema()` function (lines 50-136) and correspond to the table definitions below. Date fields are UTC ISO-8601 strings.

### Record ID Generation Strategy

The connector uses stable GCS object identifiers to generate record IDs, ensuring data integrity across file renames and reorganizations:

ID Generation Logic (`generate_record_id()` function):
- Primary: Uses GCS `blob.id` (format: `bucket/object/generation`)
  - Stable across file path changes (renames, moves within bucket)
  - Unique per object version (immutable)
  - Recommended for production use
- Fallback: SHA-1 hash of `file_uri` (when GCS ID unavailable)
  - Legacy compatibility mode
  - Caveat: File renames/moves create new IDs and duplicate records

Behavior Examples:
```
# File remains at same path
gs://bucket/motions/walk.bvh (generation 123) → ID: a1b2c3d4e5f6g7h8
gs://bucket/motions/walk.bvh (generation 123) → ID: a1b2c3d4e5f6g7h8 ✓ Same ID

# File renamed/moved (with GCS ID available)
gs://bucket/motions/walk.bvh (generation 123) → ID: a1b2c3d4e5f6g7h8
gs://bucket/archive/walk_v1.bvh (generation 123) → ID: a1b2c3d4e5f6g7h8 ✓ Same ID

# File content updated (new version)
gs://bucket/motions/walk.bvh (generation 123) → ID: a1b2c3d4e5f6g7h8
gs://bucket/motions/walk.bvh (generation 456) → ID: x9y8z7w6v5u4t3s2 ✓ New ID (new content)
```

Blend Motions ID Strategy:
- Blend pairs: SHA-1 hash of both motion URIs concatenated (`left_uri|right_uri`)
  - Stable for specific blend combinations
  - Independent of blend result file location
  - Generated by `blend_utils.generate_blend_id()`
- File-based records: Uses standard `generate_record_id()` with GCS object ID
  - For cataloging individual blend result files
  - `left_motion_id` and `right_motion_id` set to NULL

Important Notes:
- GCS object IDs are only available when files are accessed via GCS API
- Files uploaded as new objects (even with same name) get new generation numbers
- For immutable datasets where renames are rare, file_uri-based IDs are acceptable
- Downstream systems should treat record IDs as opaque stable identifiers

Data Transformation Pipeline:
1. Extract (`list_gcs_files()` function, lines 138-235) – List blobs from GCS, filter by file extension, yield GCS metadata:
   - Actual extracted data: `file_uri`, `updated_at` (from GCS blob), `size`, `name`, `gcs_id`, `gcs_generation`
   - NOT extracted: File contents are not parsed; motion-specific metadata is not extracted
2. Transform (transform functions, lines 299-432) – Normalize records based on category:
   - Generate stable deterministic ID using GCS object identifier (`generate_record_id()`, lines 263-296)
     - Primary: Uses GCS `blob.id` (stable across file renames/moves)
     - Fallback: SHA-1 hash of file_uri if GCS ID unavailable (legacy compatibility)
   - ID stability: File renames/moves do NOT create duplicate records when GCS object ID is available
   - Insert STATIC placeholder values (NOT extracted from file contents):
     - `skeleton_id`: "mixamo24" (hardcoded constant)
     - `fps`: 30 (hardcoded constant)
     - `joints_count`: 24 (hardcoded constant)
     - `frames`: 0 (hardcoded constant)
     - `build_method`: "ganimator" for build_motions (hardcoded constant)
   - Preserve actual GCS blob metadata: file URIs, timestamps, file sizes
   - Quality metrics (blend_quality, transition_smoothness) are set to NULL
3. Load (`update()` function, lines 453-598) – Upsert records to destination:
   - Files are sorted by `updated_at` timestamp to ensure chronological processing and prevent data loss
   - Tables are automatically created by Fivetran based on schema definition
   - Uses `op.upsert()` operation for inserting/updating records
   - State is updated after each successful upsert with that file's timestamp (not maximum across batch)
   - Checkpoints state every 100 records and after each prefix completes
   - Data loss prevention: Processing in chronological order ensures connector failures don't skip unprocessed files

State Management & Data Loss Prevention:
- Files are sorted by `updated_at` before processing (oldest first)
- State tracks the timestamp of the last successfully processed file (not max timestamp)
- If connector fails mid-sync, next run resumes from last successful file
- Example: Files A (Jan 10), B (Jan 15), C (Jan 12) are sorted → A, C, B and processed in order
- If failure occurs after B, state = Jan 15, and no files are skipped in next sync

Data Accuracy:
- ✅ Accurate: File URIs, GCS update timestamps, file names, file sizes, GCS object IDs
- ⚠️ STATIC placeholders (NOT extracted): Frame counts (0), FPS (30), skeleton IDs ("mixamo24"), joint counts (24), build_method ("ganimator")
- ❌ Not extracted: Actual motion data requires parsing BVH/FBX file contents (frames, actual FPS, skeleton structure)

Blend Metadata Calculation (blend_utils):
When motion pairs are provided to `transform_blend_record()` with `left_motion` and `right_motion` parameters, the connector uses `blend_utils.create_blend_metadata()` to calculate:
- `blend_ratio`: Based on motion duration ratios (heuristic)
- `transition_start_frame` / `transition_end_frame`: Calculated from blend ratio and transition window
- `estimated_quality`: Heuristic score based on duration similarity and blend parameters

Important: These calculations use placeholder frame counts (0) from the catalog, so they produce theoretical values. For production-quality blend metadata, you need to:
1. Parse actual BVH/FBX files to extract real frame counts and motion data
2. Use the [blendanim framework](https://github.com/RydlrCS/blendanim) for neural network-based blending
3. Calculate L2 velocity/acceleration metrics on generated motion sequences

Type Conversions:
- File sizes (bytes) → INTEGER
- GCS blob timestamps → UTC ISO-8601 strings

## Error handling
Refer to the `list_gcs_files()` function (lines 138-235) and `update()` function (lines 453-598) in `connector.py`.

The connector implements:
- Exponential backoff with retry logic for transient GCS failures
- Specific exception handling for permanent vs. transient errors
- Comprehensive logging using the Fivetran SDK logging module
- Memory safeguard: Warns if file count exceeds 10,000 per prefix (configurable via `__MAX_FILES_PER_SYNC`)

Retry Logic (refer to `list_gcs_files()` function, lines 163-197):
- Exponential backoff: delays of 1s, 2s, 4s (capped at 60s)
- Max attempts: 3 retries before raising RuntimeError
- Retryable errors: Transient GCS/network failures (GoogleAPIError, RetryError, ServerError, ConnectionError, Timeout, HTTPError)
- Non-retryable: Authentication errors (PermissionDenied, Unauthenticated), invalid requests (NotFound, ValueError)

Error Categories:
1. Transient errors (lines 169-185) – Retried with exponential backoff:
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

## Additional files
The connector uses the following additional file:
- **blend_utils.py** – Motion blending metadata calculation module

This lightweight Python module calculates blend metadata for motion pairs and provides the following core functions:

- `create_blend_metadata(left_motion, right_motion, transition_frames)` – Generates complete blend record with calculated parameters
- `calculate_blend_ratio(left_duration, right_duration)` – Computes blend ratio based on motion durations  
- `calculate_transition_window(left_frames, right_frames, ratio)` – Determines transition start/end frames
- `estimate_blend_quality(params)` – Heuristic quality score (0.0-1.0) based on motion compatibility
- `generate_blend_id(left_uri, right_uri)` – Deterministic SHA-1 hash for blend pair identification

Example Usage:
```python
from blend_utils import create_blend_metadata

# Input motion records
seed_motion = {'id': 'seed_001', 'file_uri': 'gs://bucket/walk.bvh', 'frames': 150, 'fps': 30}
build_motion = {'id': 'build_001', 'file_uri': 'gs://bucket/run.bvh', 'frames': 180, 'fps': 30}

# Generate blend metadata
blend = create_blend_metadata(seed_motion, build_motion, transition_frames=30)
# Returns: {'id': '...', 'blend_ratio': 0.455, 'transition_start_frame': 53, 
#           'transition_end_frame': 83, 'estimated_quality': 0.884, ...}
```

Important Limitations:
- Metadata calculation only – does not perform actual motion synthesis
- Quality estimates are heuristic (duration/ratio-based), not motion-aware
- For neural network-based blending, use [blendanim framework](https://github.com/RydlrCS/blendanim)
- File contents (BVH/FBX) are not parsed; frame counts use placeholder values (0)
- Actual blend quality requires L2 velocity/acceleration analysis on generated motions

The blend_utils module is designed for cataloging blend operations and generating metadata records for the `blend_motions` table. Refer to the "Blend Metadata Calculation (blend_utils)" subsection in the Data handling section for usage details.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
