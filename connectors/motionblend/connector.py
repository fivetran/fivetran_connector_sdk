"""
MotionBlend Fivetran Connector - Syncs motion capture metadata from GCS to BigQuery

This connector ingests motion-capture (MoCap) metadata from Google Cloud Storage (GCS)
and delivers it to BigQuery for downstream transformation and analytics. It scans folders
containing .bvh and .fbx files, extracts key metadata, and loads structured information
into BigQuery tables.

SKELETON HIERARCHY CONTEXT (from blendanim repository):
The blendanim framework uses Mixamo skeleton with specific structure:
- Total nodes: 29 (24 joints + 4 contact nodes + 1 root)
- Feature dims: 174 = 29 nodes × 6 features (position/rotation per node)
- Contact joints: 4 (LeftFoot, RightFoot, LeftToe, RightToe for physics)
- Parent array: Defines hierarchical bone connections
- Rotation: Euler angles (XYZ degrees), 6D continuous, or rotation matrices
- Quality metrics: L2 velocity/acceleration on 5 key joints (Pelvis, L/R Wrist, L/R Foot)

See the README.md for detailed documentation.
See https://github.com/RydlrCS/blendanim for framework implementation.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# For reading configuration from a JSON file
import json

from google.cloud import storage  # Import Google Cloud Storage library for accessing GCS buckets and blobs
from google.api_core import exceptions as google_exceptions  # Import Google API exceptions for specific error handling
from requests import exceptions as requests_exceptions  # For handling HTTP errors during GCS API calls
import hashlib  # For generating deterministic record IDs using SHA-1 hashing
import time  # For implementing retry delays with exponential backoff in GCS operations
from typing import Iterator, Any  # For type hints to improve code clarity and IDE support
from datetime import datetime, timezone  # For generating UTC timestamps in ISO 8601 format

# Import motion blending utilities
from blend_utils import create_blend_metadata, generate_blend_pairs

# Default configuration constants for motion capture metadata
__DEFAULT_SKELETON_ID = "mixamo24"  # Default skeleton identifier for motion data
__DEFAULT_FPS = 30  # Default frames per second for motion capture
__DEFAULT_JOINTS_COUNT = 24  # Default number of joints in the skeleton
__CHECKPOINT_INTERVAL = 100  # Number of records to process before checkpointing
__MAX_BLEND_PAIRS = 100  # Maximum number of blend pairs to generate per sync (default limit)
__MAX_RETRIES = 3  # Maximum number of retry attempts for transient failures
__MAX_MOTION_BUFFER_SIZE = 1000  # Maximum number of seed/build motions to buffer before generating blend pairs (prevents unbounded memory growth)
__BLEND_GENERATION_BATCH_SIZE = 100  # Number of blend pairs to generate in each incremental batch


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    This connector syncs BVH (BioVision Hierarchy) motion capture metadata from GCS.
    Data structure aligns with blendanim framework: seed motions, build motions, and blend results.

    Skeleton Hierarchy Structure (from blendanim repository):
    - Total nodes: 29 (24 joints + 4 contact nodes + 1 root)
    - Parent array: Defines bone hierarchy (each joint references its parent index)
    - Contact nodes: 4 foot contacts for physics simulation (LeftFoot, RightFoot, LeftToe, RightToe)
    - Root node: Pelvis/Hips with 6DOF (3 position XYZ + 3 rotation Euler angles)
    - Joint nodes: 24 skeletal joints with 3DOF rotation (Euler angles in degrees)
    - Feature representation: 174 dimensions = 29 nodes × 6 features per node
    - Rotation formats: Euler angles (XYZ degrees), 6D continuous, or rotation matrices

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "seed_motions",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "file_uri": "STRING",
                "skeleton_id": "STRING",
                "frames": "INT",
                "fps": "INT",
                "joints_count": "INT",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "build_motions",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "file_uri": "STRING",
                "skeleton_id": "STRING",
                "frames": "INT",
                "fps": "INT",
                "joints_count": "INT",
                "build_method": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "blend_motions",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "left_motion_id": "STRING",
                "right_motion_id": "STRING",
                "blend_ratio": "FLOAT",
                "transition_start_frame": "INT",
                "transition_end_frame": "INT",
                "method": "STRING",
                "file_uri": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "fid": "FLOAT",
                "coverage": "FLOAT",
                "global_diversity": "FLOAT",
                "local_diversity": "FLOAT",
                "inter_diversity": "FLOAT",
                "intra_diversity": "FLOAT",
                "l2_velocity_mean": "FLOAT",
                "l2_velocity_std": "FLOAT",
                "l2_velocity_max": "FLOAT",
                "l2_velocity_transition": "FLOAT",
                "l2_acceleration_mean": "FLOAT",
                "l2_acceleration_std": "FLOAT",
                "l2_acceleration_max": "FLOAT",
                "l2_acceleration_transition": "FLOAT",
                "transition_smoothness": "FLOAT",
                "velocity_ratio": "FLOAT",
                "acceleration_ratio": "FLOAT",
                "quality_score": "FLOAT",
                "quality_category": "STRING",
            },
        },
    ]


def list_gcs_files(bucket_name: str, prefix: str, extensions: list[str], limit: int | None = None, last_sync_time: str | None = None) -> Iterator[dict[str, Any]]:
    """
    List files from GCS bucket with optional filtering and retry logic for transient failures.

    Args:
        bucket_name: GCS bucket name
        prefix: Blob prefix to scan
        extensions: List of file extensions to include (e.g., ['.bvh', '.fbx'])
        limit: Optional limit on number of files to process
        last_sync_time: ISO 8601 timestamp; only yield files updated after this time

    Yields:
        Dictionary with file metadata

    Raises:
        RuntimeError: If all retry attempts fail for GCS operations or if permanent authentication/access errors occur
    """
    log.info(f"Listing files from GCS bucket '{bucket_name}' with prefix '{prefix}'")
    if last_sync_time:
        log.info(f"Filtering for files updated after {last_sync_time}")

    # Initialize blobs variable
    blobs = None

    # Retry logic for establishing GCS client connection with exponential backoff
    for attempt in range(__MAX_RETRIES):
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            break
        except (google_exceptions.GoogleAPIError,
                google_exceptions.RetryError,
                google_exceptions.ServerError,
                requests_exceptions.ConnectionError,
                requests_exceptions.Timeout,
                requests_exceptions.HTTPError,
                ConnectionError,
                TimeoutError) as error:
            # Transient errors that should be retried
            if attempt == __MAX_RETRIES - 1:
                log.severe(f"Failed to connect to GCS after {__MAX_RETRIES} attempts: {str(error)}")
                raise RuntimeError(f"GCS connection failed after {__MAX_RETRIES} retries: {str(error)}")

            # Exponential backoff: 2^0=1s, 2^1=2s, 2^2=4s, capped at 60s
            sleep_time = min(60, 2 ** attempt)
            log.warning(f"GCS connection attempt {attempt + 1}/{__MAX_RETRIES} failed (transient error), retrying in {sleep_time}s: {str(error)}")
            time.sleep(sleep_time)
        except (google_exceptions.PermissionDenied,
                google_exceptions.Unauthenticated,
                google_exceptions.NotFound,
                ValueError) as error:
            # Permanent errors that should not be retried
            log.severe(f"GCS connection failed with permanent error: {str(error)}")
            raise RuntimeError(f"GCS connection failed: {str(error)}")

    # Safety check - should never happen due to raise in retry loop
    if blobs is None:
        raise RuntimeError("Failed to initialize GCS blob iterator")

    count = 0
    skipped_no_timestamp = 0
    for blob in blobs:
        # Skip directories
        if blob.name.endswith('/'):
            continue

        # Check file extension
        if not any(blob.name.lower().endswith(ext) for ext in extensions):
            continue

        # Skip files without updated_at timestamp to prevent incremental sync cursor gaps
        # Files without timestamps cannot be reliably tracked for incremental sync,
        # which would create data integrity issues (state cursor gaps causing data loss/duplication)
        if not blob.updated:
            skipped_no_timestamp += 1
            log.warning(
                f"Skipping file without updated_at timestamp: gs://{bucket_name}/{blob.name}. "
                f"Files without timestamps cannot be tracked for incremental sync."
            )
            continue

        # Filter by last sync time for incremental sync
        blob_updated_str = blob.updated.isoformat()
        if last_sync_time and blob_updated_str <= last_sync_time:
            continue

        record = {
            "file_uri": f"gs://{bucket_name}/{blob.name}",
            "updated_at": blob.updated.isoformat(),  # Guaranteed to exist due to filter above
            "size": blob.size,
            "name": blob.name.split('/')[-1],
            "gcs_id": blob.id if blob.id else None,  # Stable GCS object identifier
            "gcs_generation": str(blob.generation) if blob.generation else None  # Object version
        }

        yield record

        count += 1
        if limit and count >= limit:
            log.warning(
                f"Reached batch limit of {limit} files for prefix '{prefix}'. "
                f"Remaining files will be processed in next sync. "
                f"WARNING: batch_limit is for TESTING ONLY and should NOT be used in production, "
                f"as it may cause incomplete syncs if new files arrive during processing."
            )
            break

    log.info(f"Listed {count} files from prefix '{prefix}'")
    if skipped_no_timestamp > 0:
        log.warning(
            f"Skipped {skipped_no_timestamp} files from prefix '{prefix}' due to missing updated_at timestamps. "
            f"These files cannot be tracked for incremental sync."
        )


def infer_category(file_uri: str) -> str:
    """
    Infer motion category from GCS URI.

    Args:
        file_uri: GCS URI path

    Returns:
        Category name: 'seed', 'build', or 'blend'
    """
    log.fine(f"Inferring category for file_uri: {file_uri}")

    if '/seed/' in file_uri:
        category = 'seed'
    elif '/build/' in file_uri:
        category = 'build'
    elif '/blend/' in file_uri:
        category = 'blend'
    else:
        category = 'unknown'

    log.fine(f"Inferred category '{category}' for file_uri: {file_uri}")
    return category


def generate_record_id(raw: dict[str, Any]) -> str:
    """
    Generate stable deterministic record ID using GCS object identifiers.

    Uses GCS object ID (combination of bucket/name/generation) for stable identification
    that persists across file renames/moves. Falls back to file_uri hash if GCS ID unavailable.

    ID Strategy:
    - Primary: GCS blob.id (stable, unique identifier independent of file path)
    - Fallback: SHA-1 hash of file_uri (for compatibility with older data)

    This ensures:
    - File renames/moves don't create duplicate records (if GCS ID is available)
    - Records remain trackable even if files are reorganized in GCS
    - Backward compatibility with file-based IDs for legacy data

    Args:
        raw: Dictionary containing GCS metadata with 'gcs_id' and 'file_uri' keys

    Returns:
        16-character hash string
    """
    # Prefer stable GCS object ID if available
    if raw.get('gcs_id'):
        stable_key = raw['gcs_id']
        log.fine(f"Generating record ID from GCS object ID: {stable_key}")
    else:
        # Fallback to file URI (may change on rename/move)
        stable_key = raw['file_uri']
        log.fine(f"Generating record ID from file_uri (GCS ID not available): {stable_key}")

    record_id = hashlib.sha1(stable_key.encode()).hexdigest()[:16]
    log.fine(f"Generated record ID: {record_id}")
    return record_id


def transform_seed_record(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Transform raw GCS metadata into seed_motions record.

    Args:
        raw: Dictionary containing raw file metadata from GCS

    Returns:
        Dictionary formatted for seed_motions table
    """
    log.fine(f"Transforming seed record for file_uri: {raw.get('file_uri', 'unknown')}")

    now = datetime.now(timezone.utc).isoformat()
    record = {
        "id": generate_record_id(raw),
        "file_uri": raw["file_uri"],
        "skeleton_id": __DEFAULT_SKELETON_ID,
        "frames": 0,  # Placeholder - would parse from BVH in production
        "fps": __DEFAULT_FPS,
        "joints_count": __DEFAULT_JOINTS_COUNT,
        "created_at": now,
        "updated_at": raw.get("updated_at", now)
    }

    log.fine(f"Transformed seed record with ID: {record['id']}")
    return record


def transform_build_record(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Transform raw GCS metadata into build_motions record.

    Args:
        raw: Dictionary containing raw file metadata from GCS

    Returns:
        Dictionary formatted for build_motions table
    """
    log.fine(f"Transforming build record for file_uri: {raw.get('file_uri', 'unknown')}")

    now = datetime.now(timezone.utc).isoformat()
    record = {
        "id": generate_record_id(raw),
        "file_uri": raw["file_uri"],
        "skeleton_id": __DEFAULT_SKELETON_ID,
        "frames": 0,
        "fps": __DEFAULT_FPS,
        "joints_count": __DEFAULT_JOINTS_COUNT,
        "build_method": "ganimator",
        "created_at": now,
        "updated_at": raw.get("updated_at", now)
    }

    log.fine(f"Transformed build record with ID: {record['id']}")
    return record


def transform_blend_record(raw: dict[str, Any], left_motion: dict[str, Any] | None = None, right_motion: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Transform raw GCS metadata or motion pair into blend_motions record.

    Blend ID Strategy:
    - For blend pairs: Uses generate_blend_id() which creates SHA-1 hash of both motion URIs
      This creates stable IDs for specific blend combinations regardless of file location
    - For file-based records: Uses generate_record_id() with GCS object ID for stability

    Note: left_motion_id and right_motion_id track the relationship between motion files.
    These are derived from blend metadata when motion pairs are provided, or set to NULL
    for file-based catalog entries. The blend metadata calculation is performed by
    blend_utils.create_blend_metadata().

    Args:
        raw: Dictionary containing raw file metadata from GCS
        left_motion: Optional left motion record for blend pair generation
        right_motion: Optional right motion record for blend pair generation

    Returns:
        Dictionary formatted for blend_motions table
    """
    log.fine(f"Transforming blend record for file_uri: {raw.get('file_uri', 'unknown')}")

    now = datetime.now(timezone.utc).isoformat()

    # If motion pair is provided, generate blend metadata
    if left_motion and right_motion:
        blend_meta = create_blend_metadata(
            left_motion=left_motion,
            right_motion=right_motion,
            transition_frames=30  # Default 1 second at 30fps
        )
        file_id = blend_meta['id']
        log.fine(f"Generated blend metadata for pair: {left_motion['id']} + {right_motion['id']}")
    else:
        # Fallback to file-based record with stable GCS ID
        file_id = generate_record_id(raw)
        blend_meta = {}

    record = {
        "id": file_id,
        "left_motion_id": blend_meta.get("left_motion_id"),
        "right_motion_id": blend_meta.get("right_motion_id"),
        "blend_ratio": blend_meta.get("blend_ratio"),
        "transition_start_frame": blend_meta.get("transition_start_frame"),
        "transition_end_frame": blend_meta.get("transition_end_frame"),
        "method": "linear",

        "file_uri": raw.get("file_uri", blend_meta.get("left_motion_uri", "")),
        "created_at": blend_meta.get("created_at", now),
        "updated_at": blend_meta.get("updated_at", raw.get("updated_at", now)),

        # Quality metrics - populated from blend_utils estimates or left as placeholders
        "fid": None,
        "coverage": None,
        "global_diversity": None,
        "local_diversity": None,
        "inter_diversity": None,
        "intra_diversity": None,
        "l2_velocity_mean": None,
        "l2_velocity_std": None,
        "l2_velocity_max": None,
        "l2_velocity_transition": None,
        "l2_acceleration_mean": None,
        "l2_acceleration_std": None,
        "l2_acceleration_max": None,
        "l2_acceleration_transition": None,
        "transition_smoothness": blend_meta.get("transition_smoothness"),
        "velocity_ratio": None,
        "acceleration_ratio": None,
        "quality_score": blend_meta.get("estimated_quality"),
        "quality_category": None
    }

    log.fine(f"Transformed blend record with ID: {record['id']}")
    return record


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters
    with valid formats and constraints.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or has invalid format.
    """
    # Validate required parameter: google_cloud_storage_bucket
    if "google_cloud_storage_bucket" not in configuration:
        raise ValueError("Missing required configuration value: google_cloud_storage_bucket")

    bucket = configuration.get("google_cloud_storage_bucket", "").strip()
    if not bucket:
        raise ValueError("Configuration parameter 'google_cloud_storage_bucket' cannot be empty")

    # Validate google_cloud_storage_prefixes (required, must have at least one non-empty prefix)
    if "google_cloud_storage_prefixes" not in configuration:
        raise ValueError("Missing required configuration value: google_cloud_storage_prefixes")

    prefixes = configuration.get("google_cloud_storage_prefixes", "").split(",")
    valid_prefixes = [p.strip() for p in prefixes if p.strip()]
    if not valid_prefixes:
        raise ValueError(
            "Configuration parameter 'google_cloud_storage_prefixes' must contain at least one non-empty prefix. "
            "Provide comma-separated GCS prefixes (e.g., 'mocap/seed/,mocap/build/')"
        )

    # Validate include_extensions (optional, but if provided must be valid)
    if "include_extensions" in configuration:
        extensions_str = configuration.get("include_extensions", "")
        # Check if parameter is present but empty or whitespace-only
        if not extensions_str or not extensions_str.strip():
            raise ValueError(
                "Configuration parameter 'include_extensions' cannot be empty or whitespace-only. "
                "Provide comma-separated extensions starting with '.' (e.g., '.bvh,.fbx') or omit parameter to use default (.bvh,.fbx)"
            )

        extensions = extensions_str.split(",")
        valid_extensions = []
        for ext in extensions:
            ext = ext.strip()
            if ext:
                if not ext.startswith("."):
                    raise ValueError(
                        f"Invalid file extension format '{ext}' in 'include_extensions'. "
                        f"Extensions must start with '.' (e.g., '.bvh,.fbx')"
                    )
                valid_extensions.append(ext)

        if not valid_extensions:
            raise ValueError(
                "Configuration parameter 'include_extensions' is present but contains no valid extensions. "
                "Provide comma-separated extensions starting with '.' (e.g., '.bvh,.fbx') or omit parameter for default"
            )

    # Validate batch_limit (optional, but if provided must be positive integer with reasonable upper bound)
    if "batch_limit" in configuration:
        batch_limit_str = configuration.get("batch_limit")
        if batch_limit_str is not None and str(batch_limit_str).strip():  # If provided and not empty
            try:
                batch_limit = int(batch_limit_str)
                if batch_limit <= 0 or batch_limit > 10000:
                    raise ValueError(
                        f"Configuration parameter 'batch_limit' must be between 1 and 10000, got: {batch_limit}. "
                        f"Large values can cause performance issues and incomplete syncs."
                    )
            except (ValueError, TypeError) as error:
                raise ValueError(
                    f"Configuration parameter 'batch_limit' must be a valid positive integer between 1 and 10000, got: '{batch_limit_str}'. "
                    f"Error: {str(error)}"
                )

    # Validate max_blend_pairs (optional, but if provided must be positive integer with reasonable upper bound)
    if "max_blend_pairs" in configuration:
        max_blend_pairs_str = configuration.get("max_blend_pairs")
        if max_blend_pairs_str is not None and str(max_blend_pairs_str).strip():  # If provided and not empty
            try:
                max_blend_pairs = int(max_blend_pairs_str)
                if max_blend_pairs <= 0 or max_blend_pairs > 100000:
                    raise ValueError(
                        f"Configuration parameter 'max_blend_pairs' must be between 1 and 100000, got: {max_blend_pairs}. "
                        f"Large values can cause excessive memory usage and long processing times."
                    )
            except (ValueError, TypeError) as error:
                raise ValueError(
                    f"Configuration parameter 'max_blend_pairs' must be a valid positive integer between 1 and 100000, got: '{max_blend_pairs_str}'. "
                    f"Error: {str(error)}"
                )

    # Validate max_motion_buffer_size (optional, but if provided must be positive integer with reasonable upper bound)
    if "max_motion_buffer_size" in configuration:
        max_motion_buffer_size_str = configuration.get("max_motion_buffer_size")
        if max_motion_buffer_size_str is not None and str(max_motion_buffer_size_str).strip():  # If provided and not empty
            try:
                max_motion_buffer_size = int(max_motion_buffer_size_str)
                if max_motion_buffer_size <= 0 or max_motion_buffer_size > 50000:
                    raise ValueError(
                        f"Configuration parameter 'max_motion_buffer_size' must be between 1 and 50000, got: {max_motion_buffer_size}. "
                        f"Large values can cause out-of-memory errors (max memory: ~2x buffer size for seed+build buffers)."
                    )
            except (ValueError, TypeError) as error:
                raise ValueError(
                    f"Configuration parameter 'max_motion_buffer_size' must be a valid positive integer between 1 and 50000, got: '{max_motion_buffer_size_str}'. "
                    f"Error: {str(error)}"
                )


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Connectors : MotionBlend")
    validate_configuration(configuration)

    # Extract configuration
    bucket = configuration.get("google_cloud_storage_bucket")
    prefixes = configuration.get("google_cloud_storage_prefixes", "").split(",")

    # Extract and validate extensions with defensive filtering
    extensions_raw = configuration.get("include_extensions", ".bvh,.fbx").split(",")
    extensions = [ext.strip() for ext in extensions_raw if ext.strip()]
    # Fallback to defaults if no valid extensions after filtering
    if not extensions:
        log.warning("No valid file extensions found in configuration, using defaults: .bvh,.fbx")
        extensions = [".bvh", ".fbx"]

    try:
        limit = int(configuration.get("batch_limit", 25))
    except (ValueError, TypeError):
        log.warning("Invalid batch_limit value, using default of 25")
        limit = 25

    try:
        max_blend_pairs = int(configuration.get("max_blend_pairs", __MAX_BLEND_PAIRS))
    except (ValueError, TypeError):
        log.warning(f"Invalid max_blend_pairs value, using default of {__MAX_BLEND_PAIRS}")
        max_blend_pairs = __MAX_BLEND_PAIRS

    try:
        max_motion_buffer_size = int(configuration.get("max_motion_buffer_size", __MAX_MOTION_BUFFER_SIZE))
    except (ValueError, TypeError):
        log.warning(f"Invalid max_motion_buffer_size value, using default of {__MAX_MOTION_BUFFER_SIZE}")
        max_motion_buffer_size = __MAX_MOTION_BUFFER_SIZE

    log.info(f"Starting sync for bucket: {bucket}")
    log.info(f"Memory safety: Motion buffer size limited to {max_motion_buffer_size} records per category")

    # Initialize record counter for logging progress
    total_records_synced = 0

    # Bounded collections for generating blend pairs incrementally (prevents unbounded memory growth)
    # When buffers reach max_motion_buffer_size, blend pairs are generated and buffers are cleared
    seed_motions = []
    build_motions = []
    total_blend_pairs_generated = 0

    def generate_and_sync_blends():
        """
        Generate blend pairs from current buffers and sync to destination.
        Clears buffers after generation to prevent memory accumulation.

        Returns:
            Number of blend pairs generated and synced
        """
        nonlocal total_blend_pairs_generated, total_records_synced

        if not seed_motions or not build_motions:
            log.fine("Skipping blend generation: no seed or build motions in buffers")
            return 0

        log.info(
            f"Generating blend pairs from {len(seed_motions)} seed motions "
            f"and {len(build_motions)} build motions (buffer size limit: {max_motion_buffer_size})"
        )

        try:
            # Calculate remaining blend pairs budget
            remaining_pairs = max_blend_pairs - total_blend_pairs_generated
            batch_limit = min(__BLEND_GENERATION_BATCH_SIZE, remaining_pairs) if remaining_pairs > 0 else __BLEND_GENERATION_BATCH_SIZE

            # Generate blend pairs using blend_utils
            blend_pairs = generate_blend_pairs(
                seed_motions=seed_motions,
                build_motions=build_motions,
                max_pairs=batch_limit
            )

            log.info(f"Generated {len(blend_pairs)} blend pairs in this batch")

            # Process and upsert blend pairs
            blend_count = 0
            for blend_meta in blend_pairs:
                # Transform blend metadata to match schema (blend_utils returns extra fields)
                blend_record = {
                    "id": blend_meta['id'],
                    "left_motion_id": blend_meta['left_motion_id'],
                    "right_motion_id": blend_meta['right_motion_id'],
                    "blend_ratio": blend_meta['blend_ratio'],
                    "transition_start_frame": blend_meta['transition_start_frame'],
                    "transition_end_frame": blend_meta['transition_end_frame'],
                    "method": "linear",
                    "file_uri": blend_meta.get('left_motion_uri', ''),
                    "created_at": blend_meta['created_at'],
                    "updated_at": blend_meta['updated_at'],
                    # Quality metrics - populated from blend_utils estimates or left as None
                    "fid": None,
                    "coverage": None,
                    "global_diversity": None,
                    "local_diversity": None,
                    "inter_diversity": None,
                    "intra_diversity": None,
                    "l2_velocity_mean": None,
                    "l2_velocity_std": None,
                    "l2_velocity_max": None,
                    "l2_velocity_transition": None,
                    "l2_acceleration_mean": None,
                    "l2_acceleration_std": None,
                    "l2_acceleration_max": None,
                    "l2_acceleration_transition": None,
                    "transition_smoothness": blend_meta.get('transition_smoothness'),
                    "velocity_ratio": None,
                    "acceleration_ratio": None,
                    "quality_score": blend_meta.get('estimated_quality'),
                    "quality_category": None
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert("blend_motions", blend_record)

                blend_count += 1
                total_blend_pairs_generated += 1
                total_records_synced += 1

                # Checkpoint every 100 blend records
                if blend_count % __CHECKPOINT_INTERVAL == 0:
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)
                    log.info(f"Synced {blend_count} blend pairs in this batch ({total_records_synced} total records)")

            # Checkpoint after batch completion
            if blend_count > 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)

            log.info(
                f"Completed blend batch: {blend_count} blend records synced "
                f"({total_blend_pairs_generated} total blend pairs, {total_records_synced} total records)"
            )

            # Clear buffers to free memory after successful generation
            seed_motions.clear()
            build_motions.clear()
            log.info(f"Cleared motion buffers to free memory (freed {blend_count} records)")

            return blend_count

        except (ValueError, TypeError, KeyError) as error:
            # Expected errors from blend metadata calculation (invalid data, missing keys, type mismatches)
            # These are recoverable - log warning and continue sync with cleared buffers
            log.warning(
                f"Error in blend metadata calculation: {str(error)}. "
                f"Skipping blend generation for this batch. Buffers will be cleared to prevent memory issues."
            )
            # Clear buffers even on error to prevent memory accumulation
            seed_motions.clear()
            build_motions.clear()
            return 0
        except Exception as error:
            # Unexpected errors indicate programming bugs or serious issues that should not be hidden
            # Log at severe level and re-raise for visibility and debugging
            log.severe(
                f"Unexpected error in blend pair generation: {type(error).__name__}: {str(error)}. "
                f"This indicates a bug that needs investigation."
            )
            # Clear buffers before re-raising to prevent memory leaks
            seed_motions.clear()
            build_motions.clear()
            raise

    # Define table mappings
    table_map = {
        "seed": ("seed_motions", transform_seed_record),
        "build": ("build_motions", transform_build_record),
        "blend": ("blend_motions", transform_blend_record)
    }

    # Process each prefix
    for prefix in prefixes:
        prefix = prefix.strip()
        if not prefix:
            continue

        log.info(f"Processing prefix: {prefix}")

        # Read last sync time for incremental sync
        last_sync_time = state.get(f"last_sync_{prefix}")
        if last_sync_time:
            log.info(f"Incremental sync enabled for prefix '{prefix}': last sync at {last_sync_time}")
        else:
            log.info(f"Full sync for prefix '{prefix}': no previous sync time found")

        try:
            # Initialize counter for this prefix
            prefix_record_count = 0

            log.info(f"Streaming files from prefix '{prefix}' with bounded buffering for memory safety...")

            # Bounded buffer approach: Collect files in batches, sort each batch, process chronologically
            # This prevents out-of-memory errors for large prefixes while maintaining chronological order
            # Buffer size: 1000 files (~100KB metadata per file = ~100MB per batch)
            __FILE_BUFFER_SIZE = 1000
            files_buffer = []
            total_files_processed = 0

            for file_metadata in list_gcs_files(bucket, prefix, extensions, limit, last_sync_time):
                files_buffer.append(file_metadata)

                # Process buffer when full to maintain bounded memory usage
                if len(files_buffer) >= __FILE_BUFFER_SIZE:
                    # Sort current buffer by timestamp before processing
                    files_buffer.sort(key=lambda f: f["updated_at"])
                    log.info(f"Processing batch of {len(files_buffer)} files (sorted chronologically)")

                    # Process sorted buffer
                    for raw_record in files_buffer:
                        category = infer_category(raw_record["file_uri"])

                        if category not in table_map:
                            log.warning(f"Unknown category for file: {raw_record['file_uri']}")
                            continue

                        table_name, transform_func = table_map[category]

                        # Transform record
                        record = transform_func(raw_record)

                        # The 'upsert' operation is used to insert or update data in the destination table.
                        # The first argument is the name of the destination table.
                        # The second argument is a dictionary containing the record to be upserted.
                        op.upsert(table_name, record)

                        # Collect seed and build motions for blend pair generation
                        # Only collect from non-blend categories to avoid circular dependencies
                        # Enforce buffer size limits to prevent unbounded memory growth
                        if category == "seed":
                            seed_motions.append(record)
                            log.fine(f"Collected seed motion: {record['id']} (buffer: {len(seed_motions)}/{max_motion_buffer_size})")

                            # Generate blend pairs when seed buffer reaches limit (memory safety)
                            if len(seed_motions) >= max_motion_buffer_size:
                                log.info(f"Seed motion buffer reached limit ({max_motion_buffer_size}), generating blend pairs")
                                generate_and_sync_blends()

                        elif category == "build":
                            build_motions.append(record)
                            log.fine(f"Collected build motion: {record['id']} (buffer: {len(build_motions)}/{max_motion_buffer_size})")

                            # Generate blend pairs when build buffer reaches limit (memory safety)
                            if len(build_motions) >= max_motion_buffer_size:
                                log.info(f"Build motion buffer reached limit ({max_motion_buffer_size}), generating blend pairs")
                                generate_and_sync_blends()

                        # Update state with current file's timestamp after successful upsert
                        # Safe to update per-file because files are sorted chronologically within each batch
                        # If connector fails, next sync resumes from last successfully processed timestamp
                        state[f"last_sync_{prefix}"] = raw_record["updated_at"]

                        # Increment counters
                        prefix_record_count += 1
                        total_records_synced += 1

                        # Checkpoint every 100 records to preserve progress for large datasets
                        if total_records_synced % __CHECKPOINT_INTERVAL == 0:
                            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                            # from the correct position in case of next sync or interruptions.
                            # Learn more about how and where to checkpoint by reading our best practices documentation
                            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                            op.checkpoint(state)
                            log.info(f"Synced {total_records_synced} total records ({prefix_record_count} from prefix '{prefix}')")

                    total_files_processed += len(files_buffer)
                    files_buffer.clear()  # Clear buffer to free memory

            # Process any remaining files in buffer
            if files_buffer:
                files_buffer.sort(key=lambda f: f["updated_at"])
                log.info(f"Processing final batch of {len(files_buffer)} files (sorted chronologically)")
                for raw_record in files_buffer:
                    category = infer_category(raw_record["file_uri"])

                    if category not in table_map:
                        log.warning(f"Unknown category for file: {raw_record['file_uri']}")
                        continue

                    table_name, transform_func = table_map[category]

                    # Transform record
                    record = transform_func(raw_record)

                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table_name, record)

                    # Collect seed and build motions for blend pair generation
                    # Only collect from non-blend categories to avoid circular dependencies
                    # Enforce buffer size limits to prevent unbounded memory growth
                    if category == "seed":
                        seed_motions.append(record)
                        log.fine(f"Collected seed motion: {record['id']} (buffer: {len(seed_motions)}/{max_motion_buffer_size})")

                        # Generate blend pairs when seed buffer reaches limit (memory safety)
                        if len(seed_motions) >= max_motion_buffer_size:
                            log.info(f"Seed motion buffer reached limit ({max_motion_buffer_size}), generating blend pairs")
                            generate_and_sync_blends()

                    elif category == "build":
                        build_motions.append(record)
                        log.fine(f"Collected build motion: {record['id']} (buffer: {len(build_motions)}/{max_motion_buffer_size})")

                        # Generate blend pairs when build buffer reaches limit (memory safety)
                        if len(build_motions) >= max_motion_buffer_size:
                            log.info(f"Build motion buffer reached limit ({max_motion_buffer_size}), generating blend pairs")
                            generate_and_sync_blends()

                    # Update state with current file's timestamp after successful upsert
                    # Safe to update per-file because files are sorted chronologically within each batch
                    # If connector fails, next sync resumes from last successfully processed timestamp
                    state[f"last_sync_{prefix}"] = raw_record["updated_at"]

                    # Increment counters
                    prefix_record_count += 1
                    total_records_synced += 1

                    # Checkpoint every 100 records to preserve progress for large datasets
                    if total_records_synced % __CHECKPOINT_INTERVAL == 0:
                        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                        # from the correct position in case of next sync or interruptions.
                        # Learn more about how and where to checkpoint by reading our best practices documentation
                        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                        op.checkpoint(state)
                        log.info(f"Synced {total_records_synced} total records ({prefix_record_count} from prefix '{prefix}')")

                total_files_processed += len(files_buffer)
                files_buffer.clear()

            if total_files_processed == 0:
                log.info(f"No files to process for prefix '{prefix}'")
                continue

            log.info(f"Processed {total_files_processed} files from prefix '{prefix}' using bounded buffering")

            # Log completion for this prefix
            log.info(f"Completed processing prefix '{prefix}': {prefix_record_count} records synced")

            # Clear motion buffers after each prefix to prevent unbounded memory growth across prefixes
            # This ensures memory usage is bounded per-prefix rather than accumulating across all prefixes
            if seed_motions or build_motions:
                log.info(
                    f"Clearing motion buffers after prefix '{prefix}': "
                    f"{len(seed_motions)} seed motions, {len(build_motions)} build motions"
                )
                # Generate blend pairs from buffered motions before clearing
                generate_and_sync_blends()
                # Buffers are cleared inside generate_and_sync_blends(), no need to clear again

            # Final checkpoint after completing prefix
            # State contains the timestamp of the last file processed (guaranteed to be the latest due to chronological sorting)
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)

        except RuntimeError as runtime_error:
            # RuntimeError indicates unrecoverable failures (e.g., GCS connection after retries)
            log.severe(f"Fatal error processing prefix '{prefix}': {str(runtime_error)}")
            raise
        except google_exceptions.GoogleAPIError as error:
            # Catch unexpected Google API errors not already handled by list_gcs_files() retry logic
            # Note: Transient errors (GoogleAPIError, RetryError, ServerError) are retried in list_gcs_files()
            # Permanent errors (PermissionDenied, Unauthenticated, NotFound) fail immediately in list_gcs_files()
            # This catches any other Google API errors that occur during processing (e.g., quota exceeded)
            log.severe(f"Google API error processing prefix '{prefix}': {str(error)}")
            raise
        # Note: KeyError and ValueError are NOT caught here - they indicate programming bugs
        # and should fail fast with full stack traces for debugging
        # requests_exceptions.RequestException is handled in list_gcs_files() with retries

    # Generate blend pairs from any remaining buffered motions
    if seed_motions or build_motions:
        log.info(
            f"Processing remaining motion buffers: {len(seed_motions)} seed motions, "
            f"{len(build_motions)} build motions"
        )
        generate_and_sync_blends()
    else:
        log.info("No remaining seed or build motions in buffers; blend generation complete")

    log.info(f"Sync completed successfully: {total_records_synced} total records synced")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    state = {}  # Initialize empty state for first run
    connector.debug(configuration=configuration, state=state)
