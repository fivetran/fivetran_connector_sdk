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
from typing import Iterator, Dict, Any, Optional  # For type hints to improve code clarity and IDE support
from datetime import datetime, timezone  # For generating UTC timestamps in ISO 8601 format

# Import motion blending utilities
from blend_utils import create_blend_metadata

# Default configuration constants for motion capture metadata
__DEFAULT_SKELETON_ID = "mixamo24"  # Default skeleton identifier for motion data
__DEFAULT_FPS = 30  # Default frames per second for motion capture
__DEFAULT_JOINTS_COUNT = 24  # Default number of joints in the skeleton
__CHECKPOINT_INTERVAL = 100  # Number of records to process before checkpointing
__MAX_RETRIES = 3  # Maximum number of retry attempts for transient failures


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


def list_gcs_files(bucket_name: str, prefix: str, extensions: list[str], limit: Optional[int] = None, last_sync_time: Optional[str] = None) -> Iterator[Dict[str, Any]]:
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
        RuntimeError: If all retry attempts fail for GCS operations
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
    for blob in blobs:
        # Skip directories
        if blob.name.endswith('/'):
            continue

        # Check file extension
        if not any(blob.name.lower().endswith(ext) for ext in extensions):
            continue

        # Filter by last sync time for incremental sync
        if last_sync_time and blob.updated:
            blob_updated_str = blob.updated.isoformat()
            if blob_updated_str <= last_sync_time:
                continue

        record = {
            "file_uri": f"gs://{bucket_name}/{blob.name}",
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "size": blob.size,
            "name": blob.name.split('/')[-1]
        }

        yield record

        count += 1
        if limit and count >= limit:
            log.info(f"Reached limit of {limit} files for prefix '{prefix}'")
            break

    log.info(f"Listed {count} files from prefix '{prefix}'")


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


def generate_record_id(file_uri: str) -> str:
    """
    Generate deterministic record ID from file URI using SHA-1 hash.

    Args:
        file_uri: GCS file URI

    Returns:
        16-character hash string
    """
    log.fine(f"Generating record ID for file_uri: {file_uri}")
    record_id = hashlib.sha1(file_uri.encode()).hexdigest()[:16]
    log.fine(f"Generated record ID: {record_id}")
    return record_id


def transform_seed_record(raw: Dict[str, Any]) -> Dict[str, Any]:
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
        "id": generate_record_id(raw["file_uri"]),
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


def transform_build_record(raw: Dict[str, Any]) -> Dict[str, Any]:
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
        "id": generate_record_id(raw["file_uri"]),
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


def transform_blend_record(raw: Dict[str, Any], left_motion: Optional[Dict[str, Any]] = None, right_motion: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Transform raw GCS metadata or motion pair into blend_motions record.

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
        # Fallback to file-based record
        file_id = generate_record_id(raw["file_uri"])
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
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["gcs_bucket"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


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
    extensions = configuration.get("include_extensions", ".bvh,.fbx").split(",")
    try:
        limit = int(configuration.get("batch_limit", 25))
    except (ValueError, TypeError):
        log.warning("Invalid batch_limit value, using default of 25")
        limit = 25

    log.info(f"Starting sync for bucket: {bucket}")

    # Initialize record counter for logging progress
    total_records_synced = 0

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

            # Track the maximum timestamp from processed files for accurate state management
            max_updated_at = last_sync_time

            # Track whether we hit the artificial batch limit (for testing)
            hit_batch_limit = False

            # List files from GCS with incremental sync support
            for raw_record in list_gcs_files(bucket, prefix, extensions, limit, last_sync_time):
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

                # Update max timestamp if this record is newer
                if raw_record.get("updated_at"):
                    if max_updated_at is None or raw_record["updated_at"] > max_updated_at:
                        max_updated_at = raw_record["updated_at"]

                # Increment counters
                prefix_record_count += 1
                total_records_synced += 1

                # Checkpoint every 100 records to preserve progress for large datasets
                if total_records_synced % __CHECKPOINT_INTERVAL == 0:
                    if max_updated_at:
                        state[f"last_sync_{prefix}"] = max_updated_at
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)
                    log.info(f"Synced {total_records_synced} total records ({prefix_record_count} from prefix '{prefix}')")

            # Check if we hit the batch limit
            if limit and prefix_record_count >= limit:
                hit_batch_limit = True
                log.warning(
                    f"Batch limit of {limit} reached for prefix '{prefix}'. State will NOT be updated to prevent data loss. "
                    f"Remaining files (if any) will be processed in the next sync. "
                    f"For production use, remove batch_limit or set to a high value."
                )

            # Final checkpoint after completing prefix
            # Only update state if we didn't hit an artificial batch limit to prevent skipping unprocessed files
            if not hit_batch_limit and max_updated_at:
                state[f"last_sync_{prefix}"] = max_updated_at
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
            elif hit_batch_limit:
                # Still checkpoint to save progress of other prefixes, but don't update this prefix's state
                op.checkpoint(state)

            log.info(f"Completed processing prefix '{prefix}': {prefix_record_count} records synced")

        except RuntimeError as runtime_error:
            # RuntimeError indicates unrecoverable failures (e.g., GCS connection after retries)
            log.severe(f"Fatal error processing prefix '{prefix}': {str(runtime_error)}")
            raise
        except (google_exceptions.GoogleAPIError,
                requests_exceptions.RequestException,
                KeyError,
                ValueError) as error:
            # Catch specific expected errors and re-raise with context
            log.severe(f"Error processing prefix '{prefix}': {str(error)}")
            raise RuntimeError(f"Failed to process prefix '{prefix}': {str(error)}")

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
    connector.debug(configuration=configuration)
