"""
MotionBlend Fivetran Connector - Syncs motion capture metadata from GCS to BigQuery

This connector ingests motion-capture (MoCap) metadata from Google Cloud Storage (GCS) 
and delivers it to BigQuery for downstream transformation and analytics. It scans folders 
containing .bvh and .fbx files, extracts key metadata, and loads structured information 
into BigQuery tables.

See the README.md for detailed documentation.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import Google Cloud libraries
from google.cloud import storage, bigquery
from google.api_core import retry
import hashlib
import time
from typing import Iterator, Dict, Any, Optional
from datetime import datetime


def schema(configuration: dict):
    """
    Define the schema for the connector tables.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Returns:
        List of table schemas with columns and primary keys
    """
    # Validate required configuration
    required_keys = ["gcp_project", "gcs_bucket", "bigquery_dataset"]
    for key in required_keys:
        if key not in configuration:
            raise RuntimeError(f"Missing required configuration value: {key}")
    
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
                "updated_at": "UTC_DATETIME"
            }
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
                "updated_at": "UTC_DATETIME"
            }
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
                # Quality metrics (computed post-ingestion)
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
                "quality_category": "STRING"
            }
        }
    ]


def list_gcs_files(bucket_name: str, prefix: str, extensions: list, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    """
    List files from GCS bucket with optional filtering.
    
    Args:
        bucket_name: GCS bucket name
        prefix: Blob prefix to scan
        extensions: List of file extensions to include (e.g., ['.bvh', '.fbx'])
        limit: Optional limit on number of files to process
        
    Yields:
        Dictionary with file metadata
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    count = 0
    for blob in blobs:
        # Skip directories
        if blob.name.endswith('/'):
            continue
        
        # Check file extension
        if not any(blob.name.lower().endswith(ext) for ext in extensions):
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
            break


def infer_category(file_uri: str) -> str:
    """
    Infer motion category from GCS URI.
    
    Args:
        file_uri: GCS URI path
        
    Returns:
        Category name: 'seed', 'build', or 'blend'
    """
    if '/seed/' in file_uri:
        return 'seed'
    elif '/build/' in file_uri:
        return 'build'
    elif '/blend/' in file_uri:
        return 'blend'
    return 'unknown'


def generate_record_id(file_uri: str) -> str:
    """
    Generate deterministic record ID from file URI using SHA-1 hash.
    
    Args:
        file_uri: GCS file URI
        
    Returns:
        16-character hash string
    """
    return hashlib.sha1(file_uri.encode()).hexdigest()[:16]


def transform_seed_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw GCS metadata into seed_motions record."""
    now = datetime.utcnow().isoformat()
    return {
        "id": generate_record_id(raw["file_uri"]),
        "file_uri": raw["file_uri"],
        "skeleton_id": "mixamo24",  # Default skeleton
        "frames": 0,  # Placeholder - would parse from BVH in production
        "fps": 30,
        "joints_count": 24,
        "created_at": now,
        "updated_at": raw.get("updated_at", now)
    }


def transform_build_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw GCS metadata into build_motions record."""
    now = datetime.utcnow().isoformat()
    return {
        "id": generate_record_id(raw["file_uri"]),
        "file_uri": raw["file_uri"],
        "skeleton_id": "mixamo24",
        "frames": 0,
        "fps": 30,
        "joints_count": 24,
        "build_method": "ganimator",
        "created_at": now,
        "updated_at": raw.get("updated_at", now)
    }


def transform_blend_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw GCS metadata into blend_motions record with quality metrics placeholders."""
    now = datetime.utcnow().isoformat()
    file_id = generate_record_id(raw["file_uri"])
    
    return {
        "id": file_id,
        "left_motion_id": f"{file_id}_left",  # Placeholder
        "right_motion_id": f"{file_id}_right",  # Placeholder
        "blend_ratio": 0.5,
        "transition_start_frame": 0,
        "transition_end_frame": 10,
        "method": "snn",
        "file_uri": raw["file_uri"],
        "created_at": now,
        "updated_at": raw.get("updated_at", now),
        # Quality metrics (populated by post-processing pipeline)
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
        "transition_smoothness": None,
        "velocity_ratio": None,
        "acceleration_ratio": None,
        "quality_score": None,
        "quality_category": None
    }


def update(configuration: dict, state: dict):
    """
    Main sync function - extracts, transforms, and loads motion capture data.
    
    Args:
        configuration: Connector configuration from configuration.json
        state: Sync state for incremental updates
        
    Yields:
        Operations to upsert records and update cursor state
    """
    # Extract configuration
    bucket = configuration.get("gcs_bucket")
    prefixes = configuration.get("gcs_prefixes", "").split(",")
    extensions = configuration.get("include_exts", ".bvh,.fbx").split(",")
    limit = configuration.get("batch_limit", 25)
    
    log.info(f"Starting sync for bucket: {bucket}")
    
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
        
        try:
            # List files from GCS
            for raw_record in list_gcs_files(bucket, prefix, extensions, limit):
                category = infer_category(raw_record["file_uri"])
                
                if category not in table_map:
                    log.warning(f"Unknown category for file: {raw_record['file_uri']}")
                    continue
                
                table_name, transform_func = table_map[category]
                
                # Transform record
                record = transform_func(raw_record)
                
                # Upsert to destination
                yield op.upsert(table_name, record)
                
                log.info(f"Synced {record['id']} to {table_name}")
            
            # Update cursor for this prefix
            yield op.checkpoint(state={
                "prefix": prefix,
                "last_sync": datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            log.error(f"Error processing prefix {prefix}: {str(e)}")
            raise
    
    log.info("Sync completed successfully")


# Create the connector instance
connector = Connector(update=update, schema=schema)


# This allows testing locally with `python connector.py`
if __name__ == "__main__":
    import sys
    connector.debug()
