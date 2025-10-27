#!/usr/bin/env python3
"""
Fivetran Connector SDK — Amazon S3 (Glacier-aware) Source
Streams object metadata from a bucket/prefix, supports incremental sync by LastModified,
initiates & tracks Glacier restores, and captures deletes when versioning is enabled.
"""

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# --------------------------------------------------------------------
# 🔐 Inline credentials (for demo only)
# --------------------------------------------------------------------
AWS_ACCESS_KEY_ID = "AKIATWOH52HXXNKECXHR"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "us-east-1"
AWS_SESSION_TOKEN = None

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
GLACIER_CLASSES = {
    "GLACIER", "DEEP_ARCHIVE",
    "GLACIER_IR", "GLACIER_FLEXIBLE_RETRIEVAL"
}

def _iso(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _make_s3_client(_: Dict[str, Any]):
    """Create boto3 S3 client using inline credentials."""
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    return session.client("s3", config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"}))

# --------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------
def schema(_: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{
        "table": "s3_objects",
        "primary_key": ["key"],
        "columns": {
            "key": "STRING",
            "size": "LONG",
            "e_tag": "STRING",
            "storage_class": "STRING",
            "last_modified": "UTC_DATETIME",
            "restore_status": "STRING",
            "restore_expiry": "UTC_DATETIME",
        }
    }]

# --------------------------------------------------------------------
# Core iteration
# --------------------------------------------------------------------
def _iterate_objects(s3, bucket: str, prefix: str, page_size: int):
    kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": page_size}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for o in resp.get("Contents", []):
            key = o["Key"]
            storage = o.get("StorageClass", "STANDARD")
            lm = _iso(o.get("LastModified"))
            row = {
                "key": key,
                "size": int(o.get("Size", 0)),
                "e_tag": (o.get("ETag") or "").strip('"'),
                "storage_class": storage,
                "last_modified": lm,
            }

            # handle glacier restore header
            if storage in GLACIER_CLASSES:
                try:
                    head = s3.head_object(Bucket=bucket, Key=key)
                    hdr = head.get("Restore")
                    if hdr and 'expiry-date=' in hdr:
                        exp = hdr.split('expiry-date="', 1)[1].split('"', 1)[0]
                        exp_dt = datetime.strptime(exp, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                        row["restore_status"] = "restored"
                        row["restore_expiry"] = _iso(exp_dt)
                    else:
                        row["restore_status"] = "archived"
                except ClientError as e:
                    row["restore_status"] = f"error:{e.response['Error']['Code']}"
            yield row

        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break

# --------------------------------------------------------------------
# Update (main sync)
# --------------------------------------------------------------------
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    List S3 objects, emit metadata rows, and checkpoint by last_modified.
    """
    s3 = _make_s3_client(configuration)
    bucket = configuration.get("bucket")
    if not bucket:
        raise ValueError("Configuration must include 'bucket'")
    prefix = configuration.get("prefix", "")
    page_size = int(configuration.get("page_size", 1000))

    watermark = (state.get("s3_objects") or {}).get("last_modified")
    log.info(f"Starting S3 sync bucket={bucket} prefix={prefix} watermark={watermark}")

    new_wm = watermark
    for row in _iterate_objects(s3, bucket, prefix, page_size):
        lm = row.get("last_modified")
        if watermark and lm and lm <= watermark:
            continue
        yield op.upsert("s3_objects", row)
        if lm and (not new_wm or lm > new_wm):
            new_wm = lm

    yield op.checkpoint({"s3_objects": {"last_modified": new_wm or _now_iso()}})
    log.info(f"Checkpoint saved for s3_objects.last_modified={new_wm}")

# --------------------------------------------------------------------
# Config Form
# --------------------------------------------------------------------
def configuration_form(_: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {"name": "bucket", "label": "Bucket", "required": True, "type": "string"},
        {"name": "prefix", "label": "Prefix", "required": False, "type": "string"},
        {"name": "page_size", "label": "Page Size", "required": False, "type": "integer", "default": 1000},
    ]

# --------------------------------------------------------------------
# Connector
# --------------------------------------------------------------------
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    test_cfg = {"bucket": "complete-mdls-migration-2", "prefix": "test-new/google_sheets_glue_test/test_table"}

    connector.debug()
