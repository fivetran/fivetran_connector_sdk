"""
This connector demonstrates how to fetch object and restore metadata from AWS S3 (including Glacier-compatible storage classes) and upsert it into a Fivetran destination using the Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

import json  # For reading configuration from JSON file
from datetime import datetime, timezone  # For working with UTC timestamps
from typing import Any, Dict, List, Optional  # Type hints

import boto3  # AWS SDK for Python to interact with S3
from botocore.config import Config as BotoConfig  # For setting retry and timeout configs
from botocore.exceptions import ClientError  # Exception handling for AWS responses

from fivetran_connector_sdk import Connector  # Core SDK functionality
from fivetran_connector_sdk import Logging as log  # For logging
from fivetran_connector_sdk import Operations as op  # For data operations (upsert, checkpoint)

# Constants
_GLACIER_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "GLACIER_FLEXIBLE_RETRIEVAL"}

_MAX_RETRY_ATTEMPTS = 10  # Max retry attempts for AWS API
_CHECKPOINT_EVERY = 1000  # Frequency of checkpointing rows


def iso(dt: Optional[Any]) -> Optional[str]:
    """
    Convert datetime or string to ISO 8601 UTC string, or return None.

    Args:
        dt (Optional[Any]): datetime object or string.

    Returns:
        Optional[str]: ISO 8601 formatted string or None.
    """
    if not dt:
        return None
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()


def make_s3_client(configuration: Dict[str, Any]):
    """
    Create a boto3 S3 client using credentials and retry config.

    Args:
        configuration (Dict[str, Any]): The connector configuration.

    Returns:
        boto3.client: Configured S3 client.
    """
    session = boto3.session.Session(
        aws_access_key_id=configuration.get("aws_access_key_id"),
        aws_secret_access_key=configuration.get("aws_secret_access_key"),
        aws_session_token=configuration.get("aws_session_token", ""),
        region_name=configuration.get("aws_region"),
    )
    return session.client(
        "s3", config=BotoConfig(retries={"max_attempts": _MAX_RETRY_ATTEMPTS, "mode": "standard"})
    )


def schema(_: dict) -> List[Dict[str, Any]]:
    """
    Define the output schema for the connector.

    Args:
        _ (dict): Unused config.

    Returns:
        List[Dict[str, Any]]: Schema definition for s3_objects table.
    """
    return [
        {
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
            },
        }
    ]


def iterate_objects(s3, bucket: str, prefix: str, page_size: int):
    """
    Iterate over S3 objects using pagination and yield enriched metadata rows.

    Args:
        s3: boto3 S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix to filter objects.
        page_size (int): Max objects per request.

    Yields:
        Dict[str, Any]: Object metadata rows.

    Raises:
        ClientError: For AWS request errors.
    """
    kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": page_size}
    processed = 0
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for o in resp.get("Contents", []):
            key = o["Key"]
            storage = o.get("StorageClass", "STANDARD")
            lm = iso(o.get("LastModified"))
            row = {
                "key": key,
                "size": int(o.get("Size", 0)),
                "e_tag": (o.get("ETag") or "").strip('"'),
                "storage_class": storage,
                "last_modified": lm,
            }

            # Handle Glacier restore info
            if storage in _GLACIER_CLASSES:
                try:
                    head = s3.head_object(Bucket=bucket, Key=key)
                    hdr = head.get("Restore")
                    if hdr and "expiry-date=" in hdr:
                        exp = hdr.split('expiry-date="', 1)[1].split('"', 1)[0]
                        exp_dt = datetime.strptime(exp, "%a, %d %b %Y %H:%M:%S %Z").replace(
                            tzinfo=timezone.utc
                        )
                        row["restore_status"] = "restored"
                        row["restore_expiry"] = iso(exp_dt)
                    else:
                        row["restore_status"] = "archived"
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "UnknownError")
                    log.warning(f"Failed to get restore status for {key}: {error_code}")
                    row["restore_status"] = f"error:{error_code}"
            yield row

        processed += len(resp.get("Contents", []))
        if processed % _CHECKPOINT_EVERY == 0:
            op.checkpoint()

        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration (Dict[str, Any]): Connector configuration.
        state (Dict[str, Any]): Previous sync state.

    Raises:
        ValueError: If required configuration is missing.
    """
    s3 = make_s3_client(configuration)
    bucket = configuration.get("bucket")
    if not bucket:
        raise ValueError("Configuration must include 'bucket'")
    prefix = configuration.get("prefix", "")
    page_size = 1000  # Max allowed by S3 API

    watermark = (state.get("s3_objects") or {}).get("last_modified")
    log.info(f"Starting S3 sync bucket={bucket} prefix={prefix} watermark={watermark}")

    new_wm = watermark
    for row in iterate_objects(s3, bucket, prefix, page_size):
        lm = row.get("last_modified")
        if watermark and lm and lm < watermark:
            continue

        op.upsert("s3_objects", row)
        if lm and (not new_wm or lm > new_wm):
            new_wm = lm

    if new_wm:
        op.checkpoint({"s3_objects": {"last_modified": new_wm}})
        log.info(f"Checkpoint saved for s3_objects.last_modified={new_wm}")


# Required for SDK loader
connector = Connector(update=update, schema=schema)

# Entry point for local testing
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
