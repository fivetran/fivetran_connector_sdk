"""
This connector demonstrates how to fetch object and restore metadata from AWS S3 (including Glacier-compatible storage classes) and upsert it into a Fivetran destination using the Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""


import boto3  # AWS SDK for Python used to create clients/resources (e.g., S3, CloudWatch)
from botocore.config import Config as BotoConfig  # Configuration object to control boto3 client behavior (retries, timeouts, region, etc.)
from botocore.exceptions import ClientError  # Exception type for handling AWS service errors returned by boto3
from datetime import datetime, timezone  # Utilities for working with timezone-aware UTC timestamps
from typing import Any, Dict, List  # Type hints for generic values, dictionaries and lists

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import json # For reading configuration from JSON file


__GLACIER_CLASSES = {
    "GLACIER", "DEEP_ARCHIVE",
    "GLACIER_IR", "GLACIER_FLEXIBLE_RETRIEVAL"
}

def iso(dt):
    """
    Convert datetime or string to ISO 8601 UTC string, or return None.
    Args:
        dt: Datetime object or string to convert.
    Returns:
        ISO 8601 formatted string or None if input is falsy.
    """
    if not dt:
        return None
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()

def now_iso():
    """
    Get current time as ISO 8601 UTC string.
    Returns:
        Current timestamp in ISO 8601 format.
    """
    return datetime.now(timezone.utc).isoformat()

def make_s3_client(configuration: Dict[str, Any]):
    """
    Create boto3 S3 client using inline credentials.
    Args:
        configuration: Connector configuration dictionary.
    Returns:
        S3 client object.
    """
    session = boto3.session.Session(
        aws_access_key_id=configuration.get("aws_access_key_id"),
        aws_secret_access_key=configuration.get("aws_secret_access_key"),
        aws_session_token=configuration.get("aws_session_token", ""),
        region_name=configuration.get("aws_region"),
    )
    return session.client("s3", config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"}))


def schema(configuration) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
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


def iterate_objects(s3, bucket: str, prefix: str, page_size: int):
    """
    Iterate S3 objects in bucket/prefix, return metadata rows.
    Handles Glacier restore status via head_object calls.
    Args:
        s3: S3 client object.
        bucket: S3 bucket name.
        prefix: S3 prefix to list.
        page_size: Number of objects to fetch per API call.
    Returns: Dict[str, Any]: Object metadata row.
    """

    kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": page_size}
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

            # handle glacier restore header
            if storage in __GLACIER_CLASSES:
                try:
                    head = s3.head_object(Bucket=bucket, Key=key)
                    hdr = head.get("Restore")
                    if hdr and 'expiry-date=' in hdr:
                        exp = hdr.split('expiry-date="', 1)[1].split('"', 1)[0]
                        exp_dt = datetime.strptime(exp, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                        row["restore_status"] = "restored"
                        row["restore_expiry"] = iso(exp_dt)
                    else:
                        row["restore_status"] = "archived"
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "UnknownError")
                    log.warning(f"Failed to get restore status for {key}: {error_code}")
                    row["restore_status"] = f"error:{error_code}"
                return row

        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break

def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
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
    for row in _iterate_objects(s3, bucket, prefix, page_size):
        lm = row.get("last_modified")
        if watermark and lm and lm <= watermark:
            continue

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert("s3_objects", row)
        if lm and (not new_wm or lm > new_wm):
            new_wm = lm

    if new_wm:
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint({"s3_objects": {"last_modified": new_wm}})

        log.info(f"Checkpoint saved for s3_objects.last_modified={new_wm}")


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
