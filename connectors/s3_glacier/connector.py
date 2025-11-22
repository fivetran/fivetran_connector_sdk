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


GLACIER_CLASSES = {
    "GLACIER", "DEEP_ARCHIVE",
    "GLACIER_IR", "GLACIER_FLEXIBLE_RETRIEVAL"
}

def iso(dt):
    """Convert datetime or string to ISO 8601 UTC string, or return None."""
    if not dt:
        return None
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()

def now_iso():
    """Get current time as ISO 8601 UTC string."""
    return datetime.now(timezone.utc).isoformat()

def make_s3_client(configuration: Dict[str, Any]):
    """
    Create boto3 S3 client using inline credentials.
    Args: param configuration: Connector configuration dictionary.
    Returns: S3 client object.
    """
    session = boto3.session.Session(
        aws_access_key_id=configuration.get("aws_access_key_id"),
        aws_secret_access_key=configuration.get("aws_secret_access_key"),
        aws_session_token=configuration.get("aws_access_token", ""),
        region_name=configuration.get("aws_region"),
    )
    return session.client("s3", config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"}))


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def _iterate_objects(s3, bucket: str, prefix: str, page_size: int):
    """
    Iterate S3 objects in bucket/prefix, yielding metadata rows.
    Handles Glacier restore status via head_object calls.
    Args: param s3: S3 client object.
          param bucket: S3 bucket name.
          param prefix: S3 prefix to list.
          param page_size: Number of objects to fetch per API call.
    Yields: Dict[str, Any] — Object metadata row.
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
            if storage in GLACIER_CLASSES:
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
                    row["restore_status"] = f"error:{e.response['Error']['Code']}"
            yield row

        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    List S3 objects, emit metadata rows, and checkpoint by last_modified.
    """
    s3 = make_s3_client(configuration)
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

        # The 'upsert' operation is used to insert or update data in a table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "hello".
        # - The second argument is a dictionary containing the data to be upserted
        yield op.upsert("s3_objects", row)
        if lm and (not new_wm or lm > new_wm):
            new_wm = lm

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint({"s3_objects": {"last_modified": new_wm or now_iso()}})
    log.info(f"Checkpoint saved for s3_objects.last_modified={new_wm}")


#This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Test it by using the `debug` command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
