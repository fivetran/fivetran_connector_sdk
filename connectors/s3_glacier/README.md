# Amazon S3 (Glacier-aware) Connector Example

## Connector overview

This connector extracts object metadata from an Amazon S3 bucket using the boto3 library. It demonstrates how to:
- List S3 objects within a bucket and prefix.
- Capture Glacier storage class restore statuses.
- Perform incremental synchronization using the `LastModified` field.
- Track and checkpoint synchronization progress for reliability.

It supports AWS S3 Standard, Infrequent Access, and Glacier-compatible storage classes, and it captures metadata such as file size, ETag, storage class, and last modification timestamps.

Authentication is performed using standard AWS IAM credentials (access key ID and secret access key, optionally a session token) with permissions to list and read objects in the configured S3 buckets (e.g. `s3:ListBucket`, `s3:GetObject`).

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Active AWS account with S3 access.
- Appropriate IAM permissions to call:
    - `s3:ListBucket`
    - `s3:GetObject`
    - `s3:HeadObject`
    - `s3:ListBucketVersions`

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features

- Connects securely to AWS S3 using `boto3`.
- Lists and retrieves metadata for all S3 objects in a bucket and prefix.
- Supports incremental synchronization using the `LastModified` field.
- Automatically handles Glacier and Deep Archive storage classes:
    - Detects Glacier objects and retrieves restoration metadata (status and expiry).
- Tracks progress via checkpoints for resumable syncs.
- Handles pagination automatically using `ContinuationToken`.
- Logs sync activity and progress for each run.

---

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "aws_access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "aws_secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "aws_region": "<YOUR_AWS_REGION>",
  "bucket": "<YOUR_S3_BUCKET_NAME>",
  "prefix": "<OPTIONAL_S3_PREFIX>"
}
```

Configuration parameters:
- `aws_access_key_id` (required) - Your AWS access key ID.
- `aws_secret_access_key` (required) - Your AWS secret access key.
- `aws_region` (required) - The AWS region where your S3 bucket is hosted.
- `bucket` (required) - The S3 bucket name.
- `prefix` (optional) - The S3 prefix (folder path) to filter objects.


Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements file

This connector requires the boto3 library to interact with AWS S3.

```
boto3==1.40.59
botocore==1.40.59
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

---

## Authentication

The connector uses AWS IAM credentials for authentication. The credentials are read from the `configuration.json` file and used to initialize a `boto3` session.

It uses the following authentication flow:
```python
session = boto3.session.Session(
    aws_access_key_id=configuration.get("aws_access_key_id"),
    aws_secret_access_key=configuration.get("aws_secret_access_key"),
    region_name=configuration.get("aws_region")
)
```

### Permissions required:
The IAM user or role running this connector must have the following S3 permissions:
- `s3:ListBucket`
- `s3:GetObject`
- `s3:HeadObject`

If you want to include Glacier restores:
- `s3:RestoreObject`

---

## Pagination

The connector uses AWS S3’s native pagination mechanism via the `list_objects_v2()` API:
- Fetches objects in batches (`MaxKeys` = page size).
- Automatically continues to the next page using the `NextContinuationToken`.
- Stops once all objects have been retrieved.

This ensures memory-efficient processing and smooth scaling for large buckets.

---

## Data handling

The connector processes S3 object metadata in the following manner:

- List Objects
    - Uses `list_objects_v2` to retrieve object metadata for a given bucket and prefix.
    - Extracts fields such as:
        - `Object Key`
        - `Size`
        - `ETag`
        - `Storage Class`
        - `LastModified timestamp`

- Handle Glacier Objects
    - For objects stored in Glacier or Deep Archive, calls `head_object()` to inspect the `Restore` header.
    - Parses restoration status and expiry date if available.

- Incremental Sync
    - Tracks the most recent `LastModified` timestamp.
    - Skips previously synced objects to prevent duplicates.

---

## Error handling

The connector implements robust error handling for:
- Network failures: Retries automatically up to 10 times with backoff.
- Client errors (4xx): Logs detailed error messages.
- Glacier restore errors: Captures and records errors in the `restore_status` column.
- API throttling: Handles `ThrottlingException` and rate limits via exponential backoff.

All errors are logged using the Fivetran SDK’s logging system for debugging and monitoring.

Example log output:
```
INFO: Starting S3 sync bucket=my-bucket prefix=logs/
WARNING: Object archived in Glacier; restore in progress
INFO: Checkpoint saved for s3_objects.last_modified=2025-02-12T18:32:55Z
```

---

## Tables created

This connector replicates S3 object metadata into a single table named `S3_OBJECTS`.

### S3_OBJECTS
| Column | Type | Description |
|---------|------|-------------|
| `key` | STRING | The full S3 object key (path). |
| `size` | LONG | Size of the object in bytes. |
| `e_tag` | STRING | The object’s ETag hash. |
| `storage_class` | STRING | Storage class (e.g., STANDARD, GLACIER). |
| `last_modified` | UTC_DATETIME | Last modified timestamp. |
| `restore_status` | STRING | Glacier restoration status (restored, archived, error:<code>). |
| `restore_expiry` | UTC_DATETIME | Expiration time of the restored object, if available. |

---

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
