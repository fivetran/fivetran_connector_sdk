# Amazon S3 (Glacier-aware) Connector Example

## Connector overview
This connector extracts object metadata from an Amazon S3 bucket using the `boto3` library. It supports standard and Glacier storage classes and includes incremental sync, pagination, checkpointing, and error handling.

It is useful for tracking usage, Glacier restore statuses, and incremental file change history in data lakes or cold storage.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04 or later, Debian 10 or later, Amazon Linux 2 (arm64 or x86_64)
- AWS account with appropriate S3 access

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Full and incremental sync based on `LastModified`
- Glacier and Deep Archive support via `Restore` header
- Pagination with AWS continuation tokens
- Soft checkpointing for resumable syncs
- Robust error handling with backoff and retries
- Logs each sync’s progress and outcomes

## Configuration file
The `configuration.json` file should contain:

```json
{
  "aws_access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "aws_secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "aws_region": "<YOUR_AWS_REGION>",
  "bucket": "<YOUR_S3_BUCKET_NAME>",
  "prefix": "<OPTIONAL_S3_PREFIX>"
}
```

| Key                   | Description                                           | Required |
|------------------------|-------------------------------------------------------|----------|
| aws_access_key_id      | Your AWS access key ID                                | Yes      |
| aws_secret_access_key  | Your AWS secret access key                            | Yes      |
| aws_region             | AWS region where your bucket resides                  | Yes      |
| bucket                 | The target S3 bucket                                  | Yes      |
| prefix                 | Object prefix (folder) filter                         | No       |

Note: Do not commit this file to source control.

## Requirements file
This connector requires the following libraries:

```
boto3==1.40.59
botocore==1.40.59
```

Note: `fivetran_connector_sdk` and `requests` are pre-installed in the Fivetran runtime and should not be listed.

## Authentication
AWS credentials are passed via `boto3.session.Session` initialized using the fields from `configuration.json`.

Refer to: `boto3.session.Session(...)` in the connector setup block.

IAM permissions required:
- `s3:ListBucket`
- `s3:GetObject`
- `s3:HeadObject`
- `s3:RestoreObject` (optional, for Glacier restores)

## Pagination
Pagination is handled using `list_objects_v2()` with `NextContinuationToken`.

Refer to: `def list_s3_objects(...)`

## Data handling
- Uses `LastModified` for incremental sync
- Checks storage class; if Glacier, inspects restore status via `head_object`
- Extracts and syncs:
  - Key
  - Size
  - ETag
  - Storage Class
  - LastModified
  - Restore Status / Expiry (if applicable)

Refer to: `def sync_s3_metadata(...)` and `def parse_restore_info(...)`

## Error handling
- Retries on network failures and throttling
- Logs and continues on 4xx errors
- Graceful error tracking for Glacier restore failures

Refer to: `def safe_api_call(...)`

## Tables created

### S3_OBJECTS

| Column          | Type          | Description                                     |
|------------------|---------------|-------------------------------------------------|
| key              | STRING        | Full object key                                 |
| size             | LONG          | Size in bytes                                   |
| e_tag            | STRING        | ETag (MD5 hash)                                 |
| storage_class    | STRING        | Storage class type                              |
| last_modified    | UTC_DATETIME  | Last modified timestamp                         |
| restore_status   | STRING        | Glacier restore status                          |
| restore_expiry   | UTC_DATETIME  | Glacier restore expiration time (if applicable) |
| _fivetran_deleted| BOOLEAN       | Soft delete flag                                |

## Additional considerations
This example is provided to help teams integrate AWS S3 metadata and storage class history into their data pipelines. Fivetran makes no guarantees regarding support or maintenance. For assistance, contact Support or submit improvements via pull request.