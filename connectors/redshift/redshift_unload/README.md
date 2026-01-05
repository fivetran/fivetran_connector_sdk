# Redshift UNLOAD Connector Example

## Connector Overview

This example connector demonstrates how to efficiently sync large tables from Amazon Redshift by leveraging the native UNLOAD command to export data to S3 in Parquet format, then reading from S3 for ingestion into Fivetran.

This approach offers significant performance advantages over direct querying for large datasets:

- **Parallel Export**: Redshift's UNLOAD command leverages the cluster's parallel processing capabilities
- **Optimized Format**: Parquet format provides efficient columnar storage and fast reading
- **Reduced Cluster Load**: Offloads data export to Redshift's native functionality
- **Scalability**: Handles very large tables (billions of rows) efficiently


## Architecture

```
┌─────────────────┐    UNLOAD    ┌─────────────┐    Read    ┌─────────────┐
│    Redshift     │ ──────────► │     S3      │ ────────► │  Connector  │
│    Cluster      │   Parquet    │   Bucket    │  Parquet   │   (SDK)     │
└─────────────────┘              └─────────────┘            └─────────────┘
                                       │
                                       │ Cleanup
                                       ▼
                               (Files deleted after sync)
```


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Amazon Redshift cluster with UNLOAD permissions
- S3 bucket for temporary data storage
- IAM role with appropriate permissions


## Getting Started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features

- **UNLOAD-based Export**: Uses Redshift's native UNLOAD command for efficient data export
- **Parquet Format**: Exports data in Parquet format for optimal performance
- **Incremental Sync**: Supports incremental synchronization via `replication_key`
- **Automatic Schema Detection**: Discovers tables and primary keys from Redshift automatically
- **Automatic Replication Key Inference**: Infers replication keys based on column types
- **Parallel Execution**: Syncs multiple tables concurrently using `max_parallel_workers`
- **Connection Pooling**: Reduces overhead during parallel query execution
- **Periodic Checkpointing**: Saves progress every `CHECKPOINT_EVERY_ROWS`
- **S3 Cleanup**: Automatically removes temporary files after sync
- **Graceful Fallback**: Falls back to complete resync when no suitable replication key is found


## Configuration File

The configuration file (`configuration.json`) contains the necessary parameters to connect to Amazon Redshift and S3:

```json
{
  "redshift_host": "<YOUR_REDSHIFT_HOST>",
  "redshift_port": "<YOUR_REDSHIFT_PORT>",
  "redshift_database": "<YOUR_REDSHIFT_DATABASE>",
  "redshift_user": "<YOUR_REDSHIFT_USER>",
  "redshift_password": "<YOUR_REDSHIFT_PASSWORD>",
  "redshift_schema": "<YOUR_REDSHIFT_SCHEMA>",
  "s3_bucket": "<YOUR_S3_BUCKET_NAME>",
  "s3_region": "<YOUR_S3_REGION>",
  "s3_prefix": "<YOUR_S3_PREFIX_FOR_UNLOAD>",
  "iam_role": "<YOUR_IAM_ROLE_ARN>",
  "aws_access_key_id": "<OPTIONAL_AWS_ACCESS_KEY>",
  "aws_secret_access_key": "<OPTIONAL_AWS_SECRET_KEY>",
  "batch_size": "10000",
  "auto_schema_detection": "true",
  "enable_complete_resync": "false",
  "max_parallel_workers": "2",
  "unload_format": "PARQUET",
  "unload_parallel": "ON",
  "unload_max_filesize": "512 MB",
  "unload_cleanpath": "true",
  "cleanup_s3": "true"
}
```

### Configuration Parameters

#### Redshift Connection
| Parameter | Description | Required |
|-----------|-------------|----------|
| `redshift_host` | The hostname of the Redshift cluster | Yes |
| `redshift_port` | The port number for the Redshift cluster (default: 5439) | Yes |
| `redshift_database` | The name of the Redshift database | Yes |
| `redshift_user` | The username for Redshift authentication | Yes |
| `redshift_password` | The password for Redshift authentication | Yes |
| `redshift_schema` | The schema to extract data from | Yes |

#### S3 Configuration
| Parameter | Description | Required |
|-----------|-------------|----------|
| `s3_bucket` | S3 bucket name for UNLOAD output | Yes |
| `s3_region` | AWS region for S3 bucket (default: us-east-1) | No |
| `s3_prefix` | Prefix path within the bucket for UNLOAD files | No |
| `iam_role` | IAM role ARN for Redshift to access S3 | Yes |
| `aws_access_key_id` | AWS access key for S3 operations (optional if using IAM role) | No |
| `aws_secret_access_key` | AWS secret key for S3 operations (optional if using IAM role) | No |

#### Sync Behavior
| Parameter | Description | Required |
|-----------|-------------|----------|
| `batch_size` | Number of records to process per batch | Yes |
| `auto_schema_detection` | Enable automatic table and schema discovery (`true`/`false`) | Yes |
| `enable_complete_resync` | Force full resync for all tables (`true`/`false`) | Yes |
| `max_parallel_workers` | Maximum concurrent table syncs (recommended: 2-4) | No |

#### UNLOAD Options
| Parameter | Description | Default |
|-----------|-------------|---------|
| `unload_format` | Output format (`PARQUET` or `CSV`) | `PARQUET` |
| `unload_parallel` | Enable parallel unload (`ON`/`OFF`) | `ON` |
| `unload_max_filesize` | Maximum size per output file | `512 MB` |
| `unload_cleanpath` | Clean existing files before unload (`true`/`false`) | `true` |
| `cleanup_s3` | Delete S3 files after sync (`true`/`false`) | `true` |

**Note**: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## IAM Role Requirements

The IAM role specified in `iam_role` must have the following permissions:

### Redshift UNLOAD Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetBucketLocation",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::<your-bucket>/*",
        "arn:aws:s3:::<your-bucket>"
      ]
    }
  ]
}
```

### Connector S3 Access Permissions
If using explicit AWS credentials instead of IAM role for the connector:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::<your-bucket>/*",
        "arn:aws:s3:::<your-bucket>"
      ]
    }
  ]
}
```


## Requirements File

The connector requires the following packages, which should be listed in the `requirements.txt` file:

```
redshift_connector==2.1.8
boto3==1.35.0
pyarrow==17.0.0
```

**Note**: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. Do not declare them in your `requirements.txt` to avoid dependency conflicts.


## Data Handling

The connector uses the following workflow:

1. **Schema Discovery**: Connects to Redshift and discovers tables and columns
2. **UNLOAD Execution**: For each table, executes UNLOAD command to export data to S3 as Parquet
3. **S3 Reading**: Reads Parquet files from S3 using PyArrow for efficient parsing
4. **Data Sync**: Upserts records to Fivetran using the connector SDK
5. **Checkpointing**: Periodically saves sync progress for resume capability
6. **Cleanup**: Deletes temporary S3 files after successful sync

### UNLOAD Command

The connector generates UNLOAD commands in the following format:

```sql
UNLOAD ('SELECT columns FROM schema.table WHERE replication_key > bookmark ORDER BY replication_key')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn:aws:iam::account:role/role-name'
FORMAT AS PARQUET
PARALLEL ON
MAXFILESIZE 512 MB
CLEANPATH
```


## Table Specifications

When automatic schema detection is disabled, you can define table specifications in `table_specs.py`:

```python
TABLE_SPECS = [
    {
        "name": "schema.table_name",
        "primary_keys": ["id"],
        "strategy": "INCREMENTAL",  # or "FULL"
        "replication_key": "updated_at",
        "include": [],  # Empty = all columns
        "exclude": ["sensitive_column"],
    },
]
```


## Performance Tuning

### Recommended Settings for Large Tables

| Setting | Small Tables (<1M rows) | Medium Tables (1M-100M rows) | Large Tables (>100M rows) |
|---------|------------------------|------------------------------|---------------------------|
| `batch_size` | 50000 | 25000 | 10000 |
| `max_parallel_workers` | 4 | 2-3 | 1-2 |
| `unload_max_filesize` | 256 MB | 512 MB | 1 GB |

### Best Practices

1. **IAM Role**: Use IAM role instead of explicit credentials when possible
2. **S3 Bucket Location**: Use an S3 bucket in the same region as your Redshift cluster
3. **Parallel Workers**: Start with 2 workers and increase if needed
4. **Checkpoint Frequency**: Adjust `CHECKPOINT_EVERY_ROWS` in `table_specs.py` based on data volume


## Error Handling

The connector includes robust error handling:

- **Connection Failures**: Automatic retry with configurable timeout
- **UNLOAD Failures**: Detailed error logging with SQL command information
- **S3 Access Issues**: Clear error messages for permission problems
- **Data Type Handling**: Automatic conversion of Redshift types to Fivetran types


## Tables Created

The connector creates tables in the destination based on the source schema. Table names follow the format `<schema_name>_<table_name>`.

The connector automatically maps Redshift data types to Fivetran semantic types:

| Redshift Type | Fivetran Type |
|---------------|---------------|
| `date` | `NAIVE_DATE` |
| `timestamp` | `NAIVE_DATETIME` |
| `timestamp with time zone` | `UTC_DATETIME` |
| `super` | `JSON` |


## Additional Files

- `connector.py`: Main connector entry point with schema and update functions
- `redshift_client.py`: Redshift connection, UNLOAD execution, and table plan building
- `s3_client.py`: S3 operations for listing, reading Parquet files, and cleanup
- `table_specs.py`: Table specifications and configuration constants


## Troubleshooting

### Common Issues

1. **UNLOAD Permission Denied**
   - Ensure the IAM role has `s3:PutObject` permission on the target bucket
   - Verify the IAM role is attached to your Redshift cluster

2. **Empty Sync Results**
   - Check if `auto_schema_detection` is enabled and the schema exists
   - Verify table names in `TABLE_SPECS` match the Redshift schema

3. **Slow Performance**
   - Reduce `max_parallel_workers` to decrease cluster load
   - Increase `batch_size` for fewer I/O operations
   - Ensure S3 bucket is in the same region as Redshift


## Additional Considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
