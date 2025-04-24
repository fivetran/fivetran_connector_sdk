# Parallel Fetching from Source Example

This example demonstrates how to fetch data from multiple files in an AWS S3 bucket in parallel using ThreadPoolExecutor, and stream the data efficiently without loading entire files into memory at once.

### Configuration

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "aws_access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "aws_secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "region_name": "<YOUR_AWS_REGION_NAME>",
  "bucket_name": "<YOUR_AWS_BUCKET_NAME>",
  "prefix": "<YOUR_AWS_FOLDER_NAME>",
  "max_workers": "<NUMBER_OF_THREADS>"
}
```

- `aws_access_key_id` and `aws_secret_access_key`: AWS credentials with read access to your S3 bucket
- `region_name`: AWS region where your bucket is located
- `bucket_name`: Name of the S3 bucket containing your CSV files
- `prefix`: Folder path in the bucket where CSV files are stored (optional, defaults to root "/" if not specified)
- `max_workers`: Number of threads to use (optional, defaults to 4 if not specified)

### Key Functions

1. `create_s3_client(configuration)`
Creates an S3 client using boto3. Validates required configuration parameters.

2. `get_file_table_pairs(s3_client, bucket_name, prefix)`
Retrieves all CSV files from the specified S3 location and maps them to table names. You can modify this function to filter specific files or implement custom table naming logic beyond simply removing the `.csv` extension.

3. `process_file_get_stream(s3_client, bucket_name, file_key, table_name)`
Processes a single file using streaming (not loading the entire file into memory). You can modify this function to handle different file formats or add additional data transformation logic for your specific use case.

### Important Performance Considerations

- Avoid loading entire files into memory before processing, as this can cause Out of Memory errors with large files.
- You can start with the default of 4 workers and test with your specific workload. Only increase if you see clear performance benefits after testing.

> IMPORTANT : Do not set `max_workers` too high. More workers is not always better and can actually degrade performance.