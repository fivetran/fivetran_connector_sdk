# Apache Druid Connector Example

## Connector overview
This connector integrates with Apache Druid to synchronize datasource data into your destination. It supports incremental sync using timestamp-based filtering and handles pagination automatically. The connector uses Druid's SQL API for querying data and implements robust error handling with retry logic for production use.

Apache Druid is a real-time analytics database designed for fast aggregations and queries on large event datasets. This connector provides access to your Druid datasources for analytics and reporting.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- SQL API integration: Uses Druid's SQL endpoint for flexible data querying
- Incremental sync: Fetches only new data since the last sync using timestamp filtering
- Multi-datasource support: Can sync multiple Druid datasources in a single connector
- Automatic pagination: Handles large datasets with configurable batch sizes
- State management: Tracks sync progress per datasource for reliable resumption
- Retry logic: Automatic retry with exponential backoff for transient errors
- Checkpointing: Saves progress periodically to enable safe resumption
- Basic authentication: Optional username/password authentication support

## Configuration file
The configuration file contains the Druid connection details required to connect to your Druid cluster.

```json
{
  "host": "localhost",
  "port": "8888",
  "datasources": "wikipedia,koalas",
  "username": "",
  "password": "",
  "use_https": "false"
}
```

For Druid with authentication, provide your credentials:
```json
{
  "host": "druid.example.com",
  "port": "8888",
  "datasources": "events,metrics",
  "username": "your_username",
  "password": "your_password",
  "use_https": "true"
}
```

Configuration parameters:
- `host` (required): Hostname or IP address of your Druid router/broker node
- `port` (required): Port number for the Druid router/broker (typically 8888)
- `datasources` (required): Comma-separated list of Druid datasource names to sync
- `username` (optional): Username for basic authentication (if required)
- `password` (optional): Password for basic authentication (if required)
- `use_https` (optional): Set to "true" to use HTTPS, "false" for HTTP (default: "false")

Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector uses only standard libraries and pre-installed packages.

The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector supports optional basic authentication for Druid clusters that require credentials.

### No authentication
If your Druid cluster does not require authentication, leave the `username` and `password` fields empty in the configuration.

### Basic authentication
If your Druid cluster uses basic authentication:
1. Provide your `username` in the configuration
2. Provide your `password` in the configuration
3. The connector will automatically add the `Authorization` header with base64-encoded credentials

## Data fetching
The connector uses Druid's SQL API to query data from datasources.

### SQL queries
The connector executes SQL queries against the `/druid/v2/sql` endpoint:
- Uses `SELECT *` to fetch all columns from each datasource
- Orders results by `__time` column (Druid's primary timestamp column)
- Applies `WHERE` clause for incremental sync with timestamp filtering
- Implements pagination using `LIMIT` and `OFFSET`

### Incremental sync
The connector supports incremental synchronization to minimize data transfer:
- Tracks the last sync time per datasource in state
- Filters data using `WHERE __time > TIMESTAMP 'last_sync_time'`
- Only fetches records newer than the last successful sync

### Pagination
Data is fetched in batches to handle large datasets:
- Batch size: 1000 records per request (configurable via `__BATCH_SIZE`)
- Uses SQL `OFFSET` for pagination
- Continues until fewer records than batch size are returned

## Error handling
The connector implements comprehensive error handling with retry logic.

### Retry strategy
- Maximum retries: 3 attempts for failed requests
- Exponential backoff: Waits 2^attempt seconds between retries (2s, 4s, 8s)
- Timeout: 30 seconds per request
- Fail fast on 4xx errors (client errors like auth failures)
- Retry on 5xx errors (server errors)

### Error categories
- Configuration errors: Validated at sync start with clear error messages
- HTTP errors: Handled based on status code (4xx vs 5xx)
- Network errors: Retried with exponential backoff
- Timeout errors: Retried up to max attempts

## Tables created
The connector creates one table per Druid datasource specified in the configuration. Column types are inferred from the data returned by Druid's SQL API and may vary based on the actual values received.

### Table naming
- Table names are derived from datasource names
- Hyphens are replaced with underscores (e.g., `my-data` becomes `my_data`)
- Each table uses `__time` as the primary key (Druid's timestamp column)

### Common columns
Druid datasources typically include:
- `__time`: Timestamp column (primary key) - when the event occurred
- Dimension columns: String or numeric attributes of the data
- Metric columns: Aggregated numeric values

The actual columns depend on your Druid datasource schema.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Druid-specific considerations
- Ensure your Druid broker/router is accessible from the connector environment
- The `__time` column must be present in your datasources for incremental sync
- Large historical syncs may take time depending on data volume
- Consider your Druid cluster's query capacity when setting batch sizes

### Running the connector
To run the connector locally for testing:

```bash
fivetran debug --configuration configuration.json
```

For production deployment, follow the [Fivetran Connector SDK deployment guide](https://fivetran.com/docs/connectors/connector-sdk/deployment).

### Example use cases
- Real-time analytics: Sync event data from Druid for real-time dashboards
- Historical analysis: Pull historical data from Druid datasources for long-term analysis
- Data integration: Combine Druid data with other sources in your data warehouse
- Backup and archival: Create backups of Druid datasources in your destination
- Cross-platform analytics: Analyze Druid data alongside data from other systems

### Troubleshooting

#### Connection errors
- Verify `host` and `port` are correct
- Check that the Druid broker/router is accessible from your network
- Test connectivity: `curl http://your-druid-host:8888/status`

#### Authentication errors
- Verify `username` and `password` are correct
- Check if your Druid cluster requires authentication
- Ensure basic auth is properly configured in Druid

#### Empty results
- Verify datasource names in `datasources` configuration are correct
- Check that datasources exist: Query `SELECT * FROM INFORMATION_SCHEMA.TABLES`
- Ensure your Druid cluster has data for the time range being queried

#### Performance issues
- Reduce `__BATCH_SIZE` if queries are timing out
- Consider syncing fewer datasources per connector instance
- Check Druid query performance and cluster health

### Related examples

Database connectors:
- [ClickHouse](../clickhouse/) - Another columnar analytics database connector with similar patterns
- [TimescaleDB](../timescale_db/) - Time-series database connector with timestamp-based incremental sync

Authentication patterns:
- [HTTP Basic](../../examples/common_patterns_for_connectors/authentication/http_basic) - Basic authentication implementation example

### Resources
- [Apache Druid Documentation](https://druid.apache.org/docs/latest/)
- [Druid SQL API](https://druid.apache.org/docs/latest/querying/sql.html)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [Druid Query Best Practices](https://druid.apache.org/docs/latest/operations/api-reference.html)
