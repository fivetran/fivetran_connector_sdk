# EHI at Scale - Electronic Health Information

## Connector overview

This enhanced connector is specifically designed to handle massive healthcare datasets (like EHR system data, for example Epic Caboodle) with intelligent table size categorization and adaptive processing strategies. It can handle 1+ billion row tables without timeouts or hangs, prevents memory overflow on large datasets, has automati deadlock detection and timeout recovery, and provides visibility into your syncs' progress and status.

### How it works
The connector uses the following strategies to sync large datasets efficiently:
- Automatically groups tables by size (small/medium/large). 
- Uses different strategies for each table size category.
- Processes small tables first for quick wins, and large tables last for safety.
- Never exceeds 4 threads and adapts based on table size.

### Processing strategy
| Table Size | Threads | Batch Size | Timeout | Checkpoint |
|------------|---------|------------|---------|------------|
| < 1M rows | 4 | 5K | 3 hours | 1M records |
| 1M-50M rows | 2 | 2.5K | 6 hours | 500K records |
| 50M+ rows | 1 | 1K | 12 hours | 100K records |

## Features

### Adaptive processing
- Table Size Optimization: Automatically adjusts processing parameters based on table size
- Resource Monitoring: Real-time system resource monitoring with automatic parameter adjustment (disabled)
- Processing Order: Optimized table processing order (small → medium → large tables)

### Advanced capabilities
- Incremental Sync: Efficient incremental synchronization with proper state management
- Deadlock Handling: Automatic deadlock detection and recovery
- Connection Management: Robust connection handling with automatic reconnection
- Parallel Processing: Multi-threaded processing for large tables
- Memory Management: Intelligent memory usage optimization

### AI/ML optimizations
- Schema Evolution: Handles dynamic schema changes common in AI/ML data
- High-Volume Processing: Optimized for large datasets typical in ML pipelines
- Feature Engineering: Efficient handling of wide tables with many features
- Time-Series Data: Optimized processing for time-series data patterns

## Architecture overview

### System architecture diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           FIVETRAN CONNECTOR SDK                                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           CONNECTOR INTERFACE                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   Schema()      │  │   Update()      │  │   Configuration Validation      │  │
│  │   Discovery     │  │   Processing    │  │   & String Conversion           │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        TABLE SIZE ANALYSIS ENGINE                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   get_table_    │  │   categorize_   │  │   display_processing_plan()     │  │
│  │   sizes()       │  │   and_sort_     │  │   - Shows detailed breakdown    │  │
│  │   - Single      │  │   tables()      │  │   - Groups by size category     │  │
│  │   efficient     │  │   - Small       │  │   - Displays processing order   │  │
│  │   query         │  │   - Medium      │  │   - Summary statistics          │  │
│  │                 │  │   - Large       │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        ADAPTIVE PARAMETER ENGINE                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   get_adaptive_ │  │   get_adaptive_ │  │   get_adaptive_                 │  │
│  │   partition_    │  │   batch_size()  │  │   threads()                     │  │
│  │   size()        │  │   - 5K/2.5K/1K  │  │   - 4/2/1 threads               │  │
│  │   - 50K/25K/5K  │  │   based on size │  │   based on size                 │  │
│  │   based on size │  │                 │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   get_adaptive_ │  │   get_adaptive_ │  │   get_adaptive_                 │  │
│  │   queue_size()  │  │   timeout()     │  │   checkpoint_interval()         │  │
│  │   - 10K/5K/1K   │  │   - 3/6/12      │  │   - 1M/500K/100K records        │  │
│  │   based on size │  │   hours         │  │   based on size                 │  │
│  │                 │  │                 │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        CONNECTION MANAGEMENT                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   Connection    │  │   Automatic     │  │   Error Detection               │  │
│  │   Manager       │  │   Reconnection  │  │   - Deadlock patterns           │  │
│  │   - Context     │  │   - Timeout     │  │   - Timeout patterns            │  │
│  │   manager       │  │   based         │  │   - Custom exceptions           │  │
│  │   - Thread-safe │  │   - Adaptive    │  │                                 │  │
│  │   - Connection  │  │   timeouts      │  │                                 │  │
│  │   pooling       │  │                 │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING STRATEGIES                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   Small Tables  │  │   Medium Tables │  │   Large Tables                  │  │
│  │   (<1M rows)    │  │   (1M-50M rows) │  │   (50M+ rows)                   │  │
│  │   - 4 threads   │  │   - 2 threads   │  │   - 1 thread                    │  │
│  │   - 5K batches  │  │   - 2.5K batches│  │   - 1K batches                  │  │
│  │   - 50K parts   │  │   - 25K parts   │  │   - 5K parts                    │  │
│  │   - Quick wins  │  │   - Balanced    │  │   - Safe processing             │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA PROCESSING PIPELINE                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   Full Load     │  │   Incremental   │  │   Error Handling                │  │
│  │   - Partitioned │  │   Sync          │  │   - Retry with backoff          │  │
│  │   - Threaded    │  │   - Change      │  │   - State persistence           │  │
│  │   - Queue-based │  │   detection     │  │   - Checkpointing               │  │
│  │   - Batch proc  │  │   - Upsert/     │  │   - Progress tracking           │  │
│  │                 │  │   Delete        │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT & VALIDATION                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │   Fivetran      │  │   Validation    │  │   Progress Logging              │  │
│  │   Operations    │  │   Records       │  │   - Real-time updates           │  │
│  │   - Upsert      │  │   - Count       │  │   - Processing status           │  │
│  │   - Delete      │  │   validation    │  │   - Error reporting             │  │
│  │   - Checkpoint  │  │   - Record      │  │   - Performance metrics         │  │
│  │                 │  │   tracking      │  │                                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Processing flow architecture

```
START
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SCHEMA DISCOVERY                             │
│  • Get table list from database                                 │
│  • Discover columns and primary keys                            │
│  • Validate configuration                                       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TABLE SIZE ANALYSIS                          │
│  • Single efficient query for all table sizes                   │
│  • Categorize: Small (<1M), Medium (1M-50M), Large (50M+)       │
│  • Sort by category and size for optimal processing order       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING PLAN DISPLAY                      │
│  • Show detailed breakdown by category                          │
│  • Display row counts and processing order                      │
│  • Summary statistics and estimated processing time             │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SEQUENTIAL PROCESSING                        │
│  • Process Small Tables First (Quick Wins)                      │
│  • Then Medium Tables (Balanced Approach)                       │
│  • Finally Large Tables (Safe Processing)                       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ADAPTIVE PROCESSING                          │
│  • Small: 4 threads, 5K batches, 50K partitions                 │
│  • Medium: 2 threads, 2.5K batches, 25K partitions              │
│  • Large: 1 thread, 1K batches, 5K partitions                   │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ERROR HANDLING & RECOVERY                    │
│  • Deadlock detection and retry                                 │
│  • Connection timeout handling                                  │
│  • Exponential backoff with jitter                              │
│  • State persistence and checkpointing                          │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATION & OUTPUT                          │
│  • Record count validation                                      │
│  • Progress tracking and logging                                │
│  • Fivetran operations (upsert, delete, checkpoint)             │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
END
```

## Configuration

### Available parameters

```json
{
    "mssql_server": "<YOUR_SQL_SERVER_HOST_NAME>",
    "mssql_cert_server": "<YOUR_SQL_SERVER_CERTIFICATE_SERVER>",
    "mssql_post": "<YOUR_SQL_SERVER_PORT>",
    "mssql_database": "<YOUR_SQL_SERVER_DATABASE>",
    "mssql_user": "<YOUR_SQL_SERVER_DATABASE_USER>",
    "mssql_password": "<YOUR_SQL_SERVER_USER_PASSWORD>",
    "cert": "<YOUR_SQL_SERVER_CERTIFICATE>",
    "threads": "<YOUR_THREAD_COUNT>",
    "max_queue_size": "<YOUR_MAX_QUEUE_SIZE",
    "max_retries": "<YOUR_MAX_RETRIES>",
    "retry_sleep_seconds": "<YOUR_RETRY_SLEEP_IN_SECONDS>",
    "debug": "<YOUR_DEBUG_FLAG>"

}
```

### Required parameters

```json
{
    "mssql_server": "<YOUR_SQL_SERVER_HOST_NAME>",
    "mssql_cert_server": "<YOUR_SQL_SERVER_CERTIFICATE_SERVER>",
    "mssql_post": "<YOUR_SQL_SERVER_PORT>",
    "mssql_database": "<YOUR_SQL_SERVER_DATABASE>",
    "mssql_user": "<YOUR_SQL_SERVER_DATABASE_USER>",
    "mssql_password": "<YOUR_SQL_SERVER_USER_PASSWORD>"

}
```

### Optional parameters

```json
{
    "cert": "<YOUR_SQL_SERVER_CERTIFICATE>",
    "threads": "<YOUR_THREAD_COUNT>",
    "max_queue_size": "<YOUR_MAX_QUEUE_SIZE",
    "max_retries": "<YOUR_MAX_RETRIES>",
    "retry_sleep_seconds": "<YOUR_RETRY_SLEEP_IN_SECONDS>",
    "debug": "<YOUR_DEBUG_FLAG>"
}
```

## Threshold optimization guide

### Table size thresholds

The connector uses adaptive processing based on table size thresholds:

- SMALL_TABLE_THRESHOLD (1M rows): Tables smaller than this use maximum parallelism
- LARGE_TABLE_THRESHOLD (50M rows): Tables larger than this use minimal parallelism

### AI/ML data adjustments

For AI/ML data pipelines, consider these threshold adjustments:

- Wide Tables (many features): Reduce `SMALL_TABLE_THRESHOLD` by 25%
- High Cardinality Data: Increase `LARGE_TABLE_THRESHOLD` by 50%
- Sparse Data: Reduce both thresholds by 50%
- Time-Series Data: Use default thresholds (work well as-is)

### Resource monitoring thresholds

When `psutil` is available, the connector monitors:

- **Memory usage**: 
  - High: 80% (triggers batch size reduction)
  - Critical: 90% (triggers aggressive reduction)
- **CPU usage**:
  - High: 85% (triggers thread reduction)
  - Critical: 95% (triggers aggressive reduction)

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your database connection in `config.json`

3. Test the connector:
```bash
fivetran debug --configuration config.json
```

## Usage

### Basic usage

```python
from connector import connector

# Test locally
if __name__ == "__main__":
    with open("config.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
```

## What to expect

### Startup process
```
1. Schema Discovery - Gets list of all tables
2. Table Size Analysis - Counts rows in each table (single efficient query)
3. Categorization - Groups tables by size (small/medium/large)
4. Processing Plan Display - Shows detailed breakdown of what will be processed
5. Sequential Processing - Processes tables in optimal order
```

### Processing plan output
```
================================================================================
SYNC PROCESSING PLAN
================================================================================

SMALL TABLES (200+ tables, <1M rows each):
--------------------------------------------------
   1. `PATIENT`                                  10,100,444 rows
   2. `VISIT`                                    10,555,777 rows
   3. `ENCOUNTER`                                 6,888,122 rows
   ... and 200+ more small tables

MEDIUM TABLES (50 tables, 1M-50M rows each):
--------------------------------------------------
   1. `COMPONENT`                        29,444,333 rows
   2. `TESTCOMPONENT`                    28,555,478 rows
   3. `ERROREVENT`                 27,333,896 rows
   ... and 50 more medium tables

LARGE TABLES (15 tables, 50M+ rows each):
--------------------------------------------------
   1. `FLOWSHEETVALUE`                        1,644,444,333 rows
   2. `USERLOGACTIVITY`               912,123,436 rows
   3. `PATIENTACTION`                600,541,489 rows
   ... and 15 more large tables

================================================================================
SUMMARY STATISTICS
================================================================================
Total tables: 265
Total rows: 9,444,458,452
Small tables: 200 tables, 2,456,789,123 rows (25.1%)
Medium tables: 50 tables, 1,789,123,456 rows (12.6%)
Large tables: 15 tables, 5,123,456,789 rows (62.3%)
================================================================================
```

### Real-time progress updates
```
Processing table 1/265: `PATIENT` (small, 10,455,551 rows)
Table `PATIENT`: 10,455,551 rows, 210 partitions, 4 threads, 10000 queue size, 50,000 partition size, 5,000 batch size, 1,000,000 checkpoint interval
Successfully processed table `PATIENT`: 10,511,557 records
Completed 1/265 tables. Next: `VISIT`

Processing table 5/265: `LABRESULT` (medium, 29,123,456 rows)
Table `LABRESULT`: 29,123,456 rows, 1,168 partitions, 2 threads, 5000 queue size, 25,000 partition size, 2,500 batch size, 500,000 checkpoint interval
...
```

## Processing strategy

### Table categorization

The connector categorizes tables into three processing tiers:

1. **Small Tables** (<1M rows)
   - Maximum parallelism (4 threads)
   - Large batch sizes (5K records)
   - Quick processing priority

2. **Medium Tables** (1M-50M rows)
   - Moderate parallelism (2 threads)
   - Medium batch sizes (2.5K records)
   - Balanced resource usage

3. **Large Tables** (50M+ rows)
   - Single-threaded processing
   - Small batch sizes (1K records)
   - Memory-conservative approach

### Processing order

Tables are processed in optimal order:
1. Small tables first (quick wins)
2. Medium tables second (balanced processing)
3. Large tables last (resource-intensive)

## Error handling

### Automatic recovery

- **Deadlock Detection**: Automatic detection and retry with exponential backoff
- **Connection Timeouts**: Automatic reconnection with adaptive timeouts
- **Resource Pressure**: Automatic parameter adjustment based on system resources

### Retry logic

- **Max Retries**: Configurable retry attempts (default: 5)
- **Exponential Backoff**: Intelligent backoff with jitter
- **Error Classification**: Different handling for different error types

## Monitoring and validation

### Sync validation

The connector automatically creates validation records in the `CDK_VALIDATION` table:

```sql
SELECT * FROM CDK_VALIDATION 
WHERE tablename = 'your_table_name' 
ORDER BY datetime DESC;
```

### Resource monitoring

When `psutil` is available, the connector logs:

- Memory usage percentage and available GB
- CPU usage percentage
- Disk usage percentage
- Automatic parameter adjustments

## Key benefits for healthcare data

### Handles massive tables
- `FLOWSHEETVALUE`: >1.9 billion rows processed safely
- `USERACTIONLOG`: >900 million rows with optimized partitioning
- `PATIENTACTION`: >800 million rows with adaptive batching

### Prevents timeouts and hangs
- Adaptive Timeouts: 3 hours for small tables, 12 hours for large tables
- Connection Management: Automatic reconnection and deadlock detection
- Memory Management: Smaller batches and queues for large tables

### Optimized performance
- Small Tables: Process quickly with 4 threads and large batches
- Medium Tables: Balanced approach with 2 threads and moderate batches
- Large Tables: Single-threaded to avoid overwhelming the database

## Advanced features

### Automatic error recovery
- Deadlock Detection: Pattern matching for deadlock errors
- Timeout Handling: Connection timeout detection and recovery
- Exponential Backoff: Intelligent retry with jitter
- State Persistence: Resume from last successful checkpoint

### Memory and resource management
- Adaptive Queue Sizes: Prevents memory overflow on large tables
- Connection Pooling: Efficient database connection management
- Batch Processing: Optimized record processing based on table size
- Checkpointing: Frequent state saves to prevent data loss
- System Resource Monitoring: Adaptive batches for every table

### Monitoring and validation
- Progress Tracking: Real-time updates on processing status
- Record Validation: Counts processed vs. expected records
- Performance Metrics: Processing time and throughput tracking
- Error Logging: Comprehensive error tracking and reporting

## Best practices

### Performance optimization

1. Monitor Resource Usage: Enable `psutil` for automatic optimization
2. Adjust Thresholds: Fine-tune thresholds based on your data characteristics
3. Use Debug Mode: Test with debug mode to understand processing behavior
4. Monitor Logs: Review logs for optimization opportunities

### AI/ML data considerations

1. Schema Evolution: The connector handles schema changes automatically
2. Feature Engineering: Optimize for wide tables with many features
3. Time-Series Optimization: Leverage built-in time-series optimizations
4. Batch Processing: Use appropriate batch sizes for your data volume

### Security

1. SSL Certificates: Proper SSL certificate configuration
2. Connection Security: Secure database connections
3. Credential Management: Secure credential storage and handling

## Troubleshooting

### Common issues

1. **Connection Failures**
   - Verify server address and port
   - Check SSL certificate configuration
   - Ensure network connectivity

2. **Memory Issues**
   - Reduce batch sizes
   - Enable resource monitoring
   - Adjust table size thresholds

3. **Performance Issues**
   - Review processing logs
   - Adjust thread counts
   - Monitor resource usage

### Debug mode

Enable debug mode for detailed logging:

```json
{
    "debug": "true"
}
```

## Notes

- Large Tables Take Time: Tables with 50M+ rows will process for hours, not minutes
- Memory Usage: Large tables use smaller batches to prevent memory issues
- Single Threading: Large tables use single-threading to avoid overwhelming the database
- Frequent Checkpoints: Large tables checkpoint every 100K records for safety
- Connection Timeouts: Large tables get longer connection timeouts (12 hours)
- Resource Monitoring: Disabled, but adaptive processing based on table size is fully functional

## Support

For issues and questions:

1. Review the [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
2. Check the [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)
3. Review connector logs for detailed error information
