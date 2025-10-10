# Complex Error Handling with Multi-Threading Example

This example demonstrates advanced error handling strategies in multi-threaded environments using the Fivetran Connector SDK. It provides a comprehensive framework for building robust, fault-tolerant connectors that can handle API failures, network issues, and processing errors gracefully.

## 🎯 Key Features

### Advanced Error Handling
- **Centralized Error Management**: Thread-safe error collection and analysis
- **Error Classification**: Severity-based error categorization (LOW, MEDIUM, HIGH, CRITICAL)
- **Intelligent Thresholds**: Per-thread and global error limits with automatic shutdown
- **Comprehensive Error Tracking**: Full error context including stack traces and retry counts

### Multi-Threading Architecture
- **ThreadPoolExecutor**: Efficient parallel processing with configurable worker pools
- **Thread Safety**: All shared resources protected with proper locking mechanisms
- **Graceful Degradation**: Individual thread failures don't crash the entire sync
- **Real-time Monitoring**: Per-thread metrics and performance tracking

### Resilient API Client
- **Circuit Breaker Pattern**: Automatic failure detection and recovery
- **Intelligent Retry Logic**: Multiple retry strategies (exponential, fixed, linear backoff)
- **Request Timeout Handling**: Configurable timeouts with proper error handling
- **Rate Limit Management**: Built-in handling for API rate limiting

### Production-Ready Features
- **Comprehensive Logging**: Detailed logging at all levels with context
- **Performance Metrics**: Processing rates, error rates, and timing information
- **State Management**: Proper checkpointing for resume capability
- **Configuration Validation**: Input validation with clear error messages

## 📋 Prerequisites

- Python 3.7+
- Fivetran Connector SDK
- Internet connection (uses JSONPlaceholder API for testing)

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure the connector:**
   Edit `configuration.json` with your desired settings:
   ```json
   {
     "max_workers": 3,
     "error_threshold_per_thread": 5,
     "total_error_threshold": 15,
     "api_timeout_seconds": 30,
     "enable_circuit_breaker": true,
     "retry_strategy": "exponential_backoff",
     "max_retries": 3
   }
   ```

3. **Test the connector:**
   ```bash
   python connector.py
   ```

4. **Deploy with Fivetran CLI:**
   ```bash
   fivetran connector test
   fivetran connector deploy
   ```

## ⚙️ Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_workers` | int | 3 | Number of concurrent processing threads (1-20) |
| `error_threshold_per_thread` | int | 5 | Maximum errors per thread before stopping |
| `total_error_threshold` | int | 15 | Maximum total errors before sync termination |
| `api_timeout_seconds` | int | 30 | HTTP request timeout in seconds |
| `enable_circuit_breaker` | bool | true | Enable circuit breaker pattern |
| `retry_strategy` | string | "exponential_backoff" | Retry strategy: exponential_backoff, fixed_interval, linear_backoff |
| `max_retries` | int | 3 | Maximum retry attempts per request |

## 🏗️ Architecture Overview

### Error Handling Components

#### ErrorHandler Class
Manages all error-related functionality with thread safety:
- Error collection and categorization
- Thread-specific error counting
- Global error threshold monitoring
- Shutdown coordination across threads

#### RetryableAPIClient Class
Provides resilient HTTP communication:
- Circuit breaker implementation
- Multiple retry strategies
- Intelligent error classification
- Connection pooling and reuse

#### ThreadSafeDataProcessor Class
Coordinates data processing across threads:
- Thread metrics collection
- Error context management
- Data transformation and validation
- Fivetran integration

### Threading Model

```
Main Thread
├── ThreadPoolExecutor (max_workers=3)
│   ├── Worker Thread 1: process_users_table()
│   ├── Worker Thread 2: process_posts_table()
│   └── Worker Thread 3: process_comments_table()
├── ErrorHandler (shared, thread-safe)
├── Metrics Collection (shared, thread-safe)
└── State Management
```

## 📊 Data Sources

This example uses [JSONPlaceholder](https://jsonplaceholder.typicode.com/) as a playground API, which provides:

- **Users**: `/users` - User profile information
- **Posts**: `/posts` - Blog posts with metadata
- **Comments**: `/comments` - Comment data with relationships

The connector demonstrates realistic data processing patterns including:
- Data transformation and enrichment
- JSON field handling
- Relationship mapping
- Timestamp addition

## 🔄 Multi-Process vs Multi-Threading Comparison

### Multi-Threading (This Example)
**✅ Advantages:**
- Shared memory space for efficient data sharing
- Lower overhead for thread creation/destruction
- Built-in synchronization primitives (locks, events)
- Ideal for I/O-bound operations (API calls)
- Better for error coordination across workers

**❌ Disadvantages:**
- Python GIL limitations for CPU-bound tasks
- Potential for race conditions if not properly synchronized
- Shared state complexity

**🎯 Best Use Cases:**
- API-heavy connectors with network I/O
- Moderate data volume processing
- Complex error handling requirements
- Shared configuration and state

### Multi-Processing Alternative
**✅ Advantages:**
- True parallelism for CPU-bound operations
- Process isolation prevents cascading failures
- Better scalability for compute-intensive tasks
- No GIL limitations

**❌ Disadvantages:**
- Higher memory overhead (separate memory spaces)
- Complex inter-process communication
- Serialization overhead for data sharing
- More complex error coordination

**🎯 Best Use Cases:**
- CPU-intensive data transformation
- Large dataset processing
- Independent task processing
- Maximum fault isolation required

### Recommendation Matrix

| Scenario | Threading | Processing |
|----------|-----------|------------|
| API calls + light processing | ✅ **Recommended** | ❌ Overkill |
| Heavy data transformation | ⚠️ Limited by GIL | ✅ **Recommended** |
| Mixed workloads | ✅ Good balance | ⚠️ Complex setup |
| Error handling complexity | ✅ **Easier** | ❌ More complex |
| Memory constraints | ✅ **Lower usage** | ❌ Higher usage |

## 📈 Performance Monitoring

The connector provides comprehensive metrics:

### Thread Metrics
- Records processed per thread
- Error count per thread
- Processing rate (records/second)
- Thread execution duration

### Error Analytics
- Error distribution by severity
- Error patterns by table/endpoint
- Retry success rates
- Circuit breaker activations

### Sample Output
```json
{
  "processing_summary": {
    "total_records_processed": 600,
    "total_errors": 3,
    "total_retries": 7,
    "thread_count": 3,
    "thread_summaries": {
      "thread-123": {
        "records_processed": 200,
        "errors_encountered": 1,
        "duration_seconds": 45.2,
        "records_per_second": 4.42
      }
    }
  },
  "error_summary": {
    "total_errors": 3,
    "errors_by_severity": {
      "LOW": 1,
      "MEDIUM": 2,
      "HIGH": 0,
      "CRITICAL": 0
    },
    "errors_by_table": {
      "users": 1,
      "posts": 2
    }
  }
}
```

## 🛠️ Error Scenarios & Testing

### Simulated Error Conditions
1. **Network Timeouts**: Random API timeouts to test retry logic
2. **Rate Limiting**: HTTP 429 responses with backoff testing
3. **Server Errors**: 5xx responses to validate circuit breaker
4. **Data Corruption**: Invalid JSON responses for parsing error handling
5. **Thread Failures**: Individual thread crashes and recovery

### Testing Strategies
```python
# Test error thresholds
config = {
    "max_workers": 5,
    "error_threshold_per_thread": 2,  # Low threshold for testing
    "total_error_threshold": 8
}

# Test circuit breaker
config = {
    "enable_circuit_breaker": True,
    "failure_threshold": 3,  # Trigger after 3 failures
    "recovery_timeout": 30   # 30-second recovery window
}

# Test retry strategies
config = {
    "retry_strategy": "exponential_backoff",
    "max_retries": 5,
    "base_delay": 1.0
}
```

## 🔧 Customization Guide

### Adding New Tables
```python
def process_albums_table(self) -> None:
    """Process albums data from playground API"""
    self._initialize_thread_metrics()
    thread_id = self._get_thread_id()

    try:
        with self.error_context("albums"):
            albums_data = self.api_client.get("/albums")

        for album in albums_data:
            if not self.error_handler.should_continue_processing(thread_id):
                break

            with self.error_context("albums", str(album.get("id"))):
                processed_album = {
                    "id": album["id"],
                    "user_id": album["userId"],
                    "title": album["title"],
                    "processed_at": datetime.now().isoformat()
                }

                op.upsert("albums", processed_album)
                self._update_metrics(records_processed=1)
    finally:
        self._finalize_thread_metrics()
```

### Custom Error Handling
```python
class CustomErrorHandler(ErrorHandler):
    def handle_business_logic_error(self, error_data: dict) -> None:
        """Handle domain-specific errors"""
        if error_data.get("error_code") == "DUPLICATE_RECORD":
            # Handle duplicates gracefully
            log.info(f"Skipping duplicate record: {error_data}")
            return

        # Escalate other business errors
        super().record_error(ErrorRecord(
            thread_id=self._get_thread_id(),
            error_type="BusinessLogicError",
            error_message=error_data.get("message", "Unknown business error"),
            severity=ErrorSeverity.MEDIUM
        ))
```

### Alternative APIs
Replace the JSONPlaceholder API with your target API:
```python
# Update base URL
PLAYGROUND_API_BASE = "https://api.yourdomain.com"

# Update authentication
class AuthenticatedAPIClient(RetryableAPIClient):
    def __init__(self, base_url: str, api_key: str):
        super().__init__(base_url)
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
```

## 🚨 Production Considerations

### Security
- Store API credentials in Fivetran secrets, not configuration
- Implement proper authentication token refresh
- Use HTTPS endpoints only
- Validate all input data

### Performance
- Monitor memory usage in multi-threaded environments
- Implement connection pooling for HTTP clients
- Use streaming for large data sets
- Consider data compression for network efficiency

### Reliability
- Implement health checks for external dependencies
- Set up monitoring and alerting
- Plan for graceful degradation scenarios
- Test disaster recovery procedures

### Scalability
- Profile memory usage under load
- Test with realistic data volumes
- Monitor thread contention and lock usage
- Plan for horizontal scaling if needed

## 📚 Additional Resources

- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [Python Threading Best Practices](https://docs.python.org/3/library/threading.html)
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Error Handling Strategies](https://docs.python.org/3/tutorial/errors.html)

## 🤝 Contributing

This example is designed to be educational and extensible. Consider:
- Adding support for additional retry strategies
- Implementing more sophisticated circuit breaker logic
- Adding support for different API authentication methods
- Creating specialized error handlers for specific use cases

## 📄 License

This example is provided under the same license as the Fivetran Connector SDK.