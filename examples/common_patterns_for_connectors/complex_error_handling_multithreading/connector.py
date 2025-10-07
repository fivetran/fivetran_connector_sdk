# Complex Error Handling with Multi-Threading Example for Fivetran Connector SDK
# This example demonstrates advanced error handling strategies in multi-threaded environments
# using a playground API approach for testing and iteration.

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Import required libraries for multi-threading and error handling
import requests
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from threading import Lock, Event
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import traceback
from contextlib import contextmanager
import random
from datetime import datetime, timedelta

# Playground API base URL - uses a mock API service for testing
PLAYGROUND_API_BASE = "https://jsonplaceholder.typicode.com"

# Configuration constants with default values
DEFAULT_MAX_WORKERS = 3
DEFAULT_ERROR_THRESHOLD_PER_THREAD = 5
DEFAULT_TOTAL_ERROR_THRESHOLD = 15
DEFAULT_API_TIMEOUT_SECONDS = 30
DEFAULT_ENABLE_CIRCUIT_BREAKER = True
DEFAULT_RETRY_STRATEGY = "exponential_backoff"
DEFAULT_MAX_RETRIES = 3

# Configuration validation constants
MIN_MAX_WORKERS = 1
MAX_MAX_WORKERS = 20
MIN_ERROR_THRESHOLD = 1
MIN_TIMEOUT_SECONDS = 5
MAX_TIMEOUT_SECONDS = 300

# Configuration key names
CONFIG_MAX_WORKERS = "max_workers"
CONFIG_ERROR_THRESHOLD_PER_THREAD = "error_threshold_per_thread"
CONFIG_TOTAL_ERROR_THRESHOLD = "total_error_threshold"
CONFIG_API_TIMEOUT_SECONDS = "api_timeout_seconds"
CONFIG_ENABLE_CIRCUIT_BREAKER = "enable_circuit_breaker"
CONFIG_RETRY_STRATEGY = "retry_strategy"
CONFIG_MAX_RETRIES = "max_retries"

# Valid retry strategies
VALID_RETRY_STRATEGIES = ["exponential_backoff", "fixed_interval", "linear_backoff"]


# Error handling enums and classes
class ErrorSeverity(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class RetryStrategy(Enum):
    EXPONENTIAL_BACKOFF = 1
    FIXED_INTERVAL = 2
    LINEAR_BACKOFF = 3


@dataclass
class ErrorRecord:
    """Record to track errors during processing"""

    thread_id: str
    table_name: str
    record_id: Optional[str] = None
    error_type: str = ""
    error_message: str = ""
    severity: ErrorSeverity = ErrorSeverity.LOW
    timestamp: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    stack_trace: str = ""


@dataclass
class ThreadMetrics:
    """Metrics tracking for each thread"""

    thread_id: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    records_processed: int = 0
    errors_encountered: int = 0
    retries_attempted: int = 0


class ErrorHandler:
    """Centralized error handling with thread safety"""

    def __init__(self, max_errors_per_thread: int = 10, max_total_errors: int = 100):
        self.max_errors_per_thread = max_errors_per_thread
        self.max_total_errors = max_total_errors
        self.error_records: List[ErrorRecord] = []
        self.error_lock = Lock()
        self.thread_errors: Dict[str, int] = {}
        self.shutdown_event = Event()

    def should_continue_processing(self, thread_id: str) -> bool:
        """Check if processing should continue based on error thresholds"""
        with self.error_lock:
            thread_error_count = self.thread_errors.get(thread_id, 0)
            total_errors = len(self.error_records)

            if self.shutdown_event.is_set():
                return False

            if thread_error_count >= self.max_errors_per_thread:
                log.warning(
                    f"Thread {thread_id} exceeded max errors ({self.max_errors_per_thread})"
                )
                return False

            if total_errors >= self.max_total_errors:
                log.severe(
                    f"Total errors exceeded threshold ({self.max_total_errors}). Initiating shutdown."
                )
                self.shutdown_event.set()
                return False

        return True

    def record_error(self, error: ErrorRecord) -> None:
        """Thread-safe error recording"""
        with self.error_lock:
            self.error_records.append(error)
            self.thread_errors[error.thread_id] = self.thread_errors.get(error.thread_id, 0) + 1

            log.warning(
                f"Error recorded - Thread: {error.thread_id}, Table: {error.table_name}, "
                f"Type: {error.error_type}, Severity: {error.severity.name}"
            )

            if error.severity == ErrorSeverity.CRITICAL:
                log.severe(f"Critical error encountered: {error.error_message}")
                self.shutdown_event.set()

    def get_error_summary(self) -> Dict[str, Any]:
        """Get comprehensive error summary"""
        with self.error_lock:
            total_errors = len(self.error_records)
            errors_by_severity = {severity: 0 for severity in ErrorSeverity}
            errors_by_table = {}
            errors_by_type = {}

            for error in self.error_records:
                errors_by_severity[error.severity] += 1
                errors_by_table[error.table_name] = errors_by_table.get(error.table_name, 0) + 1
                errors_by_type[error.error_type] = errors_by_type.get(error.error_type, 0) + 1

            return {
                "total_errors": total_errors,
                "errors_by_severity": {s.name: count for s, count in errors_by_severity.items()},
                "errors_by_table": errors_by_table,
                "errors_by_type": errors_by_type,
                "thread_errors": dict(self.thread_errors),
            }


class RetryableAPIClient:
    """API client with intelligent retry logic and circuit breaker pattern"""

    def __init__(
        self,
        base_url: str,
        max_retries: int = 3,
        retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
    ):
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_strategy = retry_strategy
        self.session = requests.Session()

        # Circuit breaker settings - can be made configurable if needed
        self.failure_threshold = 5
        self.recovery_timeout = 60  # seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.circuit_open = False
        self.circuit_lock = Lock()

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate retry delay based on strategy"""
        if self.retry_strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            return min(60, (2**attempt) + random.uniform(0, 1))
        elif self.retry_strategy == RetryStrategy.FIXED_INTERVAL:
            return 5.0
        elif self.retry_strategy == RetryStrategy.LINEAR_BACKOFF:
            return min(30, attempt * 2 + random.uniform(0, 1))

    def _is_retryable_error(self, response: requests.Response) -> bool:
        """Determine if an HTTP error is retryable"""
        # Retry on server errors and rate limiting
        return response.status_code in [408, 429, 500, 502, 503, 504]

    def _update_circuit_breaker(self, success: bool) -> None:
        """Update circuit breaker state"""
        with self.circuit_lock:
            if success:
                self.failure_count = 0
                self.circuit_open = False
            else:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.circuit_open = True
                    self.last_failure_time = time.time()

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows requests"""
        with self.circuit_lock:
            if not self.circuit_open:
                return True

            # Check if we can attempt recovery
            if (
                self.last_failure_time
                and time.time() - self.last_failure_time > self.recovery_timeout
            ):
                self.circuit_open = False
                self.failure_count = 0
                log.info("Circuit breaker attempting recovery")
                return True

            return False

    def get(self, endpoint: str, params: dict = None, timeout: int = None) -> dict:
        """Make HTTP GET request with retry logic and circuit breaker"""
        if not self._check_circuit_breaker():
            raise Exception("Circuit breaker is open - too many failures")

        url = f"{self.base_url}{endpoint}"
        last_exception = None

        # Use default timeout if none provided
        if timeout is None:
            timeout = DEFAULT_API_TIMEOUT_SECONDS

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)

                if response.ok:
                    self._update_circuit_breaker(success=True)
                    return response.json()

                if not self._is_retryable_error(response):
                    # Non-retryable error
                    self._update_circuit_breaker(success=False)
                    raise requests.RequestException(
                        f"HTTP {response.status_code}: {response.text}"
                    )

                # Retryable error
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    log.warning(
                        f"Retrying request to {url} in {delay:.2f}s (attempt {attempt + 1})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    self._update_circuit_breaker(success=False)
                    raise requests.RequestException(f"Max retries exceeded for {url}")

            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    log.warning(f"Request failed, retrying in {delay:.2f}s: {str(e)}")
                    time.sleep(delay)
                else:
                    self._update_circuit_breaker(success=False)
                    raise e

        if last_exception:
            raise last_exception


class ThreadSafeDataProcessor:
    """Thread-safe data processing with monitoring"""

    def __init__(self, error_handler: ErrorHandler, configuration: dict):
        self.error_handler = error_handler
        self.configuration = configuration

        # Get retry strategy from configuration
        retry_strategy_name = configuration[CONFIG_RETRY_STRATEGY]
        retry_strategy = RetryStrategy.EXPONENTIAL_BACKOFF  # default
        if retry_strategy_name == "fixed_interval":
            retry_strategy = RetryStrategy.FIXED_INTERVAL
        elif retry_strategy_name == "linear_backoff":
            retry_strategy = RetryStrategy.LINEAR_BACKOFF

        self.api_client = RetryableAPIClient(
            base_url=PLAYGROUND_API_BASE,
            max_retries=configuration[CONFIG_MAX_RETRIES],
            retry_strategy=retry_strategy,
        )
        self.metrics: Dict[str, ThreadMetrics] = {}
        self.metrics_lock = Lock()

    def _get_thread_id(self) -> str:
        """Get unique thread identifier"""
        return f"thread-{threading.current_thread().ident}"

    def _initialize_thread_metrics(self) -> None:
        """Initialize metrics for current thread"""
        thread_id = self._get_thread_id()
        with self.metrics_lock:
            if thread_id not in self.metrics:
                self.metrics[thread_id] = ThreadMetrics(thread_id=thread_id)

    def _update_metrics(
        self, records_processed: int = 0, errors: int = 0, retries: int = 0
    ) -> None:
        """Update thread metrics"""
        thread_id = self._get_thread_id()
        with self.metrics_lock:
            if thread_id in self.metrics:
                self.metrics[thread_id].records_processed += records_processed
                self.metrics[thread_id].errors_encountered += errors
                self.metrics[thread_id].retries_attempted += retries

    def _finalize_thread_metrics(self) -> None:
        """Finalize metrics for current thread"""
        thread_id = self._get_thread_id()
        with self.metrics_lock:
            if thread_id in self.metrics:
                self.metrics[thread_id].end_time = datetime.now()

    @contextmanager
    def error_context(self, table_name: str, record_id: str = None):
        """Context manager for error handling"""
        thread_id = self._get_thread_id()
        try:
            yield
        except requests.RequestException as e:
            error = ErrorRecord(
                thread_id=thread_id,
                table_name=table_name,
                record_id=record_id,
                error_type="RequestException",
                error_message=str(e),
                severity=ErrorSeverity.MEDIUM,
                stack_trace=traceback.format_exc(),
            )
            self.error_handler.record_error(error)
            self._update_metrics(errors=1)
            raise
        except json.JSONDecodeError as e:
            error = ErrorRecord(
                thread_id=thread_id,
                table_name=table_name,
                record_id=record_id,
                error_type="JSONDecodeError",
                error_message=str(e),
                severity=ErrorSeverity.LOW,
                stack_trace=traceback.format_exc(),
            )
            self.error_handler.record_error(error)
            self._update_metrics(errors=1)
            raise
        except Exception as e:
            error = ErrorRecord(
                thread_id=thread_id,
                table_name=table_name,
                record_id=record_id,
                error_type=type(e).__name__,
                error_message=str(e),
                severity=ErrorSeverity.HIGH,
                stack_trace=traceback.format_exc(),
            )
            self.error_handler.record_error(error)
            self._update_metrics(errors=1)
            raise

    def process_users_table(self) -> None:
        """Process users data from playground API"""
        self._initialize_thread_metrics()
        thread_id = self._get_thread_id()

        try:
            log.info(f"Thread {thread_id} starting users processing")

            with self.error_context("users"):
                timeout = self.configuration[CONFIG_API_TIMEOUT_SECONDS]
                users_data = self.api_client.get("/users", timeout=timeout)

            for user in users_data:
                if not self.error_handler.should_continue_processing(thread_id):
                    log.warning(f"Thread {thread_id} stopping due to error threshold")
                    break

                try:
                    with self.error_context("users", str(user.get("id"))):
                        # Simulate data transformation and validation
                        processed_user = {
                            "id": user["id"],
                            "name": user["name"],
                            "username": user["username"],
                            "email": user["email"],
                            "address": json.dumps(user.get("address", {})),
                            "phone": user.get("phone", ""),
                            "website": user.get("website", ""),
                            "company": json.dumps(user.get("company", {})),
                            "processed_at": datetime.now().isoformat(),
                        }

                        # Upsert to Fivetran
                        op.upsert("users", processed_user)
                        self._update_metrics(records_processed=1)

                except Exception as e:
                    log.warning(f"Failed to process user {user.get('id')}: {str(e)}")
                    # Continue with next record
                    continue

            log.info(f"Thread {thread_id} completed users processing")

        finally:
            self._finalize_thread_metrics()

    def process_posts_table(self) -> None:
        """Process posts data from playground API"""
        self._initialize_thread_metrics()
        thread_id = self._get_thread_id()

        try:
            log.info(f"Thread {thread_id} starting posts processing")

            with self.error_context("posts"):
                timeout = self.configuration[CONFIG_API_TIMEOUT_SECONDS]
                posts_data = self.api_client.get("/posts", timeout=timeout)

            for post in posts_data:
                if not self.error_handler.should_continue_processing(thread_id):
                    log.warning(f"Thread {thread_id} stopping due to error threshold")
                    break

                try:
                    with self.error_context("posts", str(post.get("id"))):
                        # Simulate data transformation
                        processed_post = {
                            "id": post["id"],
                            "user_id": post["userId"],
                            "title": post["title"],
                            "body": post["body"],
                            "word_count": len(post["body"].split()),
                            "processed_at": datetime.now().isoformat(),
                        }

                        op.upsert("posts", processed_post)
                        self._update_metrics(records_processed=1)

                except Exception as e:
                    log.warning(f"Failed to process post {post.get('id')}: {str(e)}")
                    continue

            log.info(f"Thread {thread_id} completed posts processing")

        finally:
            self._finalize_thread_metrics()

    def process_comments_table(self) -> None:
        """Process comments data from playground API"""
        self._initialize_thread_metrics()
        thread_id = self._get_thread_id()

        try:
            log.info(f"Thread {thread_id} starting comments processing")

            with self.error_context("comments"):
                timeout = self.configuration[CONFIG_API_TIMEOUT_SECONDS]
                comments_data = self.api_client.get("/comments", timeout=timeout)

            for comment in comments_data:
                if not self.error_handler.should_continue_processing(thread_id):
                    log.warning(f"Thread {thread_id} stopping due to error threshold")
                    break

                try:
                    with self.error_context("comments", str(comment.get("id"))):
                        processed_comment = {
                            "id": comment["id"],
                            "post_id": comment["postId"],
                            "name": comment["name"],
                            "email": comment["email"],
                            "body": comment["body"],
                            "processed_at": datetime.now().isoformat(),
                        }

                        op.upsert("comments", processed_comment)
                        self._update_metrics(records_processed=1)

                except Exception as e:
                    log.warning(f"Failed to process comment {comment.get('id')}: {str(e)}")
                    continue

            log.info(f"Thread {thread_id} completed comments processing")

        finally:
            self._finalize_thread_metrics()

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        with self.metrics_lock:
            total_records = sum(m.records_processed for m in self.metrics.values())
            total_errors = sum(m.errors_encountered for m in self.metrics.values())
            total_retries = sum(m.retries_attempted for m in self.metrics.values())

            thread_summaries = {}
            for thread_id, metrics in self.metrics.items():
                duration = None
                if metrics.end_time:
                    duration = (metrics.end_time - metrics.start_time).total_seconds()

                thread_summaries[thread_id] = {
                    "records_processed": metrics.records_processed,
                    "errors_encountered": metrics.errors_encountered,
                    "retries_attempted": metrics.retries_attempted,
                    "duration_seconds": duration,
                    "records_per_second": (
                        metrics.records_processed / duration if duration and duration > 0 else 0
                    ),
                }

            return {
                "total_records_processed": total_records,
                "total_errors": total_errors,
                "total_retries": total_retries,
                "thread_count": len(self.metrics),
                "thread_summaries": thread_summaries,
            }


def get_config_value(configuration: dict, key: str, default_value: Any) -> Any:
    """Get configuration value with fallback to default"""
    return configuration.get(key, default_value)


def validate_configuration(configuration: dict) -> None:
    """Validate configuration parameters"""
    # Apply defaults for missing configuration keys
    configuration.setdefault(CONFIG_MAX_WORKERS, DEFAULT_MAX_WORKERS)
    configuration.setdefault(CONFIG_ERROR_THRESHOLD_PER_THREAD, DEFAULT_ERROR_THRESHOLD_PER_THREAD)
    configuration.setdefault(CONFIG_TOTAL_ERROR_THRESHOLD, DEFAULT_TOTAL_ERROR_THRESHOLD)
    configuration.setdefault(CONFIG_API_TIMEOUT_SECONDS, DEFAULT_API_TIMEOUT_SECONDS)
    configuration.setdefault(CONFIG_ENABLE_CIRCUIT_BREAKER, DEFAULT_ENABLE_CIRCUIT_BREAKER)
    configuration.setdefault(CONFIG_RETRY_STRATEGY, DEFAULT_RETRY_STRATEGY)
    configuration.setdefault(CONFIG_MAX_RETRIES, DEFAULT_MAX_RETRIES)

    # Validate ranges
    max_workers = configuration[CONFIG_MAX_WORKERS]
    if (
        not isinstance(max_workers, int)
        or max_workers < MIN_MAX_WORKERS
        or max_workers > MAX_MAX_WORKERS
    ):
        raise ValueError(
            f"{CONFIG_MAX_WORKERS} must be an integer between {MIN_MAX_WORKERS} and {MAX_MAX_WORKERS}"
        )

    error_threshold_per_thread = configuration[CONFIG_ERROR_THRESHOLD_PER_THREAD]
    if (
        not isinstance(error_threshold_per_thread, int)
        or error_threshold_per_thread < MIN_ERROR_THRESHOLD
    ):
        raise ValueError(
            f"{CONFIG_ERROR_THRESHOLD_PER_THREAD} must be an integer >= {MIN_ERROR_THRESHOLD}"
        )

    total_error_threshold = configuration[CONFIG_TOTAL_ERROR_THRESHOLD]
    if not isinstance(total_error_threshold, int) or total_error_threshold < MIN_ERROR_THRESHOLD:
        raise ValueError(
            f"{CONFIG_TOTAL_ERROR_THRESHOLD} must be an integer >= {MIN_ERROR_THRESHOLD}"
        )

    api_timeout = configuration[CONFIG_API_TIMEOUT_SECONDS]
    if (
        not isinstance(api_timeout, int)
        or api_timeout < MIN_TIMEOUT_SECONDS
        or api_timeout > MAX_TIMEOUT_SECONDS
    ):
        raise ValueError(
            f"{CONFIG_API_TIMEOUT_SECONDS} must be an integer between {MIN_TIMEOUT_SECONDS} and {MAX_TIMEOUT_SECONDS}"
        )

    retry_strategy = configuration[CONFIG_RETRY_STRATEGY]
    if retry_strategy not in VALID_RETRY_STRATEGIES:
        raise ValueError(
            f"{CONFIG_RETRY_STRATEGY} must be one of: {', '.join(VALID_RETRY_STRATEGIES)}"
        )

    max_retries = configuration[CONFIG_MAX_RETRIES]
    if not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError(f"{CONFIG_MAX_RETRIES} must be a non-negative integer")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """Define schema for playground API tables"""
    return [
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "username": "STRING",
                "email": "STRING",
                "address": "JSON",
                "phone": "STRING",
                "website": "STRING",
                "company": "JSON",
                "processed_at": "UTC_DATETIME",
            },
        },
        {
            "table": "posts",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "user_id": "LONG",
                "title": "STRING",
                "body": "STRING",
                "word_count": "LONG",
                "processed_at": "UTC_DATETIME",
            },
        },
        {
            "table": "comments",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "post_id": "LONG",
                "name": "STRING",
                "email": "STRING",
                "body": "STRING",
                "processed_at": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict) -> None:
    """Main update function with multi-threading and complex error handling"""
    log.info("Starting multi-threaded sync with complex error handling")

    # Validate configuration
    validate_configuration(configuration)

    # Initialize error handler and data processor
    error_handler = ErrorHandler(
        max_errors_per_thread=configuration[CONFIG_ERROR_THRESHOLD_PER_THREAD],
        max_total_errors=configuration[CONFIG_TOTAL_ERROR_THRESHOLD],
    )

    data_processor = ThreadSafeDataProcessor(error_handler, configuration)

    # Define processing tasks
    processing_tasks = [
        ("users", data_processor.process_users_table),
        ("posts", data_processor.process_posts_table),
        ("comments", data_processor.process_comments_table),
    ]

    max_workers = configuration[CONFIG_MAX_WORKERS]
    start_time = time.time()

    # Execute tasks in parallel
    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="FivetranWorker"
    ) as executor:
        log.info(f"Submitting {len(processing_tasks)} tasks to {max_workers} worker threads")

        # Submit all tasks
        future_to_table = {
            executor.submit(task_func): table_name for table_name, task_func in processing_tasks
        }

        # Process completed tasks
        completed_tasks = 0
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            completed_tasks += 1

            try:
                future.result()  # This will raise any exception from the task
                log.info(
                    f"Successfully completed processing for table: {table_name} "
                    f"({completed_tasks}/{len(processing_tasks)})"
                )
            except Exception as e:
                log.severe(f"Task failed for table {table_name}: {str(e)}")

    # Calculate total processing time
    total_time = time.time() - start_time

    # Generate comprehensive summary
    error_summary = error_handler.get_error_summary()
    metrics_summary = data_processor.get_metrics_summary()

    log.info(f"Multi-threaded sync completed in {total_time:.2f} seconds")
    log.info(f"Processing Summary: {json.dumps(metrics_summary, indent=2)}")
    log.info(f"Error Summary: {json.dumps(error_summary, indent=2)}")

    # Update state with processing information
    state.update(
        {
            "last_sync_time": datetime.now().isoformat(),
            "processing_time_seconds": total_time,
            "total_records_processed": metrics_summary["total_records_processed"],
            "total_errors": error_summary["total_errors"],
            "sync_status": (
                "completed_with_errors" if error_summary["total_errors"] > 0 else "completed"
            ),
        }
    )

    # Checkpoint final state
    op.checkpoint(state)

    # Raise exception if critical errors occurred
    if error_handler.shutdown_event.is_set():
        raise Exception("Sync terminated due to critical errors or error thresholds exceeded")


# Create connector
connector = Connector(update=update, schema=schema)

# Debug execution
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
