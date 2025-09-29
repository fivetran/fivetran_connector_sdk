"""
Configuration constants and validation for PostgreSQL connector.

This module contains all configuration constants for adaptive processing,
resource monitoring thresholds, and configuration validation logic.
"""

# RESOURCE MONITORING & ADAPTIVE PROCESSING CONFIGURATION

# Configuration constants for adaptive processing
BATCH_SIZE = 5000  # Default batch size for data fetching
PARTITION_SIZE = 50000  # Default partition size for large tables
CHECKPOINT_INTERVAL = 1000000  # Checkpoint every 1 million records
CONNECTION_TIMEOUT_HOURS = 3  # Reconnect after 3 hours
MAX_RETRIES = 5  # Maximum retry attempts for failed operations
BASE_RETRY_DELAY = 5  # Base delay in seconds for retry backoff
MAX_RETRY_DELAY = 300  # 5 minutes max delay between retries

# Table size thresholds for adaptive processing
SMALL_TABLE_THRESHOLD = 1000000  # 1M rows - tables below this are "small"
LARGE_TABLE_THRESHOLD = 50000000  # 50M rows - tables above this are "large"

# Resource monitoring thresholds
MEMORY_THRESHOLD_HIGH = 80  # 80% memory usage triggers reduction
MEMORY_THRESHOLD_CRITICAL = 90  # 90% memory usage triggers aggressive reduction
CPU_THRESHOLD_HIGH = 85  # 85% CPU usage triggers thread reduction
CPU_THRESHOLD_CRITICAL = 95  # 95% CPU usage triggers aggressive reduction

# Resource monitoring state
# This dictionary tracks current resource pressure and monitoring status
resource_state = {
    'memory_pressure': False,  # Is system under memory pressure?
    'cpu_pressure': False,  # Is system under CPU pressure?
    'batch_size_reduced': False,  # Has batch size been reduced?
    'threads_reduced': False,  # Has thread count been reduced?
    'last_monitoring': None,  # Timestamp of last monitoring check
    'monitoring_interval': 5  # Check every 5 tables (adjust based on workload)
}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    This function is called at the start of schema and update methods to ensure 
    that the connector has all necessary configuration values before proceeding.
    
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
                      Must contain: host, port, database, username, password
    
    Raises:
        ValueError: If any required configuration parameter is missing or has wrong type.
    
    Example:
        >>> config = {"host": "localhost", "port": "5432", ...}
        >>> validate_configuration(config)  # Passes validation
        >>> validate_configuration({})  # Raises ValueError
    """
    # Define required configuration parameters for PostgreSQL connection
    required_configs = ["host", "port", "database", "username", "password"]
    
    # Check each required parameter
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        
        # Ensure all values are strings as per Fivetran SDK requirements
        # The Fivetran SDK expects all configuration values to be strings
        if not isinstance(configuration[key], str):
            raise ValueError(
                f"Configuration value for {key} must be a string, "
                f"got {type(configuration[key])}"
            )
