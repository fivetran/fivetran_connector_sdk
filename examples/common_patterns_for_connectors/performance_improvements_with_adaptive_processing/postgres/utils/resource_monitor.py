"""
Resource monitoring functions for adaptive processing.

This module provides real-time system resource monitoring to enable
adaptive processing based on memory, CPU, and disk usage.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from fivetran_connector_sdk import Logging as log

from .config import (
    MEMORY_THRESHOLD_HIGH,
    MEMORY_THRESHOLD_CRITICAL,
    CPU_THRESHOLD_HIGH,
    CPU_THRESHOLD_CRITICAL
)


def monitor_resources() -> Dict[str, Any]:
    """
    Monitor system resources and return current status.
    
    This function checks memory, CPU, and disk usage to determine if the system
    is under resource pressure. It's used to make adaptive decisions about
    batch sizes and thread counts during data processing.
    
    Returns:
        Dict containing:
            - status: 'active', 'disabled', or 'error'
            - memory_usage: Percentage of memory used
            - memory_available_gb: Available memory in GB
            - cpu_percent: CPU usage percentage
            - disk_usage: Disk usage percentage
            - memory_pressure: Boolean indicating high memory usage
            - memory_critical: Boolean indicating critical memory usage
            - cpu_pressure: Boolean indicating high CPU usage
            - cpu_critical: Boolean indicating critical CPU usage
            - timestamp: UTC timestamp of monitoring check
    
    Example:
        >>> status = monitor_resources()
        >>> if status['memory_pressure']:
        ...     # Reduce batch size
    """
    try:
        # Import psutil for resource monitoring
        # This is imported here to make it optional - if not available, monitoring is disabled
        import psutil
        
        # Get current memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent
        memory_available_gb = memory.available / (1024**3)
        
        # Get current CPU usage (1 second sampling interval)
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get disk usage for the current working directory
        disk = psutil.disk_usage('.')
        disk_usage = (disk.used / disk.total) * 100
        
        # Determine resource pressure levels based on thresholds
        memory_pressure = memory_usage > MEMORY_THRESHOLD_HIGH
        memory_critical = memory_usage > MEMORY_THRESHOLD_CRITICAL
        cpu_pressure = cpu_percent > CPU_THRESHOLD_HIGH
        cpu_critical = cpu_percent > CPU_THRESHOLD_CRITICAL
        
        # Log resource status for monitoring and debugging
        log.info(
            f"Resource Monitor: Memory {memory_usage:.1f}% "
            f"({memory_available_gb:.1f}GB available), "
            f"CPU {cpu_percent:.1f}%, Disk {disk_usage:.1f}%"
        )
        
        # Log warnings for high resource usage
        if memory_critical:
            log.warning(
                f"CRITICAL MEMORY USAGE: {memory_usage:.1f}% - "
                f"System under severe memory pressure!"
            )
        elif memory_pressure:
            log.warning(
                f"HIGH MEMORY USAGE: {memory_usage:.1f}% - "
                f"Consider reducing batch sizes"
            )
        
        if cpu_critical:
            log.warning(
                f"CRITICAL CPU USAGE: {cpu_percent:.1f}% - "
                f"System under severe CPU pressure!"
            )
        elif cpu_pressure:
            log.warning(
                f"HIGH CPU USAGE: {cpu_percent:.1f}% - "
                f"Consider reducing thread count"
            )
        
        return {
            'status': 'active',
            'memory_usage': memory_usage,
            'memory_available_gb': memory_available_gb,
            'cpu_percent': cpu_percent,
            'disk_usage': disk_usage,
            'memory_pressure': memory_pressure,
            'memory_critical': memory_critical,
            'cpu_pressure': cpu_pressure,
            'cpu_critical': cpu_critical,
            'timestamp': datetime.now(timezone.utc)
        }
        
    except ImportError:
        log.warning("psutil not available - resource monitoring will be disabled")
        return {'status': 'disabled', 'reason': 'psutil not available'}
    except Exception as e:
        log.warning(f"Resource monitoring failed: {e}")
        return {'status': 'error', 'error': str(e)}


def should_reduce_batch_size(memory_usage: float, current_batch_size: int) -> Tuple[bool, int]:
    """
    Determine if batch size should be reduced based on memory pressure.
    
    This function implements a graduated response to memory pressure:
    - Critical pressure (>90%): Reduce batch size by 50%
    - High pressure (>80%): Reduce batch size by 25%
    - Normal: Keep current batch size
    
    Args:
        memory_usage: Current memory usage percentage (0-100)
        current_batch_size: Current batch size being used
    
    Returns:
        Tuple of (should_reduce: bool, new_batch_size: int)
        - should_reduce: True if batch size should be reduced
        - new_batch_size: Recommended new batch size (minimum 100)
    
    Example:
        >>> should_reduce, new_size = should_reduce_batch_size(92.0, 5000)
        >>> # Returns (True, 2500) - critical pressure, reduce by 50%
    """
    if memory_usage > MEMORY_THRESHOLD_CRITICAL:
        # Critical memory pressure - reduce by 50% but keep minimum of 100
        new_batch_size = max(current_batch_size // 2, 100)
        log.warning(
            f"CRITICAL MEMORY PRESSURE: Reducing batch size from "
            f"{current_batch_size:,} to {new_batch_size:,} records"
        )
        return True, new_batch_size
        
    elif memory_usage > MEMORY_THRESHOLD_HIGH:
        # High memory pressure - reduce by 25% but keep minimum of 100
        new_batch_size = max(int(current_batch_size * 0.75), 100)
        log.info(
            f"HIGH MEMORY PRESSURE: Reducing batch size from "
            f"{current_batch_size:,} to {new_batch_size:,} records"
        )
        return True, new_batch_size
    
    # No reduction needed
    return False, current_batch_size


def should_reduce_threads(cpu_percent: float, current_threads: int) -> Tuple[bool, int]:
    """
    Determine if thread count should be reduced based on CPU pressure.
    
    This function implements a graduated response to CPU pressure:
    - Critical pressure (>95%): Reduce to 1 thread
    - High pressure (>85%): Reduce threads by 50%
    - Normal: Keep current thread count
    
    Args:
        cpu_percent: Current CPU usage percentage (0-100)
        current_threads: Current number of threads being used
    
    Returns:
        Tuple of (should_reduce: bool, new_threads: int)
        - should_reduce: True if thread count should be reduced
        - new_threads: Recommended new thread count (minimum 1)
    
    Example:
        >>> should_reduce, new_threads = should_reduce_threads(96.0, 4)
        >>> # Returns (True, 1) - critical pressure, reduce to single thread
    """
    if cpu_percent > CPU_THRESHOLD_CRITICAL:
        # Critical CPU pressure - reduce to 1 thread to avoid overload
        new_threads = 1
        log.warning(
            f"CRITICAL CPU PRESSURE: Reducing threads from "
            f"{current_threads} to {new_threads}"
        )
        return True, new_threads
        
    elif cpu_percent > CPU_THRESHOLD_HIGH:
        # High CPU pressure - reduce by 50% but keep minimum of 1
        new_threads = max(current_threads // 2, 1)
        log.info(
            f"HIGH CPU PRESSURE: Reducing threads from "
            f"{current_threads} to {new_threads}"
        )
        return True, new_threads
    
    # No reduction needed
    return False, current_threads
