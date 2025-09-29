"""
Table size analysis and categorization utilities.

This module provides functions to analyze table sizes and categorize them
for optimal processing order (small tables first, then medium, then large).
"""

from typing import Dict, List, Tuple
from fivetran_connector_sdk import Logging as log

from .config import SMALL_TABLE_THRESHOLD, LARGE_TABLE_THRESHOLD


def get_table_sizes(cursor, tables: List[str]) -> Dict[str, int]:
    """
    Get row counts for all tables efficiently.
    
    This function estimates table sizes to determine optimal processing parameters.
    Currently uses default estimates for reliability, but can be extended to
    query actual row counts from database statistics.
    
    Args:
        cursor: Database cursor for executing queries
        tables: List of table names to analyze
    
    Returns:
        Dictionary mapping table names to estimated row counts
        
    Note:
        For production use, you might want to query pg_stat_user_tables
        or use EXPLAIN to get actual row counts. Current implementation
        uses conservative default estimates to ensure reliable operation.
    
    Example:
        >>> sizes = get_table_sizes(cursor, ['orders', 'customers'])
        >>> # Returns {'orders': 10000, 'customers': 10000}
    """
    table_sizes = {}
    
    # For now, use default estimates to avoid complex query issues
    # This ensures the connector works reliably across different PostgreSQL versions
    # TODO: Implement actual row count queries for production use
    for table in tables:
        table_sizes[table] = 10000  # Default estimate for small tables
    
    log.info(f"Using default table size estimates for {len(tables)} tables")
    
    return table_sizes


def categorize_and_sort_tables(
    tables: List[str], 
    table_sizes: Dict[str, int]
) -> List[Tuple[str, str, int]]:
    """
    Categorize tables by size and sort for optimal processing order.
    
    This function groups tables into size categories and sorts them to process
    small tables first (quick wins), then medium, then large tables.
    This approach provides faster initial results and better progress tracking.
    
    Args:
        tables: List of table names
        table_sizes: Dictionary mapping table names to row counts
    
    Returns:
        List of tuples (table_name, category, row_count)
        Categories: 'small' (<1M), 'medium' (1M-50M), 'large' (>50M)
        Sorted by category first, then by row count within each category
    
    Example:
        >>> tables = ['huge_orders', 'small_config', 'medium_users']
        >>> sizes = {'huge_orders': 100000000, 'small_config': 500, 'medium_users': 5000000}
        >>> result = categorize_and_sort_tables(tables, sizes)
        >>> # Returns [('small_config', 'small', 500), 
        >>> #          ('medium_users', 'medium', 5000000),
        >>> #          ('huge_orders', 'large', 100000000)]
    """
    categorized = []
    
    for table in tables:
        row_count = table_sizes.get(table, 0)
        
        # Categorize based on row count thresholds
        if row_count < SMALL_TABLE_THRESHOLD:
            category = 'small'
        elif row_count < LARGE_TABLE_THRESHOLD:
            category = 'medium'
        else:
            category = 'large'
        
        categorized.append((table, category, row_count))
    
    # Sort by category (small first, then medium, then large) 
    # and by row count within each category
    # The multiplication by 1000000000 ensures category sorting takes precedence
    categorized.sort(
        key=lambda x: ('small', 'medium', 'large').index(x[1]) * 1000000000 + x[2]
    )
    
    return categorized
