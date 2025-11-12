"""Helper exports for the Bright Data scrape connector."""

from helpers.data_processing import collect_all_fields, process_scrape_result
from helpers.schema_management import update_fields_yaml
from helpers.scrape import perform_scrape
from helpers.validation import validate_configuration

__all__ = [
    "collect_all_fields",
    "process_scrape_result",
    "perform_scrape",
    "update_fields_yaml",
    "validate_configuration",
]
