"""Configuration management for QuickBase connector."""

from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum

# Constants
__API_BASE_URL = "https://api.quickbase.com/v1"
__DEFAULT_TIMEOUT = 30
__DEFAULT_RETRY_ATTEMPTS = 3
__DEFAULT_MAX_RECORDS = 1000
__DEFAULT_SYNC_FREQUENCY_HOURS = 4
__DEFAULT_INITIAL_SYNC_DAYS = 90
__RECORD_ID_FIELD = "3"  # QuickBase standard Record ID field


class SyncType(Enum):
    """Enumeration for sync types."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"


@dataclass
class QuickBaseConfig:
    """Configuration class for QuickBase connector."""

    user_token: str
    realm_hostname: str
    app_id: str
    table_ids: List[str]
    sync_frequency_hours: int = __DEFAULT_SYNC_FREQUENCY_HOURS
    initial_sync_days: int = __DEFAULT_INITIAL_SYNC_DAYS
    max_records_per_page: int = __DEFAULT_MAX_RECORDS
    request_timeout_seconds: int = __DEFAULT_TIMEOUT
    retry_attempts: int = __DEFAULT_RETRY_ATTEMPTS
    enable_incremental_sync: bool = True
    enable_fields_sync: bool = True
    enable_records_sync: bool = True
    date_field_for_incremental: str = __RECORD_ID_FIELD
    enable_debug_logging: bool = False


def parse_configuration(configuration: dict) -> QuickBaseConfig:
    """Parse and validate configuration dictionary."""

    def safe_int(value: Any, default: int) -> int:
        try:
            return int(str(value))
        except (ValueError, TypeError):
            return default

    def safe_bool(value: Any, default: bool) -> bool:
        return str(value).lower() == "true" if value is not None else default

    def parse_table_ids(table_ids_str: str) -> List[str]:
        if not table_ids_str or table_ids_str.strip() == "":
            return []
        return [tid.strip() for tid in table_ids_str.split(",") if tid.strip()]

    return QuickBaseConfig(
        user_token=str(configuration.get("user_token", "")).strip(),
        realm_hostname=str(configuration.get("realm_hostname", "")).strip(),
        app_id=str(configuration.get("app_id", "")).strip(),
        table_ids=parse_table_ids(str(configuration.get("table_ids", ""))),
        sync_frequency_hours=safe_int(
            configuration.get("sync_frequency_hours"), __DEFAULT_SYNC_FREQUENCY_HOURS
        ),
        initial_sync_days=safe_int(
            configuration.get("initial_sync_days"), __DEFAULT_INITIAL_SYNC_DAYS
        ),
        max_records_per_page=safe_int(
            configuration.get("max_records_per_page"), __DEFAULT_MAX_RECORDS
        ),
        request_timeout_seconds=safe_int(
            configuration.get("request_timeout_seconds"), __DEFAULT_TIMEOUT
        ),
        retry_attempts=safe_int(configuration.get("retry_attempts"), __DEFAULT_RETRY_ATTEMPTS),
        enable_incremental_sync=safe_bool(configuration.get("enable_incremental_sync"), True),
        enable_fields_sync=safe_bool(configuration.get("enable_fields_sync"), True),
        enable_records_sync=safe_bool(configuration.get("enable_records_sync"), True),
        date_field_for_incremental=str(
            configuration.get("date_field_for_incremental", __RECORD_ID_FIELD)
        ),
        enable_debug_logging=safe_bool(configuration.get("enable_debug_logging"), False),
    )


def validate_configuration(config: QuickBaseConfig) -> None:
    """Validate the QuickBase configuration."""
    if not config.user_token:
        raise ValueError("user_token is required and cannot be empty")

    if not config.realm_hostname:
        raise ValueError("realm_hostname is required and cannot be empty")

    if not config.app_id:
        raise ValueError("app_id is required and cannot be empty")

    if not (1 <= config.sync_frequency_hours <= 24):
        raise ValueError("sync_frequency_hours must be between 1 and 24")

    if not (1 <= config.initial_sync_days <= 365):
        raise ValueError("initial_sync_days must be between 1 and 365")

    if not (1 <= config.max_records_per_page <= 1000):
        raise ValueError("max_records_per_page must be between 1 and 1000")


def determine_sync_type(state: dict, enable_incremental: bool) -> SyncType:
    """Determine the type of sync to perform."""
    last_sync_time = state.get("last_sync_time")
    if last_sync_time and enable_incremental:
        return SyncType.INCREMENTAL
    return SyncType.INITIAL


def get_time_range(sync_type: SyncType, config: QuickBaseConfig) -> Dict[str, str]:
    """Generate time range for API queries based on sync type."""
    end_time = datetime.now().isoformat()

    if sync_type == SyncType.INCREMENTAL:
        # This would be set by the caller with actual last sync time
        start_time = end_time
    else:
        # Initial sync: get data from configured days back
        start_time = (datetime.now() - timedelta(days=config.initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}
