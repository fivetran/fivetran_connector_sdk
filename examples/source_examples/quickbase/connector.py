"""QuickBase API Connector for Fivetran Connector SDK.

This connector demonstrates how to fetch data from QuickBase API and upsert
it into destination using the Fivetran Connector SDK.
Supports querying data from Applications, Tables, Fields, and Records.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
and the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

import requests

from fivetran_connector_sdk import Connector, Logging as log, Operations as op


@dataclass
class RecordsResponse:
    """Response from get_records method containing all fetched data."""

    records: List[Dict[str, Any]]
    fields_map: Dict[str, str]
    metadata: Dict[str, Any]

    @property
    def records_count(self) -> int:
        """Get number of records in this response."""
        return len(self.records)


# Constants
_API_BASE_URL = "https://api.quickbase.com/v1"
_DEFAULT_TIMEOUT = 30
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_MAX_RECORDS = 1000
_DEFAULT_SYNC_FREQUENCY_HOURS = 4
_DEFAULT_INITIAL_SYNC_DAYS = 90
_RECORD_ID_FIELD = "3"  # QuickBase standard Record ID field


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
    sync_frequency_hours: int = _DEFAULT_SYNC_FREQUENCY_HOURS
    initial_sync_days: int = _DEFAULT_INITIAL_SYNC_DAYS
    max_records_per_page: int = _DEFAULT_MAX_RECORDS
    request_timeout_seconds: int = _DEFAULT_TIMEOUT
    retry_attempts: int = _DEFAULT_RETRY_ATTEMPTS
    enable_incremental_sync: bool = True
    enable_fields_sync: bool = True
    enable_records_sync: bool = True
    date_field_for_incremental: str = _RECORD_ID_FIELD
    enable_debug_logging: bool = False


class QuickBaseAPIClient:
    """QuickBase API client for handling all API interactions."""

    def __init__(self, config: QuickBaseConfig):
        self.config = config
        self.base_url = _API_BASE_URL
        self.headers = {
            "Authorization": f"QB-USER-TOKEN {config.user_token}",
            "QB-Realm-Hostname": config.realm_hostname,
            "Content-Type": "application/json",
            "User-Agent": "Fivetran-QuickBase-Connector/2.0",
        }

    def _make_request(
        self, endpoint: str, method: str = "GET", data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make API request with retry logic and error handling."""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.config.retry_attempts):
            try:
                if method.upper() == "POST":
                    response = requests.post(
                        url,
                        headers=self.headers,
                        json=data,
                        timeout=self.config.request_timeout_seconds,
                    )
                else:
                    response = requests.get(
                        url,
                        headers=self.headers,
                        timeout=self.config.request_timeout_seconds,
                    )

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == self.config.retry_attempts - 1:
                    log.severe(
                        f"API request failed after {self.config.retry_attempts} attempts: {e}"
                    )
                    raise RuntimeError(f"API request failed: {e}")
                else:
                    log.info(f"API request attempt {attempt + 1} failed, retrying: {e}")
                    continue

        raise RuntimeError("Unexpected error in API request execution")

    def get_application(self) -> Optional[Dict[str, Any]]:
        """Fetch application data from QuickBase."""
        if not self.config.app_id:
            return None

        try:
            endpoint = f"/apps/{self.config.app_id}"
            return self._make_request(endpoint)
        except Exception as e:
            log.info(f"Could not fetch application details: {e}")
            return None

    def get_fields(self, table_id: str) -> List[Dict[str, Any]]:
        """Fetch fields data from QuickBase table."""
        endpoint = "/fields"
        query_data = {"tableId": table_id}

        try:
            response = self._make_request(endpoint, "POST", query_data)
            if response and "metadata" in response and "fields" in response["metadata"]:
                return response["metadata"]["fields"]
            return []
        except Exception as e:
            log.info(f"Could not fetch fields for table {table_id}: {e}")
            return []

    def get_records(self, table_id: str, last_sync_time: Optional[str] = None) -> RecordsResponse:
        """Fetch records data from QuickBase table with pagination."""
        endpoint = "/records/query"
        query_data = {
            "from": table_id,
            "options": {"skip": 0, "top": self.config.max_records_per_page},
        }

        # Add incremental sync filter if available
        if last_sync_time and self.config.date_field_for_incremental:
            query_data["where"] = self._build_incremental_filter(last_sync_time)

        all_records = []
        fields_map = {}
        final_metadata = {}
        skip = 0

        while True:
            query_data["options"]["skip"] = skip

            try:
                response = self._make_request(endpoint, "POST", query_data)

                if not response or "data" not in response:
                    break

                records = response["data"]
                if not records:
                    break

                # Build fields mapping on first iteration
                if not fields_map and "fields" in response:
                    fields_map = {
                        str(field.get("id", "")): field.get(
                            "label", f"field_{field.get('id', '')}"
                        )
                        for field in response["fields"]
                    }

                all_records.extend(records)

                # Check pagination
                metadata = response.get("metadata", {})
                final_metadata = metadata  # Store the final metadata
                total_records = metadata.get("totalRecords", 0)
                num_records = metadata.get("numRecords", 0)

                if (
                    len(records) < self.config.max_records_per_page
                    or skip + num_records >= total_records
                ):
                    break

                skip += num_records

            except Exception as e:
                log.info(f"Error fetching records for table {table_id}: {e}")
                break

        return RecordsResponse(records=all_records, fields_map=fields_map, metadata=final_metadata)

    def _build_incremental_filter(self, last_sync_time: str) -> str:
        """Build incremental sync filter for QuickBase query."""
        try:
            sync_date = datetime.fromisoformat(last_sync_time.replace("Z", "+00:00"))
            sync_date_str = sync_date.strftime("%Y%m%d")
            return f"{{'{self.config.date_field_for_incremental}'.GTE.'{sync_date_str}'}}"
        except Exception as e:
            log.info(f"Could not parse last_sync_time for incremental sync: {e}")
            return ""


class QuickBaseDataProcessor:
    """Data processor for transforming QuickBase API responses."""

    @staticmethod
    def extract_timestamp_from_data(
        data: Dict[str, Any], possible_fields: Optional[List[str]] = None
    ) -> Optional[str]:
        """Extract timestamp from API data, checking common timestamp field names."""
        if possible_fields is None:
            possible_fields = [
                "updated",
                "lastModified",
                "modified",
                "dateModified",
                "created",
            ]

        for field in possible_fields:
            timestamp = data.get(field)
            if timestamp:
                return str(timestamp)
        return None

    @staticmethod
    def process_application_data(app_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process application data into standardized format."""
        api_timestamp = QuickBaseDataProcessor.extract_timestamp_from_data(
            app_data, ["updated", "created"]
        )
        timestamp = api_timestamp if api_timestamp else datetime.now().isoformat()

        return {
            "app_id": app_data.get("id", ""),
            "name": app_data.get("name", ""),
            "description": app_data.get("description", ""),
            "created": app_data.get("created", ""),
            "updated": app_data.get("updated", ""),
            "date_format": app_data.get("dateFormat", ""),
            "time_zone": app_data.get("timeZone", ""),
            "ancestor_id": app_data.get("ancestorId", ""),
            "has_everyone_on_internet": app_data.get("hasEveryoneOnTheInternet", False),
            "data_classification": app_data.get("dataClassification", ""),
            "variables": json.dumps(app_data.get("variables", [])),
            "security_properties": json.dumps(app_data.get("securityProperties", {})),
            "memory_info": json.dumps(app_data.get("memoryInfo", {})),
            "timestamp": timestamp,
        }

    @staticmethod
    def process_table_data(
        app_id: str, table_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process table data into standardized format."""
        if table_data:
            api_timestamp = (
                table_data.get("lastModified")
                or table_data.get("modified")
                or table_data.get("updated")
            )
            created_time = table_data.get("created", "")
            updated_time = table_data.get("updated", "")
            name = table_data.get("name", f"Table_{app_id}")
            description = table_data.get("description", "QuickBase table")
        else:
            api_timestamp = None
            created_time = ""
            updated_time = ""
            name = f"Table_{app_id}"
            description = "QuickBase table"

        timestamp = api_timestamp if api_timestamp else datetime.now().isoformat()

        return {
            "table_id": app_id,
            "app_id": app_id,
            "name": name,
            "description": description,
            "created": created_time,
            "updated": updated_time,
            "key_field_id": "",
            "next_field_id": "",
            "next_record_id": "",
            "default_sort_field_id": "",
            "timestamp": timestamp,
        }

    @staticmethod
    def process_field_data(field: Dict[str, Any], table_id: str) -> Dict[str, Any]:
        """Process field data into standardized format."""
        api_timestamp = QuickBaseDataProcessor.extract_timestamp_from_data(field)
        timestamp = api_timestamp if api_timestamp else datetime.now().isoformat()
        return {
            "field_id": str(field.get("id", "")),
            "table_id": table_id,
            "name": field.get("label", ""),
            "field_type": field.get("type", ""),
            "mode": field.get("mode", ""),
            "no_wrap": field.get("noWrap", False),
            "bold": field.get("bold", False),
            "required": field.get("required", False),
            "appears_by_default": field.get("appearsByDefault", False),
            "find_enabled": field.get("findEnabled", False),
            "unique": field.get("unique", False),
            "does_total": field.get("doesTotal", False),
            "does_average": field.get("doesAverage", False),
            "does_max": field.get("doesMax", False),
            "does_min": field.get("doesMin", False),
            "does_stddev": field.get("doesStdDev", False),
            "does_count": field.get("doesCount", False),
            "does_data_copy": field.get("doesDataCopy", False),
            "field_help": field.get("fieldHelp", ""),
            "audited": field.get("audited", False),
            "timestamp": timestamp,
        }

    @staticmethod
    def process_record_data(
        record: Dict[str, Any], table_id: str, fields_map: Dict[str, str]
    ) -> Dict[str, Any]:
        """Process record data into standardized format."""
        api_timestamp = None
        record_data = {
            "table_id": table_id,
            "record_id": "",
        }

        # Process each field value in the record
        for field_id, field_data in record.items():
            field_name = fields_map.get(field_id, f"field_{field_id}")
            field_value = (
                field_data.get("value", "") if isinstance(field_data, dict) else field_data
            )

            # Convert field value to string for consistency
            record_data[field_name] = str(field_value) if field_value is not None else ""

            # Use field 3 (Record ID#) as the record_id if available
            if field_id == _RECORD_ID_FIELD:
                record_data["record_id"] = str(field_value) if field_value else ""

            # Look for timestamp fields in the record data
            if field_name.lower() in [
                "date_modified",
                "datemodified",
                "last_modified",
                "lastmodified",
                "updated",
                "modified",
            ]:
                if field_value and not api_timestamp:
                    api_timestamp = str(field_value)

        # Set timestamp from API data or default to current time
        timestamp = api_timestamp if api_timestamp else datetime.now().isoformat()
        record_data["timestamp"] = timestamp

        return record_data

    @staticmethod
    def create_sync_metadata(
        table_id: str,
        records_count: int,
        status: str = "success",
        error_message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create sync metadata record."""
        timestamp = datetime.now().isoformat()

        sync_record = {
            "table_id": table_id,
            "last_sync_time": timestamp,
            "total_records_synced": records_count,
            "sync_status": status,
            "error_message": error_message,
            "timestamp": timestamp,
        }

        # Add pagination metadata if available
        if metadata:
            sync_record.update(
                {
                    "total_records": metadata.get("totalRecords", 0),
                    "num_records": metadata.get("numRecords", 0),
                    "num_fields": metadata.get("numFields", 0),
                    "skip": metadata.get("skip", 0),
                    "top": metadata.get("top", 0),
                }
            )

        return sync_record


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
            configuration.get("sync_frequency_hours"), _DEFAULT_SYNC_FREQUENCY_HOURS
        ),
        initial_sync_days=safe_int(
            configuration.get("initial_sync_days"), _DEFAULT_INITIAL_SYNC_DAYS
        ),
        max_records_per_page=safe_int(
            configuration.get("max_records_per_page"), _DEFAULT_MAX_RECORDS
        ),
        request_timeout_seconds=safe_int(
            configuration.get("request_timeout_seconds"), _DEFAULT_TIMEOUT
        ),
        retry_attempts=safe_int(configuration.get("retry_attempts"), _DEFAULT_RETRY_ATTEMPTS),
        enable_incremental_sync=safe_bool(configuration.get("enable_incremental_sync"), True),
        enable_fields_sync=safe_bool(configuration.get("enable_fields_sync"), True),
        enable_records_sync=safe_bool(configuration.get("enable_records_sync"), True),
        date_field_for_incremental=str(
            configuration.get("date_field_for_incremental", _RECORD_ID_FIELD)
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


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """Define the schema for QuickBase connector tables."""
    return [
        {
            "table": "application",
            "primary_key": ["app_id"],
        },
        {
            "table": "table",
            "primary_key": ["table_id"],
        },
        {
            "table": "field",
            "primary_key": ["field_id", "table_id"],
        },
        {
            "table": "record",
            "primary_key": ["table_id", "record_id"],
        },
        {
            "table": "sync_metadata",
            "primary_key": ["table_id"],
        },
    ]


def update(configuration: dict, state: dict) -> None:
    """Main sync function for QuickBase connector."""
    log.info("Starting QuickBase API connector sync")

    try:
        # Parse and validate configuration
        config = parse_configuration(configuration)
        validate_configuration(config)

        # Initialize API client and data processor
        api_client = QuickBaseAPIClient(config)
        processor = QuickBaseDataProcessor()

        # Determine sync type
        sync_type = determine_sync_type(state, config.enable_incremental_sync)
        last_sync_time = state.get("last_sync_time")

        if config.enable_debug_logging:
            log.info(f"Sync type: {sync_type.value}, App ID: {config.app_id}")

        # Sync applications data
        app_data = api_client.get_application()
        if app_data:
            processed_app = processor.process_application_data(app_data)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="application", data=processed_app)
            log.info("Applications data synced")

        # Sync tables data (simplified - just create entry for app_id)
        table_data = processor.process_table_data(config.app_id)
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="table", data=table_data)
        log.info("Tables data synced")

        # Determine tables to sync
        tables_to_sync = config.table_ids if config.table_ids else [config.app_id]

        # Sync each table
        for table_id in tables_to_sync:
            log.info(f"Processing table: {table_id}")

            # Sync fields data
            if config.enable_fields_sync:
                fields = api_client.get_fields(table_id)
                for field in fields:
                    processed_field = processor.process_field_data(field, table_id)
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="field", data=processed_field)
                log.info(f"Fields synced for table {table_id}: {len(fields)} fields")

            # Sync records data
            if config.enable_records_sync:
                table_last_sync = (
                    state.get(f"table_{table_id}_last_sync", last_sync_time)
                    if sync_type == SyncType.INCREMENTAL
                    else None
                )

                response = api_client.get_records(table_id, table_last_sync)

                for record in response.records:
                    processed_record = processor.process_record_data(
                        record, table_id, response.fields_map
                    )
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="record", data=processed_record)

                # Create sync metadata with API metadata
                sync_metadata = processor.create_sync_metadata(
                    table_id, response.records_count, metadata=response.metadata
                )
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="sync_metadata", data=sync_metadata)

                log.info(f"Records synced for table {table_id}: {response.records_count} records")

        # Update state
        current_time = datetime.now().isoformat()
        new_state = {"last_sync_time": current_time}

        # Add table-specific sync times
        for table_id in tables_to_sync:
            new_state[f"table_{table_id}_last_sync"] = current_time

        # Update state with the current sync time for the next run
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)
        log.info("QuickBase connector sync completed successfully")

    except Exception as e:
        log.severe(f"QuickBase sync failed: {e}")
        raise RuntimeError(f"QuickBase sync failed: {e}")


# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
