"""Data processor for transforming QuickBase API responses."""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from config import __RECORD_ID_FIELD


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
            if field_id == __RECORD_ID_FIELD:
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
