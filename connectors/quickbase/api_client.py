"""QuickBase API client for handling all API interactions."""

from typing import Dict, List, Any, Optional
import requests
from fivetran_connector_sdk import Logging as log

from config import QuickBaseConfig, __API_BASE_URL
from data_processor import RecordsResponse


class QuickBaseAPIClient:
    """QuickBase API client for handling all API interactions."""

    def __init__(self, config: QuickBaseConfig):
        self.config = config
        self.base_url = __API_BASE_URL
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

    def get_records(
        self, table_id: str, last_sync_time: Optional[str] = None
    ) -> "RecordsResponse":
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
        from datetime import datetime

        try:
            sync_date = datetime.fromisoformat(last_sync_time.replace("Z", "+00:00"))
            sync_date_str = sync_date.strftime("%Y%m%d")
            return f"{{'{self.config.date_field_for_incremental}'.GTE.'{sync_date_str}'}}"
        except Exception as e:
            log.info(f"Could not parse last_sync_time for incremental sync: {e}")
            return ""
