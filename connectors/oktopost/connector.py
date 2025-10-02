import csv
import io
import re
import time
import zipfile
from datetime import datetime
from typing import Dict, List, Any
from urllib.parse import urlparse

import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

"""
Oktopost BI Export Fivetran Connector

This connector fetches export metadata from Oktopost BI Export API and downloads
the associated CSV files for data synchronization.
"""

# Configuration constants
__BASE_URL = "https://api.oktopost.com/v2"
__API_TIMEOUT_SECONDS = 30
__MAX_RETRIES = 3
__RETRY_DELAY_SECONDS = 1  # Base delay for exponential backoff


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing or invalid.
    """
    # Validate required configuration parameters
    required_configs = ["account_id", "api_key"]
    for key in required_configs:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate base_url format if provided
    base_url = configuration.get("base_url")
    if base_url:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid base_url format: {base_url}. Must start with 'http://' or 'https://'"
            )

        # Basic URL validation
        try:
            parsed = urlparse(base_url)
            if not parsed.netloc:
                raise ValueError(
                    f"Invalid base_url format: {base_url}. Must include a valid domain"
                )
        except Exception as e:
            raise ValueError(f"Invalid base_url format: {base_url}. {str(e)}")


def make_api_request_with_retry(
    url: str, headers: Dict[str, str], auth: tuple = None
) -> requests.Response:
    """Make API request with exponential backoff retry logic."""
    for attempt in range(__MAX_RETRIES + 1):
        try:
            if attempt > 0:
                delay = __RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                log.info(f"Retrying {url} (attempt {attempt + 1}) after {delay:.1f}s")
                time.sleep(delay)

            response = requests.get(url, headers=headers, auth=auth, timeout=__API_TIMEOUT_SECONDS)

            # Retry on rate limiting or server errors
            if response.status_code in [429] or 500 <= response.status_code < 600:
                if attempt < __MAX_RETRIES:
                    log.warning(f"HTTP {response.status_code} on attempt {attempt + 1}, retrying")
                    continue
                else:
                    raise RuntimeError(
                        f"HTTP {response.status_code} after {__MAX_RETRIES + 1} attempts"
                    )

            response.raise_for_status()
            return response

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < __MAX_RETRIES:
                log.warning(f"{type(e).__name__} on attempt {attempt + 1}, retrying")
                continue
            else:
                raise RuntimeError(f"{type(e).__name__} after {__MAX_RETRIES + 1} attempts")

        except requests.exceptions.HTTPError as e:
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                raise RuntimeError(f"Client error {e.response.status_code}: {e.response.text}")
            elif attempt < __MAX_RETRIES:
                log.warning(f"HTTP {e.response.status_code} on attempt {attempt + 1}, retrying")
                continue
            else:
                raise RuntimeError(
                    f"HTTP {e.response.status_code} after {__MAX_RETRIES + 1} attempts"
                )

    raise RuntimeError(f"Request failed after {__MAX_RETRIES + 1} attempts")


def extract_csv_from_zip(zip_content: bytes) -> List[Dict[str, Any]]:
    """Extract CSV files from a ZIP archive."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            return [
                {
                    "filename": file_info.filename,
                    "content": zip_file.read(file_info.filename).decode("utf-8"),
                }
                for file_info in zip_file.filelist
                if file_info.filename.lower().endswith(".csv") and not file_info.is_dir()
            ]
    except Exception as e:
        log.severe(f"Error extracting ZIP file: {str(e)}")
        raise


def normalize_table_name(filename: str) -> str:
    """Normalize filename to generate consistent table names."""
    # Remove file extension
    name_without_ext = re.sub(r"\.[^.]*$", "", filename)
    return (
        re.sub(r"^\d{4}-\d{2}-\d{2}-", "", name_without_ext).replace("-", "_").lower().rstrip("_")
    )


def process_csv_data(
    csv_content: str, filename: str, export_id: str, current_sync: str
) -> List[Dict[str, Any]]:
    """Process CSV content and return rows for upserting."""
    try:
        return [
            {
                **{
                    key.strip()
                    .replace(" ", "_")
                    .replace('"', "")
                    .lower(): value.strip() if isinstance(value, str) else value
                    for key, value in row.items()
                },
                "export_id": export_id,
                "source_filename": filename,
            }
            for row in csv.DictReader(io.StringIO(csv_content))
        ]
    except Exception as e:
        log.severe(f"Error processing CSV content from {filename}: {str(e)}")
        raise


def update(configuration: Dict[str, str], state: Dict[str, Any]):
    """Main update function that fetches export metadata and CSV data from Oktopost BI Export API."""
    try:
        # Validate configuration first
        validate_configuration(configuration=configuration)

        account_id = configuration.get("account_id")
        api_key = configuration.get("api_key")
        base_url = configuration.get("base_url", __BASE_URL)
        test_export_id = configuration.get("test_export_id")

        auth = (account_id, api_key)
        headers = {"Content-Type": "application/json"}
        current_sync = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        log.info(f"Starting sync. Current sync: {current_sync}")

        if test_export_id:
            log.info(f"Testing with specific export ID: {test_export_id}")
            exports_to_process = [{"Id": test_export_id}]
        else:
            response = make_api_request_with_retry(f"{base_url}/bi-export", headers, auth)
            exports_data = response.json()

            if not exports_data.get("Result"):
                log.warning("API returned Result=false for export list")
                return

            exports = exports_data.get("Exports", [])
            log.info(f"Found {len(exports)} total exports")

            # Store export list metadata
            for export in exports:
                op.upsert(
                    table="export_list_metadata", data={**export, "sync_timestamp": current_sync}
                )

            exports_to_process = [export for export in exports if export.get("Status") == "active"]
            log.info(f"Found {len(exports_to_process)} active exports")

        # Process each export
        for export in exports_to_process:
            export_id = export.get("Id")
            if not export_id:
                log.warning("Export missing ID, skipping")
                continue

            log.info(f"Processing export {export_id}")

            # Fetch detailed export information
            detail_url = f"{base_url}/bi-export/{export_id}"
            try:
                detail_response = make_api_request_with_retry(detail_url, headers, auth)
                detail_data = detail_response.json()
            except Exception as e:
                log.warning(f"Failed to fetch details for export {export_id}: {str(e)}, skipping")
                continue

            if not detail_data.get("Result"):
                log.warning(f"API returned Result=false for export {export_id}")
                continue

            export_detail = detail_data.get("Export", {})

            # Store export metadata
            file_url = export_detail.get("LastRunFile")
            source_filename = urlparse(file_url).path.split("/")[-1] if file_url else None
            op.upsert(
                table="active_export_metadata",
                data={
                    **export_detail,
                    "sync_timestamp": current_sync,
                    "source_filename": source_filename,
                },
            )

            if file_url:
                log.info(f"Downloading file for export {export_id}")

                try:
                    file_response = make_api_request_with_retry(file_url, {}, None)
                    original_filename = urlparse(file_url).path.split("/")[-1]

                    if original_filename.lower().endswith(".zip"):
                        log.info(
                            f"File {original_filename} is a ZIP archive, extracting CSV files"
                        )
                        csv_files = extract_csv_from_zip(file_response.content)

                        if not csv_files:
                            log.warning(f"No CSV files found in ZIP archive {original_filename}")
                            continue

                        for csv_file in csv_files:
                            table_name = normalize_table_name(original_filename)
                            log.info(
                                f"Processing CSV data from {csv_file['filename']} into table: {table_name}"
                            )

                            rows = process_csv_data(
                                csv_file["content"], original_filename, export_id, current_sync
                            )

                            for row in rows:
                                op.upsert(table=table_name, data=row)
                            log.info(
                                f"Processed {len(rows)} rows for table {table_name} from {csv_file['filename']}"
                            )

                    else:
                        table_name = normalize_table_name(original_filename)
                        log.info(f"Processing CSV data into table: {table_name}")

                        rows = process_csv_data(
                            file_response.text, original_filename, export_id, current_sync
                        )
                        for row in rows:
                            op.upsert(table=table_name, data=row)
                        log.info(f"Processed {len(rows)} rows for table {table_name}")

                except Exception as e:
                    log.severe(f"Error processing file for export {export_id}: {str(e)}")
                    continue

        op.checkpoint(state={"last_sync_timestamp": current_sync})

        log.info("Sync completed successfully")

    except Exception as e:
        log.severe(f"Error in update function: {str(e)}")
        raise


def schema(configuration: Dict[str, str]) -> List[Dict[str, Any]]:
    """Define the schema for the connector tables."""
    return [{"table": "active_export_metadata"}, {"table": "export_list_metadata"}]


# Create the connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
