"""This connector fetches API security vulnerability data from the National Vulnerability Database (NVD) 2.0 API.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # The main class for creating a connector
from fivetran_connector_sdk import Logging as log  # For enabling Logs in the connector code
from fivetran_connector_sdk import Operations as op  # For performing data operations like upsert and checkpoint

""" SOURCE-SPECIFIC IMPORTS """
import datetime  # For handling date and time objects
import requests  # For making HTTP requests to the NVD API
import time  # For rate limiting and measuring sync duration
import os  # For file path operations when writing temp files


def validate_configuration(configuration: dict):
    """
    Validate that an API key is present in the configuration.
    """
    if 'api_key' not in configuration or not configuration['api_key']:
        log.warning("No API key provided. The connector will use public access with stricter rate limits.")
        
# Default OWASP API Top 10 CWE IDs plus CWE-79 (used if not specified in config)
__DEFAULT_CWE_IDS = [
    "CWE-285", "CWE-639", "CWE-287", "CWE-288", "CWE-290", "CWE-294", "CWE-301",
    "CWE-302", "CWE-303", "CWE-304", "CWE-306", "CWE-307", "CWE-521", "CWE-798",
    "CWE-269", "CWE-400", "CWE-770", "CWE-642", "CWE-918", "CWE-16", "CWE-209",
    "CWE-598", "CWE-20", "CWE-73", "CWE-200", "CWE-79"
]

# Constants for magic values
__RAW_DATA_DIR = "raw_data"
__RESULTS_PER_PAGE = 2000
__API_RATE_LIMIT_DELAY = 0.6

def schema(configuration: dict):
    validate_configuration(configuration)

    log_level = configuration.get("logging_level", "debug").lower()
    is_debug_logging = log_level == "debug"

    if 'api_key' not in configuration:
        log.warning("No API key provided in configuration.json; using public access with rate limits.")
    else:
        if is_debug_logging:
            log.info("Using API key from configuration.json for access.")
    
    # The 'logging_level' configuration is used to control log verbosity.
    
    return [
        {
            "table": "owasp_api_vulnerabilities",
            "primary_key": ["cve_id"],
            "columns": {
                "cve_id": "STRING",
                "description": "STRING",
                "published_date": "STRING",
                "last_modified_date": "STRING",
                "cwe_ids": "JSON",
                "affected_libraries": "JSON",
                "fixed_versions": "JSON",
                "remediations": "JSON",
                "severity": "STRING"
            }
        },
        {
            "table": "owasp_api_sync_log",
            "primary_key": ["sync_datetime"],
            "columns": {
                "sync_datetime": "STRING",
                "sync_type": "STRING",
                "total_rows_upserted": "LONG"
            }
        }
    ]

def parse_date(date_str):
    """Parses and formats a date string to a specific ISO 8601 format.

    Args:
        date_str (str): The date string to parse.

    Returns:
        str: The formatted date string, or the original string if parsing fails.
             Returns None if the input is empty.
    """
    if not date_str:
        return None
    if '+' not in date_str and '-' not in date_str[-6:]:
        date_str += '+01:00'
    try:
        dt_obj = datetime.datetime.fromisoformat(date_str)
        return dt_obj.strftime('%Y-%m-%dT%H:%M:%S.000+01:00')  # Format with millisecond precision and +01:00
    except ValueError as e:
        log.warning(f"Date parsing error for {date_str}: {str(e)} - Using original string")
        return date_str

def _get_sync_config(configuration: dict):
    """Parses sync-related configuration."""
    log_level = configuration.get("logging_level", "debug").lower()
    is_debug_logging = log_level == "debug"
    api_key = configuration.get("api_key", "")
    headers = {"apiKey": api_key} if api_key else {}
    write_temp_files = configuration.get("write_temp_files", "false").lower() == "true"
    force_full_sync = configuration.get("force_full_sync", "false").lower() == "true"
    
    cwe_ids_str = configuration.get("cwe_ids", ",".join(__DEFAULT_CWE_IDS))
    cwe_ids = [cwe.strip() for cwe in cwe_ids_str.split(",") if cwe.strip()]
    if not cwe_ids:
        log.warning("cwe_ids is empty or invalid in configuration.json; using default CWE IDs")
        cwe_ids = __DEFAULT_CWE_IDS

    if api_key:
        if is_debug_logging:
            log.info("Using API key from configuration.json for access")
    else:
        log.warning("No API key found in configuration.json; falling back to public access with rate limits")

    return is_debug_logging, headers, write_temp_files, force_full_sync, cwe_ids

def _initialize_state(state: dict, force_full_sync: bool, is_debug_logging: bool):
    """Initializes or loads the sync state."""
    if force_full_sync:
        if is_debug_logging:
            log.info("Forcing full sync; ignoring previous state")
        return {}
    
    if is_debug_logging:
        log.info("Performing incremental sync based on previous state")
    if "last_sync_time" in state:
        log.info(f"State loaded: last_sync_time = {state['last_sync_time']}")
    else:
        log.warning("No previous state found; defaulting to full sync")
        return {}
    return state

def _build_request_params(cwe: str, state: dict, force_full_sync: bool):
    """Builds the parameters for the NVD API request."""
    params = {"cweId": cwe}
    if not force_full_sync and "last_sync_time" in state:
        try:
            last_sync = datetime.datetime.fromisoformat(state["last_sync_time"].replace('Z', '+01:00'))
            params["lastModStartDate"] = (last_sync - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
            params["lastModEndDate"] = datetime.datetime.utcnow().replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
            log.info(f"Incremental sync for {cwe}: lastModStartDate={params['lastModStartDate']}, lastModEndDate={params['lastModEndDate']}")
        except ValueError as e:
            log.warning(f"Invalid last_sync_time format: {e}; defaulting to full sync")
            params.pop("lastModStartDate", None)
            params.pop("lastModEndDate", None)
    else:
        params["resultsPerPage"] = __RESULTS_PER_PAGE
        params["startIndex"] = 0
    return params

def _process_vulnerability(vuln: dict, cwe_ids: list, is_debug_logging: bool):
    """Processes a single vulnerability, returning a row for upsert or None."""
    cve = vuln.get("cve")
    if not cve:
        log.warning("Invalid CVE structure found in response.")
        return None

    cve_id = cve.get("id")
    # Caller checks for presence of cve_id

    cve_cwes = [d["value"] for w in cve.get("weaknesses", []) for d in w.get("description", []) if d.get("lang") == "en" and d.get("value", "").startswith("CWE-")]
    
    desc_lower = cve.get("descriptions", [{}])[0].get("value", "").lower()
    if cve_cwes and not set(cve_cwes) & set(cwe_ids) and "api" not in desc_lower:
        if is_debug_logging:
            log.info(f"No matching CWEs or 'api' in description for CVE {cve_id}; skipping")
        return None

    if is_debug_logging:
        log.info(f"Processing CVE {cve_id} with CWEs: {cve_cwes}, Description: {desc_lower[:50]}...")

    affected = []
    for config in cve.get("configurations", []):
        for node in config.get("nodes", []):
            for match in node.get("cpeMatch", []):
                if match.get("vulnerable"):
                    cpe = match["criteria"].split(":")
                    if len(cpe) > 4:
                        is_app_cpe = cpe[2] == "a"
                        is_python_or_java = "python" in cpe[3].lower() or "java" in cpe[3].lower() or "python" in cpe[4].lower() or "java" in cpe[4].lower()
                        if is_app_cpe and is_python_or_java:
                            lang = cpe[3] if cpe[3] in ["python", "java"] else cpe[4].split("_")[0]
                            affected.append({
                                "lang": lang,
                                "product": cpe[4],
                                "versions": match.get("versionStartIncluding", "") + "-" + match.get("versionEndExcluding", "")
                            })

    fixes = []
    remediations = [ref["url"] for ref in cve.get("references", []) if "patch" in ref.get("tags", []) or "vendor" in ref.get("tags", [])]
    desc_full = cve.get("descriptions", [{}])[0].get("value", "")
    if "fixed in" in desc_full.lower() and len(desc_full.split("fixed in")) > 1:
        fixes.append({"fix": desc_full.split("fixed in")[1].strip()[:100]})

    severity = cve.get("metrics", {}).get("cvssMetricV31", [{}])[0].get("cvssData", {}).get("baseSeverity", "UNKNOWN")
    if severity == "UNKNOWN":
        if is_debug_logging:
            log.info(f"Skipping CVE {cve_id} with UNKNOWN severity")
        return None

    return {
        "cve_id": cve_id,
        "description": desc_full,
        "published_date": parse_date(cve.get("published")),
        "last_modified_date": parse_date(cve.get("lastModified")),
        "cwe_ids": cve_cwes,
        "affected_libraries": affected,
        "fixed_versions": fixes,
        "remediations": remediations,
        "severity": severity
    }

def _fetch_and_process_cwe_data(cwe: str, base_url: str, headers: dict, params: dict, write_temp_files: bool, is_debug_logging: bool, upserted_cve_ids: set, cwe_ids_filter: list):
    """Fetches and processes all vulnerabilities for a given CWE."""
    upsert_count_per_cwe = 0
    
    while True:
        response = requests.get(base_url, params=params, headers=headers)
        
        if response.status_code != 200:
            log.severe(f"Error fetching for {cwe}: API error: {response.status_code} - {response.text or 'No detailed error'}")
            break
        
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            log.severe(f"JSON decode error for {cwe} at startIndex {params.get('startIndex', 'N/A')}: {str(e)}")
            break
        
        log.info(f"API response status for {cwe}: {response.status_code}, totalResults: {data.get('totalResults', 'N/A')}, startIndex: {params.get('startIndex', 'N/A')}")

        if data.get("totalResults") == 0 or not data.get("vulnerabilities"):
            if data.get("totalResults") == 0:
                log.info(f"No data for {cwe}")
            elif is_debug_logging:
                log.info(f"No more vulnerabilities for {cwe} at startIndex {params.get('startIndex', 'N/A')}")
            break

        if write_temp_files:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_file = os.path.join(__RAW_DATA_DIR, f"nvd_response-temp-{cwe}-{timestamp}.json")
            with open(temp_file, "w") as f:
                f.write(response.text)
            if is_debug_logging:
                log.info(f"Response saved to {temp_file} for {cwe}")

        vulnerabilities = data.get("vulnerabilities", [])
        for vuln in vulnerabilities:
            cve = vuln.get("cve", {})
            cve_id = cve.get("id")
            if not cve_id or cve_id in upserted_cve_ids:
                continue

            row = _process_vulnerability(vuln, cwe_ids_filter, is_debug_logging)
            
            if row:
                op.upsert(table="owasp_api_vulnerabilities", data=row)
                upserted_cve_ids.add(cve_id)
                upsert_count_per_cwe += 1

        if "resultsPerPage" in params:
            params["startIndex"] += data.get("resultsPerPage", 0)
            if params["startIndex"] >= data.get("totalResults", 0):
                break
        else:
            break  # No pagination for incremental sync

        time.sleep(__API_RATE_LIMIT_DELAY)
        
    return upsert_count_per_cwe

def _finalize_sync(start_time: float, total_upserts: int, force_full_sync: bool):
    """Logs sync summary, upserts sync log, and checkpoints state."""
    end_time = time.time()
    duration = end_time - start_time
    log.info(f"Sync completed. Total duration: {duration:.2f} seconds")
    log.info(f"Total upserts across all CWEs: {total_upserts}")

    sync_datetime = datetime.datetime.utcnow().isoformat() + "+00:00"
    sync_type = "full" if force_full_sync else "incremental"
    op.upsert(table="owasp_api_sync_log", data={
        "sync_datetime": sync_datetime,
        "sync_type": sync_type,
        "total_rows_upserted": total_upserts
    })
    op.checkpoint({"last_sync_time": sync_datetime})

def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    start_time = time.time()
    
    is_debug_logging, headers, write_temp_files, force_full_sync, cwe_ids = _get_sync_config(configuration)
    state = _initialize_state(state, force_full_sync, is_debug_logging)
    
    base_url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    upserted_cve_ids = set()
    total_upserts = 0
    
    if write_temp_files:
        os.makedirs(__RAW_DATA_DIR, exist_ok=True)
        
    for cwe in cwe_ids:
        log.info(f"Now pulling CVEs pertaining to {cwe}")
        params = _build_request_params(cwe, state, force_full_sync)
        
        if is_debug_logging:
            log.info(f"Request params for {cwe}: {params}")
            
        try:
            upsert_count_per_cwe = _fetch_and_process_cwe_data(
                cwe, base_url, headers, params, write_temp_files, is_debug_logging, upserted_cve_ids, cwe_ids
            )
            total_upserts += upsert_count_per_cwe
            log.info(f"Results fetched for {cwe}")
            log.info(f"Rows upserted for {cwe}: {upsert_count_per_cwe}")
        except Exception as e:
            log.severe(f"Error fetching for {cwe}: {str(e)}")
            
    _finalize_sync(start_time, total_upserts, force_full_sync)

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
