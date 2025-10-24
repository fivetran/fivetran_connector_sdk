import datetime
import json
import requests
import time
import logging
import os
import sys

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op

# Custom logging filter for standard vs debug logs
class LogLevelFilter(logging.Filter):
    def __init__(self, log_level):
        super().__init__()
        self.log_level = log_level.lower()

    def filter(self, record):
        if self.log_level == "standard":
            allowed_msgs = [
                "Now pulling CVEs",
                "Results fetched for",
                "Rows upserted for",
                "Sync completed",
                "Total upserts across all CWEs",
                "No data for",
                "Invalid last_sync_time format",
                "No API key provided",
                "No previous state found",
                "State loaded",
                "Incremental sync for"
            ]
            return record.levelname in ("ERROR", "WARNING") or any(msg in record.msg for msg in allowed_msgs)
        return True  # Debug mode allows all logs

# Configure logging to force stdout
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any existing handlers
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# Default OWASP API Top 10 CWE IDs plus CWE-79 (used if not specified in config)
DEFAULT_CWE_IDS = [
    "CWE-285", "CWE-639", "CWE-287", "CWE-288", "CWE-290", "CWE-294", "CWE-301",
    "CWE-302", "CWE-303", "CWE-304", "CWE-306", "CWE-307", "CWE-521", "CWE-798",
    "CWE-269", "CWE-400", "CWE-770", "CWE-642", "CWE-918", "CWE-16", "CWE-209",
    "CWE-598", "CWE-20", "CWE-73", "CWE-200", "CWE-79"
]

def schema(configuration: dict):
    if 'api_key' not in configuration:
        logger.warning("No API key provided in configuration.json; using public access with rate limits.")
    else:
        logger.info("Using API key from configuration.json for access.")
    
    # Apply log level filter
    log_level = configuration.get("logging_level", "debug")
    logger.handlers[0].addFilter(LogLevelFilter(log_level))
    
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
    if not date_str:
        return None
    if '+' not in date_str and '-' not in date_str[-6:]:
        date_str += '+01:00'
    try:
        dt_obj = datetime.datetime.fromisoformat(date_str)
        return dt_obj.strftime('%Y-%m-%dT%H:%M:%S.000+01:00')  # Format with millisecond precision and +01:00
    except ValueError as e:
        logger.warning(f"Date parsing error for {date_str}: {str(e)} - Using original string")
        return date_str

def update(configuration: dict, state: dict):
    start_time = time.time()
    
    base_url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    api_key = configuration.get("api_key", "")
    headers = {"apiKey": api_key} if api_key else {}
    write_temp_files = configuration.get("write_temp_files", "false").lower() == "true"
    
    # Parse configurable CWE IDs from comma-separated string
    cwe_ids_str = configuration.get("cwe_ids", ",".join(DEFAULT_CWE_IDS))
    cwe_ids = [cwe.strip() for cwe in cwe_ids_str.split(",") if cwe.strip()]
    if not cwe_ids:
        logger.warning("cwe_ids is empty or invalid in configuration.json; using default CWE IDs")
        cwe_ids = DEFAULT_CWE_IDS
    
    if api_key:
        logger.info("Using API key from configuration.json for access")
    else:
        logger.warning("No API key found in configuration.json; falling back to public access with rate limits")
    
    force_full_sync = configuration.get("force_full_sync", "false")
    if force_full_sync.lower() == "true":
        logger.info("Forcing full sync; ignoring previous state")
        state = {}
    else:
        logger.info("Performing incremental sync based on previous state")
        if "last_sync_time" in state:
            logger.info(f"State loaded: last_sync_time = {state['last_sync_time']}")
        else:
            logger.warning("No previous state found; defaulting to full sync")
            state = {}
    
    upserted_cve_ids = set()
    total_upserts = 0
    
    if write_temp_files:
        raw_data_dir = "raw_data"
        os.makedirs(raw_data_dir, exist_ok=True)
    
    for cwe in cwe_ids:
        logger.info(f"Now pulling CVEs pertaining to {cwe}")
        
        params = {
            "cweId": cwe
        }
        if not force_full_sync.lower() == "true" and "last_sync_time" in state:
            try:
                last_sync = datetime.datetime.fromisoformat(state["last_sync_time"].replace('Z', '+01:00'))
                params["lastModStartDate"] = (last_sync - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
                params["lastModEndDate"] = datetime.datetime.utcnow().replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
                logger.info(f"Incremental sync for {cwe}: lastModStartDate={params['lastModStartDate']}, lastModEndDate={params['lastModEndDate']}")
            except ValueError as e:
                logger.warning(f"Invalid last_sync_time format: {e}; defaulting to full sync")
                params.pop("lastModStartDate", None)
                params.pop("lastModEndDate", None)
        else:
            params["resultsPerPage"] = 2000
            params["startIndex"] = 0
        
        logger.info(f"Request params for {cwe}: {params}")
        
        try:
            upsert_count_per_cwe = 0
            while True:
                response = requests.get(base_url, params=params, headers=headers)
                logger.info(f"API response status for {cwe}: {response.status_code}, totalResults: {data.get('totalResults', 'N/A') if 'data' in locals() else 'N/A'}, startIndex: {params.get('startIndex', 'N/A')}")
                
                if response.status_code != 200:
                    logger.error(f"Error fetching for {cwe}: API error: {response.status_code} - {response.text or 'No detailed error'}")
                    break
                
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for {cwe} at startIndex {params.get('startIndex', 'N/A')}: {str(e)}")
                    break
                
                if "totalResults" in data and data["totalResults"] == 0:
                    logger.info(f"No data for {cwe}")
                    break
                
                if "vulnerabilities" not in data or not data.get("vulnerabilities"):
                    logger.info(f"No more vulnerabilities for {cwe} at startIndex {params.get('startIndex', 'N/A')}")
                    break
                
                if write_temp_files:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    temp_file = os.path.join(raw_data_dir, f"nvd_response-temp-{cwe}-{timestamp}.json")
                    with open(temp_file, "w") as f:
                        f.write(response.text)
                    logger.info(f"Response saved to {temp_file} for {cwe}")
                
                vulnerabilities = data.get("vulnerabilities", [])
                for vuln in vulnerabilities:
                    cve = vuln.get("cve")
                    if not cve:
                        logger.warning(f"Invalid CVE structure for {cwe} at index {params.get('startIndex', 'N/A')}")
                        continue
                    
                    cve_id = cve.get("id")
                    if not cve_id or cve_id in upserted_cve_ids:
                        continue
                    
                    cve_cwes = []
                    for w in cve.get("weaknesses", []):
                        for d in w.get("description", []):
                            if d.get("lang") == "en" and d.get("value", "").startswith("CWE-"):
                                cve_cwes.append(d["value"])
                    
                    desc = cve.get("descriptions", [{}])[0].get("value", "").lower()
                    if cve_cwes and not set(cve_cwes) & set(cwe_ids) and "api" not in desc:
                        logger.info(f"No matching CWEs or 'api' in description for CVE {cve_id} in {cwe}; skipping")
                        continue
                    
                    logger.info(f"Processing CVE {cve_id} with CWEs: {cve_cwes}, Description: {desc[:50]}...")
                    
                    affected = []
                    for config in cve.get("configurations", []):
                        for node in config.get("nodes", []):
                            for match in node.get("cpeMatch", []):
                                if match.get("vulnerable"):
                                    cpe = match["criteria"].split(":")
                                    if len(cpe) > 4 and cpe[2] == "a" and ("python" in cpe[3].lower() or "java" in cpe[3].lower() or "python" in cpe[4].lower() or "java" in cpe[4].lower()):
                                        lang = cpe[3] if cpe[3] in ["python", "java"] else cpe[4].split("_")[0]
                                        affected.append({
                                            "lang": lang,
                                            "product": cpe[4],
                                            "versions": match.get("versionStartIncluding", "") + "-" + match.get("versionEndExcluding", "")
                                        })
                    
                    fixes = []
                    remediations = [ref["url"] for ref in cve.get("references", []) if "patch" in ref.get("tags", []) or "vendor" in ref.get("tags", [])]
                    desc = cve.get("descriptions", [{}])[0].get("value", "")
                    if "fixed in" in desc.lower() and len(desc.split("fixed in")) > 1:
                        fixes.append({"fix": desc.split("fixed in")[1].strip()[:100]})
                    
                    severity = cve.get("metrics", {}).get("cvssMetricV31", [{}])[0].get("cvssData", {}).get("baseSeverity", "UNKNOWN")
                    if severity == "UNKNOWN":
                        logger.info(f"Skipping CVE {cve_id} with UNKNOWN severity")
                        continue
                    
                    row = {
                        "cve_id": cve_id,
                        "description": desc,
                        "published_date": parse_date(cve.get("published")),
                        "last_modified_date": parse_date(cve.get("lastModified")),
                        "cwe_ids": cve_cwes,
                        "affected_libraries": affected,
                        "fixed_versions": fixes,
                        "remediations": remediations,
                        "severity": severity
                    }
                    
                    op.upsert(table="owasp_api_vulnerabilities", data=row)
                    upserted_cve_ids.add(cve_id)
                    upsert_count_per_cwe += 1
                    total_upserts += 1
                
                if "resultsPerPage" in params:
                    params["startIndex"] += data.get("resultsPerPage", 0)
                    if params["startIndex"] >= data.get("totalResults", 0):
                        break
                else:
                    break  # No pagination for incremental sync
                
                time.sleep(0.6)
            
            logger.info(f"Results fetched for {cwe}")
            logger.info(f"Rows upserted for {cwe}: {upsert_count_per_cwe}")
        
        except Exception as e:
            logger.error(f"Error fetching for {cwe}: {str(e)}")
    
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Sync completed. Total duration: {duration:.2f} seconds")
    logger.info(f"Total upserts across all CWEs: {total_upserts}")
    
    # Insert sync log entry
    sync_datetime = datetime.datetime.utcnow().isoformat() + "+00:00"
    sync_type = "full" if force_full_sync.lower() == "true" else "incremental"
    op.upsert(table="owasp_api_sync_log", data={
        "sync_datetime": sync_datetime,
        "sync_type": sync_type,
        "total_rows_upserted": total_upserts
    })
    
    if total_upserts > 0:
        op.checkpoint({"last_sync_time": sync_datetime})

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)