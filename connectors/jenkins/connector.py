"""
Fivetran Connector SDK – Jenkins
Pulls jobs, builds, and artifacts from the Jenkins REST API
"""

import json
import time
from typing import Any, Dict, List, Optional, Tuple
import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log


# -----------------------------
# HTTP Client
# -----------------------------
class JenkinsClient:
    def __init__(self, base_url: str, username: str, api_token: str, timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, api_token)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "fivetran-connector-sdk-jenkins/1.0",
            }
        )

    def request(self, method: str, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make an HTTP request with retries for rate limiting and server errors.
        Args: param method: HTTP method (GET, POST, etc.)
              param path: API endpoint path
              param params: Query parameters
        Returns: Parsed JSON response
        Raises: RuntimeError on failure after retries
        """
        url = f"{self.base}{path}"
        for attempt in range(1, 6):
            r = self.session.request(method, url, params=params or {}, timeout=self.timeout)
            if r.status_code == 429:
                sleep_for = int(r.headers.get("Retry-After", 2))
                log.info(f"429 rate limited – sleeping {sleep_for}s (attempt {attempt})")
                time.sleep(sleep_for)
                continue
            if r.status_code >= 500:
                log.warning(f"{r.status_code} from {url}; retrying (attempt {attempt})")
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Failed after retries: {url}")

    # --- Jenkins API helpers ---

    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        List all top-level jobs in Jenkins.
        Returns: List of job dictionaries
        """
        # Top-level jobs
        data = self.request("GET", "/api/json")
        jobs = data.get("jobs", []) or []
        log.info(f"Fetched {len(jobs)} jobs")
        return jobs

    def list_builds_for_job(self, job_url: str) -> List[Dict[str, Any]]:
        """
        List builds for a given job.
        Args param job_url: URL of the Jenkins job
        Returns: List of build dictionaries
        """
        # Limit fields for efficiency
        # timestamp is in ms since epoch
        path = job_url.replace(self.base, "", 1) if job_url.startswith(self.base) else job_url
        if not path.endswith("/"):
            path += "/"
        data = self.request(
            "GET",
            f"{path}api/json",
            params={"tree": "builds[number,result,timestamp,url]"},
        )
        return data.get("builds", []) or []

    def get_build_detail(self, build_url: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific build.
        Args: param build_url: URL of the Jenkins build
        Returns: Build detail dictionary
        """
        path = build_url.replace(self.base, "", 1) if build_url.startswith(self.base) else build_url
        if not path.endswith("/"):
            path += "/"
        return self.request("GET", f"{path}api/json")


# -----------------------------
# Tables
# -----------------------------
TABLES: Dict[str, Dict[str, Any]] = {
    "jenkins_jobs": {
        "pk": ["id"],
        "schema": {
            "id": "STRING",         # unique job identifier (use URL)
            "name": "STRING",
            "url": "STRING",
            "color": "STRING",      # job status color from Jenkins
            "raw": "JSON",
        },
    },
    "jenkins_builds": {
        "pk": ["id"],              # jobName_number
        "updated": "timestamp_ms", # used for incremental cursor
        "schema": {
            "id": "STRING",
            "job_name": "STRING",
            "number": "INTEGER",
            "result": "STRING",
            "timestamp": "UTC_DATETIME",  # seconds resolution
            "timestamp_ms": "INTEGER",    # original Jenkins ms timestamp
            "url": "STRING",
            "raw": "JSON",
        },
    },
    "jenkins_artifacts": {
        "pk": ["id"],              # jobName_number_fileName
        "schema": {
            "id": "STRING",
            "build_id": "STRING",        # links to jenkins_builds.id
            "job_name": "STRING",
            "build_number": "INTEGER",
            "file_name": "STRING",
            "relative_path": "STRING",
            "timestamp_ms": "INTEGER",
            "url": "STRING",
            "raw": "JSON",
        },
    },
}

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]):
    declared = []
    for t, meta in TABLES.items():
        declared.append(
            {
                "table": t,
                "primary_key": meta["pk"],
                "column": meta["schema"],
            }
        )
    return declared

def get_build_cursor(state: Dict[str, Any], job_name: str) -> int:
    """
    Get the build cursor (last processed timestamp in ms) for a specific job.
    Args: param state: Connector state dictionary
          param job_name: Name of the Jenkins job
    Returns: Last processed timestamp in ms
    """
    try:
        return int((state or {}).get("cursors", {}).get("jenkins_builds", {}).get(job_name, 0))
    except Exception:
        return 0


def set_build_cursor(state: Dict[str, Any], job_name: str, value: int) -> Dict[str, Any]:
    """
    Set the build cursor (last processed timestamp in ms) for a specific job.
    Args: param state: Connector state dictionary
          param job_name: Name of the Jenkins job
          param value: New last processed timestamp in ms
    """
    new_state = dict(state or {})
    cursors = dict(new_state.get("cursors", {}))
    builds_cursor = dict(cursors.get("jenkins_builds", {}))
    if value is not None:
        builds_cursor[job_name] = int(value)
    cursors["jenkins_builds"] = builds_cursor
    new_state["cursors"] = cursors
    return new_state


def to_seconds(ms: Optional[int]) -> Optional[int]:
    """
    Convert milliseconds to seconds.
    Args param ms: Milliseconds
    Returns: Seconds
    """
    if ms is None:
        return None
    try:
        return int(ms) // 1000
    except Exception:
        return None


def fmt_utc_from_ms(ms: Optional[int]) -> Optional[str]:
    """
    Format milliseconds since epoch to ISO 8601 UTC string.
    Args param ms: Milliseconds since epoch
    Returns: ISO 8601 UTC string
    """
    s = to_seconds(ms)
    if s is None:
        return None
    # ISO 8601 without timezone suffix is okay; UTC expected by SDK type "UTC_DATETIME"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(s))

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    base = configuration["base_url"]
    username = configuration["username"]
    api_token = configuration["api_token"]

    client = JenkinsClient(base_url=base, username=username, api_token=api_token)

    # --- 1) Jobs ---
    jobs = client.list_jobs()
    for j in jobs:
        row = {
            "id": j.get("url") or f"{base}/job/{j.get('name','')}/",
            "name": j.get("name"),
            "url": j.get("url"),
            "color": j.get("color"),
            "raw": j,
        }
        # The 'upsert' operation is used to insert or update data in a table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "hello".
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="jenkins_jobs", data=row)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    # --- 2) Builds (incremental per job) ---
    # Track max cursor per job in this run, then persist
    job_maxes: Dict[str, int] = {}

    for j in jobs:
        job_name = j.get("name")
        job_url = j.get("url")
        if not job_name or not job_url:
            continue

        last_ms = get_build_cursor(state, job_name)
        max_seen_ms = last_ms

        builds = client.list_builds_for_job(job_url)

        for b in builds:
            ts_ms = int(b.get("timestamp") or 0)
            if ts_ms <= last_ms:
                # older or equal, already processed
                continue

            bid = f"{job_name}_{b.get('number')}"
            row = {
                "id": bid,
                "job_name": job_name,
                "number": b.get("number"),
                "result": b.get("result"),
                "timestamp": fmt_utc_from_ms(ts_ms),
                "timestamp_ms": ts_ms,
                "url": b.get("url"),
                "raw": b,
            }
            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "hello".
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="jenkins_builds", data=row)

            if ts_ms > max_seen_ms:
                max_seen_ms = ts_ms

        # after processing this job's builds, checkpoint state (per job)
        if max_seen_ms != last_ms:
            state = set_build_cursor(state, job_name, max_seen_ms)
            job_maxes[job_name] = max_seen_ms

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    # --- 3) Artifacts for new builds only ---
    # We look at builds that are > cursor; fetch details to enumerate artifacts
    for j in jobs:
        job_name = j.get("name")
        job_url = j.get("url")
        if not job_name or not job_url:
            continue

        last_ms = get_build_cursor(state, job_name)
        builds = client.list_builds_for_job(job_url)

        for b in builds:
            ts_ms = int(b.get("timestamp") or 0)
            if ts_ms <= last_ms:
                continue

            # fetch build detail to get artifacts
            detail = client.get_build_detail(b.get("url"))
            arts = detail.get("artifacts", []) or []
            if not arts:
                continue

            for a in arts:
                file_name = a.get("fileName")
                rel = a.get("relativePath")
                build_number = b.get("number")
                bid = f"{job_name}_{build_number}"
                aid = f"{job_name}_{build_number}_{file_name}"

                row = {
                    "id": aid,
                    "build_id": bid,
                    "job_name": job_name,
                    "build_number": build_number,
                    "file_name": file_name,
                    "relative_path": rel,
                    "timestamp_ms": ts_ms,
                    "url": b.get("url"),
                    "raw": a,
                }
                # The 'upsert' operation is used to insert or update data in a table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into, in this case, "hello".
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="jenkins_artifacts", data=row)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)
    log.info("Jenkins sync complete.")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
