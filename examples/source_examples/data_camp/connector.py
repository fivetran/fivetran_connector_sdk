"""DataCamp LMS Catalog API Connector for Fivetran.

This connector fetches course catalog data from DataCamp's LMS Catalog API including courses,
projects, assessments, practices, tracks, and custom tracks. It flattens nested objects and
creates breakout tables for array relationships following Fivetran best practices.
"""

# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Required imports
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
from typing import Dict, Any, List

# Constants for endpoints and table mapping
BASE_URL_DEFAULT = "https://lms-catalog-api.datacamp.com"
REQUEST_TIMEOUT_SECONDS = 30  # Timeout for API requests
ENDPOINTS = {
    "custom_tracks": {
        "url": "/v1/catalog/live-custom-tracks",
        "table": "custom_tracks",
        "pk": ["id"],
    },
    "custom_tracks_content": {
        "table": "custom_tracks_content",
        "pk": ["custom_track_id", "position"],
    },
    "courses": {"url": "/v1/catalog/live-courses", "table": "courses", "pk": ["id"]},
    "courses_chapters": {"table": "courses_chapters", "pk": ["id", "course_id"]},
    "projects": {"url": "/v1/catalog/live-projects", "table": "projects", "pk": ["id"]},
    "projects_topics": {"table": "projects_topics", "pk": ["project_id", "name"]},
    "assessments": {"url": "/v1/catalog/live-assessments", "table": "assessments", "pk": ["id"]},
    "practices": {"url": "/v1/catalog/live-practices", "table": "practices", "pk": ["id"]},
    "tracks": {"url": "/v1/catalog/live-tracks", "table": "tracks", "pk": ["id"]},
    "tracks_content": {"table": "tracks_content", "pk": ["track_id", "position"]},
}


# Custom flatten for custom tracks
def flatten_custom_track(track: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten custom track data by extracting nested topic and imageUrl objects.

    Args:
        track (Dict[str, Any]): Raw custom track data from API

    Returns:
        Dict[str, Any]: Flattened track data with nested objects as separate columns
    """
    flat = {}
    for key, value in track.items():
        if key == "topic" and isinstance(value, dict):
            flat["topic_name"] = value.get("name")
            flat["topic_description"] = value.get("description")
        elif key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        elif key == "content":
            # content handled separately
            continue
        else:
            flat[key] = value
    return flat


def flatten_track(track: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten track data by extracting nested topic and imageUrl objects.

    Args:
        track (Dict[str, Any]): Raw track data from API

    Returns:
        Dict[str, Any]: Flattened track data with nested objects as separate columns
    """
    flat = {}
    for key, value in track.items():
        if key == "topic" and isinstance(value, dict):
            flat["topic_name"] = value.get("name")
            flat["topic_description"] = value.get("description")
        elif key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        elif key == "includedInLicenses" and isinstance(value, list):
            flat["includedInLicenses"] = ", ".join(str(item) for item in value)
        elif key == "content":
            # content handled separately
            continue
        else:
            flat[key] = value
    return flat


def flatten_practice(practice: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten practice data by extracting nested imageUrl objects.

    Args:
        practice (Dict[str, Any]): Raw practice data from API

    Returns:
        Dict[str, Any]: Flattened practice data with nested objects as separate columns
    """
    flat = {}
    for key, value in practice.items():
        if key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        else:
            flat[key] = value
    return flat


# Custom flatten for assessments
def flatten_assessment(assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten assessment data by extracting nested imageUrl objects.

    Args:
        assessment (Dict[str, Any]): Raw assessment data from API

    Returns:
        Dict[str, Any]: Flattened assessment data with nested objects as separate columns
    """
    flat = {}
    for key, value in assessment.items():
        if key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        else:
            flat[key] = value
    return flat


# Custom flatten for projects
def flatten_project(project: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten project data by extracting nested imageUrl objects and instructors list.

    Args:
        project (Dict[str, Any]): Raw project data from API

    Returns:
        Dict[str, Any]: Flattened project data with nested objects as separate columns
    """
    flat = {}
    for key, value in project.items():
        if key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        elif key == "instructors" and isinstance(value, list):
            flat["instructors"] = ", ".join(
                instructor.get("fullName", "")
                for instructor in value
                if isinstance(instructor, dict)
            )
        elif key == "topics":
            # topics handled separately
            continue
        else:
            flat[key] = value
    return flat


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration (dict): Configuration dictionary to validate

    Raises:
        ValueError: If any required configuration value is missing or empty
    """
    required_configs = ["base_url", "bearer_token"]
    for key in required_configs:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema for each endpoint as a table with only primary key(s) defined.

    Args:
        configuration (dict): Configuration dictionary containing connection details

    Returns:
        List[Dict]: List of table schemas with primary keys defined

    Raises:
        ValueError: If configuration validation fails
    """
    validate_configuration(configuration)
    # Only include tables, not endpoints without a table key
    return [
        {"table": v["table"], "primary_key": v["pk"]} for k, v in ENDPOINTS.items() if "table" in v
    ]


# Custom flatten for courses
def flatten_course(course: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten course data by extracting nested topic and imageUrl objects, and instructors list.

    Args:
        course (Dict[str, Any]): Raw course data from API

    Returns:
        Dict[str, Any]: Flattened course data with nested objects as separate columns
    """
    flat = {}
    for key, value in course.items():
        if key == "topic" and isinstance(value, dict):
            flat["topic_name"] = value.get("name")
            flat["topic_description"] = value.get("description")
        elif key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        elif key == "includedInLicenses" and isinstance(value, list):
            flat["includedInLicenses"] = ", ".join(str(item) for item in value)
        elif key == "instructors" and isinstance(value, list):
            flat["instructors"] = ", ".join(
                instructor.get("fullName", "")
                for instructor in value
                if isinstance(instructor, dict)
            )
        elif key == "chapters":
            # chapters handled separately
            continue
        else:
            flat[key] = value
    return flat


# Generic flatten for other endpoints
def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Generic function to flatten nested dictionaries and convert lists to JSON strings.

    Args:
        data (Dict[str, Any]): Dictionary to flatten
        parent_key (str): Parent key prefix for nested keys
        sep (str): Separator for nested key names

    Returns:
        Dict[str, Any]: Flattened dictionary with nested objects as separate columns
    """
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            if value and all(isinstance(item, dict) for item in value):
                items.append((new_key, json.dumps([flatten_dict(item) for item in value])))
            else:
                items.append((new_key, json.dumps(value)))
        else:
            items.append((new_key, value))
    return dict(items)


def fetch_endpoint(base_url: str, endpoint: str, bearer_token: str) -> List[Dict[str, Any]]:
    """
    Fetch data from a DataCamp API endpoint with proper error handling.

    Args:
        base_url (str): Base URL for the API
        endpoint (str): Specific endpoint path to fetch
        bearer_token (str): Authentication token for API access

    Returns:
        List[Dict[str, Any]]: List of records from the API endpoint

    Raises:
        Exception: Logs severe errors but returns empty list on failure
    """
    url = base_url.rstrip("/") + endpoint
    headers = {"Accept": "application/json", "Authorization": f"Bearer {bearer_token}"}
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        # If the response is a dict with a top-level list, extract it
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"]
        elif isinstance(data, dict) and any(isinstance(value, list) for value in data.values()):
            for value in data.values():
                if isinstance(value, list):
                    return value
        elif isinstance(data, list):
            return data
        else:
            return [data]
    except Exception as e:
        log.severe(f"Failed to fetch {endpoint}: {e}")
        return []


def update(configuration: dict, state: dict):
    """
    Sync data from DataCamp LMS Catalog API endpoints.

    Args:
        configuration (dict): Configuration containing base_url and bearer_token
        state (dict): Current state for incremental syncing
    """
    # Extract configuration values
    base_url = configuration.get("base_url", BASE_URL_DEFAULT)
    bearer_token = configuration["bearer_token"]

    # Handle custom tracks endpoint with custom flattening and content breakout
    log.info("Fetching endpoint: /v1/catalog/live-custom-tracks")
    custom_tracks = fetch_endpoint(base_url, "/v1/catalog/live-custom-tracks", bearer_token)
    custom_tracks_upserted = 0
    custom_tracks_content_upserted = 0
    for track in custom_tracks:
        flat = flatten_custom_track(track)
        try:
            op.upsert("custom_tracks", flat)
            custom_tracks_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in custom_tracks: {e}")
        # Handle content breakout
        content = track.get("content", [])
        for content_row in content:
            content_row_out = dict(content_row)
            content_row_out["custom_track_id"] = track.get("id")
            try:
                op.upsert("custom_tracks_content", content_row_out)
                custom_tracks_content_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert content for custom_track {track.get('id')}: {e}")
    log.info(f"Upserted {custom_tracks_upserted} records into custom_tracks")
    log.info(f"Upserted {custom_tracks_content_upserted} records into custom_tracks_content")

    # Checkpoint progress after custom tracks sync
    op.checkpoint(state={"last_synced_endpoint": "custom_tracks"})

    # Handle courses endpoint with custom flattening and chapters breakout
    log.info("Fetching endpoint: /v1/catalog/live-courses")
    courses = fetch_endpoint(base_url, "/v1/catalog/live-courses", bearer_token)
    courses_upserted = 0
    courses_chapters_upserted = 0
    for course in courses:
        flat = flatten_course(course)
        try:
            op.upsert("courses", flat)
            courses_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in courses: {e}")
        # Handle chapters breakout
        chapters = course.get("chapters", [])
        for chapter in chapters:
            chapter_row = dict(chapter)  # shallow copy
            chapter_row["course_id"] = course.get("id")
            try:
                op.upsert("courses_chapters", chapter_row)
                courses_chapters_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert chapter for course {course.get('id')}: {e}")
    log.info(f"Upserted {courses_upserted} records into courses")
    log.info(f"Upserted {courses_chapters_upserted} records into courses_chapters")

    # Checkpoint after courses
    log.info(f"Upserted {courses_upserted} courses, {courses_chapters_upserted} chapters")
    op.checkpoint({"last_synced": "courses"})

    # Handle projects endpoint with custom flattening and topics breakout
    log.info("Fetching endpoint: /v1/catalog/live-projects")
    projects = fetch_endpoint(base_url, "/v1/catalog/live-projects", bearer_token)
    projects_upserted = 0
    projects_topics_upserted = 0
    for project in projects:
        flat = flatten_project(project)
        try:
            op.upsert("projects", flat)
            projects_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in projects: {e}")
        # Handle topics breakout
        topics = project.get("topics", [])
        for topic in topics:
            topic_row = dict(topic)  # shallow copy
            topic_row["project_id"] = project.get("id")
            try:
                op.upsert("projects_topics", topic_row)
                projects_topics_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert topic for project {project.get('id')}: {e}")
    log.info(f"Upserted {projects_upserted} records into projects")
    log.info(f"Upserted {projects_topics_upserted} records into projects_topics")

    # Checkpoint progress after projects sync
    op.checkpoint(state={"last_synced_endpoint": "projects"})

    # Handle tracks endpoint with custom flattening and content breakout
    log.info("Fetching endpoint: /v1/catalog/live-tracks")
    tracks = fetch_endpoint(base_url, "/v1/catalog/live-tracks", bearer_token)
    tracks_upserted = 0
    tracks_content_upserted = 0
    for track in tracks:
        flat = flatten_track(track)
        try:
            op.upsert("tracks", flat)
            tracks_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in tracks: {e}")
        # Handle content breakout
        content = track.get("content", [])
        for content_row in content:
            content_row_out = dict(content_row)
            content_row_out["track_id"] = track.get("id")
            try:
                op.upsert("tracks_content", content_row_out)
                tracks_content_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert content for track {track.get('id')}: {e}")
    log.info(f"Upserted {tracks_upserted} records into tracks")
    log.info(f"Upserted {tracks_content_upserted} records into tracks_content")

    # Checkpoint progress after tracks sync
    op.checkpoint(state={"last_synced_endpoint": "tracks"})

    # Handle practices endpoint with custom flattening
    log.info("Fetching endpoint: /v1/catalog/live-practices")
    practices = fetch_endpoint(base_url, "/v1/catalog/live-practices", bearer_token)
    practices_upserted = 0
    for practice in practices:
        flat = flatten_practice(practice)
        try:
            op.upsert("practices", flat)
            practices_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in practices: {e}")
    log.info(f"Upserted {practices_upserted} records into practices")

    # Checkpoint progress after practices sync
    op.checkpoint(state={"last_synced_endpoint": "practices"})

    # Handle assessments endpoint with custom flattening
    log.info("Fetching endpoint: /v1/catalog/live-assessments")
    assessments = fetch_endpoint(base_url, "/v1/catalog/live-assessments", bearer_token)
    assessments_upserted = 0
    for assessment in assessments:
        flat = flatten_assessment(assessment)
        try:
            op.upsert("assessments", flat)
            assessments_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in assessments: {e}")
    log.info(f"Upserted {assessments_upserted} records into assessments")

    # Final checkpoint after all endpoints synced
    op.checkpoint(state={"last_synced_endpoint": "assessments", "sync_completed": True})


# Standard connector initialization
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)

# Aug 23, 2025 03:37:27 PM: INFO Fivetran-Tester-Process: SYNC PROGRESS:
# Operation       | Calls
# ----------------+------------
# Upserts         | 20
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 10
# Checkpoints     | 6
#
# Aug 23, 2025 03:37:27 PM: INFO Fivetran-Tester-Process: Sync SUCCEEDED
