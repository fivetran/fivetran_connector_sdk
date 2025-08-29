"""This connector fetches course catalog data from DataCamp's LMS Catalog API including courses,projects, assessments, practices, tracks, and custom tracks. It flattens nested objects and creates breakout tables for array relationships following Fivetran best practices.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import requests  # For making HTTP requests to the Common Paper API
import json  # For JSON data handling and serialization
from typing import Dict, Any, List, Optional

# Base URL for the DataCamp LMS Catalog API
__BASE_URL_DEFAULT = "https://lms-catalog-api.datacamp.com"
__REQUEST_TIMEOUT_SECONDS = 30  # Timeout for API requests in seconds

__ENDPOINTS = {
    "custom_tracks": {
        "url": "/v1/catalog/live-custom-tracks",
        "table": "custom_tracks",
        "primary_key": ["id"],
    },
    "custom_tracks_content": {
        "table": "custom_tracks_content",
        "primary_key": ["custom_track_id", "position"],
    },
    "courses": {
        "url": "/v1/catalog/live-courses",
        "table": "courses",
        "primary_key": ["id"],
    },
    "courses_chapters": {
        "table": "courses_chapters",
        "primary_key": ["id", "course_id"],
    },
    "projects": {
        "url": "/v1/catalog/live-projects",
        "table": "projects",
        "primary_key": ["id"],
    },
    "projects_topics": {
        "table": "projects_topics",
        "primary_key": ["project_id", "name"],
    },
    "assessments": {
        "url": "/v1/catalog/live-assessments",
        "table": "assessments",
        "primary_key": ["id"],
    },
    "practices": {
        "url": "/v1/catalog/live-practices",
        "table": "practices",
        "primary_key": ["id"],
    },
    "tracks": {
        "url": "/v1/catalog/live-tracks",
        "table": "tracks",
        "primary_key": ["id"],
    },
    "tracks_content": {
        "table": "tracks_content",
        "primary_key": ["track_id", "position"],
    },
}


def flatten_item(
    item: Dict[str, Any],
    skip_keys: Optional[List[str]] = None,
    flatten_topic: bool = False,
    flatten_licenses: bool = False,
    flatten_instructors: bool = False,
) -> Dict[str, Any]:
    """
    A generic function to flatten nested objects within an item from the DataCamp API.

    Args:
        item: The dictionary object to flatten.
        skip_keys: A list of top-level keys to skip during processing.
        flatten_topic: Whether to flatten the 'topic' object.
        flatten_licenses: Whether to flatten the 'includedInLicenses' list.
        flatten_instructors: Whether to flatten the 'instructors' list.

    Returns:
        A flattened dictionary.
    """
    flat = {}
    if skip_keys is None:
        skip_keys = []

    for key, value in item.items():
        if key in skip_keys:
            continue

        if key == "topic" and isinstance(value, dict) and flatten_topic:
            flat["topic_name"] = value.get("name")
            flat["topic_description"] = value.get("description")
        elif key == "imageUrl" and isinstance(value, dict):
            for img_type, url in value.items():
                flat[f"imageUrl_{img_type}"] = url
        elif key == "includedInLicenses" and isinstance(value, list) and flatten_licenses:
            flat["includedInLicenses"] = ", ".join(str(v) for v in value)
        elif key == "instructors" and isinstance(value, list) and flatten_instructors:
            flat["instructors"] = ", ".join(
                instructor.get("fullName", "")
                for instructor in value
                if isinstance(instructor, dict)
            )
        else:
            flat[key] = value
    return flat


def flatten_custom_track(track: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens a custom track object by processing its nested structures.

    Args:
        track (Dict[str, Any]): The custom track dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with topic and imageUrl processed
        and content removed for separate processing
    """
    return flatten_item(track, skip_keys=["content"], flatten_topic=True)


def flatten_track(track: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens a track object by processing its nested structures.

    Args:
        track (Dict[str, Any]): The track dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with topic, imageUrl, and licenses processed
        and content removed for separate processing
    """
    return flatten_item(track, skip_keys=["content"], flatten_topic=True, flatten_licenses=True)


def flatten_practice(practice: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens a practice item by processing its nested structures.

    Args:
        practice (Dict[str, Any]): The practice dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with imageUrl processed
    """
    return flatten_item(practice)


def flatten_assessment(assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens an assessment item by processing its nested structures.

    Args:
        assessment (Dict[str, Any]): The assessment dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with imageUrl processed
    """
    return flatten_item(assessment)


def flatten_project(project: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens a project item by processing its nested structures.

    Args:
        project (Dict[str, Any]): The project dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with imageUrl and instructors processed
        and topics removed for separate processing
    """
    return flatten_item(project, skip_keys=["topics"], flatten_instructors=True)


def flatten_course(course: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens a course item by processing its nested structures.

    Args:
        course (Dict[str, Any]): The course dictionary to flatten

    Returns:
        Dict[str, Any]: A flattened dictionary with topic, imageUrl, licenses,
        and instructors processed, and chapters removed for separate processing
    """
    return flatten_item(
        course,
        skip_keys=["chapters"],
        flatten_topic=True,
        flatten_licenses=True,
        flatten_instructors=True,
    )


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
        response = requests.get(url, headers=headers, timeout=__REQUEST_TIMEOUT_SECONDS)
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


def process_custom_tracks(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process custom tracks data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches custom tracks from /v1/catalog/live-custom-tracks endpoint
        - Flattens each track and stores in custom_tracks table
        - Creates breakout records in custom_tracks_content table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-custom-tracks")
    custom_tracks = fetch_endpoint(base_url, "/v1/catalog/live-custom-tracks", bearer_token)
    custom_tracks_upserted = 0
    custom_tracks_content_upserted = 0

    for track in custom_tracks:
        flat = flatten_custom_track(track)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="custom_tracks", data=flat)
            custom_tracks_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in custom_tracks: {e}")

        # Handle content breakout
        content = track.get("content", [])
        for content_row in content:
            content_row_out = dict(content_row)
            content_row_out["custom_track_id"] = track.get("id")
            try:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="custom_tracks_content", data=content_row_out)
                custom_tracks_content_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert content for custom_track {track.get('id')}: {e}")

    log.info(f"Upserted {custom_tracks_upserted} records into custom_tracks")
    log.info(f"Upserted {custom_tracks_content_upserted} records into custom_tracks_content")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "custom_tracks"})


def process_courses(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process courses data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches courses from /v1/catalog/live-courses endpoint
        - Flattens each course and stores in courses table
        - Creates breakout records in courses_chapters table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-courses")
    courses = fetch_endpoint(base_url, "/v1/catalog/live-courses", bearer_token)
    courses_upserted = 0
    courses_chapters_upserted = 0

    for course in courses:
        flat = flatten_course(course)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="courses", data=flat)
            courses_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in courses: {e}")

        # Handle chapters breakout
        chapters = course.get("chapters", [])
        for chapter in chapters:
            chapter_row = dict(chapter)
            chapter_row["course_id"] = course.get("id")
            try:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="courses_chapters", data=chapter_row)
                courses_chapters_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert chapter for course {course.get('id')}: {e}")

    log.info(f"Upserted {courses_upserted} records into courses")
    log.info(f"Upserted {courses_chapters_upserted} records into courses_chapters")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "courses"})


def process_projects(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process projects data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches projects from /v1/catalog/live-projects endpoint
        - Flattens each project and stores in projects table
        - Creates breakout records in projects_topics table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-projects")
    projects = fetch_endpoint(base_url, "/v1/catalog/live-projects", bearer_token)
    projects_upserted = 0
    projects_topics_upserted = 0

    for project in projects:
        flat = flatten_project(project)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="projects", data=flat)
            projects_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in projects: {e}")

        # Handle topics breakout
        topics = project.get("topics", [])
        for topic in topics:
            topic_row = dict(topic)
            topic_row["project_id"] = project.get("id")
            try:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="projects_topics", data=topic_row)
                projects_topics_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert topic for project {project.get('id')}: {e}")

    log.info(f"Upserted {projects_upserted} records into projects")
    log.info(f"Upserted {projects_topics_upserted} records into projects_topics")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "projects"})


def process_tracks(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process tracks data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches tracks from /v1/catalog/live-tracks endpoint
        - Flattens each track and stores in tracks table
        - Creates breakout records in tracks_content table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-tracks")
    tracks = fetch_endpoint(base_url, "/v1/catalog/live-tracks", bearer_token)
    tracks_upserted = 0
    tracks_content_upserted = 0

    for track in tracks:
        flat = flatten_track(track)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="tracks", data=flat)
            tracks_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in tracks: {e}")

        # Handle content breakout
        content = track.get("content", [])
        for content_row in content:
            content_row_out = dict(content_row)
            content_row_out["track_id"] = track.get("id")
            try:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="tracks_content", data=content_row_out)
                tracks_content_upserted += 1
            except Exception as e:
                log.severe(f"Failed to upsert content for track {track.get('id')}: {e}")

    log.info(f"Upserted {tracks_upserted} records into tracks")
    log.info(f"Upserted {tracks_content_upserted} records into tracks_content")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "tracks"})


def process_practices(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process practices data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches practices from /v1/catalog/live-practices endpoint
        - Flattens each practice and stores in practices table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-practices")
    practices = fetch_endpoint(base_url, "/v1/catalog/live-practices", bearer_token)
    practices_upserted = 0

    for practice in practices:
        flat = flatten_practice(practice)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="practices", data=flat)
            practices_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in practices: {e}")

    log.info(f"Upserted {practices_upserted} records into practices")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "practices"})


def process_assessments(bearer_token: str, base_url: str = __BASE_URL_DEFAULT) -> None:
    """
    Process assessments data from the DataCamp API and store in database.

    Args:
        bearer_token (str): Authentication token for API access
        base_url (str): Base URL for the API, defaults to __BASE_URL_DEFAULT

    Returns:
        None

    Notes:
        - Fetches assessments from /v1/catalog/live-assessments endpoint
        - Flattens each assessment and stores in assessments table
        - Checkpoints progress after processing
    """
    log.info("Fetching endpoint: /v1/catalog/live-assessments")
    assessments = fetch_endpoint(base_url, "/v1/catalog/live-assessments", bearer_token)
    assessments_upserted = 0

    for assessment in assessments:
        flat = flatten_assessment(assessment)
        try:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="assessments", data=flat)
            assessments_upserted += 1
        except Exception as e:
            log.severe(f"Failed to upsert record in assessments: {e}")

    log.info(f"Upserted {assessments_upserted} records into assessments")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"last_synced_endpoint": "assessments"})


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    # Only include tables, not endpoints without a table key
    return [
        {"table": value["table"], "primary_key": value["primary_key"]}
        for key, value in __ENDPOINTS.items()
        if "table" in value
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    if "bearer_token" not in configuration:
        raise ValueError("Missing required configuration value: 'bearer_token'")


def update(configuration: dict, state: dict):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.warning("Example: Source Examples : DataCamp")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration)

    # Extract configuration values
    bearer_token = configuration["bearer_token"]
    base_url = configuration.get("base_url", __BASE_URL_DEFAULT)

    # Process each endpoint
    process_custom_tracks(bearer_token, base_url)
    process_courses(bearer_token, base_url)
    process_projects(bearer_token, base_url)
    process_tracks(bearer_token, base_url)
    process_practices(bearer_token, base_url)
    process_assessments(bearer_token, base_url)


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
