"""DocuSign eSignature API Connector for Fivetran.

This connector extracts data from DocuSign eSignature API to enable
analytics for Sales, Legal and other departments and teams.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
and the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
for details.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Operations as op, Logging as logger

# For API calls to DocuSign
import requests
# For handling date and time operations
from datetime import datetime, timezone
# For type hints
from typing import Any, Dict, List, Optional
# For encoding binary data of documents to base64
import base64
# For reading configuration from a JSON file
import json


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required
    parameters for DocuSign API access.
    This function is called at the start of the update method to ensure that the
    connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for
        the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["access_token", "base_url", "account_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your
    connector delivers.
    See the technical reference documentation for more details on the schema
    function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for
        the connector.
    """
    return [
        {
            "table": "envelopes",
            "primary_key": ["envelope_id"],
        },
        {
            "table": "recipients",
            "primary_key": ["envelope_id", "recipient_id"],
        },
        {
            "table": "enhanced_recipients",
            "primary_key": ["envelope_id", "recipient_id"],
        },
        {
            "table": "audit_events",
            "primary_key": ["envelope_id", "event_id"],
        },
        {
            "table": "envelope_notifications",
            "primary_key": ["envelope_id", "notification_id"],
        },
        {
            "table": "documents",
            "primary_key": ["envelope_id", "document_id"],
        },
        {
            "table": "document_contents",
            "primary_key": ["envelope_id", "document_id"],
        },
        {
            "table": "templates",
            "primary_key": ["template_id"],
        },
        {
            "table": "custom_fields",
            "primary_key": ["envelope_id", "field_name"],
        },
    ]


def get_docusign_headers(configuration: dict) -> Dict[str, str]:
    """
    Generate authentication headers for DocuSign API.
    Uses OAuth2 authentication with access token.
    Args:
        configuration: A dictionary containing the connector configuration.
    Returns:
        A dictionary of HTTP headers for DocuSign API requests.
    """
    return {
        "Authorization": f"Bearer {configuration['access_token']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_base_url(configuration: dict) -> str:
    """
    Construct the base URL for DocuSign API calls.
    Uses the account ID and base URL from configuration.
    Args:
        configuration: A dictionary containing the connector configuration.
    Returns:
        The base URL string for DocuSign API endpoints.
    """
    return f"{configuration['base_url']}/v2.1/accounts/{configuration['account_id']}"


def make_api_request(
    url: str, headers: Dict[str, str], params: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Make a simple authenticated API request to DocuSign.
    Args:
        url: The URL to make the request to.
        headers: A dictionary of HTTP headers.
        params: Optional query parameters.
    Returns:
        The JSON response as a dictionary.
    """
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_document_content(
    configuration: dict, envelope_id: str, document_id: str
) -> Optional[bytes]:
    """
    Fetch the binary content of a specific document.
    Args:
        configuration: A dictionary containing the connector configuration.
        envelope_id: The ID of the envelope.
        document_id: The ID of the document.
    Returns:
        The binary content of the document, or None if failed.
    """
    base_url = get_base_url(configuration)
    # Use a modified header that doesn't demand JSON in response
    headers = {"Authorization": f"Bearer {configuration['access_token']}"}
    url = f"{base_url}/envelopes/{envelope_id}/documents/{document_id}"

    try:
        # Logic from the helper function is now directly here
        response = requests.get(url, headers=headers, timeout=60)
        # Increased timeout for larger files
        response.raise_for_status()
        # Will raise an error for 4xx/5xx responses
        return response.content
    except requests.exceptions.RequestException as exc:
        # This specifically catches HTTP errors, timeouts, etc.
        logger.warning(
            f"Could not fetch content for document {document_id} in envelope {envelope_id}: {exc}"
        )
    return None


def fetch_envelopes(configuration: dict, state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch envelopes data with incremental sync support.
    Uses the last_modified_date from state for incremental updates.
    Args:
        configuration: A dictionary containing the connector configuration.
        state: A dictionary containing state information.
    Returns:
        A list of dictionaries representing envelopes.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)

    params = {
        "from_date": state.get("last_sync_time", "2020-01-01T00:00:00.000Z"),
        "count": 100,
    }

    all_envelopes: List[Dict[str, Any]] = []
    start_position = 0

    while True:
        params["start_position"] = start_position
        url = f"{base_url}/envelopes"

        try:
            data = make_api_request(url, headers, params)
            envelopes = data.get("envelopes", [])

            if not envelopes:
                break

            all_envelopes.extend(envelopes)
            start_position += len(envelopes)

            if len(envelopes) < params["count"]:
                break

        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 401:  # Check specifically for a 401 Unauthorized error
                logger.severe("Received 401 Unauthorized. Access token is likely expired. Aborting fetch.")
                break
            else:
                logger.severe(f"Failed to fetch envelopes with HTTP error: {exc}")
                break

        except Exception as exc:
            logger.severe(f"Failed to fetch envelopes: {exc}")    # If a non-HTTP exception occurs, break the loop to avoid infinite calls
            break

    logger.info(f"Fetched {len(all_envelopes)} envelopes")
    return all_envelopes


def fetch_audit_events(configuration: dict, envelope_id: str) -> List[Dict[str, Any]]:
    """
    Fetch envelope audit events for SLA, deviation, and workflow tracking.
    Args:
        configuration: A dictionary containing the connector configuration.
        envelope_id: The ID of the envelope.
    Returns:
        A list of dictionaries representing audit events.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/audit_events"

    try:
        data = make_api_request(url, headers)
        events = data.get("auditEvents", [])
        for event in events:
            event["envelope_id"] = envelope_id
        return events
    except Exception as exc:
        logger.warning(
            f"Could not fetch audit events for envelope {envelope_id}: {exc}"
        )
        return []


def fetch_envelope_notifications(
    configuration: dict, envelope_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch envelope notifications like reminders and expirations.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/notification"

    try:
        data = make_api_request(url, headers)
        notifications = data.get("notifications", [])
        for n in notifications:
            n["envelope_id"] = envelope_id
        return notifications
    except Exception as exc:
        logger.warning(
            f"Could not fetch notifications for envelope {envelope_id}: {exc}"
        )
        return []


def fetch_enhanced_recipients(
    configuration: dict, envelope_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch recipients with full status history, reminders, declines.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/recipients"

    try:
        data = make_api_request(url, headers)
        recipients: List[Dict[str, Any]] = []
        for recipient_type in [
            "signers",
            "carbon_copies",
            "certified_deliveries",
            "in_person_signers",
        ]:
            type_recipients = data.get(recipient_type, [])
            for r in type_recipients:
                r["recipient_type"] = recipient_type
                r["envelope_id"] = envelope_id
                recipients.append(r)
        return recipients
    except Exception as exc:
        logger.warning(
            f"Could not fetch enhanced recipients for envelope {envelope_id}: {exc}"
        )
        return []


def fetch_recipients_for_envelope(
    configuration: dict, envelope_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch recipients data for a specific envelope.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/recipients"

    try:
        data = make_api_request(url, headers)
        recipients: List[Dict[str, Any]] = []

        for recipient_type in [
            "signers",
            "carbon_copies",
            "certified_deliveries",
            "in_person_signers",
        ]:
            type_recipients = data.get(recipient_type, [])
            for recipient in type_recipients:
                recipient["recipient_type"] = recipient_type
                recipient["envelope_id"] = envelope_id
                recipients.append(recipient)

        return recipients
    except Exception as exc:
        logger.warning(
            f"Could not fetch recipients for envelope {envelope_id}: {exc}"
        )
        return []


def fetch_documents_for_envelope(
    configuration: dict, envelope_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch documents data for a specific envelope.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/documents"

    try:
        data = make_api_request(url, headers)
        documents = data.get("envelopeDocuments", [])

        for document in documents:
            document["envelope_id"] = envelope_id

        return documents
    except Exception as exc:
        logger.warning(
            f"Could not fetch documents for envelope {envelope_id}: {exc}"
        )
    return []


def fetch_templates(configuration: dict, state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch templates data for standard template usage tracking.
    Uses incremental sync by filtering templates modified after last_sync_time.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/templates"
    params = {"count": 100}

    all_templates: List[Dict[str, Any]] = []
    start_position = 0
    last_sync_time = state.get("last_sync_time", "2000-01-01T00:00:00.000Z")

    while True:
        params["start_position"] = start_position

        try:
            data = make_api_request(url, headers, params)
            templates = data.get("envelopeTemplates", [])

            if not templates:
                break

            # Filter templates that have been modified since last sync
            for template in templates:
                last_modified = template.get("lastModified", "")
                if last_modified and last_modified > last_sync_time:
                    all_templates.append(template)

            start_position += len(templates)

            if len(templates) < params["count"]:
                break

        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 401:  # Check specifically for a 401 Unauthorized error
                logger.severe("Received 401 Unauthorized. Access token is likely expired. Aborting fetch.")
                break
            else:
                logger.severe(f"Failed to fetch templates with HTTP error: {exc}")
                break

        except Exception as exc:
            logger.severe(f"Failed to fetch templates: {exc}")    # If a non-HTTP exception occurs, break the loop to avoid infinite calls
            break

    return all_templates


def fetch_custom_fields_for_envelope(
    configuration: dict, envelope_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch custom fields for a specific envelope such as envelope type.
    """
    base_url = get_base_url(configuration)
    headers = get_docusign_headers(configuration)
    url = f"{base_url}/envelopes/{envelope_id}/custom_fields"

    try:
        data = make_api_request(url, headers)
        custom_fields = data.get("textCustomFields", []) + data.get(
            "listCustomFields", []
        )

        for field in custom_fields:
            field["envelope_id"] = envelope_id

        return custom_fields
    except Exception as exc:
        logger.warning(
            f"Could not fetch custom fields for envelope {envelope_id}: {exc}"
        )
    return []


def update(configuration: dict, state: Dict[str, Any]):
    """
    Define the update function, which is a required function, and is called by
    Fivetran during each sync.
    See the technical reference documentation for more details on the update
    function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing DocuSign API connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    logger.warning("DocuSign: eSignature API Connector")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    try:
        if not state:
            state = {"last_sync_time": "2000-01-01T00:00:00.000Z"}

        current_time = datetime.now(timezone.utc).isoformat()

        # --- Process Envelopes and Related Data ---
        logger.info("Fetching envelopes data..." + current_time)
        envelopes = fetch_envelopes(configuration, state)

        for envelope in envelopes:
            envelope_id = envelope.get("envelopeId")
            if not envelope_id:
                logger.warning(
                    "Skipping an envelope record due to missing envelopeId."
                )
                continue

            processed_envelope = {
                "envelope_id": str(envelope_id),
                "status": str(envelope.get("status", "")),
                "sent_timestamp": str(envelope.get("sentDateTime", "")),
                "completed_timestamp": str(
                    envelope.get("completedDateTime", "")
                ),
                "created_timestamp": str(envelope.get("createdDateTime", "")),
                "last_modified_timestamp": str(
                    envelope.get("statusChangedDateTime", "")
                ),
                "subject": str(envelope.get("emailSubject", "")),
                "expire_after": str(envelope.get("expireAfter", "")),
                "contract_cycle_time_hours": "",
                "conversion_status": str(envelope.get("status", "")),
            }

            if envelope.get("status") == "completed":
                sent_time = envelope.get("sentDateTime")
                completed_time = envelope.get("completedDateTime")
                if sent_time and completed_time:
                    try:
                        sent_dt = datetime.fromisoformat(
                            sent_time.replace("Z", "+00:00")
                        )
                        completed_dt = datetime.fromisoformat(
                            completed_time.replace("Z", "+00:00")
                        )
                        cycle_time = (completed_dt - sent_dt).total_seconds() / 3600
                        processed_envelope["contract_cycle_time_hours"] = str(
                            cycle_time
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Could not calculate cycle time for envelope {envelope_id}: {exc}"
                        )

            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            op.upsert("envelopes", processed_envelope)

            # --- Fetch and Process Child Tables ---

            # Recipients
            recipients = fetch_recipients_for_envelope(configuration, envelope_id)
            for r in recipients:
                if r.get("recipientId"):
                    op.upsert(
                        "recipients",
                        {
                            "envelope_id": str(envelope_id),
                            "recipient_id": str(r["recipientId"]),
                            "name": str(r.get("name", "")),
                            "email": str(r.get("email", "")),
                            "status": str(r.get("status", "")),
                            "type": str(r.get("recipient_type", "")),
                            "routing_order": str(r.get("routingOrder", "0")),
                        },
                    )

            enhanced_recipients = fetch_enhanced_recipients(configuration, envelope_id)
            for er in enhanced_recipients:
                if er.get("recipientId"):
                    op.upsert(
                        "enhanced_recipients",
                        {
                            "envelope_id": str(envelope_id),
                            "recipient_id": str(er["recipientId"]),
                            "name": str(er.get("name", "")),
                            "email": str(er.get("email", "")),
                            "status": str(er.get("status", "")),
                            "type": str(er.get("recipient_type", "")),
                            "routing_order": str(er.get("routingOrder", 0)),
                            "declined_reason": str(er.get("declinedReason", "")),
                            "sent_timestamp": str(er.get("sentDateTime", "")),
                            "signed_timestamp": str(er.get("signedDateTime", "")),
                        },
                    )

            audit_events = fetch_audit_events(configuration, envelope_id)

            for event in audit_events:
                # Flatten eventFields into a dict
                flat_event = {
                    field["name"].lower(): str(field.get("value", ""))
                    for field in event.get("eventFields", [])
                }

                flat_event["envelope_id"] = envelope_id
                # Use a combination of envelope_id + logTime as a surrogate primary key
                flat_event["event_id"] = (
                    f"{envelope_id}_{flat_event.get('logtime', '')}"
                )

                op.upsert("audit_events", flat_event)

            # Notifications
            notifications = fetch_envelope_notifications(configuration, envelope_id)

            for n in notifications:
                if n.get("notificationId"):
                    op.upsert(
                        "envelope_notifications",
                        {
                            "envelope_id": str(envelope_id),
                            "notification_id": str(n.get("notificationId")),
                            "notification_type": str(n.get("notificationType", "")),
                            "scheduled_date": str(n.get("scheduledDate", "")),
                            "sent_date": str(n.get("sentDate", "")),
                        },
                    )

            # Documents
            documents = fetch_documents_for_envelope(configuration, envelope_id)
            for d in documents:
                document_id = d.get("documentId")
                if document_id:
                    op.upsert(
                        "documents",
                        {
                            "envelope_id": str(envelope_id),
                            "document_id": str(document_id),
                            "name": str(d.get("name", "")),
                            "type": str(d.get("type", "")),
                            "pages": str(d.get("pages", "0")),
                        },
                    )

                    logger.info(
                        f"Fetching content for document {document_id} in envelope {envelope_id}"
                    )
                    content = fetch_document_content(
                        configuration, envelope_id, document_id
                    )
                    if content:
                        # Content is stored as a Base64 encoded string to handle binary data safely.
                        encoded_content = base64.b64encode(content).decode("utf-8")
                        op.upsert(
                            "document_contents",
                            {
                                "envelope_id": str(envelope_id),
                                "document_id": str(document_id),
                                "content_base64": encoded_content,
                            },
                        )

            # Custom Fields
            custom_fields = fetch_custom_fields_for_envelope(
                configuration, envelope_id
            )
            for f in custom_fields:
                if f.get("name"):
                    op.upsert(
                        "custom_fields",
                        {
                            "envelope_id": str(envelope_id),
                            "field_name": str(f["name"]),
                            "value": str(f.get("value", "")),
                            "type": str(f.get("fieldType", "")),
                        },
                    )

        logger.info(
            f"Processed {len(envelopes)} envelopes and their related data."
        )

        # --- Process Templates ---
        logger.info("Fetching templates data...")
        templates = fetch_templates(configuration, state)
        for t in templates:
            if t.get("templateId"):
                op.upsert(
                    "templates",
                    {
                        "template_id": str(t["templateId"]),
                        "name": str(t.get("name", "")),
                        "description": str(t.get("description", "")),
                        "created_timestamp": str(t.get("created", "")),
                        "last_modified_timestamp": str(t.get("lastModified", "")),
                        "shared": str(t.get("shared", "false")).lower(),
                    },
                )
        logger.info(f"Upserted {len(templates)} templates")

        # Update state with the current sync time for the next run
        new_state = {"last_sync_time": str(current_time)}
        # Save the progress by checkpointing the state. This is important for
        # ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best
        # practices documentation:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(new_state)

    except Exception as exc:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(exc)}")

    logger.info("DocuSign connector update completed successfully")

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
