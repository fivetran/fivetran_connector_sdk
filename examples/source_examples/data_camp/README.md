# DataCamp LMS Catalog API Connector Example

## Connector overview

The DataCamp connector for Fivetran fetches course catalog data from the DataCamp LMS Catalog API and syncs it to your data warehouse. This connector supports multiple content endpoints including courses, projects, tracks, practices, assessments, and custom tracks. The connector implements Bearer token authentication and provides comprehensive data flattening with breakout tables for nested relationships, following Fivetran best practices for reliability, security, and maintainability.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

The connector supports the following features:

- Multiple content types: Supports courses, projects, tracks, practices, assessments, and custom tracks
- Bearer token authentication: Secure authentication using DataCamp API access tokens
- Data flattening: Comprehensive flattening of nested objects and arrays into relational structures
- Breakout tables: Separate tables for nested relationships (chapters, topics, content)
- State management: Maintains sync state with checkpointing for reliable syncs
- Error handling: Individual record error handling without stopping entire sync
- Comprehensive logging: Detailed logging for troubleshooting and monitoring
- SDK v2.0.0+ compliance: Uses direct operation calls (no yield statements)

## Configuration file

The connector requires Bearer token authentication for the DataCamp LMS Catalog API. The base URL is configured as a constant in the connector code and defaults to the standard DataCamp API endpoint.

```json
{
  "bearer_token": "YOUR_ACCESS_TOKEN"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector includes a minimal `requirements.txt` file. The core dependencies are pre-installed in the Fivetran environment.

```
# Core dependencies are pre-installed in Fivetran environment
```

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses Bearer token authentication with the DataCamp LMS Catalog API. The authentication is handled in the `update` function through the following process:

- Using a pre-configured Bearer token for all API requests
- Adding the token to the Authorization header as `Bearer {token}`
- Validating token presence during connector initialization
- Handling authentication errors gracefully with detailed logging

To obtain the necessary credentials:
1. Contact your DataCamp account representative to set up API access
2. Request Bearer token credentials for the LMS Catalog API
3. Ensure your token has access to all required catalog endpoints

## Pagination

The DataCamp LMS Catalog API does not support traditional pagination. The connector processes each endpoint as a complete dataset:

* **Full sync approach**: Each endpoint returns the complete current catalog
* **Endpoint-based processing**: Processes one endpoint type at a time
* **State management**: Uses checkpointing to track progress through endpoints
* **Resume capability**: Can resume from the last successfully processed endpoint

Refer to the `update` function for the sequential endpoint processing logic.

## Data handling

The connector processes JSON data from the DataCamp API and transforms it into structured records using specialized functions:

* **Custom flattening**: Uses specialized flatten functions for each content type (`flatten_course`, `flatten_project`, `flatten_track`, `flatten_practice`, `flatten_assessment`, `flatten_custom_track`)
* **Nested object handling**: Flattens nested dictionaries while preserving important relationships
* **Array breakout**: Creates separate tables for nested arrays (chapters, topics, content)
* **Data preservation**: Maintains all original field names and values where possible
* **Primary key generation**: Uses natural keys from the API (id fields)

Data is delivered to Fivetran using upsert operations for each record, ensuring data consistency and enabling incremental updates.

## Error handling

The connector implements comprehensive error handling strategies following Fivetran best practices:

* **Individual record handling**: Failed records don't stop the entire sync process
* **API error management**: HTTP errors are caught and logged with severity levels
* **Timeout configuration**: Configurable timeout (30 seconds default) for API requests to prevent hanging
* **Detailed logging**: Uses Fivetran's logging framework (INFO, WARNING, SEVERE levels)
* **State preservation**: Checkpoints progress after each endpoint to enable recovery

Refer to the `fetch_endpoint` function for API error handling and individual record processing loops for error isolation.

## Tables created

The connector creates the following tables in your destination:

| Table name              | Primary key                    | Description |
|-------------------------|--------------------------------|-------------|
| `COURSES`               | `[id]`                         | Course catalog with flattened course data |
| `COURSES_CHAPTERS`      | `[id, course_id]`              | Breakout table for course chapters |
| `PROJECTS`              | `[id]`                         | Project catalog with flattened project data |
| `PROJECTS_TOPICS`       | `[project_id, name]`           | Breakout table for project topics |
| `TRACKS`                | `[id]`                         | Learning track catalog |
| `TRACKS_CONTENT`        | `[track_id, position]`         | Breakout table for track content items |
| `PRACTICES`             | `[id]`                         | Practice exercise catalog |
| `ASSESSMENTS`           | `[id]`                         | Assessment catalog |
| `CUSTOM_TRACKS`         | `[id]`                         | Custom learning track catalog |
| `CUSTOM_TRACKS_CONTENT` | `[custom_track_id, position]`  | Breakout table for custom track content |

All main tables include flattened versions of complex nested objects, while breakout tables maintain relationships through foreign keys.

## Additional considerations

This connector has been updated to follow Fivetran Python coding standards including:

- **Private constants**: Uses double underscore prefix (`__BASE_URL_DEFAULT`, `__REQUEST_TIMEOUT_SECONDS`) for private constants
- **Simplified configuration**: Removes configurable base URL in favor of code-based constant for security
- **Consistent naming**: Follows PEP 8 naming conventions throughout
- **Error handling**: Comprehensive error handling with proper logging
- **State management**: Consistent checkpoint state structure across all endpoints

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
