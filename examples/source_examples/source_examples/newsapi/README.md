# NewsAPI Connector Example

This connector demonstrates how to integrate NewsAPI with Fivetran using the Connector SDK. It provides an example of retrieving news articles based on specified topics, with support for multiple search queries, pagination handling, and incremental syncs based on publication dates.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* NewsAPI account with API access
* NewsAPI API key (supports both Developer and Enterprise plans)
* Access to NewsAPI's `/v2/everything` endpoint

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates NewsAPI integration
* Supports multiple topic searches
* Implements efficient pagination handling
* Processes JSON response data
* Includes state checkpointing for resumable syncs
* Handles large datasets with configurable page sizes
* Provides comprehensive error handling
* Supports incremental syncs using publication dates
* Configurable language filtering
* Customizable result sorting

## Configuration File

The connector requires the following configuration parameters:

```json
{
    "API_KEY": "YOUR_NEWSAPI_API_KEY",
    "pageSize": "100",
    "topic": "[\"Artificial Intelligence\", \"Michigan\", \"Taylor Swift\"]"
}
```

* `API_KEY`: Your NewsAPI API key for authentication
* `pageSize`: Number of articles to retrieve per API call (max 100)
* `topic`: JSON array of topics to search for news articles

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
requests
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses NewsAPI API key authentication:
1. API key provided in configuration
2. Key included in request headers as Bearer token
3. Automatic validation on each request

## Data Handling

The connector syncs the following table:

### article Table
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| source      | STRING       | Primary key (news source name) |
| published_at| UTC_DATETIME | Primary key (article date)     |
| topic       | STRING       | Search topic for the article   |
| author      | STRING       | Article author                 |
| title       | STRING       | Article title                  |
| description | STRING       | Article description            |
| content     | STRING       | Article content                |
| url         | STRING       | Article URL                    |

The connector implements the following data handling features:
* Configurable batch size (up to 100 records per request)
* Multi-topic search support
* Incremental sync using publication dates
* English language filtering
* Sorted results by publication date
* State management for resumable syncs

## Error Handling

The connector implements the following error handling:
* Validates API responses with raise_for_status()
* Includes comprehensive logging
* Handles pagination edge cases
* Validates configuration parameters
* Manages API rate limits
* Implements proper error propagation
* Includes detailed stack traces for debugging

## Additional Considerations

This example is intended for learning purposes and demonstrates NewsAPI integration. For production use, you should:

1. Implement appropriate error retry mechanisms
2. Add rate limiting controls
3. Consider implementing custom topic management
4. Add monitoring for sync performance
5. Implement proper logging strategy
6. Consider implementing custom article filtering
7. Add proper handling for API outages
8. Consider implementing data validation
9. Add proper cleanup procedures
10. Consider implementing custom data transformations
11. Implement proper error notification system
12. Consider upgrading to Enterprise API plan for higher limits

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 