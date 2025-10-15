# New York Times API Connector

## Connector Overview
This connector enables data synchronization from the New York Times API into your destination using the Fivetran Connector SDK. It supports fetching articles from multiple endpoints including article archives and most popular articles (emailed, shared, and viewed).

## Features
- Incremental syncs using date-based pagination for archives
- Multiple data streams: article archives and most popular articles
- Support for various NY Times API endpoints
- Configurable time periods for most popular articles
- Comprehensive error handling and logging
- State management for reliable incremental syncs

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- New York Times API Key (register at https://developer.nytimes.com/)
- Operating System:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)

## Configuration
The connector requires the following configuration parameters:

```json
{
    "api_key": "your_nytimes_api_key",
    "start_date": "2023-01",
    "end_date": "2023-12",
    "period": 7,
    "share_type": "facebook"
}
```

### Configuration Parameters
- `api_key` (required): Your NY Times API key
- `start_date` (required): Start date for article retrieval (YYYY-MM format)
- `end_date` (optional): End date for article retrieval (YYYY-MM format)
- `period` (required): Time period for most popular articles (1, 7, or 30 days)
- `share_type` (optional): Type of sharing for most popular shared articles (e.g., "facebook")

## Data Synced

### Article Stream
Primary Key: `_id`
```json
{
    "table": "article",
    "columns": {
        "_id": "STRING",
        "web_url": "STRING",
        "snippet": "STRING",
        "print_page": "STRING",
        "print_section": "STRING",
        "source": "STRING",
        "pub_date": "UTC_DATETIME",
        "document_type": "STRING",
        "news_desk": "STRING",
        "section_name": "STRING",
        "type_of_material": "STRING",
        "word_count": "INTEGER",
        "uri": "STRING"
    }
}
```

### Most Popular Stream
Primary Key: `id`
```json
{
    "table": "most_popular",
    "columns": {
        "id": "INTEGER",
        "url": "STRING",
        "adx_keywords": "STRING",
        "section": "STRING",
        "byline": "STRING",
        "type": "STRING",
        "title": "STRING",
        "abstract": "STRING",
        "published_date": "UTC_DATETIME",
        "source": "STRING",
        "updated": "UTC_DATETIME",
        "uri": "STRING"
    }
}
```

## Authentication
The connector uses API key authentication. You need to provide your NY Times API key in the configuration.

## Rate Limiting
The NY Times API has rate limits that vary by endpoint and subscription level. The connector includes error handling to manage rate limits appropriately.

## Error Handling
- Validates configuration parameters before sync
- Handles API errors with retries and logging
- Checkpoints state after each successful batch
- Gracefully handles rate limiting and network errors

## Development and Testing
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create configuration.json with your API credentials
4. Run the connector locally: `python connector.py`

## Additional Notes
- Keep your API key secure and never commit it to version control
- Monitor API usage to stay within rate limits
- Consider implementing more granular error handling for production use