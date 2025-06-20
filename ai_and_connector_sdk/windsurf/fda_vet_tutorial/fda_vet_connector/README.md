# FDA Veterinary API Connector

This is a Fivetran Connector SDK implementation for the FDA Veterinary Adverse Event Reporting System (VAERS) API.

## Features

- Fetches adverse event data from the FDA Veterinary API
- Dynamically flattens nested JSON structures
- Handles pagination and rate limiting
- Implements checkpointing for incremental syncs
- Follows Fivetran Connector SDK best practices

## Prerequisites

- Python 3.9+
- Fivetran Connector SDK
- requests library

## Installation

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure the connector by editing the `configuration.json` file:
   ```json
   {
       "base_url": "https://api.fda.gov/animalandveterinary/event.json",
       "limit": "10",
       "api_key": "your_api_key_here"
   }
   ```

## Usage

To test the connector locally:

```bash
fivetran debug --configuration configuration.json
```

## Configuration

| Parameter  | Type   | Required | Description                                      |
|------------|--------|----------|--------------------------------------------------|
| base_url   | string | Yes      | Base URL for the FDA Veterinary API              |
| limit      | number | No       | Maximum number of records to fetch (default: 10) |
| api_key    | string | No       | Optional API key for higher rate limits          |

## Data Model

The connector creates a single table called `events` with all fields flattened from the API response. The primary key is set to `unique_aer_id_number`.

## Rate Limiting

Without an API key, the FDA API has the following limits:
- 240 requests per minute per IP address
- 1,000 requests per day per IP address

With an API key, the limits increase to:
- 240 requests per minute per key
- 120,000 requests per day per key
