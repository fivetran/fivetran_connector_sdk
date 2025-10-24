# Dotdigital Connector Example

## Connector overview

The [Dotdigital](https://developer.dotdigital.com/) connector demonstrates how to use the **Fivetran Connector SDK** to extract marketing and engagement data from the Dotdigital REST APIs and sync it to your destination.  
This connector supports multiple endpoints, including **contacts (v3 API)** and **campaigns (v2 API)**, and illustrates advanced SDK concepts such as pagination, incremental syncs, and rate-limit backoff.

The connector implements **Basic Authentication**, supports **region autodiscovery**, and follows Fivetran best practices for reliability, maintainability, and incremental data extraction.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for environment setup, dependency installation, and local testing with `fivetran debug`.

---

## Features

The connector supports the following capabilities:

- **Multi-endpoint support:** Syncs contacts (v3) and campaigns (v2) data from the Dotdigital API.  
- **Region autodiscovery:** Automatically detects the correct API region endpoint.  
- **Incremental sync:** Supports stateful, incremental extraction using modification timestamps.  
- **Pagination:**  
  - v3 API — *seek pagination* with `marker` tokens.  
  - v2 API — *skip/limit pagination*.  
- **Retry and backoff:** Handles rate limits (HTTP 429) using `X-RateLimit-Reset` headers.  
- **Error handling:** Resilient to transient network and API errors.  
- **Comprehensive logging:** Leverages Fivetran’s logging framework for transparency and troubleshooting.  

---

## Configuration file

The connector requires Dotdigital API credentials and configuration options defined in `configuration.json` (or provided via the Fivetran UI):

```json
{
  "DOTDIGITAL_API_USER": "your_api_user",
  "DOTDIGITAL_API_PASSWORD": "your_api_password",
  "DOTDIGITAL_REGION_ID": "r1",
  "AUTO_DISCOVER_REGION": "true",
  "CONTACTS_PAGE_SIZE": 500,
  "V2_PAGE_SIZE": 1000,
  "CONTACTS_START_TIMESTAMP": "Your_START_TIMESTAMP",
  "CAMPAIGNS_ENABLED": "true",
  "LIST_ID_FILTER": ""
}
