# **Connector Overview**

This connector fetches data extracts from the MasterTax API provided by ADP ([https://api.adp.com](https://api.adp.com)). It supports downloading ZIP files containing tab-delimited data extracts, which are parsed and streamed to Fivetran. The connector operates statelessly and dynamically handles certificate-based authentication for secure API access. It is designed for compatibility with Fivetran's Connector SDK and adheres to the standard schema-update model.

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating System:

  * Windows 10 or later
  * macOS 13 (Ventura) or later

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## **Features**

* Retrieves MasterTax data extracts defined in the `constants.py` file
* Handles certificate creation dynamically from config for each run
* Parses tab-delimited text files from downloaded ZIP archives
* Supports streaming upsert operations for Fivetran ingestion
* Configurable schema with primary key definition

## **Configuration File**

The connector reads configuration details from a `configuration.json` file. Required keys include:

```
{
  "clientId": "YOUR_CLIENT_ID",
  "clientSecret": "YOUR_CLIENT_SECRET",
  "crtFile": "<certificate string>",
  "keyFile": "<key string>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

Specify additional libraries needed by your connector in `requirements.txt` (if any). Do **not** include the following which are already pre-installed in the Fivetran environment:

* `fivetran_connector_sdk:latest`
* `requests:latest`

## **Authentication**

Authentication is handled via OAuth2 client credentials. Certificates are generated dynamically at runtime from the config and used for mutual TLS (mTLS) authentication with ADP's API. An access token is requested and added to request headers for each session.

## **Pagination**

Pagination is not required. The connector fetches full data extracts and waits for the process to complete before downloading results.

## **Data Handling**

* Each extract maps to a layout defined in the `data_extracts` dictionary from `constants.py`
* The connector reads tab-delimited files and uses column mappings defined in `column_names`
* Upserts are performed row-by-row into tables based on the layout name

## **Error Handling**

* Retries on HTTP 400 errors with a wait time of 5 minutes (configurable via `RETRY_WAIT_SECONDS` and `MAX_RETRIES`)
* Exceptions are caught and logged with full stack trace to aid debugging
* Timeouts are raised if a process does not complete within 100 polling attempts

## **Additional Files**

* **constants.py** – Holds layout-to-column mappings (`column_names`) and extract configurations (`data_extracts`). Required for extract validation and data mapping.

## **Additional Considerations**

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
