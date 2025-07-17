# IBM Informix Connector using ibm_db

**Connector Overview**

This connector uses the native `ibm_db` Python package provided by IBM to connect to IBM Informix databases. This is the recommended approach for most use cases as it provides:
- Direct interface to IBM Informix databases
- Full compatibility with IBM Informix features

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
  
## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## **Features**

- Native connection to IBM Informix databases using the `ibm_db` python package.
- Proper handling of datetime values
- Incremental sync using `state` and `op.checkpoint()`

## **Configuration File**

The connector requires the following configuration parameters:

```
{
  "hostname": "<YOUR_HOSTNAME>",
  "port": "<YOUR_PORT_NUMBER>",
  "database": "<YOUR_DATABASE_NAME>",
  "username": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "table_name": "<YOUR_TABLE_NAME>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

This connector requires the following Python packages:

```
ibm_db
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## **Authentication**

The connector uses username and password authentication to connect to your IBM Informix database. You'll need to provide valid database credentials with appropriate permissions to access the tables you want to sync in the `configuration.json` file.

## **Data Handling**

The connector:  
- Executes SQL queries against your specified table
- Properly formats datetime values for consistent representation
- Tracks the latest created_at timestamp to enable incremental syncs

The schema used in the example is as follows:

```
{
  "table": "sample_table",
  "primary_key": ["id"]
}
```

## **Error Handling**

The connector implements error handling for:  
- Database connection failures with detailed error logging
- Datetime conversion issues with graceful fallback to string representation
- Missing configuration parameters with clear error messages

## **Additional Considerations**

> NOTE : This example was tested using the IBM Informix developer edition local server. If you face any difficulties while writing your connector, please connect with [our professional services](https://support.fivetran.com/hc/en-us/requests/new?isSdkIssue=true).

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
