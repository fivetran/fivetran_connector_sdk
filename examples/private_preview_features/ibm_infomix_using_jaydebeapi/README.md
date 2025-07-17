# IBM Informix Connector using JayDeBeApi

**Connector Overview**

This connector uses the `JayDeBeApi` package to connect to IBM Informix databases. This is a recommended approach for users who prefer using JDBC drivers for database connections.

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## **Features**

- JDBC-based connection to IBM Informix databases
- Automatic execution of the installation script to set up required JDBC drivers
- SQL-based data retrieval from specified tables

## **Configuration File**

The connector requires the following configuration parameters:

```
{
  "hostname": "<YOUR_HOSTNAME>",
  "port": "<YOUR_PORT_NUMBER>",
  "database": "<YOUR_DATABASE_NAME>",
  "username": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "informix_server": "<YOUR_INFORMIX_SERVER_NAME>",
  "table_name": "<YOUR_TABLE_NAME>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

This connector requires the following Python packages:

```
jaydebeapi
```

The `JayDeBeApi` package is used to establish a connection to the IBM Informix database using JDBC drivers. The connector also requires the `jpype1` package, which is automatically installed when you install `JayDeBeApi`.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## **Authentication**

The connector uses username and password authentication to connect to your IBM Informix database. You'll need to provide valid database credentials with appropriate permissions to access the tables you want to sync in the `configuration.json` file. 

## **Data Handling**

The connector:  
- Executes SQL queries against your specified table
- Maps SQL result column names to values for each row
- Converts tuples fetched from the database into dictionary format for `op.upsert()`

## **Error Handling**

The connector implements error handling for:  
- Database connection failures with detailed error logging
- Missing configuration parameters with clear validation
- Resource management with proper cursor and connection cleanup in all scenarios

## **Additional Files**

The connector uses `jaydebeapi` to connect to the database and retrieve data. The jaydebeapi requires an external IBM Informix JDBC driver. The driver used in the example can be found [here](https://dbschema.com/jdbc-driver/informix.html?srsltid=AfmBOor07R-wVy7YwCUWZW170KfIj4ggd7VeR_4F7sr2yZHtbodR33NO)

* **drivers/installation.sh** – Installs the required JDK and Informix JDBC driver. The script:
  - Installs an appropriate Java Development Kit (JDK17/JRE17)
  - Downloads the Informix JDBC driver from [here](https://dbschema.com/jdbc-driver/informix.html?srsltid=AfmBOor07R-wVy7YwCUWZW170KfIj4ggd7VeR_4F7sr2yZHtbodR33NO)
  - Extracts and installs the driver to /opt/informix directory

> IMPORTANT: The feature to use external drivers is in private prieview. Please connect with our professional services to get more information about them and enable it for your connector.

## **Additional Considerations**

> NOTE : This example was tested using the IBM Informix developer edition local server. If you face any difficulties while writing your connector, please connect with [our professional services](https://support.fivetran.com/hc/en-us/requests/new?isSdkIssue=true).

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.