# IBM Informix Connector Example

This contains examples for connecting to IBM Informix databases and syncing data using the Fivetran Connector SDK. There are two approaches to connect to IBM Informix databases:

1. [Using the native `ibm_db` Python package provided by IBM](#using-the-ibm_db-python-package)
   - This is the recommended approach for most use cases.
   - It provides a direct interface to IBM Informix databases and is optimized for performance.
2. [Using an external JDBC driver via JayDeBeApi](#using-external-driver-with-jaydebeapi)
   - This approach is useful if you need to use specific JDBC features .
   - You can also use it if the native `ibm_db` package is not suitable for your needs.

## Using the `ibm_db` Python Package

### Overview
The `ibm_db` connector provides a direct interface to IBM Informix databases using IBM's official Python driver. This approach is recommended for most use cases as it provides native performance and full compatibility with IBM Informix databases.

### Configuration
The connector requires the following configuration parameters in your `configuration.json` file:

```json
{
    "hostname": "<YOUR_HOSTNAME_FOR_INFORMIX>",
    "port": "<YOUR_PORT_NUMBER>",
    "protocol": "<YOUR_PROTOCOL>",
    "database": "<YOUR_DATABASE_NAME>",
    "user_id": "<YOUR_USER_ID>",
    "password": "<YOUR_PASSWORD>"
}
```

### Customizing the Connector
To modify the connector for your specific requirements:

1. **Schema Definition**: Update the `schema()` function to define the tables and columns.
   
2. **Query Modification**: In the `update()` function, modify the SQL query to select the appropriate data from your source tables.
   
3. **State Management**: Customize how the connector tracks state (e.g., last synced timestamp) for incremental updates, if applicable.

## Using External Driver with `JayDeBeApi`

### Overview
The `JayDeBeApi` approach allows you to connect to IBM Informix using a JDBC driver. This method might be useful in scenarios where the native `ibm_db` package isn't suitable or when specific JDBC features are required.

### Configuration
The external driver connector requires the following configuration in your `configuration.json` file:

```json
{
  "hostname": "<YOUR_HOSTNAME>",
  "port": "<YOUR_PORT_NUMBER>",
  "informix_server": "<YOUR_INFORMIX_SERVER_NAME>",
  "database": "<YOUR_DATABASE_NAME>",
  "username": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "table_name": "<YOUR_TABLE_NAME>"
}
```

### Setting Up the Driver
The connector requires the Informix JDBC driver. An installation script ([`installation.sh`](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/ibm_informix/using_jaydebeapi/drivers/installation.sh)) is provided that:
- Installs required dependencies
- Downloads and sets up the Informix JDBC driver
- Installs the necessary Python packages (JayDeBeApi and JPype)

The IBM Informix JDBC driver is installed from [here](https://dbschema.com/jdbc-driver/informix.html?srsltid=AfmBOor07R-wVy7YwCUWZW170KfIj4ggd7VeR_4F7sr2yZHtbodR33NO).

### Customizing the Connector
To modify the external driver connector:

1. **Schema Definition**: Update the `schema()` function to define your tables and columns.
   
2. **Query Customization**: Modify the SQL query in the `update()` function to fetch your required data. Adjust how data is processed and upserted as needed.
   
3. **State Management**: Customize how the connector tracks state (e.g., last synced timestamp) for incremental updates, if applicable.

> **Note:** Using external drivers is currently in private preview. Please connect with our professional services to get more information about them and enable it for your connector.

## Important Notes

- These connectors have only been tested using locally running IBM Informix servers. 
- If you encounter any issues while setting up or using these connectors, please contact [our professional services](https://support.fivetran.com/hc/en-us/requests/new?isSdkIssue=true) for assistance.
- Ensure that you have the appropriate licenses for IBM Informix and any third-party drivers used.
- The examples provided here are meant to be starting points - you will need to adapt them to your specific data models and business requirements.