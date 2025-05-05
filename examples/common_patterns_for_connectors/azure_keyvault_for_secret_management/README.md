# **Azure Key Vault for Secret Managemen**

**Connector Overview**

This connector demonstrates how to use Azure Key Vault to securely manage database credentials. It retrieves credentials from Azure Key Vault, connects to a database, and stores information about the connection in a table. 

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## **Features**

- Securely retrieves database credentials from Azure Key Vault
- Connects to the PostgreSQL database using retrieved credentials
- Verifies the connection by executing a simple query
- Upserts a record into a table to confirm successful connection

## **Configuration File**

The connector requires the following configuration parameters:

```
{
  "tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "client_id": "<YOUR_APP_CLIENT_ID>",
  "client_secret": "<YOUR_APP_CLIENT_SECRET>",
  "vault_url": "<https://YOUR-VAULT-NAME.vault.azure.net/>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

This connector requires Azure SDK packages to access Key Vault and connect to PostgreSQL:

```
psycopg2-binary
azure-identity
azure-keyvault
azure-storage==0.36.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## **Authentication**

This connector uses service principal authentication to access Azure Key Vault. To create the necessary credentials:  
1. Register an application in Azure Active Directory  
   - Go to Azure Portal → Azure Active Directory → App registrations
   - Create a new registration with a name of your choice
   - Note the Application (client) ID and Directory (tenant) ID
2. Create a client secret  
   - In your app registration, go to Certificates & secrets
   - Create a new client secret and note its value immediately
3. Grant the application access to your Key Vault  
   - For role-based access control: Go to Key Vault → Access control (IAM) → Add role assignment
   - Assign the "Key Vault Secrets User" role to your application
   - For access policy: Go to Key Vault → Access policies → Add access policy
   - Grant "Get" and "List" permissions for secrets to your application


## **Data Handling**

The connector retrieves credentials from Azure Key Vault and uses them to establish a database connection. It then:  

1. Executes a simple query to verify connectivity and retrieve database version.
   2. Stores connection information in the "database_info" table with the following schema:
   - id (INT)
   - host (STRING)
   - database (STRING)
   - connected_at (STRING)
   - db_version (STRING)
   
The connector expects the following secrets to be stored in the Key Vault:  
   - postgresHost
   - postgresPort
   - postgresDatabase
   - postgresUser
   - postgresPassword


## **Additional Considerations**

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
