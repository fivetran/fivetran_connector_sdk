# **SSH Tunnel with Key-Based Authentication**

**Connector Overview**

This example demonstrates how to connect to an SSH server using key-based authentication with the Fivetran Connector SDK. The connector securely establishes an SSH session to a remote EC2 instance running the fivetran-api-playground server and facilitates data interaction over the SSH channel. This setup avoids the use of plaintext passwords and leverages public-private key cryptography for authentication.

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## **Features**

- Connects to a remote server over SSH using Python's paramiko library.
- Uses public/private key authentication for secure, passwordless access.
- Executes commands or interacts with services (e.g., fivetran-api-playground) over SSH.
- Handles connection errors and logs detailed diagnostics for troubleshooting.

## **Configuration File**

The connector requires the following configuration parameters: 

```
{
  "hostname": "<YOUR_CLICKHOUSE_HOSTNAME>",
  "username": "<YOUR_CLICKHOUSE_USERNAME>",
  "password": "<YOUR_CLICKHOUSE_PASSWORD>",
  "database": "<YOUR_CLICKHOUSE_DATABASE>"
}
```

- hostname: Your ClickHouse server hostname
- username: Username for authentication
- password: Password for authentication
- database: The ClickHouse database to connect to

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

This connector uses the paramiko library to establish SSH connections:
```
paramiko==3.4.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## **Authentication**

This connector uses SSH key-based authentication:

1. The connector loads a private key (e.g., .pem file) specified in the config.
2. It connects to the EC2 instance using paramiko.SSHClient and verifies the host using either strict or relaxed host key policies.
3. Make sure the EC2 instance's security group allows inbound traffic on port 22 from the connector's environment.

## **Data Handling**

The connector processes data from the SSH session as follows:
- Establishes an SSH connection to the EC2 instance
- Executes a configured command (e.g., an API call or script)
- Captures and decodes the standard output
- Parses the response and forwards it to the destination system using the Connector SDK’s upsert method

## **Error Handling**

The connector includes error handling for:

- SSH authentication and connectivity issues
- Command execution failures (non-zero exit codes or stderr output)
- Parsing errors when handling remote responses

All exceptions are logged with detailed stack traces for easier debugging and monitoring.

## **Additional Considerations**

- Ensure that your EC2 instance has the appropriate IAM role or permissions to access the services it needs.
- Use short-lived or rotated SSH keys where possible to reduce risk.
- If your server uses a jump host or bastion, additional SSH tunneling logic may be needed.
- For production environments, avoid hardcoding sensitive paths or credentials.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.