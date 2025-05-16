# Google Cloud Pub/Sub Connector Example

This connector demonstrates how to integrate Google Cloud Pub/Sub with Fivetran using the Connector SDK. It provides an example of creating topics, publishing messages, managing subscriptions, and consuming messages from a Pub/Sub topic.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* Google Cloud Platform account
* GCP Service Account with Pub/Sub permissions
* GCP Project with Pub/Sub API enabled

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates GCP Pub/Sub integration
* Creates and manages Pub/Sub topics
* Handles subscription creation and management
* Publishes sample messages (for testing)
* Implements message consumption and processing
* Includes state checkpointing for resumable syncs
* Provides comprehensive error handling
* Implements proper resource cleanup

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "service_key": {
    "type": "service_account",
    "project_id": "YOUR_PROJECT_ID",
    "private_key_id": "YOUR_PRIVATE_KEY_ID",
    "private_key": "YOUR_PRIVATE_KEY",
    "client_email": "YOUR_CLIENT_EMAIL",
    "client_id": "YOUR_CLIENT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "YOUR_CERT_URL"
  },
  "topic_name": "YOUR_TOPIC_NAME",
  "subscription_name": "YOUR_SUBSCRIPTION_NAME"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
google-cloud-pubsub
google-auth
google-api-core
protobuf==6.30.1
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses GCP Service Account authentication:
1. Service account credentials provided in configuration
2. Credentials used to initialize Pub/Sub clients
3. Automatic token management and renewal

## Data Handling

The connector syncs the following table:

### Sample Table
| Column    | Type           | Description                    |
|-----------|----------------|--------------------------------|
| key       | STRING         | Primary key                    |
| data      | STRING         | Message content                |
| timestamp | UTC_DATETIME   | Message timestamp              |

The connector implements the following data handling features:
* Message batching for efficient processing
* JSON message parsing
* Timestamp handling
* Automatic schema inference for additional fields

## Error Handling

The connector implements the following error handling:
* Validates GCP credentials and configuration
* Handles topic creation failures
* Manages subscription errors
* Implements proper message acknowledgment
* Includes comprehensive logging
* Provides proper resource cleanup

## Additional Considerations

This example is intended for learning purposes and demonstrates GCP Pub/Sub integration. For production use, you should:

- Implement appropriate IAM roles and permissions
- Adjust message batch sizes based on your needs
- Add proper error retry mechanisms
- Consider implementing dead-letter queues
- Add monitoring for message processing
- Implement proper message ordering if required
- Consider implementing message filtering
- Add proper handling for backlog messages
- Implement proper error handling for GCP service outages
- Consider implementing message deduplication
- Add proper cleanup procedures
- Consider implementing custom message processing logic

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
