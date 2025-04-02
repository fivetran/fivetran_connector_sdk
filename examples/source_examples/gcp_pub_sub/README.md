# Google Cloud Pub/Sub

This connector allows you to sync data from Google Cloud Pub/Sub to a destination using the Fivetran Connector SDK.

The connector subscribes to a Pub/Sub subscription and delivers messages to your destination as they arrive. Each message is stored in a table with the message content, ID, and timestamp.

### Configuration

Update `configuration.json` with your GCP details:

```json
{
    "service_key": "<GOOGLE_SERVICE_KEY_JSON_STRING>",
    "max_messages": "<MAX_MESSAGE_COUNT>",
    "subscription_name": "<SUBSCRIPTION_NAME>",
    "topic_name": "<TOPIC_NAME>"
}
```
The configuration parameters are:
- `service_key`: Your GCP service account key JSON string. Ensure that the string is correctly escaped.
- `max_messages`: The maximum number of messages to pull from Pub/Sub in each request.
- `subscription_name`: The name of the Pub/Sub subscription to pull messages from.
- `topic_name`: The name of the Pub/Sub topic to pull messages from.


### Customizing for Your Use Case

To adapt this connector for your needs:

1. **Service Account Credentials**: Replace the placeholders in `configuration.json` with your actual credentials.

2. **Topic and Subscription**: Update `topic_name` and `subscription_name` to match your GCP Pub/Sub resources.

3. **Message Processing**: Modify the message parsing logic in `pull_and_upsert_messages()` method if your message format differs.

4. **Remove Testing Code**: For production, you should remove the `create_sample_topic()`, `publish_test_messages()`, and `clean_up_resources()` functions.

### Syncing Multiple Topics

This example only syncs from one topic. To sync multiple topics:

1. Update the `schema()` method to define multiple tables (one per topic)
2. Modify the `pull_and_upsert_messages()` method to iterate through topic/subscription pairs
3. Process each topic's messages into its respective destination table

Alternatively, you can also create multiple connectors, each syncing from a different topic.

### Important Notes
  
- The connector automatically acknowledges messages after processing, removing them from the subscription queue.

- This connector includes message publishing functionality, but this is **only for testing**. In a real-world scenario, this connector would only need to pull and sync the messages that are already published to the topic/ subscription.

- In an actual connector, you wouldn't create or delete topics/subscriptions within the connector.

The methods which are used to testing purposes are:

- `create_sample_topic()` : This method is used to create a sample topic using the publisher instance. In a real-world scenario, you would already have a topic to pull messages from.

- `publish_test_messages()` : This method is used to publish test messages to the topic. In a real-world scenario, messages would already be published by your application. The connector would only need to pull and sync these messages.

- `clean_up_resources()` : This method is used to delete the topic and subscription created by the connector. In a real-world scenario, you would not delete these resources.