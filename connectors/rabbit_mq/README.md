# RabbitMQ Connector Example

## Connector overview

This connector demonstrates how to use the **Fivetran Connector SDK** to extract, process, and sync messages from **RabbitMQ queues** into a Fivetran destination.

It connects to one or more RabbitMQ queues, consumes messages, acknowledges delivery, and upserts processed data into your Fivetran destination tables.

The connector supports:
- Multiple queues with independent table mapping.
- Message acknowledgment (manual or automatic).
- Incremental checkpointing for stateful message tracking.
- Optional queue auto-creation and message TTL filtering.

This example uses the `pika` Python library to establish connections and consume messages from RabbitMQ.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- RabbitMQ Server (v3.8 or later)
- Python environment with access to RabbitMQ (AMQP 0.9.1 protocol support)

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to configure your environment.

---

## Features

- Connects to **RabbitMQ** using the `pika` library.
- Consumes messages from **multiple queues** defined in the configuration file.
- Supports **manual and automatic acknowledgments**.
- Implements **message checkpointing** for resumable syncs.
- Optionally **auto-creates missing queues**.
- Handles **message TTL (time-to-live)** to skip stale messages.
- Processes messages in **batches** to optimize performance.
- Converts message headers and bodies into JSON for storage.
- Tracks processed messages with unique message IDs to prevent duplication.

---

## Configuration file

The connector requires connection and authentication parameters to access RabbitMQ.  
Example configuration file:

```json
{
  "host": "<YOUR_RABBITMQ_HOST>",
  "port": "<YOUR_RABBITMQ_PORT>",
  "virtual_host": "<YOUR_RABBITMQ_VIRTUAL_HOST>",
  "username": "<YOUR_RABBITMQ_USERNAME>",
  "password": "<YOUR_RABBITMQ_PASSWORD>",
  "use_ssl": "<YOUR_USE_SSL_OPTION>",
  "queues": "<YOUR_RABBITMQ_QUEUES_COMMA_SEPARATED>",
  "auto_ack": "<YOUR_AUTO_ACK_OPTION>",
  "message_ttl_hours": "<YOUR_MESSAGE_TTL_IN_HOURS>"
}
```

### Configuration parameters

| Key | Required | Description |
|-----|-----------|-------------|
| `host` | Yes | Hostname or IP of the RabbitMQ server. |
| `port` | No | Port number for RabbitMQ (default: 5672). |
| `virtual_host` | No | Virtual host name (default: `/`). |
| `username` | Yes | RabbitMQ username. |
| `password` | Yes | RabbitMQ password. |
| `queues` | Yes | Comma-separated list of queues to consume. |
| `auto_ack` | No | Enables automatic message acknowledgment (default: `false`). |
| `use_ssl` | No | Enables SSL/TLS connection (default: `false`). |
| `message_ttl_hours` | No | Discards messages older than specified TTL (default: 0 = no TTL). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements file

The connector requires the following Python dependencies:

```
pika
```

Note: The `fivetran_connector_sdk` package is pre-installed in the Fivetran environment. Include dependencies only for local testing.

---

## Authentication

The connector uses **basic authentication** for RabbitMQ via the `pika` client:

```python
credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(
    host=host,
    port=port,
    virtual_host=virtual_host,
    credentials=credentials
)
connection = pika.BlockingConnection(parameters)
```

If `use_ssl` is enabled, an SSL context is created for secure connections using `pika.SSLOptions`.

---

## Pagination

RabbitMQ does not support traditional API pagination.  
Instead, this connector processes messages in **batches** using the following pattern:

1. The consumer fetches up to a configurable `batch_size` (default: 1000 messages).
2. Each message is acknowledged after successful processing.
3. After processing each batch, a checkpoint is created with the current state.

This ensures that messages are processed efficiently while maintaining progress tracking.

---

## Data handling

The connector processes RabbitMQ messages using the following logic:

1. **Schema Definition**
    - Each queue corresponds to a table in the destination.
    - The schema includes metadata such as message headers, delivery tags, and timestamps.

2. **Message Parsing**
    - Messages are parsed using the `parse_message()` function, which extracts and normalizes:
        - Headers
        - Properties (content type, encoding, etc.)
        - Message body (decoded and JSON-parsed if applicable)

3. **Upsert Operations**
    - Each message is inserted or updated in the destination via:
      ```python
      op.upsert(table=queue_name, data=message_data)
      ```

4. **Deduplication**
    - Messages are uniquely identified using a `message_id`, generated either from the message’s own ID or a hash of its content.

5. **Checkpointing**
    - State is periodically saved using `op.checkpoint(state=state)` after each queue is processed.
    - The state includes:
        - The list of processed message IDs.
        - The timestamp of the last sync.
        - The total number of messages processed.

---

## Error handling

The connector includes robust error handling at multiple levels:

- **Configuration validation**  
  Missing or invalid configuration parameters raise `ValueError` before the sync starts.

- **Connection failures**  
  The `get_connection()` function handles errors gracefully, logging details for debugging.

- **Queue access errors**  
  Non-existent queues are skipped or automatically created based on the `auto_create_queues` setting.

- **Message processing errors**  
  Malformed or expired messages are skipped and logged. Failed messages are requeued using `basic_nack()` when `auto_ack` is `false`.

- **Resource cleanup**  
  The connection to RabbitMQ is closed safely at the end of each sync.

---

## Tables created

Each RabbitMQ queue maps to a table in the destination.  
For example, given queues `order` and `payment`, the connector creates two tables: `order` and `payment`.

Each table includes the following columns:

| Column name | Type | Description |
|--------------|------|-------------|
| `message_id` | STRING | Unique message identifier (from RabbitMQ or MD5 hash). |
| `delivery_tag` | LONG | Message delivery tag. |
| `exchange` | STRING | RabbitMQ exchange from which the message originated. |
| `routing_key` | STRING | Routing key used for message delivery. |
| `timestamp` | UTC_DATETIME | Message creation or consumption timestamp. |
| `message_body` | JSON | Message content (parsed as JSON or text). |
| `headers` | JSON | Message headers in JSON format. |
| `content_type` | STRING | MIME type of message body. |
| `delivery_mode` | INT | Delivery mode (persistent or transient). |
| `priority` | INT | Message priority level. |
| `_fivetran_synced` | UTC_DATETIME | Timestamp when message was last synced. |
| `_fivetran_deleted` | BOOLEAN | Indicates whether the message was deleted or expired. |

---

## Additional considerations

- For high-throughput environments, increase `batch_size` to improve efficiency.
- Set `message_ttl_hours` to discard outdated messages automatically.
- Use SSL (`use_ssl=true`) for secure message transfer between RabbitMQ and Fivetran.
- The connector supports both durable and transient queues.
- Each queue syncs independently, making this connector scalable and fault-tolerant.

This connector example is intended for **educational purposes** to demonstrate best practices with RabbitMQ and the **Fivetran Connector SDK**.

For further questions, contact **Fivetran Support**.
