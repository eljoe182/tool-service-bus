# Azure Service Bus JSON Sender And Reader

Send each object in an enveloped JSON file as an independent message to an Azure Service Bus queue by default, or publish every valid file to one selected topic. In queue mode, the queue name is the file name without its final `.json` suffix. In topic mode, that file stem is an expected subscription label in safe logs only; it is not an Azure destination or application property. The envelope's common properties become Azure application properties on every message in either mode.

## Requirements

- Conda environment `tools-service-bus`
- Python 3.11 or newer in that environment
- Poetry 2
- An existing Azure Service Bus namespace and queues
- A connection string allowed to send to those queues

## Install

From the project directory:

```bash
conda run -n tools-service-bus poetry install
```

The command uses the existing Conda environment and installs the locked runtime and development dependencies through Poetry.

## Configure

Create a local `.env` in the project directory with these variables:

```dotenv
AZURE_SERVICE_BUS_CONNECTION_STRING=
SERVICE_BUS_DATA_DIR=data
LOG_LEVEL=INFO
```

Populate `AZURE_SERVICE_BUS_CONNECTION_STRING` only in the ignored local `.env`; it is required by both commands. `SERVICE_BUS_DATA_DIR` defaults to `data`; relative paths resolve from the current working directory and the directory is validated only by `service-bus-send`. `service-bus-read` does not require `SERVICE_BUS_DATA_DIR`. `LOG_LEVEL` defaults to `INFO` and accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` case-insensitively for both commands.

Process environment variables take precedence over `.env`. Local `.env` variants are ignored by Git, while `.env.example` is retained.

## Input Contract

The configured directory is scanned non-recursively. Only direct regular files with the exact lowercase `.json` suffix are processed, in ascending case-sensitive file-name order. Symbolic links are ignored.

Each file must be UTF-8 JSON whose root is an object containing both `properties` and `data`. Extra root keys are ignored. `properties` must be an object with string keys and primitive values only: string, finite integer or float, boolean, or `null`. Nested property objects and arrays are invalid. `data` must be an array of objects; non-finite numbers are rejected recursively in both properties and data. The complete envelope is decoded and validated before its sender is opened, so an invalid file sends zero messages. Empty properties and empty data are valid; empty data sends zero messages.

```json
{
  "properties": {
    "source": "service-bus-sample",
    "priority": 3,
    "isRetry": false
  },
  "data": [
    {"orderId": "A-1001", "status": "created"},
    {"orderId": "A-1002", "status": "created"}
  ]
}
```

`data/orders.json` targets the `orders` queue when no topic is selected. Each `data` object is compactly serialized as one message body; the envelope itself is never sent as a message. Every message receives its own copy of the `properties` mapping as Azure application properties.

## Run

Send each file to its stem-derived queue, unchanged:

```bash
conda run -n tools-service-bus poetry run service-bus-send
```

Publish every valid file to one topic:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --topic orders-events
```

Send one message at a time with a half-second pause between messages:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --delay 0.5
```

| Argument | Contract |
| --- | --- |
| `--topic` | Optional, non-empty topic name. When omitted, each valid file uses `get_queue_sender(queue_name=file_stem)`. When present, each valid file uses `get_topic_sender(topic_name=topic)`. |
| `--delay` | Optional non-negative seconds. Omitted or `0` keeps dynamic batching with no sleep. Values greater than `0` send one message at a time and sleep `N-1` times for `N` messages in a file. |

In topic mode, `data/dashboard.json` publishes to `orders-events`; `dashboard` appears only as the expected subscription in safe logs. Azure Service Bus publishes to topics, and subscription filters determine delivery. The tool creates one Service Bus client for the run and one sender context per valid file only after the complete envelope validates.

It logs per-file progress and finishes with a summary such as:

```text
Service Bus send summary: files=3 succeeded=2 failed=1 messages_sent=47
```

Logs include file names, queue names or topic/expected-subscription labels, exception class names, batch numbers, and confirmed counts. They do not include the connection string, complete payloads, `.env` contents, raw exception text, or application-property values.

## Queue Reader

`service-bus-read` reads a queue by default or reads a topic through an explicitly selected subscription. Azure Service Bus topics are not directly readable.

Read a queue using the existing invocation:

```bash
conda run -n tools-service-bus poetry run service-bus-read \
  --queue orders \
  --count 10 \
  --mode peek
```

Read a topic subscription:

```bash
conda run -n tools-service-bus poetry run service-bus-read \
  --queue sbt-local-indicators \
  --subscription dashboard \
  --count 10 \
  --mode peek \
  --entity-type topic
```

| Argument | Contract |
| --- | --- |
| `--queue` | Required, non-empty queue name; it is the topic name with `--entity-type topic`. |
| `--count` | Required positive integer. |
| `--mode` | Required: `peek`, `block`, or `drain`. |
| `--entity-type` | `queue` or `topic`; defaults to `queue`. |
| `--subscription` | Required for `topic`, non-empty, and rejected for `queue`. |

Queue reads use `get_queue_receiver(queue_name=...)`. Topic reads use `get_subscription_receiver(topic_name=..., subscription_name=...)`.

| Mode | Azure operation | Settlement |
| --- | --- | --- |
| `peek` | `peek_messages(max_message_count=count)` | None; messages are not locked or removed. |
| `block` | `receive_messages(max_message_count=count, max_wait_time=10)` | None; returned messages remain locked until Azure releases the lock. |
| `drain` | `receive_messages(max_message_count=count, max_wait_time=10)` | Each message is completed only after its body was successfully written. This mode is destructive. |

`--queue` must be non-empty, `--count` must be a positive integer, and `--mode` is exactly `peek`, `block`, or `drain`. `--subscription` is required for `topic` and rejected for `queue`. The command writes one rendered message body per standard-output line and writes `Read N messages` only to standard error. UTF-8 bodies render as text; binary bodies render deterministically as `base64:<payload>`. An empty queue succeeds with no standard-output bodies and `Read 0 messages` on standard error.

If rendering, output, or completion fails in `drain`, the failing message is not deliberately completed. Earlier messages completed before that failure remain removed. Errors exit with code `2` and identify only the operation, entity type/name, subscription when relevant, and exception type; they never print connection strings, raw exception details, or bodies that were not printed.

## Exit Codes

| Code | Meaning |
| --- | --- |
| `0` | Configuration is valid and every discovered file succeeds, including an empty directory. |
| `1` | At least one file fails; later files are still attempted. |
| `2` | Configuration, client creation, or another run-level startup operation fails before a normal aggregate result. |

## Delivery And File Lifecycle

Input files are never moved, renamed, deleted, or edited. After a successful run, manually remove or relocate files that must not be sent again.

The tool does not provide deduplication, retries, checkpoints, resume behavior, or exactly-once delivery. Running it again processes every discovered file again and can duplicate all messages from previously successful files.

A file can fail after one or more earlier batches were accepted by Azure, resulting in a partial send. The summary counts those confirmed earlier messages but still marks the file failed. Re-running that unchanged file can duplicate the earlier messages as well as attempt the remaining messages. Consumers should be idempotent when duplicate handling matters.

## Test

```bash
conda run -n tools-service-bus poetry run pytest -v
```

Tests use temporary directories and fakes for Azure clients, senders, batches, and SDK failures. They require no Azure credentials and make no network calls.

## Local Messaging Emulators (optional)

`docker-compose.yml` is an optional local stack for Azure Service Bus and Event Hubs emulators. Use it when you want to develop or test without a cloud namespace; the CLI works against either the emulator or a real Azure namespace.

### Quick start

1. Copy emulator variables from `.env.example` into your local `.env` (at least `ACCEPT_EULA=Y` and a strong `MSSQL_SA_PASSWORD`).
2. Start the stack:

```bash
docker compose up -d
```

3. Point the tools at the Service Bus emulator with the development connection string (also listed in `.env.example`):

```dotenv
AZURE_SERVICE_BUS_CONNECTION_STRING=Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

4. Stop the stack when finished:

```bash
docker compose down
```

Topology is defined only in JSON config files mounted into the containers. Changing those files requires a restart of the affected emulator (`docker compose restart servicebus-emulator` or `eventhubs-emulator`).

### Services And Ports

| Service | Role | Host ports (defaults) |
| --- | --- | --- |
| `servicebus-emulator` | Azure Service Bus emulator | AMQP `5672`, HTTP `5300` |
| `eventhubs-emulator` | Azure Event Hubs emulator | AMQP `5673`, Kafka `9092`, HTTP `5301` |
| `sql-edge` | Persistence backend for Service Bus | internal only |
| `azurite` | Blob/queue/table storage for Event Hubs | `10000` / `10001` / `10002` |

Override host ports with the variables in `.env.example` (`SERVICEBUS_*`, `EVENTHUBS_*`, `AZURITE_*`). Event Hubs maps container AMQP `5672` to host `5673` so both emulators can run side by side.

Default connection strings:

| Emulator | Connection string |
| --- | --- |
| Service Bus | `Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;` |
| Event Hubs | `Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;` |

### Configure Service Bus

Edit `emulators/servicebus/config.json`. Queues and topics live under the namespace `sbemulatorns`.

Current sample entities:

- Queue: `queue.1`
- Topic: `topic.1` with subscription `default`

Then restart:

```bash
docker compose restart servicebus-emulator
```

### Configure Event Hubs

Edit `emulators/eventhubs/config.json`. Event hubs live under the namespace `emulatorNs1`.

Current sample entity:

- Event hub: `eh1` (2 partitions) with consumer group `cg1`

Then restart:

```bash
docker compose restart eventhubs-emulator
```

### Create A New Queue

Add an object to the `Queues` array in `emulators/servicebus/config.json`:

```json
{
  "Name": "orders",
  "Properties": {
    "DeadLetteringOnMessageExpiration": false,
    "DefaultMessageTimeToLive": "PT1H",
    "DuplicateDetectionHistoryTimeWindow": "PT20S",
    "ForwardDeadLetteredMessagesTo": "",
    "ForwardTo": "",
    "LockDuration": "PT1M",
    "MaxDeliveryCount": 3,
    "RequiresDuplicateDetection": false,
    "RequiresSession": false
  }
}
```

Restart `servicebus-emulator`. With `data/orders.json`, `service-bus-send` targets the `orders` queue when no `--topic` is set.

### Create A New Topic

Add an object to the `Topics` array in `emulators/servicebus/config.json`. Include at least one subscription:

```json
{
  "Name": "orders-events",
  "Properties": {
    "DefaultMessageTimeToLive": "PT1H",
    "DuplicateDetectionHistoryTimeWindow": "PT20S",
    "RequiresDuplicateDetection": false
  },
  "Subscriptions": [
    {
      "Name": "dashboard",
      "Properties": {
        "DeadLetteringOnMessageExpiration": false,
        "DefaultMessageTimeToLive": "PT1H",
        "LockDuration": "PT1M",
        "MaxDeliveryCount": 3,
        "ForwardDeadLetteredMessagesTo": "",
        "ForwardTo": "",
        "RequiresSession": false
      }
    }
  ]
}
```

Restart `servicebus-emulator`. Publish with `--topic orders-events` and read with `--entity-type topic --subscription dashboard`.

### Create A New Event Hub

Add an entity under `Entities` in `emulators/eventhubs/config.json`:

```json
{
  "Name": "eh-orders",
  "PartitionCount": 2,
  "ConsumerGroups": [
    {
      "Name": "cg-default"
    }
  ]
}
```

Restart `eventhubs-emulator`. Use the Event Hubs connection string above with your SDK or Kafka client against host port `9092` when needed.
