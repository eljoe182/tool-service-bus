# Azure Service Bus JSON Sender

Send each object in an enveloped JSON file as an independent message to an Azure Service Bus queue. The queue name is the file name without its final `.json` suffix, and the envelope's common properties become Azure application properties on every message.

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

`data/orders.json` targets the `orders` queue. Each `data` object is compactly serialized as one message body; the envelope itself is never sent as a message. Every message receives its own copy of the `properties` mapping as Azure application properties.

## Run

```bash
conda run -n tools-service-bus poetry run service-bus-send
```

The tool creates one Service Bus client for the run and one queue sender context per valid file. It logs per-file progress and finishes with a summary such as:

```text
Service Bus send summary: files=3 succeeded=2 failed=1 messages_sent=47
```

Logs include file names, queue names, exception class names, batch numbers, and confirmed counts. They do not include the connection string, complete payloads, `.env` contents, or raw exception text.

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
