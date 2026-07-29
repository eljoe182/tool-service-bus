# Azure Service Bus JSON Sender Design

## Decision Summary

Build a small Python package that reads JSON files from a local directory and sends each JSON object as an independent message to an Azure Service Bus queue. The queue name comes from the file name, and the command is run as `poetry run service-bus-send` inside the Conda environment `tools-service-bus`.

The implementation uses the Azure Service Bus SDK's dynamic message batches, isolates failures by file, and leaves all input files unchanged. Because successful files are not moved or marked as processed, every rerun resends their messages and can create duplicates.

## Goal

Provide a predictable local tool for sending fixture or operational JSON data to Azure Service Bus queues without requiring custom code for each queue. The package must be easy to test without Azure access and safe enough to avoid leaking credentials or complete payloads through logs.

## Non-Goals

- Receiving, peeking, or deleting Service Bus messages.
- Creating namespaces, queues, or other Azure resources.
- Supporting topics or subscriptions.
- Recursively scanning nested directories.
- Moving, deleting, renaming, or recording the state of processed files.
- Providing exactly-once delivery, deduplication, retries, checkpoints, or resume behavior.
- Adding message metadata, scheduled delivery, sessions, transactions, or custom routing.
- Building an interactive or feature-heavy CLI.

## Architecture

Use a `src`-layout package with four narrow responsibilities. Keep data exchange between modules to standard Python values and small typed result objects; no dependency-injection framework or domain layer is needed.

| Component | Responsibility | Dependencies |
| --- | --- | --- |
| `config.py` | Load `.env`, read environment variables, apply defaults, and validate startup configuration. | `python-dotenv`, standard library |
| `files.py` | Discover files, derive queue names, decode JSON, and validate the complete file before sending. | Standard library |
| `sender.py` | Convert validated objects to messages and send dynamic batches through an SDK sender. | `azure-servicebus` |
| `cli.py` | Orchestrate files, isolate per-file failures, log progress, print the final summary, and return an exit code. | Other package modules, standard library logging |

Azure SDK objects stay behind `sender.py`. File parsing does not know about Azure, and orchestration receives explicit success or failure results. These boundaries allow unit tests to use temporary files and fake senders rather than a live Service Bus namespace.

## Directory Layout

```text
service-bus/
├── .env.example
├── .gitignore
├── README.md
├── pyproject.toml
├── data/
│   └── orders.json
├── src/
│   └── service_bus_sender/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── files.py
│       └── sender.py
└── tests/
    ├── test_cli.py
    ├── test_config.py
    ├── test_files.py
    └── test_sender.py
```

The package entry point will map `service-bus-send` to `service_bus_sender.cli:main`. Runtime dependencies are `azure-servicebus` and `python-dotenv`; `pytest` is a development dependency. Poetry remains the package manager and build tool.

## Configuration Contract

Configuration is loaded once at startup. If `.env` exists in the command's current working directory, `python-dotenv` loads it without overriding variables already present in the process environment.

| Variable | Required | Default | Contract |
| --- | --- | --- | --- |
| `AZURE_SERVICE_BUS_CONNECTION_STRING` | Yes | None | Must be present and non-empty after trimming whitespace. Its value is passed only to `ServiceBusClient.from_connection_string`. |
| `SERVICE_BUS_DATA_DIR` | No | `data` | Directory containing input files. A relative path is resolved from the command's current working directory. The path must exist and be a readable directory. |
| `LOG_LEVEL` | No | `INFO` | Case-insensitive value from `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. Every other value is a configuration error. |

Missing or invalid startup configuration prevents all file processing and returns exit code `2`. The connection string must never appear in validation errors, logs, summaries, or exception representations emitted by the application.

`.env.example` will contain variable names with empty or demonstrably non-secret example values. `.gitignore` will exclude `.env` and other local secret-bearing environment files while retaining `.env.example`.

## Input Contract

The configured data directory is scanned non-recursively. Discovery includes only regular files whose suffix is exactly `.json`, then sorts them by `Path.name` using Python's ascending, case-sensitive lexical ordering. Directory enumeration order must not affect send order.

Each file follows these rules:

- The queue name is the file name without its final `.json` suffix. For example, `data/orders.json` targets queue `orders`.
- The file is UTF-8 encoded JSON.
- The top-level JSON value must be an array.
- Every array element must be a JSON object. Scalars, arrays, and `null` elements are invalid.
- The complete file is decoded and validated before its first message is sent.
- An empty array is valid and succeeds with zero messages sent.

Queue names are not transformed or normalized. Azure Service Bus remains the authority on whether a derived queue name exists and is valid; any resulting SDK error is a failure for that file.

Example input:

```json
[
  {
    "orderId": "A-1001",
    "status": "created"
  },
  {
    "orderId": "A-1002",
    "status": "created"
  }
]
```

This file produces two independent messages. Each message body is the corresponding object serialized with `json.dumps(object, separators=(",", ":"), ensure_ascii=False)`, not the original array and not the whole file. This compact encoding preserves non-ASCII text and the JSON value represented by each object.

## Data Flow

1. The CLI loads and validates configuration.
2. The file module discovers `.json` files in deterministic sorted order.
3. The CLI creates one `ServiceBusClient` with `ServiceBusClient.from_connection_string` for the run.
4. For each file, the file module derives the queue name and validates the complete JSON array.
5. The sender module obtains a queue sender with `client.get_queue_sender(queue_name)`.
6. Each object becomes one `ServiceBusMessage` with a serialized JSON body.
7. The sender module builds and sends one or more SDK-sized batches.
8. The CLI records that file's outcome and continues to the next discovered file regardless of success or failure.
9. The CLI emits a final summary and returns an exit code based on all outcomes.

If there are no `.json` files, the run is successful: the summary reports zero files and zero messages, and the process returns `0`.

## Batching Algorithm

Batch capacity must come from the Azure SDK sender. Do not configure a fixed message count or byte limit in application code.

For each validated file:

1. Create an empty batch with `sender.create_message_batch()`.
2. Serialize the next object and wrap it in `ServiceBusMessage`.
3. Call `batch.add_message(message)`.
4. If the message fits, continue with the next object.
5. If `batch.add_message` raises the SDK's `MessageSizeExceededError` and the current batch contains messages, send the current batch with `sender.send_messages(batch)`, create a fresh batch, and try the same message once more.
6. If `batch.add_message` raises `MessageSizeExceededError` for a fresh empty batch, fail the file with a clear oversized-message error. Do not skip that message or continue sending later messages from the same file.
7. After all objects have been added, send the final batch only when it contains at least one message.

This algorithm lets the SDK enforce the queue sender's actual batch limit and prevents empty sends. The implementation must track message counts separately rather than relying on undocumented batch internals.

## Failure And Duplicate Semantics

Failures are isolated at file boundaries:

- A file fails on read errors, UTF-8 errors, malformed JSON, input-contract violations, oversized messages, queue sender creation errors, or send errors.
- Processing stops immediately for the failed file and continues with the next file in sorted order.
- A failed file remains unchanged, as do successful files.
- Any batches sent before a later batch fails remain delivered. The file is still reported as failed, including the count confirmed as sent before failure.
- No application-level retry occurs within the run.

The tool provides at-least-once operational behavior, not exactly-once delivery. A rerun processes every discovered file again. It can duplicate all messages from previously successful files and any messages from a failed file that were delivered before its failure. Operators must remove or relocate files manually when resending is not intended, and consumers should be idempotent when duplicates matter.

## Security And Logging

Use standard Python logging configured from `LOG_LEVEL`. Logs and the final summary may include:

- Input file name or path.
- Derived queue name.
- Batch and message counts.
- Success or failure status.
- Sanitized exception type and concise context.

Logs must never include:

- The Service Bus connection string or any substring derived from it.
- Complete message bodies or complete parsed objects.
- `.env` contents.
- Raw exception text; application code cannot reliably prove that arbitrary SDK exception text contains no credential or payload data.

Errors identify the operation and safe resource context, for example: `orders.json -> orders: ServiceBusError while sending batch 2`. Detailed diagnostics can include exception class names, but application logging must construct its own sanitized message instead of dumping arbitrary object state.

## CLI Behavior And Exit Codes

The only supported command is:

```bash
poetry run service-bus-send
```

Runtime behavior is configured through environment variables; no interactive prompts or application-specific command options are required. Progress is logged per file, followed by one aggregate summary containing total files discovered, succeeded, failed, and messages confirmed as sent.

| Exit code | Meaning |
| --- | --- |
| `0` | Configuration was valid and every discovered file succeeded, including a run with no files. |
| `1` | One or more files failed; remaining files were still attempted. |
| `2` | Startup failed before file processing, such as missing configuration, invalid log level, inaccessible data directory, or Service Bus client creation failure. |

Unexpected uncaught errors are startup or run-level failures and return `2`; the CLI logs only sanitized context before exiting.

Example summary:

```text
Service Bus send summary: files=3 succeeded=2 failed=1 messages_sent=47
```

## Testing Strategy

All automated tests are unit tests and must run without Azure credentials or network access. Use temporary directories for file behavior and mocks or small fakes for `ServiceBusClient`, queue senders, batches, and SDK exceptions.

| Area | Required coverage |
| --- | --- |
| Configuration | Required connection string, whitespace-only rejection, defaults, environment overrides, relative data path behavior, and invalid log levels. |
| Discovery and validation | Sorted non-recursive discovery, ignored non-JSON entries, queue derivation, malformed JSON, non-array top level, non-object array elements, UTF-8 errors, empty arrays, and whole-file validation before sending. |
| Batching | Multiple messages in one batch, rollover when a batch fills, final partial-batch flush, no empty send, and failure when one message cannot fit an empty batch. |
| Failure isolation | A failed file stops at that file's failure point while later files are attempted. Partial sends are counted accurately. |
| Summary and status | All-success, no-file, mixed-result, startup-error summaries, and exit codes `0`, `1`, and `2`. |
| Security | Connection strings and complete payloads are absent from captured logs for success and failure paths. |

Batch tests must model `add_message` rejecting a message based on fake capacity. They should assert the exact grouping and order of sent messages, not Azure's internal batch implementation.

## Acceptance Criteria

- [ ] The project installs through Poetry in the `tools-service-bus` Conda environment with Python 3.11 or newer.
- [ ] `poetry run service-bus-send` loads configuration and processes direct `.json` children of the configured data directory in sorted file-name order.
- [ ] `data/orders.json` targets the `orders` queue.
- [ ] Every valid top-level array object becomes one independent `ServiceBusMessage` containing serialized JSON.
- [ ] Invalid files send no messages when validation fails before sending and do not prevent later files from being attempted.
- [ ] Messages are grouped with SDK-created dynamic batches, full batches are flushed, and an oversized single message fails clearly.
- [ ] Successful and failed input files remain unchanged.
- [ ] The summary reports file outcomes and confirmed sent-message counts, including partial sends before a failure.
- [ ] Any file failure produces exit code `1`; startup failures produce `2`; complete success produces `0`.
- [ ] Logs contain operational context without connection strings or complete payloads.
- [ ] Unit tests cover configuration, input validation, batching, continuation, summary, exit status, and secret-safe logging without contacting Azure.
- [ ] README documents setup, execution, duplicate risk, and manual input-file lifecycle; `.env.example` contains no real credentials and local secrets are ignored.

## Operational Examples

Activate the existing environment, then install the package:

```bash
conda activate tools-service-bus
poetry install
```

Create a local `.env` from the documented variable names and provide a real connection string only in that ignored local file:

```dotenv
AZURE_SERVICE_BUS_CONNECTION_STRING=
SERVICE_BUS_DATA_DIR=data
LOG_LEVEL=INFO
```

Place an input file at `data/orders.json`, then run:

```bash
poetry run service-bus-send
```

After a successful run, `data/orders.json` still exists and is unchanged. Running the command again sends every object in that file again.

## Implementation Constraints

- Use `ServiceBusClient.from_connection_string`, `client.get_queue_sender(queue_name)`, `sender.create_message_batch()`, `batch.add_message(...)`, and `sender.send_messages(batch)` following the Azure Service Bus Python SDK V7 pattern.
- Use context managers for SDK client and sender cleanup.
- Keep the CLI orchestration thin and synchronous.
- Do not add a third-party CLI framework, schema framework, retry framework, or application architecture beyond the four stated modules.
- Preserve deterministic file and message order within each run.
