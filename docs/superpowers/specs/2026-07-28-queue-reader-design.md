# Add Queue Reader Command

Add a separate command that displays messages from one Azure Service Bus queue using an explicit read mode. Operators choose whether to inspect without consuming, temporarily lock messages, or drain messages after they are printed.

## Quick Path

```bash
conda run -n tools-service-bus poetry run service-bus-read \
  --queue orders \
  --count 10 \
  --mode peek
```

The command uses the existing connection-string configuration and writes each returned message body to standard output.

## Command Contract

| Argument | Required | Contract |
| --- | --- | --- |
| `--queue` | Yes | Non-empty Azure Service Bus queue name. |
| `--count` | Yes | Integer greater than zero; maximum number of messages to obtain. |
| `--mode` | Yes | One of `peek`, `block`, or `drain`. |

No new environment variables or dependencies are needed. `service-bus-send` remains unchanged.

## Read Modes

| Mode | Azure operation | Settlement | Result |
| --- | --- | --- | --- |
| `peek` | `receiver.peek_messages(max_message_count=count)` | None | Messages are inspected without a lock or removal. |
| `block` | `receiver.receive_messages(max_message_count=count, max_wait_time=10)` | None | Messages are printed and left locked until Azure releases the lock after its configured duration. |
| `drain` | `receiver.receive_messages(max_message_count=count, max_wait_time=10)` | `receiver.complete_message(message)` | Each message is printed, then removed only after successful completion. |

`drain` is destructive. The command prints each message before attempting completion. If output or completion fails, the failing message is not deliberately completed and the command exits with an error. Previously completed messages remain removed.

## Output And Errors

The command prints one body per line to standard output. UTF-8 bodies are printed as text; non-decodable binary bodies use a safe deterministic representation. It prints a final count to standard error so message content remains directly consumable from standard output.

An empty queue is successful and prints no message bodies. Invalid arguments, client or receiver errors, body rendering failures, and settlement errors exit with code `2`. Errors use safe context only: operation, queue name, and exception type. They must not include the connection string, raw SDK exception text, or unprinted message bodies.

## Architecture

Keep the existing sender workflow unchanged. Add a small reader module for argument validation, receiver interaction, body rendering, and result accounting. Add a dedicated CLI entry point that loads the existing configuration and returns the reader exit code. The reader receives client and output dependencies explicitly so unit tests use fakes and captured streams without Azure access.

## Verification Checklist

- [ ] `service-bus-read --queue orders --count 10 --mode peek` peeks and prints up to ten bodies without settlement.
- [ ] `block` receives and prints messages without completion or abandonment.
- [ ] `drain` completes each message only after it is printed.
- [ ] Invalid queue, count, or mode values are rejected before creating an Azure client.
- [ ] Empty queues succeed.
- [ ] Output and errors do not expose credentials or unprinted message bodies.
- [ ] The existing sender command and its tests remain unchanged in behavior.
