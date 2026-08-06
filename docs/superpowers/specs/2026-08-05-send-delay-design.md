# Add Optional Inter-Message Send Delay

Extend `service-bus-send` with an optional delay between individual messages. Without the new argument, dynamic batching and send behavior remain unchanged. With a positive delay, each message is sent one at a time and the process sleeps between consecutive messages inside the same file.

## Quick Path

Send with current batching, unchanged:

```bash
conda run -n tools-service-bus poetry run service-bus-send
```

Send one message at a time with a half-second pause between messages:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --delay 0.5
```

Combine with topic publishing:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --topic orders-events \
  --delay 1
```

## Contract

| Argument | Required | Contract |
| --- | --- | --- |
| `--topic` | No | Existing contract: non-empty topic name. |
| `--delay` | No | Non-negative float seconds. Omitted or `0` keeps dynamic batching with no sleep. Values greater than `0` enable per-message sends with `N-1` sleeps for `N` messages in a file. |

Invalid `--delay` values (negative, missing value, non-numeric, or unrecognized arguments) fail with exit code `2` before loading configuration or creating an Azure client. Help text is:

```text
usage: service-bus-send [--topic TOPIC] [--delay SECONDS]
```

## Send Behavior

| Mode | How messages are sent | Sleep |
| --- | --- | --- |
| Default / `--delay 0` | Existing dynamic batching via `create_message_batch` / `send_messages(batch)` | None |
| `--delay > 0` | One `ServiceBusMessage` per `send_messages` call | `time.sleep(delay_seconds)` after each confirmed send except the last message in that file |

`send_objects` gains an optional `delay_seconds: float = 0.0` parameter. The CLI parses `--delay`, passes it through `run`, and into `send_objects`. Sleep is injectable in tests so offline suites do not wait wall-clock time.

There is no delay between files. Sleep applies only between consecutive messages of the same file's `data` array.

## Error And Count Semantics

`FileSendError` remains the batching/send error boundary. Confirmed partial counts, per-file continuation, safe logging, exit codes, and input-file immutability do not change.

In delay mode, each individual send is treated as its own batch for error reporting (`batch_number` advances `1..N`). A failure after earlier successful sends still reports only confirmed earlier messages and marks the file failed.

## Preserved Behavior

Envelope validation, application properties, compact serialization, queue-default and topic modes, input-file lifecycle, summary format, and duplicate semantics are unchanged when `--delay` is omitted or `0`.

## Out Of Scope

- Delay between files
- Delay configuration via environment variables
- Changes to `service-bus-read`
- Retries, deduplication, or pacing based on Azure throttling signals

## Verification Checklist

- [x] Omitted `--delay` and `--delay 0` keep dynamic batching and send zero sleeps.
- [x] `--delay` accepts non-negative floats (including decimals) and rejects invalid values with exit code `2` before configuration loading.
- [x] Positive delay sends one message per Azure call and sleeps `N-1` times for `N` messages in a file.
- [x] `--delay` combines with `--topic` without changing destination selection.
- [x] Partial-send errors in delay mode still report confirmed counts and continue later files.
- [x] README documents `--delay` with an example and argument table row.
- [x] Offline tests cover parsing, delay-mode sends with injectable sleep, and existing batching regressions.
