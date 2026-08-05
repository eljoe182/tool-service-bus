# Add Optional Topic Sending

Extend `service-bus-send` with an optional topic destination. Without the new argument, every JSON file continues to target the queue identified by its file name. With a topic, every file publishes its messages to that one topic.

## Quick Path

Send files to queues, unchanged:

```bash
conda run -n tools-service-bus poetry run service-bus-send
```

Publish all files to a topic:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --topic orders-events
```

## Contract

| Argument | Required | Contract |
| --- | --- | --- |
| `--topic` | No | Non-empty topic name. When omitted, each file name identifies a queue as before. |

When `--topic` is present, a file such as `data/dashboard.json` publishes its data objects to `orders-events`. The `dashboard` stem is an expected subscription name for traceability only. It does not become an Azure destination or an application property.

Azure Service Bus sends messages to topics, not directly to subscriptions. Subscription filters determine which subscription receives each published message.

## Sender Selection

| Mode | Azure sender operation | File-stem behavior |
| --- | --- | --- |
| Queue default | `client.get_queue_sender(queue_name=file_stem)` | The stem is the queue name. |
| Topic | `client.get_topic_sender(topic_name=topic)` | The stem is included only in safe log context as the expected subscription. |

The sender creates a topic sender per input file, matching the existing file-isolation lifecycle. No topic sender is opened until that file's envelope has completely validated.

## Preserved Behavior

Message envelopes, application properties, dynamic batching, input-file immutability, per-file continuation, confirmed partial counts, safe logging, exit codes, and duplicate semantics do not change.

Invalid `--topic` arguments fail before loading configuration or creating an Azure client. Topic-mode logs identify the file, topic, and expected subscription but never message bodies, application property values, raw SDK exception text, or credentials.

## Verification Checklist

- [x] Existing queue invocation still uses queue senders based on file stems.
- [x] `--topic` is non-empty and validated before configuration loading.
- [x] Topic mode uses topic senders and never queue senders.
- [x] Topic-mode logs distinguish topic and expected subscription safely.
- [x] Invalid files still open no sender, and one failed file does not stop later files.
- [x] Existing batching, partial-count, error, and exit behavior remains covered by offline tests.
