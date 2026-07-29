# Add Topic Subscription Reading

Extend `service-bus-read` so it can read from either a queue or a topic subscription. Azure Service Bus topics cannot be read directly; the command reads messages through the explicitly selected subscription.

## Quick Path

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

## Argument Contract

| Argument | Required | Contract |
| --- | --- | --- |
| `--queue` | Yes | Non-empty queue name, or topic name when `--entity-type topic`. |
| `--count` | Yes | Integer greater than zero. |
| `--mode` | Yes | `peek`, `block`, or `drain`. |
| `--entity-type` | No | `queue` or `topic`; defaults to `queue`. |
| `--subscription` | For `topic` | Non-empty subscription name. Rejected for `queue`. |

The default preserves all existing queue invocations.

## Receiver Selection

| Entity type | SDK operation |
| --- | --- |
| `queue` | `client.get_queue_receiver(queue_name=queue_name)` |
| `topic` | `client.get_subscription_receiver(topic_name=topic_name, subscription_name=subscription_name)` |

After the receiver is created, the existing modes behave unchanged:

- `peek` uses `peek_messages` without locks or settlement.
- `block` receives messages and deliberately leaves them unsettled.
- `drain` renders, writes, flushes, then completes each message.

## Errors And Output

Argument validation occurs before loading configuration or creating an Azure client. Reader errors identify the entity type and name; topic errors include the subscription name. Errors remain sanitized and must not contain connection strings, raw SDK exception text, or message bodies that were not printed.

No new dependencies, environment variables, reader modes, or settlement behavior are introduced.

## Verification Checklist

- [ ] Existing queue calls work without `--entity-type` or `--subscription`.
- [ ] Topic calls require both `--entity-type topic` and `--subscription`.
- [ ] Queue calls reject `--subscription`.
- [ ] Queue and topic paths use their respective receiver factories.
- [ ] All read modes retain their existing receiver, output, and settlement behavior.
- [ ] Reader argument and Azure errors remain safe and return exit code `2`.
