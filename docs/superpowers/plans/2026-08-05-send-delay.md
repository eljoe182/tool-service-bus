# Inter-Message Send Delay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional `service-bus-send --delay SECONDS` so positive delays send one message at a time with `N-1` sleeps per file, while omitted/`0` keeps dynamic batching.

**Architecture:** CLI fail-fast parsing extends the existing `--topic` parser into `SendArguments`. `run` passes `delay_seconds` into `send_objects`. When `delay_seconds > 0`, `send_objects` sends single-message batches and calls an injectable sleeper between confirmed sends; otherwise the current batching path is unchanged.

**Tech Stack:** Python 3.11, argparse, azure-servicebus protocols/fakes, pytest, Conda env `tools-service-bus`.

**Spec:** `docs/superpowers/specs/2026-08-05-send-delay-design.md`

---

### File map

| File | Responsibility |
| --- | --- |
| `src/sender/cli.py` | Parse `--delay`, pass through `run`/`main`, update help |
| `src/sender/service.py` | Optional delay path in `send_objects` with injectable sleeper |
| `tests/sender/test_cli.py` | Parsing, help, wiring, README assertions |
| `tests/sender/test_service.py` | Delay-mode send/sleep/partial-failure behavior |
| `README.md` | Document `--delay` usage and contract |

---

### Task 1: Parse `--delay` in the CLI

**Files:**
- Modify: `src/sender/cli.py`
- Modify: `tests/sender/test_cli.py`

- [ ] **Step 1: Write failing parse tests**

Replace `parse_topic` usage with `parse_send_arguments` / `SendArguments`. Add:

```python
from sender.cli import ArgumentParseError, SendArguments, parse_send_arguments

def test_parse_send_arguments_defaults_and_accepts_delay() -> None:
    assert parse_send_arguments([]) == SendArguments(topic=None, delay_seconds=0.0)
    assert parse_send_arguments(["--delay", "0"]) == SendArguments(
        topic=None, delay_seconds=0.0
    )
    assert parse_send_arguments(["--delay", "0.5"]) == SendArguments(
        topic=None, delay_seconds=0.5
    )
    assert parse_send_arguments(["--topic", " orders-events ", "--delay", "1"]) == (
        SendArguments(topic="orders-events", delay_seconds=1.0)
    )


@pytest.mark.parametrize(
    ("argv", "expected_message"),
    [
        (["--topic", "   "], "--topic must be non-empty"),
        (["--delay", "-1"], "--delay must be a non-negative number"),
        (["--delay"], "expected one argument"),
        (["--delay", "abc"], "--delay must be a non-negative number"),
        (["--unknown"], "unrecognized arguments: --unknown"),
    ],
)
def test_parse_send_arguments_rejects_invalid_arguments(
    argv: list[str], expected_message: str
) -> None:
    with pytest.raises(ArgumentParseError, match=expected_message):
        parse_send_arguments(argv)
```

Update existing `parse_topic` tests and imports accordingly. Keep the main-rejects-invalid-before-config pattern for negative delay.

- [ ] **Step 2: Run tests (expect FAIL)**

```bash
conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_parse_send_arguments_defaults_and_accepts_delay tests/sender/test_cli.py::test_parse_send_arguments_rejects_invalid_arguments -v
```

- [ ] **Step 3: Implement parsing**

In `src/sender/cli.py`:

```python
@dataclass(frozen=True, slots=True)
class SendArguments:
    topic: str | None = None
    delay_seconds: float = 0.0


def _non_negative_delay(value: str) -> float:
    try:
        delay = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "--delay must be a non-negative number"
        ) from error
    if delay < 0:
        raise argparse.ArgumentTypeError("--delay must be a non-negative number")
    return delay


def parse_send_arguments(argv: Sequence[str]) -> SendArguments:
    parser = SenderArgumentParser(prog="service-bus-send", add_help=False)
    parser.add_argument("--topic", type=_non_empty_topic)
    parser.add_argument("--delay", type=_non_negative_delay, default=0.0)
    namespace = parser.parse_args(argv)
    return SendArguments(topic=namespace.topic, delay_seconds=namespace.delay)
```

Remove `parse_topic` (or keep as thin wrapper only if tests still need it — prefer remove and update callers).

In `main`:
- help: `usage: service-bus-send [--topic TOPIC] [--delay SECONDS]\n`
- parse via `parse_send_arguments`
- pass `topic=arguments.topic` and `delay_seconds=arguments.delay_seconds` into `run`

Extend `run(..., delay_seconds: float = 0.0)` and pass it to `send_objects` (Task 2 may stub the kwarg first).

- [ ] **Step 4: Re-run parse/help tests (expect PASS for parsing; wiring may wait Task 2)**

```bash
conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py -k "parse_send_arguments or prints_help or rejects_invalid" -v
```

---

### Task 2: Delay path in `send_objects`

**Files:**
- Modify: `src/sender/service.py`
- Modify: `tests/sender/test_service.py`
- Modify: `src/sender/cli.py` (wire `delay_seconds`)

- [ ] **Step 1: Write failing delay-mode tests**

```python
def test_send_objects_with_delay_sends_one_message_at_a_time_and_sleeps_between() -> None:
    sender = FakeSender()
    sleeps: list[float] = []

    sent_count = send_objects(
        sender,
        [{"n": 1}, {"n": 2}, {"n": 3}],
        {},
        message_factory=lambda body, properties: body,
        delay_seconds=0.5,
        sleeper=sleeps.append,
    )

    assert sent_count == 3
    assert sender.sent_batches == [['{"n":1}'], ['{"n":2}'], ['{"n":3}']]
    assert sleeps == [0.5, 0.5]


def test_send_objects_with_delay_zero_keeps_batching_and_does_not_sleep() -> None:
    sender = FakeSender(capacity=2)
    sleeps: list[float] = []

    sent_count = send_objects(
        sender,
        [{"n": 1}, {"n": 2}, {"n": 3}],
        {},
        message_factory=lambda body, properties: body,
        delay_seconds=0.0,
        sleeper=sleeps.append,
    )

    assert sent_count == 3
    assert sender.sent_batches == [['{"n":1}', '{"n":2}'], ['{"n":3}']]
    assert sleeps == []


def test_send_objects_with_delay_reports_confirmed_count_before_send_failure() -> None:
    sender = FakeSender(fail_on_send=2, failure_text="secret-marker")
    sleeps: list[float] = []

    with pytest.raises(FileSendError) as error:
        send_objects(
            sender,
            [{"n": 1}, {"n": 2}, {"n": 3}],
            {},
            message_factory=lambda body, properties: body,
            delay_seconds=1.0,
            sleeper=sleeps.append,
        )

    assert error.value.sent_count == 1
    assert error.value.batch_number == 2
    assert error.value.operation == "sending"
    assert sender.sent_batches == [['{"n":1}']]
    assert sleeps == [1.0]
```

- [ ] **Step 2: Run tests (expect FAIL)**

```bash
conda run -n tools-service-bus poetry run pytest tests/sender/test_service.py -k delay -v
```

- [ ] **Step 3: Implement delay path**

In `src/sender/service.py`:

```python
import time
from collections.abc import Callable, Mapping, Sequence

Sleeper = Callable[[float], None]


def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    properties: Mapping[str, ApplicationProperty],
    *,
    message_factory: MessageFactory = _create_service_bus_message,
    delay_seconds: float = 0.0,
    sleeper: Sleeper = time.sleep,
) -> int:
    if delay_seconds > 0:
        return _send_objects_with_delay(
            sender,
            objects,
            properties,
            message_factory=message_factory,
            delay_seconds=delay_seconds,
            sleeper=sleeper,
        )
    # existing batching body unchanged
    ...


def _send_objects_with_delay(...) -> int:
    sent_count = 0
    total = len(objects)
    for index, item in enumerate(objects):
        batch_number = index + 1
        body = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
        message = message_factory(body, dict(properties))
        batch = _create_batch(sender, sent_count=sent_count, batch_number=batch_number)
        try:
            batch.add_message(message)
        except Exception as error:
            raise FileSendError(
                sent_count=sent_count,
                batch_number=batch_number,
                operation="adding message to",
                cause=error,
            ) from error
        sent_count = _send_batch(
            sender,
            batch,
            batch_count=1,
            sent_count=sent_count,
            batch_number=batch_number,
        )
        if index < total - 1:
            sleeper(delay_seconds)
    return sent_count
```

Wire `run(..., delay_seconds=...)` → `send_objects(..., delay_seconds=delay_seconds)`.

- [ ] **Step 4: Run service + CLI suite slices (expect PASS)**

```bash
conda run -n tools-service-bus poetry run pytest tests/sender/test_service.py tests/sender/test_cli.py -v
```

---

### Task 3: README + full verification

**Files:**
- Modify: `README.md`
- Modify: `tests/sender/test_cli.py` (README assertions)

- [ ] **Step 1: Extend README assertion test**

```python
assert "--delay 0.5" in readme
assert "non-negative" in readme or "Non-negative" in readme
```

Document in README Run section:

```bash
conda run -n tools-service-bus poetry run service-bus-send \
  --delay 0.5
```

| `--delay` | Optional non-negative seconds. Omitted or `0` keeps dynamic batching. Values `> 0` send one message at a time with `N-1` sleeps per file. |

- [ ] **Step 2: Full suite**

```bash
conda run -n tools-service-bus poetry run pytest -v
```

Expected: all tests pass.

- [ ] **Step 3: Mark design checklist items complete in the spec**

Update `docs/superpowers/specs/2026-08-05-send-delay-design.md` verification boxes to `[x]` after green suite.

---

### Spec coverage

| Spec requirement | Task |
| --- | --- |
| `--delay` float ≥ 0, fail-fast | Task 1 |
| omitted/`0` = batching | Task 2 |
| `> 0` = one-at-a-time, N−1 sleeps | Task 2 |
| combines with `--topic` | Task 1 |
| partial counts / FileSendError | Task 2 |
| README | Task 3 |
| injectable sleep / offline tests | Task 2–3 |
