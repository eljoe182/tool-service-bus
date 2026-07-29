# Message Envelope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace array input files with validated JSON envelopes that apply common Azure Service Bus application properties to every message body in the envelope.

**Architecture:** Keep the existing four-module structure. `files.py` will decode and validate the whole envelope into a small `MessageEnvelope` value before `cli.py` opens a queue sender; `sender.py` will serialize its `data` objects and create one Azure message per object with a fresh copy of the envelope properties. No new module, dependency, schema library, retry behavior, queue naming rule, batching rule, or exit-code behavior is introduced.

**Tech Stack:** Python 3.11+, Poetry 2, `azure-servicebus` 7.x, `pytest` 8.x, standard-library `dataclasses`, `json`, `math`, `pathlib`, and fake Azure client/sender/batch objects.

---

## Execution Prerequisites

- Work from `/Users/garciajoise/Projects/esmax/tools/service-bus`.
- Run every Poetry, Python, and pytest command through `conda run -n tools-service-bus`.
- Do not contact Azure, use credentials, add dependencies, create modules, initialize Git, or alter the existing client/sender cleanup structure.
- This directory is not a Git repository. Each commit step is conditional: skip it unless a maintainer has initialized Git outside this plan.
- `data/orders.json` is absent in the current working tree. Create it as the canonical documented envelope sample; retain the existing valid `data/sbt-local-indicators.json` sample unchanged.

## File And Responsibility Map

| Path | Change | Responsibility |
| --- | --- | --- |
| `src/service_bus_sender/files.py` | Modify | Define the envelope value and validate complete UTF-8 JSON envelopes before returning data or properties. |
| `src/service_bus_sender/sender.py` | Modify | Serialize each data object, copy application properties for each message, and preserve dynamic batching/error accounting. |
| `src/service_bus_sender/cli.py` | Modify | Consume a validated envelope before opening each queue sender while retaining per-file continuation, safe logs, counts, cleanup resilience, and exit codes. |
| `tests/test_files.py` | Modify | Specify envelope shape, property validation, recursive finite-number checks, and legacy parser failure behavior. |
| `tests/test_sender.py` | Modify | Specify message body/property construction, independent property copies, and batch rollover with properties. |
| `tests/test_cli.py` | Modify | Specify validation-before-sender behavior, file continuation, summaries, counts, exit codes, and cleanup behavior using envelopes. |
| `README.md` | Modify | Explain the envelope contract, accepted property values, sender timing, and message delivery behavior. |
| `data/orders.json` | Create | Provide the documented two-message `orders` envelope sample. |
| `pyproject.toml` | Modify | Update only the package description so it no longer claims array input; dependencies remain byte-for-byte unchanged. |

## Stable Interfaces

Use these names and signatures consistently throughout implementation and tests:

```python
# src/service_bus_sender/files.py
ApplicationProperty = str | int | float | bool | None

@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    properties: dict[str, ApplicationProperty]
    data: list[dict[str, object]]

class InputFileError(ValueError):
    """Raised when an input file violates the JSON input contract."""

def discover_json_files(data_dir: Path) -> list[Path]:
    raise NotImplementedError

def derive_queue_name(path: Path) -> str:
    raise NotImplementedError

def load_message_envelope(path: Path) -> MessageEnvelope:
    raise NotImplementedError

# src/service_bus_sender/sender.py
MessageFactory = Callable[[str, dict[str, ApplicationProperty]], object]

def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    properties: Mapping[str, ApplicationProperty],
    *,
    message_factory: MessageFactory = _create_service_bus_message,
) -> int:
    raise NotImplementedError

# src/service_bus_sender/cli.py
def run(
    config: Config,
    *,
    client_factory: ClientFactory = _default_client_factory,
    logger: logging.Logger = _LOGGER,
) -> RunSummary:
    raise NotImplementedError
```

`MessageEnvelope` is intentionally local to `files.py`: it is the complete validated result for one file. `send_objects` continues to own batching and count handling, but receives the data and common properties separately. `_create_service_bus_message` must construct `ServiceBusMessage(body, application_properties=dict(properties))`; the `dict` call prevents message instances from sharing the mutable mapping.

## Acceptance Coverage Map

| Acceptance criterion | Tasks |
| --- | --- |
| Root object, required fields, and ignored root keys | 1 |
| Primitive property values, string keys, and recursive non-finite rejection | 2 |
| Complete validation before sender opening and later-file continuation | 3 |
| One body plus copied application properties per message | 4 |
| Dynamic rollover preserves properties and existing error/count behavior | 4, 5 |
| README, canonical sample, no dependency change | 6 |
| Safe logs, cleanup resilience, queue derivation, summaries, and exit codes remain unchanged | 3, 5 |

### Task 1: Introduce The Valid Envelope Boundary

**Files:**
- Modify: `tests/test_files.py`
- Modify: `src/service_bus_sender/files.py`

**Work unit:** Valid envelope files return both common properties and message objects; unknown root fields do not affect the returned value.

- [x] **Step 1: Replace array-success tests with valid-envelope behavior tests**

Replace the existing `load_message_objects` imports and array-success tests in `tests/test_files.py` with these tests while retaining discovery, queue-name, read-error, and UTF-8 tests:

```python
from service_bus_sender.files import InputFileError, load_message_envelope


def test_load_message_envelope_returns_properties_and_data_objects(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orders.json"
    customer_name = "Jos" + chr(233)
    path.write_text(
        json.dumps(
            {
                "properties": {"source": "fixture", "priority": 3, "retry": False},
                "data": [{"name": customer_name, "nested": {"count": 2}}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    envelope = load_message_envelope(path)

    assert envelope.properties == {"source": "fixture", "priority": 3, "retry": False}
    assert envelope.data == [{"name": customer_name, "nested": {"count": 2}}]


def test_load_message_envelope_ignores_extra_root_keys(tmp_path: Path) -> None:
    path = tmp_path / "orders.json"
    path.write_text(
        '{"properties":{},"data":[{"orderId":"A-1"}],"ignored":{"trace":true}}',
        encoding="utf-8",
    )

    assert load_message_envelope(path).data == [{"orderId": "A-1"}]


def test_load_message_envelope_accepts_empty_properties_and_data(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_text('{"properties":{},"data":[]}', encoding="utf-8")

    assert load_message_envelope(path).properties == {}
    assert load_message_envelope(path).data == []
```

- [x] **Step 2: Run the focused tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest \
  tests/test_files.py::test_load_message_envelope_returns_properties_and_data_objects \
  tests/test_files.py::test_load_message_envelope_ignores_extra_root_keys \
  tests/test_files.py::test_load_message_envelope_accepts_empty_properties_and_data -v
```

Expected: collection fails because `load_message_envelope` is not exported; the old array parser cannot satisfy the envelope assertions.

- [x] **Step 3: Implement the minimal valid-envelope loader**

Replace `load_message_objects` in `src/service_bus_sender/files.py` and add the shown import and types. Keep `discover_json_files`, `derive_queue_name`, and `_contains_non_finite_number` unchanged in this step.

```python
from dataclasses import dataclass


ApplicationProperty = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    properties: dict[str, ApplicationProperty]
    data: list[dict[str, object]]


def load_message_envelope(path: Path) -> MessageEnvelope:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputFileError(
            f"{path.name}: {type(error).__name__} while reading input"
        ) from error

    try:
        value = json.loads(content)
    except json.JSONDecodeError as error:
        raise InputFileError(f"{path.name}: invalid JSON") from error

    if not isinstance(value, dict):
        raise InputFileError(f"{path.name}: top-level JSON value must be an object")
    if "properties" not in value:
        raise InputFileError(f"{path.name}: properties is required")
    if "data" not in value:
        raise InputFileError(f"{path.name}: data is required")

    properties = value["properties"]
    data = value["data"]
    if not isinstance(properties, dict):
        raise InputFileError(f"{path.name}: properties must be an object")
    if not isinstance(data, list):
        raise InputFileError(f"{path.name}: data must be an array")

    return MessageEnvelope(properties=properties, data=data)
```

- [x] **Step 4: Run the focused tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected: the three selected envelope-success tests pass.

- [ ] **Step 5: Conditionally commit the completed work unit**

Skip while `git rev-parse --is-inside-work-tree` fails. After external Git initialization:

```bash
git add tests/test_files.py src/service_bus_sender/files.py
git commit -m "feat: load service bus message envelopes"
```

### Task 2: Validate Every Envelope Field Before Returning It

**Files:**
- Modify: `tests/test_files.py`
- Modify: `src/service_bus_sender/files.py`

**Work unit:** No malformed envelope leaves the file boundary: required fields have their exact shapes, property values are primitives with finite numbers, and every data object passes recursive finite-number validation.

- [x] **Step 1: Add the failing invalid-envelope tests**

Append these public-loader tests to `tests/test_files.py`:

```python
@pytest.mark.parametrize(
    ("content", "safe_message"),
    [
        ("[]", "top-level JSON value must be an object"),
        ("{}", "properties is required"),
        ('{"properties":{}}', "data is required"),
        ('{"properties":[],"data":[]}', "properties must be an object"),
        ('{"properties":{},"data":{}}', "data must be an array"),
        ('{"properties":{},"data":[7]}', "data item at index 0 must be an object"),
        ('{"properties":{"tags":[]},"data":[]}', "properties.tags must be a primitive"),
        ('{"properties":{"metadata":{}},"data":[]}', "properties.metadata must be a primitive"),
        ('{"properties":{"rate":1e400},"data":[]}', "non-finite numeric value"),
        ('{"properties":{"nested":{"rate":1e400}},"data":[]}', "non-finite numeric value"),
        ('{"properties":{},"data":[{"nested":[{"rate":-1e400}]}]}', "non-finite numeric value"),
    ],
)
def test_load_message_envelope_rejects_invalid_contract_values(
    tmp_path: Path, content: str, safe_message: str
) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(InputFileError, match=safe_message):
        load_message_envelope(path)


def test_load_message_envelope_rejects_a_non_string_property_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "invalid.json"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "service_bus_sender.files.json.loads",
        lambda content: {"properties": {1: "value"}, "data": []},
    )

    with pytest.raises(InputFileError, match="property keys must be strings"):
        load_message_envelope(path)
```

The injected decoded value is deliberate: JSON grammar only permits string object member names, so a non-string key cannot be represented in a UTF-8 JSON fixture. This verifies the defensive contract guard through the public loader without adding a public validation API.

- [x] **Step 2: Run the new validation tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected: tests for non-object data entries, primitive property restrictions, and non-finite values fail because Task 1 only validates root field types.

- [x] **Step 3: Complete validation before constructing `MessageEnvelope`**

Add this helper above `load_message_envelope`, then replace the complete `load_message_envelope` function with the exact implementation below.

```python
def _validate_properties(
    path: Path, properties: dict[object, object]
) -> dict[str, ApplicationProperty]:
    validated: dict[str, ApplicationProperty] = {}
    for key, value in properties.items():
        if not isinstance(key, str):
            raise InputFileError(f"{path.name}: property keys must be strings")
        if _contains_non_finite_number(value):
            raise InputFileError(
                f"{path.name}: properties.{key} contains a non-finite numeric value"
            )
        if value is not None and not isinstance(value, (str, int, float, bool)):
            raise InputFileError(f"{path.name}: properties.{key} must be a primitive")
        validated[key] = value
    return validated


def load_message_envelope(path: Path) -> MessageEnvelope:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputFileError(
            f"{path.name}: {type(error).__name__} while reading input"
        ) from error

    try:
        value = json.loads(content)
    except json.JSONDecodeError as error:
        raise InputFileError(f"{path.name}: invalid JSON") from error

    if not isinstance(value, dict):
        raise InputFileError(f"{path.name}: top-level JSON value must be an object")
    if "properties" not in value:
        raise InputFileError(f"{path.name}: properties is required")
    if "data" not in value:
        raise InputFileError(f"{path.name}: data is required")

    properties = value["properties"]
    data = value["data"]
    if not isinstance(properties, dict):
        raise InputFileError(f"{path.name}: properties must be an object")
    if not isinstance(data, list):
        raise InputFileError(f"{path.name}: data must be an array")

    validated_properties = _validate_properties(path, properties)
    validated_data: list[dict[str, object]] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InputFileError(
                f"{path.name}: data item at index {index} must be an object"
            )
        if _contains_non_finite_number(item):
            raise InputFileError(
                f"{path.name}: data item at index {index} contains a non-finite numeric value"
            )
        validated_data.append(item)

    return MessageEnvelope(properties=validated_properties, data=validated_data)
```

Update retained malformed-JSON, UTF-8, and read-error tests to call `load_message_envelope`; preserve their safe-error assertions. Replace every old top-level-array expectation with its corresponding root-object or `data`-item error above.

- [x] **Step 4: Run the file suite to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected: all file tests pass. Every valid return is a completely validated `MessageEnvelope`; no sender or Azure fake has been created in this suite.

- [ ] **Step 5: Conditionally commit the completed validation unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_files.py src/service_bus_sender/files.py
git commit -m "feat: validate message envelope properties and data"
```

### Task 3: Validate Envelopes Before Opening Senders

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/service_bus_sender/cli.py`

**Work unit:** The CLI opens a queue sender only after a whole envelope is valid, then keeps the existing per-file continuation, cleanup resilience, count accounting, safe logs, queue derivation, and exit code behavior.

- [x] **Step 1: Convert CLI fixtures and add the pre-open validation regression**

Replace `write_json` call values in `tests/test_cli.py` with the helper and test below. Convert every existing successful, empty, invalid, partial-send, cleanup, summary, and exit-code fixture through `envelope(...)`; preserve their existing expected queue order, counts, logs, and statuses.

```python
def envelope(
    data: list[dict[str, object]],
    properties: dict[str, object] | None = None,
) -> dict[str, object]:
    return {"properties": properties or {}, "data": data}


def test_run_validates_the_complete_envelope_before_opening_its_sender_and_continues(
    tmp_path: Path,
) -> None:
    write_json(
        tmp_path / "a.json",
        {"properties": {"source": "fixture"}, "data": [{"valid": True}, 7]},
    )
    write_json(tmp_path / "b.json", envelope([{"sent": True}], {"source": "fixture"}))
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.validation-continuation"),
    )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert factory.client.queue_names == ["b"]
```

Also replace the old empty-array test with `write_json(tmp_path / "empty.json", envelope([]))`; it must still expect a successful zero-message file and an opened/closed sender because a valid empty envelope is processed normally.

- [x] **Step 2: Run the CLI suite to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected: collection or execution fails because `cli.py` still imports `load_message_objects` and passes only object arrays to `send_objects`.

- [x] **Step 3: Consume the envelope before the queue sender context**

Update imports and the success path in `run` exactly as follows; leave all existing exception handlers and logging statements intact:

```python
from service_bus_sender.files import (
    InputFileError,
    derive_queue_name,
    discover_json_files,
    load_message_envelope,
)


            try:
                envelope = load_message_envelope(path)
                primary_send_error: FileSendError | None = None
                try:
                    with client.get_queue_sender(queue_name=queue_name) as sender:
                        try:
                            sent_for_file = send_objects(
                                sender, envelope.data, envelope.properties
                            )
                        except FileSendError as error:
                            primary_send_error = error
                            raise
                except Exception:
                    if primary_send_error is not None:
                        raise primary_send_error
                    raise
                if primary_send_error is not None:
                    raise primary_send_error
```

This ordering is required: `load_message_envelope(path)` must finish before `client.get_queue_sender(...)` executes. Do not move validation into `sender.py` or catch raw exception text in logs.

- [x] **Step 4: Run the CLI suite to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected: all converted CLI tests pass. Invalid `a.json` creates no sender, valid `b.json` is still sent, and existing cleanup/partial-send/safe-log tests retain their original expectations.

- [ ] **Step 5: Conditionally commit the orchestration unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_cli.py src/service_bus_sender/cli.py
git commit -m "feat: validate envelopes before opening queue senders"
```

### Task 4: Attach Copied Application Properties To Every Message

**Files:**
- Modify: `tests/test_sender.py`
- Modify: `src/service_bus_sender/sender.py`

**Work unit:** Each data object is serialized exactly as before, and each independently created message receives a distinct copy of the validated common properties.

- [x] **Step 1: Make sender fakes observable and write the first property test**

Replace string-only message factory use in `tests/test_sender.py` with this test-local message and factory:

```python
from dataclasses import dataclass


@dataclass
class FakeMessage:
    body: str
    application_properties: dict[str, object]


def fake_message_factory(body: str, properties: dict[str, object]) -> FakeMessage:
    return FakeMessage(body=body, application_properties=properties)


def test_send_objects_serializes_bodies_and_copies_properties_per_message() -> None:
    sender = FakeSender()
    properties = {"source": "fixture", "priority": 3, "retry": False, "note": None}

    sent_count = send_objects(
        sender,
        [{"orderId": "A-1"}, {"orderId": "A-2"}],
        properties,
        message_factory=fake_message_factory,
    )

    first, second = sender.sent_batches[0]
    assert sent_count == 2
    assert first.body == '{"orderId":"A-1"}'
    assert second.body == '{"orderId":"A-2"}'
    assert first.application_properties == properties
    assert second.application_properties == properties
    assert first.application_properties is not properties
    assert second.application_properties is not properties
    assert first.application_properties is not second.application_properties
```

- [x] **Step 2: Run the focused sender test to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py::test_send_objects_serializes_bodies_and_copies_properties_per_message -v
```

Expected: the test fails because `send_objects` does not accept `properties`, and the current message factory accepts only a body.

- [x] **Step 3: Change the message construction interface minimally**

In `src/service_bus_sender/sender.py`, import `ApplicationProperty` from `files.py`, replace `MessageFactory`, add the factory below, and update the `send_objects` signature and loop:

```python
from service_bus_sender.files import ApplicationProperty


MessageFactory = Callable[[str, dict[str, ApplicationProperty]], object]


def _create_service_bus_message(
    body: str, properties: dict[str, ApplicationProperty]
) -> ServiceBusMessage:
    return ServiceBusMessage(body, application_properties=properties)


def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    properties: Mapping[str, ApplicationProperty],
    *,
    message_factory: MessageFactory = _create_service_bus_message,
) -> int:
    sent_count = 0
    batch_number = 1
    batch = _create_batch(sender, sent_count=sent_count, batch_number=batch_number)
    batch_count = 0

    for item in objects:
        body = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
        message = message_factory(body, dict(properties))
        try:
            batch.add_message(message)
            batch_count += 1
            continue
        except MessageSizeExceededError as error:
            if batch_count == 0:
                raise FileSendError(
                    sent_count=sent_count,
                    batch_number=batch_number,
                    operation="adding message to",
                    cause=error,
                ) from error
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
            batch_count=batch_count,
            sent_count=sent_count,
            batch_number=batch_number,
        )
        batch_number += 1
        batch = _create_batch(
            sender, sent_count=sent_count, batch_number=batch_number
        )
        batch_count = 0
        try:
            batch.add_message(message)
            batch_count = 1
        except Exception as error:
            raise FileSendError(
                sent_count=sent_count,
                batch_number=batch_number,
                operation="adding message to",
                cause=error,
            ) from error

    if batch_count:
        sent_count = _send_batch(
            sender,
            batch,
            batch_count=batch_count,
            sent_count=sent_count,
            batch_number=batch_number,
        )

    return sent_count
```

The constructor form is supported by the installed Azure SDK: `ServiceBusMessage(body, application_properties=properties)`.

- [x] **Step 4: Run the focused sender test to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py::test_send_objects_serializes_bodies_and_copies_properties_per_message -v
```

Expected: PASS; the fake batch contains two bodies in order and two distinct property dictionaries with equal values.

- [ ] **Step 5: Conditionally commit the message-construction unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_sender.py src/service_bus_sender/sender.py
git commit -m "feat: add envelope properties to service bus messages"
```

### Task 5: Preserve Batching And Existing Runtime Behavior

**Files:**
- Modify: `tests/test_sender.py`
- Modify: `tests/test_cli.py`

**Work unit:** Application properties survive every batch rollover and no existing batching, cleanup, safe-log, count, queue-order, summary, or exit-code behavior regresses.

- [x] **Step 1: Convert existing sender tests and add rollover property assertions**

Pass `{}` as the third positional argument to every existing `send_objects` test. Add this behavior test using the capacity-aware fake already in the file:

```python
def test_send_objects_preserves_properties_across_batch_rollover() -> None:
    sender = FakeSender(capacity=2)

    sent_count = send_objects(
        sender,
        [{"n": 1}, {"n": 2}, {"n": 3}],
        {"source": "fixture"},
        message_factory=fake_message_factory,
    )

    assert sent_count == 3
    assert [[message.body for message in batch] for batch in sender.sent_batches] == [
        ['{"n":1}', '{"n":2}'],
        ['{"n":3}'],
    ]
    assert [
        message.application_properties for batch in sender.sent_batches for message in batch
    ] == [{"source": "fixture"}, {"source": "fixture"}, {"source": "fixture"}]
```

Retain existing assertions for no empty send, oversized message failure, partial sent counts, safe `FileSendError` text, final flush, and exact batch grouping. In `tests/test_cli.py`, add an assertion in the sorted successful-run test that each fake sender’s batches carry the expected envelope property mapping after extending `FakeQueueSender.send_messages` to retain `sent_batches`.

- [ ] **Step 2: Run focused rollover and orchestration regressions to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py::test_send_objects_preserves_properties_across_batch_rollover tests/test_cli.py::test_run_uses_one_client_and_sorted_sender_contexts_without_changing_files -v
```

Expected: RED until the test fakes and all legacy sender call sites consistently use the new message-factory signature and envelope fixtures.

- [x] **Step 3: Update test fakes only, not production behavior**

Make `FakeQueueSender` retain the exact sent batch objects in addition to its existing `messages_sent` counter:

```python
self.sent_batches: list[list[object]] = []

def send_messages(self, batch: FakeBatch) -> None:
    self.send_attempts += 1
    if self.send_attempts == self.fail_on_send:
        raise RuntimeError(self.failure_text)
    self.sent_batches.append(list(batch.messages))
    self.messages_sent += len(batch.messages)
```

Do not change `FileSendError`, `run`, `RunSummary`, logging formats, context-manager nesting, or `main`. The purpose of this step is to adapt test doubles to observe the new public message behavior while proving all prior observable behaviors remain stable.

- [x] **Step 4: Run all runtime regression tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py tests/test_cli.py -v
```

Expected: PASS. Batches remain capacity-driven and ordered; no empty batch is sent; oversized and send failures retain confirmed counts; cleanup failure retains the primary failure; later files continue; safe logs omit secrets/payloads; and exit codes remain `0`, `1`, and `2`.

- [ ] **Step 5: Conditionally commit the regression unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_sender.py tests/test_cli.py
git commit -m "test: preserve sender behavior with envelope properties"
```

### Task 6: Publish The Envelope Contract And Verify The Complete Change

**Files:**
- Create: `data/orders.json`
- Modify: `README.md`
- Modify: `pyproject.toml`

**Work unit:** Operators receive a correct canonical sample and concise contract documentation; package metadata no longer advertises obsolete array input.

- [x] **Step 1: Add the documentation/sample contract tests**

Append this test to `tests/test_files.py` so the canonical sample is validated by the production parser:

```python
def test_canonical_orders_sample_is_a_valid_envelope() -> None:
    project_root = Path(__file__).resolve().parents[1]

    envelope = load_message_envelope(project_root / "data" / "orders.json")

    assert envelope.properties == {
        "source": "service-bus-sample",
        "priority": 3,
        "isRetry": False,
    }
    assert envelope.data == [
        {"orderId": "A-1001", "status": "created"},
        {"orderId": "A-1002", "status": "created"},
    ]
```

- [x] **Step 2: Run the sample test to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py::test_canonical_orders_sample_is_a_valid_envelope -v
```

Expected: FAIL with `FileNotFoundError` wrapped as `InputFileError` because `data/orders.json` does not yet exist.

- [x] **Step 3: Create the sample and replace obsolete README input language**

Create `data/orders.json` exactly:

```json
{
  "properties": {
    "source": "service-bus-sample",
    "priority": 3,
    "isRetry": false
  },
  "data": [
    {
      "orderId": "A-1001",
      "status": "created"
    },
    {
      "orderId": "A-1002",
      "status": "created"
    }
  ]
}
```

Replace README lines 1-3 and the complete `## Input Contract` section with this content, keeping its existing Run, Exit Codes, Delivery And File Lifecycle, and Test sections unchanged:

```markdown
# Azure Service Bus JSON Sender

Send each object in an enveloped JSON file as an independent message to an Azure Service Bus queue. The queue name is the file name without its final `.json` suffix, and the envelope's common properties become Azure application properties on every message.

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
```

In `pyproject.toml`, change only the description value:

```toml
description = "Send enveloped JSON messages to Azure Service Bus queues"
```

- [x] **Step 4: Run documentation/sample checks and the full suite**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py::test_canonical_orders_sample_is_a_valid_envelope -v
conda run -n tools-service-bus poetry run pytest -v
conda run -n tools-service-bus poetry check
```

Expected: the sample test and full test suite pass without Azure/network access; `poetry check` reports a valid project; `pyproject.toml` dependencies and `poetry.lock` are unchanged.

- [ ] **Step 5: Conditionally commit documentation and sample updates**

Skip while Git is unavailable. After external Git initialization:

```bash
git add README.md data/orders.json pyproject.toml tests/test_files.py
git commit -m "docs: describe service bus message envelopes"
```

## Final Verification Checklist

- [x] Run `conda run -n tools-service-bus poetry run pytest -v` and confirm all tests pass using fakes/mocks only.
- [x] Run `conda run -n tools-service-bus poetry check` and confirm it exits `0`.
- [x] Confirm `pyproject.toml` dependency declarations and `poetry.lock` have not changed.
- [x] Confirm no production code opens a queue sender before `load_message_envelope(path)` returns.
- [x] Confirm each message creation uses `dict(properties)` and `ServiceBusMessage(body, application_properties=properties)`.
- [x] Confirm README documents required keys, ignored extras, primitive values, empty envelope fields, queue derivation, and duplicate/partial-send risk.
- [x] Confirm the plan contains no `TODO`, `TBD`, omitted implementation, or unwrapped Poetry/Python/pytest command.
