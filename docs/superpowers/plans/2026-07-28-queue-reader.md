# Queue Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe, separately invocable Azure Service Bus queue reader with explicit peek, temporary-lock, and destructive-drain modes.

**Architecture:** Keep the sender CLI and its workflow untouched. Add `reader.py` as the focused, dependency-injected read boundary and `reader_cli.py` as the thin parser/configuration entry point; both reuse the existing `Config` and connection-string client factory pattern. The reader will render and write one message before settling it in drain mode, so a write or rendering failure never deliberately removes that message.

**Tech Stack:** Python 3.11+, Poetry 2, `azure-servicebus` 7.x, `pytest` 8.x, and standard-library `argparse`, `base64`, `contextlib`, `dataclasses`, `io`, and `typing`.

---

## Execution Prerequisites

- Work from `/Users/garciajoise/Projects/esmax/tools/service-bus`.
- Run every command through `conda run -n tools-service-bus`.
- Do not use credentials, contact Azure, add dependencies, or alter `service-bus-send`, `src/service_bus_sender/cli.py`, sender tests, or sender behavior.
- This directory is not a Git repository. Run a commit step only if a maintainer initializes Git outside this plan; otherwise record the completed work unit without committing.
- All reader tests use fake clients, receivers, messages, and `io.StringIO` streams. They must make no Azure or network calls.

## File And Responsibility Map

| Path | Change | Responsibility |
| --- | --- | --- |
| `src/service_bus_sender/reader.py` | Create | Read request/result values, receiver protocols, body rendering, exact Azure receiver calls, drain settlement ordering, cleanup, and safe read errors. |
| `src/service_bus_sender/reader_cli.py` | Create | Required argument parsing, existing configuration loading, client construction, safe `stderr` diagnostics, and reader exit codes. |
| `tests/test_reader.py` | Create | Offline behavioral tests with fake context-managed clients, receivers, messages, and streams. |
| `tests/test_reader_cli.py` | Create | Parser, CLI exit-code, configuration, client-factory, and safe-diagnostic tests. |
| `pyproject.toml` | Modify | Register `service-bus-read` while retaining the existing `service-bus-send` entry point. |
| `README.md` | Modify | Document reader invocation, modes, output, exit code, and destructive-drain semantics alongside sender documentation. |

## Stable Interfaces

Use these names and signatures consistently in every implementation and test step:

| File | Stable item | Type or signature |
| --- | --- | --- |
| `reader.py` | `ReadRequest` | Frozen slotted dataclass with `queue_name: str`, `count: int`, and `mode: str`. |
| `reader.py` | `ReadResult` | Frozen slotted dataclass with `message_count: int`. |
| `reader.py` | `QueueReadError` | Runtime error exposing `operation: str`, `queue_name: str`, and `error_type: str`. |
| `reader.py` | `render_message_body` | `(body: object) -> str` |
| `reader.py` | `read_messages` | `(config: Config, request: ReadRequest, *, client_factory: ClientFactory, stdout: TextIO, stderr: TextIO) -> ReadResult` |
| `reader_cli.py` | `parse_request` | `(argv: Sequence[str]) -> ReadRequest` |
| `reader_cli.py` | `main` | `(argv: Sequence[str] | None = None, *, config_loader: ConfigLoader = load_config, client_factory: ClientFactory = _default_client_factory, stdout: TextIO = sys.stdout, stderr: TextIO = sys.stderr) -> int` |

`render_message_body` accepts `str`, `bytes`, and iterable byte DATA bodies. UTF-8 data is decoded and line separators are escaped as `\\n` and `\\r` so each message occupies exactly one physical stdout line. Non-UTF-8 bytes render as `base64:<ASCII payload>` using `base64.b64encode`; unsupported body values or a broken byte iterable raise and are reported only by safe context. `ReadResult.message_count` is the number of messages successfully output (and, for `drain`, successfully completed) during a successful command.

## Acceptance Coverage Map

| Acceptance criterion | Tasks |
| --- | --- |
| Separate command and existing sender entry point retained | 5 |
| Required non-empty queue, positive count, and exact mode parsing before client creation | 4 |
| Exact peek operation with no settlement | 1 |
| Exact block receive operation with no completion or abandonment | 2 |
| Drain write-before-complete ordering and partial completion | 2, 3 |
| UTF-8, deterministic binary rendering, one stdout line each, and stderr-only count | 1, 2 |
| Empty queue success | 1 |
| Safe exit-2 diagnostics for config, client, receiver, render, write, and settlement failures | 3, 4 |
| Client and receiver cleanup | 3 |
| README and full offline regression coverage | 5, 6 |

### Task 1: Establish Peek Reading And Body Rendering

**Files:**
- Create: `tests/test_reader.py`
- Create: `src/service_bus_sender/reader.py`

**Work unit:** A valid peek request obtains messages with the exact SDK parameters, writes one safe body per stdout line, reports only the count to stderr, and closes client and receiver contexts.

- [x] **Step 1: Write the first failing reader tests and reusable fakes**

Create `tests/test_reader.py` with the following fakes and tests:

```python
from contextlib import AbstractContextManager
from io import StringIO
from pathlib import Path

from service_bus_sender.config import Config
from service_bus_sender.reader import ReadRequest, read_messages, render_message_body


class FakeMessage:
    def __init__(self, body: object) -> None:
        self.body = body


class FakeReceiver(AbstractContextManager["FakeReceiver"]):
    def __init__(self, *, peeked: list[FakeMessage] | None = None) -> None:
        self.peeked = peeked or []
        self.peek_calls: list[int] = []
        self.receive_calls: list[tuple[int, int]] = []
        self.completed: list[FakeMessage] = []
        self.abandoned: list[FakeMessage] = []
        self.entered = False
        self.exited = False

    def __enter__(self) -> "FakeReceiver":
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.exited = True

    def peek_messages(self, *, max_message_count: int) -> list[FakeMessage]:
        self.peek_calls.append(max_message_count)
        return self.peeked

    def receive_messages(
        self, *, max_message_count: int, max_wait_time: int
    ) -> list[FakeMessage]:
        self.receive_calls.append((max_message_count, max_wait_time))
        return []

    def complete_message(self, message: FakeMessage) -> None:
        self.completed.append(message)

    def abandon_message(self, message: FakeMessage) -> None:
        self.abandoned.append(message)


class FakeClient(AbstractContextManager["FakeClient"]):
    def __init__(self, receiver: FakeReceiver) -> None:
        self.receiver = receiver
        self.queue_names: list[str] = []
        self.entered = False
        self.exited = False

    def __enter__(self) -> "FakeClient":
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.exited = True

    def get_queue_receiver(self, *, queue_name: str) -> FakeReceiver:
        self.queue_names.append(queue_name)
        return self.receiver


class FakeClientFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.connection_strings: list[str] = []

    def __call__(self, connection_string: str) -> FakeClient:
        self.connection_strings.append(connection_string)
        return self.client


def make_config() -> Config:
    return Config(
        connection_string="test-connection", data_dir=Path("data"), log_level=20
    )


def test_peek_prints_utf8_bodies_and_count_without_settlement() -> None:
    receiver = FakeReceiver(peeked=[FakeMessage(b"first"), FakeMessage("second")])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=5, mode="peek"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 2
    assert factory.connection_strings == ["test-connection"]
    assert factory.client.queue_names == ["orders"]
    assert receiver.peek_calls == [5]
    assert receiver.receive_calls == []
    assert receiver.completed == []
    assert receiver.abandoned == []
    assert stdout.getvalue() == "first\nsecond\n"
    assert stderr.getvalue() == "Read 2 messages\n"
    assert factory.client.entered is True
    assert factory.client.exited is True
    assert receiver.entered is True
    assert receiver.exited is True


def test_render_message_body_uses_deterministic_binary_and_single_line_text() -> None:
    assert render_message_body(b"\\xff\\x00") == "base64:/wA="
    assert render_message_body([b"caf", b"\\xc3\\xa9"]) == "café"
    assert render_message_body("first\nsecond\rthird") == "first\\nsecond\\rthird"
```

- [x] **Step 2: Run the focused tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader.py -v
```

Expected: collection fails because `service_bus_sender.reader` does not exist.

- [x] **Step 3: Implement the minimal peek reader and renderer**

Create `src/service_bus_sender/reader.py` with this implementation. Keep operation assignment immediately before each external operation so its safe error context identifies the failed stage.

```python
from __future__ import annotations

import base64
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol, TextIO

from azure.servicebus import ServiceBusClient

from service_bus_sender.config import Config


@dataclass(frozen=True, slots=True)
class ReadRequest:
    queue_name: str
    count: int
    mode: str


@dataclass(frozen=True, slots=True)
class ReadResult:
    message_count: int


class QueueReadError(RuntimeError):
    def __init__(self, *, operation: str, queue_name: str, cause: BaseException) -> None:
        self.operation = operation
        self.queue_name = queue_name
        self.error_type = type(cause).__name__
        super().__init__(f"{self.error_type} while {operation} queue {queue_name}")


class ReceivedMessage(Protocol):
    @property
    def body(self) -> object: ...


class QueueReceiver(Protocol):
    def peek_messages(self, *, max_message_count: int) -> list[ReceivedMessage]: ...
    def receive_messages(
        self, *, max_message_count: int, max_wait_time: int
    ) -> list[ReceivedMessage]: ...
    def complete_message(self, message: ReceivedMessage) -> None: ...


class ServiceBusClientLike(Protocol):
    def get_queue_receiver(
        self, *, queue_name: str
    ) -> AbstractContextManager[QueueReceiver]: ...


ClientFactory = Callable[[str], AbstractContextManager[ServiceBusClientLike]]


def _default_client_factory(
    connection_string: str,
) -> AbstractContextManager[ServiceBusClientLike]:
    return ServiceBusClient.from_connection_string(connection_string)


def render_message_body(body: object) -> str:
    if isinstance(body, str):
        text = body
    else:
        if isinstance(body, bytes):
            raw = body
        elif isinstance(body, Iterable):
            raw = b"".join(body)
        else:
            raise TypeError("message body is not text or bytes")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"base64:{base64.b64encode(raw).decode('ascii')}"
    return text.replace("\n", "\\n").replace("\r", "\\r")


def read_messages(
    config: Config,
    request: ReadRequest,
    *,
    client_factory: ClientFactory = _default_client_factory,
    stdout: TextIO,
    stderr: TextIO,
) -> ReadResult:
    operation = "creating client"
    try:
        with client_factory(config.connection_string) as client:
            operation = "opening receiver"
            with client.get_queue_receiver(queue_name=request.queue_name) as receiver:
                operation = "peeking messages"
                messages = receiver.peek_messages(max_message_count=request.count)
                for message in messages:
                    operation = "rendering message"
                    rendered = render_message_body(message.body)
                    operation = "writing message"
                    stdout.write(f"{rendered}\n")
    except QueueReadError:
        raise
    except Exception as error:
        raise QueueReadError(
            operation=operation, queue_name=request.queue_name, cause=error
        ) from error

    try:
        stderr.write(f"Read {len(messages)} messages\n")
    except Exception as error:
        raise QueueReadError(
            operation="writing result count", queue_name=request.queue_name, cause=error
        ) from error
    return ReadResult(message_count=len(messages))
```

- [x] **Step 4: Run the focused tests to verify GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader.py -v
```

Expected: PASS; no Azure client is constructed outside `FakeClientFactory`.

- [ ] **Step 5: Conditionally commit the peek work unit**

Run only if Git has been initialized by a maintainer:

```bash
git add src/service_bus_sender/reader.py tests/test_reader.py
git commit -m "feat: add queue peek reader"
```

Expected: one commit containing only the reader module and its tests. Otherwise, skip this command because the working directory is not a Git repository.

### Task 2: Add Block And Drain Semantics

**Files:**
- Modify: `tests/test_reader.py`
- Modify: `src/service_bus_sender/reader.py`

**Work unit:** Block receives messages without any settlement; drain writes each line before completing that exact message.

- [x] **Step 1: Write the failing block and drain behavior tests**

Extend `FakeReceiver.__init__` with `received: list[FakeMessage] | None = None`, store `self.received = received or []`, and make `receive_messages` return `self.received`. Then append:

```python
def test_block_receives_with_exact_parameters_and_never_settles() -> None:
    receiver = FakeReceiver(received=[FakeMessage(b"locked")])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=3, mode="block"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 1
    assert receiver.peek_calls == []
    assert receiver.receive_calls == [(3, 10)]
    assert receiver.completed == []
    assert receiver.abandoned == []
    assert stdout.getvalue() == "locked\n"
    assert stderr.getvalue() == "Read 1 messages\n"


def test_drain_writes_each_message_before_completing_it() -> None:
    first = FakeMessage(b"one")
    second = FakeMessage(b"two")
    receiver = FakeReceiver(received=[first, second])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=2, mode="drain"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 2
    assert receiver.receive_calls == [(2, 10)]
    assert stdout.getvalue() == "one\ntwo\n"
    assert receiver.completed == [first, second]
    assert receiver.abandoned == []
    assert stderr.getvalue() == "Read 2 messages\n"


def test_empty_queue_is_successful() -> None:
    receiver = FakeReceiver(received=[])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=1, mode="drain"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 0
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == "Read 0 messages\n"
```

- [x] **Step 2: Run the new tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest \
  tests/test_reader.py::test_block_receives_with_exact_parameters_and_never_settles \
  tests/test_reader.py::test_drain_writes_each_message_before_completing_it \
  tests/test_reader.py::test_empty_queue_is_successful -v
```

Expected: FAIL because `read_messages` always calls `peek_messages`.

- [x] **Step 3: Extend the reader with the two receive modes and ordered completion**

Replace the message acquisition and loop inside the receiver context in `read_messages` with this exact block:

```python
                if request.mode == "peek":
                    operation = "peeking messages"
                    messages = receiver.peek_messages(max_message_count=request.count)
                else:
                    operation = "receiving messages"
                    messages = receiver.receive_messages(
                        max_message_count=request.count, max_wait_time=10
                    )
                for message in messages:
                    operation = "rendering message"
                    rendered = render_message_body(message.body)
                    operation = "writing message"
                    stdout.write(f"{rendered}\n")
                    if request.mode == "drain":
                        operation = "completing message"
                        receiver.complete_message(message)
```

Do not call `complete_message` or `abandon_message` in `peek` or `block`. Do not move the drain completion above rendering or `stdout.write`.

- [x] **Step 4: Run the focused reader suite to verify GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader.py -v
```

Expected: PASS; `block` and `drain` both call `receive_messages(max_message_count=count, max_wait_time=10)`, but only drain completes messages.

- [ ] **Step 5: Conditionally commit the mode work unit**

Run only if Git has been initialized by a maintainer:

```bash
git add src/service_bus_sender/reader.py tests/test_reader.py
git commit -m "feat: add queue block and drain modes"
```

Expected: one mode-focused commit, or no commit because Git is unavailable.

### Task 3: Prove Failure Safety And Cleanup

**Files:**
- Modify: `tests/test_reader.py`
- Modify: `src/service_bus_sender/reader.py`

**Work unit:** Failures return structured safe context, leave the failing drain message unsettled, preserve earlier completions, and close receiver/client contexts.

- [x] **Step 1: Write failing failure-ordering and safe-error tests**

Add `import pytest` and append these helpers and tests. `FailingStream` fails before retaining the attempted body, proving the reader does not leak it through its diagnostic.

```python
class FailingStream(StringIO):
    def write(self, value: str) -> int:
        raise OSError("Endpoint=sb://secret-marker payload=unprinted-body")


def test_drain_does_not_complete_message_when_stdout_write_fails() -> None:
    first = FakeMessage(b"completed")
    second = FakeMessage(b"unprinted-body")
    receiver = FakeReceiver(received=[first, second])
    factory = FakeClientFactory(FakeClient(receiver))

    with pytest.raises(Exception) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=2, mode="drain"),
            client_factory=factory,
            stdout=FailingStream(),
            stderr=StringIO(),
        )

    assert receiver.completed == []
    assert receiver.abandoned == []
    assert "orders" in str(error.value)
    assert "OSError" in str(error.value)
    assert "secret-marker" not in str(error.value)
    assert "unprinted-body" not in str(error.value)
    assert factory.client.exited is True
    assert receiver.exited is True


def test_drain_preserves_earlier_completion_when_later_completion_fails() -> None:
    first = FakeMessage(b"first")
    second = FakeMessage(b"second")
    receiver = FakeReceiver(received=[first, second])
    original_complete = receiver.complete_message

    def complete_message(message: FakeMessage) -> None:
        if message is second:
            raise RuntimeError("Endpoint=sb://secret-marker payload=second")
        original_complete(message)

    receiver.complete_message = complete_message
    stdout = StringIO()

    with pytest.raises(Exception) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=2, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=stdout,
            stderr=StringIO(),
        )

    assert stdout.getvalue() == "first\nsecond\n"
    assert receiver.completed == [first]
    assert "completing message" in str(error.value)
    assert "RuntimeError" in str(error.value)
    assert "secret-marker" not in str(error.value)
    assert "second" not in str(error.value)


def test_render_failure_does_not_write_or_complete_the_message() -> None:
    message = FakeMessage(object())
    receiver = FakeReceiver(received=[message])
    stdout = StringIO()

    with pytest.raises(Exception) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=stdout,
            stderr=StringIO(),
        )

    assert stdout.getvalue() == ""
    assert receiver.completed == []
    assert "rendering message" in str(error.value)
    assert "TypeError" in str(error.value)
```

- [x] **Step 2: Run the failure tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest \
  tests/test_reader.py::test_drain_does_not_complete_message_when_stdout_write_fails \
  tests/test_reader.py::test_drain_preserves_earlier_completion_when_later_completion_fails \
  tests/test_reader.py::test_render_failure_does_not_write_or_complete_the_message -v
```

Expected: the stdout failure test currently fails its safe-error assertions because raw `OSError` text is exposed.

- [x] **Step 3: Narrow tests to the public read error and preserve cleanup context**

Import `QueueReadError` in `tests/test_reader.py` and replace each `pytest.raises(Exception)` above with `pytest.raises(QueueReadError)`. Then extend the fake receiver and client constructors with optional `exit_failure: Exception | None = None`; in each `__exit__`, set `exited = True` first and raise `exit_failure` when supplied. Append this cleanup test:

```python
def test_receiver_cleanup_failure_is_safe_and_client_still_closes() -> None:
    receiver = FakeReceiver(
        received=[FakeMessage(b"visible")],
        exit_failure=RuntimeError("Endpoint=sb://secret-marker cleanup detail"),
    )
    client = FakeClient(receiver)

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="block"),
            client_factory=FakeClientFactory(client),
            stdout=StringIO(),
            stderr=StringIO(),
        )

    assert receiver.exited is True
    assert client.exited is True
    assert "RuntimeError" in str(error.value)
    assert "secret-marker" not in str(error.value)
    assert "cleanup detail" not in str(error.value)
```

- [x] **Step 4: Implement cleanup-safe operation tracking**

In `read_messages`, set `operation = "closing receiver"` immediately after the message loop and before leaving the receiver `with` block. Set `operation = "closing client"` immediately after that `with` block and before leaving the client `with` block. Retain the existing catch-and-wrap block exactly:

```python
    except QueueReadError:
        raise
    except Exception as error:
        raise QueueReadError(
            operation=operation, queue_name=request.queue_name, cause=error
        ) from error
```

This keeps error text limited to the operation, queue name, and exception type while Python context managers still close the client after any receiver, rendering, output, completion, or receiver-cleanup failure. Do not add exception messages, connection strings, or body values to `QueueReadError`.

- [x] **Step 5: Run all reader tests to verify GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader.py -v
```

Expected: PASS; an already completed first drain message remains completed when a later write or completion fails, and the failing message is not deliberately completed.

- [ ] **Step 6: Conditionally commit the safety work unit**

Run only if Git has been initialized by a maintainer:

```bash
git add src/service_bus_sender/reader.py tests/test_reader.py
git commit -m "test: cover queue reader failure safety"
```

Expected: one safety-focused commit, or no commit because Git is unavailable.

### Task 4: Add The Thin Reader CLI And Validate Arguments First

**Files:**
- Create: `tests/test_reader_cli.py`
- Create: `src/service_bus_sender/reader_cli.py`

**Work unit:** The separate CLI rejects invalid arguments before configuration/client creation and maps every operational failure to exit code 2 with safe stderr diagnostics.

- [x] **Step 1: Write the failing parser and main tests**

Create `tests/test_reader_cli.py`. Keep this test file independent from the non-packaged `tests/` directory by defining the following minimal fakes before its tests:

```python
from contextlib import AbstractContextManager
from io import StringIO
from pathlib import Path

import pytest

from service_bus_sender.config import Config, ConfigError
from service_bus_sender.reader_cli import main, parse_request


class FakeMessage:
    def __init__(self, body: object) -> None:
        self.body = body


class FakeReceiver(AbstractContextManager["FakeReceiver"]):
    def __init__(self) -> None:
        self.received: list[FakeMessage] = []

    def __enter__(self) -> "FakeReceiver":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def peek_messages(self, *, max_message_count: int) -> list[FakeMessage]:
        return self.received

    def receive_messages(
        self, *, max_message_count: int, max_wait_time: int
    ) -> list[FakeMessage]:
        return self.received

    def complete_message(self, message: FakeMessage) -> None:
        return None


class FakeClient(AbstractContextManager["FakeClient"]):
    def __init__(self, receiver: FakeReceiver) -> None:
        self.receiver = receiver

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def get_queue_receiver(self, *, queue_name: str) -> FakeReceiver:
        return self.receiver


class FakeClientFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def __call__(self, connection_string: str) -> FakeClient:
        return self.client


def make_config() -> Config:
    return Config(
        connection_string="test-connection", data_dir=Path("data"), log_level=20
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--count", "1", "--mode", "peek"],
        ["--queue", "   ", "--count", "1", "--mode", "peek"],
        ["--queue", "orders", "--count", "0", "--mode", "peek"],
        ["--queue", "orders", "--count", "one", "--mode", "peek"],
        ["--queue", "orders", "--count", "1", "--mode", "PEEK"],
    ],
)
def test_main_rejects_invalid_required_arguments_before_loading_config(argv: list[str]) -> None:
    config_calls = 0

    def config_loader():
        nonlocal config_calls
        config_calls += 1
        raise AssertionError("configuration must not be loaded")

    stderr = StringIO()
    exit_code = main(argv, config_loader=config_loader, stderr=stderr, stdout=StringIO())

    assert exit_code == 2
    assert config_calls == 0
    assert "argument error" in stderr.getvalue()


def test_parse_request_accepts_only_the_contract_values() -> None:
    assert parse_request(["--queue", "orders", "--count", "2", "--mode", "drain"]).queue_name == "orders"
    assert parse_request(["--queue", "orders", "--count", "2", "--mode", "drain"]).count == 2
    assert parse_request(["--queue", "orders", "--count", "2", "--mode", "drain"]).mode == "drain"


def test_main_returns_two_for_safe_configuration_failure() -> None:
    stderr = StringIO()

    exit_code = main(
        ["--queue", "orders", "--count", "1", "--mode", "peek"],
        config_loader=lambda: (_ for _ in ()).throw(
            ConfigError("Endpoint=sb://secret-marker raw configuration detail")
        ),
        stdout=StringIO(),
        stderr=stderr,
    )

    assert exit_code == 2
    assert "ConfigError while loading configuration" in stderr.getvalue()
    assert "secret-marker" not in stderr.getvalue()
    assert "raw configuration detail" not in stderr.getvalue()
```

- [x] **Step 2: Run the CLI tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader_cli.py -v
```

Expected: collection fails because `service_bus_sender.reader_cli` does not exist.

- [x] **Step 3: Implement parser and safe CLI delegation**

Create `src/service_bus_sender/reader_cli.py`:

```python
from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from typing import TextIO

from service_bus_sender.config import Config, load_config
from service_bus_sender.reader import (
    ClientFactory,
    QueueReadError,
    ReadRequest,
    _default_client_factory,
    read_messages,
)


class ArgumentParseError(ValueError):
    pass


class ReaderArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ArgumentParseError(message)


ConfigLoader = Callable[[], Config]


def _non_empty_queue(value: str) -> str:
    queue_name = value.strip()
    if not queue_name:
        raise argparse.ArgumentTypeError("--queue must be non-empty")
    return queue_name


def _positive_integer(value: str) -> int:
    try:
        count = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--count must be an integer") from error
    if count <= 0:
        raise argparse.ArgumentTypeError("--count must be greater than zero")
    return count


def parse_request(argv: Sequence[str]) -> ReadRequest:
    parser = ReaderArgumentParser(prog="service-bus-read", add_help=True)
    parser.add_argument("--queue", required=True, type=_non_empty_queue)
    parser.add_argument("--count", required=True, type=_positive_integer)
    parser.add_argument("--mode", required=True, choices=("peek", "block", "drain"))
    namespace = parser.parse_args(argv)
    return ReadRequest(
        queue_name=namespace.queue, count=namespace.count, mode=namespace.mode
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    config_loader: ConfigLoader = load_config,
    client_factory: ClientFactory = _default_client_factory,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    try:
        request = parse_request(sys.argv[1:] if argv is None else argv)
    except ArgumentParseError as error:
        stderr.write(f"argument error: {error}\n")
        return 2

    try:
        config = config_loader()
    except Exception as error:
        stderr.write(f"{type(error).__name__} while loading configuration\n")
        return 2

    try:
        read_messages(
            config,
            request,
            client_factory=client_factory,
            stdout=stdout,
            stderr=stderr,
        )
    except QueueReadError as error:
        stderr.write(
            f"{error.error_type} while {error.operation} queue {error.queue_name}\n"
        )
        return 2
    return 0
```

- [x] **Step 4: Run parser and configuration tests to verify GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader_cli.py -v
```

Expected: PASS; every invalid argument returns 2 before configuration/client construction and configuration diagnostics contain only the exception type plus operation.


- [x] **Step 5: Add operational error integration tests and verify every exit-2 boundary**

Append this test after choosing local fakes as described in Step 1:

```python
def test_main_returns_two_for_receiver_failure_without_leaking_body_or_connection() -> None:
    receiver = FakeReceiver()
    receiver.received = [FakeMessage(b"unprinted-body")]

    def failing_receive(*, max_message_count: int, max_wait_time: int):
        raise RuntimeError("Endpoint=sb://secret-marker payload=unprinted-body")

    receiver.receive_messages = failing_receive
    stderr = StringIO()
    exit_code = main(
        ["--queue", "orders", "--count", "1", "--mode", "block"],
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(receiver)),
        stdout=StringIO(),
        stderr=stderr,
    )

    assert exit_code == 2
    assert "RuntimeError while receiving messages queue orders" in stderr.getvalue()
    assert "secret-marker" not in stderr.getvalue()
    assert "unprinted-body" not in stderr.getvalue()


class FailingStream(StringIO):
    def write(self, value: str) -> int:
        raise OSError("Endpoint=sb://secret-marker payload=unprinted-body")


def test_main_returns_two_for_client_render_write_and_settlement_failures() -> None:
    arguments = ["--queue", "orders", "--count", "1", "--mode", "drain"]

    def failing_client_factory(connection_string: str):
        raise RuntimeError("Endpoint=sb://secret-marker client detail")

    client_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=failing_client_factory,
        stdout=StringIO(),
        stderr=client_error,
    ) == 2
    assert "RuntimeError while creating client queue orders" in client_error.getvalue()
    assert "secret-marker" not in client_error.getvalue()

    render_receiver = FakeReceiver()
    render_receiver.received = [FakeMessage(object())]
    render_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(render_receiver)),
        stdout=StringIO(),
        stderr=render_error,
    ) == 2
    assert "TypeError while rendering message queue orders" in render_error.getvalue()

    write_receiver = FakeReceiver()
    write_receiver.received = [FakeMessage(b"unprinted-body")]
    write_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(write_receiver)),
        stdout=FailingStream(),
        stderr=write_error,
    ) == 2
    assert "OSError while writing message queue orders" in write_error.getvalue()
    assert "unprinted-body" not in write_error.getvalue()

    settlement_receiver = FakeReceiver()
    settlement_receiver.received = [FakeMessage(b"visible")]

    def failing_complete(message: FakeMessage) -> None:
        raise RuntimeError("Endpoint=sb://secret-marker settlement detail")

    settlement_receiver.complete_message = failing_complete
    settlement_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(settlement_receiver)),
        stdout=StringIO(),
        stderr=settlement_error,
    ) == 2
    assert "RuntimeError while completing message queue orders" in settlement_error.getvalue()
    assert "secret-marker" not in settlement_error.getvalue()
```

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_reader_cli.py -v
```

Expected: PASS; parser, configuration, client/receiver, rendering, writing, and settlement errors all follow the same exit-2, safe-diagnostic path.

- [ ] **Step 6: Conditionally commit the CLI work unit**

Run only if Git has been initialized by a maintainer:

```bash
git add src/service_bus_sender/reader_cli.py tests/test_reader_cli.py
git commit -m "feat: add queue reader command"
```

Expected: one CLI-focused commit, or no commit because Git is unavailable.

### Task 5: Register And Document The Reader Command

**Files:**
- Modify: `pyproject.toml`
- Modify: `README.md`
- Modify: `tests/test_reader_cli.py`

**Work unit:** Poetry exposes both commands and the README makes destructive behavior, output channels, and no-Azure test guarantees explicit.

- [x] **Step 1: Write the failing entry-point and README contract tests**

Append to `tests/test_reader_cli.py`:

```python
from pathlib import Path


def test_project_registers_reader_without_replacing_sender_entry_point() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pyproject = project_root.joinpath("pyproject.toml").read_text(encoding="utf-8")

    assert 'service-bus-send = "service_bus_sender.cli:main"' in pyproject
    assert 'service-bus-read = "service_bus_sender.reader_cli:main"' in pyproject


def test_readme_documents_all_reader_modes_and_conda_command() -> None:
    project_root = Path(__file__).resolve().parents[1]
    readme = project_root.joinpath("README.md").read_text(encoding="utf-8")

    assert "service-bus-read" in readme
    assert "conda run -n tools-service-bus poetry run service-bus-read" in readme
    assert "peek" in readme
    assert "block" in readme
    assert "drain" in readme
    assert "destructive" in readme
    assert "standard error" in readme
```

- [x] **Step 2: Run the documentation/entry-point tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest \
  tests/test_reader_cli.py::test_project_registers_reader_without_replacing_sender_entry_point \
  tests/test_reader_cli.py::test_readme_documents_all_reader_modes_and_conda_command -v
```

Expected: FAIL because the reader entry point and documentation are absent.

- [x] **Step 3: Register the CLI without changing dependencies or sender mapping**

Modify the existing `[project.scripts]` section in `pyproject.toml` to exactly:

```toml
[project.scripts]
service-bus-send = "service_bus_sender.cli:main"
service-bus-read = "service_bus_sender.reader_cli:main"
```

Do not modify the dependency list or `poetry.lock`.

- [x] **Step 4: Add a scan-friendly Queue Reader README section**

Insert this section after the current `## Run` sender section and before `## Exit Codes`:

```markdown
## Queue Reader

Read one queue with all arguments required:

```bash
conda run -n tools-service-bus poetry run service-bus-read \
  --queue orders \
  --count 10 \
  --mode peek
```

| Mode | Azure operation | Settlement |
| --- | --- | --- |
| `peek` | `peek_messages(max_message_count=count)` | None; messages are not locked or removed. |
| `block` | `receive_messages(max_message_count=count, max_wait_time=10)` | None; returned messages remain locked until Azure releases the lock. |
| `drain` | `receive_messages(max_message_count=count, max_wait_time=10)` | Each message is completed only after its body was successfully written. This mode is destructive. |

`--queue` must be non-empty, `--count` must be a positive integer, and `--mode` is exactly `peek`, `block`, or `drain`. The command writes one rendered message body per standard-output line and writes `Read N messages` only to standard error. UTF-8 bodies render as text; binary bodies render deterministically as `base64:<payload>`. An empty queue succeeds with no standard-output bodies and `Read 0 messages` on standard error.

If rendering, output, or completion fails in `drain`, the failing message is not deliberately completed. Earlier messages completed before that failure remain removed. Errors exit with code `2` and identify only the operation, queue, and exception type; they never print connection strings, raw exception details, or bodies that were not printed.
```

- [x] **Step 5: Run the focused documentation tests to verify GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest \
  tests/test_reader_cli.py::test_project_registers_reader_without_replacing_sender_entry_point \
  tests/test_reader_cli.py::test_readme_documents_all_reader_modes_and_conda_command -v
```

Expected: PASS; both entry points remain visible and the README contains the reader contract.

- [ ] **Step 6: Conditionally commit the distribution/documentation work unit**

Run only if Git has been initialized by a maintainer:

```bash
git add pyproject.toml README.md tests/test_reader_cli.py
git commit -m "docs: document queue reader modes"
```

Expected: one documentation/entry-point commit, or no commit because Git is unavailable.

### Task 6: Full Offline Regression And Plan Self-Review

**Files:**
- Verify: `src/service_bus_sender/cli.py`
- Verify: `src/service_bus_sender/reader.py`
- Verify: `src/service_bus_sender/reader_cli.py`
- Verify: `tests/test_cli.py`
- Verify: `tests/test_reader.py`
- Verify: `tests/test_reader_cli.py`
- Verify: `pyproject.toml`
- Verify: `README.md`

**Work unit:** Demonstrate that reader behavior is complete, sender behavior remains covered, documentation is consistent, and no live Azure dependency exists in tests.

- [x] **Step 1: Run the complete offline suite**

Run:

```bash
conda run -n tools-service-bus poetry run pytest -v
```

Expected: PASS. Existing sender tests remain unchanged and pass; reader tests use only fake clients, receivers, messages, and streams.

- [x] **Step 2: Validate Poetry metadata without installing or updating packages**

Run:

```bash
conda run -n tools-service-bus poetry check
```

Expected: `All set!`; `service-bus-send` and `service-bus-read` resolve as project scripts and no dependency change is required.

- [x] **Step 3: Perform the required plan self-review**

Review the plan and final code against this checklist:

```markdown
- [ ] Parser tests reject missing/blank queue, non-positive/non-integer count, and non-exact modes before configuration or client creation.
- [ ] `peek_messages(max_message_count=count)` is used only for `peek`; no mode settles or abandons peeked messages.
- [ ] `receive_messages(max_message_count=count, max_wait_time=10)` is used for both `block` and `drain`.
- [ ] `block` never calls completion or abandonment; `drain` writes before completing each message and preserves earlier completions after a later failure.
- [ ] Empty reads succeed; stdout contains bodies only, one physical line each; the final count is written only to stderr.
- [ ] UTF-8 and non-decodable binary rendering are tested; rendering/output/settlement/client/receiver/configuration errors return 2 with operation, queue, and exception type only.
- [ ] Client and receiver contexts close on success and failure; neither diagnostics nor tests expose connection strings, raw exception text, or unprinted bodies.
- [ ] `service-bus-send` still maps to `service_bus_sender.cli:main`; no existing sender source or tests changed.
- [ ] `service-bus-read` maps to `service_bus_sender.reader_cli:main`; README documents command, arguments, modes, output, drain risk, and tests.
- [ ] The plan and code contain no unresolved planning markers, invalid type aliases, or command missing the `conda run -n tools-service-bus` prefix.
```

- [ ] **Step 4: Conditionally commit the verified complete feature**

Run only if Git has been initialized by a maintainer and all verification steps pass:

```bash
git add src/service_bus_sender/reader.py src/service_bus_sender/reader_cli.py \
  tests/test_reader.py tests/test_reader_cli.py pyproject.toml README.md
git commit -m "feat: add Azure Service Bus queue reader"
```

Expected: no commit in the current directory because it is not a Git repository; otherwise, the commit contains only the completed queue-reader feature.

## Plan Self-Review Result

| Check | Result |
| --- | --- |
| Approved requirement coverage | Complete across Tasks 1-6. |
| RED/GREEN vertical slices | Each behavior introduces a failing test, focused command, minimal implementation, and passing command before the next behavior. |
| Exact paths and stable names | Defined in the file map and stable-interface section; all later tasks use `ReadRequest`, `ReadResult`, `QueueReadError`, `read_messages`, and `parse_request`. |
| Placeholder scan | No omitted test fixture, unresolved type, or deferred implementation marker remains. |
| Command wrapping | Every executable command is prefixed with `conda run -n tools-service-bus`. |
| Readability | Outcome-first header, responsibility table, acceptance map, short work units, and verification checklist support direct review. |

## Risks And Decisions

- `block` intentionally leaves messages locked and does not abandon them. This is an explicit operational choice, not a retry or recovery mechanism.
- `drain` is at-least-once at the operator boundary: a failure after writing but before completion can cause a later reread; a failure after completing an earlier message leaves that earlier message removed.
- Existing `load_config` also validates sender-oriented `SERVICE_BUS_DATA_DIR`. The reader reuses it unchanged to preserve the existing configuration boundary; a missing or unreadable configured data directory therefore remains a configuration failure for both commands.
- Azure Service Bus may expose DATA bodies as `bytes` or iterable byte chunks. The reader handles both; unsupported VALUE or SEQUENCE forms fail safely with exit code 2 rather than guessing at a body representation.
