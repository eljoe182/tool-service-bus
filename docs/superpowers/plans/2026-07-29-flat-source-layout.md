# Flat Source Packages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `service_bus_sender` with independently navigable `shared`, `sender`, and `reader` packages without changing the send/read operational contract.

**Architecture:** `shared` owns configuration validation and Azure-facing protocols/factory; it has no file-discovery, batching, read-mode, or argument-parsing logic. `sender` owns input-envelope discovery, serialization, batching, aggregate send orchestration, and its console command. `reader` owns request parsing, rendering, receiving, and settlement. The migration is performed as vertical TDD slices, retaining behavior tests while changing only their imports and configuration fixtures.

**Tech Stack:** Python 3.11, Poetry 2, pytest 8, python-dotenv, azure-servicebus 7, Conda environment `tools-service-bus`.

---

## Quick Path

1. Establish `shared` configuration and client boundaries with focused RED-GREEN tests.
2. Migrate sender and reader behavior, including every existing offline regression, into their target package tests.
3. Switch Poetry scripts and package declarations, remove every legacy module/test, reinstall, then prove entry points and the full suite.

## Responsibilities And Stable Interfaces

| Area | Owns | Stable public symbols and signatures | Must not import |
| --- | --- | --- | --- |
| `shared.config` | `.env` loading, connection/log validation, sender data-directory validation | `ConfigError`; `ReaderConfig(connection_string: str, log_level: int)`; `SenderConfig(connection_string: str, data_dir: Path, log_level: int)`; `load_reader_config(*, cwd: Path | None = None) -> ReaderConfig`; `load_sender_config(*, cwd: Path | None = None) -> SenderConfig` | `sender`, `reader` |
| `shared.client` | Azure factory and structural client/message protocols | `MessageBatch`, `QueueSender`, `ReceivedMessage`, `QueueReceiver`, `ServiceBusClientLike`, `ClientFactory`, `default_client_factory(connection_string: str) -> AbstractContextManager[ServiceBusClientLike]` | `sender`, `reader` |
| `sender.files` | Sender JSON envelope contract | `ApplicationProperty`, `MessageEnvelope`, `InputFileError`, `discover_json_files`, `derive_queue_name`, `load_message_envelope` | `reader` |
| `sender.service` | Compact serialization and dynamic Service Bus batches | `FileSendError`, `MessageFactory`, `send_objects(sender, objects, properties, *, message_factory=_create_service_bus_message) -> int` | `reader` |
| `sender.cli` | Sender run summary, orchestration, logging, help, exit codes | `RunSummary`, `format_summary`, `run(config: SenderConfig, *, client_factory=default_client_factory, logger=_LOGGER) -> RunSummary`, `main(argv: Sequence[str] | None = None, *, config_loader=load_sender_config, client_factory=default_client_factory, stdout=sys.stdout) -> int` | `reader` |
| `reader.service` | Body rendering, receive modes, settlement | `ReadRequest`, `ReadResult`, `QueueReadError`, `render_message_body`, `read_messages(config: ReaderConfig, request: ReadRequest, *, client_factory=default_client_factory, stdout: TextIO, stderr: TextIO) -> ReadResult` | `sender` |
| `reader.cli` | Reader arguments and process-level errors | `ArgumentParseError`, `ReaderArgumentParser`, `parse_request`, `main(argv: Sequence[str] | None = None, *, config_loader=load_reader_config, client_factory=default_client_factory, stdout=sys.stdout, stderr=sys.stderr) -> int` | `sender` |

## Exact Migration Map

| Current path | Destination | Required change |
| --- | --- | --- |
| `src/service_bus_sender/config.py` | `src/shared/config.py` | Split the old `Config` and `load_config` into reader and sender configurations; only sender resolves/validates `SERVICE_BUS_DATA_DIR`. |
| Duplicated protocols/factories in `cli.py`, `reader.py` | `src/shared/client.py` | Define the shared client/message protocols and one `default_client_factory`. |
| `src/service_bus_sender/files.py` | `src/sender/files.py` | Relocate unchanged behavior and types. |
| `src/service_bus_sender/sender.py` | `src/sender/service.py` | Relocate batching behavior; consume `QueueSender` from `shared.client`. |
| `src/service_bus_sender/cli.py` | `src/sender/cli.py` | Use `SenderConfig`, `load_sender_config`, and shared client factory. Add the required safe `--help` branch without contacting Azure. |
| `src/service_bus_sender/reader.py` | `src/reader/service.py` | Use `ReaderConfig` and shared client protocols/factory; retain body/render/settlement logic unchanged. |
| `src/service_bus_sender/reader_cli.py` | `src/reader/cli.py` | Use `load_reader_config` and reader service imports. |
| `tests/test_config.py` | `tests/shared/test_config.py` | Retain all settings validation plus the reader-without-data-directory regression. |
| `tests/test_files.py`, `tests/test_sender.py`, `tests/test_cli.py` | `tests/sender/test_files.py`, `tests/sender/test_service.py`, `tests/sender/test_cli.py` | Retain every sender behavior test with only target imports/config fixture updates; add sender help test. |
| `tests/test_reader.py`, `tests/test_reader_cli.py` | `tests/reader/test_service.py`, `tests/reader/test_cli.py` | Retain every reader behavior test with only target imports/config fixture updates. |

No compatibility modules, import aliases, dependency changes, or live Azure calls are permitted. The intentional top-level package collision trade-off is accepted by the design.

### Task 1: Establish Shared Configuration

**Files:**
- Create: `src/shared/__init__.py`
- Create: `src/shared/config.py`
- Create: `tests/shared/test_config.py`
- Delete: `tests/test_config.py` after its assertions are represented in the new test file

- [x] **Step 1: Write the reader configuration regression before creating `shared.config`.**

```python
from shared.config import ReaderConfig, load_reader_config
import logging


def test_load_reader_config_does_not_require_service_bus_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "reader-connection")
    monkeypatch.delenv("SERVICE_BUS_DATA_DIR", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    config = load_reader_config(cwd=tmp_path)

    assert config == ReaderConfig(connection_string="reader-connection", log_level=logging.INFO)
```

- [x] **Step 2: Run the focused reader test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/shared/test_config.py::test_load_reader_config_does_not_require_service_bus_data_dir -v`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'shared'`.

- [x] **Step 3: Implement the minimal shared configuration split.**

```python
# src/shared/config.py
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class ConfigError(ValueError):
    """Raised when startup configuration is invalid."""


@dataclass(frozen=True, slots=True)
class ReaderConfig:
    connection_string: str
    log_level: int


@dataclass(frozen=True, slots=True)
class SenderConfig:
    connection_string: str
    data_dir: Path
    log_level: int


def _load_base_config(*, cwd: Path | None) -> tuple[Path, str, int]:
    base_dir = (cwd or Path.cwd()).resolve()
    load_dotenv(dotenv_path=base_dir / ".env", override=False)
    connection_string = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "").strip()
    if not connection_string:
        raise ConfigError("AZURE_SERVICE_BUS_CONNECTION_STRING is required")
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    try:
        log_level = _LOG_LEVELS[log_level_name]
    except KeyError as error:
        raise ConfigError(
            "LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
        ) from error
    return base_dir, connection_string, log_level


def load_reader_config(*, cwd: Path | None = None) -> ReaderConfig:
    _, connection_string, log_level = _load_base_config(cwd=cwd)
    return ReaderConfig(connection_string=connection_string, log_level=log_level)


def load_sender_config(*, cwd: Path | None = None) -> SenderConfig:
    base_dir, connection_string, log_level = _load_base_config(cwd=cwd)
    configured_data_dir = Path(os.getenv("SERVICE_BUS_DATA_DIR", "data"))
    data_dir = (configured_data_dir if configured_data_dir.is_absolute() else base_dir / configured_data_dir).resolve()
    if not data_dir.is_dir() or not os.access(data_dir, os.R_OK):
        raise ConfigError("SERVICE_BUS_DATA_DIR must be a readable directory")
    return SenderConfig(
        connection_string=connection_string, data_dir=data_dir, log_level=log_level
    )
```

Create empty `src/shared/__init__.py`. Keep `_load_base_config` private; neither operation may access another operation's configuration requirements.

- [x] **Step 4: Run the focused reader regression to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/shared/test_config.py::test_load_reader_config_does_not_require_service_bus_data_dir -v`

Expected: PASS; no directory is created or required.

- [x] **Step 5: Migrate the remaining configuration tests as behavior tests.**

Move all existing dotenv precedence, empty-connection, safe-invalid-log-level, and parameterized unreadable-directory assertions into `tests/shared/test_config.py`. Replace old imports with `shared.config`; use `load_sender_config` and `SenderConfig` for the two data-directory tests, and use `load_reader_config` and `ReaderConfig` for connection/log tests. Add this sender-specific regression alongside the existing parameterized directory test:

```python
def test_load_sender_config_still_requires_a_readable_data_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "sender-connection")
    monkeypatch.setenv("SERVICE_BUS_DATA_DIR", str(tmp_path / "missing"))

    with pytest.raises(ConfigError, match="SERVICE_BUS_DATA_DIR"):
        load_sender_config(cwd=tmp_path)
```

- [x] **Step 6: Run the shared configuration test file to verify all migrated behavior is green.**

Run: `conda run -n tools-service-bus poetry run pytest tests/shared/test_config.py -v`

Expected: PASS, including both the reader-no-data-directory and sender-requires-directory regressions.

- [x] **Step 7: Delete the superseded root configuration test only after GREEN.**

Delete `tests/test_config.py`; do not delete `src/service_bus_sender/config.py` until all source callers have moved in Task 7.

- [x] **Step 8: Conditionally commit this self-contained shared configuration work unit.**

Run only if `.git` exists:

```bash
git add src/shared/__init__.py src/shared/config.py tests/shared/test_config.py tests/test_config.py
git commit -m "feat: split service bus configuration by operation"
```

Verification recorded by this work unit: `tests/shared/test_config.py` passes. Runtime harness: N/A; configuration loading is covered through its public loaders and no live Azure boundary exists. Rollback boundary: remove `src/shared/config.py` and `tests/shared/test_config.py` together.

### Task 2: Extract Shared Azure Client Contracts

**Files:**
- Create: `src/shared/client.py`
- Create: `tests/shared/test_client.py`

- [x] **Step 1: Write a structural factory test before implementing the shared factory.**

```python
from shared.client import default_client_factory


def test_default_client_factory_delegates_connection_string(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "shared.client.ServiceBusClient.from_connection_string",
        lambda connection_string: calls.append(connection_string) or object(),
    )

    assert default_client_factory("safe-connection") is not None
    assert calls == ["safe-connection"]
```

- [x] **Step 2: Run the focused client test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/shared/test_client.py::test_default_client_factory_delegates_connection_string -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.client'`.

- [x] **Step 3: Implement the common protocols and one SDK factory.**

```python
# src/shared/client.py
from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Protocol

from azure.servicebus import ServiceBusClient


class MessageBatch(Protocol):
    def add_message(self, message: object) -> None:
        raise NotImplementedError


class QueueSender(Protocol):
    def create_message_batch(self) -> MessageBatch:
        raise NotImplementedError

    def send_messages(self, batch: MessageBatch) -> None:
        raise NotImplementedError


class ReceivedMessage(Protocol):
    @property
    def body(self) -> object:
        raise NotImplementedError


class QueueReceiver(Protocol):
    def peek_messages(self, *, max_message_count: int) -> list[ReceivedMessage]:
        raise NotImplementedError

    def receive_messages(self, *, max_message_count: int, max_wait_time: int) -> list[ReceivedMessage]:
        raise NotImplementedError

    def complete_message(self, message: ReceivedMessage) -> None:
        raise NotImplementedError


class ServiceBusClientLike(Protocol):
    def get_queue_sender(self, queue_name: str) -> AbstractContextManager[QueueSender]:
        raise NotImplementedError

    def get_queue_receiver(self, *, queue_name: str) -> AbstractContextManager[QueueReceiver]:
        raise NotImplementedError


ClientFactory = Callable[[str], AbstractContextManager[ServiceBusClientLike]]


def default_client_factory(connection_string: str) -> AbstractContextManager[ServiceBusClientLike]:
    return ServiceBusClient.from_connection_string(connection_string)
```

- [x] **Step 4: Run the client test to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/shared/test_client.py -v`

Expected: PASS without opening a network connection.

- [x] **Step 5: Conditionally commit the shared client boundary.**

Run only if `.git` exists:

```bash
git add src/shared/client.py tests/shared/test_client.py
git commit -m "feat: share Azure client contracts"
```

Verification recorded by this work unit: `tests/shared/test_client.py` passes. Runtime harness: N/A; the factory is monkeypatched and no Azure operation is invoked. Rollback boundary: remove only `src/shared/client.py` and its test.

### Task 3: Migrate Sender Envelope And Batch Service

**Files:**
- Create: `src/sender/__init__.py`
- Create: `src/sender/files.py`
- Create: `src/sender/service.py`
- Create: `tests/sender/test_files.py`
- Create: `tests/sender/test_service.py`
- Delete: `tests/test_files.py`
- Delete: `tests/test_sender.py`

- [x] **Step 1: Copy the existing envelope tests to their target path and change only imports.**

At the top of `tests/sender/test_files.py`, replace the old import with:

```python
from sender.files import (
    InputFileError,
    derive_queue_name,
    discover_json_files,
    load_message_envelope,
)
```

Retain every existing test and assertion, including exact suffix ordering, symlink rejection, primitive property validation, non-finite recursive validation, safe UTF-8/OS errors, and the canonical `data/orders.json` sample.

- [x] **Step 2: Run one envelope test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_files.py::test_load_message_envelope_returns_properties_and_data_objects -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'sender'`.

- [x] **Step 3: Create `sender.files` by moving the complete existing implementation unchanged.**

Move the contents of `src/service_bus_sender/files.py` to `src/sender/files.py` without behavioral edits. The target module must retain these exact definitions:

```python
ApplicationProperty = str | int | float | bool | None

@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    properties: dict[str, ApplicationProperty]
    data: list[dict[str, object]]

class InputFileError(ValueError):
    """Raised when an input file violates the JSON input contract."""
```

Create empty `src/sender/__init__.py`. Preserve the current private `_contains_non_finite_number` and `_validate_properties` logic exactly, including safe error strings.

- [x] **Step 4: Run all envelope tests to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_files.py -v`

Expected: PASS with the same envelope behavior as the baseline.

- [x] **Step 5: Copy sender batching tests to their target path and change only public imports.**

At the top of `tests/sender/test_service.py`, use:

```python
from sender.service import FileSendError, send_objects
```

Retain all eight existing tests, fake batch/sender behavior, confirmed-count assertions, compact UTF-8 serialization, property-copy assertions, rollover behavior, oversized-message behavior, and secret/payload-safe errors.

- [x] **Step 6: Run one sender-service test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_service.py::test_send_objects_serializes_bodies_and_copies_properties_per_message -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'sender.service'`.

- [x] **Step 7: Create `sender.service` with the existing batching algorithm and the shared sender protocol.**

```python
# imports that differ from the old module
from shared.client import MessageBatch, QueueSender
from sender.files import ApplicationProperty

MessageFactory = Callable[[str, dict[str, ApplicationProperty]], object]

class FileSendError(RuntimeError):
    def __init__(
        self,
        *,
        sent_count: int,
        batch_number: int,
        operation: str,
        cause: BaseException,
    ) -> None:
        self.sent_count = sent_count
        self.batch_number = batch_number
        self.operation = operation
        self.error_type = type(cause).__name__
        super().__init__(
            f"{self.error_type} while {self.operation} batch {self.batch_number}"
        )
```

Move the complete existing `_create_batch`, `_send_batch`, `_create_service_bus_message`, and `send_objects` bodies unchanged. In particular, create the initial empty batch, avoid sending it, serialize with `separators=(",", ":")` and `ensure_ascii=False`, flush a full batch after `MessageSizeExceededError`, and propagate `FileSendError` with the confirmed count and safe error type.

- [x] **Step 8: Run all sender envelope and batching tests to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_files.py tests/sender/test_service.py -v`

Expected: PASS; no Azure client is constructed.

- [x] **Step 9: Remove the legacy sender-only tests after their target replacements pass.**

Delete `tests/test_files.py` and `tests/test_sender.py`. Keep legacy source modules until Task 7 so no remaining caller breaks mid-migration.

- [x] **Step 10: Conditionally commit the envelope and batching work unit.**

Run only if `.git` exists:

```bash
git add src/sender/__init__.py src/sender/files.py src/sender/service.py tests/sender/test_files.py tests/sender/test_service.py tests/test_files.py tests/test_sender.py
git commit -m "feat: move sender envelope and batching services"
```

Verification recorded by this work unit: sender envelope and batching test files pass. Runtime harness: N/A; all Service Bus types are replaced by fakes. Rollback boundary: remove `src/sender/files.py`, `src/sender/service.py`, and their corresponding tests together.

### Task 4: Migrate Sender Orchestration And Safe Help

**Files:**
- Create: `src/sender/cli.py`
- Create: `tests/sender/test_cli.py`
- Delete: `tests/test_cli.py`

- [x] **Step 1: Copy the sender CLI tests to their target path and update stable imports/config fixtures.**

Replace imports with:

```python
from sender.cli import RunSummary, format_summary, main, run
from shared.config import ConfigError, SenderConfig
```

Change `make_config` to return `SenderConfig`. Retain every existing send orchestration regression: sorted files, one client, untouched files, empty files/directories, complete pre-sender validation, continuation after failures, partial confirmed counts, cleanup handling, summary text, safe logging, and exit codes.

- [x] **Step 2: Add the required safe sender-help regression and run it RED.**

```python
def test_main_prints_help_without_loading_configuration() -> None:
    stdout = StringIO()
    config_calls = 0

    def config_loader() -> SenderConfig:
        nonlocal config_calls
        config_calls += 1
        raise AssertionError("configuration must not load for help")

    assert main(["--help"], config_loader=config_loader, stdout=stdout) == 0
    assert config_calls == 0
    assert "usage: service-bus-send" in stdout.getvalue()
```

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_main_prints_help_without_loading_configuration -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'sender.cli'`.

- [x] **Step 3: Implement `sender.cli` by relocating the sender run behavior and adding only the required help branch.**

```python
from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TextIO

from shared.client import ClientFactory, default_client_factory
from shared.config import SenderConfig, load_sender_config
from sender.files import InputFileError, derive_queue_name, discover_json_files, load_message_envelope
from sender.service import FileSendError, send_objects

ConfigLoader = Callable[[], SenderConfig]

def main(
    argv: Sequence[str] | None = None,
    *,
    config_loader: ConfigLoader = load_sender_config,
    client_factory: ClientFactory = default_client_factory,
    stdout: TextIO = sys.stdout,
) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if arguments == ["--help"]:
        stdout.write("usage: service-bus-send\\n")
        return 0
    # Preserve the existing configuration, logging, run, summary, and 0/1/2 behavior.
```

Move `RunSummary`, `format_summary`, `run`, `_configure_logging`, and the normal `main` error paths from the old CLI unchanged except for the imports and `SenderConfig` type. `run` must still open exactly one client, validate a full envelope before opening its sender, continue per-file after errors, preserve a primary send failure over cleanup failure, count confirmed partial sends, and log only file name, queue, exception class, operation, batch, and confirmed count. Update every existing direct call to `main` in this test file to pass `[]` as the first argument so pytest arguments are never interpreted as console arguments.

- [x] **Step 4: Run the targeted sender CLI tests to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py -v`

Expected: PASS, including safe `--help`, existing summaries, errors, and exit codes.

- [x] **Step 5: Delete the old sender CLI tests only after all sender CLI regressions pass.**

Delete `tests/test_cli.py`. Keep `src/service_bus_sender/cli.py` until Task 7 removes the old package atomically.

- [x] **Step 6: Conditionally commit the sender orchestration work unit.**

Run only if `.git` exists:

```bash
git add src/sender/cli.py tests/sender/test_cli.py tests/test_cli.py
git commit -m "feat: move sender command into dedicated package"
```

Verification recorded by this work unit: `tests/sender/test_cli.py` passes. Runtime harness: N/A; fake clients/senders cover the public `run` and `main` paths without Azure. Rollback boundary: remove `src/sender/cli.py` and `tests/sender/test_cli.py` together.

### Task 5: Migrate Reader Service

**Files:**
- Create: `src/reader/__init__.py`
- Create: `src/reader/service.py`
- Create: `tests/reader/test_service.py`
- Delete: `tests/test_reader.py`

- [x] **Step 1: Copy all reader service tests to their target path and make only import/config fixture changes.**

Use these imports and fixture return type:

```python
from reader.service import QueueReadError, ReadRequest, read_messages, render_message_body
from shared.config import ReaderConfig

def make_config() -> ReaderConfig:
    return ReaderConfig(connection_string="test-connection", log_level=20)
```

Keep every existing observable behavior assertion: peek/block/drain operations, no settlement outside drain, binary base64 rendering, escaped one-line text, empty queue, write/flush/render/complete failures, prior completion preservation, safe errors, and receiver/client cleanup.

- [x] **Step 2: Run the drain ordering test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py::test_drain_writes_each_message_before_completing_it -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'reader'`.

- [x] **Step 3: Create `reader.service` using reader configuration and shared client contracts.**

```python
# imports that differ from the old module
from shared.client import ClientFactory, QueueReceiver, ReceivedMessage, default_client_factory
from shared.config import ReaderConfig

```

Move the entire existing `ReadRequest`, `ReadResult`, `QueueReadError`, `render_message_body`, and `read_messages` behavior into this file. Preserve the exact receive parameters (`max_wait_time=10`), operation labels, safe wrapped error type, stdout write then flush then completion order for drain, and stderr count only after the client/receiver contexts close successfully. Create empty `src/reader/__init__.py`.

- [x] **Step 4: Run reader service behavior tests to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py -v`

Expected: PASS; drain still flushes visible output before settlement and never deliberately completes a failing message.

- [x] **Step 5: Delete the legacy reader service test after GREEN.**

Delete `tests/test_reader.py`; retain `src/service_bus_sender/reader.py` until Task 7.

- [x] **Step 6: Conditionally commit the reader service work unit.**

Run only if `.git` exists:

```bash
git add src/reader/__init__.py src/reader/service.py tests/reader/test_service.py tests/test_reader.py
git commit -m "feat: move queue reading service into reader package"
```

Verification recorded by this work unit: `tests/reader/test_service.py` passes. Runtime harness: N/A; fakes cover receiver settlement and stream failures without live Azure. Rollback boundary: remove reader service and its test together.

### Task 6: Migrate Reader CLI

**Files:**
- Create: `src/reader/cli.py`
- Create: `tests/reader/test_cli.py`
- Delete: `tests/test_reader_cli.py`

- [x] **Step 1: Copy reader CLI tests to their target location and use reader/shared imports.**

```python
from reader.cli import main, parse_request
from shared.config import ConfigError, ReaderConfig

def make_config() -> ReaderConfig:
    return ReaderConfig(connection_string="test-connection", log_level=20)
```

Retain all argument-before-config, exact mode/value, safe startup/read/render/write/settlement error, and exit-code tests. Do not carry the old packaging assertion yet; replace it in Task 7 after `pyproject.toml` changes.

- [x] **Step 2: Run the argument-validation test to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py::test_parse_request_accepts_only_the_contract_values -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'reader.cli'`.

- [x] **Step 3: Create `reader.cli` with relocated parsing and reader-specific configuration.**

```python
from shared.client import ClientFactory, default_client_factory
from shared.config import ReaderConfig, load_reader_config
from reader.service import QueueReadError, ReadRequest, read_messages

ConfigLoader = Callable[[], ReaderConfig]

```

Move `ArgumentParseError`, `ReaderArgumentParser`, `_non_empty_queue`, `_positive_integer`, `parse_request`, and `main` bodies unchanged except for these imports/types. Preserve argparse's current `--help` behavior, required `--queue`, `--count`, `--mode`, argument errors before configuration loading, and safe error output with exit code `2`.

- [x] **Step 4: Run reader CLI tests to verify GREEN.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py -v`

Expected: PASS with no sender configuration or sender imports.

- [x] **Step 5: Delete the legacy reader CLI test after GREEN.**

Delete `tests/test_reader_cli.py`; retain `src/service_bus_sender/reader_cli.py` only until Task 7.

- [x] **Step 6: Conditionally commit the reader CLI work unit.**

Run only if `.git` exists:

```bash
git add src/reader/cli.py tests/reader/test_cli.py tests/test_reader_cli.py
git commit -m "feat: move reader command into dedicated package"
```

Verification recorded by this work unit: `tests/reader/test_cli.py` passes. Runtime harness: N/A; CLI error and output paths use injected streams and fake clients. Rollback boundary: remove `src/reader/cli.py` and its test together.

### Task 7: Switch Packaging, Documentation, And Remove The Legacy Package

**Files:**
- Modify: `pyproject.toml:14-20`
- Modify: `README.md:23-35`
- Modify: `tests/reader/test_cli.py`
- Delete: `src/service_bus_sender/__init__.py`
- Delete: `src/service_bus_sender/config.py`
- Delete: `src/service_bus_sender/files.py`
- Delete: `src/service_bus_sender/sender.py`
- Delete: `src/service_bus_sender/cli.py`
- Delete: `src/service_bus_sender/reader.py`
- Delete: `src/service_bus_sender/reader_cli.py`

- [x] **Step 1: Write the packaging regression before changing Poetry metadata.**

Replace the old project metadata test in `tests/reader/test_cli.py` with:

```python
def test_project_registers_flat_sender_and_reader_packages() -> None:
    project_root = Path(__file__).resolve().parents[2]
    pyproject = project_root.joinpath("pyproject.toml").read_text(encoding="utf-8")

    assert 'service-bus-send = "sender.cli:main"' in pyproject
    assert 'service-bus-read = "reader.cli:main"' in pyproject
    assert '{ include = "shared", from = "src" }' in pyproject
    assert '{ include = "sender", from = "src" }' in pyproject
    assert '{ include = "reader", from = "src" }' in pyproject
    assert "service_bus_sender" not in pyproject
```

- [x] **Step 2: Run the packaging regression to verify RED.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py::test_project_registers_flat_sender_and_reader_packages -v`

Expected: FAIL because `pyproject.toml` still references `service_bus_sender`.

- [x] **Step 3: Update entry points and explicit Poetry packages exactly.**

```toml
[project.scripts]
service-bus-send = "sender.cli:main"
service-bus-read = "reader.cli:main"

[tool.poetry]
requires-poetry = ">=2.0"
packages = [
    { include = "shared", from = "src" },
    { include = "sender", from = "src" },
    { include = "reader", from = "src" },
]
```

- [x] **Step 4: Update README configuration language without changing the user-facing send/read contracts.**

Replace the shared configuration explanation with language that says `AZURE_SERVICE_BUS_CONNECTION_STRING` and `LOG_LEVEL` apply to both commands; `SERVICE_BUS_DATA_DIR` defaults to `data` and is validated only by `service-bus-send`; `service-bus-read` does not require it. Keep the existing Conda installation, send input contract, reader mode, safe logging, partial delivery, and offline test documentation intact.

- [x] **Step 5: Run the packaging and reader CLI tests to verify GREEN before deleting legacy source.**

Run: `conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py -v`

Expected: PASS with the flat Poetry entry-point declarations.

- [x] **Step 6: Delete the old source package only after all target imports are green.**

Delete every listed `.py` file under `src/service_bus_sender/`, then remove the empty directory. Do not create `service_bus_sender` wrappers or aliases. Remove generated `__pycache__` directories only if present; they are build artifacts, not migration source.

- [x] **Step 7: Prove no source or tests retain the legacy import path.**

Run: `conda run -n tools-service-bus poetry run python -c 'from pathlib import Path; paths = [*Path("src").rglob("*.py"), *Path("tests").rglob("*.py"), Path("pyproject.toml"), Path("README.md")]; matches = [str(path) for path in paths if "service_bus_sender" in path.read_text(encoding="utf-8")]; assert not matches, matches'`

Expected: exit code `0`; the assertion finds no legacy import or packaging reference.

- [x] **Step 8: Conditionally commit the atomic packaging and removal work unit.**

Run only if `.git` exists:

```bash
git add pyproject.toml README.md src tests
git commit -m "refactor: replace legacy service bus package layout"
```

Verification recorded by this work unit: the packaging regression and reader CLI tests pass; the legacy-name scan has no matches. Runtime harness: N/A; package installation and console scripts are verified in Task 8. Rollback boundary: restore only the metadata/docs and legacy-source deletion as one atomic layout migration.

### Task 8: Reinstall And Perform Offline End-To-End Verification

**Files:**
- Verify only: `pyproject.toml`, `src/shared/`, `src/sender/`, `src/reader/`, `tests/`, `README.md`

- [x] **Step 1: Reinstall the project so Poetry regenerates console scripts from the new targets.**

Run: `conda run -n tools-service-bus poetry install`

Expected: successful install with the current project installed; no dependency additions or lock-file changes.

- [x] **Step 2: Verify the sender entry point resolves safely.**

Run: `conda run -n tools-service-bus poetry run service-bus-send --help`

Expected: exit code `0`, output containing `usage: service-bus-send`, and no configuration loading or Azure call.

- [x] **Step 3: Verify the reader entry point resolves.**

Run: `conda run -n tools-service-bus poetry run service-bus-read --help`

Expected: exit code `0`, output containing `usage: service-bus-read`, and no configuration loading or Azure call.

- [x] **Step 4: Run the complete offline regression suite.**

Run: `conda run -n tools-service-bus poetry run pytest -v`

Expected: PASS for every test under `tests/shared`, `tests/sender`, and `tests/reader`; no network access, Azure credentials, or live queue required.

- [x] **Step 5: Validate Poetry metadata.**

Run: `conda run -n tools-service-bus poetry check`

Expected: `All set!`

- [x] **Step 6: Conditionally commit no-op verification only if a previous command changed tracked metadata.**

Do not create an empty commit. If `poetry install` or verification legitimately changed tracked project metadata, inspect and commit that change with its owning work unit; otherwise no commit is required because this workspace has no Git repository.

Verification recorded by this work unit: install, both help commands, full offline suite, and `poetry check` all pass. Runtime harness: console-script help paths prove registered targets without opening Azure. Rollback boundary: N/A; no intended source change belongs to this verification task.

## Coverage Checklist

- [x] `shared` contains only configuration and Azure client concerns.
- [x] Reader configuration succeeds with connection/log settings and no `SERVICE_BUS_DATA_DIR`.
- [x] Sender configuration still validates a readable data directory.
- [x] Sender envelope discovery, validation, serialization, batching, partial counts, safe logging, summaries, and exit codes retain their existing regressions.
- [x] Reader `peek`, `block`, and `drain` modes retain receive/settlement behavior, safe errors, and stdout flush-before-completion semantics.
- [x] Tests mirror `shared`, `sender`, and `reader`; no root `tests/test_*.py` files remain.
- [x] Poetry declares precisely `shared`, `sender`, and `reader`, and scripts target `sender.cli:main` and `reader.cli:main`.
- [x] `src/service_bus_sender/` and every legacy import reference are removed.
- [x] `poetry install`, both `--help` commands, the full suite, and `poetry check` pass under Conda.

## Self-Review

| Check | Result |
| --- | --- |
| Design requirement coverage | Complete: Tasks 1-8 map every target layout, responsibility, configuration, packaging, migration, and verification requirement to executable steps. |
| TDD ordering | Complete: each implementation slice starts with a concrete target-package behavior test, specifies RED, implements the smallest boundary change, and specifies GREEN. |
| Import and signature consistency | Complete: the interface table matches the imports/signatures used by all subsequent tasks; no old-package symbol is retained. |
| Observable behavior preservation | Complete: every one of the 76 baseline tests is migrated into a target package, with two additional regressions for config separation and sender help. |
| Placeholder scan | Complete: no omitted implementation or unspecified error-handling steps; migration map, symbols, commands, expected results, and deletion timing are explicit. |
| Command/environment consistency | Complete: all Python/Poetry commands use `conda run -n tools-service-bus`; no live Azure command is included. |
| Commit strategy | Complete: each work unit includes tests and code, has a rollback boundary, and is conditional because `.git` is absent. |

## Risks

- The approved top-level package names can collide with another installed distribution. Poetry's explicit package list and the dedicated Conda environment constrain this accepted risk.
- The legacy sender command has no current help parser. Task 4 adds the smallest safe `--help` branch solely to satisfy the required post-install command; it must not load configuration or contact Azure.
- During the phased migration, old source remains only until every new caller/test is green. Task 7 removes it atomically and rejects compatibility wrappers.
