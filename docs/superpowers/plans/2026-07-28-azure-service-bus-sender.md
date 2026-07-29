# Azure Service Bus JSON Sender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a synchronous Python CLI that validates direct JSON files from a configured directory and sends each object to the Azure Service Bus queue named by its file, with deterministic ordering, dynamic batching, safe failure isolation, and accurate summaries.

**Architecture:** Use a `src`-layout package with the four approved modules: `config.py` owns startup configuration, `files.py` owns deterministic discovery and complete input validation, `sender.py` owns Azure message serialization and SDK-sized batching, and `cli.py` owns one-client orchestration, file-level continuation, sanitized logging, summaries, and exit codes. Tests exercise public functions with temporary directories and small Azure fakes; no test contacts Azure.

**Tech Stack:** Python 3.11+, Poetry 2, `azure-servicebus` 7.x, `python-dotenv` 1.x, `pytest` 8.x, standard-library `dataclasses`, `json`, `logging`, and `pathlib`.

---

## Execution Prerequisites

- Work from `/Users/garciajoise/Projects/esmax/tools/service-bus`.
- The Conda environment `tools-service-bus` already exists. Every Poetry, Python, and pytest command below runs through `conda run -n tools-service-bus` so shell activation state is irrelevant.
- Poetry is already initialized. Do not run `poetry init`.
- This directory is not currently a Git repository. Git initialization is an external maintainer prerequisite, outside this plan. Every commit step is conditional and must be skipped until a maintainer has initialized Git.
- Automated verification uses fakes only. Do not add credentials to tests and do not make a live Azure call.

## File And Responsibility Map

| Path | Change | Responsibility |
| --- | --- | --- |
| `pyproject.toml` | Modify | Declare Python compatibility, runtime and development dependencies, the `src` package, pytest settings, and the `service-bus-send` entry point. |
| `poetry.lock` | Create through Poetry | Record the exact dependency graph resolved in the existing Conda environment. |
| `.gitignore` | Create | Exclude local environments, Python artifacts, coverage output, and secret-bearing `.env` variants while retaining `.env.example`. |
| `.env.example` | Create | Document supported variable names with an empty connection string and non-secret defaults. |
| `data/orders.json` | Create | Provide a valid, non-secret two-message sample; the application never mutates it. |
| `README.md` | Create | Document setup, execution, input rules, exit codes, security, partial-send behavior, duplicates, and manual file lifecycle. |
| `src/service_bus_sender/__init__.py` | Create | Mark the package; contain no application behavior. |
| `src/service_bus_sender/config.py` | Create | Expose `Config`, `ConfigError`, and `load_config`. Load `.env` once, preserve process-environment precedence, resolve paths, and reject unsafe startup state. |
| `src/service_bus_sender/files.py` | Create | Expose `InputFileError`, `discover_json_files`, `derive_queue_name`, and `load_message_objects`. Validate the entire UTF-8 JSON document before returning objects. |
| `src/service_bus_sender/sender.py` | Create | Expose `FileSendError` and `send_objects`. Compactly serialize objects, create `ServiceBusMessage` values, roll dynamic batches, and retain confirmed-send counts on errors. |
| `src/service_bus_sender/cli.py` | Create | Expose `RunSummary`, `format_summary`, `run`, and `main`. Create one client per run, use sender context managers, continue by file, sanitize logs, and return `0`, `1`, or `2`. |
| `tests/test_config.py` | Create | Specify dotenv precedence, defaults, path resolution, validation, and secret-safe configuration errors. |
| `tests/test_files.py` | Create | Specify discovery order, non-recursion, exact suffix handling, queue derivation, UTF-8 decoding, complete JSON validation, and empty arrays. |
| `tests/test_sender.py` | Create | Specify compact serialization, no empty sends, dynamic rollover, message order, oversized-message behavior, and confirmed counts after send failures. |
| `tests/test_cli.py` | Create | Specify one-client orchestration, sender contexts, unchanged files, continuation, summaries, log sanitization, and exit codes. |

## Stable Interfaces

These names and signatures are used unchanged throughout the tasks:

```python
# src/service_bus_sender/config.py
@dataclass(frozen=True, slots=True)
class Config:
    connection_string: str
    data_dir: Path
    log_level: int

class ConfigError(ValueError): ...

def load_config(*, cwd: Path | None = None) -> Config: ...

# src/service_bus_sender/files.py
class InputFileError(ValueError): ...

def discover_json_files(data_dir: Path) -> list[Path]: ...
def derive_queue_name(path: Path) -> str: ...
def load_message_objects(path: Path) -> list[dict[str, object]]: ...

# src/service_bus_sender/sender.py
class FileSendError(RuntimeError):
    sent_count: int
    batch_number: int
    operation: str
    error_type: str

def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    *,
    message_factory: Callable[[str], object] = ServiceBusMessage,
) -> int: ...

# src/service_bus_sender/cli.py
@dataclass(frozen=True, slots=True)
class RunSummary:
    files: int
    succeeded: int
    failed: int
    messages_sent: int

    @property
    def exit_code(self) -> int: ...

def format_summary(summary: RunSummary) -> str: ...
def run(config: Config, *, client_factory: ClientFactory, logger: logging.Logger) -> RunSummary: ...
def main(*, config_loader: ConfigLoader = load_config, client_factory: ClientFactory = _default_client_factory) -> int: ...
```

## Work-Unit Strategy

Each task is a reviewable work unit with its focused test command and rollback boundary. Tests remain adjacent to the behavior they specify. The optional Conventional Commit commands are executable only after the external Git prerequisite is satisfied.

### Task 1: Configure Poetry And Create The Safe Package Scaffold

**Files:**
- Modify: `pyproject.toml`
- Create: `poetry.lock` through Poetry
- Create: `.gitignore`
- Create: `.env.example`
- Create: `data/orders.json`
- Create: `src/service_bus_sender/__init__.py`

**Work unit:** An installable package with a non-secret local operating scaffold.

**Rollback boundary:** Remove the five created paths and restore `pyproject.toml`; no later source behavior is part of this unit.

- [x] **Step 1: Verify the package does not exist yet**

Run:

```bash
conda run -n tools-service-bus poetry run python -c "import service_bus_sender"
```

Expected RED outcome: exit status is non-zero with `ModuleNotFoundError: No module named 'service_bus_sender'`.

- [x] **Step 2: Replace `pyproject.toml` with the exact Poetry 2 configuration**

```toml
[project]
name = "tool-service-bus"
version = "0.1.0"
description = "Send JSON object arrays to Azure Service Bus queues"
authors = [
    { name = "Joise Garcia", email = "ec20@esmax.cl" }
]
requires-python = ">=3.11"
dependencies = [
    "azure-servicebus>=7.13,<8.0",
    "python-dotenv>=1.0,<2.0",
]

[project.scripts]
service-bus-send = "service_bus_sender.cli:main"

[tool.poetry]
requires-poetry = ">=2.0"
packages = [{ include = "service_bus_sender", from = "src" }]

[tool.poetry.group.dev.dependencies]
pytest = ">=8.0,<9.0"

[tool.pytest.ini_options]
testpaths = ["tests"]

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0"]
build-backend = "poetry.core.masonry.api"
```

- [x] **Step 3: Create the exact non-secret support files**

Create `.gitignore`:

```gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.coverage
htmlcov/
.venv/
.env
.env.*
!.env.example
```

Create `.env.example`:

```dotenv
AZURE_SERVICE_BUS_CONNECTION_STRING=
SERVICE_BUS_DATA_DIR=data
LOG_LEVEL=INFO
```

Create `data/orders.json`:

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

Create an empty `src/service_bus_sender/__init__.py`.

- [x] **Step 4: Resolve and install the declared dependencies in the existing environment**

Run:

```bash
conda run -n tools-service-bus poetry lock
conda run -n tools-service-bus poetry install
conda run -n tools-service-bus poetry check
```

Expected GREEN outcome: all three commands exit `0`; Poetry creates `poetry.lock`, installs the project and development group, and reports that `pyproject.toml` is valid.

- [x] **Step 5: Verify package and dependency imports without contacting Azure**

Run:

```bash
conda run -n tools-service-bus poetry run python -c "import azure.servicebus, dotenv, pytest, service_bus_sender"
```

Expected GREEN outcome: exit `0` with no output and no network access.

- [x] **Step 6: Conditionally commit this work unit**

Skip this step while the directory is not a Git repository. After external Git initialization:

```bash
git add pyproject.toml poetry.lock .gitignore .env.example data/orders.json src/service_bus_sender/__init__.py
git commit -m "build: configure service bus sender package"
```

### Task 2: Load Dotenv Values, Environment Overrides, And Defaults

**Files:**
- Create: `tests/test_config.py`
- Create: `src/service_bus_sender/config.py`

**Work unit:** Valid configuration values load once from the working directory with process-environment precedence.

**Rollback boundary:** Remove `tests/test_config.py` and `src/service_bus_sender/config.py`.

- [x] **Step 1: Write the first configuration behavior tests**

Create `tests/test_config.py`:

```python
import logging
from pathlib import Path

from service_bus_sender.config import Config, load_config


def test_load_config_reads_dotenv_and_applies_defaults(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (tmp_path / ".env").write_text(
        "AZURE_SERVICE_BUS_CONNECTION_STRING=from-dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AZURE_SERVICE_BUS_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("SERVICE_BUS_DATA_DIR", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    config = load_config(cwd=tmp_path)

    assert config == Config(
        connection_string="from-dotenv",
        data_dir=data_dir.resolve(),
        log_level=logging.INFO,
    )


def test_process_environment_overrides_dotenv_and_resolves_relative_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    (tmp_path / ".env").write_text(
        "AZURE_SERVICE_BUS_CONNECTION_STRING=from-dotenv\n"
        "SERVICE_BUS_DATA_DIR=data\n"
        "LOG_LEVEL=ERROR\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "from-process")
    monkeypatch.setenv("SERVICE_BUS_DATA_DIR", "fixtures")
    monkeypatch.setenv("LOG_LEVEL", "debug")

    config = load_config(cwd=tmp_path)

    assert config.connection_string == "from-process"
    assert config.data_dir == fixture_dir.resolve()
    assert config.log_level == logging.DEBUG
```

- [x] **Step 2: Run the focused tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_config.py -v
```

Expected RED outcome: collection fails with `ModuleNotFoundError: No module named 'service_bus_sender.config'`.

- [x] **Step 3: Implement the minimum valid-loading path**

Create `src/service_bus_sender/config.py`:

```python
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
class Config:
    connection_string: str
    data_dir: Path
    log_level: int


def load_config(*, cwd: Path | None = None) -> Config:
    base_dir = (cwd or Path.cwd()).resolve()
    load_dotenv(dotenv_path=base_dir / ".env", override=False)

    connection_string = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "").strip()
    if not connection_string:
        raise ConfigError("AZURE_SERVICE_BUS_CONNECTION_STRING is required")

    configured_data_dir = Path(os.getenv("SERVICE_BUS_DATA_DIR", "data"))
    data_dir = (
        configured_data_dir
        if configured_data_dir.is_absolute()
        else base_dir / configured_data_dir
    ).resolve()
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    return Config(
        connection_string=connection_string,
        data_dir=data_dir,
        log_level=_LOG_LEVELS[log_level_name],
    )
```

- [x] **Step 4: Run the focused tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_config.py -v
```

Expected GREEN outcome: `2 passed`; no Azure connection is created.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_config.py src/service_bus_sender/config.py
git commit -m "feat: load service bus sender configuration"
```

### Task 3: Reject Invalid Configuration Without Exposing Secrets

**Files:**
- Modify: `tests/test_config.py`
- Modify: `src/service_bus_sender/config.py`

**Work unit:** Startup validation rejects missing credentials, invalid levels, and unusable data paths using application-authored messages that never contain the credential value.

**Rollback boundary:** Revert only the validation tests and validation branches added in this task; valid loading from Task 2 remains intact.

- [x] **Step 1: Append the exact validation tests**

Append to `tests/test_config.py`:

```python
import os

import pytest

from service_bus_sender.config import ConfigError


@pytest.mark.parametrize("value", [None, "   "])
def test_connection_string_must_be_non_empty(
    tmp_path: Path, monkeypatch, value: str | None
) -> None:
    (tmp_path / "data").mkdir()
    if value is None:
        monkeypatch.delenv("AZURE_SERVICE_BUS_CONNECTION_STRING", raising=False)
    else:
        monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", value)

    with pytest.raises(ConfigError, match="AZURE_SERVICE_BUS_CONNECTION_STRING") as error:
        load_config(cwd=tmp_path)

    assert value is None or value not in str(error.value)


def test_invalid_log_level_is_a_safe_configuration_error(
    tmp_path: Path, monkeypatch
) -> None:
    (tmp_path / "data").mkdir()
    secret = "Endpoint=sb://secret-marker/;SharedAccessKey=do-not-log"
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", secret)
    monkeypatch.setenv("LOG_LEVEL", "verbose")

    with pytest.raises(ConfigError, match="LOG_LEVEL") as error:
        load_config(cwd=tmp_path)

    assert secret not in str(error.value)


@pytest.mark.parametrize("kind", ["missing", "file", "unreadable"])
def test_data_path_must_be_a_readable_directory(
    tmp_path: Path, monkeypatch, kind: str
) -> None:
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "safe-test-value")
    candidate = tmp_path / kind
    if kind == "file":
        candidate.write_text("not a directory", encoding="utf-8")
    elif kind == "unreadable":
        candidate.mkdir()
        real_access = os.access
        monkeypatch.setattr(
            "service_bus_sender.config.os.access",
            lambda path, mode: False if Path(path) == candidate else real_access(path, mode),
        )
    monkeypatch.setenv("SERVICE_BUS_DATA_DIR", str(candidate))

    with pytest.raises(ConfigError, match="SERVICE_BUS_DATA_DIR"):
        load_config(cwd=tmp_path)
```

- [x] **Step 2: Run the focused tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_config.py -v
```

Expected RED outcome: invalid `LOG_LEVEL` raises `KeyError`, and missing, file, and unreadable data paths are accepted instead of raising `ConfigError`.

- [x] **Step 3: Replace `load_config` with the validated implementation**

Keep the existing imports, constants, `ConfigError`, and `Config`, then replace only `load_config` with:

```python
def load_config(*, cwd: Path | None = None) -> Config:
    base_dir = (cwd or Path.cwd()).resolve()
    load_dotenv(dotenv_path=base_dir / ".env", override=False)

    connection_string = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "").strip()
    if not connection_string:
        raise ConfigError("AZURE_SERVICE_BUS_CONNECTION_STRING is required")

    configured_data_dir = Path(os.getenv("SERVICE_BUS_DATA_DIR", "data"))
    data_dir = (
        configured_data_dir
        if configured_data_dir.is_absolute()
        else base_dir / configured_data_dir
    ).resolve()
    if not data_dir.is_dir() or not os.access(data_dir, os.R_OK):
        raise ConfigError("SERVICE_BUS_DATA_DIR must be a readable directory")

    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    try:
        log_level = _LOG_LEVELS[log_level_name]
    except KeyError as error:
        raise ConfigError(
            "LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
        ) from error

    return Config(
        connection_string=connection_string,
        data_dir=data_dir,
        log_level=log_level,
    )
```

- [x] **Step 4: Run the focused tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_config.py -v
```

Expected GREEN outcome: `8 passed`; neither assertion output nor exception text contains the test connection string.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_config.py src/service_bus_sender/config.py
git commit -m "feat: validate service bus sender startup settings"
```

### Task 4: Discover Direct JSON Files And Derive Queue Names Deterministically

**Files:**
- Create: `tests/test_files.py`
- Create: `src/service_bus_sender/files.py`

**Work unit:** File discovery is direct-only, exact-suffix filtered, and stable regardless of directory enumeration order.

**Rollback boundary:** Remove `tests/test_files.py` and `src/service_bus_sender/files.py`.

- [x] **Step 1: Write discovery and queue-name tests**

Create `tests/test_files.py`:

```python
from pathlib import Path

from service_bus_sender.files import discover_json_files, derive_queue_name


def test_discover_json_files_returns_only_direct_exact_suffix_files_sorted_by_name(
    tmp_path: Path,
) -> None:
    (tmp_path / "zeta.json").write_text("[]", encoding="utf-8")
    (tmp_path / "Alpha.json").write_text("[]", encoding="utf-8")
    (tmp_path / "ignored.JSON").write_text("[]", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("[]", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "inside.json").write_text("[]", encoding="utf-8")

    discovered = discover_json_files(tmp_path)

    assert [path.name for path in discovered] == ["Alpha.json", "zeta.json"]


def test_derive_queue_name_removes_only_the_final_json_suffix() -> None:
    assert derive_queue_name(Path("data/orders.json")) == "orders"
    assert derive_queue_name(Path("data/archive.orders.json")) == "archive.orders"
```

- [x] **Step 2: Run the focused tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected RED outcome: collection fails with `ModuleNotFoundError: No module named 'service_bus_sender.files'`.

- [x] **Step 3: Implement discovery and derivation**

Create `src/service_bus_sender/files.py`:

```python
from pathlib import Path


class InputFileError(ValueError):
    """Raised when an input file violates the JSON input contract."""


def discover_json_files(data_dir: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in data_dir.iterdir()
            if path.is_file() and path.suffix == ".json"
        ),
        key=lambda path: path.name,
    )


def derive_queue_name(path: Path) -> str:
    return path.name.removesuffix(".json")
```

- [x] **Step 4: Run the focused tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected GREEN outcome: `2 passed`.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_files.py src/service_bus_sender/files.py
git commit -m "feat: discover queue input files deterministically"
```

### Task 5: Decode And Completely Validate UTF-8 JSON Arrays

**Files:**
- Modify: `tests/test_files.py`
- Modify: `src/service_bus_sender/files.py`

**Work unit:** A file yields all JSON objects only after complete decoding and contract validation; an empty array yields no messages.

**Rollback boundary:** Remove `load_message_objects` and its tests; discovery and queue derivation from Task 4 remain.

- [x] **Step 1: Append complete input-contract tests**

Append to `tests/test_files.py`:

```python
import json

import pytest

from service_bus_sender.files import InputFileError, load_message_objects


def test_load_message_objects_returns_all_objects_and_preserves_json_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orders.json"
    customer_name = "Jos" + chr(233)
    path.write_text(
        json.dumps(
            [{"name": customer_name, "active": True, "nested": {"count": 2}}],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    objects = load_message_objects(path)

    assert objects == [
        {"name": customer_name, "active": True, "nested": {"count": 2}}
    ]


def test_load_message_objects_accepts_an_empty_array(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_text("[]", encoding="utf-8")

    assert load_message_objects(path) == []


@pytest.mark.parametrize(
    ("content", "safe_message"),
    [
        ("{", "invalid JSON"),
        ('{"orderId":"A-1"}', "top-level JSON value must be an array"),
        ('[{"orderId":"A-1"}, 7]', "item at index 1 must be an object"),
        ('[{"orderId":"A-1"}, []]', "item at index 1 must be an object"),
        ('[{"orderId":"A-1"}, null]', "item at index 1 must be an object"),
    ],
)
def test_load_message_objects_rejects_the_complete_invalid_document(
    tmp_path: Path, content: str, safe_message: str
) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(InputFileError, match=safe_message):
        load_message_objects(path)


def test_load_message_objects_rejects_invalid_utf8_without_echoing_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid.json"
    path.write_bytes(b"[\xff]")

    with pytest.raises(InputFileError, match="UnicodeDecodeError") as error:
        load_message_objects(path)

    assert "\\xff" not in str(error.value)


def test_load_message_objects_wraps_read_errors_without_raw_os_text(
    tmp_path: Path,
) -> None:
    path = tmp_path / "not-a-file.json"
    path.mkdir()

    with pytest.raises(InputFileError, match="OSError|IsADirectoryError") as error:
        load_message_objects(path)

    assert str(path) not in str(error.value)
```

- [x] **Step 2: Run the new behavior tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected RED outcome: collection fails because `load_message_objects` is not defined.

- [x] **Step 3: Add complete decode and validation before return**

Add `import json` to `src/service_bus_sender/files.py`, then append:

```python
def load_message_objects(path: Path) -> list[dict[str, object]]:
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

    if not isinstance(value, list):
        raise InputFileError(
            f"{path.name}: top-level JSON value must be an array"
        )

    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise InputFileError(
                f"{path.name}: item at index {index} must be an object"
            )

    return value
```

- [x] **Step 4: Run the file-contract suite to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
```

Expected GREEN outcome: `11 passed`; the late invalid element rejects the whole return value, read failures are wrapped, and invalid bytes are absent from the safe exception message.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_files.py src/service_bus_sender/files.py
git commit -m "feat: validate complete JSON message files"
```

### Task 6: Serialize Objects And Send One Dynamic Batch

**Files:**
- Create: `tests/test_sender.py`
- Create: `src/service_bus_sender/sender.py`

**Work unit:** Each object becomes one compact UTF-8-preserving JSON message, while empty arrays never trigger an empty send.

**Rollback boundary:** Remove `tests/test_sender.py` and `src/service_bus_sender/sender.py`.

- [x] **Step 1: Write basic serialization and empty-array tests with fakes**

Create `tests/test_sender.py`:

```python
from service_bus_sender.sender import send_objects


class FakeBatch:
    def __init__(self) -> None:
        self.messages: list[object] = []

    def add_message(self, message: object) -> None:
        self.messages.append(message)


class FakeSender:
    def __init__(self) -> None:
        self.created_batches: list[FakeBatch] = []
        self.sent_batches: list[list[object]] = []

    def create_message_batch(self) -> FakeBatch:
        batch = FakeBatch()
        self.created_batches.append(batch)
        return batch

    def send_messages(self, batch: FakeBatch) -> None:
        self.sent_batches.append(list(batch.messages))


def test_send_objects_serializes_each_object_as_one_compact_ordered_message() -> None:
    sender = FakeSender()
    customer_name = "Jos" + chr(233)

    sent_count = send_objects(
        sender,
        [
            {"orderId": "A-1", "status": "created"},
            {"customer": customer_name, "items": []},
        ],
        message_factory=lambda body: body,
    )

    assert sent_count == 2
    assert sender.sent_batches == [
        [
            '{"orderId":"A-1","status":"created"}',
            f'{{"customer":"{customer_name}","items":[]}}',
        ]
    ]


def test_send_objects_does_not_send_an_empty_batch() -> None:
    sender = FakeSender()

    sent_count = send_objects(sender, [], message_factory=lambda body: body)

    assert sent_count == 0
    assert len(sender.created_batches) == 1
    assert sender.sent_batches == []
```

- [x] **Step 2: Run the focused sender tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py -v
```

Expected RED outcome: collection fails with `ModuleNotFoundError: No module named 'service_bus_sender.sender'`.

- [x] **Step 3: Implement compact message creation and the first batch**

Create `src/service_bus_sender/sender.py`:

```python
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from azure.servicebus import ServiceBusMessage


class MessageBatch(Protocol):
    def add_message(self, message: object) -> None: ...


class QueueSender(Protocol):
    def create_message_batch(self) -> MessageBatch: ...

    def send_messages(self, batch: MessageBatch) -> None: ...


MessageFactory = Callable[[str], object]


def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    *,
    message_factory: MessageFactory = ServiceBusMessage,
) -> int:
    batch = sender.create_message_batch()
    batch_count = 0

    for item in objects:
        body = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
        batch.add_message(message_factory(body))
        batch_count += 1

    if batch_count:
        sender.send_messages(batch)

    return batch_count
```

- [x] **Step 4: Run the focused sender tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py -v
```

Expected GREEN outcome: `2 passed`; the fakes receive exact compact bodies and no Azure client exists.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_sender.py src/service_bus_sender/sender.py
git commit -m "feat: serialize JSON objects into service bus messages"
```

### Task 7: Roll Dynamic Batches And Preserve Confirmed Counts On Failure

**Files:**
- Modify: `tests/test_sender.py`
- Modify: `src/service_bus_sender/sender.py`

**Work unit:** SDK capacity controls batch rollover; a message that cannot fit an empty batch fails the file, and send failures retain only confirmed prior counts.

**Rollback boundary:** Restore the single-batch `send_objects` from Task 6 and remove `FileSendError` plus rollover tests.

- [x] **Step 1: Replace the sender fakes with capacity-aware fakes**

In `tests/test_sender.py`, add this import:

```python
import pytest
from azure.servicebus.exceptions import MessageSizeExceededError

from service_bus_sender.sender import FileSendError, send_objects
```

Replace the existing `FakeBatch` and `FakeSender` definitions with:

```python
class FakeBatch:
    def __init__(self, capacity: int, rejected_message: object | None) -> None:
        self.capacity = capacity
        self.rejected_message = rejected_message
        self.messages: list[object] = []

    def add_message(self, message: object) -> None:
        if message == self.rejected_message or len(self.messages) >= self.capacity:
            raise MessageSizeExceededError(message="fake batch capacity exceeded")
        self.messages.append(message)


class FakeSender:
    def __init__(
        self,
        *,
        capacity: int = 100,
        fail_on_send: int | None = None,
        failure_text: str = "fake send failure",
        rejected_message: object | None = None,
    ) -> None:
        self.capacity = capacity
        self.fail_on_send = fail_on_send
        self.failure_text = failure_text
        self.rejected_message = rejected_message
        self.created_batches: list[FakeBatch] = []
        self.sent_batches: list[list[object]] = []
        self.send_attempts = 0

    def create_message_batch(self) -> FakeBatch:
        batch = FakeBatch(self.capacity, self.rejected_message)
        self.created_batches.append(batch)
        return batch

    def send_messages(self, batch: FakeBatch) -> None:
        self.send_attempts += 1
        if self.send_attempts == self.fail_on_send:
            raise RuntimeError(self.failure_text)
        self.sent_batches.append(list(batch.messages))
```

- [x] **Step 2: Append rollover, oversized-message, and partial-count tests**

Append to `tests/test_sender.py`:

```python
def test_send_objects_rolls_full_batches_and_flushes_the_final_partial_batch() -> None:
    sender = FakeSender(capacity=2)

    sent_count = send_objects(
        sender,
        [{"n": number} for number in range(1, 6)],
        message_factory=lambda body: body,
    )

    assert sent_count == 5
    assert sender.sent_batches == [
        ['{"n":1}', '{"n":2}'],
        ['{"n":3}', '{"n":4}'],
        ['{"n":5}'],
    ]
    assert sender.send_attempts == 3


def test_send_objects_fails_when_one_message_cannot_fit_a_fresh_batch() -> None:
    sender = FakeSender(capacity=0)

    with pytest.raises(FileSendError) as error:
        send_objects(
            sender,
            [{"oversized": True}],
            message_factory=lambda body: body,
        )

    assert error.value.sent_count == 0
    assert error.value.batch_number == 1
    assert error.value.operation == "adding message to"
    assert error.value.error_type == "MessageSizeExceededError"
    assert sender.sent_batches == []


def test_oversized_later_message_retains_prior_count_and_stops_the_file() -> None:
    oversized_body = '{"oversized":true}'
    sender = FakeSender(capacity=10, rejected_message=oversized_body)

    with pytest.raises(FileSendError) as error:
        send_objects(
            sender,
            [{"sent": 1}, {"oversized": True}, {"notAttempted": 3}],
            message_factory=lambda body: body,
        )

    assert error.value.sent_count == 1
    assert error.value.batch_number == 2
    assert error.value.operation == "adding message to"
    assert error.value.error_type == "MessageSizeExceededError"
    assert sender.sent_batches == [['{"sent":1}']]
    assert len(sender.created_batches) == 2


def test_send_objects_reports_only_batches_confirmed_before_send_failure() -> None:
    sender = FakeSender(
        capacity=2,
        fail_on_send=2,
        failure_text="Endpoint=secret-marker payload=complete-object",
    )

    with pytest.raises(FileSendError) as error:
        send_objects(
            sender,
            [{"n": number} for number in range(1, 6)],
            message_factory=lambda body: body,
        )

    assert error.value.sent_count == 2
    assert error.value.batch_number == 2
    assert error.value.operation == "sending"
    assert error.value.error_type == "RuntimeError"
    assert sender.sent_batches == [['{"n":1}', '{"n":2}']]
    assert "secret-marker" not in str(error.value)
    assert "complete-object" not in str(error.value)
```

- [x] **Step 3: Run the expanded sender tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py -v
```

Expected RED outcome: collection fails because `FileSendError` does not exist; without the new algorithm, capacity rollover would also raise `MessageSizeExceededError`.

- [x] **Step 4: Replace `sender.py` with the exact rollover implementation**

```python
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from azure.servicebus import ServiceBusMessage
from azure.servicebus.exceptions import MessageSizeExceededError


class MessageBatch(Protocol):
    def add_message(self, message: object) -> None: ...


class QueueSender(Protocol):
    def create_message_batch(self) -> MessageBatch: ...

    def send_messages(self, batch: MessageBatch) -> None: ...


MessageFactory = Callable[[str], object]


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


def _create_batch(
    sender: QueueSender, *, sent_count: int, batch_number: int
) -> MessageBatch:
    try:
        return sender.create_message_batch()
    except Exception as error:
        raise FileSendError(
            sent_count=sent_count,
            batch_number=batch_number,
            operation="creating",
            cause=error,
        ) from error


def _send_batch(
    sender: QueueSender,
    batch: MessageBatch,
    *,
    batch_count: int,
    sent_count: int,
    batch_number: int,
) -> int:
    try:
        sender.send_messages(batch)
    except Exception as error:
        raise FileSendError(
            sent_count=sent_count,
            batch_number=batch_number,
            operation="sending",
            cause=error,
        ) from error
    return sent_count + batch_count


def send_objects(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    *,
    message_factory: MessageFactory = ServiceBusMessage,
) -> int:
    sent_count = 0
    batch_number = 1
    batch = _create_batch(sender, sent_count=sent_count, batch_number=batch_number)
    batch_count = 0

    for item in objects:
        body = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
        message = message_factory(body)
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

- [x] **Step 5: Run the sender suite to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_sender.py -v
```

Expected GREEN outcome: `6 passed`; sent groups are exactly `2, 2, 1`, the empty batch is never sent, and send or oversized-message failures retain only confirmed counts.

- [x] **Step 6: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_sender.py src/service_bus_sender/sender.py
git commit -m "feat: roll service bus batches at SDK capacity"
```

### Task 8: Orchestrate Successful And Empty Runs With One Client

**Files:**
- Create: `tests/test_cli.py`
- Create: `src/service_bus_sender/cli.py`

**Work unit:** The CLI discovers once, creates one client context per run, opens one sender context per file, preserves sorted order and files, and reports successful summaries.

**Rollback boundary:** Remove `tests/test_cli.py` and `src/service_bus_sender/cli.py`; lower-level modules remain usable.

- [x] **Step 1: Write successful orchestration tests with context-manager fakes**

Create `tests/test_cli.py`:

```python
import json
import logging
from pathlib import Path

from service_bus_sender.cli import RunSummary, format_summary, main, run
from service_bus_sender.config import Config


class FakeBatch:
    def __init__(self) -> None:
        self.messages: list[object] = []

    def add_message(self, message: object) -> None:
        self.messages.append(message)


class FakeQueueSender:
    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name
        self.entered = False
        self.exited = False
        self.messages_sent = 0

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True

    def create_message_batch(self) -> FakeBatch:
        return FakeBatch()

    def send_messages(self, batch: FakeBatch) -> None:
        self.messages_sent += len(batch.messages)


class FakeClient:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False
        self.queue_names: list[str] = []
        self.senders: list[FakeQueueSender] = []

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True

    def get_queue_sender(self, queue_name: str) -> FakeQueueSender:
        sender = FakeQueueSender(queue_name)
        self.queue_names.append(queue_name)
        self.senders.append(sender)
        return sender


class FakeClientFactory:
    def __init__(self, client: FakeClient | None = None) -> None:
        self.client = client or FakeClient()
        self.connection_strings: list[str] = []

    def __call__(self, connection_string: str) -> FakeClient:
        self.connection_strings.append(connection_string)
        return self.client


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def make_config(data_dir: Path) -> Config:
    return Config(
        connection_string="Endpoint=sb://secret-marker/;SharedAccessKey=test-only",
        data_dir=data_dir,
        log_level=logging.INFO,
    )


def test_run_uses_one_client_and_sorted_sender_contexts_without_changing_files(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "b.json", [{"n": 2}])
    write_json(tmp_path / "a.json", [{"n": 1}, {"n": 3}])
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.success"),
    )

    assert summary == RunSummary(files=2, succeeded=2, failed=0, messages_sent=3)
    assert summary.exit_code == 0
    assert factory.connection_strings == [make_config(tmp_path).connection_string]
    assert factory.client.entered is True
    assert factory.client.exited is True
    assert factory.client.queue_names == ["a", "b"]
    assert [sender.entered for sender in factory.client.senders] == [True, True]
    assert [sender.exited for sender in factory.client.senders] == [True, True]
    assert [sender.messages_sent for sender in factory.client.senders] == [2, 1]
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_run_with_no_json_files_is_successful_and_sends_nothing(tmp_path: Path) -> None:
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.empty"),
    )

    assert summary == RunSummary(files=0, succeeded=0, failed=0, messages_sent=0)
    assert summary.exit_code == 0
    assert factory.connection_strings == [make_config(tmp_path).connection_string]
    assert factory.client.queue_names == []


def test_run_treats_an_empty_array_as_a_successful_zero_message_file(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "empty.json", [])
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.empty-array"),
    )

    assert summary == RunSummary(files=1, succeeded=1, failed=0, messages_sent=0)
    assert factory.client.queue_names == ["empty"]
    assert factory.client.senders[0].entered is True
    assert factory.client.senders[0].exited is True
    assert factory.client.senders[0].messages_sent == 0


def test_main_returns_zero_and_logs_the_exact_summary_for_success(
    tmp_path: Path, caplog
) -> None:
    factory = FakeClientFactory()

    with caplog.at_level(logging.INFO):
        exit_code = main(
            config_loader=lambda: make_config(tmp_path),
            client_factory=factory,
        )

    assert exit_code == 0
    assert format_summary(RunSummary(0, 0, 0, 0)) in caplog.text
```

- [x] **Step 2: Run the focused CLI tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected RED outcome: collection fails with `ModuleNotFoundError: No module named 'service_bus_sender.cli'`.

- [x] **Step 3: Implement the successful orchestration path**

Create `src/service_bus_sender/cli.py`:

```python
from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from azure.servicebus import ServiceBusClient

from service_bus_sender.config import Config, load_config
from service_bus_sender.files import (
    derive_queue_name,
    discover_json_files,
    load_message_objects,
)
from service_bus_sender.sender import QueueSender, send_objects


_LOGGER = logging.getLogger("service_bus_sender")


class ServiceBusClientLike(Protocol):
    def get_queue_sender(
        self, queue_name: str
    ) -> AbstractContextManager[QueueSender]: ...


ClientFactory = Callable[[str], AbstractContextManager[ServiceBusClientLike]]
ConfigLoader = Callable[[], Config]


def _default_client_factory(
    connection_string: str,
) -> AbstractContextManager[ServiceBusClientLike]:
    return ServiceBusClient.from_connection_string(connection_string)


@dataclass(frozen=True, slots=True)
class RunSummary:
    files: int
    succeeded: int
    failed: int
    messages_sent: int

    @property
    def exit_code(self) -> int:
        return 1 if self.failed else 0


def format_summary(summary: RunSummary) -> str:
    return (
        "Service Bus send summary: "
        f"files={summary.files} "
        f"succeeded={summary.succeeded} "
        f"failed={summary.failed} "
        f"messages_sent={summary.messages_sent}"
    )


def run(
    config: Config,
    *,
    client_factory: ClientFactory = _default_client_factory,
    logger: logging.Logger = _LOGGER,
) -> RunSummary:
    paths = discover_json_files(config.data_dir)
    messages_sent = 0

    with client_factory(config.connection_string) as client:
        for path in paths:
            queue_name = derive_queue_name(path)
            objects = load_message_objects(path)
            with client.get_queue_sender(queue_name) as sender:
                sent_for_file = send_objects(sender, objects)
            messages_sent += sent_for_file
            logger.info(
                "%s -> %s: sent %d messages",
                path.name,
                queue_name,
                sent_for_file,
            )

    return RunSummary(
        files=len(paths),
        succeeded=len(paths),
        failed=0,
        messages_sent=messages_sent,
    )


def _configure_logging(level: int) -> None:
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def main(
    *,
    config_loader: ConfigLoader = load_config,
    client_factory: ClientFactory = _default_client_factory,
) -> int:
    config = config_loader()
    _configure_logging(config.log_level)
    summary = run(config, client_factory=client_factory, logger=_LOGGER)
    _LOGGER.info(format_summary(summary))
    return summary.exit_code
```

- [x] **Step 4: Run the focused CLI tests to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected GREEN outcome: `4 passed`; two files use one client, queue order is `a`, `b`, every context exits, file bytes are unchanged, and an empty array succeeds with zero messages.

- [x] **Step 5: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_cli.py src/service_bus_sender/cli.py
git commit -m "feat: orchestrate successful service bus sends"
```

### Task 9: Continue After File Failures And Sanitize Operational Logs

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/service_bus_sender/cli.py`

**Work unit:** Validation, queue sender, oversized, and send failures stop only the affected file; later files run, partial confirmed counts survive, and logs expose no credential, payload, or raw exception text.

**Rollback boundary:** Restore Task 8's all-success `run` implementation and remove failure-capable CLI fakes and tests.

- [x] **Step 1: Replace the CLI fakes with exact failure-capable versions**

Add these imports to `tests/test_cli.py`:

```python
from azure.servicebus.exceptions import MessageSizeExceededError
```

Replace `FakeBatch`, `FakeQueueSender`, and `FakeClient` with:

```python
class FakeBatch:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.messages: list[object] = []

    def add_message(self, message: object) -> None:
        if len(self.messages) >= self.capacity:
            raise MessageSizeExceededError(message="fake batch capacity exceeded")
        self.messages.append(message)


class FakeQueueSender:
    def __init__(
        self,
        queue_name: str,
        *,
        capacity: int = 100,
        fail_on_send: int | None = None,
        failure_text: str = "fake send failure",
    ) -> None:
        self.queue_name = queue_name
        self.capacity = capacity
        self.fail_on_send = fail_on_send
        self.failure_text = failure_text
        self.entered = False
        self.exited = False
        self.messages_sent = 0
        self.send_attempts = 0

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True

    def create_message_batch(self) -> FakeBatch:
        return FakeBatch(self.capacity)

    def send_messages(self, batch: FakeBatch) -> None:
        self.send_attempts += 1
        if self.send_attempts == self.fail_on_send:
            raise RuntimeError(self.failure_text)
        self.messages_sent += len(batch.messages)


class FakeClient:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False
        self.queue_names: list[str] = []
        self.senders: list[FakeQueueSender] = []
        self.sender_options: dict[str, dict[str, object]] = {}
        self.queue_creation_failures: dict[str, str] = {}

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True

    def get_queue_sender(self, queue_name: str) -> FakeQueueSender:
        if queue_name in self.queue_creation_failures:
            raise RuntimeError(self.queue_creation_failures[queue_name])
        sender = FakeQueueSender(queue_name, **self.sender_options.get(queue_name, {}))
        self.queue_names.append(queue_name)
        self.senders.append(sender)
        return sender
```

The existing `FakeClientFactory`, `write_json`, `make_config`, and Task 8 tests remain unchanged and use the new defaults.

- [x] **Step 2: Append validation-continuation and queue-creation tests**

Append to `tests/test_cli.py`:

```python
def test_run_validates_the_whole_file_before_opening_its_sender_and_continues(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "a.json", [{"valid": True}, 7])
    write_json(tmp_path / "b.json", [{"sent": True}])
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.validation-continuation"),
    )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert factory.client.queue_names == ["b"]


def test_run_continues_after_queue_sender_creation_failure(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", [{"notSent": True}])
    write_json(tmp_path / "b.json", [{"sent": True}])
    client = FakeClient()
    client.queue_creation_failures["a"] = "Endpoint=secret-marker raw SDK detail"
    factory = FakeClientFactory(client)

    with caplog.at_level(logging.ERROR, logger="test.sender-creation"):
        summary = run(
            make_config(tmp_path),
            client_factory=factory,
            logger=logging.getLogger("test.sender-creation"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert client.queue_names == ["b"]
    assert "a.json -> a: RuntimeError" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "raw SDK detail" not in caplog.text


def test_run_continues_after_one_message_cannot_fit_an_empty_batch(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "a.json", [{"tooLarge": True}])
    write_json(tmp_path / "b.json", [{"sent": True}])
    client = FakeClient()
    client.sender_options["a"] = {"capacity": 0}
    factory = FakeClientFactory(client)

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.oversized"),
    )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert client.queue_names == ["a", "b"]
    assert client.senders[0].messages_sent == 0
```

- [x] **Step 3: Append partial-send and sanitized-log tests**

Append to `tests/test_cli.py`:

```python
def test_run_counts_partial_sends_continues_and_never_logs_secrets_or_payloads(
    tmp_path: Path, caplog
) -> None:
    payload_marker = "complete-payload-must-not-appear"
    write_json(
        tmp_path / "a.json",
        [{"n": number, "marker": payload_marker} for number in range(1, 6)],
    )
    write_json(tmp_path / "b.json", [{"sent": True}])
    client = FakeClient()
    client.sender_options["a"] = {
        "capacity": 2,
        "fail_on_send": 2,
        "failure_text": (
            "Endpoint=sb://secret-marker/;SharedAccessKey=test-only "
            f"payload={payload_marker}"
        ),
    }
    factory = FakeClientFactory(client)

    with caplog.at_level(logging.INFO, logger="test.partial"):
        summary = run(
            make_config(tmp_path),
            client_factory=factory,
            logger=logging.getLogger("test.partial"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=3)
    assert summary.exit_code == 1
    assert client.queue_names == ["a", "b"]
    assert "a.json -> a: RuntimeError while sending batch 2" in caplog.text
    assert "b.json -> b: sent 1 messages" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "SharedAccessKey" not in caplog.text
    assert payload_marker not in caplog.text
```

- [x] **Step 4: Run the expanded CLI tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected RED outcome: `run` stops on the first invalid or failed file instead of returning a mixed summary, so the new continuation tests fail.

- [x] **Step 5: Replace `run` with file-isolated orchestration**

Add these imports to `src/service_bus_sender/cli.py`:

```python
from service_bus_sender.files import InputFileError
from service_bus_sender.sender import FileSendError
```

Replace `run` with:

```python
def run(
    config: Config,
    *,
    client_factory: ClientFactory = _default_client_factory,
    logger: logging.Logger = _LOGGER,
) -> RunSummary:
    paths = discover_json_files(config.data_dir)
    succeeded = 0
    failed = 0
    messages_sent = 0

    with client_factory(config.connection_string) as client:
        for path in paths:
            queue_name = derive_queue_name(path)
            sent_for_file = 0
            try:
                objects = load_message_objects(path)
                with client.get_queue_sender(queue_name) as sender:
                    sent_for_file = send_objects(sender, objects)
            except InputFileError as error:
                failed += 1
                logger.error(
                    "%s -> %s: %s while validating input",
                    path.name,
                    queue_name,
                    type(error).__name__,
                )
                continue
            except FileSendError as error:
                failed += 1
                messages_sent += error.sent_count
                logger.error(
                    "%s -> %s: %s while %s batch %d",
                    path.name,
                    queue_name,
                    error.error_type,
                    error.operation,
                    error.batch_number,
                )
                continue
            except Exception as error:
                failed += 1
                messages_sent += sent_for_file
                logger.error(
                    "%s -> %s: %s while opening or closing queue sender",
                    path.name,
                    queue_name,
                    type(error).__name__,
                )
                continue

            succeeded += 1
            messages_sent += sent_for_file
            logger.info(
                "%s -> %s: sent %d messages",
                path.name,
                queue_name,
                sent_for_file,
            )

    return RunSummary(
        files=len(paths),
        succeeded=succeeded,
        failed=failed,
        messages_sent=messages_sent,
    )
```

- [x] **Step 6: Run CLI and sender suites to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py tests/test_sender.py -v
```

Expected GREEN outcome: `14 passed`; later files run after validation, sender-creation, oversized-message, and send failures; the partial run reports exactly three confirmed messages and no secret or complete payload in logs.

- [x] **Step 7: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_cli.py src/service_bus_sender/cli.py
git commit -m "feat: isolate service bus failures by input file"
```

### Task 10: Return Startup And Aggregate Exit Codes `0`, `1`, And `2`

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/service_bus_sender/cli.py`

**Work unit:** `main` returns `0` for complete success, `1` for any file failure, and `2` for configuration, client creation, or unexpected run-level failure while always emitting a sanitized summary.

**Rollback boundary:** Restore Task 9's direct `main` implementation and remove startup/exit tests; `run` retains file isolation.

- [x] **Step 1: Append mixed-result and startup exit-code tests**

Append to `tests/test_cli.py`:

```python
from service_bus_sender.config import ConfigError


def test_main_returns_one_and_logs_summary_when_any_file_fails(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", [7])
    write_json(tmp_path / "b.json", [{"sent": True}])

    with caplog.at_level(logging.INFO):
        exit_code = main(
            config_loader=lambda: make_config(tmp_path),
            client_factory=FakeClientFactory(),
        )

    assert exit_code == 1
    assert (
        "Service Bus send summary: files=2 succeeded=1 failed=1 messages_sent=1"
        in caplog.text
    )


def test_main_returns_two_with_zero_summary_for_configuration_failure(
    caplog,
) -> None:
    def invalid_config() -> Config:
        raise ConfigError(
            "Endpoint=sb://secret-marker/;SharedAccessKey=test-only"
        )

    with caplog.at_level(logging.ERROR):
        exit_code = main(
            config_loader=invalid_config,
            client_factory=FakeClientFactory(),
        )

    assert exit_code == 2
    assert "ConfigError while loading configuration" in caplog.text
    assert (
        "Service Bus send summary: files=0 succeeded=0 failed=0 messages_sent=0"
        in caplog.text
    )
    assert "secret-marker" not in caplog.text
    assert "SharedAccessKey" not in caplog.text


def test_main_returns_two_with_zero_summary_when_client_creation_fails(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "orders.json", [{"neverSent": True}])

    def failing_client_factory(connection_string: str):
        raise RuntimeError(
            f"{connection_string} payload=complete-payload-must-not-appear"
        )

    with caplog.at_level(logging.ERROR):
        exit_code = main(
            config_loader=lambda: make_config(tmp_path),
            client_factory=failing_client_factory,
        )

    assert exit_code == 2
    assert "RuntimeError while starting or running sender" in caplog.text
    assert (
        "Service Bus send summary: files=0 succeeded=0 failed=0 messages_sent=0"
        in caplog.text
    )
    assert "secret-marker" not in caplog.text
    assert "complete-payload-must-not-appear" not in caplog.text
```

- [x] **Step 2: Run the focused exit-code tests to establish RED**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected RED outcome: the mixed run already returns `1`, but both startup cases raise uncaught exceptions instead of returning `2` and logging a zero summary.

- [x] **Step 3: Replace `main` with sanitized startup handling**

Replace `main` in `src/service_bus_sender/cli.py` with:

```python
def main(
    *,
    config_loader: ConfigLoader = load_config,
    client_factory: ClientFactory = _default_client_factory,
) -> int:
    empty_summary = RunSummary(files=0, succeeded=0, failed=0, messages_sent=0)
    try:
        config = config_loader()
    except Exception as error:
        _LOGGER.error(
            "%s while loading configuration",
            type(error).__name__,
        )
        _LOGGER.error(format_summary(empty_summary))
        return 2

    _configure_logging(config.log_level)
    try:
        summary = run(config, client_factory=client_factory, logger=_LOGGER)
    except Exception as error:
        _LOGGER.error(
            "%s while starting or running sender",
            type(error).__name__,
        )
        _LOGGER.error(format_summary(empty_summary))
        return 2

    _LOGGER.info(format_summary(summary))
    return summary.exit_code
```

- [x] **Step 4: Run the complete CLI suite to establish GREEN**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected GREEN outcome: `11 passed`; observed exit codes include `0`, `1`, and `2`, and every startup path emits an exact summary without raw exception text.

- [x] **Step 5: Verify the installed entry point resolves without invoking Azure**

Run:

```bash
conda run -n tools-service-bus poetry run python -c "from service_bus_sender.cli import main; assert callable(main)"
```

Expected GREEN outcome: exit `0` with no output and no Azure connection.

- [x] **Step 6: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add tests/test_cli.py src/service_bus_sender/cli.py
git commit -m "feat: report service bus sender exit status safely"
```

### Task 11: Document Setup, Operation, Partial Sends, And Duplicate Risk

**Files:**
- Create: `README.md`
- Verify: `.env.example`
- Verify: `data/orders.json`

**Work unit:** Operators can install, configure, run, interpret, and safely manage the tool without inferring delivery guarantees.

**Rollback boundary:** Remove `README.md`; package behavior and safe sample files remain unchanged.

- [x] **Step 1: Create the complete operational README**

Create `README.md`:

````markdown
# Azure Service Bus JSON Sender

Send each JSON object from direct `.json` files in a local directory as an independent message to an Azure Service Bus queue. The queue name is the file name without its final `.json` suffix.

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

Populate `AZURE_SERVICE_BUS_CONNECTION_STRING` only in the ignored local `.env`; it is required. `SERVICE_BUS_DATA_DIR` defaults to `data`; relative paths resolve from the current working directory. `LOG_LEVEL` defaults to `INFO` and accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` case-insensitively.

Process environment variables take precedence over `.env`. Local `.env` variants are ignored by Git, while `.env.example` is retained.

## Input Contract

The configured directory is scanned non-recursively. Only direct regular files with the exact lowercase `.json` suffix are processed, in ascending case-sensitive file-name order.

Each file must be UTF-8 JSON with an array at the top level. Every array element must be an object. The complete file is decoded and validated before its sender is opened, so an invalid file sends zero messages. An empty array is valid and sends zero messages.

`data/orders.json` targets the `orders` queue. Each array object is compactly serialized as one message; the entire array is never sent as one message.

## Run

```bash
conda run -n tools-service-bus poetry run service-bus-send
```

The tool creates one Service Bus client for the run and one queue sender context per valid file. It logs per-file progress and finishes with a summary such as:

```text
Service Bus send summary: files=3 succeeded=2 failed=1 messages_sent=47
```

Logs include file names, queue names, exception class names, batch numbers, and confirmed counts. They do not include the connection string, complete payloads, `.env` contents, or raw exception text.

## Exit Codes

| Code | Meaning |
| --- | --- |
| `0` | Configuration is valid and every discovered file succeeds, including an empty directory. |
| `1` | At least one file fails; later files are still attempted. |
| `2` | Configuration, client creation, or another run-level startup operation fails before a normal aggregate result. |

## Delivery And File Lifecycle

Input files are never moved, renamed, deleted, or edited. After a successful run, manually remove or relocate files that must not be sent again.

The tool does not provide deduplication, retries, checkpoints, resume behavior, or exactly-once delivery. Running it again processes every discovered file again and can duplicate all messages from previously successful files.

A file can fail after one or more earlier batches were accepted by Azure. The summary counts those confirmed earlier messages but still marks the file failed. Re-running that unchanged file can duplicate the earlier messages as well as attempt the remaining messages. Consumers should be idempotent when duplicate handling matters.

## Test

```bash
conda run -n tools-service-bus poetry run pytest -v
```

Tests use temporary directories and fakes for Azure clients, senders, batches, and SDK failures. They require no Azure credentials and make no network calls.
````

- [x] **Step 2: Verify documentation and safe examples mechanically**

Run:

```bash
conda run -n tools-service-bus poetry run python -c "from pathlib import Path; readme=Path('README.md').read_text(encoding='utf-8'); env=Path('.env.example').read_text(encoding='utf-8'); sample=Path('data/orders.json').read_text(encoding='utf-8'); required=('conda run -n tools-service-bus poetry install','conda run -n tools-service-bus poetry run service-bus-send','partial','duplicate','messages_sent','exit'); assert all(term.lower() in readme.lower() for term in required); assert 'AZURE_SERVICE_BUS_CONNECTION_STRING=' in env and 'SharedAccessKey=' not in env; assert 'A-1001' in sample and 'A-1002' in sample"
```

Expected GREEN outcome: exit `0` with no output; setup, execution, partial-send semantics, duplicate risk, summaries, exit behavior, safe environment names, and sample records are present.

- [x] **Step 3: Check package metadata after adding README**

Run:

```bash
conda run -n tools-service-bus poetry check
```

Expected GREEN outcome: exit `0` and valid Poetry metadata.

- [x] **Step 4: Conditionally commit this work unit**

Skip while Git is unavailable. After external Git initialization:

```bash
git add README.md .env.example data/orders.json
git commit -m "docs: explain service bus sender operations"
```

### Task 12: Run Focused And Full Offline Verification

**Files:**
- Verify: `pyproject.toml`
- Verify: `src/service_bus_sender/config.py`
- Verify: `src/service_bus_sender/files.py`
- Verify: `src/service_bus_sender/sender.py`
- Verify: `src/service_bus_sender/cli.py`
- Verify: `tests/test_config.py`
- Verify: `tests/test_files.py`
- Verify: `tests/test_sender.py`
- Verify: `tests/test_cli.py`
- Verify: `README.md`

**Work unit:** Produce final evidence that every approved behavior passes offline in the existing Conda environment.

**Rollback boundary:** Verification changes no files; if a command fails, return to the task owning that behavior rather than weakening its assertion.

- [x] **Step 1: Validate Poetry metadata and the locked install**

Run:

```bash
conda run -n tools-service-bus poetry check
conda run -n tools-service-bus poetry install
```

Expected outcome: both commands exit `0`; the lock file is current and the package installs under Python 3.11 or newer.

- [x] **Step 2: Run focused suites by responsibility**

Run:

```bash
conda run -n tools-service-bus poetry run pytest tests/test_config.py -v
conda run -n tools-service-bus poetry run pytest tests/test_files.py -v
conda run -n tools-service-bus poetry run pytest tests/test_sender.py -v
conda run -n tools-service-bus poetry run pytest tests/test_cli.py -v
```

Expected outcome: every command exits `0`; expected totals are `8`, `11`, `6`, and `11` passing tests respectively, with no skipped tests and no network access.

- [x] **Step 3: Run the complete suite in one process**

Run:

```bash
conda run -n tools-service-bus poetry run pytest -v
```

Expected outcome: exit `0` with `36 passed`, no failures, and no Azure credentials required.

- [x] **Step 4: Verify the package entry point and exact module count without running the sender**

Run:

```bash
conda run -n tools-service-bus poetry run python -c "from pathlib import Path; from service_bus_sender.cli import main; modules=sorted(path.name for path in Path('src/service_bus_sender').glob('*.py') if path.name != '__init__.py'); assert callable(main); assert modules == ['cli.py', 'config.py', 'files.py', 'sender.py']"
```

Expected outcome: exit `0` with no output; the source architecture contains exactly the four approved application modules.

## Acceptance-Criteria Traceability

| Approved acceptance criterion | Implemented and verified by |
| --- | --- |
| Poetry installation in `tools-service-bus` with Python 3.11+ | Tasks 1 and 12 |
| Entry point loads configuration and processes direct JSON files in sorted order | Tasks 1, 2, 4, 8, and 10 |
| `data/orders.json` targets queue `orders` | Tasks 1, 4, and 8 |
| Every valid object becomes one independently serialized `ServiceBusMessage` | Tasks 5, 6, and 8 |
| Complete invalid-file validation prevents sends and later files continue | Tasks 5 and 9 |
| SDK-created dynamic batching, rollover, final flush, and oversized-message failure | Tasks 6 and 7 |
| Successful and failed files remain unchanged | Tasks 1, 8, 9, and 11 |
| Summary includes file outcomes and exact partial confirmed counts | Tasks 7, 9, and 10 |
| Exit codes `0`, `1`, and `2` | Tasks 8, 9, and 10 |
| Operational logs exclude credentials, complete payloads, and raw exception text | Tasks 3, 7, 9, and 10 |
| Offline tests cover configuration, files, batching, continuation, summary, status, and security | Tasks 2 through 10 and Task 12 |
| README, safe `.env.example`, ignored local secrets, and manual file lifecycle | Tasks 1 and 11 |

## Implementation Risks

- Git work-unit commands remain unavailable until a maintainer initializes Git externally. This does not block implementation or offline verification.
- Azure queue existence, authorization, namespace policy, and real AMQP size overhead are intentionally not exercised by tests. The sender delegates capacity to SDK-created batches and models documented SDK failures with fakes.
- Confirmed batches cannot be rolled back when a later batch fails. Accurate partial counts and explicit duplicate documentation reduce operational ambiguity but do not create exactly-once delivery.
- Dependency versions are bounded by compatible major versions; `poetry.lock` records the exact versions resolved when Task 1 executes.

## Self-Review Record

- [x] Every design acceptance criterion maps to one or more implementation tasks in the traceability table.
- [x] Every production behavior is introduced by an observable RED test before its minimum GREEN implementation.
- [x] `Config`, `load_message_objects`, `send_objects`, `FileSendError`, `RunSummary`, `run`, and `main` retain the same names and signatures across later tasks.
- [x] Every Poetry, Python, pytest, install, and runtime command explicitly uses `conda run -n tools-service-bus`.
- [x] Tests use temporary paths and fakes only; no command in this plan invokes a live Azure sender.
- [x] Source architecture remains limited to `config.py`, `files.py`, `sender.py`, and `cli.py`, plus the package marker.
- [x] Git is identified as an external prerequisite, and all commit commands are explicitly conditional.
- [x] Documentation states unchanged-file, partial-send, at-least-once, duplicate, and manual lifecycle semantics.
