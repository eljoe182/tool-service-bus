import logging
import os
from pathlib import Path

import pytest

from shared.config import (
    ConfigError,
    ReaderConfig,
    SenderConfig,
    load_reader_config,
    load_sender_config,
)


def test_load_reader_config_does_not_require_service_bus_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "reader-connection")
    monkeypatch.delenv("SERVICE_BUS_DATA_DIR", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    config = load_reader_config(cwd=tmp_path)

    assert config == ReaderConfig(connection_string="reader-connection", log_level=logging.INFO)


def test_load_sender_config_reads_dotenv_and_applies_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (tmp_path / ".env").write_text(
        "AZURE_SERVICE_BUS_CONNECTION_STRING=from-dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AZURE_SERVICE_BUS_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("SERVICE_BUS_DATA_DIR", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    config = load_sender_config(cwd=tmp_path)

    assert config == SenderConfig(
        connection_string="from-dotenv",
        data_dir=data_dir.resolve(),
        log_level=logging.INFO,
    )


def test_process_environment_overrides_dotenv_and_resolves_relative_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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

    config = load_sender_config(cwd=tmp_path)

    assert config.connection_string == "from-process"
    assert config.data_dir == fixture_dir.resolve()
    assert config.log_level == logging.DEBUG


@pytest.mark.parametrize("value", [None, "   "])
def test_reader_connection_string_must_be_non_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    if value is None:
        monkeypatch.delenv("AZURE_SERVICE_BUS_CONNECTION_STRING", raising=False)
    else:
        monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", value)

    with pytest.raises(ConfigError, match="AZURE_SERVICE_BUS_CONNECTION_STRING") as error:
        load_reader_config(cwd=tmp_path)

    assert value is None or value not in str(error.value)


def test_reader_invalid_log_level_is_a_safe_configuration_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "Endpoint=sb://secret-marker/;SharedAccessKey=do-not-log"
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", secret)
    monkeypatch.setenv("LOG_LEVEL", "verbose")

    with pytest.raises(ConfigError, match="LOG_LEVEL") as error:
        load_reader_config(cwd=tmp_path)

    assert secret not in str(error.value)


@pytest.mark.parametrize("kind", ["missing", "file", "unreadable"])
def test_sender_data_path_must_be_a_readable_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "safe-test-value")
    candidate = tmp_path / kind
    if kind == "file":
        candidate.write_text("not a directory", encoding="utf-8")
    elif kind == "unreadable":
        candidate.mkdir()
        real_access = os.access
        monkeypatch.setattr(
            "shared.config.os.access",
            lambda path, mode: False if Path(path) == candidate else real_access(path, mode),
        )
    monkeypatch.setenv("SERVICE_BUS_DATA_DIR", str(candidate))

    with pytest.raises(ConfigError, match="SERVICE_BUS_DATA_DIR"):
        load_sender_config(cwd=tmp_path)


def test_load_sender_config_still_requires_a_readable_data_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "sender-connection")
    monkeypatch.setenv("SERVICE_BUS_DATA_DIR", str(tmp_path / "missing"))

    with pytest.raises(ConfigError, match="SERVICE_BUS_DATA_DIR"):
        load_sender_config(cwd=tmp_path)
