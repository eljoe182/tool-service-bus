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
    data_dir = (
        configured_data_dir
        if configured_data_dir.is_absolute()
        else base_dir / configured_data_dir
    ).resolve()
    if not data_dir.is_dir() or not os.access(data_dir, os.R_OK):
        raise ConfigError("SERVICE_BUS_DATA_DIR must be a readable directory")
    return SenderConfig(
        connection_string=connection_string,
        data_dir=data_dir,
        log_level=log_level,
    )
