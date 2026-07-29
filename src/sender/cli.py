from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TextIO

from shared.client import ClientFactory, default_client_factory
from shared.config import SenderConfig, load_sender_config
from sender.files import (
    InputFileError,
    derive_queue_name,
    discover_json_files,
    load_message_envelope,
)
from sender.service import FileSendError, send_objects


_LOGGER = logging.getLogger("sender")


ConfigLoader = Callable[[], SenderConfig]


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
    config: SenderConfig,
    *,
    client_factory: ClientFactory = default_client_factory,
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
            except InputFileError as error:
                failed += 1
                logger.error(
                    "%s -> %s: %s while validating input; messages_sent=%d",
                    path.name,
                    queue_name,
                    type(error).__name__,
                    0,
                )
                continue
            except FileSendError as error:
                failed += 1
                messages_sent += error.sent_count
                logger.error(
                    "%s -> %s: %s while %s batch %d; messages_sent=%d",
                    path.name,
                    queue_name,
                    error.error_type,
                    error.operation,
                    error.batch_number,
                    error.sent_count,
                )
                continue
            except Exception as error:
                failed += 1
                messages_sent += sent_for_file
                logger.error(
                    "%s -> %s: %s while opening or closing queue sender; "
                    "messages_sent=%d",
                    path.name,
                    queue_name,
                    type(error).__name__,
                    sent_for_file,
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


def _configure_logging(level: int) -> None:
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def main(
    argv: Sequence[str] | None = None,
    *,
    config_loader: ConfigLoader = load_sender_config,
    client_factory: ClientFactory = default_client_factory,
    stdout: TextIO = sys.stdout,
) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if arguments == ["--help"]:
        stdout.write("usage: service-bus-send\n")
        return 0
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
