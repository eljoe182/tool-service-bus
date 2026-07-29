from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from typing import TextIO

from reader.service import QueueReadError, ReadRequest, read_messages
from shared.client import ClientFactory, default_client_factory
from shared.config import ReaderConfig, load_reader_config


class ArgumentParseError(ValueError):
    pass


class ReaderArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ArgumentParseError(message)


ConfigLoader = Callable[[], ReaderConfig]


def _non_empty_queue(value: str) -> str:
    queue_name = value.strip()
    if not queue_name:
        raise argparse.ArgumentTypeError("--queue must be non-empty")
    return queue_name


def _non_empty_subscription(value: str) -> str:
    subscription_name = value.strip()
    if not subscription_name:
        raise argparse.ArgumentTypeError("--subscription must be non-empty")
    return subscription_name


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
    parser.add_argument("--entity-type", choices=("queue", "topic"), default="queue")
    parser.add_argument("--subscription", type=_non_empty_subscription)
    namespace = parser.parse_args(argv)
    if namespace.entity_type == "topic" and namespace.subscription is None:
        parser.error("--subscription is required when --entity-type topic")
    if namespace.entity_type == "queue" and namespace.subscription is not None:
        parser.error("--subscription is only valid when --entity-type topic")
    return ReadRequest(
        queue_name=namespace.queue,
        count=namespace.count,
        mode=namespace.mode,
        entity_type=namespace.entity_type,
        subscription_name=namespace.subscription,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    config_loader: ConfigLoader = load_reader_config,
    client_factory: ClientFactory = default_client_factory,
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
            f"{error.error_type} while {error.operation} {error.entity_description}\n"
        )
        return 2
    return 0
