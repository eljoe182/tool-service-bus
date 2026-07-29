from __future__ import annotations

import base64
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TextIO

from shared.client import (
    ClientFactory,
    QueueReceiver,
    ReceivedMessage,
    default_client_factory,
)
from shared.config import ReaderConfig


@dataclass(frozen=True, slots=True)
class ReadRequest:
    queue_name: str
    count: int
    mode: str
    entity_type: str = "queue"
    subscription_name: str | None = None


@dataclass(frozen=True, slots=True)
class ReadResult:
    message_count: int


class QueueReadError(RuntimeError):
    def __init__(
        self, *, operation: str, request: ReadRequest, cause: BaseException
    ) -> None:
        self.operation = operation
        self.queue_name = request.queue_name
        self.entity_type = request.entity_type
        self.subscription_name = request.subscription_name
        self.error_type = type(cause).__name__
        super().__init__(f"{self.error_type} while {operation} {self.entity_description}")

    @property
    def entity_description(self) -> str:
        if self.entity_type == "topic":
            return f"topic {self.queue_name} subscription {self.subscription_name}"
        return f"queue {self.queue_name}"


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
    config: ReaderConfig,
    request: ReadRequest,
    *,
    client_factory: ClientFactory = default_client_factory,
    stdout: TextIO,
    stderr: TextIO,
) -> ReadResult:
    operation = "creating client"
    try:
        with client_factory(config.connection_string) as client:
            operation = "opening receiver"
            if request.entity_type == "topic":
                receiver_context = client.get_subscription_receiver(
                    topic_name=request.queue_name,
                    subscription_name=request.subscription_name,
                )
            else:
                receiver_context = client.get_queue_receiver(queue_name=request.queue_name)
            with receiver_context as receiver:
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
                        operation = "flushing stdout"
                        stdout.flush()
                        operation = "completing message"
                        receiver.complete_message(message)
                operation = "closing receiver"
            operation = "closing client"
    except QueueReadError:
        raise
    except Exception as error:
        raise QueueReadError(
            operation=operation, request=request, cause=error
        ) from error

    try:
        stderr.write(f"Read {len(messages)} messages\n")
    except Exception as error:
        raise QueueReadError(
            operation="writing result count", request=request, cause=error
        ) from error
    return ReadResult(message_count=len(messages))
