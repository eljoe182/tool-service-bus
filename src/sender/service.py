from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence

from azure.servicebus import ServiceBusMessage
from azure.servicebus.exceptions import MessageSizeExceededError

from shared.client import MessageBatch, QueueSender
from sender.files import ApplicationProperty


MessageFactory = Callable[[str, dict[str, ApplicationProperty]], object]
Sleeper = Callable[[float], None]


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


def _create_service_bus_message(
    body: str, properties: dict[str, ApplicationProperty]
) -> ServiceBusMessage:
    return ServiceBusMessage(body, application_properties=properties)


def _send_objects_with_delay(
    sender: QueueSender,
    objects: Sequence[Mapping[str, object]],
    properties: Mapping[str, ApplicationProperty],
    *,
    message_factory: MessageFactory,
    delay_seconds: float,
    sleeper: Sleeper,
) -> int:
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
