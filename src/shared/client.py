from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Protocol

from azure.servicebus import ServiceBusClient


class MessageBatch(Protocol):
    def add_message(self, message: object) -> None: ...


class QueueSender(Protocol):
    def create_message_batch(self) -> MessageBatch: ...

    def send_messages(self, batch: MessageBatch) -> None: ...


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
    def get_queue_sender(self, queue_name: str) -> AbstractContextManager[QueueSender]: ...

    def get_queue_receiver(
        self, *, queue_name: str
    ) -> AbstractContextManager[QueueReceiver]: ...

    def get_subscription_receiver(
        self, *, topic_name: str, subscription_name: str
    ) -> AbstractContextManager[QueueReceiver]: ...


ClientFactory = Callable[[str], AbstractContextManager[ServiceBusClientLike]]


def default_client_factory(
    connection_string: str,
) -> AbstractContextManager[ServiceBusClientLike]:
    return ServiceBusClient.from_connection_string(connection_string)
