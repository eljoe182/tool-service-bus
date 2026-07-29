import pytest
from azure.servicebus.exceptions import MessageSizeExceededError
from dataclasses import dataclass

from sender.service import FileSendError, send_objects


@dataclass
class FakeMessage:
    body: str
    application_properties: dict[str, object]


def fake_message_factory(body: str, properties: dict[str, object]) -> FakeMessage:
    return FakeMessage(body=body, application_properties=properties)


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


def test_send_objects_serializes_bodies_and_copies_properties_per_message() -> None:
    sender = FakeSender()
    properties = {"source": "fixture", "priority": 3, "retry": False, "note": None}

    sent_count = send_objects(
        sender,
        [{"orderId": "A-1"}, {"orderId": "A-2"}],
        properties,
        message_factory=fake_message_factory,
    )

    first, second = sender.sent_batches[0]
    assert sent_count == 2
    assert first.body == '{"orderId":"A-1"}'
    assert second.body == '{"orderId":"A-2"}'
    assert first.application_properties == properties
    assert second.application_properties == properties
    assert first.application_properties is not properties
    assert second.application_properties is not properties
    assert first.application_properties is not second.application_properties


def test_send_objects_serializes_each_object_as_one_compact_ordered_message() -> None:
    sender = FakeSender()
    customer_name = "Jos" + chr(233)

    sent_count = send_objects(
        sender,
        [
            {"orderId": "A-1", "status": "created"},
            {"customer": customer_name, "items": []},
        ],
        {},
        message_factory=lambda body, properties: body,
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

    sent_count = send_objects(sender, [], {}, message_factory=lambda body, properties: body)

    assert sent_count == 0
    assert len(sender.created_batches) == 1
    assert sender.sent_batches == []


def test_send_objects_rolls_full_batches_and_flushes_the_final_partial_batch() -> None:
    sender = FakeSender(capacity=2)

    sent_count = send_objects(
        sender,
        [{"n": number} for number in range(1, 6)],
        {},
        message_factory=lambda body, properties: body,
    )

    assert sent_count == 5
    assert sender.sent_batches == [
        ['{"n":1}', '{"n":2}'],
        ['{"n":3}', '{"n":4}'],
        ['{"n":5}'],
    ]
    assert sender.send_attempts == 3


def test_send_objects_preserves_properties_across_batch_rollover() -> None:
    sender = FakeSender(capacity=2)

    sent_count = send_objects(
        sender,
        [{"n": 1}, {"n": 2}, {"n": 3}],
        {"source": "fixture"},
        message_factory=fake_message_factory,
    )

    assert sent_count == 3
    assert [[message.body for message in batch] for batch in sender.sent_batches] == [
        ['{"n":1}', '{"n":2}'],
        ['{"n":3}'],
    ]
    assert [
        message.application_properties for batch in sender.sent_batches for message in batch
    ] == [{"source": "fixture"}, {"source": "fixture"}, {"source": "fixture"}]


def test_send_objects_fails_when_one_message_cannot_fit_a_fresh_batch() -> None:
    sender = FakeSender(capacity=0)

    with pytest.raises(FileSendError) as error:
        send_objects(
            sender,
            [{"oversized": True}],
            {},
            message_factory=lambda body, properties: body,
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
            {},
            message_factory=lambda body, properties: body,
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
            {},
            message_factory=lambda body, properties: body,
        )

    assert error.value.sent_count == 2
    assert error.value.batch_number == 2
    assert error.value.operation == "sending"
    assert error.value.error_type == "RuntimeError"
    assert sender.sent_batches == [['{"n":1}', '{"n":2}']]
    assert "secret-marker" not in str(error.value)
    assert "complete-object" not in str(error.value)
