from contextlib import AbstractContextManager
from io import StringIO

import pytest

from reader.service import (
    QueueReadError,
    ReadRequest,
    read_messages,
    render_message_body,
)
from shared.config import ReaderConfig


class FakeMessage:
    def __init__(self, body: object) -> None:
        self.body = body


class FakeReceiver(AbstractContextManager["FakeReceiver"]):
    def __init__(
        self,
        *,
        peeked: list[FakeMessage] | None = None,
        received: list[FakeMessage] | None = None,
        exit_failure: Exception | None = None,
    ) -> None:
        self.peeked = peeked or []
        self.received = received or []
        self.peek_calls: list[int] = []
        self.receive_calls: list[tuple[int, int]] = []
        self.completed: list[FakeMessage] = []
        self.abandoned: list[FakeMessage] = []
        self.entered = False
        self.exited = False
        self.exit_failure = exit_failure

    def __enter__(self) -> "FakeReceiver":
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.exited = True
        if self.exit_failure is not None:
            raise self.exit_failure

    def peek_messages(self, *, max_message_count: int) -> list[FakeMessage]:
        self.peek_calls.append(max_message_count)
        return self.peeked

    def receive_messages(
        self, *, max_message_count: int, max_wait_time: int
    ) -> list[FakeMessage]:
        self.receive_calls.append((max_message_count, max_wait_time))
        return self.received

    def complete_message(self, message: FakeMessage) -> None:
        self.completed.append(message)

    def abandon_message(self, message: FakeMessage) -> None:
        self.abandoned.append(message)


class FakeClient(AbstractContextManager["FakeClient"]):
    def __init__(
        self, receiver: FakeReceiver, *, exit_failure: Exception | None = None
    ) -> None:
        self.receiver = receiver
        self.queue_names: list[str] = []
        self.subscription_names: list[tuple[str, str]] = []
        self.entered = False
        self.exited = False
        self.exit_failure = exit_failure

    def __enter__(self) -> "FakeClient":
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.exited = True
        if self.exit_failure is not None:
            raise self.exit_failure

    def get_queue_receiver(self, *, queue_name: str) -> FakeReceiver:
        self.queue_names.append(queue_name)
        return self.receiver

    def get_subscription_receiver(
        self, *, topic_name: str, subscription_name: str
    ) -> FakeReceiver:
        self.subscription_names.append((topic_name, subscription_name))
        return self.receiver


class FakeClientFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.connection_strings: list[str] = []

    def __call__(self, connection_string: str) -> FakeClient:
        self.connection_strings.append(connection_string)
        return self.client


class RecordingStream(StringIO):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def write(self, value: str) -> int:
        self.events.append(f"write:{value}")
        return super().write(value)

    def flush(self) -> None:
        self.events.append("flush")
        super().flush()


def make_config() -> ReaderConfig:
    return ReaderConfig(connection_string="test-connection", log_level=20)


def test_peek_prints_utf8_bodies_and_count_without_settlement() -> None:
    receiver = FakeReceiver(peeked=[FakeMessage(b"first"), FakeMessage("second")])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=5, mode="peek"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 2
    assert factory.connection_strings == ["test-connection"]
    assert factory.client.queue_names == ["orders"]
    assert receiver.peek_calls == [5]
    assert receiver.receive_calls == []
    assert receiver.completed == []
    assert receiver.abandoned == []
    assert stdout.getvalue() == "first\nsecond\n"
    assert stderr.getvalue() == "Read 2 messages\n"
    assert factory.client.entered is True
    assert factory.client.exited is True
    assert receiver.entered is True
    assert receiver.exited is True


def test_topic_peek_uses_subscription_receiver_without_settlement() -> None:
    receiver = FakeReceiver(peeked=[FakeMessage(b"indicator")])
    client = FakeClient(receiver)
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(
            queue_name="sbt-local-indicators",
            count=2,
            mode="peek",
            entity_type="topic",
            subscription_name="dashboard",
        ),
        client_factory=FakeClientFactory(client),
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 1
    assert client.queue_names == []
    assert client.subscription_names == [("sbt-local-indicators", "dashboard")]
    assert receiver.peek_calls == [2]
    assert receiver.receive_calls == []
    assert receiver.completed == []
    assert stdout.getvalue() == "indicator\n"
    assert stderr.getvalue() == "Read 1 messages\n"
    assert receiver.exited is True
    assert client.exited is True


def test_render_message_body_uses_deterministic_binary_and_single_line_text() -> None:
    assert render_message_body(b"\xff\x00") == "base64:/wA="
    assert render_message_body([b"caf", b"\xc3\xa9"]) == "café"
    assert render_message_body("first\nsecond\rthird") == "first\\nsecond\\rthird"


def test_block_receives_with_exact_parameters_and_never_settles() -> None:
    receiver = FakeReceiver(received=[FakeMessage(b"locked")])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=3, mode="block"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 1
    assert receiver.peek_calls == []
    assert receiver.receive_calls == [(3, 10)]
    assert receiver.completed == []
    assert receiver.abandoned == []
    assert stdout.getvalue() == "locked\n"
    assert stderr.getvalue() == "Read 1 messages\n"


def test_topic_block_receives_without_settlement() -> None:
    message = FakeMessage(b"locked")
    receiver = FakeReceiver(received=[message])
    client = FakeClient(receiver)

    result = read_messages(
        make_config(),
        ReadRequest(
            queue_name="sbt-local-indicators",
            count=3,
            mode="block",
            entity_type="topic",
            subscription_name="dashboard",
        ),
        client_factory=FakeClientFactory(client),
        stdout=StringIO(),
        stderr=StringIO(),
    )

    assert result.message_count == 1
    assert client.queue_names == []
    assert client.subscription_names == [("sbt-local-indicators", "dashboard")]
    assert receiver.peek_calls == []
    assert receiver.receive_calls == [(3, 10)]
    assert receiver.completed == []
    assert receiver.abandoned == []


def test_drain_writes_each_message_before_completing_it() -> None:
    first = FakeMessage(b"one")
    second = FakeMessage(b"two")
    receiver = FakeReceiver(received=[first, second])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=2, mode="drain"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 2
    assert receiver.receive_calls == [(2, 10)]
    assert stdout.getvalue() == "one\ntwo\n"
    assert receiver.completed == [first, second]
    assert receiver.abandoned == []
    assert stderr.getvalue() == "Read 2 messages\n"


def test_topic_drain_flushes_output_before_completing_each_message() -> None:
    first = FakeMessage(b"one")
    second = FakeMessage(b"two")
    receiver = FakeReceiver(received=[first, second])
    client = FakeClient(receiver)
    events: list[str] = []
    original_complete = receiver.complete_message

    def complete_message(message: FakeMessage) -> None:
        events.append(f"complete:{message.body.decode()}")
        original_complete(message)

    receiver.complete_message = complete_message
    stdout = RecordingStream(events)

    result = read_messages(
        make_config(),
        ReadRequest(
            queue_name="sbt-local-indicators",
            count=2,
            mode="drain",
            entity_type="topic",
            subscription_name="dashboard",
        ),
        client_factory=FakeClientFactory(client),
        stdout=stdout,
        stderr=StringIO(),
    )

    assert result.message_count == 2
    assert client.queue_names == []
    assert client.subscription_names == [("sbt-local-indicators", "dashboard")]
    assert receiver.receive_calls == [(2, 10)]
    assert receiver.completed == [first, second]
    assert events == [
        "write:one\n",
        "flush",
        "complete:one",
        "write:two\n",
        "flush",
        "complete:two",
    ]
    assert stdout.getvalue() == "one\ntwo\n"


def test_empty_queue_is_successful() -> None:
    receiver = FakeReceiver(received=[])
    factory = FakeClientFactory(FakeClient(receiver))
    stdout = StringIO()
    stderr = StringIO()

    result = read_messages(
        make_config(),
        ReadRequest(queue_name="orders", count=1, mode="drain"),
        client_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert result.message_count == 0
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == "Read 0 messages\n"


class FailingStream(StringIO):
    def write(self, value: str) -> int:
        raise OSError("Endpoint=sb://secret-marker payload=unprinted-body")


class FailingFlushStream(StringIO):
    def flush(self) -> None:
        raise OSError("Endpoint=sb://secret-marker flush detail")


class FailingSecondFlushStream(StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self) -> None:
        self.flushes += 1
        if self.flushes == 2:
            raise OSError("Endpoint=sb://secret-marker flush detail")
        super().flush()


class FailingSecondWriteStream(StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.writes = 0

    def write(self, value: str) -> int:
        self.writes += 1
        if self.writes == 2:
            raise OSError("Endpoint=sb://secret-marker payload=unprinted-body")
        return super().write(value)


def test_drain_does_not_complete_message_when_stdout_write_fails() -> None:
    message = FakeMessage(b"unprinted-body")
    receiver = FakeReceiver(received=[message])
    factory = FakeClientFactory(FakeClient(receiver))

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="drain"),
            client_factory=factory,
            stdout=FailingStream(),
            stderr=StringIO(),
        )

    assert receiver.completed == []
    assert receiver.abandoned == []
    assert error.value.operation == "writing message"
    assert "secret-marker" not in str(error.value)
    assert "unprinted-body" not in str(error.value)
    assert factory.client.exited is True
    assert receiver.exited is True


def test_drain_does_not_complete_message_when_stdout_flush_fails() -> None:
    message = FakeMessage(b"visible")
    receiver = FakeReceiver(received=[message])

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=FailingFlushStream(),
            stderr=StringIO(),
        )

    assert receiver.completed == []
    assert error.value.operation == "flushing stdout"
    assert error.value.error_type == "OSError"
    assert "secret-marker" not in str(error.value)
    assert "flush detail" not in str(error.value)


def test_drain_preserves_earlier_completion_when_later_stdout_flush_fails() -> None:
    first = FakeMessage(b"completed")
    second = FakeMessage(b"visible")
    receiver = FakeReceiver(received=[first, second])

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=2, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=FailingSecondFlushStream(),
            stderr=StringIO(),
        )

    assert receiver.completed == [first]
    assert error.value.operation == "flushing stdout"


def test_drain_preserves_earlier_completion_when_later_stdout_write_fails() -> None:
    first = FakeMessage(b"completed")
    second = FakeMessage(b"unprinted-body")
    receiver = FakeReceiver(received=[first, second])

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=2, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=FailingSecondWriteStream(),
            stderr=StringIO(),
        )

    assert receiver.completed == [first]
    assert error.value.operation == "writing message"


def test_drain_preserves_earlier_completion_when_later_completion_fails() -> None:
    first = FakeMessage(b"first")
    second = FakeMessage(b"second")
    receiver = FakeReceiver(received=[first, second])
    original_complete = receiver.complete_message

    def complete_message(message: FakeMessage) -> None:
        if message is second:
            raise RuntimeError("Endpoint=sb://secret-marker payload=second")
        original_complete(message)

    receiver.complete_message = complete_message
    stdout = StringIO()

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=2, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=stdout,
            stderr=StringIO(),
        )

    assert stdout.getvalue() == "first\nsecond\n"
    assert receiver.completed == [first]
    assert error.value.operation == "completing message"
    assert "secret-marker" not in str(error.value)
    assert "second" not in str(error.value)


def test_render_failure_does_not_write_or_complete_the_message() -> None:
    message = FakeMessage(object())
    receiver = FakeReceiver(received=[message])
    stdout = StringIO()

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="drain"),
            client_factory=FakeClientFactory(FakeClient(receiver)),
            stdout=stdout,
            stderr=StringIO(),
        )

    assert stdout.getvalue() == ""
    assert receiver.completed == []
    assert error.value.operation == "rendering message"
    assert error.value.error_type == "TypeError"


def test_receiver_cleanup_failure_is_safe_and_client_still_closes() -> None:
    receiver = FakeReceiver(
        received=[FakeMessage(b"visible")],
        exit_failure=RuntimeError("Endpoint=sb://secret-marker cleanup detail"),
    )
    client = FakeClient(receiver)

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(queue_name="orders", count=1, mode="block"),
            client_factory=FakeClientFactory(client),
            stdout=StringIO(),
            stderr=StringIO(),
        )

    assert receiver.exited is True
    assert client.exited is True
    assert error.value.operation == "closing receiver"
    assert "secret-marker" not in str(error.value)
    assert "cleanup detail" not in str(error.value)


def test_topic_receiver_cleanup_error_is_sanitized_and_closes_client() -> None:
    receiver = FakeReceiver(
        received=[FakeMessage(b"visible")],
        exit_failure=RuntimeError("Endpoint=sb://secret-marker cleanup detail"),
    )
    client = FakeClient(receiver)

    with pytest.raises(QueueReadError) as error:
        read_messages(
            make_config(),
            ReadRequest(
                queue_name="sbt-local-indicators",
                count=1,
                mode="block",
                entity_type="topic",
                subscription_name="dashboard",
            ),
            client_factory=FakeClientFactory(client),
            stdout=StringIO(),
            stderr=StringIO(),
        )

    assert error.value.operation == "closing receiver"
    assert error.value.entity_type == "topic"
    assert error.value.queue_name == "sbt-local-indicators"
    assert error.value.subscription_name == "dashboard"
    assert str(error.value) == (
        "RuntimeError while closing receiver topic sbt-local-indicators "
        "subscription dashboard"
    )
    assert "secret-marker" not in str(error.value)
    assert "cleanup detail" not in str(error.value)
    assert receiver.exited is True
    assert client.exited is True
