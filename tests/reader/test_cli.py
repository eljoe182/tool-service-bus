from contextlib import AbstractContextManager
from io import StringIO
from pathlib import Path

import pytest

from reader.cli import main, parse_request
from reader.service import ReadRequest
from shared.config import ConfigError, ReaderConfig


class FakeMessage:
    def __init__(self, body: object) -> None:
        self.body = body


class FakeReceiver(AbstractContextManager["FakeReceiver"]):
    def __init__(self) -> None:
        self.received: list[FakeMessage] = []

    def __enter__(self) -> "FakeReceiver":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def peek_messages(self, *, max_message_count: int) -> list[FakeMessage]:
        return self.received

    def receive_messages(
        self, *, max_message_count: int, max_wait_time: int
    ) -> list[FakeMessage]:
        return self.received

    def complete_message(self, message: FakeMessage) -> None:
        return None


class FakeClient(AbstractContextManager["FakeClient"]):
    def __init__(self, receiver: FakeReceiver) -> None:
        self.receiver = receiver
        self.queue_names: list[str] = []
        self.subscription_names: list[tuple[str, str]] = []

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

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

    def __call__(self, connection_string: str) -> FakeClient:
        return self.client


def make_config() -> ReaderConfig:
    return ReaderConfig(connection_string="test-connection", log_level=20)


@pytest.mark.parametrize(
    "argv",
    [
        ["--count", "1", "--mode", "peek"],
        ["--queue", "   ", "--count", "1", "--mode", "peek"],
        ["--queue", "orders", "--count", "0", "--mode", "peek"],
        ["--queue", "orders", "--count", "one", "--mode", "peek"],
        ["--queue", "orders", "--count", "1", "--mode", "PEEK"],
    ],
)
def test_main_rejects_invalid_required_arguments_before_loading_config(
    argv: list[str],
) -> None:
    config_calls = 0

    def config_loader() -> ReaderConfig:
        nonlocal config_calls
        config_calls += 1
        raise AssertionError("configuration must not be loaded")

    stderr = StringIO()
    exit_code = main(argv, config_loader=config_loader, stderr=stderr, stdout=StringIO())

    assert exit_code == 2
    assert config_calls == 0
    assert "argument error" in stderr.getvalue()


def test_parse_request_accepts_only_the_contract_values() -> None:
    request = parse_request(["--queue", "orders", "--count", "2", "--mode", "drain"])

    assert request.queue_name == "orders"
    assert request.count == 2
    assert request.mode == "drain"


@pytest.mark.parametrize(
    ("argv", "expected_message"),
    [
        (
            [
                "--queue",
                "indicators",
                "--count",
                "1",
                "--mode",
                "peek",
                "--entity-type",
                "topic",
            ],
            "--subscription is required when --entity-type topic",
        ),
        (
            [
                "--queue",
                "indicators",
                "--count",
                "1",
                "--mode",
                "peek",
                "--entity-type",
                "topic",
                "--subscription",
                "   ",
            ],
            "--subscription must be non-empty",
        ),
        (
            [
                "--queue",
                "orders",
                "--count",
                "1",
                "--mode",
                "peek",
                "--subscription",
                "dashboard",
            ],
            "--subscription is only valid when --entity-type topic",
        ),
    ],
)
def test_main_rejects_invalid_entity_subscription_pairs_before_loading_config(
    argv: list[str], expected_message: str
) -> None:
    def config_loader() -> ReaderConfig:
        raise AssertionError("configuration must not be loaded")

    stderr = StringIO()
    assert main(argv, config_loader=config_loader, stdout=StringIO(), stderr=stderr) == 2
    assert expected_message in stderr.getvalue()


def test_parse_request_keeps_existing_queue_contract_and_accepts_topic_contract() -> None:
    queue_request = parse_request(
        ["--queue", "orders", "--count", "2", "--mode", "drain"]
    )
    topic_request = parse_request(
        [
            "--queue",
            "sbt-local-indicators",
            "--count",
            "2",
            "--mode",
            "peek",
            "--entity-type",
            "topic",
            "--subscription",
            "dashboard",
        ]
    )

    assert queue_request == ReadRequest(queue_name="orders", count=2, mode="drain")
    assert topic_request == ReadRequest(
        queue_name="sbt-local-indicators",
        count=2,
        mode="peek",
        entity_type="topic",
        subscription_name="dashboard",
    )


def test_main_returns_two_for_safe_configuration_failure() -> None:
    stderr = StringIO()

    exit_code = main(
        ["--queue", "orders", "--count", "1", "--mode", "peek"],
        config_loader=lambda: (_ for _ in ()).throw(
            ConfigError("Endpoint=sb://secret-marker raw configuration detail")
        ),
        stdout=StringIO(),
        stderr=stderr,
    )

    assert exit_code == 2
    assert "ConfigError while loading configuration" in stderr.getvalue()
    assert "secret-marker" not in stderr.getvalue()
    assert "raw configuration detail" not in stderr.getvalue()


def test_main_returns_two_for_receiver_failure_without_leaking_body_or_connection() -> None:
    receiver = FakeReceiver()
    receiver.received = [FakeMessage(b"unprinted-body")]

    def failing_receive(*, max_message_count: int, max_wait_time: int) -> list[FakeMessage]:
        raise RuntimeError("Endpoint=sb://secret-marker payload=unprinted-body")

    receiver.receive_messages = failing_receive
    stderr = StringIO()
    exit_code = main(
        ["--queue", "orders", "--count", "1", "--mode", "block"],
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(receiver)),
        stdout=StringIO(),
        stderr=stderr,
    )

    assert exit_code == 2
    assert "RuntimeError while receiving messages queue orders" in stderr.getvalue()
    assert "secret-marker" not in stderr.getvalue()
    assert "unprinted-body" not in stderr.getvalue()


def test_main_reads_topic_subscription_with_existing_peek_output() -> None:
    receiver = FakeReceiver()
    receiver.received = [FakeMessage(b"indicator")]
    client = FakeClient(receiver)
    stdout = StringIO()
    stderr = StringIO()

    exit_code = main(
        [
            "--queue",
            "sbt-local-indicators",
            "--count",
            "1",
            "--mode",
            "peek",
            "--entity-type",
            "topic",
            "--subscription",
            "dashboard",
        ],
        config_loader=make_config,
        client_factory=FakeClientFactory(client),
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert client.queue_names == []
    assert client.subscription_names == [("sbt-local-indicators", "dashboard")]
    assert stdout.getvalue() == "indicator\n"
    assert stderr.getvalue() == "Read 1 messages\n"


def test_main_sanitizes_topic_receiver_error_with_subscription_context() -> None:
    receiver = FakeReceiver()

    def failing_receive(
        *, max_message_count: int, max_wait_time: int
    ) -> list[FakeMessage]:
        raise RuntimeError("Endpoint=sb://secret-marker payload=unprinted-body")

    receiver.receive_messages = failing_receive
    stderr = StringIO()
    exit_code = main(
        [
            "--queue",
            "sbt-local-indicators",
            "--count",
            "1",
            "--mode",
            "block",
            "--entity-type",
            "topic",
            "--subscription",
            "dashboard",
        ],
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(receiver)),
        stdout=StringIO(),
        stderr=stderr,
    )

    assert exit_code == 2
    assert (
        "RuntimeError while receiving messages topic sbt-local-indicators "
        "subscription dashboard"
    ) in stderr.getvalue()
    assert "secret-marker" not in stderr.getvalue()
    assert "unprinted-body" not in stderr.getvalue()


class FailingStream(StringIO):
    def write(self, value: str) -> int:
        raise OSError("Endpoint=sb://secret-marker payload=unprinted-body")


def test_main_returns_two_for_client_render_write_and_settlement_failures() -> None:
    arguments = ["--queue", "orders", "--count", "1", "--mode", "drain"]

    def failing_client_factory(connection_string: str) -> FakeClient:
        raise RuntimeError("Endpoint=sb://secret-marker client detail")

    client_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=failing_client_factory,
        stdout=StringIO(),
        stderr=client_error,
    ) == 2
    assert "RuntimeError while creating client queue orders" in client_error.getvalue()
    assert "secret-marker" not in client_error.getvalue()

    render_receiver = FakeReceiver()
    render_receiver.received = [FakeMessage(object())]
    render_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(render_receiver)),
        stdout=StringIO(),
        stderr=render_error,
    ) == 2
    assert "TypeError while rendering message queue orders" in render_error.getvalue()

    write_receiver = FakeReceiver()
    write_receiver.received = [FakeMessage(b"unprinted-body")]
    write_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(write_receiver)),
        stdout=FailingStream(),
        stderr=write_error,
    ) == 2
    assert "OSError while writing message queue orders" in write_error.getvalue()
    assert "unprinted-body" not in write_error.getvalue()

    settlement_receiver = FakeReceiver()
    settlement_receiver.received = [FakeMessage(b"visible")]

    def failing_complete(message: FakeMessage) -> None:
        raise RuntimeError("Endpoint=sb://secret-marker settlement detail")

    settlement_receiver.complete_message = failing_complete
    settlement_error = StringIO()
    assert main(
        arguments,
        config_loader=make_config,
        client_factory=FakeClientFactory(FakeClient(settlement_receiver)),
        stdout=StringIO(),
        stderr=settlement_error,
    ) == 2
    assert (
        "RuntimeError while completing message queue orders" in settlement_error.getvalue()
    )
    assert "secret-marker" not in settlement_error.getvalue()


def test_project_registers_flat_sender_and_reader_packages() -> None:
    project_root = Path(__file__).resolve().parents[2]
    pyproject = project_root.joinpath("pyproject.toml").read_text(encoding="utf-8")

    assert 'service-bus-send = "sender.cli:main"' in pyproject
    assert 'service-bus-read = "reader.cli:main"' in pyproject
    assert '{ include = "shared", from = "src" }' in pyproject
    assert '{ include = "sender", from = "src" }' in pyproject
    assert '{ include = "reader", from = "src" }' in pyproject
    assert "service_bus" + "_sender" not in pyproject


def test_readme_documents_all_reader_modes_and_conda_command() -> None:
    project_root = Path(__file__).resolve().parents[2]
    readme = project_root.joinpath("README.md").read_text(encoding="utf-8")

    assert "service-bus-read" in readme
    assert "conda run -n tools-service-bus poetry run service-bus-read" in readme
    assert "peek" in readme
    assert "block" in readme
    assert "drain" in readme
    assert "destructive" in readme
    assert "standard error" in readme
    assert "--entity-type topic" in readme
    assert "--subscription dashboard" in readme
    assert "get_subscription_receiver" in readme
    assert "defaults to `queue`" in readme
    assert "required for `topic`" in readme
    assert "rejected for `queue`" in readme
