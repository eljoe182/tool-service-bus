import json
import logging
from io import StringIO
from pathlib import Path

import pytest
from azure.servicebus.exceptions import MessageSizeExceededError

from sender.cli import ArgumentParseError, RunSummary, format_summary, main, parse_topic, run
from shared.config import ConfigError, SenderConfig


class FakeBatch:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.messages: list[object] = []

    def add_message(self, message: object) -> None:
        if len(self.messages) >= self.capacity:
            raise MessageSizeExceededError(message="fake batch capacity exceeded")
        self.messages.append(message)


class FakeQueueSender:
    def __init__(
        self,
        queue_name: str,
        *,
        capacity: int = 100,
        fail_on_send: int | None = None,
        failure_text: str = "fake send failure",
        exit_failure: Exception | None = None,
    ) -> None:
        self.queue_name = queue_name
        self.capacity = capacity
        self.fail_on_send = fail_on_send
        self.failure_text = failure_text
        self.exit_failure = exit_failure
        self.entered = False
        self.exited = False
        self.messages_sent = 0
        self.send_attempts = 0
        self.sent_batches: list[list[object]] = []

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True
        if self.exit_failure is not None:
            raise self.exit_failure

    def create_message_batch(self) -> FakeBatch:
        return FakeBatch(self.capacity)

    def send_messages(self, batch: FakeBatch) -> None:
        self.send_attempts += 1
        if self.send_attempts == self.fail_on_send:
            raise RuntimeError(self.failure_text)
        self.sent_batches.append(list(batch.messages))
        self.messages_sent += len(batch.messages)


class FakeTopicSender(FakeQueueSender):
    def __init__(self, topic_name: str, **kwargs: object) -> None:
        super().__init__(topic_name, **kwargs)
        self.topic_name = topic_name


class FakeClient:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False
        self.queue_names: list[str] = []
        self.topic_names: list[str] = []
        self.senders: list[FakeQueueSender] = []
        self.sender_options: dict[str, dict[str, object]] = {}
        self.topic_sender_options: dict[str, dict[str, object]] = {}
        self.queue_creation_failures: dict[str, str] = {}
        self.topic_creation_failures: dict[str, str] = {}

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.exited = True

    def get_queue_sender(self, queue_name: str) -> FakeQueueSender:
        if queue_name in self.queue_creation_failures:
            raise RuntimeError(self.queue_creation_failures[queue_name])
        sender = FakeQueueSender(queue_name, **self.sender_options.get(queue_name, {}))
        self.queue_names.append(queue_name)
        self.senders.append(sender)
        return sender

    def get_topic_sender(self, *, topic_name: str) -> FakeTopicSender:
        if topic_name in self.topic_creation_failures:
            raise RuntimeError(self.topic_creation_failures[topic_name])
        sender = FakeTopicSender(
            topic_name, **self.topic_sender_options.get(topic_name, {})
        )
        self.topic_names.append(topic_name)
        self.senders.append(sender)
        return sender


class FakeClientFactory:
    def __init__(self, client: FakeClient | None = None) -> None:
        self.client = client or FakeClient()
        self.connection_strings: list[str] = []

    def __call__(self, connection_string: str) -> FakeClient:
        self.connection_strings.append(connection_string)
        return self.client


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def envelope(
    data: list[dict[str, object]], properties: dict[str, object] | None = None
) -> dict[str, object]:
    return {"properties": properties or {}, "data": data}


def make_config(data_dir: Path) -> SenderConfig:
    return SenderConfig(
        connection_string="Endpoint=sb://secret-marker/;SharedAccessKey=test-only",
        data_dir=data_dir,
        log_level=logging.INFO,
    )


def test_run_uses_one_client_and_sorted_sender_contexts_without_changing_files(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "b.json", envelope([{"n": 2}], {"source": "fixture"}))
    write_json(tmp_path / "a.json", envelope([{"n": 1}, {"n": 3}], {"source": "fixture"}))
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.success"),
    )

    assert summary == RunSummary(files=2, succeeded=2, failed=0, messages_sent=3)
    assert summary.exit_code == 0
    assert factory.connection_strings == [make_config(tmp_path).connection_string]
    assert factory.client.entered is True
    assert factory.client.exited is True
    assert factory.client.queue_names == ["a", "b"]
    assert [sender.entered for sender in factory.client.senders] == [True, True]
    assert [sender.exited for sender in factory.client.senders] == [True, True]
    assert [sender.messages_sent for sender in factory.client.senders] == [2, 1]
    assert [
        message.application_properties
        for sender in factory.client.senders
        for batch in sender.sent_batches
        for message in batch
    ] == [{"source": "fixture"}] * 3
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_run_sends_each_valid_file_to_one_topic_without_queue_senders_or_property_changes(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "dashboard.json", envelope([{"n": 1}], {"source": "fixture"}))
    write_json(tmp_path / "alerts.json", envelope([{"n": 2}], {"source": "fixture"}))
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    factory = FakeClientFactory()

    with caplog.at_level(logging.INFO, logger="test.topic-selection"):
        summary = run(
            make_config(tmp_path),
            topic="orders-events",
            client_factory=factory,
            logger=logging.getLogger("test.topic-selection"),
        )

    assert summary == RunSummary(files=2, succeeded=2, failed=0, messages_sent=2)
    assert factory.client.queue_names == []
    assert factory.client.topic_names == ["orders-events", "orders-events"]
    assert [sender.entered for sender in factory.client.senders] == [True, True]
    assert [sender.exited for sender in factory.client.senders] == [True, True]
    assert [
        message.application_properties
        for sender in factory.client.senders
        for batch in sender.sent_batches
        for message in batch
    ] == [{"source": "fixture"}, {"source": "fixture"}]
    assert "dashboard.json -> topic orders-events expected_subscription dashboard: sent 1 messages" in caplog.text
    assert "alerts.json -> topic orders-events expected_subscription alerts: sent 1 messages" in caplog.text
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_run_with_no_json_files_is_successful_and_sends_nothing(tmp_path: Path) -> None:
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.empty"),
    )

    assert summary == RunSummary(files=0, succeeded=0, failed=0, messages_sent=0)
    assert summary.exit_code == 0
    assert factory.connection_strings == [make_config(tmp_path).connection_string]
    assert factory.client.queue_names == []


def test_run_treats_an_empty_envelope_as_a_successful_zero_message_file(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "empty.json", envelope([]))
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.empty-array"),
    )

    assert summary == RunSummary(files=1, succeeded=1, failed=0, messages_sent=0)
    assert factory.client.queue_names == ["empty"]
    assert factory.client.senders[0].entered is True
    assert factory.client.senders[0].exited is True
    assert factory.client.senders[0].messages_sent == 0


def test_main_returns_zero_and_logs_the_exact_summary_for_success(
    tmp_path: Path, caplog
) -> None:
    factory = FakeClientFactory()

    with caplog.at_level(logging.INFO):
        exit_code = main(
            [],
            config_loader=lambda: make_config(tmp_path),
            client_factory=factory,
        )

    assert exit_code == 0
    assert format_summary(RunSummary(0, 0, 0, 0)) in caplog.text


def test_run_validates_the_complete_envelope_before_opening_its_sender_and_continues(
    tmp_path: Path,
) -> None:
    write_json(
        tmp_path / "a.json",
        {"properties": {"source": "fixture"}, "data": [{"valid": True}, 7]},
    )
    write_json(tmp_path / "b.json", envelope([{"sent": True}], {"source": "fixture"}))
    factory = FakeClientFactory()

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.validation-continuation"),
    )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert factory.client.queue_names == ["b"]


def test_run_continues_after_queue_sender_creation_failure(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", envelope([{"notSent": True}]))
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.queue_creation_failures["a"] = "Endpoint=secret-marker raw SDK detail"
    factory = FakeClientFactory(client)

    with caplog.at_level(logging.ERROR, logger="test.sender-creation"):
        summary = run(
            make_config(tmp_path),
            client_factory=factory,
            logger=logging.getLogger("test.sender-creation"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert client.queue_names == ["b"]
    assert "a.json -> a: RuntimeError" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "raw SDK detail" not in caplog.text


def test_run_continues_after_one_message_cannot_fit_an_empty_batch(
    tmp_path: Path,
) -> None:
    write_json(tmp_path / "a.json", envelope([{"tooLarge": True}]))
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.sender_options["a"] = {"capacity": 0}
    factory = FakeClientFactory(client)

    summary = run(
        make_config(tmp_path),
        client_factory=factory,
        logger=logging.getLogger("test.oversized"),
    )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert client.queue_names == ["a", "b"]
    assert client.senders[0].messages_sent == 0


def test_run_counts_partial_sends_continues_and_never_logs_secrets_or_payloads(
    tmp_path: Path, caplog
) -> None:
    payload_marker = "complete-payload-must-not-appear"
    write_json(
        tmp_path / "a.json",
        envelope([{"n": number, "marker": payload_marker} for number in range(1, 6)]),
    )
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.sender_options["a"] = {
        "capacity": 2,
        "fail_on_send": 2,
        "failure_text": (
            "Endpoint=sb://secret-marker/;SharedAccessKey=test-only "
            f"payload={payload_marker}"
        ),
    }
    factory = FakeClientFactory(client)

    with caplog.at_level(logging.INFO, logger="test.partial"):
        summary = run(
            make_config(tmp_path),
            client_factory=factory,
            logger=logging.getLogger("test.partial"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=3)
    assert summary.exit_code == 1
    assert client.queue_names == ["a", "b"]
    assert "a.json -> a: RuntimeError while sending batch 2" in caplog.text
    assert "b.json -> b: sent 1 messages" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "SharedAccessKey" not in caplog.text
    assert payload_marker not in caplog.text


def test_failed_file_log_includes_its_confirmed_count_with_multiple_files(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", envelope([{"n": number} for number in range(1, 6)]))
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.sender_options["a"] = {
        "capacity": 2,
        "fail_on_send": 2,
        "failure_text": "Endpoint=secret-marker raw SDK detail",
    }

    with caplog.at_level(logging.INFO, logger="test.failed-count"):
        summary = run(
            make_config(tmp_path),
            client_factory=FakeClientFactory(client),
            logger=logging.getLogger("test.failed-count"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=3)
    assert (
        "a.json -> a: RuntimeError while sending batch 2; messages_sent=2"
        in caplog.text
    )
    assert "b.json -> b: sent 1 messages" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "raw SDK detail" not in caplog.text


def test_run_preserves_primary_send_failure_when_cleanup_also_fails(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", envelope([{"n": number} for number in range(1, 6)]))
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.sender_options["a"] = {
        "capacity": 2,
        "fail_on_send": 2,
        "failure_text": "primary-secret raw send detail",
        "exit_failure": ValueError("cleanup-secret raw cleanup detail"),
    }

    with caplog.at_level(logging.INFO, logger="test.send-and-cleanup-failure"):
        summary = run(
            make_config(tmp_path),
            client_factory=FakeClientFactory(client),
            logger=logging.getLogger("test.send-and-cleanup-failure"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=3)
    assert (
        "a.json -> a: RuntimeError while sending batch 2; messages_sent=2"
        in caplog.text
    )
    assert "b.json -> b: sent 1 messages" in caplog.text
    assert "primary-secret" not in caplog.text
    assert "cleanup-secret" not in caplog.text


def test_run_retains_confirmed_count_when_cleanup_fails_after_success(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", envelope([{"n": 1}, {"n": 2}]))
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))
    client = FakeClient()
    client.sender_options["a"] = {
        "exit_failure": ValueError("cleanup-secret raw cleanup detail")
    }

    with caplog.at_level(logging.INFO, logger="test.cleanup-after-success"):
        summary = run(
            make_config(tmp_path),
            client_factory=FakeClientFactory(client),
            logger=logging.getLogger("test.cleanup-after-success"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=3)
    assert client.senders[0].messages_sent == 2
    assert (
        "a.json -> a: ValueError while opening or closing sender; "
        "messages_sent=2" in caplog.text
    )
    assert "b.json -> b: sent 1 messages" in caplog.text
    assert "cleanup-secret" not in caplog.text


def test_topic_run_validates_each_complete_envelope_before_sender_opening_and_continues(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "invalid.json", {"properties": {}, "data": [{"ok": True}, 7]})
    write_json(tmp_path / "dashboard.json", envelope([{"sent": True}], {"source": "fixture"}))
    factory = FakeClientFactory()

    with caplog.at_level(logging.INFO, logger="test.topic-validation"):
        summary = run(
            make_config(tmp_path),
            topic="orders-events",
            client_factory=factory,
            logger=logging.getLogger("test.topic-validation"),
        )

    assert summary == RunSummary(files=2, succeeded=1, failed=1, messages_sent=1)
    assert factory.client.queue_names == []
    assert factory.client.topic_names == ["orders-events"]
    assert "invalid.json -> topic orders-events expected_subscription invalid: InputFileError while validating input; messages_sent=0" in caplog.text
    assert "dashboard.json -> topic orders-events expected_subscription dashboard: sent 1 messages" in caplog.text


def test_topic_run_keeps_confirmed_partial_count_and_primary_safe_error_when_cleanup_fails(
    tmp_path: Path, caplog
) -> None:
    payload_marker = "complete-payload-must-not-appear"
    write_json(
        tmp_path / "dashboard.json",
        envelope([{"n": number, "marker": payload_marker} for number in range(1, 6)]),
    )
    client = FakeClient()
    client.topic_sender_options["orders-events"] = {
        "capacity": 2,
        "fail_on_send": 2,
        "failure_text": "Endpoint=sb://secret-marker primary-send-detail",
        "exit_failure": ValueError("cleanup-secret cleanup-detail"),
    }

    with caplog.at_level(logging.INFO, logger="test.topic-partial"):
        summary = run(
            make_config(tmp_path),
            topic="orders-events",
            client_factory=FakeClientFactory(client),
            logger=logging.getLogger("test.topic-partial"),
        )

    assert summary == RunSummary(files=1, succeeded=0, failed=1, messages_sent=2)
    assert summary.exit_code == 1
    assert client.queue_names == []
    assert client.topic_names == ["orders-events"]
    assert "dashboard.json -> topic orders-events expected_subscription dashboard: RuntimeError while sending batch 2; messages_sent=2" in caplog.text
    assert "secret-marker" not in caplog.text
    assert "primary-send-detail" not in caplog.text
    assert "cleanup-secret" not in caplog.text
    assert "cleanup-detail" not in caplog.text
    assert payload_marker not in caplog.text


def test_main_returns_one_and_logs_summary_when_any_file_fails(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "a.json", {"properties": {}, "data": [7]})
    write_json(tmp_path / "b.json", envelope([{"sent": True}]))

    with caplog.at_level(logging.INFO):
        exit_code = main(
            [],
            config_loader=lambda: make_config(tmp_path),
            client_factory=FakeClientFactory(),
        )

    assert exit_code == 1
    assert (
        "Service Bus send summary: files=2 succeeded=1 failed=1 messages_sent=1"
        in caplog.text
    )


def test_main_returns_two_with_zero_summary_for_configuration_failure(
    caplog,
) -> None:
    def invalid_config() -> SenderConfig:
        raise ConfigError(
            "Endpoint=sb://secret-marker/;SharedAccessKey=test-only"
        )

    with caplog.at_level(logging.ERROR):
        exit_code = main(
            [],
            config_loader=invalid_config,
            client_factory=FakeClientFactory(),
        )

    assert exit_code == 2
    assert "ConfigError while loading configuration" in caplog.text
    assert (
        "Service Bus send summary: files=0 succeeded=0 failed=0 messages_sent=0"
        in caplog.text
    )
    assert "secret-marker" not in caplog.text
    assert "SharedAccessKey" not in caplog.text


def test_main_returns_two_with_zero_summary_when_client_creation_fails(
    tmp_path: Path, caplog
) -> None:
    write_json(tmp_path / "orders.json", envelope([{"neverSent": True}]))

    def failing_client_factory(connection_string: str):
        raise RuntimeError(
            f"{connection_string} payload=complete-payload-must-not-appear"
        )

    with caplog.at_level(logging.ERROR):
        exit_code = main(
            [],
            config_loader=lambda: make_config(tmp_path),
            client_factory=failing_client_factory,
        )

    assert exit_code == 2
    assert "RuntimeError while starting or running sender" in caplog.text
    assert (
        "Service Bus send summary: files=0 succeeded=0 failed=0 messages_sent=0"
        in caplog.text
    )
    assert "secret-marker" not in caplog.text
    assert "complete-payload-must-not-appear" not in caplog.text


def test_parse_topic_keeps_queue_default_and_strips_a_valid_topic() -> None:
    assert parse_topic([]) is None
    assert parse_topic(["--topic", " orders-events "]) == "orders-events"


@pytest.mark.parametrize(
    ("argv", "expected_message"),
    [
        (["--topic", "   "], "--topic must be non-empty"),
        (["--topic"], "expected one argument"),
        (["--unknown"], "unrecognized arguments: --unknown"),
        (["--topic", "orders-events", "unexpected"], "unrecognized arguments: unexpected"),
    ],
)
def test_parse_topic_rejects_invalid_arguments(
    argv: list[str], expected_message: str
) -> None:
    with pytest.raises(ArgumentParseError, match=expected_message):
        parse_topic(argv)


@pytest.mark.parametrize("argv", [["--topic", "   "], ["--topic"]])
def test_main_rejects_invalid_topic_before_loading_configuration(argv: list[str], caplog) -> None:
    def config_loader() -> SenderConfig:
        raise AssertionError("configuration must not load for invalid topic")

    with caplog.at_level(logging.ERROR):
        exit_code = main(
            argv,
            config_loader=config_loader,
            client_factory=FakeClientFactory(),
        )

    assert exit_code == 2
    assert "ArgumentParseError while parsing arguments" in caplog.text
    assert "Service Bus send summary: files=0 succeeded=0 failed=0 messages_sent=0" in caplog.text


def test_main_prints_help_without_loading_configuration() -> None:
    stdout = StringIO()
    config_calls = 0

    def config_loader() -> SenderConfig:
        nonlocal config_calls
        config_calls += 1
        raise AssertionError("configuration must not load for help")

    assert main(["--help"], config_loader=config_loader, stdout=stdout) == 0
    assert config_calls == 0
    assert "usage: service-bus-send" in stdout.getvalue()


def test_readme_documents_queue_default_topic_sending_and_conda_commands() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "conda run -n tools-service-bus poetry run service-bus-send" in readme
    assert "--topic orders-events" in readme
    assert "get_queue_sender(queue_name=file_stem)" in readme
    assert "get_topic_sender(topic_name=topic)" in readme
    assert "expected subscription" in readme
    assert "not an Azure destination or application property" in readme
    assert "non-empty" in readme
