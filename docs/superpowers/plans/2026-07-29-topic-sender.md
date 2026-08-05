# Optional Topic Sender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `service-bus-send` with an optional nonblank `--topic` so valid input files publish to one Azure Service Bus topic while the existing file-stem queue behavior remains the default.

**Architecture:** Keep envelope decoding, validation, batching, message construction, and accounting in their current modules. Add the topic-sender factory to the shared client protocol, pass an optional validated topic from the CLI into `run`, and choose exactly one sender context per fully validated file: queue by file stem when omitted, topic by `topic_name` when present. In topic mode, use the stem only in a safe destination label as the expected subscription; never pass it to Azure or add it to application properties.

**Tech Stack:** Python 3.11+, Poetry 2, `azure-servicebus` 7.x, `pytest` 8.x, and standard-library `argparse`, `dataclasses`, `logging`, and `pathlib`.

---

## Quick Path

1. Add the typed topic-sender protocol and a direct `run(..., topic=...)` tracer test that proves topic selection, queue compatibility, and unchanged message properties.
2. Add `--topic` parsing with fail-fast validation before configuration loading.
3. Add topic-mode failure, cleanup, partial-count, continuation, and safe-log coverage using only fakes.
4. Document both send forms, then run the complete offline suite and metadata validation.

## Execution Prerequisites

- Work from `/Users/garciajoise/Projects/esmax/tools/service-bus`.
- Run every command exactly through `conda run -n tools-service-bus`; all tests use temporary directories and fakes, require no Azure credentials, and make no network calls.
- Do not add dependencies or test frameworks. `pyproject.toml`, lockfiles, `sender/files.py`, and `sender/service.py` stay unchanged.
- Do not read or alter `.env`, initialize Git, commit automatically, or contact Azure. Commit steps are conditional and run only if a maintainer has initialized Git and the stated verification passes.

## File And Responsibility Map

| Path | Change | Responsibility |
| --- | --- | --- |
| `src/shared/client.py` | Modify | Declare the SDK-shaped `get_topic_sender(topic_name=...)` factory used by sender orchestration. |
| `src/sender/cli.py` | Modify | Parse and validate `--topic` before configuration loading; select queue or topic context per valid file and write destination-aware safe logs. |
| `tests/sender/test_cli.py` | Modify | Extend offline fakes and cover queue compatibility, topic selection, argument validation, safe logs, continuation, partial sends, and cleanup. |
| `README.md` | Modify | Explain queue-default and topic publishing, expected-subscription traceability, unchanged envelope semantics, and Conda commands. |
| `pyproject.toml` | Verify only | Retain the existing entry point and dependency set. |

## Stable Interfaces

Use these names and signatures consistently in implementation and tests:

| Item | Definition |
| --- | --- |
| `ServiceBusClientLike.get_topic_sender` | `(self, *, topic_name: str) -> AbstractContextManager[QueueSender]`. Azure senders have the same batch/send surface already modeled by `QueueSender`; no duplicate protocol is needed. |
| `run` | `(config: SenderConfig, *, topic: str | None = None, client_factory: ClientFactory = default_client_factory, logger: logging.Logger = _LOGGER) -> RunSummary`. Existing callers retain queue behavior through the default. |
| `_non_empty_topic` | `(value: str) -> str`; strips whitespace and raises `argparse.ArgumentTypeError("--topic must be non-empty")` for a blank value. |
| `parse_topic` | `(argv: Sequence[str]) -> str | None`; accepts no arguments or exactly `--topic TOPIC`, and raises `ArgumentParseError` before configuration/client creation for invalid input. |
| Topic destination label | `topic <topic> expected_subscription <file_stem>`. This label is built only from validated CLI input and a local file name; it is log context only. |

`FileSendError` remains the batching error boundary. Do not change its attributes, `send_objects`, message serialization, or `MessageEnvelope.properties`. Sender exceptions must continue to log only exception type, operation, batch number, and confirmed count, never `str(error)`.

## Acceptance Coverage Map

| Requirement | Tasks |
| --- | --- |
| Queue invocation remains default and uses `get_queue_sender(queue_name=file_stem)` | 1, 2, 4 |
| Topic sender uses `get_topic_sender(topic_name=topic)` once per valid file and never opens a queue sender | 1, 3 |
| File stem is only an expected-subscription log label, not destination or application property | 1, 3, 4 |
| Complete envelope validation occurs before any sender opens; invalid files continue to later files | 1, 3 |
| Topic is nonempty and rejected before configuration/client creation | 2 |
| Dynamic batching, confirmed partial count, cleanup resilience, continuation, exit codes, and input immutability remain intact | 1, 3, 4 |
| Logs and errors remain secret/payload-safe | 2, 3, 4 |
| Documentation uses Conda commands; no new dependency or live Azure test | 4 |

### Task 1: Select Topic Senders After Envelope Validation

**Files:**
- Modify: `src/shared/client.py:35-44`
- Modify: `src/sender/cli.py:48-124`
- Modify: `tests/sender/test_cli.py:23-98,117-146,149-180`

**Work unit:** `run(..., topic="orders-events")` opens `get_topic_sender(topic_name="orders-events")` for each valid file only after its entire envelope validates. Queue callers remain unchanged, and message application properties remain exactly those in the file.

- [x] **Step 1: Extend the sender fakes and write the topic-selection tracer test**

  In `tests/sender/test_cli.py`, retain `FakeQueueSender` and make its destination visible through subclasses. Replace the current `FakeClient` sender-recording fields and factories with this code:

  ```python
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
          sender = FakeQueueSender(
              queue_name, **self.sender_options.get(queue_name, {})
          )
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
  ```

  Append this test after the existing successful queue-run test. It exercises the public orchestration function, records the Azure-shaped factory calls, verifies per-file context isolation, and checks that the source bytes and message properties did not gain the file stem:

  ```python
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
  ```

- [x] **Step 2: Run the tracer test to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_run_sends_each_valid_file_to_one_topic_without_queue_senders_or_property_changes -v
  ```

  Expected: FAIL because `run` does not accept `topic` and `ServiceBusClientLike` does not declare `get_topic_sender`.

- [x] **Step 3: Add the protocol and minimal sender-context selection**

  In `src/shared/client.py`, add this method directly after `get_queue_sender` in `ServiceBusClientLike`:

  ```python
      def get_topic_sender(
          self, *, topic_name: str
      ) -> AbstractContextManager[QueueSender]: ...
  ```

  In `src/sender/cli.py`, change the `run` signature and add the destination label immediately after deriving the file stem:

  ```python
  def run(
      config: SenderConfig,
      *,
      topic: str | None = None,
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
              destination = (
                  queue_name
                  if topic is None
                  else f"topic {topic} expected_subscription {queue_name}"
              )
              sent_for_file = 0
  ```

  Replace only the queue-sender opening line inside the existing nested `try` block with this selection. Keep `load_message_envelope(path)` before this block and preserve the existing `primary_send_error` handling exactly:

  ```python
                  if topic is None:
                      sender_context = client.get_queue_sender(queue_name=queue_name)
                  else:
                      sender_context = client.get_topic_sender(topic_name=topic)
                  with sender_context as sender:
                      try:
                          sent_for_file = send_objects(
                              sender, envelope.data, envelope.properties
                          )
                      except FileSendError as error:
                          primary_send_error = error
                          raise
  ```

  In every log call within `run`, replace the destination interpolation argument `queue_name` with `destination`. Replace the generic cleanup message with this safe entity-neutral wording:

  ```python
                  "%s -> %s: %s while opening or closing sender; messages_sent=%d",
  ```

  Do not pass `queue_name` into `get_topic_sender`, `send_objects`, `ServiceBusMessage`, or `application_properties`. Do not modify `src/sender/service.py` or `src/sender/files.py`.

- [x] **Step 4: Run focused queue and topic tests to verify GREEN**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_run_uses_one_client_and_sorted_sender_contexts_without_changing_files tests/sender/test_cli.py::test_run_sends_each_valid_file_to_one_topic_without_queue_senders_or_property_changes -v
  ```

  Expected: PASS. The queue test still records `queue_names == ["a", "b"]`; the topic test records two topic calls and no queue calls. Both run without Azure.

- [ ] **Step 5: Conditionally commit topic sender selection**

  Run only if a maintainer has initialized Git and Step 4 passes:

  ```bash
  conda run -n tools-service-bus git add src/shared/client.py src/sender/cli.py tests/sender/test_cli.py
  conda run -n tools-service-bus git commit -m "feat: send files to an optional topic"
  ```

  Expected: one focused commit, or no commit when Git is not initialized.

### Task 2: Parse And Validate `--topic` Before Configuration

**Files:**
- Modify: `src/sender/cli.py:3-17,138-172`
- Modify: `tests/sender/test_cli.py:405-468`

**Work unit:** The CLI accepts its existing no-argument queue invocation and optional `--topic <name>`. Blank, missing-value, unknown, or extra arguments return code `2` without loading configuration or creating a client; `--help` returns `0` without configuration.

- [x] **Step 1: Write failing parser and fail-fast CLI tests**

  Add `import pytest` and `from sender.cli import ArgumentParseError, parse_topic` to `tests/sender/test_cli.py`. Then append these tests:

  ```python
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
  ```

- [x] **Step 2: Run the parser tests to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_parse_topic_keeps_queue_default tests/sender/test_cli.py::test_parse_topic_rejects_invalid_arguments tests/sender/test_cli.py::test_main_rejects_invalid_topic_before_loading_configuration -v
  ```

  Expected: FAIL because `parse_topic` and `ArgumentParseError` do not exist.

- [x] **Step 3: Add safe argument parsing before `config_loader`**

  In `src/sender/cli.py`, add `import argparse` with the other imports and define these symbols above `ConfigLoader`:

  ```python
  class ArgumentParseError(ValueError):
      pass


  class SenderArgumentParser(argparse.ArgumentParser):
      def error(self, message: str) -> None:
          raise ArgumentParseError(message)


  def _non_empty_topic(value: str) -> str:
      topic_name = value.strip()
      if not topic_name:
          raise argparse.ArgumentTypeError("--topic must be non-empty")
      return topic_name


  def parse_topic(argv: Sequence[str]) -> str | None:
      parser = SenderArgumentParser(prog="service-bus-send", add_help=False)
      parser.add_argument("--topic", type=_non_empty_topic)
      namespace = parser.parse_args(argv)
      return namespace.topic
  ```

  In `main`, retain the explicit `arguments == ["--help"]` shortcut, but update its output and parse before the existing configuration `try`:

  ```python
      arguments = sys.argv[1:] if argv is None else argv
      if arguments == ["--help"]:
          stdout.write("usage: service-bus-send [--topic TOPIC]\n")
          return 0
      empty_summary = RunSummary(files=0, succeeded=0, failed=0, messages_sent=0)
      try:
          topic = parse_topic(arguments)
      except ArgumentParseError as error:
          _LOGGER.error("%s while parsing arguments", type(error).__name__)
          _LOGGER.error(format_summary(empty_summary))
          return 2
      try:
          config = config_loader()
  ```

  Pass the parsed value through the existing call:

  ```python
          summary = run(
              config, topic=topic, client_factory=client_factory, logger=_LOGGER
          )
  ```

  Do not log `str(error)` or a raw invalid topic value. This preserves the established exception-type-only startup diagnostics.

- [x] **Step 4: Run parser, help, and queue-default tests to verify GREEN**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_parse_topic_keeps_queue_default tests/sender/test_cli.py::test_parse_topic_rejects_invalid_arguments tests/sender/test_cli.py::test_main_rejects_invalid_topic_before_loading_configuration tests/sender/test_cli.py::test_main_prints_help_without_loading_configuration tests/sender/test_cli.py::test_main_returns_zero_and_logs_the_exact_summary_for_success -v
  ```

  Expected: PASS. Invalid topic input yields code `2` before config/client creation; help remains code `0`; calling `main([])` still runs queue mode.

- [ ] **Step 5: Conditionally commit CLI validation**

  Run only if a maintainer has initialized Git and Step 4 passes:

  ```bash
  conda run -n tools-service-bus git add src/sender/cli.py tests/sender/test_cli.py
  conda run -n tools-service-bus git commit -m "feat: validate optional sender topic"
  ```

  Expected: one parser-focused commit, or no commit when Git is not initialized.

### Task 3: Preserve Topic-Mode Failure And Accounting Semantics

**Files:**
- Modify: `tests/sender/test_cli.py:199-382`
- Modify: `src/sender/cli.py:81-115` only if a test reveals that topic-mode destination context is not used consistently

**Work unit:** Topic publishing retains the queue sender's full-envelope validation boundary, later-file continuation, confirmed partial-send counts, primary-send-over-cleanup failure precedence, sanitized diagnostics, and exit code `1` for file failures.

- [x] **Step 1: Write topic invalid-file continuation coverage**

  Append this test. The invalid first file proves no sender was opened for it; the valid second file proves the run continues through the topic factory:

  ```python
  def test_topic_run_validates_each_complete_envelope_before_sender_opening_and_continues(
      tmp_path: Path, caplog
  ) -> None:
      write_json(tmp_path / "invalid.json", {"properties": {}, "data": [{"ok": True}, 7]})
      write_json(tmp_path / "dashboard.json", envelope([{"sent": True}], {"source": "fixture"}))
      factory = FakeClientFactory()

      with caplog.at_level(logging.ERROR, logger="test.topic-validation"):
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
  ```

- [x] **Step 2: Run the invalid-file test to verify the topic validation boundary**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_topic_run_validates_each_complete_envelope_before_sender_opening_and_continues -v
  ```

  Expected: PASS after Task 1. If it fails, correct only topic destination-label use or sender selection; do not weaken `load_message_envelope` validation or move sender opening before it.

- [x] **Step 3: Write partial-send, cleanup, and sanitization coverage for a topic sender**

  Append this test. It injects both a raw send exception and a raw cleanup exception, but verifies the logged public outcome contains only the primary exception type, operation, batch, and confirmed count:

  ```python
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
  ```

- [x] **Step 4: Run the topic resilience tests and existing queue equivalents**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_topic_run_validates_each_complete_envelope_before_sender_opening_and_continues tests/sender/test_cli.py::test_topic_run_keeps_confirmed_partial_count_and_primary_safe_error_when_cleanup_fails tests/sender/test_cli.py::test_run_counts_partial_sends_continues_and_never_logs_secrets_or_payloads tests/sender/test_cli.py::test_run_preserves_primary_send_failure_when_cleanup_also_fails tests/sender/test_cli.py::test_run_retains_confirmed_count_when_cleanup_fails_after_success -v
  ```

  Expected: PASS. Both modes retain their existing outcome rules. No test creates a live `ServiceBusClient`; all paths use `FakeClientFactory`.

- [x] **Step 5: Verify no topic-specific application property or delivery behavior was introduced**

  Inspect the only allowed sender-orchestration change against this behavioral invariant:

  ```python
  sent_for_file = send_objects(sender, envelope.data, envelope.properties)
  ```

  The call must pass the unmodified `envelope.properties`, and `sender.service.send_objects` must remain unchanged. In particular, do not add `expected_subscription`, `queue_name`, or `topic` to `properties`; do not add retries, deduplication, file moves, or topic-specific batching.

- [ ] **Step 6: Conditionally commit topic failure coverage**

  Run only if a maintainer has initialized Git and Step 4 passes:

  ```bash
  conda run -n tools-service-bus git add src/sender/cli.py tests/sender/test_cli.py
  conda run -n tools-service-bus git commit -m "test: cover topic sender failure handling"
  ```

  Expected: one focused resilience commit, or no commit when Git is not initialized.

### Task 4: Document Topic Publishing And Verify Offline

**Files:**
- Modify: `README.md:1-4,57-71`
- Modify: `tests/sender/test_cli.py` (append README contract test)
- Verify: `pyproject.toml`, `src/sender/files.py`, `src/sender/service.py`, `tests/sender/test_files.py`, `tests/sender/test_service.py`

**Work unit:** The README makes the queue default and topic mode immediately recognizable, states that file stems are expected-subscription log labels only, and preserves the operational guarantees without introducing a live-Azure test or dependency change.

- [x] **Step 1: Write the failing README contract test**

  Append this test to `tests/sender/test_cli.py`:

  ```python
  def test_readme_documents_queue_default_topic_sending_and_conda_commands() -> None:
      readme = Path("README.md").read_text(encoding="utf-8")

      assert "conda run -n tools-service-bus poetry run service-bus-send" in readme
      assert "--topic orders-events" in readme
      assert "get_queue_sender(queue_name=file_stem)" in readme
      assert "get_topic_sender(topic_name=topic)" in readme
      assert "expected subscription" in readme
      assert "not an Azure destination or application property" in readme
      assert "non-empty" in readme
  ```

- [x] **Step 2: Run the README test to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_readme_documents_queue_default_topic_sending_and_conda_commands -v
  ```

  Expected: FAIL because the sender documentation is queue-only and does not name the topic factory or expected-subscription constraint.

- [x] **Step 3: Update README sender description and run section**

  Replace the opening description in `README.md` with:

  ```markdown
  Send each object in an enveloped JSON file as an independent message to an Azure Service Bus queue by default, or publish every valid file to one selected topic. In queue mode, the queue name is the file name without its final `.json` suffix. In topic mode, that file stem is an expected subscription label in safe logs only; it is not an Azure destination or application property. The envelope's common properties become Azure application properties on every message in either mode.
  ```

  Replace the queue-only text beginning with `` `data/orders.json` targets`` through the per-file sender/log paragraph with this section:

  ```markdown
  `data/orders.json` targets the `orders` queue when no topic is selected. Each `data` object is compactly serialized as one message body; the envelope itself is never sent as a message. Every message receives its own copy of the `properties` mapping as Azure application properties.

  ## Run

  Send each file to its stem-derived queue, unchanged:

  ```bash
  conda run -n tools-service-bus poetry run service-bus-send
  ```

  Publish every valid file to one topic:

  ```bash
  conda run -n tools-service-bus poetry run service-bus-send \
    --topic orders-events
  ```

  | Argument | Contract |
  | --- | --- |
  | `--topic` | Optional, non-empty topic name. When omitted, each file uses `get_queue_sender(queue_name=file_stem)`. When present, each valid file uses `get_topic_sender(topic_name=topic)`. |

  In topic mode, `data/dashboard.json` publishes to `orders-events`; `dashboard` appears only as the expected subscription in safe logs. Azure Service Bus publishes to topics, and subscription filters determine delivery. The tool creates one Service Bus client for the run and one sender context per valid file only after the complete envelope validates.

  The summary format and exit codes are unchanged. Logs include file names, queue names or topic/expected-subscription labels, exception class names, batch numbers, and confirmed counts. They do not include the connection string, complete payloads, `.env` contents, raw exception text, or application-property values.
  ```

  Keep the existing `## Exit Codes`, `## Delivery And File Lifecycle`, and `## Test` sections unchanged. They already describe continuation, partial sends, input immutability, duplicate semantics, and offline fakes for both modes.

- [x] **Step 4: Run documentation and complete offline verification**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/sender/test_cli.py::test_readme_documents_queue_default_topic_sending_and_conda_commands -v
  conda run -n tools-service-bus poetry run pytest -v
  conda run -n tools-service-bus poetry check
  ```

  Expected: all tests PASS using fakes only, and `poetry check` prints `All set!`. `pyproject.toml` remains unchanged with only the existing `azure-servicebus`, `python-dotenv`, and `pytest` dependencies and both entry points.

- [x] **Step 5: Run the required implementation self-review**

  Verify the completed change against this checklist:

  ```markdown
  - [ ] `main([])` and `run(config)` retain queue-default behavior; queue factory calls are `get_queue_sender(queue_name=file_stem)`.
  - [ ] `--topic` accepts a trimmed nonempty name and rejects blank/malformed input with exit code `2` before `config_loader` or `client_factory` runs.
  - [ ] Topic mode invokes only `get_topic_sender(topic_name=topic)` once for each valid file; no queue sender is created in this mode.
  - [ ] Every file completes `load_message_envelope` validation before either sender factory is called; invalid files send zero messages and later files continue.
  - [ ] The file stem is used in topic mode only in the `topic <topic> expected_subscription <stem>` log label, never in an Azure factory argument or application properties.
  - [ ] `send_objects`, envelope validation, dynamic batching, compact serialization, per-message copied properties, and input-file bytes are unchanged.
  - [ ] Topic partial failures count only confirmed earlier batches, preserve the primary send error if sender cleanup also fails, continue when another file exists, and return aggregate exit code `1`.
  - [ ] Topic logs and argument/startup errors include no raw exception text, connection string, credentials, body, or application-property values.
  - [ ] README documents both Conda-wrapped send forms, sender selection, expected-subscription semantics, and existing lifecycle/exit behavior.
  - [ ] No live Azure call, package/framework addition, `pyproject.toml` change, lockfile change, or unresolved placeholder remains.
  ```

- [ ] **Step 6: Conditionally commit documentation and final verification**

  Run only if a maintainer has initialized Git and Step 4 passes:

  ```bash
  conda run -n tools-service-bus git add README.md tests/sender/test_cli.py
  conda run -n tools-service-bus git commit -m "docs: explain optional topic sending"
  ```

  Expected: one documentation-focused commit, or no commit when Git is not initialized. Do not make a catch-all final commit if the prior conditional work-unit commits were created.

## Plan Self-Review Result

| Check | Result |
| --- | --- |
| Spec coverage | Complete: optional topic contract, queue default, topic factory selection, expected-subscription-only stem, pre-client validation, envelope-before-sender boundary, batching, cleanup, partial counts, continuation, safe diagnostics, exit codes, immutability, README, and fake-only testing map to Tasks 1-4. |
| TDD sequencing | Complete: each behavior is introduced in a vertical RED-GREEN slice with a focused failing test, minimal implementation, and focused Conda command before subsequent coverage. |
| Stable names and signatures | Complete: `ServiceBusClientLike.get_topic_sender`, `run`, `_non_empty_topic`, `parse_topic`, and the topic label are defined once and reused consistently. |
| Placeholder scan | Complete: all tasks name exact paths, test bodies, code fragments, expected results, and commands; no deferred or undefined work remains. |
| Command wrapping | Complete: every executable test, check, `git add`, and commit command begins with `conda run -n tools-service-bus`. |
| Import and API review | Complete: `argparse` and `pytest` additions are named where needed; topic sender uses keyword-only `topic_name`, while queue compatibility keeps the existing `queue_name` invocation. |
| Documentation design | Complete: outcome-first header, quick path, responsibility/coverage maps, stable interfaces, bite-sized checklists, and final review make the implementation directly reviewable. |

## Risks And Decisions

- Azure Service Bus sends to topics, not directly to subscriptions. The expected subscription is observability context only; making it an application property would change filtering and message semantics without a requirement.
- `QueueSender` remains the shared batch/send protocol for topic senders. A `TopicSender` duplicate would add a name without a behavioral distinction.
- The CLI is intentionally the fail-fast boundary for `--topic`. `run` accepts its optional `topic` for direct orchestration tests and trusted callers; adding duplicate service validation would not improve the required CLI guarantee.
- Sender cleanup can fail after batches are accepted. The existing `primary_send_error` precedence and `sent_for_file` accounting must be preserved because changing either would misreport confirmed delivery.
- Re-running inputs retains the existing at-least-once and duplicate behavior. Topic support does not add retries, deduplication, checkpointing, or file mutation.
