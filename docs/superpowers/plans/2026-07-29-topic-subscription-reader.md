# Topic Subscription Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `service-bus-read` to read a queue by default or a named Azure Service Bus topic subscription while preserving every existing queue behavior.

**Architecture:** Keep `--queue` as the entity-name argument and add entity metadata to `ReadRequest`: `entity_type` defaults to `"queue"` and `subscription_name` is present only for topics. Extend the shared client protocol with the subscription receiver factory, choose the receiver once in `read_messages`, and leave peek/block/drain rendering, flush-before-complete, cleanup, and result output unchanged. Keep `QueueReadError` as the public error type for queue compatibility, but derive a safe entity description from the request so topic diagnostics include both topic and subscription without exception text or unprinted bodies.

**Tech Stack:** Python 3.11+, Poetry 2, `azure-servicebus` 7.x, `pytest` 8.x, and standard-library `argparse`, `dataclasses`, `contextlib`, and `io`.

---

## Quick Path

1. Add the subscription receiver protocol and a topic `peek` test, then implement only receiver selection.
2. Add topic `block` and `drain` tests, then prove their existing behavior is unchanged.
3. Add parser cross-validation and safe topic error tests before updating the parser and diagnostics.
4. Document both invocation forms and run the complete offline suite.

## Execution Prerequisites

- Work from `/Users/garciajoise/Projects/esmax/tools/service-bus`.
- Run every command exactly through `conda run -n tools-service-bus`; tests must use fakes and must not contact Azure or require credentials.
- Do not add packages, modify `pyproject.toml`, alter sender source/tests/behavior, access `.env`, or initialize Git.
- Git is not initialized in this directory. Each commit step is conditional: execute it only after a maintainer initializes Git and only after its stated verification passes.

## File And Responsibility Map

| Path | Change | Responsibility |
| --- | --- | --- |
| `src/shared/client.py` | Modify | Add the typed `get_subscription_receiver(topic_name=..., subscription_name=...)` factory to `ServiceBusClientLike`. |
| `src/reader/service.py` | Modify | Carry entity metadata in `ReadRequest`, select the correct receiver, and create sanitized entity-aware diagnostics while retaining reader modes and cleanup. |
| `src/reader/cli.py` | Modify | Parse `--entity-type` and `--subscription`, validate their combination before configuration loading, and print safe entity-aware errors. |
| `tests/reader/test_service.py` | Modify | Use receiver/client fakes that record queue and subscription calls; cover topic peek, block, drain, cleanup, sanitization, and queue compatibility. |
| `tests/reader/test_cli.py` | Modify | Cover parser cross-validation, topic CLI selection/error output, and the existing queue invocation contract. |
| `README.md` | Modify | Explain queue-default and topic-subscription invocation, argument rules, unchanged modes, output, and safe error semantics. |

## Stable Interfaces

Use these names and signatures consistently in implementation and tests:

| Item | Definition |
| --- | --- |
| `ReadRequest` | `@dataclass(frozen=True, slots=True)` with `queue_name: str`, `count: int`, `mode: str`, `entity_type: str = "queue"`, and `subscription_name: str | None = None`. `queue_name` remains the queue name for queues and is the topic name for topics. |
| `QueueReadError` | Existing public error type. It exposes `operation`, `queue_name`, `entity_type`, `subscription_name`, `error_type`, and an `entity_description` property. The queue description stays `queue <name>`; the topic description is `topic <name> subscription <name>`. |
| `ServiceBusClientLike.get_subscription_receiver` | `(self, *, topic_name: str, subscription_name: str) -> AbstractContextManager[QueueReceiver]`. |
| `parse_request` | `(argv: Sequence[str]) -> ReadRequest`; argument parsing and cross-validation complete before `main` loads configuration. |

`QueueReadError` must not store or print `str(cause)`. `entity_description` is constructed only from validated command-line names. It must not use a raw exception, connection string, or a message body.

## Acceptance Coverage Map

| Requirement | Tasks |
| --- | --- |
| Queue remains the default and existing queue request/call behavior stays unchanged | 1, 2, 4 |
| Topic selects `get_subscription_receiver` with exact keyword names | 1, 2 |
| Subscription topic `peek`, `block`, and `drain` preserve modes/output/flush settlement order | 1, 2 |
| Subscription required/non-empty for topic and forbidden for queue before configuration loading | 3 |
| Diagnostics safely name queue or topic/subscription and return exit code 2 | 3 |
| No packages or sender changes; README examples use Conda commands | 4 |

### Task 1: Add Topic Receiver Selection And Peek

**Files:**
- Modify: `src/shared/client.py:35-40`
- Modify: `src/reader/service.py:17-35,54-99`
- Modify: `tests/reader/test_service.py:64-95,102-128`

**Work unit:** A `ReadRequest` with `entity_type="topic"` opens the requested subscription receiver, peeks with the existing parameters, prints the existing output, and closes both contexts. Existing queue requests continue calling only `get_queue_receiver`.

- [x] **Step 1: Write the failing topic-peek test and extend only its fakes**

  In `tests/reader/test_service.py`, replace `FakeClient` with this version. The test fake deliberately records the two SDK-shaped calls independently, so a topic path cannot accidentally fall through to queue selection:

  ```python
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
  ```

  Append this test after `test_peek_prints_utf8_bodies_and_count_without_settlement`:

  ```python
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
  ```

- [x] **Step 2: Run the focused test to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py::test_topic_peek_uses_subscription_receiver_without_settlement -v
  ```

  Expected: FAIL because `ReadRequest` does not accept `entity_type` or `subscription_name`.

- [x] **Step 3: Add the protocol and minimal entity-aware receiver selection**

  In `src/shared/client.py`, add the following method directly after `get_queue_receiver` in `ServiceBusClientLike`:

  ```python
      def get_subscription_receiver(
          self, *, topic_name: str, subscription_name: str
      ) -> AbstractContextManager[QueueReceiver]: ...
  ```

  In `src/reader/service.py`, replace `ReadRequest` and `QueueReadError` with the following definitions. Preserve the class name so current imports and queue tests remain valid:

  ```python
  @dataclass(frozen=True, slots=True)
  class ReadRequest:
      queue_name: str
      count: int
      mode: str
      entity_type: str = "queue"
      subscription_name: str | None = None


  class QueueReadError(RuntimeError):
      def __init__(
          self, *, operation: str, request: ReadRequest, cause: BaseException
      ) -> None:
          self.operation = operation
          self.queue_name = request.queue_name
          self.entity_type = request.entity_type
          self.subscription_name = request.subscription_name
          self.error_type = type(cause).__name__
          super().__init__(
              f"{self.error_type} while {operation} {self.entity_description}"
          )

      @property
      def entity_description(self) -> str:
          if self.entity_type == "topic":
              return (
                  f"topic {self.queue_name} subscription {self.subscription_name}"
              )
          return f"queue {self.queue_name}"
  ```

  Replace the receiver-opening line in `read_messages` with this block; leave the existing mode branch, rendering, `stdout.flush()` before drain completion, result write, and context-manager structure unchanged:

  ```python
              operation = "opening receiver"
              if request.entity_type == "topic":
                  receiver_context = client.get_subscription_receiver(
                      topic_name=request.queue_name,
                      subscription_name=request.subscription_name,
                  )
              else:
                  receiver_context = client.get_queue_receiver(
                      queue_name=request.queue_name
                  )
              with receiver_context as receiver:
  ```

  In both `QueueReadError(...)` construction sites, replace `queue_name=request.queue_name` with `request=request`.

- [x] **Step 4: Run the focused service tests to verify GREEN and queue compatibility**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py::test_peek_prints_utf8_bodies_and_count_without_settlement tests/reader/test_service.py::test_topic_peek_uses_subscription_receiver_without_settlement -v
  ```

  Expected: PASS. The original queue test records `queue_names == ["orders"]`; the topic test records only `subscription_names == [("sbt-local-indicators", "dashboard")]`.

- [ ] **Step 5: Conditionally commit the receiver-selection work unit**

  Run only if a maintainer has initialized Git and Step 4 passes:

  ```bash
  conda run -n tools-service-bus git add src/shared/client.py src/reader/service.py tests/reader/test_service.py
  conda run -n tools-service-bus git commit -m "feat: select topic subscription receivers"
  ```

  Expected: one commit limited to protocol, reader selection, and its tests. In the current directory, skip both commands because Git is absent.

### Task 2: Prove Topic Block And Drain Preserve Existing Semantics

**Files:**
- Modify: `tests/reader/test_service.py:137-181,237-385`
- Modify: `src/reader/service.py:67-84` only if a focused failure exposes a receiver-selection integration defect

**Work unit:** A topic subscription uses the same `block` receive behavior and the same `drain` render-write-flush-complete ordering as a queue. No implementation change is expected after Task 1 unless the tests expose one.

- [x] **Step 1: Write one block test and one drain ordering test for subscriptions**

  Add a recording stream before the tests:

  ```python
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
  ```

  Add the two tests below. In the second test, wrap `complete_message` after constructing the fake so the test observes the externally visible ordering rather than an internal helper:

  ```python
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
  ```

- [x] **Step 2: Run the subscription mode tests to prove the shared selection path**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py::test_topic_block_receives_without_settlement tests/reader/test_service.py::test_topic_drain_flushes_output_before_completing_each_message -v
  ```

  Expected: PASS after Task 1. The first tracer slice made receiver selection entity-aware; these tests prove that its deliberately shared mode loop applies unchanged to subscription receivers rather than introducing topic-only behavior.

- [x] **Step 3: Keep the existing mode loop unchanged unless the focused test fails**

  Verify that the receiver body in `src/reader/service.py` remains exactly behaviorally equivalent to this code:

  ```python
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
  ```

  Do not add topic-specific mode branches, abandon calls, retries, or settlement changes. This is the minimum implementation: only receiver creation differs by entity type.

- [x] **Step 4: Add topic cleanup and sanitization coverage**

  Append this service-level test, which confirms a subscription receiver cleanup error is contextualized without exposing its raw detail and that the client still exits:

  ```python
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
  ```

- [x] **Step 5: Run the complete service suite to verify GREEN**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_service.py -v
  ```

  Expected: PASS. Existing queue tests remain unchanged and pass; topic tests prove `peek`, `block`, `drain`, flush-before-complete, cleanup, and sanitized errors without network activity.

- [ ] **Step 6: Conditionally commit subscription-mode coverage**

  Run only if a maintainer has initialized Git and Step 5 passes:

  ```bash
  conda run -n tools-service-bus git add src/reader/service.py tests/reader/test_service.py
  conda run -n tools-service-bus git commit -m "test: cover topic reader modes and cleanup"
  ```

  Expected: one focused commit, or no commit in the current non-Git directory.

### Task 3: Parse Entity Options And Preserve Safe CLI Exit Behavior

**Files:**
- Modify: `src/reader/cli.py:25-50,81-85`
- Modify: `tests/reader/test_cli.py:38-58,64-138,146-206`

**Work unit:** The CLI accepts old queue invocations unchanged, requires a nonblank subscription for topics, rejects subscriptions on queues before configuration loading, and reports topic operation errors with type/topic/subscription only at exit code 2.

- [x] **Step 1: Extend the CLI fake client and write parser cross-validation tests**

  Add this method to the `FakeClient` in `tests/reader/test_cli.py`:

  ```python
      def get_subscription_receiver(
          self, *, topic_name: str, subscription_name: str
      ) -> FakeReceiver:
          return self.receiver
  ```

  Add these tests. Each invalid input uses a loader that raises if invoked, proving validation happens before configuration or Azure-client creation:

  ```python
  @pytest.mark.parametrize(
      ("argv", "expected_message"),
      [
          (
              [
                  "--queue", "indicators", "--count", "1", "--mode", "peek",
                  "--entity-type", "topic",
              ],
              "--subscription is required when --entity-type topic",
          ),
          (
              [
                  "--queue", "indicators", "--count", "1", "--mode", "peek",
                  "--entity-type", "topic", "--subscription", "   ",
              ],
              "--subscription must be non-empty",
          ),
          (
              [
                  "--queue", "orders", "--count", "1", "--mode", "peek",
                  "--subscription", "dashboard",
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
              "--queue", "sbt-local-indicators", "--count", "2", "--mode", "peek",
              "--entity-type", "topic", "--subscription", "dashboard",
          ]
      )

      assert queue_request == ReadRequest(
          queue_name="orders", count=2, mode="drain"
      )
      assert topic_request == ReadRequest(
          queue_name="sbt-local-indicators",
          count=2,
          mode="peek",
          entity_type="topic",
          subscription_name="dashboard",
      )
  ```

  Add `ReadRequest` to the existing `from reader.cli import ...` imports by importing it from `reader.service`:

  ```python
  from reader.service import ReadRequest
  ```

- [x] **Step 2: Run parser tests to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py::test_main_rejects_invalid_entity_subscription_pairs_before_loading_config tests/reader/test_cli.py::test_parse_request_keeps_existing_queue_contract_and_accepts_topic_contract -v
  ```

  Expected: FAIL because `--entity-type` and `--subscription` are not registered and `ReadRequest` lacks the additional metadata.

- [x] **Step 3: Implement parser cross-validation before configuration loading**

  In `src/reader/cli.py`, add this helper after `_non_empty_queue`:

  ```python
  def _non_empty_subscription(value: str) -> str:
      subscription_name = value.strip()
      if not subscription_name:
          raise argparse.ArgumentTypeError("--subscription must be non-empty")
      return subscription_name
  ```

  Replace `parse_request` with:

  ```python
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
  ```

  This code remains inside `parse_request`, which `main` calls before `config_loader`; do not move cross-validation into the service layer or after configuration loading.

- [x] **Step 4: Write topic operation-error and CLI receiver-selection tests**

  Replace the `FakeClient` in `tests/reader/test_cli.py` with an equivalent fake that records both factory paths:

  ```python
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
  ```

  Append the following tests. The error test installs a failing receive method containing both a connection string marker and an unprinted body marker:

  ```python
  def test_main_reads_topic_subscription_with_existing_peek_output() -> None:
      receiver = FakeReceiver()
      receiver.received = [FakeMessage(b"indicator")]
      client = FakeClient(receiver)
      stdout = StringIO()
      stderr = StringIO()

      exit_code = main(
          [
              "--queue", "sbt-local-indicators", "--count", "1", "--mode", "peek",
              "--entity-type", "topic", "--subscription", "dashboard",
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
              "--queue", "sbt-local-indicators", "--count", "1", "--mode", "block",
              "--entity-type", "topic", "--subscription", "dashboard",
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
  ```

- [x] **Step 5: Update safe CLI output and run the complete CLI suite**

  In `src/reader/cli.py`, replace the `QueueReadError` handling output with:

  ```python
      except QueueReadError as error:
          stderr.write(
              f"{error.error_type} while {error.operation} "
              f"{error.entity_description}\n"
          )
          return 2
  ```

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py -v
  ```

  Expected: PASS. Existing queue diagnostic assertions remain exactly `... queue orders`; topic diagnostics include the topic and subscription but never raw SDK exception text, connection strings, or unprinted bodies.

- [ ] **Step 6: Conditionally commit CLI validation and diagnostics**

  Run only if a maintainer has initialized Git and Step 5 passes:

  ```bash
  conda run -n tools-service-bus git add src/reader/cli.py tests/reader/test_cli.py
  conda run -n tools-service-bus git commit -m "feat: validate topic reader subscriptions"
  ```

  Expected: one parser/diagnostics commit, or no commit in the current non-Git directory.

### Task 4: Document Both Entity Forms And Verify Offline

**Files:**
- Modify: `README.md:73-92`
- Modify: `tests/reader/test_cli.py:221-230`
- Verify: `pyproject.toml`
- Verify: `src/sender/`, `tests/sender/`

**Work unit:** The README makes the queue default and topic subscription path easy to recognize, preserves the mode contract, and the full test suite proves no sender, dependency, or network behavior changed.

- [x] **Step 1: Write the failing README contract test**

  Expand `test_readme_documents_all_reader_modes_and_conda_command` in `tests/reader/test_cli.py` with these assertions:

  ```python
      assert "--entity-type topic" in readme
      assert "--subscription dashboard" in readme
      assert "get_subscription_receiver" in readme
      assert "defaults to `queue`" in readme
      assert "required for `topic`" in readme
      assert "rejected for `queue`" in readme
  ```

- [x] **Step 2: Run the README test to establish RED**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py::test_readme_documents_all_reader_modes_and_conda_command -v
  ```

  Expected: FAIL because the Queue Reader section documents queues only.

- [x] **Step 3: Replace the Queue Reader introduction and argument paragraph**

  In `README.md`, replace the first paragraph and queue-only invocation under `## Queue Reader` with the following section, keeping the existing mode table directly after it:

  ```markdown
  ## Queue Reader

  `service-bus-read` reads a queue by default or reads a topic through an explicitly selected subscription. Azure Service Bus topics are not directly readable.

  Read a queue using the existing invocation:

  ```bash
  conda run -n tools-service-bus poetry run service-bus-read \
    --queue orders \
    --count 10 \
    --mode peek
  ```

  Read a topic subscription:

  ```bash
  conda run -n tools-service-bus poetry run service-bus-read \
    --queue sbt-local-indicators \
    --subscription dashboard \
    --count 10 \
    --mode peek \
    --entity-type topic
  ```

  | Argument | Contract |
  | --- | --- |
  | `--queue` | Required, non-empty queue name; it is the topic name with `--entity-type topic`. |
  | `--count` | Required positive integer. |
  | `--mode` | Required: `peek`, `block`, or `drain`. |
  | `--entity-type` | `queue` or `topic`; defaults to `queue`. |
  | `--subscription` | Required for `topic`, non-empty, and rejected for `queue`. |

  Queue reads use `get_queue_receiver(queue_name=...)`. Topic reads use `get_subscription_receiver(topic_name=..., subscription_name=...)`.
  ```

  Replace the first sentence of the existing validation/output paragraph with:

  ```markdown
  `--queue` must be non-empty, `--count` must be a positive integer, and `--mode` is exactly `peek`, `block`, or `drain`. `--subscription` is required for `topic` and rejected for `queue`. The command writes one rendered message body per standard-output line and writes `Read N messages` only to standard error.
  ```

  Replace the final error sentence of that paragraph with:

  ```markdown
  Errors exit with code `2` and identify only the operation, entity type/name, subscription when relevant, and exception type; they never print connection strings, raw exception details, or bodies that were not printed.
  ```

  Do not change the mode table: `peek`, `block`, and `drain` retain the documented operations and destructive-drain semantics for both queue and subscription receivers.

- [ ] **Step 4: Run focused documentation tests and complete offline verification**

  Run:

  ```bash
  conda run -n tools-service-bus poetry run pytest tests/reader/test_cli.py::test_readme_documents_all_reader_modes_and_conda_command -v
  conda run -n tools-service-bus poetry run pytest -v
  conda run -n tools-service-bus poetry check
  ```

  Expected: the README test and full suite PASS with fakes only; `poetry check` prints `All set!`. `pyproject.toml` remains unchanged, retaining both existing entry points and the unchanged dependency list.

- [x] **Step 5: Run the required implementation self-review**

  Verify the implementation and documentation against this checklist:

  ```markdown
   - [x] `ReadRequest` preserves the three original positional fields and defaults to `entity_type="queue"` with no subscription.
   - [x] Queue requests use only `get_queue_receiver(queue_name=...)`; topic requests use only `get_subscription_receiver(topic_name=..., subscription_name=...)`.
   - [x] Parser validation rejects missing/blank topic subscriptions and queue subscriptions before configuration loading; existing queue-only arguments still parse identically.
   - [x] Topic `peek` calls `peek_messages` and does not settle; topic `block` calls `receive_messages(..., max_wait_time=10)` and does not settle; topic `drain` renders, writes, flushes, then completes each message.
   - [x] Existing queue tests, stdout/stderr output, empty-read success, cleanup, and safe queue diagnostics remain green.
   - [x] Topic failures return code 2, identify topic and subscription, and omit raw exception text, connection strings, and unprinted bodies.
   - [x] `src/sender/`, `tests/sender/`, `pyproject.toml`, dependencies, and lockfiles are unchanged.
   - [x] README includes both Conda-wrapped examples, the argument table, receiver selection, existing modes, and safe diagnostics.
   - [x] Every command in this plan begins with `conda run -n tools-service-bus`; no unresolved placeholder or inconsistent item name remains.
  ```

- [ ] **Step 6: Conditionally commit documentation and verified feature**

  Run only if a maintainer has initialized Git and all Task 4 verification passes:

  ```bash
  conda run -n tools-service-bus git add README.md tests/reader/test_cli.py
  conda run -n tools-service-bus git commit -m "docs: explain topic subscription reading"
  ```

  Expected: one documentation-focused commit, or no commit in the current non-Git directory. Do not create a catch-all final commit if the earlier conditional work-unit commits were made.

## Plan Self-Review Result

| Check | Result |
| --- | --- |
| Approved requirement coverage | Complete: entity options, receiver factory selection, all subscription modes, safe errors, cleanup, queue compatibility, documentation, and offline tests map to Tasks 1-4. |
| TDD sequencing | Each new behavior has a focused failing test, a minimal implementation step, and a focused passing command before the next vertical slice. |
| Stable names and signatures | `ReadRequest`, `QueueReadError`, `parse_request`, `ServiceBusClientLike`, and receiver method signatures are defined once and reused consistently. |
| Placeholder scan | Complete test bodies, exact paths, exact messages, and implementation fragments are provided; no deferred markers remain. |
| Command wrapping | Every executable command, including conditional Git commands, begins with `conda run -n tools-service-bus`. |
| Readability | Outcome-first header, quick path, responsibility map, stable interfaces, coverage map, short work units, and a final verification checklist support direct implementation and review. |

## Risks And Decisions

- Azure topics are read through subscriptions only. The CLI intentionally retains `--queue` as the entity-name flag to avoid an unnecessary breaking rename; for `topic`, that value is the topic name.
- `QueueReadError` is retained for compatibility with existing imports and assertions even though it now safely describes either entity kind. Renaming it would add a public-surface change without a requirement.
- The service trusts `ReadRequest` values produced by the parser. Parser cross-validation is the fail-fast boundary required by the design; no redundant service validation or new exception type is introduced.
- `drain` remains at-least-once at the operator boundary: an output failure before completion leaves the current message unsettled, while earlier completed messages remain removed.
- `ServiceBusClientLike` needs the subscription method solely for typed use by the reader. The default Azure `ServiceBusClient` already supplies the runtime method through the existing dependency; no package or factory change is needed.
