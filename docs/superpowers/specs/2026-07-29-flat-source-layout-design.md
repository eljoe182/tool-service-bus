# Split Sender And Reader Into Flat Source Packages

Replace the inaccurate `service_bus_sender` root package with three top-level packages: `shared`, `sender`, and `reader`. This makes the two operations independently navigable while keeping only genuinely common concerns together.

## Target Layout

```text
src/
├── shared/
│   ├── __init__.py
│   ├── client.py
│   └── config.py
├── sender/
│   ├── __init__.py
│   ├── cli.py
│   ├── files.py
│   └── service.py
└── reader/
    ├── __init__.py
    ├── cli.py
    └── service.py

tests/
├── shared/
│   └── test_config.py
├── sender/
│   ├── test_cli.py
│   ├── test_files.py
│   └── test_service.py
└── reader/
    ├── test_cli.py
    └── test_service.py
```

## Responsibilities

| Package | Responsibility | Must not own |
| --- | --- | --- |
| `shared` | Connection-string configuration, log configuration, Azure client factory and shared client protocols. | Sender input validation, message batching, reader modes, CLI argument parsing. |
| `sender` | JSON envelope discovery and validation, message serialization, dynamic batching, send orchestration and sender CLI. | Reader modes and receiver settlement. |
| `reader` | Queue reading modes, body rendering, receiver settlement, reader CLI. | File-system input and sender batching. |

`shared.config` must expose separate configuration types: the reader needs Azure connection and log settings, while the sender additionally needs its readable data directory. This removes the reader's unnecessary dependency on `SERVICE_BUS_DATA_DIR`.

## Entry Points And Packaging

```toml
[project.scripts]
service-bus-send = "sender.cli:main"
service-bus-read = "reader.cli:main"

[tool.poetry]
packages = [
  { include = "shared", from = "src" },
  { include = "sender", from = "src" },
  { include = "reader", from = "src" },
]
```

Poetry must reinstall the current project after the migration so the new script targets are registered. No dependency or environment-variable names change.

## Migration Rules

1. Move modules without changing observable sender or reader behavior.
2. Update imports, tests, entry points, and package declarations atomically.
3. Delete the old `src/service_bus_sender` package after all callers use the new paths.
4. Retain existing test coverage and add regression coverage showing reader configuration works without a data directory.
5. Run the full offline test suite and both entry-point help commands after reinstalling the project.

## Trade-Off

Top-level names such as `shared`, `sender`, and `reader` can collide with packages from another installed distribution. This is accepted for this local operational tool to avoid an extra wrapper package that does not carry domain meaning.

## Verification Checklist

- [ ] Sender code imports only `shared` and `sender` modules.
- [ ] Reader code imports only `shared` and `reader` modules.
- [ ] Reader startup does not require `SERVICE_BUS_DATA_DIR`.
- [ ] Both Poetry scripts resolve to their new CLI modules after `poetry install`.
- [ ] No source or test import references `service_bus_sender`.
- [ ] Existing sender and reader behavior remains covered by offline tests.
