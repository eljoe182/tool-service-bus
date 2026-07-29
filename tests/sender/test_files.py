import json
from pathlib import Path

import pytest

from sender.files import (
    InputFileError,
    derive_queue_name,
    discover_json_files,
    load_message_envelope,
)


def test_discover_json_files_returns_only_direct_exact_suffix_files_sorted_by_name(
    tmp_path: Path,
) -> None:
    (tmp_path / "zeta.json").write_text("[]", encoding="utf-8")
    (tmp_path / "Alpha.json").write_text("[]", encoding="utf-8")
    (tmp_path / "ignored.JSON").write_text("[]", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("[]", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "inside.json").write_text("[]", encoding="utf-8")

    discovered = discover_json_files(tmp_path)

    assert [path.name for path in discovered] == ["Alpha.json", "zeta.json"]


def test_discover_json_files_ignores_symlinks_to_external_json(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    external = tmp_path / "external.json"
    external.write_text("[]", encoding="utf-8")
    (data_dir / "external.json").symlink_to(external)
    (data_dir / "orders.json").write_text("[]", encoding="utf-8")

    discovered = discover_json_files(data_dir)

    assert [path.name for path in discovered] == ["orders.json"]


def test_derive_queue_name_removes_only_the_final_json_suffix() -> None:
    assert derive_queue_name(Path("data/orders.json")) == "orders"
    assert derive_queue_name(Path("data/archive.orders.json")) == "archive.orders"


def test_load_message_envelope_returns_properties_and_data_objects(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orders.json"
    customer_name = "Jos" + chr(233)
    path.write_text(
        json.dumps(
            {
                "properties": {"source": "fixture", "priority": 3, "retry": False},
                "data": [{"name": customer_name, "nested": {"count": 2}}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    envelope = load_message_envelope(path)

    assert envelope.properties == {"source": "fixture", "priority": 3, "retry": False}
    assert envelope.data == [{"name": customer_name, "nested": {"count": 2}}]


def test_load_message_envelope_ignores_extra_root_keys(tmp_path: Path) -> None:
    path = tmp_path / "orders.json"
    path.write_text(
        '{"properties":{},"data":[{"orderId":"A-1"}],"ignored":{"trace":true}}',
        encoding="utf-8",
    )

    assert load_message_envelope(path).data == [{"orderId": "A-1"}]


def test_load_message_envelope_accepts_empty_properties_and_data(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_text('{"properties":{},"data":[]}', encoding="utf-8")

    assert load_message_envelope(path).properties == {}
    assert load_message_envelope(path).data == []


def test_load_message_envelope_accepts_nested_finite_data_numbers(tmp_path: Path) -> None:
    path = tmp_path / "finite.json"
    path.write_text(
        '{"properties":{},"data":[{"numbers":[0,-1.5,1e308],"nested":{"value":2.25}}]}',
        encoding="utf-8",
    )

    assert load_message_envelope(path).data == [
        {"numbers": [0, -1.5, 1e308], "nested": {"value": 2.25}}
    ]


@pytest.mark.parametrize(
    ("content", "safe_message"),
    [
        ("{", "invalid JSON"),
        ("[]", "top-level JSON value must be an object"),
        ("{}", "properties is required"),
        ('{"properties":{}}', "data is required"),
        ('{"properties":[],"data":[]}', "properties must be an object"),
        ('{"properties":{},"data":{}}', "data must be an array"),
        ('{"properties":{},"data":[7]}', "data item at index 0 must be an object"),
        ('{"properties":{"tags":[]},"data":[]}', "properties.tags must be a primitive"),
        ('{"properties":{"metadata":{}},"data":[]}', "properties.metadata must be a primitive"),
        ('{"properties":{"rate":1e400},"data":[]}', "non-finite numeric value"),
        ('{"properties":{"nested":{"rate":1e400}},"data":[]}', "non-finite numeric value"),
        ('{"properties":{},"data":[{"nested":[{"rate":-1e400}]}]}', "non-finite numeric value"),
    ],
)
def test_load_message_envelope_rejects_invalid_contract_values(
    tmp_path: Path, content: str, safe_message: str
) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(InputFileError, match=safe_message):
        load_message_envelope(path)


def test_load_message_envelope_rejects_a_non_string_property_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "invalid.json"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "sender.files.json.loads",
        lambda content: {"properties": {1: "value"}, "data": []},
    )

    with pytest.raises(InputFileError, match="property keys must be strings"):
        load_message_envelope(path)


def test_load_message_envelope_rejects_invalid_utf8_without_echoing_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid.json"
    path.write_bytes(b"[\xff]")

    with pytest.raises(InputFileError, match="UnicodeDecodeError") as error:
        load_message_envelope(path)

    assert "\\xff" not in str(error.value)


def test_load_message_envelope_wraps_read_errors_without_raw_os_text(
    tmp_path: Path,
) -> None:
    path = tmp_path / "not-a-file.json"
    path.mkdir()

    with pytest.raises(InputFileError, match="OSError|IsADirectoryError") as error:
        load_message_envelope(path)

    assert str(path) not in str(error.value)
