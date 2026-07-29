import json
import math
from dataclasses import dataclass
from pathlib import Path


ApplicationProperty = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    properties: dict[str, ApplicationProperty]
    data: list[dict[str, object]]


class InputFileError(ValueError):
    """Raised when an input file violates the JSON input contract."""


def _contains_non_finite_number(value: object) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, dict):
        return any(_contains_non_finite_number(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_non_finite_number(item) for item in value)
    return False


def discover_json_files(data_dir: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in data_dir.iterdir()
            if path.suffix == ".json" and not path.is_symlink() and path.is_file()
        ),
        key=lambda path: path.name,
    )


def derive_queue_name(path: Path) -> str:
    return path.name.removesuffix(".json")


def _validate_properties(
    path: Path, properties: dict[object, object]
) -> dict[str, ApplicationProperty]:
    validated: dict[str, ApplicationProperty] = {}
    for key, value in properties.items():
        if not isinstance(key, str):
            raise InputFileError(f"{path.name}: property keys must be strings")
        if _contains_non_finite_number(value):
            raise InputFileError(
                f"{path.name}: properties.{key} contains a non-finite numeric value"
            )
        if value is not None and not isinstance(value, (str, int, float, bool)):
            raise InputFileError(f"{path.name}: properties.{key} must be a primitive")
        validated[key] = value
    return validated


def load_message_envelope(path: Path) -> MessageEnvelope:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputFileError(
            f"{path.name}: {type(error).__name__} while reading input"
        ) from error

    try:
        value = json.loads(content)
    except json.JSONDecodeError as error:
        raise InputFileError(f"{path.name}: invalid JSON") from error

    if not isinstance(value, dict):
        raise InputFileError(f"{path.name}: top-level JSON value must be an object")
    if "properties" not in value:
        raise InputFileError(f"{path.name}: properties is required")
    if "data" not in value:
        raise InputFileError(f"{path.name}: data is required")

    properties = value["properties"]
    data = value["data"]
    if not isinstance(properties, dict):
        raise InputFileError(f"{path.name}: properties must be an object")
    if not isinstance(data, list):
        raise InputFileError(f"{path.name}: data must be an array")

    validated_properties = _validate_properties(path, properties)
    validated_data: list[dict[str, object]] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InputFileError(
                f"{path.name}: data item at index {index} must be an object"
            )
        if _contains_non_finite_number(item):
            raise InputFileError(
                f"{path.name}: data item at index {index} contains a non-finite numeric value"
            )
        validated_data.append(item)

    return MessageEnvelope(properties=validated_properties, data=validated_data)
