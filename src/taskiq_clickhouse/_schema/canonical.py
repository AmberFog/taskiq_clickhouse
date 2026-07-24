"""Canonical JSON, checksum and DDL normalization primitives."""

import hashlib
import json
import textwrap
from typing import Final, cast


_UTF8_ENCODING: Final = "utf-8"


def canonical_json_bytes(document: object) -> bytes:
    """Encode JSON into the frozen compact, sorted UTF-8 representation."""
    try:
        encoded = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        msg = "value is not canonical-JSON encodable"
        raise ValueError(msg) from error
    return encoded.encode(_UTF8_ENCODING)


def decode_canonical_json(payload: bytes) -> object:
    """Decode only byte-exact canonical UTF-8 JSON."""
    payload_bytes = _require_bytes(payload, field="canonical JSON payload")
    try:
        text = payload_bytes.decode(_UTF8_ENCODING, errors="strict")
        decoded = cast("object", json.loads(text))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        msg = "payload is not valid UTF-8 JSON"
        raise ValueError(msg) from error
    if canonical_json_bytes(decoded) != payload_bytes:
        msg = "payload is not in canonical JSON form"
        raise ValueError(msg)
    return decoded


def sha256_hex(payload: bytes) -> str:
    """Return a lowercase SHA-256 checksum for exact bytes."""
    payload_bytes = _require_bytes(payload, field="checksum payload")
    return hashlib.sha256(payload_bytes).hexdigest()


def normalize_ddl(ddl: str) -> str:
    """Normalize layout without changing whitespace inside SQL expressions."""
    ddl_text = _require_text(ddl, field="DDL")
    if "\x00" in ddl_text:
        msg = "DDL must not contain NUL"
        raise ValueError(msg)
    lines = _normalized_ddl_lines(ddl_text)
    normalized = "\n".join(lines)
    if not normalized:
        msg = "DDL must not be empty"
        raise ValueError(msg)
    return normalized


def _normalized_ddl_lines(ddl: str) -> list[str]:
    normalized_newlines = ddl.replace("\r\n", "\n").replace("\r", "\n")
    dedented = textwrap.dedent(normalized_newlines)
    lines = [line.rstrip() for line in dedented.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return lines


def _require_bytes(candidate: object, *, field: str) -> bytes:
    if not isinstance(candidate, bytes):
        msg = f"{field} must be bytes"
        raise TypeError(msg)
    return candidate


def _require_text(candidate: object, *, field: str) -> str:
    if not isinstance(candidate, str):
        msg = f"{field} must be a string"
        raise TypeError(msg)
    return candidate
