"""Immutable namespace contracts and append-only metadata records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import TYPE_CHECKING, Final

from taskiq_clickhouse._datetime64 import require_datetime64
from taskiq_clickhouse._identifiers import QualifiedTable
from taskiq_clickhouse._schema.canonical import canonical_json_bytes, decode_canonical_json, sha256_hex
from taskiq_clickhouse._schema.validation import (
    require_instance,
    require_nonempty_text,
    require_uint32,
)
from taskiq_clickhouse._storage_policy import NamespaceKey, RetentionPolicy
from taskiq_clickhouse._uuid4 import require_uuid4


if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


_STORAGE_ID_PATTERN: Final = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,63}\Z")
_CHECKSUM_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_INVALID_CHECKSUM_ERROR: Final = "checksum must be lowercase SHA-256 hex"


@dataclass(frozen=True, slots=True)
class NamespaceContract:
    """Immutable serializer and retention contract for one namespace."""

    namespace: str
    result_table: QualifiedTable
    progress_table: QualifiedTable
    serializer_id: str
    result_ttl_us: int
    purge_ttl_us: int
    payload_format: str = "taskiq-pydantic2-python-v1"

    def __post_init__(self) -> None:
        """Validate scope, storage ids and exact integer retention."""
        NamespaceKey(self.namespace)
        require_instance(self.result_table, QualifiedTable, field="result table")
        require_instance(self.progress_table, QualifiedTable, field="progress table")
        if self.result_table == self.progress_table:
            msg = "result and progress tables must differ"
            raise ValueError(msg)
        if self.result_table.database != self.progress_table.database:
            msg = "result and progress tables must share one database"
            raise ValueError(msg)
        _storage_id(self.serializer_id, field="serializer_id")
        _storage_id(self.payload_format, field="payload_format")
        RetentionPolicy(self.result_ttl_us, self.purge_ttl_us)

    @property
    def scope(self) -> str:
        """Return the canonical table-set scope."""
        result_table = self.result_table.canonical
        progress_table = self.progress_table.canonical
        return f"{result_table}|{progress_table}"

    @property
    def payload_bytes(self) -> bytes:
        """Return the exact canonical namespace descriptor."""
        return canonical_json_bytes(self.canonical_data())

    @property
    def payload_text(self) -> str:
        """Return the canonical descriptor as Unicode text."""
        return self.payload_bytes.decode()

    @property
    def checksum(self) -> str:
        """Return the descriptor SHA-256 checksum."""
        return sha256_hex(self.payload_bytes)

    def require_retention_feasible_at(self, observed_at: object) -> None:
        """Validate configured deadlines against a fresh authoritative clock."""
        RetentionPolicy(self.result_ttl_us, self.purge_ttl_us).require_feasible_at(observed_at)

    def canonical_data(self) -> dict[str, object]:
        """Return the frozen namespace payload key set."""
        return {
            "payload_format": self.payload_format,
            "progress_table": self.progress_table.canonical,
            "purge_ttl_us": self.purge_ttl_us,
            "result_table": self.result_table.canonical,
            "result_ttl_us": self.result_ttl_us,
            "serializer_id": self.serializer_id,
        }


@dataclass(frozen=True, slots=True)
class MetadataRecord:
    """One exact append-only metadata table row."""

    record_kind: str
    scope: str
    record_key: str
    version: int
    name: str
    payload: bytes
    checksum: str
    package_version: str
    recorded_at: datetime
    attempt_id: UUID

    def __post_init__(self) -> None:
        """Validate the complete logical row before its first write attempt."""
        for field_name in ("record_kind", "scope", "record_key", "name", "package_version"):
            require_nonempty_text(getattr(self, field_name), field=field_name)
        require_uint32(self.version, field="metadata version")
        decode_canonical_json(self.payload)
        _checksum(self.checksum, payload=self.payload)
        require_datetime64(self.recorded_at, field="recorded_at")
        require_uuid4(self.attempt_id, field="attempt_id")

    def as_row(self) -> tuple[object, ...]:
        """Return values in the fixed metadata column order."""
        return (  # noqa: WPS227 - exact metadata table contract has ten columns.
            self.record_kind,
            self.scope,
            self.record_key,
            self.version,
            self.name,
            self.payload,
            self.checksum,
            self.package_version,
            self.recorded_at,
            self.attempt_id,
        )


def _storage_id(storage_id: object, *, field: str) -> None:
    if not isinstance(storage_id, str):
        msg = f"{field} must be a stable storage identifier"
        raise TypeError(msg)
    if _STORAGE_ID_PATTERN.fullmatch(storage_id) is None:
        msg = f"{field} must be a stable storage identifier"
        raise ValueError(msg)


def _checksum(checksum: object, *, payload: bytes) -> None:
    if not isinstance(checksum, str):
        raise TypeError(_INVALID_CHECKSUM_ERROR)
    if _CHECKSUM_PATTERN.fullmatch(checksum) is None:
        raise ValueError(_INVALID_CHECKSUM_ERROR)
    if checksum != sha256_hex(payload):
        msg = "checksum does not match payload"
        raise ValueError(msg)
