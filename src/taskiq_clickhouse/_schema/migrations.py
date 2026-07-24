"""Immutable forward migration definitions and plans."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from itertools import pairwise
import re
from typing import Final

from taskiq_clickhouse._schema.canonical import canonical_json_bytes, normalize_ddl, sha256_hex
from taskiq_clickhouse._schema.contracts import SchemaContract
from taskiq_clickhouse._schema.migration_parameters import (
    empty_query_parameters,
    freeze_query_parameters,
)
from taskiq_clickhouse._schema.validation import (
    require_bool,
    require_instance,
    require_tuple_of,
    require_uint32,
)
from taskiq_clickhouse._types import MigrationExecution


_MIGRATION_NAME_PATTERN: Final = re.compile(r"[a-z][a-z0-9_-]{0,127}\Z")
_DUPLICATE_NAMES_ERROR: Final = "migration names must not contain duplicates"


@dataclass(frozen=True, slots=True)
class MigrationStep:
    """One normalized DDL statement with complete before/after contracts."""

    ddl: str
    before: SchemaContract
    after: SchemaContract
    query_parameters: Mapping[str, str] = field(default_factory=empty_query_parameters)

    def __post_init__(self) -> None:
        """Normalize DDL and require structured contracts."""
        object.__setattr__(self, "ddl", normalize_ddl(self.ddl))
        require_instance(self.before, SchemaContract, field="migration before state")
        require_instance(self.after, SchemaContract, field="migration after state")
        object.__setattr__(self, "query_parameters", freeze_query_parameters(self.query_parameters))
        if self.before == self.after:
            msg = "migration step must change its schema contract"
            raise ValueError(msg)

    def canonical_data(self) -> dict[str, object]:
        """Return normalized migration-step data for checksum encoding."""
        return {
            "after": self.after.canonical_data(),
            "before": self.before.canonical_data(),
            "ddl": self.ddl,
            "query_parameters": dict(self.query_parameters),
        }


@dataclass(frozen=True, slots=True)
class MigrationDefinition:
    """Immutable forward migration definition."""

    version: int
    name: str
    execution: MigrationExecution
    reentrant: bool
    concurrent_safe: bool
    steps: tuple[MigrationStep, ...]

    def __post_init__(self) -> None:
        """Validate identity, policy and ordered migration steps."""
        require_uint32(self.version, field="migration version")
        _name(self.name)
        require_instance(self.execution, MigrationExecution, field="migration execution")
        require_bool(self.reentrant, field="reentrant")
        require_bool(self.concurrent_safe, field="concurrent_safe")
        _steps(self.steps)
        is_auto = self.execution is MigrationExecution.AUTO
        is_safe = self.reentrant and self.concurrent_safe
        if is_auto and not is_safe:
            msg = "AUTO migration must be reentrant and concurrent-safe"
            raise ValueError(msg)

    @property
    def target(self) -> SchemaContract:
        """Return the complete final schema contract."""
        return self.steps[-1].after

    @property
    def payload_bytes(self) -> bytes:
        """Return the exact canonical migration descriptor."""
        return canonical_json_bytes(self.canonical_data())

    @property
    def payload_text(self) -> str:
        """Return the canonical descriptor as Unicode text."""
        return self.payload_bytes.decode()

    @property
    def checksum(self) -> str:
        """Return the descriptor SHA-256 checksum."""
        return sha256_hex(self.payload_bytes)

    def canonical_data(self) -> dict[str, object]:
        """Return every immutable field covered by the checksum."""
        return {
            "concurrent_safe": self.concurrent_safe,
            "execution_class": self.execution.value,
            "name": self.name,
            "reentrant": self.reentrant,
            "steps": [step.canonical_data() for step in self.steps],
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class SchemaPlan:
    """One immutable, contiguous forward migration plan."""

    migrations: tuple[MigrationDefinition, ...]

    def __post_init__(self) -> None:
        """Require contiguous versions, names and physical states."""
        _plan(self.migrations)

    @property
    def target_version(self) -> int:
        """Return zero for an empty plan or its final version."""
        if not self.migrations:
            return 0
        return self.migrations[-1].version


def _name(name: object) -> None:
    if not isinstance(name, str):
        msg = "migration name must be a stable lowercase identifier"
        raise TypeError(msg)
    if _MIGRATION_NAME_PATTERN.fullmatch(name) is None:
        msg = "migration name must be a stable lowercase identifier"
        raise ValueError(msg)


def _steps(steps: object) -> None:
    validated_steps = require_tuple_of(steps, MigrationStep, field="migration steps")
    if not validated_steps:
        msg = "migration must contain at least one step"
        raise ValueError(msg)
    for previous, current in pairwise(validated_steps):
        if previous.after != current.before:
            msg = "migration step contracts must form one continuous chain"
            raise ValueError(msg)


def _plan(migrations: object) -> None:
    validated_migrations = require_tuple_of(
        migrations,
        MigrationDefinition,
        field="migrations",
    )
    names: list[str] = []
    previous_target: SchemaContract | None = None
    for expected_version, migration in enumerate(validated_migrations, start=1):
        _plan_migration(migration, expected_version=expected_version, previous_target=previous_target)
        names.append(migration.name)
        previous_target = migration.target
    if len(names) != len(set(names)):
        raise ValueError(_DUPLICATE_NAMES_ERROR)


def _plan_migration(
    migration: MigrationDefinition,
    *,
    expected_version: int,
    previous_target: SchemaContract | None,
) -> None:
    if migration.version != expected_version:
        msg = "migration plan versions must be contiguous from one"
        raise ValueError(msg)
    if previous_target is not None and migration.steps[0].before != previous_target:
        msg = "migration plan schema contracts must form one continuous chain"
        raise ValueError(msg)
