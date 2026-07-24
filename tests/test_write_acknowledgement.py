"""Unit tests for the reusable bounded write acknowledgement protocol."""

import asyncio
from dataclasses import dataclass, field
from typing import TypeAlias, cast

import pytest

from taskiq_clickhouse._clickhouse.errors import AmbiguousClickHouseError
from taskiq_clickhouse._identifiers import Identifier
from taskiq_clickhouse._schema.layout import MetadataLayout
from taskiq_clickhouse._schema.transport import ExactMetadataWriter
from taskiq_clickhouse._write_acknowledgement import (
    AttemptOutcome,
    acknowledge_bounded_write,
)
from taskiq_clickhouse.exceptions import ClickHouseBackendIOError
from tests.factories.schema import MetadataRecordFactory
from tests.schema_testkit import ScriptedGateway, native_row, synthetic_plan


AttemptEvent: TypeAlias = AttemptOutcome | BaseException
ConfirmationEvent: TypeAlias = bool | BaseException
_OPERATION = "result_write"
_MAX_ATTEMPTS = 2


@dataclass(slots=True)
class _ProtocolScript:
    """Return scripted insert and confirmation outcomes in issuance order."""

    attempt_events: list[AttemptEvent]
    confirmation_events: list[ConfirmationEvent] = field(default_factory=list)
    attempt_count: int = 0
    confirmation_count: int = 0

    async def attempt(self) -> AttemptOutcome:
        """Consume one insert attempt event."""
        self.attempt_count += 1
        event = self.attempt_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event

    async def confirm(self) -> bool:
        """Consume one exact-confirmation event."""
        self.confirmation_count += 1
        event = self.confirmation_events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attempt_events", "confirmation_events", "attempt_count", "confirmation_count"),
    [
        ([AttemptOutcome.ACKNOWLEDGED], [], 1, 0),
        ([AttemptOutcome.AMBIGUOUS], [True], 1, 1),
        (
            [AttemptOutcome.AMBIGUOUS, AttemptOutcome.ACKNOWLEDGED],
            [False],
            2,
            1,
        ),
        (
            [AttemptOutcome.AMBIGUOUS, AttemptOutcome.AMBIGUOUS],
            [False, True],
            2,
            2,
        ),
    ],
)
async def test_bounded_acknowledgement_success_paths(
    attempt_events: list[AttemptEvent],
    confirmation_events: list[ConfirmationEvent],
    attempt_count: int,
    confirmation_count: int,
) -> None:
    """Accept direct ack, confirmed presence, one retry and final presence."""
    script = _ProtocolScript(attempt_events, confirmation_events)

    await acknowledge_bounded_write(
        script.attempt,
        script.confirm,
        operation=_OPERATION,
    )

    assert script.attempt_count == attempt_count
    assert script.confirmation_count == confirmation_count


@pytest.mark.asyncio
async def test_bounded_acknowledgement_stops_after_final_absence() -> None:
    """Fail safely after exactly two ambiguous-and-absent rounds."""
    script = _ProtocolScript(
        [AttemptOutcome.AMBIGUOUS, AttemptOutcome.AMBIGUOUS],
        [False, False],
    )

    with pytest.raises(ClickHouseBackendIOError, match="write_unconfirmed") as raised:
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert script.attempt_count == _MAX_ATTEMPTS
    assert script.confirmation_count == _MAX_ATTEMPTS
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.asyncio
async def test_bounded_acknowledgement_rejects_invalid_attempt_outcome() -> None:
    """Fail closed when an adapter violates the attempt-result protocol."""
    script = _ProtocolScript([cast("AttemptOutcome", "invalid")])

    with pytest.raises(TypeError, match="invalid outcome"):
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert script.attempt_count == 1
    assert script.confirmation_count == 0


@pytest.mark.asyncio
async def test_bounded_acknowledgement_rejects_invalid_confirmation_outcome() -> None:
    """Fail closed when an adapter returns a truthy non-boolean confirmation."""
    script = _ProtocolScript(
        [AttemptOutcome.AMBIGUOUS],
        [cast("bool", 1)],
    )

    with pytest.raises(TypeError, match="invalid outcome"):
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert script.attempt_count == 1
    assert script.confirmation_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["attempt", "confirmation"])
async def test_bounded_acknowledgement_propagates_definite_failure(
    failure_stage: str,
) -> None:
    """Never confirm or retry after a definite insert or confirmation failure."""
    failure = ClickHouseBackendIOError(f"{_OPERATION}_confirm", "database_error")
    if failure_stage == "attempt":
        script = _ProtocolScript([failure])
    else:
        script = _ProtocolScript([AttemptOutcome.AMBIGUOUS], [failure])

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert raised.value is failure
    assert script.attempt_count == 1
    assert script.confirmation_count == (failure_stage == "confirmation")


@pytest.mark.asyncio
async def test_bounded_acknowledgement_does_not_retry_ambiguous_confirmation() -> None:
    """Propagate an ambiguous confirmation instead of issuing a second insert."""
    failure = ClickHouseBackendIOError(f"{_OPERATION}_confirm", "ambiguous_response")
    script = _ProtocolScript([AttemptOutcome.AMBIGUOUS], [failure])

    with pytest.raises(ClickHouseBackendIOError) as raised:
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert raised.value is failure
    assert script.attempt_count == 1
    assert script.confirmation_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation_stage", ["attempt", "confirmation"])
async def test_bounded_acknowledgement_preserves_cancellation_identity(
    cancellation_stage: str,
) -> None:
    """Propagate cancellation without confirmation or retry."""
    cancellation = asyncio.CancelledError()
    if cancellation_stage == "attempt":
        script = _ProtocolScript([cancellation])
    else:
        script = _ProtocolScript([AttemptOutcome.AMBIGUOUS], [cancellation])

    with pytest.raises(asyncio.CancelledError) as raised:
        await acknowledge_bounded_write(
            script.attempt,
            script.confirm,
            operation=_OPERATION,
        )

    assert raised.value is cancellation
    assert script.attempt_count == 1
    assert script.confirmation_count == (cancellation_stage == "confirmation")


@pytest.mark.asyncio
async def test_metadata_writer_reuses_one_frozen_native_request() -> None:
    """Adapt metadata confirmation while retaining the exact request on retry."""
    record = MetadataRecordFactory.from_migration(synthetic_plan().migrations[0])
    gateway = ScriptedGateway(
        insert_events=[AmbiguousClickHouseError(), AmbiguousClickHouseError()],
        query_events=[(), (native_row(record),)],
    )

    await ExactMetadataWriter(gateway, MetadataLayout(Identifier("test_db"))).write(
        record,
        operation="migration_record_write",
    )

    assert len(gateway.inserts) == _MAX_ATTEMPTS
    assert gateway.inserts[0] is gateway.inserts[1]
