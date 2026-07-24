"""Bounded acknowledgement for one frozen logical write."""

from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Final

from taskiq_clickhouse.exceptions import ClickHouseBackendIOError


_INVALID_ATTEMPT_OUTCOME: Final = "write attempt returned an invalid outcome"
_INVALID_CONFIRMATION_OUTCOME: Final = "write confirmation returned an invalid outcome"


class AttemptOutcome(StrEnum):
    """Classify only the acknowledgement fact needed by the protocol."""

    ACKNOWLEDGED = "acknowledged"
    AMBIGUOUS = "ambiguous"


InsertAttempt = Callable[[], Awaitable[AttemptOutcome]]
ExactConfirmation = Callable[[], Awaitable[bool]]


async def acknowledge_bounded_write(
    attempt: InsertAttempt,
    confirm: ExactConfirmation,
    *,
    operation: str,
) -> None:
    """Confirm an ambiguous insert, then retry the same frozen write once."""
    if await _attempt_or_confirm(attempt, confirm):
        return
    if await _attempt_or_confirm(attempt, confirm):
        return
    raise ClickHouseBackendIOError(operation, "write_unconfirmed") from None


async def _attempt_or_confirm(
    attempt: InsertAttempt,
    confirm: ExactConfirmation,
) -> bool:
    outcome = await attempt()
    if outcome is AttemptOutcome.ACKNOWLEDGED:
        return True
    if outcome is not AttemptOutcome.AMBIGUOUS:
        raise TypeError(_INVALID_ATTEMPT_OUTCOME)
    confirmation = await confirm()
    if type(confirmation) is not bool:  # noqa: WPS516 - confirmations are exact protocol facts.
        raise TypeError(_INVALID_CONFIRMATION_OUTCOME)
    return confirmation
