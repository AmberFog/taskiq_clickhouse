"""Stable values shared by public result-contract integration scenarios."""

from datetime import timedelta
from typing import Final


RESULT_TTL: Final = timedelta(hours=1)
PURGE_TTL: Final = timedelta(days=1)
THREE_MIB: Final = 3 * 1024 * 1024
CORRUPT_LOG_PAYLOAD: Final = b"{corrupt-log"
OBSERVED_AT_EXPRESSION: Final = "now64(6, 'UTC') AS observed_at"
VISIBILITY_BOUNDARY_EXPRESSION: Final = "visible_until AS observed_at"
CONCURRENCY_TIMEOUT_SECONDS: Final = 15
