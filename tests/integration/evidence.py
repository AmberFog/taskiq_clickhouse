"""Write non-secret, reviewable evidence for integration POC runs."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tests.integration.settings import ClickHouseTestSettings


async def write_evidence(
    settings: ClickHouseTestSettings,
    filename: str,
    payload: Mapping[str, object],
) -> None:
    """Persist one version-attributed JSON observation outside the event loop."""
    document: dict[str, object] = {
        "client_version": settings.expected_client_version,
        "server_profile": settings.profile,
        "server_version": settings.expected_version,
        **payload,
    }
    await asyncio.to_thread(
        _write_json,
        settings.evidence_dir / filename,
        document,
    )


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
