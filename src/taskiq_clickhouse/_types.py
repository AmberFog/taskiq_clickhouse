"""Shared public and internal literal types."""

__all__ = ("MigrationExecution", "SchemaActor", "SchemaMode")

from enum import StrEnum
from typing import Literal, TypeAlias


SchemaMode: TypeAlias = Literal["migrate", "validate"]


class MigrationExecution(StrEnum):
    """Operator policy attached immutably to a migration definition."""

    AUTO = "AUTO"
    CONTROLLED = "CONTROLLED"


class SchemaActor(StrEnum):
    """Caller class used to enforce migration execution policy."""

    WORKER = "WORKER"
    MANAGER = "MANAGER"
