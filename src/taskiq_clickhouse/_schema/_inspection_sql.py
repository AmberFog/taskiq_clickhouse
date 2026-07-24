"""Small SQL/catalog normalizers used by physical-schema inspection."""

from collections.abc import Sequence
import re
from typing import Final, NoReturn

from taskiq_clickhouse._schema._inspection_types import SchemaInspectionError


_ENGINE_SECTION_PATTERN: Final = re.compile(r"\sENGINE\s*=\s*", flags=re.IGNORECASE)
_TTL_PATTERN: Final = re.compile(
    r"(?:^|\s)TTL\s+(?P<expression>.*?)(?=\sSETTINGS\s|\sCOMMENT\s|$)",
    flags=re.DOTALL | re.IGNORECASE,
)
_SETTINGS_PATTERN: Final = re.compile(
    r"\sSETTINGS\s+(?P<settings>.+)\Z",
    flags=re.DOTALL | re.IGNORECASE,
)
_SETTING_SPLIT_PATTERN: Final = re.compile(
    r",\s*(?=[A-Za-z_][A-Za-z0-9_]*\s*=)",
)
_FORMATTED_TABLE_PREFIX: Final = "CREATE TABLE "
_FORMATTED_COLUMN_LIST_OPEN: Final = "\n(\n"
_FORMATTED_COLUMN_LIST_CLOSE: Final = "\n)\nENGINE = "
_FORMATTED_CONSTRAINT_PREFIX: Final = "    CONSTRAINT "


class SQLExpressionParser:
    """Normalize server-generated clauses without comparing raw CREATE SQL."""

    def table_ttl(self, create_table_query: str) -> str:
        """Return the normalized top-level table TTL expression."""
        engine_section = _ENGINE_SECTION_PATTERN.split(create_table_query, maxsplit=1)[-1]
        match = _TTL_PATTERN.search(engine_section)
        if match is None:
            return ""
        normalized = self.normalize(match.group("expression"))
        if normalized.endswith(" DELETE") and "," not in normalized:
            return normalized.removesuffix(" DELETE")
        return normalized

    def engine_settings(self, engine_full: str) -> tuple[tuple[str, str], ...]:
        """Return normalized engine settings for selective critical checks."""
        match = _SETTINGS_PATTERN.search(engine_full)
        if match is None:
            return ()
        assignments = _SETTING_SPLIT_PATTERN.split(match.group("settings"))
        return tuple(sorted(self._setting(assignment) for assignment in assignments))

    def normalize(self, expression: str) -> str:
        """Remove boundary layout while preserving server-normalized SQL contents."""
        return expression.strip()

    def has_constraints(self, formatted_create_query: str) -> bool:
        """Inspect the top-level column list produced by ClickHouse ``formatQuery``."""
        if not formatted_create_query.startswith(_FORMATTED_TABLE_PREFIX):
            _raise_inspection("formatQuery returned an unsupported CREATE TABLE shape")
        list_start = formatted_create_query.find(_FORMATTED_COLUMN_LIST_OPEN)
        if list_start < 0:
            _raise_inspection("formatQuery returned an unsupported CREATE TABLE shape")
        body_start = list_start + len(_FORMATTED_COLUMN_LIST_OPEN)
        body_end = formatted_create_query.find(
            _FORMATTED_COLUMN_LIST_CLOSE,
            body_start,
        )
        if body_end < 0:
            _raise_inspection("formatQuery returned an unsupported CREATE TABLE shape")
        column_list = formatted_create_query[body_start:body_end]
        return any(line.startswith(_FORMATTED_CONSTRAINT_PREFIX) for line in column_list.splitlines())

    def _setting(self, assignment: str) -> tuple[str, str]:
        name, separator, expression = assignment.partition("=")
        if not separator or not name.strip() or not expression.strip():
            msg = "system.tables.engine_full contains malformed SETTINGS"
            _raise_inspection(msg)
        return self.normalize(name), self.normalize(expression)


class CatalogValueParser:
    """Decode exact typed values from the selected catalog projections."""

    def text(self, raw_value: object, path: str) -> str:
        """Strictly decode a String column requested in bytes format."""
        if not isinstance(raw_value, bytes):
            msg = f"{path} is not an exact byte string"
            _raise_inspection(msg)
        try:
            return raw_value.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            msg = f"{path} is not valid UTF-8"
            raise SchemaInspectionError(msg) from None

    def positive_int(self, raw_value: object, path: str) -> int:
        """Require an exact positive integer catalog position."""
        if not isinstance(raw_value, int) or isinstance(raw_value, bool):
            msg = f"{path} is not a positive integer"
            _raise_inspection(msg)
        if raw_value < 1:
            msg = f"{path} is not a positive integer"
            _raise_inspection(msg)
        return raw_value

    def binary_flag(self, raw_value: object, path: str) -> bool:
        """Require an exact UInt8-style zero-or-one catalog projection."""
        if not isinstance(raw_value, int) or isinstance(raw_value, bool):
            msg = f"{path} is not a binary flag"
            _raise_inspection(msg)
        if raw_value not in (0, 1):
            msg = f"{path} is not a binary flag"
            _raise_inspection(msg)
        return bool(raw_value)

    def require_row_size(self, row: Sequence[object], expected: int, path: str) -> None:
        """Reject catalog shape changes before positional decoding."""
        if len(row) != expected:
            msg = f"{path} returned {len(row)} fields instead of {expected}"
            _raise_inspection(msg)


def _raise_inspection(message: str) -> NoReturn:
    raise SchemaInspectionError(message)


SQL_EXPRESSION_PARSER: Final = SQLExpressionParser()
CATALOG_VALUE_PARSER: Final = CatalogValueParser()
