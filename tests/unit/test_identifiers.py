"""Test strict ClickHouse identifier value objects."""

from typing import cast

import pytest

from taskiq_clickhouse._identifiers import Identifier, QualifiedTable


@pytest.mark.parametrize("value", ["a", "_", "Database_01", "a" * 127])
def test_identifier_accepts_exact_contract(value: str) -> None:
    """Accept one safe unqualified identifier component."""
    identifier = Identifier(value)

    assert str(identifier) == value
    assert identifier.value == value
    assert identifier.quoted == f"`{value}`"


@pytest.mark.parametrize(
    "value",
    ["", "1table", "has.dot", "has-hyphen", "has space", "a" * 128, "таблица"],
)
def test_identifier_rejects_unsafe_text(value: str) -> None:
    """Reject input that is not one ASCII identifier component."""
    with pytest.raises(ValueError, match="identifier contract"):
        Identifier(value)


def test_identifier_rejects_non_string() -> None:
    """Do not coerce arbitrary values to identifier text."""
    with pytest.raises(TypeError, match="identifier contract"):
        Identifier(cast("str", 1))


def test_qualified_table_has_canonical_and_sql_forms() -> None:
    """Keep metadata and quoted SQL representations distinct."""
    table = QualifiedTable(Identifier("analytics"), Identifier("results"))

    assert table.canonical == "analytics.results"
    assert table.quoted == "`analytics`.`results`"
    assert str(table) == table.canonical


@pytest.mark.parametrize(
    ("database", "table"),
    [
        pytest.param(object(), Identifier("table"), id="database"),
        pytest.param(Identifier("database"), object(), id="table"),
    ],
)
def test_qualified_table_requires_identifiers(database: object, table: object) -> None:
    """Reject bypassing component validation."""
    with pytest.raises(TypeError, match="Identifier instances"):
        QualifiedTable(cast("Identifier", database), cast("Identifier", table))
