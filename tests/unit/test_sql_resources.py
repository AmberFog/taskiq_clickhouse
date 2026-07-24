"""Test packaged SQL loading and validated table bindings."""

from pathlib import Path
from typing import cast

import pytest

from taskiq_clickhouse import _sql
from taskiq_clickhouse._identifiers import Identifier, QualifiedTable
from taskiq_clickhouse._schema.layout import MetadataLayout
from taskiq_clickhouse._storage.layout import StorageLayout
from taskiq_clickhouse._storage.queries import ProgressQueries, ResultQueries


_TABLE = QualifiedTable(Identifier("analytics"), Identifier("task_results"))


def _unexpected_resource_read(
    _path: Path,
    _encoding: str | None = None,
    _errors: str | None = None,
) -> str:
    """Fail when an already-imported runtime attempts another disk read."""
    msg = "SQL resources must be loaded only once"
    raise AssertionError(msg)


def test_load_sql_reads_utf8_and_strips_only_resource_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Decode UTF-8 once and preserve meaningful SQL formatting."""
    resource = tmp_path / "query.sql"
    resource.write_text("\n  SELECT 'данные'\n    FROM system.one  \n", encoding="utf-8")
    monkeypatch.setattr(_sql, "_SQL_ROOT", tmp_path)

    assert _sql.load_sql("query.sql") == "SELECT 'данные'\n    FROM system.one"


def test_load_sql_is_independent_of_process_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve production resources from the package instead of the CWD."""
    monkeypatch.chdir(tmp_path)

    query = _sql.load_sql("storage/result_no_log.sql")

    assert query.startswith("SELECT now64(6, 'UTC') AS observed_at,")
    assert "FROM {database:Identifier}.{table:Identifier}" in query


def test_runtime_query_access_does_not_reread_sql_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep request construction free from synchronous filesystem access."""
    progress_table = QualifiedTable(Identifier("analytics"), Identifier("task_progress"))
    storage_layout = StorageLayout(_TABLE, progress_table)
    metadata_layout = MetadataLayout(Identifier("analytics"))
    result_queries = ResultQueries(_TABLE)
    progress_queries = ProgressQueries(progress_table)
    monkeypatch.setattr(Path, "read_text", _unexpected_resource_read)

    assert result_queries.no_log.startswith("SELECT now64")
    assert progress_queries.latest.startswith("SELECT now64")
    assert storage_layout.create_result_query.startswith("CREATE TABLE")
    assert storage_layout.create_progress_query.startswith("CREATE TABLE")
    assert metadata_layout.create_query.startswith("CREATE TABLE")
    assert metadata_layout.read_query.startswith("SELECT record_kind")


@pytest.mark.parametrize(
    "relative_path",
    ["", ".", "../query.sql", "storage/../query.sql"],
)
def test_load_sql_rejects_paths_outside_a_named_resource(relative_path: str) -> None:
    """Do not turn the package resource loader into a filesystem API."""
    with pytest.raises(ValueError, match="relative to the package SQL root"):
        _sql.load_sql(relative_path)


def test_load_sql_rejects_absolute_path(tmp_path: Path) -> None:
    """Reject absolute paths even when they point at an existing file."""
    with pytest.raises(ValueError, match="relative to the package SQL root"):
        _sql.load_sql(str(tmp_path / "query.sql"))


def test_load_sql_rejects_non_string_path() -> None:
    """Keep resource identity textual and explicit."""
    with pytest.raises(TypeError, match="path must be a string"):
        _sql.load_sql(cast("str", Path("query.sql")))


def test_load_sql_propagates_missing_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail fast when a required SQL file is absent from a distribution."""
    monkeypatch.setattr(_sql, "_SQL_ROOT", tmp_path)

    with pytest.raises(FileNotFoundError):
        _sql.load_sql("missing.sql")


def test_load_sql_rejects_empty_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never send an empty package resource to ClickHouse."""
    (tmp_path / "empty.sql").write_text(" \n\t", encoding="utf-8")
    monkeypatch.setattr(_sql, "_SQL_ROOT", tmp_path)

    with pytest.raises(ValueError, match="must not be empty"):
        _sql.load_sql("empty.sql")


def test_bind_table_copies_values_and_adds_validated_identifiers() -> None:
    """Keep caller mutation and SQL identifier interpolation outside bindings."""
    parameters: dict[str, object] = {"namespace": "production"}

    bound = _sql.bind_table(_TABLE, parameters)
    parameters["namespace"] = "changed"

    assert bound == {
        "database": "analytics",
        "table": "task_results",
        "namespace": "production",
    }


def test_bind_table_accepts_omitted_parameters() -> None:
    """Bind a table without requiring unrelated query values."""
    assert _sql.bind_table(_TABLE) == {
        "database": "analytics",
        "table": "task_results",
    }


@pytest.mark.parametrize("reserved_name", ["database", "table"])
def test_bind_table_rejects_identifier_override(reserved_name: str) -> None:
    """Do not let caller values replace validated identifier components."""
    with pytest.raises(ValueError, match="must not override"):
        _sql.bind_table(_TABLE, {reserved_name: "other"})


def test_bind_table_rejects_non_table() -> None:
    """Require the identifier value object before producing driver values."""
    with pytest.raises(TypeError, match="must be a QualifiedTable"):
        _sql.bind_table(cast("QualifiedTable", object()))


def test_bind_table_rejects_non_mapping_parameters() -> None:
    """Do not coerce arbitrary containers into query parameters."""
    with pytest.raises(TypeError, match="must be a mapping"):
        _sql.bind_table(_TABLE, cast("dict[str, object]", ()))


def test_bind_table_rejects_non_string_parameter_name() -> None:
    """Keep names compatible with clickhouse-connect parameter binding."""
    with pytest.raises(TypeError, match="names must be strings"):
        _sql.bind_table(_TABLE, cast("dict[str, object]", {1: "value"}))
