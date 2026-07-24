"""Test the installable package scaffold."""

from importlib import metadata, resources

import taskiq_clickhouse
from taskiq_clickhouse import (
    backend as backend_module,
    receiver as receiver_module,
    schema as schema_module,
)


def test_distribution_version() -> None:
    """Expose the intended initial distribution version."""
    assert metadata.version("taskiq-clickhouse") == "0.1.0rc1"


def test_distribution_compatibility_metadata_is_exact() -> None:
    """Keep installed interpreter and direct dependency promises evidence-backed."""
    distribution_metadata = metadata.metadata("taskiq-clickhouse")

    assert distribution_metadata["Requires-Python"] == ">=3.11"
    assert distribution_metadata["License-Expression"] == "MIT"
    assert distribution_metadata.get_all("License-File") == ["LICENSE"]
    assert distribution_metadata.get_all("Classifier") == [
        "Development Status :: 3 - Alpha",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
    ]
    assert metadata.requires("taskiq-clickhouse") == [
        "clickhouse-connect[async]>=1.4.2,<2",
        "pydantic>=2.7,<3",
        "taskiq>=0.12.4,<1",
    ]


def test_schema_console_script_is_installed() -> None:
    """Keep the documented schema command reachable from an installed wheel."""
    entry_points = tuple(
        metadata.entry_points(
            group="console_scripts",
            name="taskiq-clickhouse-schema",
        ),
    )

    assert len(entry_points) == 1
    assert entry_points[0].value == "taskiq_clickhouse._cli:main"


def test_root_exports_are_exact() -> None:
    """Expose only the frozen backend, schema mode and safe errors."""
    expected_exports = (
        "ClickHouseBackendIOError",
        "ClickHouseConfigurationError",
        "ClickHouseDataCorruptionError",
        "ClickHouseDecodeError",
        "ClickHouseEncodeError",
        "ClickHouseLifecycleError",
        "ClickHouseMigrationError",
        "ClickHouseNamespaceError",
        "ClickHouseProgressError",
        "ClickHouseResultBackend",
        "ClickHouseResultBackendError",
        "ClickHouseResultNotFoundError",
        "ClickHouseSchemaDriftError",
        "ClickHouseSchemaError",
        "ClickHouseSerializationError",
        "ResultPersistenceReceiver",
        "SchemaMode",
    )

    assert taskiq_clickhouse.__all__ == expected_exports
    assert all(hasattr(taskiq_clickhouse, export) for export in expected_exports)
    assert not hasattr(taskiq_clickhouse, "ClickHouseSchemaManager")
    assert not hasattr(taskiq_clickhouse, "JSONSerializer")


def test_public_submodules_export_only_supported_facades() -> None:
    """Keep package-private runtime seams out of supported star imports."""
    assert backend_module.__all__ == ("ClickHouseResultBackend",)
    assert receiver_module.__all__ == ("ResultPersistenceReceiver",)
    assert schema_module.__all__ == ("ClickHouseSchemaManager",)


def test_typing_marker() -> None:
    """Ship the PEP 561 marker as a package resource."""
    package_files = resources.files(taskiq_clickhouse)

    assert package_files.joinpath("py.typed").is_file()


def test_complete_sql_resource_tree_is_packaged() -> None:
    """Ship every production statement in the wheel package directory."""
    expected_tree = {
        "migrations": {
            "v001_create_progress_table.sql",
            "v001_create_result_table.sql",
        },
        "schema": {
            "create_metadata_table.sql",
            "describe_table.sql",
            "inspect_columns.sql",
            "inspect_table.sql",
            "metadata_confirmation.sql",
            "metadata_read.sql",
            "server_now.sql",
        },
        "storage": {
            "allocate_generation.sql",
            "progress_confirmation.sql",
            "progress_latest.sql",
            "result_confirmation.sql",
            "result_no_log.sql",
            "result_readiness.sql",
            "result_with_log.sql",
        },
    }
    sql_root = resources.files(taskiq_clickhouse).joinpath("sql")

    assert sql_root.is_dir()
    assert {entry.name for entry in sql_root.iterdir() if entry.is_file()} == set()
    assert {entry.name for entry in sql_root.iterdir() if entry.is_dir()} == set(expected_tree)
    for directory_name, expected_files in expected_tree.items():
        directory = sql_root.joinpath(directory_name)
        entries = tuple(directory.iterdir())

        assert all(entry.is_file() for entry in entries)
        assert {entry.name for entry in entries} == expected_files
        assert all(entry.read_text(encoding="utf-8").strip() for entry in entries)
