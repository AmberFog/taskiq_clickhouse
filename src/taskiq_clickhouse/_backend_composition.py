"""Explicit composition root for one side-effect-free backend instance."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from taskiq_clickhouse._backend_runtime import BackendRuntime, RuntimeDependencies
from taskiq_clickhouse._clickhouse.adapter import ClickHouseGateway
from taskiq_clickhouse._executor_admission import SubmissionAdmission
from taskiq_clickhouse._lifecycle import create_client
from taskiq_clickhouse._progress_serialization import ProgressCodec
from taskiq_clickhouse._schema.records import NamespaceContract
from taskiq_clickhouse._schema.runner import SchemaBarrierContext, run_schema_barrier
from taskiq_clickhouse._serialization import ResultCodec
from taskiq_clickhouse._storage.layout import (
    StorageLayout,
    build_storage_plan,
    storage_layout_from_names,
)
from taskiq_clickhouse._storage.repository import StorageRepository


if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from taskiq.abc.serializer import TaskiqSerializer

    from taskiq_clickhouse._config_models import BackendConfig
    from taskiq_clickhouse._storage_policy import StoragePolicy


@dataclass(frozen=True, slots=True, repr=False)
class BackendComponents:
    """Cohesive collaborators retained by the public Taskiq facade."""

    runtime: BackendRuntime
    result_codec: ResultCodec
    progress_codec: ProgressCodec
    keep_results: bool


def compose_backend(
    config: BackendConfig,
    serializer: TaskiqSerializer,
) -> BackendComponents:
    """Wire immutable policies to the concrete ClickHouse adapter."""
    storage = config.storage
    layout = storage_layout_from_names(
        config.endpoint.database,
        storage.result_table,
        storage.progress_table,
    )
    context = _schema_context(config, layout)
    dependencies = RuntimeDependencies(
        client_factory=partial(create_client, config),
        schema_runner=partial(run_schema_barrier, context),
        repository_factory=partial(
            _build_repository,
            layout=layout,
            policy=storage.policy,
        ),
    )
    serializer_admission = SubmissionAdmission()
    return BackendComponents(
        runtime=BackendRuntime(
            dependencies,
            schema_mode=storage.schema_mode,
        ),
        result_codec=ResultCodec(serializer, serializer_admission),
        progress_codec=ProgressCodec(
            serializer,
            serializer_admission,
        ),
        keep_results=storage.keep_results,
    )


def _schema_context(
    config: BackendConfig,
    layout: StorageLayout,
) -> SchemaBarrierContext:
    plan = build_storage_plan(layout)
    namespace_contract = NamespaceContract(
        namespace=config.storage.policy.namespace.namespace,
        result_table=layout.result_table,
        progress_table=layout.progress_table,
        serializer_id=config.storage.serializer_id,
        result_ttl_us=config.storage.policy.retention.result_ttl_us,
        purge_ttl_us=config.storage.policy.retention.purge_ttl_us,
    )
    return SchemaBarrierContext.production(namespace_contract, plan)


def _build_repository(
    client: AsyncClient,
    *,
    layout: StorageLayout,
    policy: StoragePolicy,
) -> StorageRepository:
    return StorageRepository(
        gateway=ClickHouseGateway(client),
        layout=layout,
        policy=policy,
    )
