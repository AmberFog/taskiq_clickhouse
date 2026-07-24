CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier}
(
    `namespace` String,
    `task_id` String,
    `generation_at` DateTime64(6, 'UTC'),
    `generation_id` UUID,
    `written_at` DateTime64(6, 'UTC'),
    `visible_until` DateTime64(6, 'UTC'),
    `purge_at` DateTime64(6, 'UTC'),
    `progress_payload` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(purge_at)
PRIMARY KEY (namespace, task_id)
ORDER BY
(
    namespace,
    task_id,
    generation_at,
    generation_id
)
TTL purge_at DELETE
