CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier}
(
    `record_kind` String,
    `scope` String,
    `record_key` String,
    `version` UInt32,
    `name` String,
    `payload` String,
    `checksum` String,
    `package_version` String,
    `recorded_at` DateTime64(6, 'UTC'),
    `attempt_id` UUID
)
ENGINE = MergeTree
PRIMARY KEY (record_kind, scope, record_key, version)
ORDER BY
(
    record_kind,
    scope,
    record_key,
    version,
    checksum,
    recorded_at,
    attempt_id
)
