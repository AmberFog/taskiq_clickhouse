SELECT record_kind, scope, record_key, version, name, payload, checksum,
       package_version, recorded_at, attempt_id
FROM {database:Identifier}.{table:Identifier}
PREWHERE record_kind = {record_kind:String}
    AND scope = {scope:String}
    AND record_key = {record_key:String}
ORDER BY record_kind, scope, record_key, version, checksum, recorded_at, attempt_id
