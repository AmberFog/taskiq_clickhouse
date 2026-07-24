SELECT
    now64(6, 'UTC') AS written_at,
    maxOrNull(generation_at) AS latest_generation_at,
    maxOrNull(purge_at) AS latest_purge_at
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
