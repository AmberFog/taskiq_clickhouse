SELECT now64(6, 'UTC') AS observed_at,
       generation_at, generation_id, state, visible_until, purge_at,
       result_payload
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
ORDER BY namespace DESC, task_id DESC,
         generation_at DESC, generation_id DESC, state DESC
LIMIT 1
