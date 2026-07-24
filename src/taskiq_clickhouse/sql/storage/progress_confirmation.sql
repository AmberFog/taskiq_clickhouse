SELECT 1
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE generation_at = {generation_at:DateTime64(6, 'UTC')}
  AND generation_id = {generation_id:UUID}
LIMIT 1
