SELECT 1
FROM {database:Identifier}.{table:Identifier}
PREWHERE namespace = {namespace:String} AND task_id = {task_id:String}
WHERE generation_at = {generation_at:DateTime64(6, 'UTC')}
  AND generation_id = {generation_id:UUID}
  AND state = {state:UInt8}
  AND written_at = {written_at:DateTime64(6, 'UTC')}
  AND visible_until = {visible_until:DateTime64(6, 'UTC')}
  AND purge_at = {purge_at:DateTime64(6, 'UTC')}
LIMIT 1
