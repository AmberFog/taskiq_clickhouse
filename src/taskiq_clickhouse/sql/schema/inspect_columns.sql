SELECT position, name, type, default_kind, default_expression,
       compression_codec
FROM system.columns
WHERE database = {database:String} AND table = {table:String}
ORDER BY position
