SELECT engine, engine_full, partition_key, sorting_key, primary_key,
       sampling_key, create_table_query,
       formatQuery(create_table_query) AS formatted_create_table_query,
       formatQuery(concat(
           'CREATE TABLE taskiq_clickhouse_constraint_probe ',
           '(value UInt8, CONSTRAINT taskiq_probe CHECK value > 0) ',
           'ENGINE = MergeTree ORDER BY value'
       )) AS formatted_constraint_probe,
       EXISTS(
           SELECT 1
           FROM system.data_skipping_indices AS auxiliary_index
           WHERE auxiliary_index.database = {database:String}
             AND auxiliary_index.table = {table:String}
       ) AS has_data_skipping_indices,
       EXISTS(
           SELECT 1
           FROM system.projections AS auxiliary_projection
           WHERE auxiliary_projection.database = {database:String}
             AND auxiliary_projection.table = {table:String}
       ) AS has_projections,
       toUInt8(
           notEmpty(dependencies_database) OR notEmpty(dependencies_table)
       ) AS has_dependent_materialized_views
FROM system.tables
WHERE database = {database:String} AND name = {table:String}
