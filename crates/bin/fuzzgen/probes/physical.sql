-- sitediff physical probes (plan §4.4 / §5.2): per written table, the
-- on-disk shape of every column — pg_column_size, compression method,
-- whether the datum went out of line — pk-ordered (total order), plus a
-- relation-size bucket (never the exact byte count: page rounding and
-- fillfactor differ by build, the bucket does not).
-- {{table}} {{pk}} {{column}} rendered by probes.rs; `each: column`.

-- name: column_shape
-- when: table
-- each: column
SELECT {{pk}} AS pk, '{{column}}' AS col,
       pg_column_size({{column}}) AS size,
       pg_column_compression({{column}}) AS compression,
       (pg_column_toast_chunk_id({{column}}) IS NOT NULL) AS toasted
  FROM {{table}}
 ORDER BY {{pk}};

-- name: relation_size_bucket
-- when: table
SELECT '{{table}}' AS rel,
       CASE WHEN s = 0 THEN 'empty'
            WHEN s <= 8192 * 4 THEN 'le4p'
            WHEN s <= 8192 * 64 THEN 'le64p'
            WHEN s <= 8192 * 1024 THEN 'le1kp'
            ELSE 'gt1kp' END AS bucket
  FROM (SELECT pg_relation_size('{{table}}') AS s) x;
