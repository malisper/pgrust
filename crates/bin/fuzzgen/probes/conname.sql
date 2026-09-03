-- sitediff per-statement auto-name probe (plan §4.4, parse_utilcmd-6):
-- after every DDL statement whose target table still exists, the
-- auto-generated constraint and index names of that table, sorted. Runs
-- on the issuing session so an in-transaction DDL is visible.
-- {{table}} = the last DDL target, rendered by probes.rs.

-- name: conname
-- when: table
SELECT c.conname, c.contype, c.convalidated, pg_get_constraintdef(c.oid) AS def
  FROM pg_constraint c
 WHERE c.conrelid = '{{table}}'::regclass
 ORDER BY 1, 2, 3, 4;

-- name: indexname
-- when: table
SELECT i.indexrelid::regclass::text AS idx, i.indisprimary, i.indisunique, i.indisvalid,
       pg_get_indexdef(i.indexrelid) AS def
  FROM pg_index i
 WHERE i.indrelid = '{{table}}'::regclass
 ORDER BY 1, 2, 3, 4, 5;

-- name: attname
-- when: table
SELECT a.attnum, a.attname, a.atttypid::regtype::text AS typ, a.attnotnull, a.atthasdef, a.attidentity,
       a.attgenerated, a.attisdropped, a.attcompression
  FROM pg_attribute a
 WHERE a.attrelid = '{{table}}'::regclass AND a.attnum > 0
 ORDER BY 1;
