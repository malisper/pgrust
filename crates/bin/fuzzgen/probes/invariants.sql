-- sitediff invariants deck (plan §4.4): every statement MUST return zero
-- rows on C 18.6. Any row on B is a self-oracle finding (plane
-- probe:invariants), no A/B compare needed. The runner also runs every
-- statement on A: a row there is a rig error (the invariant is wrong),
-- reported once as `invariant-rig-error-<key>` and never as a B finding. Same marker grammar as
-- catalog-deck.sql. `-- each: seq` statements render once per generated
-- sequence; nextval() consumes one pair of values on both sides alike.

-- name: dangling_depend
SELECT d.classid::regclass::text AS class, d.deptype,
       pg_describe_object(d.classid, d.objid, d.objsubid) AS obj
  FROM pg_depend d
 WHERE d.deptype <> 'p'
   AND pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) IS NULL
 ORDER BY 1, 2, 3;

-- name: relnatts_mismatch
SELECT n.nspname, c.relname, c.relnatts,
       (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0) AS atts
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f', 'c', 't')
   AND c.relnatts <> (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0)
 ORDER BY 1, 2;

-- name: index_without_pg_index
SELECT n.nspname, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('i', 'I')
   AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indexrelid = c.oid)
 ORDER BY 1, 2;

-- name: pg_index_without_relation
SELECT i.indexrelid::regclass::text AS idx, i.indrelid::regclass::text AS rel
  FROM pg_index i
 WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = i.indexrelid)
    OR NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = i.indrelid)
 ORDER BY 1, 2;

-- name: constraint_without_relation
SELECT c.conname
  FROM pg_constraint c
 WHERE c.conrelid <> 0
   AND NOT EXISTS (SELECT 1 FROM pg_class r WHERE r.oid = c.conrelid)
 ORDER BY 1;

-- name: relation_without_type
SELECT n.nspname, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.reltype <> 0
   AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.reltype)
 ORDER BY 1, 2;

-- name: type_relid_mismatch
SELECT n.nspname, t.typname
  FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
 WHERE t.typrelid <> 0
   AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = t.typrelid AND c.reltype = t.oid)
 ORDER BY 1, 2;

-- name: relation_without_namespace
SELECT c.relname
  FROM pg_class c
 WHERE NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.oid = c.relnamespace)
 ORDER BY 1;

-- name: namespace_without_owner
SELECT n.nspname
  FROM pg_namespace n
 WHERE NOT EXISTS (SELECT 1 FROM pg_authid r WHERE r.oid = n.nspowner)
 ORDER BY 1;

-- name: duplicate_attribute
SELECT a.attrelid::regclass::text AS rel, a.attnum, count(*) AS n
  FROM pg_attribute a
 GROUP BY 1, 2
HAVING count(*) > 1
 ORDER BY 1, 2;

-- name: toast_without_relation
SELECT n.nspname, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.reltoastrelid <> 0
   AND NOT EXISTS (SELECT 1 FROM pg_class t WHERE t.oid = c.reltoastrelid AND t.relkind = 't')
 ORDER BY 1, 2;

-- name: inherits_without_parent
SELECT i.inhrelid::regclass::text AS child
  FROM pg_inherits i
 WHERE NOT EXISTS (SELECT 1 FROM pg_class p WHERE p.oid = i.inhparent)
 ORDER BY 1;

-- name: attrdef_without_column
SELECT d.adrelid::regclass::text AS rel, d.adnum
  FROM pg_attrdef d
 WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                    WHERE a.attrelid = d.adrelid AND a.attnum = d.adnum AND a.atthasdef)
 ORDER BY 1, 2;

-- name: partition_without_bound
-- Partition INDEXES (relkind 'I'/'i') also carry relispartition = true and
-- never have a relpartbound (first LIVE smoke: 894 false rows on C 18.6),
-- so only relations that can carry a bound are checked.
SELECT n.nspname, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relispartition
   AND c.relkind IN ('r', 'p', 'f')
   AND (c.relpartbound IS NULL
        OR NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid))
 ORDER BY 1, 2;

-- name: trigger_without_function
SELECT t.tgrelid::regclass::text AS rel, t.tgname
  FROM pg_trigger t
 WHERE NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = t.tgfoid)
 ORDER BY 1, 2;

-- name: sequence_monotone
-- each: seq
SELECT '{{seq}}' AS seq
 WHERE NOT (nextval('{{seq}}') < nextval('{{seq}}'));
