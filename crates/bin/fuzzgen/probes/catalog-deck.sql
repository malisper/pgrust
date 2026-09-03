-- sitediff catalog deck (plan §4.4). Runs after every DDL bracket, restart
-- and stream end on BOTH sides. Every statement is OID-free (object
-- identity through pg_describe_object / regclass / names; raw OIDs only
-- under {{class_oid_col}} for the binupgrade cell) and totally ordered.
-- Each statement is introduced by `-- name: <key>`; probes.rs splits on
-- those markers and keys the deck result by them. Optional markers:
--   -- when: <flag>     render only when the flag is set (raw_oids, gucs)
--   -- each: <list>     render once per list element (seq, column, guc)
-- Placeholders: {{class_oid_col}} {{guc}} {{table}} {{pk}} {{column}} {{seq}}

-- name: depend
SELECT d.deptype,
       pg_describe_object(d.classid, d.objid, d.objsubid) AS obj,
       pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) AS ref
  FROM pg_depend d
 WHERE d.objid >= 16384 OR d.refobjid >= 16384
 ORDER BY 1, 2, 3;

-- name: init_privs
SELECT pg_describe_object(classoid, objoid, objsubid) AS obj, privtype, initprivs::text
  FROM pg_init_privs
 ORDER BY 1, 2, 3;

-- name: description_builtin
SELECT count(*) AS n,
       md5(string_agg(pg_describe_object(classoid, objoid, objsubid) || E'\t' || description, E'\n'
                      ORDER BY pg_describe_object(classoid, objoid, objsubid), description)) AS digest
  FROM pg_description
 WHERE objoid <= 9999;

-- name: description_user
SELECT pg_describe_object(classoid, objoid, objsubid) AS obj, description
  FROM pg_description
 WHERE objoid > 9999
 ORDER BY 1, 2;

-- name: constraint
SELECT n.nspname, c.conname, c.contype, c.connoinherit, c.convalidated, (c.conbin IS NULL) AS nobin,
       c.conrelid::regclass::text AS rel, c.contypid::regtype::text AS typ, pg_get_constraintdef(c.oid) AS def
  FROM pg_constraint c
  JOIN pg_namespace n ON n.oid = c.connamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%'
 ORDER BY 1, 2, 3, 7, 8, 9;

-- name: class
SELECT {{class_oid_col}}n.nspname, c.relname, c.relkind, c.relpersistence, c.reloptions::text,
       coalesce(s.spcname, '') AS tablespace, t.typname AS reltype, c.relhasindex, c.relispartition,
       c.relreplident, c.relrowsecurity, c.relforcerowsecurity, c.relam::regclass::text AS am
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_tablespace s ON s.oid = c.reltablespace
  LEFT JOIN pg_type t ON t.oid = c.reltype
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%'
 ORDER BY 1, 2, 3, 4;

-- name: type
SELECT n.nspname, t.typname, t.typtype, t.typcategory, t.typlen, t.typbyval, t.typalign, t.typstorage,
       t.typnotnull, t.typndims, t.typdefault, t.typbasetype::regtype::text AS basetype,
       t.typelem::regtype::text AS elem, t.typarray::regtype::text AS arr, t.typcollation::regcollation::text AS coll
  FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%'
 ORDER BY 1, 2, 3;

-- name: proc
SELECT n.nspname, p.proname, p.proacl::text, p.prokind, p.pronargs, p.provolatile, p.proparallel,
       p.prosecdef, p.proleakproof, p.proisstrict, p.proretset, p.prorettype::regtype::text AS rettype,
       pg_get_function_identity_arguments(p.oid) AS args, l.lanname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_language l ON l.oid = p.prolang
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
 ORDER BY 1, 2, 13, 3, 4;

-- name: subscription
SELECT subname, subenabled, subtwophasestate, subskiplsn::text, subslotname, subbinary, substream, subowner::regrole::text
  FROM pg_subscription
 ORDER BY 1;

-- name: replication_origin_status
SELECT external_id, remote_lsn::text, (local_lsn IS NOT NULL) AS has_local
  FROM pg_replication_origin_status
 ORDER BY 1, 2;

-- name: event_trigger
SELECT evtname, evtevent, evtowner::regrole::text, evtfoid::regproc::text, evtenabled, evttags::text
  FROM pg_event_trigger
 ORDER BY 1;

-- name: extension
SELECT e.extname, e.extversion, e.extrelocatable, n.nspname, e.extconfig IS NOT NULL AS has_config
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
 ORDER BY 1;

-- name: extension_members
SELECT e.extname, pg_describe_object(d.classid, d.objid, d.objsubid) AS member
  FROM pg_depend d
  JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass AND d.refobjid = e.oid
 WHERE d.deptype = 'e'
 ORDER BY 1, 2;

-- name: db_role_setting
SELECT coalesce(d.datname, '') AS datname, coalesce(r.rolname, '') AS rolname, s.setconfig::text
  FROM pg_db_role_setting s
  LEFT JOIN pg_database d ON d.oid = s.setdatabase
  LEFT JOIN pg_authid r ON r.oid = s.setrole
 ORDER BY 1, 2, 3;

-- name: largeobject_metadata
SELECT rank() OVER (ORDER BY oid) AS lo_rank, lomowner::regrole::text AS owner, lomacl::text
  FROM pg_largeobject_metadata
 ORDER BY 1;

-- name: loaded_modules
SELECT module_name, version, file_name
  FROM pg_get_loaded_modules()
 ORDER BY 1, 2, 3;

-- name: matviews
SELECT schemaname, matviewname, ispopulated, hasindexes, definition
  FROM pg_matviews
 ORDER BY 1, 2;

-- name: show_guc
-- each: guc
SELECT '{{guc}}' AS name, current_setting('{{guc}}', true) AS value;

-- name: file_settings
-- when: gucs
SELECT regexp_replace(sourcefile, '^.*/', '') AS file, seqno, name, setting, applied, error
  FROM pg_file_settings
 ORDER BY 2, 3;

-- name: settings_source
-- when: gucs
SELECT name, setting, source, regexp_replace(sourcefile, '^.*/', '') AS file
  FROM pg_settings
 WHERE source NOT IN ('default', 'override', 'client', 'session')
 ORDER BY 1;
