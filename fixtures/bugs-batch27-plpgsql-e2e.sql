-- Differential fixture for the 2026-09-13 bug batch 27 (pl/plpgsql:
-- scanner keyword spelling, FOR-loop grammar, cursor-declaration lookup
-- mode, %TYPE array decoration, record-field collation, RETURNS record
-- rowtype, array-variable storage, FOR-over-query tuptable release).
-- Run via scripts/bugs-batch27-plpgsql-e2e.sh against the frozen expected
-- captured from C PostgreSQL 18.6.
\set VERBOSITY verbose
\pset pager off

-- fp-pl-plpgsql-pl_scanner#1: an unreserved keyword keeps its own spelling
DO $$DECLARE elsif integer; BEGIN elsif := 1; RAISE NOTICE 'elsif=%', elsif; END$$;
DO $$DECLARE elseif integer; BEGIN elseif := 2; RAISE NOTICE 'elseif=%', elseif; END$$;

-- fp-pl-plpgsql-pl_gram-p1#1: a qualified integer-FOR variable does not shadow the plain name
CREATE FUNCTION b27_qualified_loop() RETURNS integer LANGUAGE plpgsql AS $$
<<blk>> DECLARE i integer := 10;
BEGIN FOR blk.i IN 1..1 LOOP RETURN i; END LOOP; RETURN -1; END $$;
SELECT b27_qualified_loop();

-- fp-pl-plpgsql-pl_gram-p1#2: cursor FOR arguments are read before the loop record exists
CREATE FUNCTION b27_cursor_shadow() RETURNS integer LANGUAGE plpgsql AS $$
DECLARE x integer := 7; c CURSOR(p integer) FOR SELECT p AS n;
BEGIN FOR x IN c(x) LOOP RETURN x.n; END LOOP; RETURN -1; END $$;
SELECT b27_cursor_shadow();

-- fp-pl-plpgsql-pl_gram-p1#3: DECLARE-mode lookup survives a cursor argument list
DO $$ DECLARE x integer := 3; c CURSOR(p integer) FOR SELECT p; y x%TYPE := 4; BEGIN RAISE NOTICE 'y=%', y; END $$;

-- fp-pl-plpgsql-pl_comp#3: typisarray excludes plain-storage vectors, includes array domains
CREATE DOMAIN b27_intarrdom AS integer[];
CREATE FUNCTION b27_vector_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v int2vector; a v%TYPE[]; BEGIN RETURN pg_typeof(a)::text; END $$;
SELECT b27_vector_type();
CREATE FUNCTION b27_oidvector_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v oidvector; a v%TYPE ARRAY; BEGIN RETURN pg_typeof(a)::text; END $$;
SELECT b27_oidvector_type();
CREATE FUNCTION b27_dom_arr_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v b27_intarrdom; a v%TYPE[]; BEGIN RETURN pg_typeof(a)::text; END $$;
SELECT b27_dom_arr_type();
CREATE FUNCTION b27_int_arr_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v integer[]; a v%TYPE[]; BEGIN RETURN pg_typeof(a)::text; END $$;
SELECT b27_int_arr_type();

-- fp-adt-expandedrecord#1 / fp-pl-plpgsql-pl_comp#6 / fp-pl-plpgsql-pl_exec-p3#1:
-- a record field carries the attribute's collation
CREATE TABLE b27_er_coll (x text COLLATE "C", y text);
CREATE TYPE b27_audit_coll AS (x text COLLATE "C", y text COLLATE "POSIX");
CREATE FUNCTION b27_er_coll_test() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r b27_er_coll; BEGIN r.x := 'x'; RETURN pg_collation_for(r.x); END $$;
SELECT b27_er_coll_test();
CREATE FUNCTION b27_field_collation() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r b27_er_coll%ROWTYPE; BEGIN RETURN COLLATION FOR (r.x) || ',' || COLLATION FOR (r.y); END $$;
SELECT b27_field_collation();
CREATE FUNCTION b27_audit_collation() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r b27_audit_coll; BEGIN r.x := 'a'; RETURN COLLATION FOR (r.x) || ',' || COLLATION FOR (r.y); END $$;
SELECT b27_audit_collation();
CREATE FUNCTION b27_record_collation() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN r := ROW('a', 'b')::b27_audit_coll; RETURN COLLATION FOR (r.x) || ',' || COLLATION FOR (r.y); END $$;
SELECT b27_record_collation();
CREATE FUNCTION b27_query_record_collation() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN SELECT 'a'::text COLLATE "POSIX" AS x INTO r; RETURN COLLATION FOR (r.x); END $$;
SELECT b27_query_record_collation();

-- fp-pl-plpgsql-pl_exec-p1#1: RETURNS record hands back the composite datum's own rowtype
CREATE TYPE b27_audit_pair AS (a integer);
CREATE FUNCTION b27_audit_record() RETURNS record LANGUAGE plpgsql AS $$
BEGIN RETURN ROW(1)::b27_audit_pair; END $$;
CREATE FUNCTION b27_audit_record_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN r := b27_audit_record(); RETURN pg_typeof(r)::text || ':' || r.a; END $$;
SELECT b27_audit_record_type();
CREATE FUNCTION b27_anon_record() RETURNS record LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN SELECT 1 AS a, 'x'::text AS b INTO r; RETURN r; END $$;
SELECT * FROM b27_anon_record() AS t(a integer, b text);
CREATE FUNCTION b27_anon_record_type() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN r := b27_anon_record(); RETURN pg_typeof(r)::text || ':' || r.b; END $$;
SELECT b27_anon_record_type();

-- fp-adt-array_expanded#1: an assigned array variable holds a flat, decompressed image
CREATE TABLE b27_expanded_probe (a integer[] COMPRESSION pglz, d b27_intarrdom COMPRESSION pglz);
INSERT INTO b27_expanded_probe VALUES (array_fill(1, ARRAY[10000]), array_fill(2, ARRAY[10000]));
SELECT pg_column_compression(a), pg_column_size(a), pg_column_compression(d), pg_column_size(d) FROM b27_expanded_probe;
CREATE FUNCTION b27_expanded_probe_var() RETURNS text LANGUAGE plpgsql AS $$
DECLARE x integer[]; dd b27_intarrdom; v int2vector;
BEGIN
  x := (SELECT a FROM b27_expanded_probe LIMIT 1);
  dd := (SELECT d FROM b27_expanded_probe LIMIT 1);
  v := (SELECT '1 2 3'::int2vector);
  RETURN coalesce(pg_column_compression(x), '-') || ':' || pg_column_size(x) || ' '
      || coalesce(pg_column_compression(dd), '-') || ':' || pg_column_size(dd) || ' '
      || coalesce(pg_column_compression(v), '-') || ':' || pg_column_size(v) || ' '
      || cardinality(x) || ':' || x[10000] || ':' || dd[1];
END $$;
SELECT b27_expanded_probe_var();
CREATE TABLE b27_toasted (a text[]);
INSERT INTO b27_toasted SELECT array_agg(md5(g::text) || repeat('z', 200)) FROM generate_series(1, 200) g;
CREATE FUNCTION b27_toasted_var() RETURNS text LANGUAGE plpgsql AS $$
DECLARE x text[];
BEGIN
  x := (SELECT a FROM b27_toasted LIMIT 1);
  DELETE FROM b27_toasted;
  RETURN cardinality(x) || ':' || left(x[200], 32) || ':' || coalesce(pg_column_compression(x), '-');
END $$;
SELECT b27_toasted_var();

-- bug_ee504849 (Detail): FOR-over-query loops release their last tuptable on exhaustion
CREATE FUNCTION b27_leak_probe() RETURNS int LANGUAGE plpgsql AS $$
DECLARE r record; n int;
BEGIN
  FOR i IN 1..5 LOOP FOR r IN SELECT 1 AS a LOOP NULL; END LOOP; END LOOP;
  FOR i IN 1..5 LOOP FOR r IN SELECT g FROM generate_series(1, 3) g LOOP NULL; END LOOP; END LOOP;
  FOR i IN 1..5 LOOP FOR r IN EXECUTE 'SELECT 1 AS a' LOOP NULL; END LOOP; END LOOP;
  FOR i IN 1..5 LOOP FOR r IN SELECT g FROM generate_series(1, 3) g LOOP EXIT; END LOOP; END LOOP;
  SELECT count(*) INTO n FROM pg_backend_memory_contexts WHERE name = 'SPI TupTable';
  RETURN n;
END $$;
SELECT b27_leak_probe();
