-- bugs/batch-09-backend-commands-misc: C-vs-pgrust parity for the fixes in
-- commands/{copy,createas,prepare,publicationcmds,trigger,collationcmds,
-- tablespace,dbcommands,explain,commands_async}. Expected file captured from
-- C 18.6 (scripts/regress-diff.sh --capture). The LISTEN/NOTIFY case must
-- stay last: C answers it with FATAL and closes the session.
\set VERBOSITY verbose

-- copyfromparse: omitted-column defaults still run after a soft error
CREATE SEQUENCE b09_s;
CREATE TABLE b09_t(a int, b bigint DEFAULT nextval('b09_s'));
COPY b09_t(a) FROM STDIN WITH (ON_ERROR ignore);
bad
1
\.
SELECT * FROM b09_t;
DROP TABLE b09_t;
DROP SEQUENCE b09_s;

-- createas: each output row is inserted immediately
CREATE FUNCTION b09_ctas_seen(integer) RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$ DECLARE n bigint; BEGIN EXECUTE 'SELECT count(*) FROM b09_ctas_target' INTO n; RETURN n; END $$;
CREATE TABLE b09_ctas_target USING heap AS SELECT g AS i, b09_ctas_seen(g) AS seen FROM generate_series(1,3) AS g;
SELECT * FROM b09_ctas_target ORDER BY i;
DROP TABLE b09_ctas_target;
DROP FUNCTION b09_ctas_seen(integer);

-- prepare: every EXECUTE parameter is planned before any is evaluated;
-- named-argument calls are accepted
CREATE SEQUENCE b09_prepare_seq;
PREPARE b09_p(bigint, integer) AS SELECT $1, $2;
EXECUTE b09_p(nextval('b09_prepare_seq'), 1/0);
SELECT is_called FROM b09_prepare_seq;
DEALLOCATE b09_p;
DROP SEQUENCE b09_prepare_seq;
PREPARE b09_p(date) AS SELECT $1;
EXECUTE b09_p(make_date(year => 2026, month => 9, day => 13));
EXPLAIN (COSTS OFF) EXECUTE b09_p(make_date(year => 2026, month => 9, day => 13));
DEALLOCATE b09_p;

-- publicationcmds: system columns in a row filter over a table with a
-- virtual generated column
CREATE TABLE b09_pubt (a int, g int GENERATED ALWAYS AS (a + 1) VIRTUAL);
CREATE PUBLICATION b09_pub FOR TABLE b09_pubt WHERE (ctid IS NOT NULL);
CREATE PUBLICATION b09_pub FOR TABLE b09_pubt WHERE (tableoid IS NOT NULL);
CREATE PUBLICATION b09_pub FOR TABLE b09_pubt WHERE (xmin IS NOT NULL);
CREATE PUBLICATION b09_pub FOR TABLE b09_pubt WHERE (g > 1);
DROP PUBLICATION b09_pub;
DROP TABLE b09_pubt;

-- trigger: overlong lookup names; pg_trigger_depth() inside RI cascades
CREATE TABLE b09_trig(i int);
SELECT pg_get_object_address('trigger', ARRAY['public','b09_trig',repeat('x',64)], ARRAY[]::text[]);
SELECT pg_get_object_address('trigger', ARRAY['public','b09_trig',repeat('x',63)], ARRAY[]::text[]);
SELECT pg_get_object_address('trigger', ARRAY['public','b09_trig',repeat('x',200)], ARRAY[]::text[]);
DROP TABLE b09_trig;
CREATE TABLE b09_p(id int PRIMARY KEY);
CREATE TABLE b09_c(id int REFERENCES b09_p ON DELETE CASCADE);
CREATE TABLE b09_depths(d int);
CREATE FUNCTION b09_record_depth() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO b09_depths VALUES (pg_trigger_depth()); RETURN OLD; END$$;
CREATE TRIGGER b09_record_depth BEFORE DELETE ON b09_c FOR EACH ROW EXECUTE FUNCTION b09_record_depth();
INSERT INTO b09_p VALUES (1);
INSERT INTO b09_c VALUES (1);
DELETE FROM b09_p;
SELECT * FROM b09_depths;
DROP TABLE b09_c;
DROP TABLE b09_p;
DROP TABLE b09_depths;
DROP FUNCTION b09_record_depth();

-- collationcmds: REFRESH VERSION detoasts a compressed collversion
CREATE COLLATION b09_longver (PROVIDER=builtin, LOCALE='C', VERSION='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
SELECT length(collversion) FROM pg_collation WHERE collname='b09_longver';
ALTER COLLATION b09_longver REFRESH VERSION;
SELECT collversion FROM pg_collation WHERE collname='b09_longver';
DROP COLLATION b09_longver;

-- tablespace / dbcommands: a 64-byte name never matches a 63-byte catalog name
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa LOCATION '';
SELECT (pg_get_object_address('tablespace', ARRAY[repeat('a',64)], ARRAY[]::text[])).classid;
SELECT (pg_get_object_address('tablespace', ARRAY[repeat('a',63)], ARRAY[]::text[])).classid;
DROP TABLESPACE aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;
RESET allow_in_place_tablespaces;
CREATE DATABASE aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;
CREATE DATABASE b09_child TEMPLATE 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
CREATE DATABASE b09_child TEMPLATE 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
DROP DATABASE b09_child;
DROP DATABASE aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;

-- explain: worker numbers are local to each Gather subtree
CREATE FUNCTION b09_explain(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE ln text;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || q LOOP
    RETURN NEXT regexp_replace(ln, 'Sort Method: .*$', 'Sort Method: N');
  END LOOP;
END $$;
CREATE TABLE b09_ea AS SELECT g AS v FROM generate_series(1,200000) g;
CREATE TABLE b09_eb AS SELECT g AS v FROM generate_series(1,200000) g;
ANALYZE b09_ea;
ANALYZE b09_eb;
SET max_parallel_workers_per_gather = 2;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_parallel_append = off;
SET work_mem = '64MB';
SELECT b09_explain('SELECT sum(v) FROM (SELECT v FROM b09_ea ORDER BY v) x UNION ALL SELECT sum(v) FROM (SELECT v FROM b09_eb ORDER BY v) y');
RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET enable_parallel_append;
RESET work_mem;
DROP TABLE b09_ea;
DROP TABLE b09_eb;
DROP FUNCTION b09_explain(text);

-- commands_async: an undeliverable notification is FATAL (ExitOnAnyError)
LISTEN b09_c;
SET client_encoding = 'LATIN1';
SELECT pg_notify('b09_c', U&'\20AC');
SELECT 'not reached';
