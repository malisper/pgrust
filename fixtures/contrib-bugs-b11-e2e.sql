-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-11-misc-contrib (C 18.6 oracle vs pgrust). Each leg is one row of the
-- batch; the ISO8859-1 leg is last because a locale the host lacks aborts psql.
\set VERBOSITY verbose
CREATE DATABASE b11e2e TEMPLATE template0 ENCODING 'UTF8';
CREATE DATABASE b11e2e_ascii TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C';
CREATE DATABASE b11e2e_tr TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'tr' LC_COLLATE 'C' LC_CTYPE 'C';
\c b11e2e
\set VERBOSITY verbose
-- fp-contrib-hstore-hstore_io#1: hstoreUniquePairs is pg_qsort + first-of-run
CREATE EXTENSION hstore;
SELECT 'b=>0,a=>1,a=>2,a=>3,a=>4,a=>5,a=>6'::hstore -> 'a' AS qsort_first_of_run;
SELECT 'a=>1,a=>2,a=>3,a=>4,a=>5,a=>6,a=>7,a=>8,a=>9,b=>0,a=>x'::hstore AS qsort_eleven;
SELECT hstore(ARRAY['a','a','a','a','a','a','a'], ARRAY['1','2','3','4','5','6','7']) AS qsort_arrays;
-- fp-contrib-hstore-hstore_io#2: hstore_send converts to client_encoding
SET client_encoding = 'LATIN1';
SELECT encode(hstore_send(hstore('k', chr(233))), 'hex') AS send_latin1;
SELECT encode(hstore_send(hstore(chr(233), NULL)), 'hex') AS send_latin1_key;
RESET client_encoding;
-- fp-misc-guc-p3#1: passwordcheck.min_password_length is an int GUC with GUC_UNIT_BYTE
LOAD 'passwordcheck';
SHOW passwordcheck.min_password_length;
SET passwordcheck.min_password_length = '1kB';
SHOW passwordcheck.min_password_length;
CREATE ROLE b11_pc PASSWORD 'Aa123456';
SET passwordcheck.min_password_length = 'abc';
SET passwordcheck.min_password_length = '-1';
SET passwordcheck.min_password_length = 4;
CREATE ROLE b11_pc PASSWORD 'Aa123456';
DROP ROLE b11_pc;
RESET passwordcheck.min_password_length;
SHOW passwordcheck.min_password_length;
-- fp-contrib-dblink-dblink-p2#1: nested registry read from a domain CHECK
CREATE EXTENSION dblink;
SELECT 'host=' || current_setting('unix_socket_directories') || ' port=' || current_setting('port') || ' dbname=b11e2e user=postgres' AS conn \gset
SELECT dblink_connect('n', :'conn');
CREATE DOMAIN b11_checked_int AS integer CHECK (dblink_get_connections() IS NOT NULL);
SELECT * FROM dblink('n', 'SELECT 1') AS t(v b11_checked_int);
-- fp-contrib-dblink-dblink-p1#1: local GUCs restored after rows-then-command
SELECT dblink_exec('n', 'SET DateStyle = ''SQL, DMY''');
SET DateStyle = 'ISO, MDY';
BEGIN;
SELECT * FROM dblink('n', 'SELECT 1; SET application_name = test') AS t(x text);
SHOW DateStyle;
SELECT * FROM dblink('n', 'SELECT 1; SELECT 2') AS t(x text);
SHOW DateStyle;
ROLLBACK;
SELECT dblink_disconnect('n');
-- fp-contrib-tablefunc-tablefunc#1: output domain whose CHECK re-enters SPI
CREATE EXTENSION tablefunc;
CREATE FUNCTION b11_tf_check(integer) RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN RETURN true; END $$;
CREATE DOMAIN b11_tf_domain AS integer CHECK (b11_tf_check(VALUE));
SELECT * FROM crosstab('SELECT 1, 1, 7', 'SELECT 1') AS t(r integer, v b11_tf_domain);
SELECT * FROM crosstab('SELECT 1, 1, 7 UNION ALL SELECT 2, 1, 8 ORDER BY 1') AS t(r integer, v b11_tf_domain);
SELECT * FROM connectby('(SELECT 1 AS k, NULL::int AS p) src', 'k', 'p', '1', 0) AS t(k b11_tf_domain, p integer, l integer);
-- fp-contrib-pg_visibility-pg_visibility#1: fixed descriptor regardless of the wrapper
CREATE TABLE b11_vt(i int);
INSERT INTO b11_vt VALUES (1);
CREATE FUNCTION b11_vm_bad(regclass, bigint, OUT a boolean, OUT b boolean, OUT c boolean) RETURNS record AS '$libdir/pg_visibility', 'pg_visibility_map' LANGUAGE C STRICT;
SELECT b11_vm_bad('b11_vt'::regclass, 0)::text;
CREATE FUNCTION b11_v_bad(regclass, bigint, OUT a boolean, OUT b boolean, OUT c boolean, OUT d boolean) RETURNS record AS '$libdir/pg_visibility', 'pg_visibility' LANGUAGE C STRICT;
SELECT b11_v_bad('b11_vt'::regclass, 0)::text;
-- fp-contrib-pg_surgery-heap_surgery#1: relam must be the heap AM oid
CREATE EXTENSION pg_surgery;
CREATE ACCESS METHOD b11_heap_alias TYPE TABLE HANDLER heap_tableam_handler;
CREATE TABLE b11_surgery(i int) USING b11_heap_alias;
INSERT INTO b11_surgery VALUES (1);
SELECT heap_force_kill('b11_surgery', ARRAY['(0,1)'::tid]);
SELECT heap_force_freeze('b11_surgery', ARRAY['(0,1)'::tid]);
SELECT * FROM b11_surgery;
-- fp-contrib-cube-cube#1: GiST consistent/distance detoast the query
CREATE EXTENSION cube;
ALTER TYPE cube SET (STORAGE = extended);
CREATE TABLE b11_cq(c cube) WITH (toast_tuple_target = 128);
INSERT INTO b11_cq SELECT cube(array_agg(x::float8)) FROM generate_series(1, 100) x;
SELECT pg_column_compression(c) FROM b11_cq;
CREATE TABLE b11_ct(c cube);
INSERT INTO b11_ct SELECT c FROM b11_cq;
INSERT INTO b11_ct SELECT cube(array_agg(x::float8)) FROM generate_series(2, 101) x;
CREATE INDEX b11_ct_idx ON b11_ct USING gist(c);
SET enable_seqscan = off;
SELECT count(*) FROM b11_ct WHERE c @> (SELECT c FROM b11_cq);
SELECT count(*) FROM b11_ct WHERE c && (SELECT c FROM b11_cq);
SELECT count(*) FROM b11_ct WHERE c <@ (SELECT c FROM b11_cq);
SELECT c <-> (SELECT c FROM b11_cq) FROM b11_ct ORDER BY 1 LIMIT 1;
RESET enable_seqscan;
ALTER TYPE cube SET (STORAGE = plain);
-- fp-contrib-ltree-b1#1: '@' folding follows the default collation's ctype (ICU)
\c b11e2e_tr
\set VERBOSITY verbose
CREATE EXTENSION ltree;
SELECT 'I'::ltree ~ 'i@'::lquery AS lq, 'I'::ltree @ 'i@'::ltxtquery AS ltq;
SELECT 'i'::ltree ~ 'I@'::lquery AS lq2, 'a.I'::ltree ~ '*.i@'::lquery AS lq3, 'Abc'::ltree ~ 'aBC@'::lquery AS lq4;
SELECT ARRAY['I'::ltree] ~ 'i@'::lquery AS alq, ARRAY['I'::ltree] @ 'i@'::ltxtquery AS altq;
-- SQL_ASCII legs: pageinspect / dblink output-function bytes preserved
\c b11e2e_ascii
\set VERBOSITY verbose
CREATE EXTENSION dblink;
CREATE TABLE b11_t(k text);
SELECT encode(convert_to(dblink_build_sql_delete('b11_t', '1'::int2vector, 1, ARRAY[convert_from(decode('ff','hex'),'SQL_ASCII')]),'SQL_ASCII'),'hex') AS build_delete;
SELECT encode(convert_to(dblink_build_sql_insert('b11_t', '1'::int2vector, 1, ARRAY[convert_from(decode('ff','hex'),'SQL_ASCII')], ARRAY[convert_from(decode('fe27','hex'),'SQL_ASCII')]),'SQL_ASCII'),'hex') AS build_insert;
CREATE EXTENSION pageinspect;
CREATE TABLE b11_bt(v text);
INSERT INTO b11_bt VALUES (convert_from(decode('ff','hex'),'SQL_ASCII'));
CREATE INDEX b11_bt_idx ON b11_bt USING brin(v);
SELECT encode(convert_to(value,'SQL_ASCII'),'hex') AS brin_value FROM brin_page_items(get_raw_page('b11_bt_idx',2),'b11_bt_idx'::regclass);
CREATE TABLE b11_gt(p point, s text);
INSERT INTO b11_gt VALUES (point(1,2), convert_from(decode('ff','hex'),'SQL_ASCII'));
CREATE INDEX b11_gt_idx ON b11_gt USING gist(p) INCLUDE (s);
SELECT itemoffset, encode(convert_to(keys,'SQL_ASCII'),'hex') AS gist_keys FROM gist_page_items(get_raw_page('b11_gt_idx',0),'b11_gt_idx'::regclass);
-- fp-contrib-pageinspect-brinfuncs#1: lp_len is not the decoder's bound
CREATE TABLE b11_bl(i int);
INSERT INTO b11_bl SELECT g FROM generate_series(1,1000) g;
CREATE INDEX b11_bl_idx ON b11_bl USING brin(i);
WITH p AS (SELECT get_raw_page('b11_bl_idx', 2) AS pg),
 w AS (SELECT pg, get_byte(pg,24) | (get_byte(pg,25)<<8) | (get_byte(pg,26)<<16) | (get_byte(pg,27)<<24) AS lp FROM p),
 m AS (SELECT pg, ((lp & 131071) | (1<<17)) AS nlp FROM w),
 s AS (SELECT set_byte(set_byte(set_byte(set_byte(pg,24,nlp&255),25,(nlp>>8)&255),26,(nlp>>16)&255),27,(nlp>>24)&255) AS pg2 FROM m)
SELECT itemoffset, blknum, attnum, allnulls, hasnulls, placeholder, empty, value FROM s, LATERAL brin_page_items(pg2, 'b11_bl_idx'::regclass) WHERE itemoffset = 1;
-- fp-contrib-fuzzystrmatch-*: libc isalpha/toupper under the database LC_CTYPE
\c postgres
CREATE DATABASE b11e2e_l1 TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'en_US.ISO8859-1';
\c b11e2e_l1
\set VERBOSITY verbose
CREATE EXTENSION fuzzystrmatch;
SELECT encode(convert_to(dmetaphone(convert_from(decode('e7','hex'),'SQL_ASCII')),'SQL_ASCII'),'hex') AS dm, encode(convert_to(dmetaphone_alt(convert_from(decode('f1','hex'),'SQL_ASCII')),'SQL_ASCII'),'hex') AS dma;
SELECT encode(convert_to(soundex(convert_from(decode('e9','hex'),'SQL_ASCII')),'SQL_ASCII'),'hex') AS sx, difference(convert_from(decode('e9','hex'),'SQL_ASCII'),'') AS diff, metaphone(convert_from(decode('e961','hex'),'SQL_ASCII'),4) AS mp;
SELECT soundex('Anderson'), metaphone('Thompson', 10), dmetaphone('Schmidt'), dmetaphone_alt('Schmidt');
