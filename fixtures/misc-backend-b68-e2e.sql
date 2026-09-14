-- bugs/batch-68-misc-backend: SQL-reachable rows, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-adt-misc#1: pg_column_is_updatable computes col in an int16 AttrNumber,
-- so attnum >= 32761 wraps negative and bms_make_singleton raises XX000.
CREATE TABLE b68t(a int);
SELECT pg_column_is_updatable('b68t'::regclass, 32767::smallint, false);
SELECT pg_column_is_updatable('b68t'::regclass, 32761::smallint, false);
SELECT pg_column_is_updatable('b68t'::regclass, 32760::smallint, false);
SELECT pg_column_is_updatable('b68t'::regclass, 1::smallint, false);
SELECT pg_column_is_updatable('b68t'::regclass, 0::smallint, false);
-- fp-gentable-logic#1: error after the Language option (stemmer env is closed).
CREATE TEXT SEARCH DICTIONARY b68_leak_dict (TEMPLATE = snowball, LANGUAGE = english, invalid_option = 'x');
CREATE TEXT SEARCH DICTIONARY b68_leak_dict (TEMPLATE = snowball, LANGUAGE = english, LANGUAGE = french);
CREATE TEXT SEARCH DICTIONARY b68_ok_dict (TEMPLATE = snowball, LANGUAGE = english);
SELECT ts_lexize('b68_ok_dict', 'spaceships');
-- fp-mmgr-mcxt#3: ErrorContext exists under TopMemoryContext.
SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'ErrorContext';
SELECT name, level, path[1] = (SELECT path[1] FROM pg_backend_memory_contexts WHERE name = 'TopMemoryContext') AS under_top FROM pg_backend_memory_contexts WHERE name = 'ErrorContext';
-- fp-partitioning-partbounds-p2#2: satisfies_hash_partition uses relation_open,
-- so indexes and composite types reach the "not a hash partitioned table" error.
CREATE INDEX b68ti ON b68t(a);
SELECT satisfies_hash_partition('b68ti'::regclass, 1, 0, 1);
CREATE TYPE b68ct AS (a int);
SELECT satisfies_hash_partition('b68ct'::regclass, 1, 0, 1);
SELECT satisfies_hash_partition('b68t'::regclass, 1, 0, 1);
CREATE TABLE b68hp (a int) PARTITION BY HASH (a);
SELECT satisfies_hash_partition('b68hp'::regclass, 4, 0, 1::int);
DROP TABLE b68hp;
DROP TYPE b68ct;
DROP TABLE b68t;
DROP TEXT SEARCH DICTIONARY b68_ok_dict;
