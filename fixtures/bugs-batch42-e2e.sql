-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-42-backend-utils-adt (C 18.6 oracle vs pgrust). Each leg is one row
-- of the batch (utils/adt: mac8, misc, multirangetypes, pgstatfuncs).
\set VERBOSITY verbose
CREATE DATABASE b42e2e TEMPLATE template0 ENCODING 'UTF8';
\c b42e2e
\set VERBOSITY verbose
-- fp-adt-misc#5: pg_input_error_info checks the composite return type before
-- parsing the type name.
CREATE FUNCTION b42_bad_probe(text,text) RETURNS integer LANGUAGE internal STRICT AS 'pg_input_error_info';
SELECT b42_bad_probe('1','no_such_type_for_probe');
SELECT b42_bad_probe('1','int4');
-- fp-adt-multirangetypes#2: the niladic constructor rejects arguments.
CREATE FUNCTION b42_mr_extra(integer) RETURNS int4multirange AS 'multirange_constructor0' LANGUAGE internal;
SELECT b42_mr_extra(1);
-- fp-adt-multirangetypes#3: NULL member checks are elog(ERROR)s.
CREATE FUNCTION b42_mr_null(int4range) RETURNS int4multirange AS 'multirange_constructor1' LANGUAGE internal CALLED ON NULL INPUT;
SELECT b42_mr_null(NULL);
CREATE FUNCTION b42_mr_null2(int4range[]) RETURNS int4multirange AS 'multirange_constructor2' LANGUAGE internal CALLED ON NULL INPUT;
SELECT b42_mr_null2(NULL);
SELECT b42_mr_null2(ARRAY[int4range(1,2), NULL]);
-- fp-adt-pgstatfuncs#2: namestrcpy clips the slot name at 63 bytes.
SELECT octet_length(slot_name), slot_name FROM pg_stat_get_replication_slot(repeat('a', 62) || 'é');
SELECT octet_length(slot_name) FROM pg_stat_get_replication_slot(repeat('b', 70));
-- fp-adt-mac8#2: macaddr8_in reads raw bytes (a SQL_ASCII database can hand
-- over a non-UTF-8 trailing byte, which the pair loop leaves unread).
CREATE DATABASE b42e2e_ascii TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C';
\c b42e2e_ascii
\set VERBOSITY verbose
SELECT convert_from('\x303031313232333334343535ff'::bytea, 'SQL_ASCII')::macaddr8;
SELECT convert_from('\x30303131323233333434353566'::bytea, 'SQL_ASCII')::macaddr8;
SELECT convert_from('\x3030313132323333ff'::bytea, 'SQL_ASCII')::macaddr8;
