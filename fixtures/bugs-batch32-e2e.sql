-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-32-support-types (C 18.6 oracle vs pgrust). Each leg is one row of
-- the batch (_support/types: fmgr, stringinfo, rel).
\set VERBOSITY verbose
CREATE DATABASE b32e2e TEMPLATE template0 ENCODING 'UTF8';
\c b32e2e
\set VERBOSITY verbose
-- fp-fmgr-fmgr#1: SendFunctionCall detoasts (DatumGetByteaP) a compressed or
-- short-header bytea handed back by a SQL-language send function.
CREATE TABLE b32_big(k int, payload bytea);
INSERT INTO b32_big VALUES (1, decode(repeat('00', 200000), 'hex'));
INSERT INTO b32_big VALUES (2, '\x0102030405'::bytea);
CREATE TYPE b32_myt;
CREATE FUNCTION b32_myt_in(cstring) RETURNS b32_myt AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION b32_myt_out(b32_myt) RETURNS cstring AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE b32_myt (INPUT = b32_myt_in, OUTPUT = b32_myt_out, INTERNALLENGTH = 4, PASSEDBYVALUE);
CREATE FUNCTION b32_myt_send(b32_myt) RETURNS bytea LANGUAGE sql
  AS 'SELECT payload FROM b32_big WHERE k = $1::text::int';
ALTER TYPE b32_myt SET (SEND = b32_myt_send);
SELECT length(array_send(ARRAY['1'::b32_myt])), md5(array_send(ARRAY['1'::b32_myt]));
SELECT length(array_send(ARRAY['2'::b32_myt])), encode(array_send(ARRAY['2'::b32_myt]), 'hex');
COPY (SELECT '1'::b32_myt, '2'::b32_myt) TO '/dev/null' WITH (FORMAT binary);
DROP TYPE b32_myt CASCADE;
DROP TABLE b32_big;
