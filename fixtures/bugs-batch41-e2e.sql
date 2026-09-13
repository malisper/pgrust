-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-41-backend-utils-adt (C 18.6 oracle vs pgrust). Each leg is one row
-- of the batch (utils/adt: float, formatting, geo, domains, lockfuncs, enum).
\set VERBOSITY verbose
CREATE DATABASE b41e2e TEMPLATE template0 ENCODING 'UTF8';
\c b41e2e
\set VERBOSITY verbose
-- fp-adt-float-p1#2: strtod's nan(n-char-sequence) payload lands in the
-- mantissa; float8send/float4send expose the bits.
SELECT s, encode(float8send(s::float8), 'hex') AS f8, encode(float4send(s::float4), 'hex') AS f4
FROM unnest(ARRAY['NaN', 'NaN(1)', 'nan(0x10)', 'nan(07)', 'NaN(123456789)', '-NaN(2)',
                  'NaN(abc)', 'NaN(4294967295)', 'NaN(8388608)', 'NaN()']) AS s;
-- fp-adt-formatting-p3#2: numeric to_char rounds through numeric_round, which
-- clamps the requested scale to NUMERIC_DSCALE_MAX.
SELECT length(to_char(1::numeric, '9.' || repeat('9', 16384))), md5(to_char(1::numeric, '9.' || repeat('9', 16384)));
SELECT md5(to_char(-123.456::numeric, '999.' || repeat('9', 16384)));
-- fp-adt-formatting-p3#3: EEEE conversion precision is capped at 350 digits;
-- the rest is zero padding before the exponent.
SELECT md5(to_char(1e-300::double precision, '9.' || repeat('9', 400) || 'EEEE'));
SELECT md5(to_char(1e-30::real, '9.' || repeat('9', 400) || 'EEEE'));
SELECT md5(to_char(123456789, '9.' || repeat('9', 400) || 'EEEE'));
-- fp-adt-formatting-p2#3: the picture workspace is admitted at C's 16-byte
-- FormatNode size.
SELECT to_date('', repeat('"', 68000000));
-- fp-adt-geo_ops-p1#1 / fp-adt-geo_ops-p2#1: point-count overflow is a soft
-- error under pg_input_is_valid.
SELECT pg_input_is_valid(repeat(',', 268435455), 'path');
SELECT pg_input_is_valid(repeat(',', 268435455), 'polygon');
-- fp-adt-geo_ops-p2#2: the whole polygon (header included) is admitted
-- before any coordinate is decoded.
SELECT repeat(',', 134217723)::polygon;
-- fp-adt-domains#3: domain_in prepares the CHECK constraints (function ACL
-- included) before the base type's input function runs.
CREATE FUNCTION b41_domain_pred(integer) RETURNS boolean LANGUAGE plpgsql VOLATILE
  AS $$ BEGIN RETURN $1 > 0; END $$;
REVOKE EXECUTE ON FUNCTION b41_domain_pred(integer) FROM PUBLIC;
CREATE DOMAIN b41_denied_d AS integer CHECK (b41_domain_pred(VALUE));
CREATE ROLE b41_limited;
SET ROLE b41_limited;
SELECT pg_input_is_valid('bad', 'b41_denied_d');
SELECT pg_input_is_valid('1', 'b41_denied_d');
RESET ROLE;
DROP DOMAIN b41_denied_d;
DROP FUNCTION b41_domain_pred(integer);
DROP ROLE b41_limited;
-- fp-adt-lockfuncs#1: pg_lock_status is value-per-call, so a target-list
-- LIMIT stops early without a tuplestore; FROM still materializes.
SELECT count(*) FROM (SELECT pg_advisory_lock(g::bigint) FROM generate_series(1, 2000) g) s;
SET work_mem = '64kB';
SET temp_file_limit = 0;
SELECT count(*) FROM (SELECT pg_lock_status() LIMIT 1) s;
SELECT count(*) FROM pg_lock_status();
RESET work_mem;
RESET temp_file_limit;
SELECT pg_advisory_unlock_all();
-- fp-adt-enum#2: a label that is not valid in the database encoding never
-- matches (SQL_ASCII database, byte 0xff via convert_from).
\c postgres
CREATE DATABASE b41sa TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C';
\c b41sa
\set VERBOSITY verbose
CREATE TYPE b41_e AS ENUM ('');
SELECT convert_from('\xff'::bytea, 'SQL_ASCII')::b41_e;
SELECT ''::b41_e;
