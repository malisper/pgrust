-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-144-backend-libpq (C 18.6 oracle vs pgrust): scripts/backend-libpq-b144-e2e.sh.
\set VERBOSITY verbose
CREATE DATABASE b144e2e TEMPLATE template0 ENCODING 'UTF8';
\c b144e2e
\set VERBOSITY verbose
-- fp-libpq-be-fsstubs#3: lo_get_fragment_internal's loSize - offset must not
-- overflow (pgrust panicked "attempt to subtract with overflow").
SELECT lo_create(4242);
SELECT length(lo_get(4242, -9223372036854775808, 1));
SELECT length(lo_get(4242, -9223372036854775808, -1));
SELECT length(lo_get(4242, -9223372036854775807, 1));
SELECT length(lo_get(4242, -1, 1));
SELECT length(lo_get(4242, 0, 1));
SELECT lo_from_bytea(4243, '\xdeadbeef');
SELECT length(lo_get(4243, -9223372036854775808, 4));
SELECT lo_get(4243, -9223372036854775807, 4);
SELECT lo_get(4243, 2, 4);
SELECT lo_get(4243, 2, 2147483647);
SELECT lo_unlink(4242), lo_unlink(4243);
