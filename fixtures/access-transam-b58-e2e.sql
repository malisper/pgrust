-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-58-backend-access-transam (C 18.6 oracle vs pgrust). Each leg is
-- one row of the batch (access/transam and its callees).
\set VERBOSITY verbose
CREATE DATABASE b58e2e TEMPLATE template0 ENCODING 'UTF8';
\c b58e2e
\set VERBOSITY verbose
-- fp-transam-slru#1 / fp-transam-twophase#3: ShmemIndex sizes carry C's layouts
SELECT name, size FROM pg_shmem_allocations WHERE name IN ('multixact_offset', 'multixact_member', 'subtransaction', 'transaction', 'notify', 'Prepared Transaction Table') ORDER BY name;
-- fp-transam-multixact-p2#2: SRF_FIRSTCALL_INIT precedes the member read
CREATE FUNCTION b58_mx_scalar(xid) RETURNS record LANGUAGE internal STRICT AS 'pg_get_multixact_members';
CREATE FUNCTION b58_mx_members_alias(xid) RETURNS SETOF record LANGUAGE internal STRICT AS 'pg_get_multixact_members';
SELECT b58_mx_scalar('1'::xid);
-- fp-transam-multixact-p2#1: the result descriptor comes from the call
CREATE TABLE b58_mxt(i int);
INSERT INTO b58_mxt VALUES (1);
BEGIN;
SELECT i FROM b58_mxt WHERE i = 1 FOR SHARE;
SAVEPOINT a;
SELECT i FROM b58_mxt WHERE i = 1 FOR UPDATE;
COMMIT;
SELECT mode FROM b58_mxt, LATERAL pg_get_multixact_members(xmax) m ORDER BY 1;
SELECT b58_mx_members_alias(xmax) FROM b58_mxt;
SELECT b58_mx_scalar(xmax) FROM b58_mxt;
SELECT a - a AS zero, b FROM b58_mxt, LATERAL b58_mx_members_alias(xmax) AS (a int8, b text) ORDER BY 2;
SELECT count(a) FROM b58_mxt, LATERAL b58_mx_members_alias(xmax) AS (a xid);
-- fp-transam-multixact-p1#2: the MultiXact cache context dies with the transaction
BEGIN;
SELECT count(*) FROM b58_mxt, LATERAL pg_get_multixact_members(xmax) m;
SELECT name FROM pg_backend_memory_contexts WHERE name = 'MultiXact cache context';
COMMIT;
SELECT name FROM pg_backend_memory_contexts WHERE name = 'MultiXact cache context';
DROP TABLE b58_mxt;
DROP FUNCTION b58_mx_members_alias(xid);
DROP FUNCTION b58_mx_scalar(xid);
