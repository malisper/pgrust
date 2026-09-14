-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-59-backend-access-transam (C 18.6 oracle vs pgrust). Each leg is
-- one row of the batch (access/transam xlog / xloginsert / xlogprefetcher).
\set VERBOSITY verbose
CREATE DATABASE b59e2e TEMPLATE template0 ENCODING 'UTF8';
\c b59e2e
\set VERBOSITY verbose
-- fp-transam-xlog-p2#7 / fp-transam-xlogprefetcher#1: XLOGShmemInit and
-- XLogPrefetchShmemInit register their ShmemIndex rows (default wal_buffers)
SELECT name, size FROM pg_shmem_allocations WHERE name IN ('XLOG Ctl', 'Control File', 'XLogPrefetchStats') ORDER BY name;
-- fp-transam-xloginsert#1: InitXLogInsert's named context is listed
SELECT name, level FROM pg_backend_memory_contexts WHERE name = 'WAL record construction';
\c postgres
DROP DATABASE b59e2e;
