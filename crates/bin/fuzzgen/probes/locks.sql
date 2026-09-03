-- sitediff in-transaction pg_locks probe (plan §4.4): after lock-taking
-- DDL inside an open transaction, issued on the SAME session that took
-- the locks (pid = pg_backend_pid() binds it). Catalog-lock noise is
-- canonicalized away: relation locks on pg_catalog objects are dropped
-- (both engines hold them in engine-specific sets while a DDL is open).

-- name: relation_locks
SELECT l.relation::regclass::text AS relation, l.mode, l.granted
  FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE l.pid = pg_backend_pid()
   AND l.locktype = 'relation'
   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
 ORDER BY 1, 2, 3;

-- name: object_locks
SELECT l.locktype, l.mode, l.granted, l.objsubid
  FROM pg_locks l
 WHERE l.pid = pg_backend_pid()
   AND l.locktype IN ('object', 'advisory', 'userlock', 'tuple', 'page', 'extend', 'frozenid', 'spectoken', 'applytransaction')
 ORDER BY 1, 2, 3, 4;
