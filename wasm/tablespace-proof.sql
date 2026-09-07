-- tablespace-proof.sql — CREATE TABLESPACE over a memory-backed MOUNT.
--
-- Run against the threaded wire lane and the broker store, with a second store
-- mounted at /pgeph:
--
--   node run-node-wire-threads.mjs --dispatch stdio-wire-threaded --fs broker \
--        --mount /pgeph=memory --sql tablespace-proof.sql
--
-- What every statement is here to prove, in order:
--   1. the symlink itself: CREATE TABLESPACE mkdirs <location>/PG_18_<catver>
--      and then symlink()s pg_tblspc/<oid> at the location;
--   2. that the link reads back: pg_tablespace_location() is a readlink();
--   3. that relations open THROUGH the link, so their filepath is under
--      pg_tblspc/<oid>/PG_18_.../<db>/<relfilenode>;
--   4. that a temp relation lands there too, via temp_tablespaces;
--   5. that pg_ls_dir sees the link in the directory listing;
--   6. that a CHECKPOINT — a different code path, and on a different thread
--      under the postmaster — can still reach every one of those files.
--
-- The storage coordinator's per-port file counts at stop are the other half of
-- the proof: the relation files must be in the MOUNT's store, not the root's.
--
-- One statement per line; '--' comment lines are skipped by the runner.
SELECT 1
CREATE TABLESPACE eph LOCATION '/pgeph'
SELECT spcname, pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='eph'
CREATE UNLOGGED TABLE e(id int primary key, v text) TABLESPACE eph
INSERT INTO e SELECT g, 'row-' || g FROM generate_series(1,1000) g
SELECT count(*), pg_relation_filepath('e') FROM e
SET temp_tablespaces = eph
CREATE TEMP TABLE tt(id int, v text)
INSERT INTO tt SELECT g, 'tmp-' || g FROM generate_series(1,20000) g
SELECT count(*), pg_relation_filepath('tt') FROM tt
SELECT pg_ls_dir('pg_tblspc')
CHECKPOINT
SELECT count(*) FROM e
