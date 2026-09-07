-- tablespace-inplace-proof.sql — an IN-PLACE tablespace over a memory-backed MOUNT that sits
-- INSIDE the seeded datadir.
--
-- Run against the threaded wire lane and the broker store, with the second store mounted at
-- pg_tblspc itself:
--
--   node run-node-wire-threads.mjs --dispatch stdio-wire-threaded --fs broker \
--        --mount /pgdata/pg_tblspc=memory --sql tablespace-inplace-proof.sql
--
-- How this differs from tablespace-proof.sql. There, LOCATION '/pgeph' names a directory OUTSIDE
-- the datadir and `pg_tblspc/<oid>` is a SYMLINK to it. Here `LOCATION ''` (which needs
-- allow_in_place_tablespaces) makes `pg_tblspc/<oid>` a REAL DIRECTORY and there is no link at all
-- — so nothing about this proof depends on symlinks, and everything about it depends on the mount
-- prefix lying inside the tree the coordinator seeds. That is the arrangement a browser-side
-- ephemeral tablespace actually wants: the datadir keeps its normal shape and one subtree of it is
-- served by a second, volatile store.
--
-- What every statement is here to prove, in order:
--   1. that MakePGDirectory("pg_tblspc/<oid>") lands in the MOUNT (its store, not the root's);
--   2. that pg_tablespace_location() reports the in-place path rather than a link target;
--   3. that relations open under it, so their filepath is pg_tblspc/<oid>/PG_18_.../<db>/<node>;
--   4. that a temp relation lands there too, via temp_tablespaces;
--   5. that pg_ls_dir('pg_tblspc') — a readdir of the mount's own root — lists the oid;
--   6. that a CHECKPOINT, on a different thread, can still reach every one of those files.
--
-- The storage coordinator's per-port file counts at stop are the other half of the proof: the
-- relation files must be in the MOUNT's store, and the root store must hold only the placeholder.
--
-- One statement per line; '--' comment lines are skipped by the runner.
SET allow_in_place_tablespaces = on
CREATE TABLESPACE eph LOCATION ''
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
