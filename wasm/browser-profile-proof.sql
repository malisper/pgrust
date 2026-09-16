-- browser-profile-proof.sql — what the `browser` feature profile KEEPS.
--
-- Run against the threaded wire lane on a module built with
-- PGRUST_WASM_FEATURES=browser:
--
--   node run-node-wire-threads.mjs --dispatch stdio-wire-threaded --fs broker \
--        --sql browser-profile-proof.sql
--
-- WHY `LOAD` AND NOT `CREATE EXTENSION`. The seeded image these lanes boot
-- (wasm/assets/vfs.img) carries a datadir and share/timezone and nothing else —
-- there is no share/extension in it at all, so `CREATE EXTENSION pg_trgm` fails
-- with `extension "pg_trgm" is not available` (0A000) on EVERY profile, the full
-- one included, and proves nothing about the feature gates. `LOAD 'name'` goes
-- straight to the thing the gates actually move: dfmgr's named-builtin-library
-- lookup, which a contrib crate registers from its `init_seams()` and which is
-- simply not there when the crate is not linked. It is the same lookup
-- `CREATE EXTENSION` would reach after reading a control file, minus the control
-- file. The complementary half is browser-profile-refusal.sql.
--
-- What every statement is here to prove, in order:
--   1. the lane is alive at all;
--   2-4. pg_trgm, pgcrypto and pgvector (which registers under its EXTENSION
--      name, `vector`) are in the keep set: their libraries resolve;
--   5-7. a small jsonb table and a GIN index over it — `index-gin` is a group
--      feature and it is ON in this profile, so putting the access methods
--      behind features did not cost the browser an index method;
--   8. the index answers a containment query.
--
-- One statement per line; '--' comment lines are skipped by the runner.
SELECT 1
LOAD 'pg_trgm'
LOAD 'pgcrypto'
LOAD 'vector'
CREATE TABLE jb(id int, doc jsonb)
INSERT INTO jb SELECT g, jsonb_build_object('k', g) FROM generate_series(1,50) g
CREATE INDEX jb_gin ON jb USING gin (doc)
SELECT count(*) FROM jb WHERE doc @> '{"k": 7}'
