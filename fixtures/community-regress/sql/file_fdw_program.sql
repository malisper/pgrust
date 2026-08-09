-- Issue malisper/pgrust#76: file_fdw "program" foreign tables — DDL is
-- accepted, and scanning fails with a clean feature_not_supported error.
-- C-PARITY GAP (deliberate, pinned): real PostgreSQL runs the program via
-- COPY FROM PROGRAM (OpenPipeStream); pgrust has no PROGRAM pipe anywhere
-- (commands/copy rejects is_program the same way), so this expected file is
-- pgrust's clean error, NOT real-PG output. Re-freeze from C when PROGRAM
-- pipes are ported.
CREATE EXTENSION file_fdw;
CREATE SERVER issue76_srv FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE issue76_ft(line text) SERVER issue76_srv
  OPTIONS (program 'printf "hello\n"', format 'text');
SELECT * FROM issue76_ft;
DROP FOREIGN TABLE issue76_ft;
DROP SERVER issue76_srv;
DROP EXTENSION file_fdw;
