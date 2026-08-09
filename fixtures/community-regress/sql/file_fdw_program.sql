-- Issue malisper/pgrust#76: file_fdw "program" foreign tables run the
-- command via the COPY FROM PROGRAM pipe (OpenPipeStream lane). Expected
-- output re-frozen from real C PG 18 after the PROGRAM port landed.
CREATE EXTENSION file_fdw;
CREATE SERVER issue76_srv FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE issue76_ft(line text) SERVER issue76_srv
  OPTIONS (program 'printf "hello\n"', format 'text');
SELECT * FROM issue76_ft;
DROP FOREIGN TABLE issue76_ft;
DROP SERVER issue76_srv;
DROP EXTENSION file_fdw;
