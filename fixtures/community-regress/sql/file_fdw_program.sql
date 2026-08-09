-- Issue malisper/pgrust#76: file_fdw "program" foreign tables run the
-- command via the COPY FROM PROGRAM pipe (popen) and scan its stdout.
-- Expected output frozen from real C PostgreSQL 18.x (the pre-port
-- feature_not_supported pin is retired).
CREATE EXTENSION file_fdw;
CREATE SERVER issue76_srv FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE issue76_ft(line text) SERVER issue76_srv
  OPTIONS (program 'printf "hello\n"', format 'text');
SELECT * FROM issue76_ft;
SELECT * FROM issue76_ft;
-- multi-column, csv format
CREATE FOREIGN TABLE issue76_csv(a int, b text) SERVER issue76_srv
  OPTIONS (program 'printf "1,one\n2,two\n"', format 'csv');
SELECT * FROM issue76_csv ORDER BY a;
-- failing program surfaces the child's exit status
CREATE FOREIGN TABLE issue76_fail(line text) SERVER issue76_srv
  OPTIONS (program 'exit 7', format 'text');
SELECT * FROM issue76_fail;
-- only pg_execute_server_program members may set the program option
CREATE ROLE issue76_nsu LOGIN;
GRANT ALL ON FOREIGN SERVER issue76_srv TO issue76_nsu;
GRANT CREATE ON SCHEMA public TO issue76_nsu;
SET SESSION AUTHORIZATION issue76_nsu;
CREATE FOREIGN TABLE issue76_denied(line text) SERVER issue76_srv
  OPTIONS (program 'printf "nope\n"', format 'text');
RESET SESSION AUTHORIZATION;
DROP FOREIGN TABLE issue76_ft, issue76_csv, issue76_fail;
REVOKE ALL ON FOREIGN SERVER issue76_srv FROM issue76_nsu;
REVOKE CREATE ON SCHEMA public FROM issue76_nsu;
DROP ROLE issue76_nsu;
DROP SERVER issue76_srv;
DROP EXTENSION file_fdw;
