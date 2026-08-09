-- COPY FROM/TO PROGRAM (the malisper/pgrust#76 C-parity gap, PR #449
-- stopgap retired): the server popens the command and streams its
-- stdout/stdin as the COPY data pipe. Expected output frozen from real
-- C PostgreSQL 18.x.
CREATE TABLE copyprog_t (a int, b text);
COPY copyprog_t FROM PROGRAM 'printf ''1\thello\n2\tworld\n''';
SELECT * FROM copyprog_t ORDER BY a;
-- round-trip through a program pipe pair
COPY copyprog_t TO PROGRAM 'cat > /tmp/pgrust_copyprog76.txt';
CREATE TABLE copyprog_rt (a int, b text);
COPY copyprog_rt FROM PROGRAM 'cat /tmp/pgrust_copyprog76.txt';
SELECT * FROM copyprog_rt ORDER BY a;
-- COPY (query) TO PROGRAM
COPY (SELECT a + 10, b FROM copyprog_t ORDER BY a) TO PROGRAM 'cat > /dev/null';
-- failing program: nonzero exit is reported with the child's status
COPY copyprog_rt FROM PROGRAM 'exit 3';
COPY copyprog_t TO PROGRAM 'exit 3';
-- a program that keeps writing after the \. terminator dies of SIGPIPE;
-- a COPY FROM that stopped before EOF must tolerate that, not error
COPY copyprog_rt FROM PROGRAM 'printf ''3\tearly\n\\.\n''; yes ''4	late''';
SELECT * FROM copyprog_rt ORDER BY a;
-- privilege gate: superuser or pg_execute_server_program members only,
-- checked before any table permissions
CREATE ROLE copyprog_nsu LOGIN;
SET SESSION AUTHORIZATION copyprog_nsu;
COPY copyprog_t FROM PROGRAM 'printf ''9\tnope\n''';
COPY copyprog_t TO PROGRAM 'cat > /dev/null';
RESET SESSION AUTHORIZATION;
GRANT pg_execute_server_program TO copyprog_nsu;
GRANT ALL ON copyprog_t TO copyprog_nsu;
SET SESSION AUTHORIZATION copyprog_nsu;
COPY copyprog_t FROM PROGRAM 'printf ''5\tmember\n''';
RESET SESSION AUTHORIZATION;
SELECT * FROM copyprog_t ORDER BY a;
-- cleanup (the program consumes stdin first so the writer never sees EPIPE)
COPY (SELECT 1) TO PROGRAM 'cat > /dev/null; rm -f /tmp/pgrust_copyprog76.txt';
DROP TABLE copyprog_t, copyprog_rt;
DROP ROLE copyprog_nsu;
