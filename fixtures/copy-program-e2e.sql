-- COPY TO/FROM PROGRAM + file_fdw PROGRAM differential corpus (issue #76
-- close-out). Run via scripts/copy-program-e2e.sh (regress-diff.sh two-binary
-- mode against C 18.3): loads, exit-code error surfaces (38000), the COPY TO
-- PROGRAM EPIPE lane, and file_fdw program scans incl. the early-termination
-- SIGPIPE tolerance. TO PROGRAM content is verified in-band with exit-code
-- assertions (grep/wc in the child) so no per-server temp paths leak into
-- the diffable output.
\set VERBOSITY verbose

CREATE TABLE prog_t (a int, b text);

-- FROM PROGRAM: plain text rows from printf.
COPY prog_t FROM PROGRAM 'printf ''1\talpha\n2\tbeta\n''';
SELECT * FROM prog_t ORDER BY a;

-- FROM PROGRAM under CSV options.
COPY prog_t FROM PROGRAM 'printf ''3,gamma\n4,delta\n''' WITH (FORMAT csv);
SELECT count(*) FROM prog_t;

-- A load big enough to cross the 64KB refill boundary repeatedly.
CREATE TABLE prog_seq (n int);
COPY prog_seq FROM PROGRAM 'seq 1 100000';
SELECT count(*), sum(n) FROM prog_seq;

-- Child exit codes surface as external_routine_exception (38000).
COPY prog_t FROM PROGRAM 'exit 3';
-- Shell 127: command not found.
COPY prog_t FROM PROGRAM 'no_such_command_pgrust_copyprog';
-- Shell 126: command not executable.
COPY prog_t FROM PROGRAM '/etc/hosts';

-- Data error mid-stream (child exited clean; COPY aborts on its own error).
COPY prog_t FROM PROGRAM 'printf ''x\tbad\n''';

-- TO PROGRAM: content asserted by the child (mismatch = visible exit 9).
COPY (SELECT 42 AS x) TO PROGRAM 'grep -q ''^42$'' || exit 9';
COPY prog_seq TO PROGRAM 'test "$(wc -l)" -eq 100000 || exit 9';

-- TO PROGRAM failing child: nonzero exit at close.
COPY (SELECT 1) TO PROGRAM 'exit 5';

-- TO PROGRAM child that stops reading early: 100k rows overflow the pipe
-- buffer after head exits, so the write fails with EPIPE (an error for COPY
-- TO PROGRAM even though the child exited 0).
COPY prog_seq TO PROGRAM 'head -c 16 >/dev/null';

-- file_fdw PROGRAM sources.
CREATE EXTENSION file_fdw;
CREATE SERVER prog_srv FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE prog_ft (line text) SERVER prog_srv
  OPTIONS (program 'printf ''hello\nworld\n''', format 'text');
SELECT * FROM prog_ft;
EXPLAIN (COSTS OFF) SELECT * FROM prog_ft;

-- Rescan re-runs the program (nested loop over VALUES).
SELECT v.i, p.line FROM (VALUES (1),(2)) v(i) CROSS JOIN prog_ft p
ORDER BY v.i, p.line;

-- Early termination: LIMIT ends the scan before the program reaches EOF;
-- the child dying of SIGPIPE is NOT an error (ClosePipeFromProgram).
CREATE FOREIGN TABLE prog_ft_big (n int) SERVER prog_srv
  OPTIONS (program 'seq 1 1000000', format 'text');
SELECT * FROM prog_ft_big LIMIT 3;

-- A failing program surfaces through the scan.
CREATE FOREIGN TABLE prog_ft_fail (n int) SERVER prog_srv
  OPTIONS (program 'seq 1 5; exit 3', format 'text');
SELECT count(*) FROM prog_ft_fail;

-- ANALYZE on a program source: no file size, quietly skipped.
ANALYZE prog_ft;

DROP FOREIGN TABLE prog_ft_fail;
DROP FOREIGN TABLE prog_ft_big;
DROP FOREIGN TABLE prog_ft;
DROP SERVER prog_srv;
DROP EXTENSION file_fdw;
DROP TABLE prog_seq;
DROP TABLE prog_t;
