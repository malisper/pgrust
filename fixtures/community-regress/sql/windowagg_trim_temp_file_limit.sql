-- nodeWindowAgg.c:2385: rows behind every mark and boundary read pointer are
-- trimmed from the window tuplestore, so a streaming window over a large
-- partition never spills what C keeps in memory.
CREATE TABLE wf_trim AS SELECT i FROM generate_series(1, 100000) g(i);
SET work_mem = '64kB';
SET temp_file_limit = 0;
SELECT count(*), max(r), max(l) FROM (
  SELECT row_number() OVER w AS r, lead(i::text || '', 0) OVER w AS l
  FROM wf_trim WINDOW w AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) s;
SELECT count(*), sum(s) FROM (
  SELECT sum(i) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
  FROM wf_trim) t;
SELECT count(*), sum(c) FROM (
  SELECT count(*) OVER (ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS c
  FROM wf_trim) u;
RESET temp_file_limit;
RESET work_mem;
DROP TABLE wf_trim;
