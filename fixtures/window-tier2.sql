-- window tier-2 differential fixture: value window functions + explicit
-- ROWS/RANGE frames, ties and NULLs included. Diffed against real C 18.3.
\set VERBOSITY verbose
CREATE TABLE wt2 (a int, b int, c int);
INSERT INTO wt2 VALUES
  (1, 10, 1), (2, 10, 2), (3, 20, 1), (4, 20, 2), (5, 20, 1),
  (6, 30, NULL), (NULL, 30, 2), (8, NULL, 1), (9, NULL, 2), (10, 40, 1);
-- lag/lead family over ties and NULL sort keys
SELECT a, b, lag(a) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, lead(a) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, lag(a, 2) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, lead(a, 2, -1) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, lag(a, 1, 0) OVER (PARTITION BY c ORDER BY b, a) FROM wt2;
SELECT a, b, lag(a, NULL) OVER (ORDER BY b, a) FROM wt2;
-- first/last/nth over default and explicit frames
SELECT a, b, first_value(a) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, last_value(a) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, last_value(a) OVER (ORDER BY b, a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM wt2;
SELECT a, b, first_value(a) OVER (ORDER BY b, a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM wt2;
SELECT a, b, nth_value(a, 2) OVER (ORDER BY b, a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM wt2;
SELECT a, b, nth_value(a, 3) OVER (ORDER BY b, a) FROM wt2;
-- ntile / percent_rank / cume_dist with ties and NULLs
SELECT a, b, ntile(4) OVER (ORDER BY b, a) FROM wt2;
SELECT a, b, ntile(3) OVER (PARTITION BY c ORDER BY b) FROM wt2;
SELECT a, b, percent_rank() OVER (ORDER BY b) FROM wt2;
SELECT a, b, cume_dist() OVER (ORDER BY b) FROM wt2;
SELECT a, b, percent_rank() OVER (PARTITION BY c ORDER BY b) FROM wt2;
SELECT a, b, cume_dist() OVER (PARTITION BY c ORDER BY b) FROM wt2;
-- sliding-window aggregates: moving sum/count with inverse transitions
SELECT a, b, sum(a) OVER (ORDER BY b, a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b, a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM wt2;
SELECT a, b, count(*) OVER (ORDER BY b, a ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b, a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b, a ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM wt2;
SELECT a, b, sum(a) OVER (PARTITION BY c ORDER BY b, a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wt2;
-- RANGE offset frames (int in_range machinery), ASC/DESC, NULLS FIRST/LAST
SELECT a, b, sum(a) OVER (ORDER BY b RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM wt2;
SELECT a, b, sum(a) OVER (ORDER BY b NULLS FIRST RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM wt2;
SELECT a, b, first_value(a) OVER (ORDER BY b RANGE BETWEEN 10 PRECEDING AND 0 FOLLOWING) FROM wt2;
SELECT a, b, last_value(a) OVER (ORDER BY b RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING) FROM wt2;
-- RANGE offset over a smallint-typed offset expression
SELECT a, b, sum(a) OVER (ORDER BY b RANGE BETWEEN 10::int8 PRECEDING AND CURRENT ROW) FROM wt2;
-- mixed functions sharing one window
SELECT a, b, rank() OVER w, lag(a) OVER w, sum(a) OVER (w ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wt2 WINDOW w AS (ORDER BY b, a);
-- errors: parse-time frame validation (42P20) and runtime arg errors
SELECT sum(a) OVER (ORDER BY b ROWS UNBOUNDED FOLLOWING) FROM wt2;
SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM wt2;
SELECT sum(a) OVER (ORDER BY b, a RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wt2;
SELECT ntile(0) OVER (ORDER BY b) FROM wt2;
SELECT nth_value(a, 0) OVER (ORDER BY b) FROM wt2;
SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wt2;
-- EXPLAIN: Window lines with frame text, costs
EXPLAIN SELECT sum(a) OVER (ORDER BY b, a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wt2;
EXPLAIN SELECT first_value(a) OVER (ORDER BY b RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM wt2;
EXPLAIN SELECT lag(a) OVER (PARTITION BY c ORDER BY b) FROM wt2;
EXPLAIN SELECT ntile(4) OVER (ORDER BY b), percent_rank() OVER (ORDER BY b), cume_dist() OVER (ORDER BY b) FROM wt2;
EXPLAIN SELECT sum(a) OVER (ORDER BY b, a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM wt2;
DROP TABLE wt2;
