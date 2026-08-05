-- expr-gaps batch differential battery: aggregate FILTER, whole-row Vars,
-- record_out, row comparisons (=/<>), bit-string literals, set-op sources.
\set VERBOSITY verbose
-- aggregate FILTER
CREATE TABLE eg_t (a int, b boolean, c text);
INSERT INTO eg_t VALUES (1, true, 'x'), (2, false, 'y'), (3, true, NULL), (NULL, NULL, 'z');
SELECT count(*) FILTER (WHERE b) FROM eg_t;
SELECT count(*) FILTER (WHERE a > 1), sum(a) FILTER (WHERE b), count(c) FILTER (WHERE a IS NOT NULL) FROM eg_t;
SELECT count(*) FILTER (WHERE NULL::boolean) FROM eg_t;
SELECT b, count(*) FILTER (WHERE a > 1) FROM eg_t GROUP BY b ORDER BY b;
SELECT count(*) FILTER (WHERE a > 0) - count(*) FILTER (WHERE a > 2) FROM eg_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FILTER (WHERE b) FROM eg_t;
SELECT max(a) FILTER (WHERE b) FROM eg_t;
SELECT length('x') FILTER (WHERE true);
SELECT 1 + 2 FILTER (WHERE true);
-- whole-row Vars + record_out
SELECT t FROM eg_t t ORDER BY t.a;
SELECT t.* FROM eg_t t WHERE t.a = 1;
SELECT eg_t FROM eg_t ORDER BY a;
SELECT count(t) FROM eg_t t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT t FROM eg_t t;
-- quoting arms of record_out
CREATE TABLE eg_q (s text, u text);
INSERT INTO eg_q VALUES ('plain', 'with space'), ('a,b', 'q"uote'), ('back\slash', '(paren)'), ('', NULL);
SELECT q FROM eg_q q;
-- jsonb composite gate (jsonb tier2 documented gate: to_jsonb over whole-row)
SELECT to_jsonb(t.*) FROM eg_t t ORDER BY t.a;
SELECT to_jsonb(t) FROM eg_t t WHERE t.a = 2;
-- row comparisons
SELECT ROW(1,2) = ROW(1,2), ROW(1,2) = ROW(1,3), (1,2) <> (1,3);
SELECT ROW(1,'a') = ROW(1,'a'), ROW(NULL,2) = ROW(1,2);
SELECT (a, b) = (1, true) FROM eg_t ORDER BY a;
SELECT ROW(1) = ROW(2);
SELECT (1,2) IN ((1,2), (3,4)), (1,5) IN ((1,2), (3,4));
SELECT (1,2) = (1,2,3);
-- bit-string literals
SELECT B'1010';
SELECT B'0', B'1', B'00000000011111';
SELECT X'1F', X'ff', X'0';
SELECT B'';
SELECT B'102';
SELECT X'1G';
-- set-op sources under FROM and INSERT
SELECT * FROM (SELECT 1 AS x UNION SELECT 2) ss ORDER BY x;
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 2) ss ORDER BY x;
CREATE TABLE eg_u (v int);
INSERT INTO eg_u SELECT 1 UNION SELECT 2;
INSERT INTO eg_u SELECT 3 UNION ALL SELECT 3;
SELECT v FROM eg_u ORDER BY v;
SELECT count(*) FROM (SELECT a FROM eg_t INTERSECT SELECT v FROM eg_u) ss;
DROP TABLE eg_t;
DROP TABLE eg_q;
DROP TABLE eg_u;
-- nested subquery pull-up (recursive pull_up_subqueries; the empty-FROM
-- inner leg stays out: replace_empty_jointree is a named loud)
CREATE TABLE eg_n (v int);
INSERT INTO eg_n VALUES (1), (2);
SELECT * FROM (SELECT x FROM (SELECT v AS x FROM eg_n) y) z ORDER BY x;
SELECT * FROM (SELECT x + 1 AS w FROM (SELECT v AS x FROM eg_n UNION SELECT 5) y) z ORDER BY w;
DROP TABLE eg_n;
