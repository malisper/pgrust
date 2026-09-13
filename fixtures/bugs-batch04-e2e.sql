-- Differential fixture for the 2026-09-13 workers-io bug batch 04
-- (planner internal errors, plpgsql resolution, output-function aliasing,
-- hash-join strict keys, ALTER CONSTRAINT, statement triggers, window
-- results). Run via scripts/bugs-batch04-e2e.sh against the frozen expected
-- captured from C PostgreSQL 18.6.
\set VERBOSITY verbose
\pset pager off

-- P004/P005: GROUPING SETS over a set-operation or LATERAL input
CREATE TABLE b04_a (k bigint PRIMARY KEY, v bigint);
CREATE TABLE b04_b (k bigint PRIMARY KEY, v bigint);
INSERT INTO b04_a SELECT g, g * 2 FROM generate_series(1, 20) g;
INSERT INTO b04_b SELECT g, g * 3 FROM generate_series(10, 30) g;
SELECT t.v % 3 AS g, GROUPING(t.v % 3) AS total, string_agg(t.k::text, ',' ORDER BY t.k)
FROM (SELECT k, v FROM b04_a UNION ALL SELECT k, v FROM b04_b) t
GROUP BY GROUPING SETS ((t.v % 3), ()) ORDER BY total, g NULLS LAST;
SELECT t.v % 3 AS g, GROUPING(t.v % 3) AS total, count(t.v)
FROM (SELECT k, v FROM b04_a UNION SELECT k, v FROM b04_b) t
WHERE t.k BETWEEN 7 AND 10
GROUP BY GROUPING SETS ((t.v % 3), ()) ORDER BY total, g NULLS LAST;
SELECT t.v % 3 AS g, GROUPING(t.v % 3) AS total, bool_and(t.v IS NULL)
FROM (SELECT k, v FROM b04_a INTERSECT SELECT k, v FROM b04_b) t
GROUP BY GROUPING SETS ((t.v % 3), ()) ORDER BY total, g NULLS LAST;
SELECT t.v % 3 AS g, GROUPING(t.v % 3) AS total, count(*) FILTER (WHERE t.v IS NULL)
FROM b04_a t, LATERAL (SELECT count(*) AS n FROM b04_b u WHERE u.k <= t.k) l
GROUP BY GROUPING SETS ((t.v % 3), ()) ORDER BY total, g NULLS LAST;

-- P009: min/max optimization with an OR ... EXISTS qual
CREATE TABLE b04_t (k bigint PRIMARY KEY, v bigint, c1 int8, c2 numeric);
CREATE TABLE b04_u (k bigint PRIMARY KEY, v bigint);
INSERT INTO b04_t SELECT g, g * 2, g, g / 10.0 FROM generate_series(1, 40) g;
INSERT INTO b04_u SELECT g, g * 3 FROM generate_series(5, 25) g;
SELECT min(t.k) FROM b04_t t WHERE t.k >= 8 OR t.c2 <> -693.93 AND EXISTS (SELECT 1 FROM b04_u u WHERE u.k = t.k);
SELECT max(t.k) FROM b04_t t WHERE t.k = 15 OR t.c2 <> 596.53 AND EXISTS (SELECT 1 FROM b04_u u WHERE u.k = t.k);
SELECT min(t.k) FROM b04_t t WHERE t.k <= 7 OR t.c1 <> 961682 AND EXISTS (SELECT 1 FROM b04_u u WHERE u.k = t.k);

-- P026: outer aggregate two levels down inside a pulled-up EXISTS
CREATE TABLE b04_w (k bigint, v bigint);
INSERT INTO b04_w VALUES (5, 40), (7, 3);
SELECT count(t.v) FROM b04_t t HAVING (SELECT count(*) FROM b04_u u WHERE EXISTS (SELECT 1 FROM b04_w w WHERE w.k = u.k AND w.v = max(t.v))) > 0;
SELECT count(t.v) FROM b04_t t HAVING (SELECT count(*) FROM b04_u u WHERE EXISTS (SELECT 1 FROM b04_w w WHERE w.k = u.k AND w.v = max(t.v) - 1)) > 0;

-- P020: ENFORCED that changes nothing still applies the deferrability words
CREATE TABLE b04_pk (k bigint PRIMARY KEY);
CREATE TABLE b04_fk (k bigint, CONSTRAINT b04_c_fk FOREIGN KEY (k) REFERENCES b04_pk (k));
ALTER TABLE b04_fk ALTER CONSTRAINT b04_c_fk ENFORCED DEFERRABLE INITIALLY DEFERRED;
SELECT condeferrable, condeferred, conenforced FROM pg_constraint WHERE conname = 'b04_c_fk';
ALTER TABLE b04_fk ALTER CONSTRAINT b04_c_fk ENFORCED NOT DEFERRABLE;
SELECT condeferrable, condeferred, conenforced FROM pg_constraint WHERE conname = 'b04_c_fk';
ALTER TABLE b04_fk ALTER CONSTRAINT b04_c_fk NOT ENFORCED DEFERRABLE;
SELECT condeferrable, condeferred, conenforced FROM pg_constraint WHERE conname = 'b04_c_fk';

-- P021: statement-level BEFORE triggers fire once per statement
CREATE TABLE b04_trig (k int, v int);
CREATE TABLE b04_audit (op text);
CREATE FUNCTION b04_audit_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO b04_audit VALUES (TG_OP); RETURN NULL; END $$;
CREATE TRIGGER b04_bi BEFORE INSERT ON b04_trig FOR EACH STATEMENT EXECUTE FUNCTION b04_audit_f();
CREATE TRIGGER b04_bu BEFORE UPDATE ON b04_trig FOR EACH STATEMENT EXECUTE FUNCTION b04_audit_f();
CREATE TRIGGER b04_bd BEFORE DELETE ON b04_trig FOR EACH STATEMENT EXECUTE FUNCTION b04_audit_f();
BEGIN;
INSERT INTO b04_trig VALUES (1, 1);
INSERT INTO b04_trig VALUES (2, 2);
UPDATE b04_trig SET v = v + 1;
UPDATE b04_trig SET v = v + 1;
DELETE FROM b04_trig WHERE k = 1;
DELETE FROM b04_trig WHERE k = 2;
COMMIT;
SELECT op, count(*) FROM b04_audit GROUP BY op ORDER BY op;

-- P028: r.* that is both a FROM alias and a plpgsql record is ambiguous
CREATE TABLE b04_r (k int, v int);
INSERT INTO b04_r VALUES (1, 2);
CREATE FUNCTION b04_amb(a int) RETURNS json LANGUAGE plpgsql AS $$ DECLARE r record; j json; BEGIN SELECT row_to_json(r.*) INTO j FROM b04_r r WHERE k <= a LIMIT 1; RETURN j; END $$;
SELECT b04_amb(15);

-- P029: WHERE CURRENT OF with a refcursor parameter in a SQL function
CREATE FUNCTION b04_bump(curs refcursor) RETURNS int LANGUAGE sql AS $$ UPDATE b04_r SET v = v + 1 WHERE CURRENT OF curs RETURNING k $$;
BEGIN;
DECLARE b04_cur CURSOR FOR SELECT * FROM b04_r ORDER BY k FOR UPDATE;
FETCH 1 FROM b04_cur;
SELECT b04_bump('b04_cur');
COMMIT;
SELECT * FROM b04_r ORDER BY k;

-- P030: an unassigned record target is resolved when the plan is prepared, not per call
CREATE SEQUENCE b04_s;
CREATE TABLE b04_one (v int);
INSERT INTO b04_one VALUES (7);
CREATE FUNCTION b04_assign(a int) RETURNS int LANGUAGE plpgsql AS $$ DECLARE rec record; BEGIN IF a IS NOT NULL THEN SELECT * INTO rec FROM b04_one; END IF; rec.v := nextval('b04_s'); RETURN rec.v; END $$;
SELECT b04_assign(1);
SELECT last_value FROM b04_s;
SELECT b04_assign(NULL);
SELECT last_value FROM b04_s;

-- P033: RETURN QUERY structure mismatch is refused before any row flows
CREATE TABLE b04_empty (k bigint, v bigint);
CREATE FUNCTION b04_rq_exec(a bigint) RETURNS SETOF bigint LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY EXECUTE 'SELECT v::text FROM b04_empty WHERE k > $1 ORDER BY k' USING a; END $$;
CREATE FUNCTION b04_rq(a bigint) RETURNS SETOF bigint LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT v::text FROM b04_empty WHERE k > a ORDER BY k; END $$;
SELECT b04_rq_exec(NULL);
SELECT b04_rq(NULL);

-- P036: lag()/lead() over a text expression with a rank() in between
CREATE TABLE b04_win (k bigint);
INSERT INTO b04_win SELECT g FROM generate_series(1, 6) g;
INSERT INTO b04_win VALUES (11);
INSERT INTO b04_win SELECT g FROM generate_series(1000, 1006) g;
SELECT t.k, lag(t.k::text || '-a') OVER w AS a, rank() OVER w AS r, lead(t.k::text || '-b') OVER w AS b FROM b04_win t WINDOW w AS (ORDER BY t.k) ORDER BY t.k;

-- P044: two output-function calls in one row each print their own value
CREATE TABLE b04_out (k bigint, v bigint, i int, f float8);
INSERT INTO b04_out VALUES (1, 2, 3, 4.5), (2, 4, 5, 6.5), (3, 40, 7, 8.5), (4, 8, 9, 10.5);
SELECT k, v, int8out(k) AS a, int8out(v) AS b FROM b04_out ORDER BY k;
SELECT int4out(i) AS a, int4out(k::int) AS b, float8out(f) AS c, float8out(f * 2) AS d, boolout(k > 2) AS e, boolout(k > 3) AS f FROM b04_out ORDER BY k;
SELECT ROW(int2out(1::int2), int2out(2::int2)) AS r, date_out(date '2024-01-02') AS d1, date_out(date '2025-03-04') AS d2;

-- P059: a hash join stops evaluating a row's keys after a strict NULL
CREATE TABLE b04_hj (k bigint, v bigint);
INSERT INTO b04_hj VALUES (1, 1), (2, NULL), (3, 3);
SET enable_mergejoin = off;
SET enable_nestloop = off;
SELECT count(*) FROM b04_hj t JOIN b04_hj u ON t.v = u.v AND 1 / CASE WHEN t.v IS NULL THEN 0 ELSE 1 END = u.k;
SELECT count(*) FROM b04_hj t LEFT JOIN b04_hj u ON t.v = u.v AND u.k = 1 + t.k - t.k;
RESET enable_mergejoin;
RESET enable_nestloop;
