-- bugs/batch-26-backend-access-misc: pgrcolumnar (v1) buffered ingest keys
-- its per-statement writer by the caller's command id, so a BEFORE INSERT
-- ROW trigger whose body runs DML (CommandCounterIncrement mid-statement)
-- no longer evicts the statement's own writer and drops its rows. Expected
-- file captured from pgrust (C has no pgrcolumnar); the counts are the
-- hand-verified truth (every inserted row lands, every trigger fires).
\set VERBOSITY terse
SET client_min_messages = error;
CREATE ACCESS METHOD pgrcolumnar TYPE TABLE HANDLER heap_tableam_handler;
CREATE TABLE b26_cb(a int, b text) USING pgrcolumnar;
CREATE TABLE b26_audit(a int);
CREATE FUNCTION b26_trg() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO b26_audit VALUES (NEW.a); RETURN NEW; END $$;
CREATE TRIGGER b26_t BEFORE INSERT ON b26_cb FOR EACH ROW EXECUTE FUNCTION b26_trg();
INSERT INTO b26_cb VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT count(*) AS cb_rows, sum(a) AS cb_sum FROM b26_cb;
SELECT count(*) AS audit_rows FROM b26_audit;
COPY b26_cb FROM STDIN;
10	x
11	y
12	z
\.
SELECT count(*) AS cb_rows, sum(a) AS cb_sum FROM b26_cb;
SELECT count(*) AS audit_rows FROM b26_audit;
-- A statement that errors mid-ingest inside a savepoint leaves nothing for
-- a later row-less statement's flush to publish.
BEGIN;
SAVEPOINT s;
INSERT INTO b26_cb SELECT g, (CASE WHEN g = 3 THEN 1 / (g - 3) ELSE 1 END)::text FROM generate_series(1, 3) g;
ROLLBACK TO s;
INSERT INTO b26_cb SELECT 9, 'z' WHERE false;
COMMIT;
SELECT count(*) AS cb_rows, sum(a) AS cb_sum FROM b26_cb;
DROP TABLE b26_cb, b26_audit;
DROP FUNCTION b26_trg();
DROP ACCESS METHOD pgrcolumnar;
