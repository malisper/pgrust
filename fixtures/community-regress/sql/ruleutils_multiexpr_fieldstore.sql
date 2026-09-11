-- ruleutils get_update_query_targetlist_def (walker-arms audit, PR #2102 row
-- "T_FieldStore"): a multi-column SET whose targets are composite sub-fields
-- or array elements wraps the PARAM_MULTIEXPR Param in FieldStore /
-- SubscriptingRef (and an implicit CoerceToDomain for a domain column); the
-- deparser must dig through them to print "(c.a, x) = (SELECT ...)".
-- pgrust used to fail with "PARAM_MULTIEXPR deparse unported" for every
-- pg_get_ruledef / pg_rules / pg_dump of such a rule. Expected output is
-- captured from C PG 18.
CREATE TYPE mx_ctyp AS (a int, b text);
CREATE DOMAIN mx_dom AS int CHECK (VALUE >= 0);
CREATE TABLE mx_t (id int PRIMARY KEY, c mx_ctyp, x int, arr int[], d mx_dom);
CREATE TABLE mx_log (id int, note text);
CREATE RULE mx_r1 AS ON INSERT TO mx_log DO ALSO
  UPDATE mx_t SET (c.a, x) = (SELECT 1, 2) WHERE mx_t.id = NEW.id;
CREATE RULE mx_r2 AS ON INSERT TO mx_log DO ALSO
  UPDATE mx_t SET (c.b, arr[1]) = (SELECT 'z', 7) WHERE mx_t.id = NEW.id;
CREATE RULE mx_r3 AS ON INSERT TO mx_log DO ALSO
  UPDATE mx_t SET (x, d) = (SELECT 3, 4), (c.a, c.b) = (SELECT 5, 'q') WHERE mx_t.id = NEW.id;
CREATE RULE mx_r4 AS ON INSERT TO mx_log DO ALSO
  UPDATE mx_t SET (d, arr[2], c.a) = (SELECT NEW.id, NEW.id * 10, NEW.id * 100) WHERE mx_t.id = NEW.id;
SELECT rulename, definition FROM pg_rules WHERE tablename = 'mx_log' ORDER BY rulename;
SELECT rulename, pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename LIKE 'mx_r%' ORDER BY rulename;
SELECT rulename, pg_get_ruledef(oid, false) FROM pg_rewrite WHERE rulename LIKE 'mx_r%' ORDER BY rulename;
-- the rules must also fire correctly
INSERT INTO mx_t VALUES (1, ROW(0, 'a'), 0, ARRAY[0, 0], 0), (2, ROW(0, 'b'), 0, ARRAY[0, 0], 0);
INSERT INTO mx_log VALUES (2, 'fire');
SELECT id, c, x, arr, d FROM mx_t ORDER BY id;
-- pg_get_ruledef of a rule whose action is the multi-assignment inside a
-- data-modifying CTE (get_with_clause -> get_update_query_def)
CREATE RULE mx_r5 AS ON DELETE TO mx_log DO INSTEAD
  WITH u AS (UPDATE mx_t SET (c.a, arr[1]) = (SELECT 9, 9) WHERE id = 1 RETURNING id)
  DELETE FROM mx_log WHERE id IN (SELECT id FROM u);
SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'mx_r5';
DROP TABLE mx_log, mx_t;
DROP DOMAIN mx_dom;
DROP TYPE mx_ctyp;
