-- Finding (thermite idx 236): flatten_unplanned_rtes' walker stopped at T_Query
-- (expression_tree_walker returns false there), so relation RTEs inside SubLink
-- subselects / CTE bodies of a never-planned (dead) subquery never reached
-- finalrtable and the executor skipped their permission checks. An unprivileged
-- role reading a secret table via a dead subquery must be denied.
CREATE TABLE thermite_secret (a int);
INSERT INTO thermite_secret VALUES (1);
CREATE TABLE thermite_visible (id int);
INSERT INTO thermite_visible VALUES (1);
CREATE ROLE thermite_role NOLOGIN;
GRANT SELECT ON thermite_visible TO thermite_role;
SET ROLE thermite_role;
SELECT * FROM (SELECT (SELECT sum(a) FROM thermite_secret) AS x
               FROM thermite_visible LIMIT 10) s
WHERE s.x = 1 AND s.x = 2;
RESET ROLE;
DROP TABLE thermite_secret, thermite_visible CASCADE;
DROP OWNED BY thermite_role CASCADE;
DROP ROLE thermite_role;
