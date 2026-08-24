-- Finding (thermite idx 145): subquery pull-up never descended into a nested
-- cteList, leaving a stale correlated Var that tripped the planner. A
-- correlated CTE inside a sublink over a pullable FROM-subquery must plan.
CREATE TABLE thermite_t(id int);
INSERT INTO thermite_t VALUES (1),(2);
SELECT * FROM (SELECT id FROM thermite_t) s
WHERE EXISTS (WITH cte AS (SELECT s.id AS y) SELECT 1 FROM cte WHERE cte.y = s.id)
ORDER BY id;
DROP TABLE thermite_t;
