-- Finding (thermite idx 152): grouped-column substitution skipped TABLESAMPLE
-- args in nested subqueries, so an ungrouped outer column inside a nested
-- TABLESAMPLE was wrongly accepted instead of raising error 42803.
CREATE TABLE thermite_g(g int, v int);
INSERT INTO thermite_g VALUES (1,10),(1,20),(2,30);
SELECT g, (SELECT count(*) FROM thermite_g s TABLESAMPLE bernoulli(t.v)) FROM thermite_g t GROUP BY g;
DROP TABLE thermite_g;
