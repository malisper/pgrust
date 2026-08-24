-- Finding (thermite idx 106): missed Var substitution in TABLESAMPLE args
-- (rewrite_manip omitted the RTE_RELATION tablesample arm) mis-mapped a view
-- column inside a TABLESAMPLE expression during rewrite. The UPDATE must work.
CREATE TABLE thermite_base(a text, b int);
INSERT INTO thermite_base VALUES ('s', 5);
CREATE VIEW thermite_v AS SELECT b AS c FROM thermite_base;
UPDATE thermite_v SET c = c
  WHERE EXISTS (SELECT 1 FROM pg_class
                TABLESAMPLE bernoulli(CASE WHEN c <= 3 THEN 100.0 ELSE 100.0 END) REPEATABLE(1));
SELECT c FROM thermite_v;
DROP VIEW thermite_v;
DROP TABLE thermite_base;
