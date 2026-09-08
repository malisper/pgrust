\set ON_ERROR_STOP on
\pset format unaligned
\pset tuples_only on
SET statement_timeout = '20s';
CREATE TABLE sample_subplans (id int);
INSERT INTO sample_subplans SELECT generate_series(1, 20);
SELECT 'const', count(*) FROM sample_subplans TABLESAMPLE BERNOULLI (100) REPEATABLE (42);
SELECT 'arg-bernoulli', count(*) FROM sample_subplans TABLESAMPLE BERNOULLI ((SELECT 100)) REPEATABLE (42);
SELECT 'seed-bernoulli', count(*) FROM sample_subplans TABLESAMPLE BERNOULLI (100) REPEATABLE ((SELECT 42));
SELECT 'both-bernoulli', count(*) FROM sample_subplans TABLESAMPLE BERNOULLI ((SELECT 100)) REPEATABLE ((SELECT 42));
SELECT 'both-system', count(*) FROM sample_subplans TABLESAMPLE SYSTEM ((SELECT 100)) REPEATABLE ((SELECT 42));
SELECT 'zero', count(*) FROM sample_subplans TABLESAMPLE BERNOULLI ((SELECT 0)) REPEATABLE ((SELECT 42));
SELECT 'rescan', v.pct, s.n FROM (VALUES (0), (100), (0), (100)) v(pct), LATERAL (SELECT count(*) n FROM sample_subplans TABLESAMPLE BERNOULLI ((SELECT v.pct)) REPEATABLE ((SELECT v.pct + 42))) s ORDER BY v.pct, s.n;
SELECT 'seed-parity', (SELECT array_agg(id ORDER BY id) FROM sample_subplans TABLESAMPLE BERNOULLI (50) REPEATABLE ((SELECT 42))) IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sample_subplans TABLESAMPLE BERNOULLI (50) REPEATABLE (42));
DROP TABLE sample_subplans;
