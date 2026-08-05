-- EXPLAIN (GENERIC_PLAN) differential: unbound PARAM_EXTERN quals must plan
-- and cost byte-identically vs C 18.3 (index path generation + eqsel's
-- var_eq_non_const leg). Envelope per explain-costs-e2e.sql.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE TABLE gp_t(a int, b int, c int);
INSERT INTO gp_t SELECT g % 1000, g, g % 7 FROM generate_series(1, 10000) g;
ANALYZE gp_t;
CREATE INDEX gp_t_ab ON gp_t(a, b);

EXPLAIN (GENERIC_PLAN) SELECT b FROM gp_t WHERE a = $1;
EXPLAIN (GENERIC_PLAN) SELECT b FROM gp_t WHERE a = $1 AND b < $2;
EXPLAIN (GENERIC_PLAN) SELECT count(*) FROM gp_t WHERE a > $1;
SET enable_seqscan = off;
EXPLAIN (GENERIC_PLAN) SELECT b FROM gp_t WHERE a = $1;
SET enable_seqscan = on;
SET enable_bitmapscan = off;
EXPLAIN (GENERIC_PLAN) SELECT b FROM gp_t WHERE a = $1;
SET enable_bitmapscan = on;
-- const control (must stay identical too)
EXPLAIN SELECT b FROM gp_t WHERE a = 42;
