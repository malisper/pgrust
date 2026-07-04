-- Inlined bodies must be visible in the plan; non-inlinable calls must stay
-- FuncExpr. Captured on C 18 and pgrust; byte-diffed.
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_add(a, 1) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_add_named(a, id) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_stable(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_cast(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_sq(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_sq(a + 1) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT inl_sq(a + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_poly(a, id) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_rec(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_multi(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_subq(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_strict_unused(a, id) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_agg(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_imm_volatile_body(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT inl_setconf(a) FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT b || id FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT id || b FROM inl_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM inl_t WHERE inl_add(a, 1) = 5;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM inl_t WHERE id = inl_add(1, 2);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM inl_t WHERE b || id = 'row44';

SELECT 'x' || 1;
SELECT 1 || 'y'::text;
SELECT b || id FROM inl_t WHERE id <= 3 ORDER BY id;
SELECT id || b FROM inl_t WHERE id <= 3 ORDER BY id;
SELECT max(b || id) FROM inl_t;
SELECT ('a' COLLATE "C") || 1;
SELECT inl_add(2, 3), inl_sq(7), inl_stable(41), inl_cast(9);
SELECT inl_poly(2, 5), inl_poly(b, 'zzz') FROM inl_t WHERE id = 1;
SELECT inl_rec(3);
SELECT inl_multi(5), inl_strict_unused(4, 9), inl_agg(8);
SELECT inl_setconf(2);
SELECT id, b || id FROM inl_t WHERE inl_add(a, 1) = 5 ORDER BY id LIMIT 5;
SELECT a.b AS x, b.b AS y, inl_lt_noninline(a.b, b.b) FROM inl_short a, inl_short b ORDER BY a.b, b.b;
SET max_stack_depth = '2048kB';
\set VERBOSITY sqlstate
SELECT inl_selfrec(1);
\set VERBOSITY terse
SELECT inl_selfrec(1);
\set VERBOSITY verbose
RESET max_stack_depth;
