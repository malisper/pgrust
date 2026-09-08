-- Large-query fuzz finding F1 (2026-08-18): a UNION ALL whose members are
-- subqueries over a NOT MATERIALIZED join-CTE. flatten_simple_union_all /
-- pull_up_simple_union_all build one appendrel per member, then decline to
-- pull the join-bearing member (is_safe_append_member) so each stays a
-- SubqueryScan. pgrust shares one append_rel_list across query levels (C
-- gives each subroot its own), so perform_pullup_replace_vars during a
-- member's nested join pull-up must be scoped to this level's appendrels --
-- otherwise a numerically-colliding varno rewrites an OUTER member's
-- AppendRelInfo.translated_vars, corrupting the second Append arm's targetlist
-- (Var varno != scanrelid at trivial_subqueryscan; wrong/NULL output columns).

CREATE TEMP TABLE fx (id int, g int, h int, v numeric(12,3));
INSERT INTO fx
SELECT i, i % 8, i % 24, ((i * 131 % 1000))::numeric / 10
FROM generate_series(1, 60) AS i;

-- Minimal shape: MATERIALIZED UNION ALL over a NOT MATERIALIZED self-join CTE.
SELECT * FROM (
  WITH w0 AS (SELECT id, g, h, v FROM fx WHERE g < 6),
       w3 AS NOT MATERIALIZED
            (SELECT a.id, a.g, b.h, a.v FROM w0 a JOIN w0 b ON a.g = b.g),
       w4 AS MATERIALIZED
            (SELECT id, g, h, v FROM w3 UNION ALL SELECT id, g, h, v FROM w3)
  SELECT a.id, a.g, b.h, a.v FROM w4 a JOIN w0 b ON a.g = b.g
) _q ORDER BY 1, 2, 3, 4 NULLS LAST LIMIT 40;

-- Both Append arms must carry real column Vars (the second arm previously
-- degenerated to NULL::... constants).
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (
  WITH w0 AS (SELECT id, g, h, v FROM fx WHERE g < 6),
       w3 AS NOT MATERIALIZED
            (SELECT a.id, a.g, b.h, a.v FROM w0 a JOIN w0 b ON a.g = b.g),
       w4 AS MATERIALIZED
            (SELECT id, g, h, v FROM w3 UNION ALL SELECT id, g, h, v FROM w3)
  SELECT a.id, a.g, b.h, a.v FROM w4 a JOIN w0 b ON a.g = b.g
) _q ORDER BY 1, 2, 3, 4 NULLS LAST LIMIT 40;

-- Fuller 10-CTE shape from the fuzz repro (deterministically reproduces the
-- assertion / wrong-plan in debug builds).
SELECT id AS c1, g AS c2, h AS c3, v AS c4 FROM (
  WITH w0 AS (SELECT id, g, h, v FROM fx WHERE g < 6),
       w1 AS NOT MATERIALIZED (SELECT id, g, h, v FROM w0 WHERE h >= 17),
       w3 AS NOT MATERIALIZED
            (SELECT a.id, a.g, b.h, a.v FROM w1 a JOIN w1 b ON a.g = b.g),
       w4 AS MATERIALIZED
            (SELECT id, g, h, v FROM w3 UNION ALL SELECT id, g, h, v FROM w3),
       w5 AS (SELECT a.id, a.g, b.h, a.v FROM w4 a JOIN w0 b ON a.g = b.g),
       w9 AS NOT MATERIALIZED (SELECT id, g, h, v FROM w0 WHERE h < 13),
       w10 AS NOT MATERIALIZED
             (SELECT a.id, a.g, b.h, a.v FROM w9 a JOIN w5 b ON a.g = b.g)
  SELECT id, g, h, v FROM w10
) _q ORDER BY 1, 2, 3, 4 NULLS LAST LIMIT 40;

DO $$
DECLARE total bigint; nonnull bigint;
BEGIN
  WITH w0 AS (SELECT id, g, h, v FROM fx WHERE g < 6),
       w3 AS NOT MATERIALIZED
            (SELECT a.id, a.g, b.h, a.v FROM w0 a JOIN w0 b ON a.g = b.g),
       w4 AS MATERIALIZED
            (SELECT id, g, h, v FROM w3 UNION ALL SELECT id, g, h, v FROM w3)
  SELECT count(*), count(id) INTO total, nonnull FROM w4;
  IF total <> 708 OR nonnull <> 708 THEN
    RAISE EXCEPTION 'incomplete UNION ALL: rows %, nonnull %', total, nonnull;
  END IF;
END $$;
