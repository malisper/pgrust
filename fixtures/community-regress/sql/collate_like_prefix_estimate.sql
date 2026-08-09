-- LIKE/regex prefix range-bound generation over a non-C collation
-- (make_greater_string's suffix leg, like_support.c). Sibling of the
-- convert_string_datum pg_strxfrm panic (HN 49220159): planning a
-- `s LIKE 'prefix%'` or `s ~ '^prefix'` predicate over a histogram on a
-- non-C-collated text column reached prefix_selectivity ->
-- make_greater_string, which panicked instead of building the
-- suffixed comparison string and verifying candidates with the
-- collation-aware "<" operator. The collation is created explicitly so the
-- case is independent of the runner's initdb locale (--no-locale); libc
-- provider exercises the varstr_cmp lane on both glibc and macOS.
CREATE COLLATION regress_en_us_libc (provider = libc, locale = 'en_US.UTF-8');
CREATE TABLE collate_like_est (s text COLLATE regress_en_us_libc);
INSERT INTO collate_like_est SELECT md5(i::text) FROM generate_series(1, 5000) i;
-- patternsel only consults the prefix heuristic when the histogram has
-- fewer than 100 entries (hist_size < 100 in patternsel_common); shrink the
-- statistics target so prefix_selectivity -> make_greater_string engages
ALTER TABLE collate_like_est ALTER COLUMN s SET STATISTICS 50;
CREATE INDEX collate_like_est_pat_idx ON collate_like_est (s text_pattern_ops);
ANALYZE collate_like_est;
-- plan-shape discriminator: match_pattern_prefix runs make_greater_string
-- with the (non-C) index collation, and the generated upper bound appears
-- verbatim in the index condition (~<~ '00fg'). A degradation to the
-- give-up fallback (no upper bound) would drop that arm, so this pin
-- cannot pass vacuously.
EXPLAIN (COSTS OFF) SELECT * FROM collate_like_est WHERE s LIKE '00ff%';
-- fixed prefix inside the histogram range -> prefix_selectivity ->
-- make_greater_string (the panic path)
SELECT count(*) FROM collate_like_est WHERE s LIKE '8%';
-- same leg through the regex path
SELECT count(*) FROM collate_like_est WHERE s ~ '^8';
-- multi-character prefix: increment falls back through shorter prefixes
SELECT count(*) FROM collate_like_est WHERE s LIKE '00ff%';
-- negated form goes through the same estimator
SELECT count(*) FROM collate_like_est WHERE s NOT LIKE '8%';
DROP TABLE collate_like_est;
DROP COLLATION regress_en_us_libc;
