-- C-side setup for scripts/explain-analyze-e2e.sh (INSERT..SELECT and VACUUM
-- are unported; the C server prepares the shared datadir).
CREATE TABLE em (a int, b int, c text);
INSERT INTO em SELECT i, i % 100, 'val' || i FROM generate_series(1, 50000) i;
CREATE INDEX em_a_idx ON em (a);
VACUUM ANALYZE em;

CREATE TABLE em_small (x int, y int);
INSERT INTO em_small SELECT i, i * 2 FROM generate_series(1, 200) i;
ANALYZE em_small;

