CREATE EXTENSION btree_gist;
CREATE EXTENSION pageinspect;

-- gbt_numeric_penalty (btree_numeric.c) measures range growth in numeric
-- space and drives gistchoose. Split the root leaf once (deterministic: no
-- gistchoose involved), then insert values whose two candidate penalties
-- differ so each choice is deterministic, and compare the pages.
CREATE TABLE numeric_penalty_t(n numeric);
CREATE INDEX numeric_penalty_idx ON numeric_penalty_t USING gist(n);
DO $$
BEGIN
  FOR g IN 1..2000 LOOP
    INSERT INTO numeric_penalty_t VALUES ((g * 7919 % 1000)::numeric / 3);
    EXIT WHEN pg_relation_size('numeric_penalty_idx') > 8192;
  END LOOP;
END $$;
SELECT count(*) AS rows_until_split FROM numeric_penalty_t;
SELECT b, count(*) FROM generate_series(0, 2) b, LATERAL gist_page_items_bytea(get_raw_page('numeric_penalty_idx', b)) GROUP BY b ORDER BY b;
-- Every value must give the two subtrees different penalties: none inside
-- the first page's range once the second covers it (0 vs 0), and moderate
-- magnitudes only (far outside both, both growth ratios round to 1.0). A tie
-- is broken by gistchoose at random.
INSERT INTO numeric_penalty_t VALUES (1000), (-1000), (0.5), (500.5), (2000), (-5), (166.5), (250), (-1500.75), (4000), (333.333333), (-0.75), (12000.5);
SELECT b, count(*) FROM generate_series(0, 2) b, LATERAL gist_page_items_bytea(get_raw_page('numeric_penalty_idx', b)) GROUP BY b ORDER BY b;
SELECT b, itemoffset, encode(key_data, 'hex')
FROM generate_series(0, 2) b, LATERAL gist_page_items_bytea(get_raw_page('numeric_penalty_idx', b))
ORDER BY b, itemoffset;
DROP TABLE numeric_penalty_t;
