CREATE EXTENSION intarray;
CREATE EXTENSION pageinspect;

-- g_int_picksplit orders equal split costs with pg_qsort, which decides the
-- membership of the two halves. Only the first split is free of gistchoose's
-- random tie-breaking, so insert singletons until the root leaf splits once
-- and compare the resulting pages.
CREATE TABLE picksplit_t(a int[]);
CREATE INDEX picksplit_idx ON picksplit_t USING gist(a);
DO $$
BEGIN
  FOR g IN 1..2000 LOOP
    INSERT INTO picksplit_t VALUES (ARRAY[g]);
    EXIT WHEN pg_relation_size('picksplit_idx') > 8192;
  END LOOP;
END $$;
SELECT count(*) AS rows_until_split FROM picksplit_t;
SELECT pg_relation_size('picksplit_idx') / 8192 AS blocks;
SELECT b, count(*), min(keys), max(keys)
FROM generate_series(0, (pg_relation_size('picksplit_idx') / 8192)::int - 1) b,
     LATERAL gist_page_items(get_raw_page('picksplit_idx', b), 'picksplit_idx')
GROUP BY b ORDER BY b;
SELECT b, itemoffset, keys
FROM generate_series(1, (pg_relation_size('picksplit_idx') / 8192)::int - 1) b,
     LATERAL gist_page_items(get_raw_page('picksplit_idx', b), 'picksplit_idx')
ORDER BY b, itemoffset;
DROP TABLE picksplit_t;
