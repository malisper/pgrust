-- Selectivity differential: range/multirange/inet/booltest row estimates,
-- with and without ANALYZE stats. Tables stay under the 30000-row ANALYZE
-- sample floor so statistics are deterministic across binaries.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE TABLE selr(ir int4range, nr numrange);
INSERT INTO selr
  SELECT int4range(g % 100, g % 100 + (g % 7) + 1),
         numrange(((g % 50)::numeric) / 3, ((g % 50)::numeric) / 3 + ((g % 11)::numeric) / 7)
  FROM generate_series(1, 2000) g;
INSERT INTO selr VALUES ('empty', 'empty');
INSERT INTO selr VALUES (NULL, NULL);
INSERT INTO selr VALUES ('[5,)', '(,10]');

EXPLAIN SELECT * FROM selr WHERE ir @> 5;
EXPLAIN SELECT * FROM selr WHERE 5 <@ ir;
EXPLAIN SELECT * FROM selr WHERE ir && '[10,20)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir @> '[5,7)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <@ '[0,200)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir << '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir >> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir &< '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir &> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir < '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <= '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir > '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir >= '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir && 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir @> 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <@ 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE nr @> 4.5;
EXPLAIN SELECT * FROM selr WHERE nr && numrange(2, 8);
EXPLAIN SELECT * FROM selr WHERE nr <@ numrange(0, 100);

ANALYZE selr;

EXPLAIN SELECT * FROM selr WHERE ir @> 5;
EXPLAIN SELECT * FROM selr WHERE 5 <@ ir;
EXPLAIN SELECT * FROM selr WHERE ir && '[10,20)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir @> '[5,7)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <@ '[0,200)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir << '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir >> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir &< '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir &> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir < '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <= '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir > '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir >= '[50,60)'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir && 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir @> 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE ir <@ 'empty'::int4range;
EXPLAIN SELECT * FROM selr WHERE nr @> 4.5;
EXPLAIN SELECT * FROM selr WHERE nr && numrange(2, 8);
EXPLAIN SELECT * FROM selr WHERE nr <@ numrange(0, 100);

SELECT staattnum, stakind1, stakind2, stakind3, staop2, stanumbers2
  FROM pg_statistic s JOIN pg_class c ON c.oid = s.starelid
  WHERE c.relname = 'selr' ORDER BY staattnum;

CREATE TABLE selm(mr int4multirange);
INSERT INTO selm
  SELECT int4multirange(int4range(g % 50, g % 50 + (g % 5) + 1),
                        int4range(g % 50 + 10, g % 50 + 12))
  FROM generate_series(1, 1500) g;
INSERT INTO selm VALUES ('{}'), (NULL);

EXPLAIN SELECT * FROM selm WHERE mr @> 5;
EXPLAIN SELECT * FROM selm WHERE mr && '[10,20)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr && '{[10,20)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr @> '[5,7)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr <@ '{[0,200)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr << '[50,60)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr >> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr < '{[50,60)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr && '{}'::int4multirange;

ANALYZE selm;

EXPLAIN SELECT * FROM selm WHERE mr @> 5;
EXPLAIN SELECT * FROM selm WHERE mr && '[10,20)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr && '{[10,20)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr @> '[5,7)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr <@ '{[0,200)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr << '[50,60)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr >> '[50,60)'::int4range;
EXPLAIN SELECT * FROM selm WHERE mr < '{[50,60)}'::int4multirange;
EXPLAIN SELECT * FROM selm WHERE mr && '{}'::int4multirange;

CREATE TABLE seln(a inet, c cidr);
INSERT INTO seln
  SELECT (('10.' || (g % 4) || '.' || (g % 250) || '.' || (g % 200 + 1)))::inet,
         (('10.' || (g % 4) || '.' || (g % 250) || '.0/24'))::cidr
  FROM generate_series(1, 1800) g;
INSERT INTO seln
  SELECT '192.168.1.7'::inet, '192.168.0.0/16'::cidr
  FROM generate_series(1, 200) g;
INSERT INTO seln VALUES (NULL, NULL);

EXPLAIN SELECT * FROM seln WHERE c << '10.1.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a << '10.2.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a <<= '10.2.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a >> '10.1.3.0'::inet;
EXPLAIN SELECT * FROM seln WHERE a >>= '10.1.3.5'::inet;
EXPLAIN SELECT * FROM seln WHERE a && '10.0.0.0/8'::inet;
EXPLAIN SELECT * FROM seln WHERE '10.1.0.0/16'::inet >> a;

ANALYZE seln;

EXPLAIN SELECT * FROM seln WHERE c << '10.1.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a << '10.2.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a <<= '10.2.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a >> '10.1.3.0'::inet;
EXPLAIN SELECT * FROM seln WHERE a >>= '10.1.3.5'::inet;
EXPLAIN SELECT * FROM seln WHERE a && '10.0.0.0/8'::inet;
EXPLAIN SELECT * FROM seln WHERE '10.1.0.0/16'::inet >> a;

EXPLAIN SELECT count(*) FROM seln x, seln y WHERE x.a << y.c;
EXPLAIN SELECT count(*) FROM seln x WHERE EXISTS (SELECT 1 FROM seln y WHERE x.a << y.c);

CREATE INDEX seln_a_idx ON seln(a);
SET enable_seqscan = off;
EXPLAIN SELECT * FROM seln WHERE a << '10.2.0.0/16'::inet;
EXPLAIN SELECT * FROM seln WHERE a <<= '10.2.0.0/16'::inet;
SET enable_seqscan = on;

CREATE TABLE selb(b boolean, x int4);
INSERT INTO selb SELECT (g % 3 = 0), g FROM generate_series(1, 1000) g;
INSERT INTO selb SELECT NULL, g FROM generate_series(1, 100) g;

EXPLAIN SELECT * FROM selb WHERE b IS TRUE;
EXPLAIN SELECT * FROM selb WHERE b IS NOT TRUE;
EXPLAIN SELECT * FROM selb WHERE b IS FALSE;
EXPLAIN SELECT * FROM selb WHERE b IS NOT FALSE;
EXPLAIN SELECT * FROM selb WHERE b IS UNKNOWN;
EXPLAIN SELECT * FROM selb WHERE b IS NOT UNKNOWN;
EXPLAIN SELECT * FROM selb WHERE b;

ANALYZE selb;

EXPLAIN SELECT * FROM selb WHERE b IS TRUE;
EXPLAIN SELECT * FROM selb WHERE b IS NOT TRUE;
EXPLAIN SELECT * FROM selb WHERE b IS FALSE;
EXPLAIN SELECT * FROM selb WHERE b IS NOT FALSE;
EXPLAIN SELECT * FROM selb WHERE b IS UNKNOWN;
EXPLAIN SELECT * FROM selb WHERE b IS NOT UNKNOWN;
EXPLAIN SELECT * FROM selb WHERE b;

-- neqjoinsel (oprjoin 106): inner and semi shapes.
CREATE TABLE seljoin(x int4);
INSERT INTO seljoin SELECT g % 20 FROM generate_series(1, 500) g;
ANALYZE seljoin;
EXPLAIN SELECT count(*) FROM selb t1, seljoin t2 WHERE t1.x <> t2.x;
EXPLAIN SELECT count(*) FROM selb t1 WHERE EXISTS (SELECT 1 FROM seljoin t2 WHERE t1.x <> t2.x);

DROP TABLE selr, selm, seln, selb, seljoin;
