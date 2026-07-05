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
  SELECT (('10.' || (g % 4)::text || '.' || (g % 250)::text || '.' || (g % 200 + 1)::text))::inet,
         (('10.' || (g % 4)::text || '.' || (g % 250)::text || '.0/24'))::cidr
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
INSERT INTO selb SELECT NULL, 1000 + g FROM generate_series(1, 100) g;

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

-- eqjoinsel MCV x MCV (both sides carry MCV lists): inner/left/full/semi/
-- anti/reversed-semi/neq, plus a restricted inner (nd2 clamp in the semi arm).
CREATE TABLE selmcv1(v int4, w text);
CREATE TABLE selmcv2(v int4, w text);
INSERT INTO selmcv1 SELECT g % 7, 'r' || (g % 7) FROM generate_series(1, 500) g;
INSERT INTO selmcv1 SELECT 100 + g, 'r' || (100 + g) FROM generate_series(1, 1500) g;
INSERT INTO selmcv2 SELECT g % 5, 'q' || (g % 5) FROM generate_series(1, 500) g;
INSERT INTO selmcv2 SELECT 100000 + g, 'q' || (100000 + g) FROM generate_series(1, 1000) g;
INSERT INTO selmcv1 SELECT NULL, NULL FROM generate_series(1, 50);
INSERT INTO selmcv2 SELECT NULL, NULL FROM generate_series(1, 30);
ANALYZE selmcv1;
ANALYZE selmcv2;
EXPLAIN SELECT count(*) FROM selmcv1 a, selmcv2 b WHERE a.v = b.v;
EXPLAIN SELECT count(*) FROM selmcv1 a LEFT JOIN selmcv2 b ON a.v = b.v;
EXPLAIN SELECT count(*) FROM selmcv1 a FULL JOIN selmcv2 b ON a.v = b.v;
EXPLAIN SELECT count(*) FROM selmcv1 a WHERE EXISTS (SELECT 1 FROM selmcv2 b WHERE a.v = b.v);
EXPLAIN SELECT count(*) FROM selmcv1 a WHERE NOT EXISTS (SELECT 1 FROM selmcv2 b WHERE a.v = b.v);
EXPLAIN SELECT count(*) FROM selmcv1 a WHERE EXISTS (SELECT 1 FROM selmcv2 b WHERE b.v = a.v);
EXPLAIN SELECT count(*) FROM selmcv2 b WHERE EXISTS (SELECT 1 FROM selmcv1 a WHERE a.v = b.v);
EXPLAIN SELECT count(*) FROM selmcv1 a, selmcv2 b WHERE a.v <> b.v;
EXPLAIN SELECT count(*) FROM selmcv1 a WHERE EXISTS (SELECT 1 FROM selmcv2 b WHERE a.v = b.v AND b.w LIKE 'q1%');
EXPLAIN SELECT count(*) FROM selmcv1 a, selmcv2 b WHERE a.w = b.w;

-- convert_bytea_to_scalar: histogram interpolation over bytea bounds.
CREATE TABLE selbytea(b bytea);
INSERT INTO selbytea SELECT decode(substr(md5(g::text), 1, 8), 'hex') FROM generate_series(1, 400) g;
ANALYZE selbytea;
EXPLAIN SELECT * FROM selbytea WHERE b > '\x80'::bytea;
EXPLAIN SELECT * FROM selbytea WHERE b < '\x40ff'::bytea;
EXPLAIN SELECT * FROM selbytea WHERE b >= '\x20'::bytea AND b <= '\xa0'::bytea;

-- estimate_multivariate_bucketsize: ndistinct extended stats on the inner.
CREATE TABLE selmvb1(a int4, b int4);
CREATE TABLE selmvb2(a int4, b int4);
INSERT INTO selmvb1 SELECT g % 50, (g % 50) / 10 FROM generate_series(1, 2000) g;
INSERT INTO selmvb2 SELECT g % 100, g % 10 FROM generate_series(1, 3000) g;
CREATE STATISTICS selmvb1_nd (ndistinct) ON a, b FROM selmvb1;
ANALYZE selmvb1;
ANALYZE selmvb2;
SET enable_mergejoin = off;
SET enable_nestloop = off;
EXPLAIN SELECT count(*) FROM selmvb2 t2 JOIN selmvb1 t1 ON t1.a = t2.a AND t1.b = t2.b;
RESET enable_mergejoin;
RESET enable_nestloop;

-- gincost_scalararrayopexpr: SAOP quals against a GIN jsonb index.
CREATE TABLE selgin(j jsonb);
INSERT INTO selgin SELECT jsonb_build_object('k' || (g % 20), g, 'tag', g % 7) FROM generate_series(1, 800) g;
CREATE INDEX selgin_idx ON selgin USING gin (j);
ANALYZE selgin;
EXPLAIN SELECT count(*) FROM selgin WHERE j ? ANY (ARRAY['k1', 'k2', 'k3']);
EXPLAIN SELECT count(*) FROM selgin WHERE j ? ANY (ARRAY['tag', NULL]);
EXPLAIN SELECT count(*) FROM selgin WHERE j ? ANY (ARRAY['tag', 'k1']);

-- examine_indexcol_variable expression arm: skip-scan gap over an
-- expression index column (btree) and a BRIN expression index. Keys are
-- duplicate-heavy: the CREATE INDEX build deduplicates into posting lists,
-- so relpages must match C's (build-dedup lane).
CREATE TABLE selexpr(a int4, b int4, c int4);
INSERT INTO selexpr SELECT g % 5, g % 11, g % 3 FROM generate_series(1, 3000) g;
CREATE INDEX selexpr_idx ON selexpr (a, (b + 1), c);
ANALYZE selexpr;
SELECT staattnum, stadistinct FROM pg_statistic WHERE starelid = 'selexpr_idx'::regclass ORDER BY staattnum;
SELECT relpages FROM pg_class WHERE relname = 'selexpr_idx';
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN SELECT count(*) FROM selexpr WHERE a = 1 AND c = 2;
SET enable_indexonlyscan = off;
EXPLAIN SELECT count(*) FROM selexpr WHERE a = 1 AND c = 2;
RESET enable_indexonlyscan;
EXPLAIN SELECT * FROM selexpr WHERE a = 1 AND (b + 1) = 5 AND c = 2;
RESET enable_seqscan;
RESET enable_bitmapscan;
CREATE TABLE selbrin(x int4);
INSERT INTO selbrin SELECT g FROM generate_series(1, 5000) g;
CREATE INDEX selbrin_idx ON selbrin USING brin ((x * 2));
ANALYZE selbrin;
SET enable_seqscan = off;
EXPLAIN SELECT count(*) FROM selbrin WHERE x * 2 < 100;
RESET enable_seqscan;

-- all_rows_selectable: table-privilege path; numeric_eq is not leakproof, so
-- MCV use keys off acl_ok. (Partitioned ANALYZE/pruning and column-level
-- GRANT are unported lanes; the appendrel/column walk is covered by audit.)
CREATE TABLE selacl(i int4, n numeric);
INSERT INTO selacl SELECT g % 200, (g % 40) * 0.5 FROM generate_series(1, 1000) g;
ANALYZE selacl;
GRANT SELECT ON selacl TO pg_checkpoint;
SET SESSION AUTHORIZATION pg_checkpoint;
EXPLAIN SELECT i FROM selacl WHERE i = 5;
EXPLAIN SELECT n FROM selacl WHERE n = 2.5;
RESET SESSION AUTHORIZATION;
EXPLAIN SELECT n FROM selacl WHERE n = 2.5;
REVOKE SELECT ON selacl FROM pg_checkpoint;
SET SESSION AUTHORIZATION pg_checkpoint;
EXPLAIN SELECT n FROM selacl WHERE n = 2.5;
RESET SESSION AUTHORIZATION;

DROP TABLE selr, selm, seln, selb, seljoin, selmcv1, selmcv2, selbytea, selmvb1, selmvb2, selgin, selexpr, selbrin, selacl;

-- estimate_num_groups completion: boolean group exprs (x2 short-circuit),
-- whole-expression stats via expression index, SRF multiplier, known-equal
-- cross-rel dedup; scalarineqsel CTID block-position arm.
CREATE TABLE selgrp(a int4, b int4, flag bool, t text);
INSERT INTO selgrp SELECT g % 50, g % 7, (g % 3 = 0), 'v' || (g % 20) FROM generate_series(1, 2000) g;
CREATE INDEX selgrp_expr_idx ON selgrp ((a + b));
ANALYZE selgrp;
EXPLAIN SELECT flag, count(*) FROM selgrp GROUP BY flag;
EXPLAIN SELECT a, flag, count(*) FROM selgrp GROUP BY a, flag;
EXPLAIN SELECT count(*) FROM selgrp GROUP BY (a = 1);
EXPLAIN SELECT a + b, count(*) FROM selgrp GROUP BY a + b;
EXPLAIN SELECT DISTINCT a + b FROM selgrp;
EXPLAIN SELECT DISTINCT t || 'x' FROM selgrp;
EXPLAIN SELECT DISTINCT a, generate_series(1, 3) FROM selgrp;
CREATE TABLE selgrp2(a int4, c int4);
INSERT INTO selgrp2 SELECT g % 10, g % 4 FROM generate_series(1, 1000) g;
ANALYZE selgrp2;
EXPLAIN SELECT selgrp.a, selgrp2.a, count(*) FROM selgrp JOIN selgrp2 ON selgrp.a = selgrp2.a GROUP BY selgrp.a, selgrp2.a;
EXPLAIN SELECT selgrp2.a, selgrp.a, count(*) FROM selgrp JOIN selgrp2 ON selgrp.a = selgrp2.a GROUP BY selgrp2.a, selgrp.a;
SET enable_tidscan = off;
EXPLAIN SELECT count(*) FROM selgrp WHERE ctid < '(3,0)';
EXPLAIN SELECT count(*) FROM selgrp WHERE ctid <= '(3,10)';
EXPLAIN SELECT count(*) FROM selgrp WHERE ctid > '(5,1)';
EXPLAIN SELECT count(*) FROM selgrp WHERE ctid >= '(8,40)';
EXPLAIN SELECT count(*) FROM selgrp WHERE '(3,0)' > ctid;
RESET enable_tidscan;
DROP TABLE selgrp, selgrp2;

-- arraycontsel/arraycontjoinsel + scalararraysel_containment
-- (array_selfuncs.c): MCELEM/DECHIST estimates, pre and post ANALYZE.
CREATE TABLE selarr(ia int4[], ta text[]);
INSERT INTO selarr
  SELECT ARRAY[g % 10, g % 23, g % 61, 1000 + g % 5],
         ARRAY['w' || (g % 15)::text, 'w' || (g % 40)::text]
  FROM generate_series(1, 2000) g;
INSERT INTO selarr
  SELECT ARRAY[g % 7, NULL, g % 13], ARRAY['x' || (g % 9)::text, NULL]
  FROM generate_series(1, 60) g;
INSERT INTO selarr VALUES ('{}', '{}');
INSERT INTO selarr VALUES (NULL, NULL);

EXPLAIN SELECT * FROM selarr WHERE ia @> ARRAY[3];
EXPLAIN SELECT * FROM selarr WHERE ia && ARRAY[3, 1002];
EXPLAIN SELECT * FROM selarr WHERE ia <@ ARRAY[0, 1, 2, 3, 23, 46, 1000, 1001];
EXPLAIN SELECT * FROM selarr WHERE ARRAY[3] <@ ia;
EXPLAIN SELECT * FROM selarr WHERE ARRAY[0, 1, 2, 3] @> ia;
EXPLAIN SELECT * FROM selarr WHERE ia @> '{3,NULL}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia && '{NULL,7}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia @> NULL;
EXPLAIN SELECT * FROM selarr WHERE 3 = ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 <> ALL (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 = ALL (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 <> ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE ta @> ARRAY['w3'];
EXPLAIN SELECT * FROM selarr WHERE ta && ARRAY['w3', 'zzz'];
EXPLAIN SELECT * FROM selarr WHERE 'w3' = ANY (ta);

ANALYZE selarr;

EXPLAIN SELECT * FROM selarr WHERE ia @> ARRAY[3];
EXPLAIN SELECT * FROM selarr WHERE ia @> ARRAY[3, 3, 16, 1002];
EXPLAIN SELECT * FROM selarr WHERE ia && ARRAY[3, 1002];
EXPLAIN SELECT * FROM selarr WHERE ia && ARRAY[999999];
EXPLAIN SELECT * FROM selarr WHERE ia <@ ARRAY[0, 1, 2, 3, 23, 46, 1000, 1001];
EXPLAIN SELECT * FROM selarr WHERE ia <@ (SELECT array_agg(g) FROM generate_series(0, 70) g);
EXPLAIN SELECT * FROM selarr WHERE ARRAY[3] <@ ia;
EXPLAIN SELECT * FROM selarr WHERE ARRAY[0, 1, 2, 3] @> ia;
EXPLAIN SELECT * FROM selarr WHERE ia @> '{3,NULL}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia && '{NULL,7}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia <@ '{NULL,0,1,2,3}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia @> '{}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia && '{}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE ia <@ '{}'::int4[];
EXPLAIN SELECT * FROM selarr WHERE 3 = ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE 1002 = ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE 999999 = ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 <> ALL (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 = ALL (ia);
EXPLAIN SELECT * FROM selarr WHERE 3 <> ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE NULL::int4 = ANY (ia);
EXPLAIN SELECT * FROM selarr WHERE ta @> ARRAY['w3'];
EXPLAIN SELECT * FROM selarr WHERE ta && ARRAY['w3', 'zzz'];
EXPLAIN SELECT * FROM selarr WHERE ta <@ ARRAY['w0','w1','w2','w3','w4','x1','x2'];
EXPLAIN SELECT * FROM selarr WHERE 'w3' = ANY (ta);
EXPLAIN SELECT * FROM selarr WHERE 'w3' <> ALL (ta);
-- join oprjoin stub (arraycontjoinsel)
CREATE TABLE selarr2(ia int4[]);
INSERT INTO selarr2 SELECT ARRAY[g % 10, g % 4] FROM generate_series(1, 500) g;
ANALYZE selarr2;
EXPLAIN SELECT count(*) FROM selarr a JOIN selarr2 b ON a.ia @> b.ia;
EXPLAIN SELECT count(*) FROM selarr a JOIN selarr2 b ON a.ia && b.ia;
DROP TABLE selarr, selarr2;
