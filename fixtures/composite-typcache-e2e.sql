-- composite-typcache lane corpus: TYPECACHE_TUPDESC for named composite
-- types (load_typcache_tupdesc sourcing the relcache descriptor), composite
-- comparison support (cache_record_field_properties over the cached
-- descriptor), and the relcache-inval reset discipline
-- (TypeCacheRelCallback -> InvalidateCompositeTypeCacheEntry): DDL on the
-- underlying relation must refresh the cached tupdesc AND the cached
-- operator flags before the next use. Byte-diffed pgrust vs C 18.
CREATE TYPE pair AS (a int, b text);
SELECT ROW(1,'x')::pair < ROW(2,'a')::pair;
SELECT ROW(1,'x')::pair = ROW(1,'x')::pair;
SELECT ROW(2,'a')::pair > ROW(1,'z')::pair;
SELECT ROW(1,NULL)::pair = ROW(1,NULL)::pair;
SELECT ROW(1,'a')::pair IS DISTINCT FROM ROW(1,'a')::pair;
SELECT (ROW(7,'q')::pair).a, (ROW(7,'q')::pair).b;
-- composite column: sort / group / distinct / where / index
CREATE TABLE pt (p pair);
INSERT INTO pt VALUES (ROW(3,'c')), (ROW(1,'a')), (ROW(2,'b')), (ROW(1,'b')), (NULL);
SELECT * FROM pt ORDER BY p;
SELECT p, count(*) FROM pt GROUP BY p ORDER BY p;
SELECT DISTINCT p FROM pt ORDER BY p;
SELECT * FROM pt WHERE p < ROW(2,'b')::pair ORDER BY p;
SELECT * FROM pt WHERE p = ROW(1,'b')::pair;
CREATE INDEX pt_p_idx ON pt (p);
SET enable_seqscan = off;
SELECT * FROM pt WHERE p = ROW(1,'b')::pair;
SELECT * FROM pt WHERE p > ROW(1,'a')::pair ORDER BY p;
RESET enable_seqscan;
-- arrays of composites
SELECT ARRAY[ROW(2,'b')::pair, ROW(1,'a')::pair] = ARRAY[ROW(2,'b')::pair, ROW(1,'a')::pair];
SELECT ARRAY[ROW(1,'a')::pair] < ARRAY[ROW(1,'b')::pair];
CREATE TABLE apt (ap pair[]);
INSERT INTO apt VALUES (ARRAY[ROW(2,'b')::pair]), (ARRAY[ROW(1,'a')::pair, ROW(3,'c')::pair]);
SELECT * FROM apt ORDER BY ap;
-- table rowtype through the same cache (whole-row vars)
CREATE TABLE base_t (x int, y int);
INSERT INTO base_t VALUES (1,2),(3,4);
SELECT (t).x, (t).y FROM base_t t ORDER BY 1;
SELECT t < ROW(2,0)::base_t AS lt FROM base_t t ORDER BY 1;
-- relcache inval on the underlying relation: cached tupdesc must refresh
ALTER TABLE base_t ADD COLUMN z text;
SELECT t = ROW(1,2,NULL)::base_t AS eq FROM base_t t ORDER BY 1;
SELECT (t).z IS NULL AS znull FROM base_t t ORDER BY 1;
ALTER TABLE base_t DROP COLUMN y;
SELECT t < ROW(9,'q')::base_t AS lt2 FROM base_t t ORDER BY 1;
SELECT * FROM base_t ORDER BY 1;
-- ALTER TYPE attribute churn on a standalone composite
CREATE TYPE trio AS (a int, b int);
SELECT ROW(1,2)::trio < ROW(1,3)::trio;
ALTER TYPE trio ADD ATTRIBUTE c text;
SELECT ROW(1,2,'x')::trio < ROW(1,2,'y')::trio;
SELECT ROW(1,2,'x')::trio = ROW(1,2,'x')::trio;
ALTER TYPE trio DROP ATTRIBUTE b;
SELECT ROW(5,'x')::trio = ROW(5,'x')::trio;
SELECT ROW(4,'a')::trio < ROW(5,'a')::trio;
-- record vs named composite: both sides of the shared record cmp family
SELECT ROW(1,'a') = ROW(1,'a');
SELECT r.p = ROW(1,'a')::pair AS req FROM (SELECT p FROM pt WHERE p IS NOT NULL ORDER BY p LIMIT 1) r(p);
-- error parity: non-composite through the rowtype path
SELECT ROW(1,'a')::pair = NULL::pair;
DROP TABLE apt;
DROP TABLE pt;
DROP TYPE pair;
DROP TYPE trio;
DROP TABLE base_t;
