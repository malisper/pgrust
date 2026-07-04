-- Coercion-completion matrix: ArrayCoerceExpr element coercions (NULLs,
-- multidim, typmod truncation), record/composite casts, inheritance
-- child-row -> parent (ConvertRowtypeExpr), domains over arrays, and the
-- \dRp+ column-list publication deparse. Byte-diffed pgrust vs C 18.3.
SELECT '{1,2,3}'::int4[]::int8[];
SELECT ARRAY[1,2,3]::text[];
SELECT ARRAY['1','22','333']::int[];
SELECT ARRAY[1.7,2.5]::numeric[]::int[];
SELECT ARRAY[1, NULL, 3]::int8[];
SELECT ARRAY[NULL, NULL]::int[]::text[];
SELECT '{}'::int[]::text[];
SELECT ARRAY[[1,2],[3,4]]::int8[];
SELECT ARRAY[[1,NULL],[3,4]]::text[];
SELECT ('[3:5]={7,8,9}'::int[])::int8[];
SELECT ARRAY['abc','defgh']::varchar(3)[];
SELECT ARRAY['abcdef']::char(4)[];
SELECT '{123456,7}'::text[]::varchar(5)[];
SELECT ARRAY['1.53','2.44']::numeric(3,1)[];
SELECT ARRAY[1,2]::int[]::oid[];
SELECT pg_typeof(ARRAY[1,2,3]::int8[]);
CREATE TABLE coerce_vc5 (v varchar(5)[]);
INSERT INTO coerce_vc5 VALUES (ARRAY['12345']);
INSERT INTO coerce_vc5 VALUES (ARRAY['123456']);
INSERT INTO coerce_vc5 VALUES (ARRAY['1234  ']);
SELECT * FROM coerce_vc5;
CREATE VIEW coerce_vc5_v AS SELECT v::text[] AS t, v::varchar(3)[] AS v3 FROM coerce_vc5;
SELECT pg_get_viewdef('coerce_vc5_v'::regclass);
SELECT * FROM coerce_vc5_v;
PREPARE castarr(int[]) AS SELECT $1::int8[];
EXECUTE castarr('{1,NULL,3}');
DEALLOCATE castarr;
-- element-coercion const folding
EXPLAIN (COSTS OFF, VERBOSE) SELECT '{1,2}'::int[]::int8[];
CREATE TABLE coerce_fold (a int[]);
EXPLAIN (COSTS OFF, VERBOSE) SELECT a::int8[] FROM coerce_fold;
DROP TABLE coerce_fold;
-- record -> composite and composite -> record
CREATE TYPE coerce_ct AS (a int, b text);
SELECT ROW(1,'x')::coerce_ct;
SELECT (ROW(1,'x')::coerce_ct).b;
SELECT ROW(1,'x')::coerce_ct::record;
SELECT CAST(ROW(1,2) AS coerce_ct);
SELECT ROW(1,'x','extra')::coerce_ct;
SELECT ROW(1)::coerce_ct;
-- inheritance: child row -> parent rowtype
CREATE TABLE coerce_parent (f1 int, f2 text);
CREATE TABLE coerce_child (f3 int) INHERITS (coerce_parent);
INSERT INTO coerce_child VALUES (1, 'one', 10);
SELECT c::coerce_parent FROM coerce_child c;
SELECT (c::coerce_parent).f2 FROM coerce_child c;
SELECT CAST(c AS coerce_parent) FROM coerce_child c;
CREATE TABLE coerce_gchild (f4 int) INHERITS (coerce_child);
INSERT INTO coerce_gchild VALUES (2, 'two', 20, 200);
SELECT g::coerce_parent FROM coerce_gchild g;
SELECT g::coerce_child FROM coerce_gchild g;
SELECT c::coerce_parent FROM coerce_parent c;
-- domain over array + array-of-domain element coercions
CREATE DOMAIN coerce_posint AS int CHECK (VALUE > 0);
SELECT ARRAY[1,2]::coerce_posint[];
SELECT ARRAY[1,-2]::coerce_posint[];
SELECT ARRAY[NULL::int, 3]::coerce_posint[];
CREATE DOMAIN coerce_intarr AS int[] CHECK (array_length(VALUE,1) <= 3);
SELECT '{1,2,3}'::coerce_intarr;
SELECT '{1,2,3,4}'::coerce_intarr;
SELECT ARRAY[1,2]::coerce_intarr;
SELECT ('{1,2}'::coerce_intarr)::int8[];
CREATE DOMAIN coerce_vc3arr AS varchar(3)[];
SELECT ARRAY['ab','cdef']::coerce_vc3arr;
-- can_coerce_type-driven resolution through function args
CREATE FUNCTION coerce_takes_parent(coerce_parent) RETURNS text
  LANGUAGE sql AS $$ SELECT ($1).f2 $$;
SELECT coerce_takes_parent(c) FROM coerce_child c;
CREATE FUNCTION coerce_takes_anyarr(anyarray) RETURNS int
  LANGUAGE sql AS $$ SELECT array_length($1,1) $$;
SELECT coerce_takes_anyarr(ARRAY[ROW(1,'x')::coerce_ct]);
-- composite array -> record[]
SELECT ARRAY[ROW(1,'x')::coerce_ct]::record[];
-- publication with a column list (the \dRp+ deparse arm)
CREATE TABLE coerce_pubt (id int primary key, tags text[], note text);
CREATE PUBLICATION coerce_pub FOR TABLE coerce_pubt (id, tags) WHERE (id > 0);
SELECT c.relname,
  (CASE WHEN pr.prattrs IS NOT NULL THEN
     (SELECT pg_catalog.string_agg(a.attname, ', ')
        FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s(i),
             pg_catalog.pg_attribute a
       WHERE a.attrelid = pr.prrelid AND a.attnum = (pr.prattrs::pg_catalog.int2[])[s.i])
   END) AS attnames
FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid;
\dRp+ coerce_pub
SELECT * FROM pg_publication_tables WHERE pubname = 'coerce_pub';
DROP PUBLICATION coerce_pub;
DROP TABLE coerce_pubt;
DROP FUNCTION coerce_takes_parent(coerce_parent);
DROP FUNCTION coerce_takes_anyarr(anyarray);
DROP DOMAIN coerce_vc3arr;
DROP DOMAIN coerce_intarr;
DROP DOMAIN coerce_posint;
DROP TABLE coerce_gchild;
DROP TABLE coerce_child;
DROP TABLE coerce_parent;
DROP TYPE coerce_ct;
DROP VIEW coerce_vc5_v;
DROP TABLE coerce_vc5;
