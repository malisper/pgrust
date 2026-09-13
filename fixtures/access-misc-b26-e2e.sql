-- bugs/batch-26-backend-access-misc: C-vs-pgrust parity for the fixes in
-- access/{spgist,gist,common,index}. Expected file captured from C 18.6
-- (scripts/regress-diff.sh --capture).
\set VERBOSITY verbose

-- spgist: index-only scan reconstructs a text value wider than an index
-- tuple can hold (storeGettuple forms a heap tuple, not an index tuple)
CREATE TABLE b26_spg(v text);
INSERT INTO b26_spg SELECT string_agg(md5(g::text), '' ORDER BY g) FROM generate_series(1, 1024) g;
INSERT INTO b26_spg VALUES ('short'), (NULL);
CREATE INDEX b26_spg_i ON b26_spg USING spgist (v);
VACUUM ANALYZE b26_spg;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT length(v), md5(v) FROM b26_spg WHERE v IS NOT NULL ORDER BY 1;
SELECT length(v) FROM b26_spg WHERE v > 'a' ORDER BY 1;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE b26_spg;

-- AM handlers called through a LANGUAGE internal alias return a non-null
-- routine pointer (C: makeNode(IndexAmRoutine))
CREATE FUNCTION b26_gist_h(bigint) RETURNS bigint AS 'gisthandler' LANGUAGE internal;
CREATE FUNCTION b26_spg_h(bigint) RETURNS bigint AS 'spghandler' LANGUAGE internal;
CREATE FUNCTION b26_brin_h(bigint) RETURNS bigint AS 'brinhandler' LANGUAGE internal;
CREATE FUNCTION b26_bt_h(bigint) RETURNS bigint AS 'bthandler' LANGUAGE internal;
CREATE FUNCTION b26_hash_h(bigint) RETURNS bigint AS 'hashhandler' LANGUAGE internal;
CREATE FUNCTION b26_heap_h(bigint) RETURNS bigint AS 'heap_tableam_handler' LANGUAGE internal;
SELECT b26_gist_h(0) IS NOT NULL, b26_spg_h(0) IS NOT NULL, b26_brin_h(0) IS NOT NULL,
       b26_bt_h(0) IS NOT NULL, b26_hash_h(0) IS NOT NULL, b26_heap_h(0) IS NOT NULL;
DROP FUNCTION b26_gist_h, b26_spg_h, b26_brin_h, b26_bt_h, b26_hash_h, b26_heap_h;

-- gist_point_sortsupport reached through an alias as an opclass's support
-- function 11 installs the z-order comparator for the sorted build
CREATE FUNCTION b26_point_ss(internal) RETURNS void AS 'gist_point_sortsupport' LANGUAGE internal STRICT;
CREATE OPERATOR CLASS b26_point_ops FOR TYPE point USING gist AS
  OPERATOR 1 <<, OPERATOR 5 >>, OPERATOR 6 ~=, OPERATOR 10 <<|, OPERATOR 11 |>>,
  OPERATOR 15 <-> FOR ORDER BY pg_catalog.float_ops,
  OPERATOR 28 <@ (point, box), OPERATOR 29 <^, OPERATOR 30 >^,
  OPERATOR 48 <@ (point, polygon), OPERATOR 68 <@ (point, circle),
  FUNCTION 1 gist_point_consistent(internal, point, smallint, oid, internal),
  FUNCTION 2 gist_box_union(internal, internal),
  FUNCTION 3 gist_point_compress(internal),
  FUNCTION 5 gist_box_penalty(internal, internal, internal),
  FUNCTION 6 gist_box_picksplit(internal, internal),
  FUNCTION 7 gist_box_same(box, box, internal),
  FUNCTION 8 gist_point_distance(internal, point, smallint, oid, internal),
  FUNCTION 9 gist_point_fetch(internal),
  FUNCTION 11 b26_point_ss(internal),
  STORAGE box;
CREATE TABLE b26_pt(id int, pt point);
INSERT INTO b26_pt SELECT g, point(g % 97, g % 89) FROM generate_series(1, 5000) g;
CREATE INDEX b26_pt_i ON b26_pt USING gist (pt b26_point_ops);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), min(id), max(id) FROM b26_pt WHERE pt <@ box '((10,10),(30,30))';
SELECT id FROM b26_pt ORDER BY pt <-> point(50,50), id LIMIT 3;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE b26_pt;
DROP OPERATOR CLASS b26_point_ops USING gist;
DROP FUNCTION b26_point_ss(internal);
