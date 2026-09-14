-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-136-backend-access-spgist (C 18.6 oracle vs pgrust). Each leg is one
-- row of the batch (scripts/access-spgist-b136-e2e.sh).
\set VERBOSITY verbose
CREATE DATABASE b136e2e TEMPLATE template0 ENCODING 'UTF8';
\c b136e2e
\set VERBOSITY verbose
-- fp-spgist-spgquadtreeproc#1: a cross-type operator bound to an unsupported
-- strategy in quad_point_ops / kd_point_ops raises XX000 from the consistent
-- functions instead of decoding the by-value argument as a point pointer.
CREATE TABLE b136_tp(p point);
INSERT INTO b136_tp SELECT point(i, i*2) FROM generate_series(1,200) i;
CREATE INDEX b136_tp_k ON b136_tp USING spgist (p kd_point_ops);
CREATE INDEX b136_tp_q ON b136_tp USING spgist (p);
CREATE FUNCTION b136_pt_int(point, int4) RETURNS bool LANGUAGE plpgsql IMMUTABLE
  AS $$ BEGIN RETURN $1[0] < $2; END $$;
CREATE OPERATOR ##< (LEFTARG = point, RIGHTARG = int4, FUNCTION = b136_pt_int);
ALTER OPERATOR FAMILY quad_point_ops USING spgist ADD OPERATOR 2 ##<(point, int4);
ALTER OPERATOR FAMILY kd_point_ops USING spgist ADD OPERATOR 2 ##<(point, int4);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM b136_tp WHERE p ##< 5;
SELECT * FROM b136_tp WHERE p ##< 5;
DROP INDEX b136_tp_q;
EXPLAIN (COSTS OFF) SELECT * FROM b136_tp WHERE p ##< 5;
SELECT * FROM b136_tp WHERE p ##< 5;
RESET enable_seqscan;
RESET enable_bitmapscan;
ALTER OPERATOR FAMILY quad_point_ops USING spgist DROP OPERATOR 2 (point, int4);
ALTER OPERATOR FAMILY kd_point_ops USING spgist DROP OPERATOR 2 (point, int4);
DROP TABLE b136_tp;
DROP OPERATOR ##<(point, int4);
DROP FUNCTION b136_pt_int(point, int4);
-- fp-spgist-spgutils#1: an opclass declared FOR TYPE "any" keeps "any" as the
-- index input type ("any" is not polymorphic), so a config leaf type of box
-- without a compress method is rejected at index build.
CREATE TABLE b136_tb(b box);
INSERT INTO b136_tb SELECT box(point(i,i), point(i+1,i+1)) FROM generate_series(1,20) i;
CREATE OPERATOR CLASS b136_boxany_ops FOR TYPE "any" USING spgist AS
  FUNCTION 1 spg_bbox_quad_config(internal, internal),
  FUNCTION 2 spg_box_quad_choose(internal, internal),
  FUNCTION 3 spg_box_quad_picksplit(internal, internal),
  FUNCTION 4 spg_box_quad_inner_consistent(internal, internal),
  FUNCTION 5 spg_box_quad_leaf_consistent(internal, internal);
CREATE INDEX b136_tb_any ON b136_tb USING spgist (b b136_boxany_ops);
SELECT count(*) FROM pg_class WHERE relname = 'b136_tb_any';
DROP TABLE b136_tb;
DROP OPERATOR CLASS b136_boxany_ops USING spgist;
DROP OPERATOR FAMILY b136_boxany_ops USING spgist;
-- fp-spgist-b1#1: the SP-GiST insert temporary context is created and deleted
-- per inserted row, so a BEFORE ROW trigger on a later row never sees it.
CREATE TABLE b136_tm(p point);
CREATE INDEX b136_tm_i ON b136_tm USING spgist (p);
CREATE FUNCTION b136_tm_trg() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM pg_backend_memory_contexts WHERE name LIKE '%SP-GiST%';
  RAISE NOTICE 'spgist contexts: %', n;
  RETURN NEW;
END $$;
CREATE TRIGGER b136_tm_t BEFORE INSERT ON b136_tm FOR EACH ROW EXECUTE FUNCTION b136_tm_trg();
INSERT INTO b136_tm VALUES (point(1,1)), (point(2,2)), (point(3,3));
SELECT count(*) FROM pg_backend_memory_contexts WHERE name LIKE '%SP-GiST%';
DROP TABLE b136_tm;
DROP FUNCTION b136_tm_trg();
