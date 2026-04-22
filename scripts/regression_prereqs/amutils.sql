-- Minimal persistent fixtures needed by amutils.sql when run in isolation.

DROP INDEX IF EXISTS onek_hundred;
CREATE INDEX onek_hundred ON onek USING btree (hundred int4_ops);

DROP TABLE IF EXISTS circle_tbl;
CREATE TABLE circle_tbl (f1 circle);
INSERT INTO circle_tbl (f1) VALUES
    ('<(5,1),3>'),
    ('((1,2),100)'),
    ('<(3,5),0>');
CREATE INDEX gcircleind ON circle_tbl USING gist (f1);

DROP TABLE IF EXISTS hash_i4_heap;
CREATE TABLE hash_i4_heap (
    seqno int4,
    random int4
);
INSERT INTO hash_i4_heap (seqno, random) VALUES
    (1, 843938989),
    (2, 66766766),
    (3, 1492795354);
CREATE INDEX hash_i4_index ON hash_i4_heap USING hash (random int4_ops);

DROP TABLE IF EXISTS quad_point_tbl;
CREATE TABLE quad_point_tbl (p point);
INSERT INTO quad_point_tbl (p) VALUES
    ('(333.0,400.0)'),
    ('(4585,365)'),
    ('(0,0)'),
    (NULL),
    (NULL);
CREATE INDEX sp_quad_ind ON quad_point_tbl USING spgist (p);

DROP TABLE IF EXISTS radix_text_tbl;
CREATE TABLE radix_text_tbl (t text);
INSERT INTO radix_text_tbl (t) VALUES
    ('Aztec                         Ct  '),
    ('Worth                         St  '),
    ('P0123456789abcdef'),
    ('P0123456789abcde'),
    ('P0123456789abcdefF');
CREATE INDEX sp_radix_ind ON radix_text_tbl USING spgist (t);

DROP TABLE IF EXISTS array_index_op_test;
CREATE TABLE array_index_op_test (
    seqno int4,
    i int4[],
    t text[]
);
INSERT INTO array_index_op_test (seqno, i, t) VALUES
    (1, ARRAY[32, 17], ARRAY['AAAAAAA80240', 'AAAAAAAA72908']),
    (2, ARRAY[47, 77], ARRAY['AAAAAAAAAA646', 'A87088']),
    (3, ARRAY[]::int4[], ARRAY[]::text[]);
CREATE INDEX botharrayidx ON array_index_op_test USING gin (i, t);

DROP TABLE IF EXISTS brintest;
CREATE TABLE brintest (
    byteacol bytea,
    int4col int4
);
INSERT INTO brintest (byteacol, int4col) VALUES
    ('\x00010203'::bytea, 1),
    ('\x04050607'::bytea, 2),
    ('\x08090a0b'::bytea, 3);
CREATE INDEX brinidx ON brintest USING brin (byteacol);
