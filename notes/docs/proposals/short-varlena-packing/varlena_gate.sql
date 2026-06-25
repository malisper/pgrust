CREATE TABLE vt (
  id int,
  t text,
  vc varchar(2000),
  b bytea,
  n numeric,
  ia int[],
  ta text[],
  jb jsonb,
  comp point
);
INSERT INTO vt VALUES
 (1, 'short text', 'short varchar', '\x00ff10'::bytea, 123.456, '{1,2,3}', '{a,bb,ccc}', '{"k":"v","arr":[1,2,3]}', '(1.5,2.5)'),
 (2, repeat('A', 200), repeat('B', 200), decode(repeat('41',200),'hex'), 9876543210.0123456789, '{10,20,30,40}', '{xx,yy}', ('{"big":"'||repeat('z',300)||'"}')::jsonb, '(0,0)');
INSERT INTO vt (id, t) VALUES (3, repeat('Q', 5000));
CREATE TYPE myc AS (a text, b int);
CREATE TABLE ct(c myc);
INSERT INTO ct VALUES (ROW('hello',7));
CREATE INDEX vt_t_idx ON vt(t);
