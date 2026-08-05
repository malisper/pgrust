-- Sorted-vs-hashed plan-choice probe: EXPLAIN with costs against live C.
-- Diffs here are EXPECTED signal (cost divergence), not a gate.
create table gstest2c (a integer, b integer, c integer, d integer, e integer, f integer,
                       g integer, h integer);
insert into gstest2c values
  (1, 1, 1, 1, 1, 1, 1, 1),
  (1, 1, 1, 1, 1, 1, 1, 2),
  (1, 1, 1, 1, 1, 1, 2, 2),
  (1, 1, 1, 1, 1, 2, 2, 2),
  (1, 1, 1, 1, 2, 2, 2, 2),
  (1, 1, 1, 2, 2, 2, 2, 2),
  (1, 1, 2, 2, 2, 2, 2, 2),
  (1, 2, 2, 2, 2, 2, 2, 2),
  (2, 2, 2, 2, 2, 2, 2, 2);
explain select a, b, count(*) from gstest2c group by grouping sets ((a, b), (a)) having a > 1 and b > 1;
explain select a, b, count(*) from gstest2c group by rollup(a), b having b > 1;
explain select a, count(*) from gstest2c group by grouping sets ((a), ()) having false;
explain select a, b, count(*) from gstest2c group by grouping sets ((a), (b)) having false;
create table int8_tblc(q1 int8, q2 int8);
insert into int8_tblc values
  ('123','456'),('123','4567890123456789'),('4567890123456789','123'),
  ('4567890123456789','4567890123456789'),('4567890123456789','-4567890123456789');
explain select * from (
  select 1 as x, q1, sum(q2)
  from int8_tblc i1
  group by grouping sets(1, 2)
) ss
where x = 1 and q1 = 123;
explain select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) group by rollup (a,b);
drop table gstest2c;
drop table int8_tblc;
