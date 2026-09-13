-- DEFAULT-settings grouping-sets matrix: hashed / AGG_MIXED strategies.
-- Result queries pin ORDER BY: hashed grouping-set row order is
-- implementation-defined and our hash-table iteration order differs from
-- C's simplehash bucket order.
create table gsd(a int, b int, c int);
insert into gsd values (1,1,1),(1,1,2),(1,2,3),(2,1,4),(2,2,5),(2,2,6),(1,1,10),(3,1,7);
explain (costs off) select a, b, sum(c) from gsd group by grouping sets ((a),(b));
select a, b, sum(c) from gsd group by grouping sets ((a),(b)) order by 1,2,3;
explain (costs off) select a, b, sum(c) from gsd group by grouping sets ((a),(b),());
select a, b, sum(c) from gsd group by grouping sets ((a),(b),()) order by 1,2,3;
explain (costs off) select a, b, sum(c) from gsd group by rollup(a,b);
select a, b, sum(c) from gsd group by rollup(a,b) order by 1,2,3;
explain (costs off) select a, b, sum(c) from gsd group by cube(a,b);
select a, b, sum(c) from gsd group by cube(a,b) order by 1,2,3;
explain (costs off) select a, b, grouping(a,b), sum(c) from gsd group by cube(a,b);
select a, b, grouping(a,b), sum(c) from gsd group by cube(a,b) order by 1,2,3,4;
select a, b, grouping(a,b), sum(c) from gsd group by grouping sets ((a,b),(a),()) order by 1,2,3,4;
select a, grouping(a), sum(c) from gsd group by grouping sets (a,b) having sum(c) > 6 order by 1,2,3;
select count(*) from gsd group by grouping sets ((a),(a,b),()) order by 1;
select a, sum(c) from gsd where b = 1 group by grouping sets (a, ()) order by 1,2;
set work_mem = '64kB';
explain (costs off) select a, b, sum(c) from gsd group by cube(a,b);
select a, b, sum(c) from gsd group by cube(a,b) order by 1,2,3;
reset work_mem;
set enable_hashagg = off;
explain (costs off) select a, b, sum(c) from gsd group by cube(a,b);
select a, b, sum(c) from gsd group by cube(a,b) order by 1,2,3;
reset enable_hashagg;
-- issue #54: byref-initcond transtypes (avg/stddev) per-set init copies,
-- default (hashed/AGG_MIXED) strategies.
select a, avg(c), stddev_samp(c) from gsd group by grouping sets ((a),(b),()) order by 1,2,3;
select a, b, avg(c) from gsd group by rollup(a,b) order by 1,2,3;
-- fp-executor-nodeAgg-p1#1: hashed and mixed grouping sets evaluate each
-- aggregate's arguments and FILTER once per input row, shared across sets.
create sequence gsd_seq;
set enable_sort = off;
explain (costs off) select a, b, sum(nextval('gsd_seq')) from gsd group by grouping sets ((a),(b));
select count(*), sum(sum) from (select a, b, sum(nextval('gsd_seq')) from gsd group by grouping sets ((a),(b))) s;
select currval('gsd_seq');
select a, b, sum(nextval('gsd_seq')) from (values (1,1)) v(a,b) group by grouping sets ((a),(b)) order by 1,2;
select currval('gsd_seq');
reset enable_sort;
explain (costs off) select a, b, c, sum(nextval('gsd_seq')) from gsd group by grouping sets ((a),(b),(c,a),rollup(c));
select count(*), sum(sum) from (select a, b, c, sum(nextval('gsd_seq')) from gsd group by grouping sets ((a),(b),(c,a),rollup(c))) s;
select currval('gsd_seq');
select c, count(*) filter (where nextval('gsd_seq') % 2 = 0), sum(a) from gsd group by grouping sets ((c),(a),()) order by 1,2,3;
select currval('gsd_seq');
drop sequence gsd_seq;
-- fp-executor-nodeAgg-p2#2: a row whose group spills still evaluates the
-- aggregate arguments (C advance_aggregates over a NULL pergroup); the
-- exact spill count depends on memory accounting, so only the excess is pinned.
create sequence gsd_spill_seq;
set work_mem = '64kB';
set enable_sort = off;
select count(*), sum(sum) > 200010000, max(sum) > 20000 from (select i, sum(nextval('gsd_spill_seq')) from generate_series(1,20000) g(i) group by i) s;
select last_value > 20000 from gsd_spill_seq;
reset enable_sort;
reset work_mem;
drop sequence gsd_spill_seq;
drop table gsd;
