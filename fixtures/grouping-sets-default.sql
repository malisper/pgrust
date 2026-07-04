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
drop table gsd;
