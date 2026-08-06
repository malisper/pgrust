create table gs(a int, b int, c int);
insert into gs values (1,1,1),(1,1,2),(1,2,3),(2,1,4),(2,2,5),(2,2,6),(1,1,10),(3,1,7);
set enable_hashagg = off;
set enable_incremental_sort = off;
select a, b, sum(c) from gs group by rollup(a,b) order by a, b, sum(c);
select a, b, sum(c) from gs group by cube(a,b) order by a, b, sum(c);
select a, b, sum(c) from gs group by grouping sets ((a),(b),()) order by a, b, sum(c);
select a, b, grouping(a,b), sum(c) from gs group by rollup(a,b) order by a, b;
select a, grouping(a) as g, sum(c) from gs group by rollup(a) having sum(c) > 10 order by a;
select sum(c) from gs group by ();
select a, count(*) from gs group by grouping sets (a, rollup(b)) order by 1, 2;
select a, b, sum(c) from gs group by grouping sets ((a,b)) order by 1, 2;
explain (costs off) select a, b, sum(c) from gs group by rollup(a,b);
explain (costs off) select a, b, sum(c) from gs group by grouping sets ((a),(b),());
explain (costs off) select sum(c) from gs group by ();
explain (costs off) select a, b, grouping(a,b), sum(c) from gs group by cube(a,b);
select grouping(c) from gs group by a;
select a from gs group by grouping sets ((a),(c));
-- issue #54: byref-initcond transtypes (avg/stddev '{0,0,...}' arrays) must
-- be datumCopy'd per grouping set; aliased init states compound every set
-- into one running aggregate.
create table gs54(k text, x int);
insert into gs54 values ('g1',10),('g1',20),('g2',100);
select k, avg(x), grouping(k) from gs54 group by grouping sets ((k),()) order by 1,2,3;
select k, stddev_samp(x), var_samp(x) from gs54 group by rollup(k) order by 1,2,3;
select k, avg(x::numeric), sum(x) from gs54 group by cube(k) order by 1,2,3;
drop table gs54;
drop table gs;
