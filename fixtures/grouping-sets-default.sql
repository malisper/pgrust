create table gs2(a int, b int, c int);
insert into gs2 values (1,1,1),(2,2,2);
select a, sum(c) from gs2 group by rollup(a);
drop table gs2;
