-- issue #167: LIMIT/OFFSET expressions fed by uncorrelated initplans
-- (`LIMIT (SELECT n)`): the Limit node must run the pending initplan
-- (C ExecEvalParamExec -> ExecSetParamPlan) when it recomputes the bounds.
create table li(id int primary key, v text);
insert into li select g, 'row' || g from generate_series(1, 40) g;
create table lim(n int);
insert into lim values (15);
-- LIMIT from an initplan.
select id, v from li order by id limit (select 15);
select id, v from li order by id limit (select n from lim);
-- OFFSET variant, and both together.
select id from li order by id offset (select 35);
select id from li order by id limit (select n - 12 from lim) offset (select 2);
-- NULL initplan output = LIMIT ALL / OFFSET 0.
select count(*) from (select id from li order by id limit (select null::int)) s;
-- WITH TIES through an initplan count.
select v from (values ('a',1),('b',1),('c',2)) t(v,k)
  order by k fetch first (select 1) rows with ties;
-- Correlated-adjacent control: a correlated subplan NEXT TO the limit shape
-- (regular SubPlan lane, not an initplan) still works.
select id from li o where id <= (select max(id) - 38 from li i where i.id >= o.id - 100)
  order by id limit (select 3);
-- Rescan: the initplan-fed limit under a re-executed subquery.
select k, (select count(*) from (select id from li order by id limit (select n from lim)) s
           where s.id > k * 10) as c
  from generate_series(0, 2) k order by k;
-- Inline SubPlan in the LIMIT expression (#190's second half): an
-- uncorrelated ANY sublink stays an inline SubPlan, never an initplan, so
-- ExecInitLimit must compile the expression with a SubPlan-capable parent
-- (C ExecInitExpr with the Limit planstate) and evaluation needs the
-- EEOP_SUBPLAN pump.
select id, v from li order by id limit case when 5 in (select id from li) then 3 else 5 end;
drop table lim;
drop table li;
