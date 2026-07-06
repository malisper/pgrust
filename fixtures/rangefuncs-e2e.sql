-- rangefuncs conformance lane corpus: function-RTE whole-row aliasing,
-- coldeflist coercion + mismatch errors, OUT-param records, SQL SRF
-- inlining plan shapes, composite drift after ALTER TABLE.
select row_to_json(s.*) from generate_series(11,14) with ordinality s;
select row_to_json(s.*) from generate_series(11,12) s;
select row_to_json(s.*) from (values (3),(4)) s(x);
explain (verbose, costs off) select row_to_json(s.*) from generate_series(11,14) with ordinality s;
select row_to_json(s.*) from rows from(generate_series(1,2), generate_series(5,6)) s;
select row_to_json(s.*) from unnest(array[7,8]) with ordinality s;
select row_to_json(s.*) from generate_series(11,12) with ordinality s(a,b);
create function ats(anyarray) returns setof record as $$
  select i AS "index", $1[i] AS "value" from generate_subscripts($1, 1) i
$$ language sql strict immutable;
select * from ats(array['one', 'two']) as t(f1 int,f2 text);
select * from ats(array['one', 'two']) as t(f1 numeric(4,2),f2 text);
select * from ats(array['one', 'two']) as t(f1 point,f2 text);
explain (verbose, costs off) select * from ats(array['one', 'two']) as t(f1 numeric(4,2),f2 text);
create or replace function ats(anyarray) returns setof record as $$
  select i AS "index", $1[i] AS "value" from generate_subscripts($1, 1) i
$$ language sql immutable;
select * from ats(array['one', 'two']) as t(f1 numeric(4,2),f2 text);
select * from ats(array['one', 'two']) as t(f1 point,f2 text);
explain (verbose, costs off) select * from ats(array['one', 'two']) as t(f1 numeric(4,2),f2 text);
drop function ats(anyarray);
create function rfb(out integer, out numeric) as $$ select (1, 2.1) $$ language sql;
select * from rfb();
create or replace function rfb(out integer, out numeric) as $$ select (1, 2) $$ language sql;
select * from rfb();
create or replace function rfb(out integer, out numeric) as $$ select (1, 2.1, 3) $$ language sql;
select * from rfb();
drop function rfb();
create function trf() returns table(f1 numeric(35,6), f2 numeric(35,2)) as $$
  select 7.136178319899999964, 7.136178319899999964;
$$ language sql immutable;
explain (verbose, costs off) select * from trf();
select * from trf();
drop function trf();
create type tr_t as (f1 numeric(35,6), f2 numeric(35,2));
create function trf2() returns setof tr_t as $$
  select 1, 2 union all select 3, 4 order by 1;
$$ language sql immutable;
explain (verbose, costs off) select * from trf2();
select * from trf2();
drop function trf2();
drop type tr_t;
create table e2eusers (userid text, seq int, email text, todrop bool, moredrop int, enabled bool);
insert into e2eusers values ('id',1,'email',true,11,true);
insert into e2eusers values ('id2',2,'email2',true,12,true);
alter table e2eusers drop column todrop;
create function get_e2eusers() returns setof e2eusers as $$ select * from e2eusers $$ language sql stable;
create view e2eusersview as select * from get_e2eusers() with ordinality;
select * from e2eusersview;
alter table e2eusers add column junk text;
select * from e2eusersview;
begin;
alter table e2eusers drop column junk;
alter table e2eusers drop column moredrop;
select * from e2eusersview;
rollback;
begin;
alter table e2eusers drop column junk;
alter table e2eusers alter column seq type numeric;
select * from e2eusersview;
rollback;
drop view e2eusersview;
drop function get_e2eusers();
drop table e2eusers;
create table e2eint8 (q1 int8, q2 int8);
insert into e2eint8 values (123, 456), (4567890123456789, -4567890123456789);
create function xq2(t e2eint8) returns int8 as $$ select t.q2 $$ language sql immutable;
create function xq2_2(e2eint8) returns table(ret1 int8) as $$ select xq2(rw) from (select $1 as rw) ss $$ language sql immutable;
explain (verbose, costs off) select x from e2eint8, xq2_2(e2eint8) f(x);
select x from e2eint8, xq2_2(e2eint8) f(x);
create function xq2_opt(e2eint8) returns table(ret1 int8) as $$ select $1.q2 $$ language sql immutable;
explain (verbose, costs off) select x from e2eint8, xq2_opt(e2eint8) f(x);
select x from e2eint8, xq2_opt(e2eint8) f(x);
drop function xq2_2(e2eint8);
drop function xq2_opt(e2eint8);
drop function xq2(t e2eint8);
drop table e2eint8;
with a(b) as (values (row(1,2,3)))
select * from a, coalesce(b) as c(d int, e int);
with a(b) as (values (row(1,2,3)))
select * from a, coalesce(b) as c(d int, e int, f float);
create function rngfuncbar_v() returns setof varchar as $$ select 'foo'::varchar union all select 'bar'::varchar $$ language sql immutable;
explain (verbose, costs off) select * from rngfuncbar_v();
select * from rngfuncbar_v();
drop function rngfuncbar_v();
