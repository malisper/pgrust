-- psql fidelity-gate corpus: run through real PGDG psql 18 and the Rust
-- psql, against BOTH stock PostgreSQL 18 and pgrust, stdout/stderr diffed
-- byte-for-byte (after the normalizations listed in run-gate.sh).
-- Statement mix: DDL, DML, mixed-type SELECTs with NULLs, multibyte text,
-- wide/narrow columns, empty results, errors (incl. caret clipping),
-- transactions, COPY both directions, and every implemented meta-command.

-- 1. DDL ---------------------------------------------------------------
create table t1 (id serial primary key, name text not null,
                 val numeric(10,2) default 0, tag varchar(20), created date);
create unique index t1_name_idx on t1 (name);
create table t2 (id int primary key,
                 t1_id int references t1(id),
                 note text,
                 check (id > 0));
create view v1 as select id, name from t1 where val > 0;
create sequence seq1 start 5 increment 2;
create function addone(i int) returns int as $$ select i + 1 $$ language sql;
create schema s1;
create table s1.hidden (x int);
alter table t2 add column extra float8;

-- 2. DML ---------------------------------------------------------------
insert into t1 (name, val, tag, created) values
  ('alpha', 1.50, 'a', '2024-01-01'),
  ('beta', null, null, null),
  ('多字节宽字符', 42.42, '宽', '2025-06-30'),
  ('newline
in-name', 7.00, 'nl', '2024-12-31');
insert into t2 (id, t1_id, note) values (1, 1, 'ref one'), (2, 2, null);
update t1 set val = 2.25 where name = 'beta' returning id, name, val;
delete from t2 where id = 2;

-- 3. SELECT shapes ------------------------------------------------------
select * from t1 order by id;
select id as "只", name, val from t1 order by id;
select 1 as a, 'x' as b, null as c, 2.5 as d, true as e;
select * from t1 where false;
select;
select 'wide '||repeat('W', 40) as wide_column, 1 as n;
select E'line one\nline two\nline three' as multi, 42 as answer;
table v1;
select addone(41);
select nextval('seq1'), nextval('seq1');
select * from generate_series(1,3) g(n);

-- 4. errors -------------------------------------------------------------
select * from nowhere;
select 1 + ;
select 1/0;
select aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb, cccccccccccccccccccc, no_such_col from t1;
insert into t2 (id) values (0);

-- 5. transactions -------------------------------------------------------
begin;
insert into t1 (name) values ('tx-rollback');
rollback;
begin;
insert into t1 (name) values ('tx-commit');
commit;
begin;
select bogus_in_txn;
select 'unreachable';
rollback;
select name from t1 where name like 'tx-%' order by 1;

-- 6. COPY ---------------------------------------------------------------
copy (select id, name from t1 where id <= 2 order by id) to stdout;
create table copt (a int, b text);
copy copt from stdin;
10	ten
20	twenty
\.
select * from copt order by a;

-- 7. meta-commands ------------------------------------------------------
\echo === meta section ===
\echo -n no newline...
\echo done
\set who world
\echo hello :who
select :'who' as quoted_var, 'plain :who inside quotes' as untouched;
\unset who
\echo hello :who
\x
select * from t1 where id = 3;
\x off
\x on
\x off
\a
select id, name from t1 order by id limit 2;
\a
\t
select 'tuples only' as hdr;
\t
\timing
select 1;
\timing
\dt
\dt t*
\dt "NoSuchTable"
\dt nosuch
\d
\d t1
\d t2
\d v1
\d seq1
\d t1_pkey
\d nosuch
\dt s1.*
\di
\dv
\ds
\dn
\df
\df addone
\l
\conninfo
\zzz_invalid
\?
\i gate-include.sql
-- interpolation edge: dollar-quote + comments hold semicolons
select $tag$ dollar ; quoted $tag$ as dq /* block ; comment */ -- tail ; comment
;
-- 8. async notification (PID normalized in diff)
listen chan1;
notify chan1, 'hello';
-- 9. \c to another database and back
\c gate2
select current_database();
\c gate1
select current_database();
\q
