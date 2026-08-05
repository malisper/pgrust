-- Hashed-aggregation memory evidence vs live C: EXPLAIN ANALYZE Memory Usage
-- for a near-hash_mem hashed GROUP BY. Diffs are diagnostic, not a gate.
set work_mem = '4MB';
create table hmp as select g, (g % 50021) h from generate_series(1, 200000) g;
explain (analyze, timing off, costs off, summary off, buffers off)
  select h, count(*) from hmp group by h;
explain (analyze, timing off, costs off, summary off, buffers off)
  select g % 97, count(*) from hmp group by 1;
drop table hmp;
reset work_mem;
