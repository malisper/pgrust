-- Minimal catalog-corruption repro.
-- Expected behavior:
-- 1. `create table ... like ctlseq1` errors because LIKE on sequences is unsupported.
-- 2. `drop sequence ctlseq1` succeeds.
-- 3. A fresh `select count(*) from public.int2_tbl` should still work.
-- Actual pgrust behavior after this script:
-- 1. `public.int2_tbl` becomes `unknown table`
-- 2. even `pg_namespace` becomes `unknown table`

create sequence ctlseq1;
create table ctlt10 (like ctlseq1);
drop sequence ctlseq1;
