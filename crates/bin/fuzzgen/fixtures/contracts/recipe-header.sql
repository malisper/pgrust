-- id: commands/analyze/analyze-1
-- targets: ereport:analyze.c:316, ereport:analyze.c:321, ereport:analyze.c:1345
-- env: base
-- session: superuser
-- protocol: simple
-- slots: n:int4, t:table
-- provides: table(t), stats(t)
-- requires: rows(t)>=1
-- ordered: none
-- expect_c: N:INFO:analyzing "public.%s"
-- oracle: transcript
-- origin: audit
create table t(a int);
insert into t select g from generate_series(1, :n) g;
analyze verbose :t;
drop table t;
