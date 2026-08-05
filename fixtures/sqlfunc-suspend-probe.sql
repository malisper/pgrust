-- Abandoned suspended value-per-call scans must restart (C: ShutdownSQLFunction),
-- never resume mid-stream. Probe verdict 2026-07-04 (job pgrust-fast-tests-1d56267f8e-1783148578):
-- every intra-query rescan-abandonment shape is planner-unreachable today —
-- ProjectSet under a SubPlan loud-panics (subselect.rs finalize_plan T_ProjectSet),
-- lateral SRF subqueries loud-panic (relids "no relation entry" / execexpr scan slot):
--   SELECT x, ARRAY(SELECT probe_srf(x) LIMIT 2) FROM (VALUES (1),(5)) v(x);
--   SELECT x, s FROM (VALUES (1),(5)) v(x), LATERAL (SELECT probe_srf(x) LIMIT 2) t(s);
--   SELECT x, (SELECT p FROM probe_srf(x) p LIMIT 1) FROM (VALUES (1),(5)) v(x);
-- Re-enable those probes when ProjectSet-under-SubPlan / lateral SRF land.
-- Reachable today: top-level LIMIT abandonment; fcache dies with the query, so
-- an identical re-execution must restart from row 1.
SELECT probe_srf0() LIMIT 3;
SELECT probe_srf0() LIMIT 3;
SELECT probe_srf(6) LIMIT 2;
SELECT probe_srf(6) LIMIT 2;
SELECT probe_srf(6);
BEGIN;
SELECT probe_srf0() LIMIT 4;
SELECT probe_srf0() LIMIT 4;
COMMIT;
