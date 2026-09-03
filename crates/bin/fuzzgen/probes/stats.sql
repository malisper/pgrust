-- sitediff stats deck (plan §4.4): exact integer counters only, never
-- timings. The runner keeps the previous snapshot per side and compares
-- DELTAS (probes::stats_delta) after each DML bracket; the flush call
-- makes the cumulative stats current on both engines first.

-- name: flush
SELECT pg_stat_force_next_flush();

-- name: user_tables
SELECT schemaname, relname, seq_scan, seq_tup_read, coalesce(idx_scan, 0) AS idx_scan,
       coalesce(idx_tup_fetch, 0) AS idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
       n_tup_newpage_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
       vacuum_count, autovacuum_count, analyze_count, autoanalyze_count
  FROM pg_stat_user_tables
 ORDER BY 1, 2;

-- name: user_functions
SELECT schemaname, funcname, calls
  FROM pg_stat_user_functions
 ORDER BY 1, 2;

-- name: xact_function_calls
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) AS args,
       coalesce(pg_stat_get_xact_function_calls(p.oid), 0) AS xact_calls
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
 ORDER BY 1, 2, 3;

-- name: checksum_failures
SELECT coalesce(checksum_failures, 0) AS checksum_failures
  FROM pg_stat_database
 WHERE datname = current_database();
