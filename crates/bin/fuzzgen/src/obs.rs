//! Observability-probe module (Q3, sql-reachable-queue chunk
//! `observability`): the pg_stat_get_* / pg_stat_reset* / pgstat-machinery
//! SQL surface (backend/utils/adt/pgstatfuncs.c and the
//! backend/utils/activity family). gap-report-009's observability profile
//! arm hit 0/210 of these functions because the PROFILE only flips GUCs —
//! nothing in the stream ever CALLED the functions. This module emits the
//! missing statement shapes.
//!
//! The determinism discipline (every probe hand-verified byte-identical on
//! C 18.3 and pgrust before landing — docs/fuzzing/findings-q3-nodesobs.md):
//!
//!   - live counters are never projected raw: they appear only under
//!     flush-timing-immune wrappers (`x >= 0`, `count(*) > 0`, `bool_and`,
//!     `coalesce(ts >= '2000-01-01', true)`). Cumulative-stats flushes are
//!     WALL-CLOCK gated (PGSTAT_MIN_INTERVAL), so even NULL-ness of a
//!     flushable value is nondeterministic — the coalesce wrapper is the
//!     load-bearing part, not decoration;
//!   - exact values are asserted only where the source is backend-local
//!     and transaction-scoped: the pg_stat_get_xact_* family read inside
//!     one explicit transaction (`obs:xact`, `obs:func`) returns exact
//!     counts (3 inserts = 3), identically on both engines;
//!   - per-backend probes filter pg_stat_get_backend_idset() down to the
//!     session's own backend (pid = pg_backend_pid()) and aggregate, so
//!     background-worker population differences can never leak in;
//!   - SRF row populations that may legitimately differ between engines
//!     (pg_stat_get_io backend types, SLRU inventory) are compared only as
//!     `count(*) > 0` / `count(*) >= 0`;
//!   - void functions (the reset family) are exercised via `f() IS NULL`
//!     projections — deterministic `f` either way. Resetting shared stats
//!     mid-stream is safe under this module's own discipline because no
//!     probe ever reads a raw counter value.
//!
//! Groups are self-contained (`obs_`-prefixed objects created and dropped
//! in-group; every SET is RESET in-group). The one matched-error arm is
//! pg_shmem_allocations_numa (errors identically on non-NUMA hosts; on a
//! NUMA Linux pair both sides run the same host so the verdict still
//! matches).

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "obs:table",
    "obs:xact",
    "obs:func",
    "obs:db",
    "obs:global",
    "obs:backend",
    "obs:reset",
    "obs:snapshot",
    "obs:views",
    "obs:advisory",
];

/// Deterministic-projection per-table probes over the group-local table
/// (referenced via a pg_class subselect so the oid never appears in text).
const TABLE_PROBES: &[&str] = &[
    "SELECT pg_stat_get_numscans(c.oid) >= 0, pg_stat_get_tuples_returned(c.oid) >= 0, pg_stat_get_tuples_fetched(c.oid) >= 0, pg_stat_get_tuples_inserted(c.oid) >= 0, pg_stat_get_tuples_updated(c.oid) >= 0, pg_stat_get_tuples_deleted(c.oid) >= 0, pg_stat_get_tuples_hot_updated(c.oid) >= 0, pg_stat_get_live_tuples(c.oid) >= 0, pg_stat_get_dead_tuples(c.oid) >= 0 FROM pg_class c WHERE relname = 'obs_t';",
    "SELECT pg_stat_get_blocks_fetched(c.oid) >= 0, pg_stat_get_blocks_hit(c.oid) >= 0, pg_stat_get_mod_since_analyze(c.oid) >= 0, pg_stat_get_ins_since_vacuum(c.oid) >= 0, coalesce(pg_stat_get_lastscan(c.oid) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_last_vacuum_time(c.oid) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_last_analyze_time(c.oid) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_last_autovacuum_time(c.oid) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_last_autoanalyze_time(c.oid) >= '2000-01-01'::timestamptz, true) FROM pg_class c WHERE relname = 'obs_t';",
    "SELECT pg_stat_get_vacuum_count(c.oid) >= 0, pg_stat_get_autovacuum_count(c.oid) >= 0, pg_stat_get_analyze_count(c.oid) >= 0, pg_stat_get_autoanalyze_count(c.oid) >= 0, pg_stat_get_total_vacuum_time(c.oid) >= 0, pg_stat_get_total_autovacuum_time(c.oid) >= 0, pg_stat_get_total_analyze_time(c.oid) >= 0, pg_stat_get_total_autoanalyze_time(c.oid) >= 0, pg_stat_get_tuples_newpage_updated(c.oid) >= 0 FROM pg_class c WHERE relname = 'obs_t';",
];

/// Database-wide probes over the current database's pg_database row.
const DB_PROBES: &[&str] = &[
    "SELECT d.oid IS NOT NULL, coalesce(pg_stat_get_db_numbackends(d.oid) >= 0, true), coalesce(pg_stat_get_db_xact_commit(d.oid) >= 0, true), coalesce(pg_stat_get_db_xact_rollback(d.oid) >= 0, true), coalesce(pg_stat_get_db_blocks_fetched(d.oid) >= 0, true), coalesce(pg_stat_get_db_blocks_hit(d.oid) >= 0, true), coalesce(pg_stat_get_db_tuples_returned(d.oid) >= 0, true), coalesce(pg_stat_get_db_tuples_fetched(d.oid) >= 0, true), coalesce(pg_stat_get_db_tuples_inserted(d.oid) >= 0, true), coalesce(pg_stat_get_db_tuples_updated(d.oid) >= 0, true), coalesce(pg_stat_get_db_tuples_deleted(d.oid) >= 0, true) FROM pg_database d WHERE datname = current_database();",
    "SELECT coalesce(pg_stat_get_db_conflict_tablespace(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_lock(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_snapshot(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_bufferpin(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_startup_deadlock(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_logicalslot(d.oid) >= 0, true), coalesce(pg_stat_get_db_conflict_all(d.oid) >= 0, true), coalesce(pg_stat_get_db_deadlocks(d.oid) >= 0, true), coalesce(pg_stat_get_db_checksum_failures(d.oid) >= 0, true), coalesce(pg_stat_get_db_checksum_last_failure(d.oid) >= '2000-01-01'::timestamptz, true) FROM pg_database d WHERE datname = current_database();",
    "SELECT coalesce(pg_stat_get_db_temp_files(d.oid) >= 0, true), coalesce(pg_stat_get_db_temp_bytes(d.oid) >= 0, true), coalesce(pg_stat_get_db_blk_read_time(d.oid) >= 0, true), coalesce(pg_stat_get_db_blk_write_time(d.oid) >= 0, true), coalesce(pg_stat_get_db_session_time(d.oid) >= 0, true), coalesce(pg_stat_get_db_active_time(d.oid) >= 0, true), coalesce(pg_stat_get_db_idle_in_transaction_time(d.oid) >= 0, true), coalesce(pg_stat_get_db_sessions(d.oid) >= 0, true), coalesce(pg_stat_get_db_sessions_abandoned(d.oid) >= 0, true), coalesce(pg_stat_get_db_sessions_fatal(d.oid) >= 0, true), coalesce(pg_stat_get_db_sessions_killed(d.oid) >= 0, true), coalesce(pg_stat_get_db_parallel_workers_to_launch(d.oid) >= 0, true), coalesce(pg_stat_get_db_parallel_workers_launched(d.oid) >= 0, true), coalesce(pg_stat_get_db_stat_reset_time(d.oid) >= '2000-01-01'::timestamptz, true) FROM pg_database d WHERE datname = current_database();",
];

/// Cluster-global machinery probes (bgwriter/checkpointer/archiver/wal/io/
/// slru/progress + the index-AM progress phase names).
const GLOBAL_PROBES: &[&str] = &[
    "SELECT pg_stat_get_bgwriter_buf_written_clean() >= 0, pg_stat_get_bgwriter_maxwritten_clean() >= 0, coalesce(pg_stat_get_bgwriter_stat_reset_time() >= '2000-01-01'::timestamptz, true), pg_stat_get_buf_alloc() >= 0;",
    "SELECT pg_stat_get_checkpointer_num_timed() >= 0, pg_stat_get_checkpointer_num_requested() >= 0, pg_stat_get_checkpointer_num_performed() >= 0, pg_stat_get_checkpointer_restartpoints_timed() >= 0, pg_stat_get_checkpointer_restartpoints_requested() >= 0, pg_stat_get_checkpointer_restartpoints_performed() >= 0, pg_stat_get_checkpointer_buffers_written() >= 0, pg_stat_get_checkpointer_slru_written() >= 0, pg_stat_get_checkpointer_sync_time() >= 0, pg_stat_get_checkpointer_write_time() >= 0, coalesce(pg_stat_get_checkpointer_stat_reset_time() >= '2000-01-01'::timestamptz, true);",
    "SELECT (a).archived_count >= 0, (a).failed_count >= 0, coalesce((a).stats_reset >= '2000-01-01'::timestamptz, true) FROM pg_stat_get_archiver() a;",
    "SELECT (w).wal_records >= 0, (w).wal_fpi >= 0, (w).wal_bytes >= 0 FROM pg_stat_get_wal() w;",
    "SELECT count(*) > 0 FROM pg_stat_get_io();",
    "SELECT count(*) > 0 FROM pg_stat_get_slru();",
    "SELECT count(*) >= 0 FROM pg_stat_get_progress_info('VACUUM');",
    "SELECT pg_indexam_progress_phasename((SELECT oid FROM pg_am WHERE amname = 'btree'), 2) IS NOT NULL;",
    "SELECT count(*) >= 0 FROM pg_stat_get_backend_io(pg_backend_pid());",
    "SELECT count(*) >= 0 FROM pg_stat_get_backend_wal(pg_backend_pid());",
];

/// Own-backend probes: idset filtered to this session's backend, then
/// aggregated — the surrounding worker population can never leak in.
const BACKEND_PROBES: &[&str] = &[
    "SELECT count(*) > 0 FROM pg_stat_get_backend_idset() s WHERE pg_stat_get_backend_pid(s) = pg_backend_pid();",
    "SELECT bool_and(pg_stat_get_backend_dbid(s) IS NOT NULL AND pg_stat_get_backend_userid(s) IS NOT NULL AND pg_stat_get_backend_activity(s) LIKE 'SELECT%'), count(*) = 1 FROM pg_stat_get_backend_idset() s WHERE pg_stat_get_backend_pid(s) = pg_backend_pid();",
    "SELECT coalesce(pg_stat_get_backend_activity_start(s) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_backend_start(s) >= '2000-01-01'::timestamptz, true), coalesce(pg_stat_get_backend_xact_start(s) >= '2000-01-01'::timestamptz, true), pg_stat_get_backend_client_addr(s) IS NOT NULL, pg_stat_get_backend_client_port(s) > 0, coalesce(pg_stat_get_backend_subxact(s) IS NOT NULL, true) FROM pg_stat_get_backend_idset() s WHERE pg_stat_get_backend_pid(s) = pg_backend_pid();",
    "SELECT coalesce(pg_stat_get_backend_wait_event_type(s) IS NOT NULL, true), coalesce(pg_stat_get_backend_wait_event(s) IS NOT NULL, true) FROM pg_stat_get_backend_idset() s WHERE pg_stat_get_backend_pid(s) = pg_backend_pid();",
    "SELECT count(*) >= 0 FROM pg_stat_get_activity(pg_backend_pid());",
];

/// The reset/flush family: void functions projected as `f() IS NULL`
/// (constant false), plus the pg_stat_have_stats probe.
const RESET_PROBES: &[&str] = &[
    "SELECT pg_stat_force_next_flush() IS NULL;",
    "SELECT pg_stat_have_stats('database', (SELECT oid FROM pg_database WHERE datname = current_database()), 0);",
    "SELECT pg_stat_reset_single_table_counters(0) IS NULL;",
    "SELECT pg_stat_reset_single_function_counters(0) IS NULL;",
    "SELECT pg_stat_reset_slru(NULL) IS NULL;",
    "SELECT pg_stat_reset_slru('commit_timestamp') IS NULL;",
    "SELECT pg_stat_reset_shared('bgwriter') IS NULL;",
    "SELECT pg_stat_reset_shared('checkpointer') IS NULL;",
    "SELECT pg_stat_reset_shared('archiver') IS NULL;",
    "SELECT pg_stat_reset_shared('io') IS NULL;",
    "SELECT pg_stat_reset_shared('wal') IS NULL;",
    "SELECT pg_stat_reset_shared('slru') IS NULL;",
    "SELECT pg_stat_reset_shared('recovery_prefetch') IS NULL;",
    "SELECT pg_stat_reset_shared(NULL) IS NULL;",
    "SELECT pg_stat_reset() IS NULL;",
    "SELECT pg_stat_reset_backend_stats(pg_backend_pid()) IS NULL;",
    "SELECT pg_stat_reset_subscription_stats(NULL) IS NULL;",
];

/// Deterministic system-view sweeps (the shmem/wait-event/aio/slru/lock
/// surface named by the queue's reachability notes).
const VIEW_PROBES: &[&str] = &[
    "SELECT count(*) > 0 FROM pg_shmem_allocations;",
    // Matched-error arm on non-NUMA hosts (identical 0A000/XX000 text on
    // both engines of a same-host pair; hand-verified on the local rig).
    "SELECT count(*) >= 0 FROM pg_shmem_allocations_numa;",
    "SELECT count(*) > 0 FROM pg_wait_events;",
    "SELECT count(*) >= 0 FROM pg_aios;",
    "SELECT count(*) > 0 FROM pg_stat_slru;",
    "SELECT count(*) >= 0 FROM pg_stat_subscription;",
    "SELECT count(*) >= 0 FROM pg_stat_get_subscription_stats(0);",
    "SELECT count(*) > 0 FROM pg_locks WHERE pid = pg_backend_pid();",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// Random nonempty curated subset, order-preserving (stream stays seeded-
/// deterministic; subsetting keeps group sizes bounded).
fn pick_subset(g: &mut Gen, pool: &[&str], min: usize) -> Vec<StmtKind> {
    debug_assert!(min >= 1 && min <= pool.len());
    loop {
        let out: Vec<StmtKind> =
            pool.iter().filter(|_| g.rng.chance(1, 2)).map(|s| raw(*s)).collect();
        if out.len() >= min {
            return out;
        }
    }
}

fn body(g: &mut Gen, shape: &str) -> Vec<StmtKind> {
    match shape {
        "obs:table" => {
            let mut out = vec![
                raw("CREATE TABLE obs_t (a int PRIMARY KEY, b text);"),
                raw("INSERT INTO obs_t SELECT g, 'x' || g FROM generate_series(1, 20) g;"),
                raw("SELECT count(*) FROM obs_t;"),
            ];
            out.extend(pick_subset(g, TABLE_PROBES, 1));
            if g.rng.chance(1, 2) {
                g.fire("obs:table:reset");
                out.push(raw(
                    "SELECT pg_stat_reset_single_table_counters((SELECT oid FROM pg_class WHERE relname = 'obs_t')) IS NULL;",
                ));
            }
            out.push(raw("DROP TABLE obs_t;"));
            out
        }
        "obs:xact" => vec![
            raw("CREATE TABLE obs_xt (a int PRIMARY KEY);"),
            raw("BEGIN;"),
            raw("INSERT INTO obs_xt VALUES (1), (2), (3);"),
            raw("UPDATE obs_xt SET a = a + 10 WHERE a = 1;"),
            raw("DELETE FROM obs_xt WHERE a = 2;"),
            // Backend-local, transaction-scoped: exact counts compare.
            raw("SELECT pg_stat_get_xact_tuples_inserted(c.oid), pg_stat_get_xact_tuples_updated(c.oid), pg_stat_get_xact_tuples_deleted(c.oid), pg_stat_get_xact_numscans(c.oid) >= 0, pg_stat_get_xact_tuples_returned(c.oid) >= 0, pg_stat_get_xact_tuples_fetched(c.oid) >= 0, pg_stat_get_xact_tuples_hot_updated(c.oid) >= 0, pg_stat_get_xact_blocks_fetched(c.oid) >= 0, pg_stat_get_xact_blocks_hit(c.oid) >= 0, pg_stat_get_xact_tuples_newpage_updated(c.oid) >= 0 FROM pg_class c WHERE relname = 'obs_xt';"),
            raw("COMMIT;"),
            raw("DROP TABLE obs_xt;"),
        ],
        "obs:func" => vec![
            raw("CREATE FUNCTION obs_f(int) RETURNS int LANGUAGE plpgsql AS 'begin return $1 * 2; end';"),
            raw("SET track_functions TO 'all';"),
            raw("BEGIN;"),
            raw("SELECT obs_f(1), obs_f(2);"),
            raw("SELECT obs_f(3);"),
            // Exact in-transaction call count (3); times wrapped >= 0.
            raw("SELECT pg_stat_get_xact_function_calls(p.oid), pg_stat_get_xact_function_self_time(p.oid) >= 0, pg_stat_get_xact_function_total_time(p.oid) >= 0 FROM pg_proc p WHERE proname = 'obs_f';"),
            raw("COMMIT;"),
            raw("SELECT coalesce(pg_stat_get_function_calls(p.oid) >= 0, true), coalesce(pg_stat_get_function_self_time(p.oid) >= 0, true), coalesce(pg_stat_get_function_total_time(p.oid) >= 0, true) FROM pg_proc p WHERE proname = 'obs_f';"),
            raw("RESET track_functions;"),
            raw("DROP FUNCTION obs_f(int);"),
        ],
        "obs:db" => pick_subset(g, DB_PROBES, 1),
        "obs:global" => pick_subset(g, GLOBAL_PROBES, 2),
        "obs:backend" => pick_subset(g, BACKEND_PROBES, 1),
        "obs:reset" => pick_subset(g, RESET_PROBES, 2),
        "obs:snapshot" => vec![
            raw("SET stats_fetch_consistency TO snapshot;"),
            raw("SELECT pg_stat_get_db_xact_commit(d.oid) >= 0 FROM pg_database d WHERE datname = current_database();"),
            // A wide scan builds a big snapshot hash (pgstat_snapshot_grow)
            // that the trailing clear releases (pgstat_snapshot_free).
            raw("SELECT count(*) >= 0 FROM pg_stat_all_tables;"),
            raw("SELECT coalesce(pg_stat_get_snapshot_timestamp() >= '2000-01-01'::timestamptz, true);"),
            raw("SELECT pg_stat_clear_snapshot() IS NULL;"),
            raw("RESET stats_fetch_consistency;"),
        ],
        "obs:views" => pick_subset(g, VIEW_PROBES, 2),
        "obs:advisory" => {
            let k = 100 + g.rng.below(100);
            vec![
                raw(format!("SELECT pg_advisory_lock({});", k)),
                raw("SELECT count(*) > 0 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid();"),
                raw(format!("SELECT pg_advisory_unlock({});", k)),
            ]
        }
        other => unreachable!("unknown obs shape {other}"),
    }
}

/// Registry entry point (stmt::STMT_MODULES): one observability probe group.
pub fn gen_obs_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("obs");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    body(g, shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            groups.push(
                gen_obs_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    /// The volatility gate: no probe ever projects a raw counter — every
    /// pg_stat_get_* call site in a SELECT list is wrapped (`>= 0` /
    /// `IS NULL` / `IS NOT NULL` / coalesce / count / bool aggregation),
    /// SETs are RESET in-group, and created objects are dropped in-group.
    #[test]
    fn probes_are_wrapped_and_groups_self_contained() {
        let (groups, prods) = gen_groups(0x0B5, 900, &WeightTable::defaults());
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
            }
            // SET/RESET pairing.
            let sets: Vec<&str> = group
                .iter()
                .filter_map(|s| s.strip_prefix("SET ").and_then(|r| r.split(' ').next()))
                .collect();
            for name in &sets {
                assert!(
                    group.iter().any(|s| *s == format!("RESET {};", name)),
                    "SET {name} never RESET: {group:?}"
                );
            }
            // Self-containment.
            for (create, drop) in
                [("CREATE TABLE", "DROP TABLE"), ("CREATE FUNCTION", "DROP FUNCTION")]
            {
                let c = group.iter().filter(|s| s.starts_with(create)).count();
                let d = group.iter().filter(|s| s.starts_with(drop)).count();
                assert_eq!(c, d, "{create} without {drop}: {group:?}");
            }
            let opens = group.iter().filter(|s| *s == "BEGIN;").count();
            let closes = group.iter().filter(|s| *s == "COMMIT;").count();
            assert_eq!(opens, closes, "unbalanced txn bracket: {group:?}");
            // Wrapper discipline: every statement mentioning a stat getter
            // carries at least one deterministic wrapper form, and the
            // known-volatile raw projections never appear bare.
            for sql in group {
                if sql.contains("pg_stat_get_") && sql.starts_with("SELECT") {
                    let wrapped = sql.contains(">= 0")
                        || sql.contains("IS NULL")
                        || sql.contains("IS NOT NULL")
                        || sql.contains("coalesce(")
                        || sql.contains("count(*)")
                        || sql.contains("pg_stat_get_xact_")
                        || sql.contains("pg_stat_have_stats");
                    assert!(wrapped, "unwrapped stat probe: {sql}");
                }
                // Raw wall-clock values must never be a bare output column.
                for volatile in ["SELECT pg_stat_get_snapshot_timestamp()"] {
                    assert!(!sql.starts_with(volatile), "raw volatile projection: {sql}");
                }
            }
        }
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        assert!(prods.iter().any(|q| q == "obs:table:reset"));
    }

    /// Under an exclusive weight every curated probe of every pool fires
    /// (subsetting never starves an entry).
    #[test]
    fn every_curated_probe_fires() {
        for (shape, pool) in [
            ("obs:db", DB_PROBES),
            ("obs:global", GLOBAL_PROBES),
            ("obs:backend", BACKEND_PROBES),
            ("obs:reset", RESET_PROBES),
            ("obs:views", VIEW_PROBES),
            ("obs:table", TABLE_PROBES),
        ] {
            let spec: String = SHAPES
                .iter()
                .map(|s| format!("{}={}", s, if *s == shape { 1 } else { 0 }))
                .collect::<Vec<_>>()
                .join(",");
            let w = WeightTable::parse(&spec).unwrap();
            let (groups, _) = gen_groups(0xE55, 300, &w);
            for probe in pool {
                assert!(
                    groups.iter().flatten().any(|s| s == probe),
                    "{shape} probe never fired: {probe}"
                );
            }
        }
    }

    #[test]
    fn obs_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(5, 120, &w);
        let (b, _) = gen_groups(5, 120, &w);
        assert_eq!(a, b);
        let (c, _) = gen_groups(6, 120, &w);
        assert_ne!(a, c);
    }
}
