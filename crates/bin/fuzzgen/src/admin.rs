//! Q5 admin-funcs module: server administration SQL functions — the
//! `admin-funcs` chunk of docs/fuzzing/sql-reachable-queue.tsv (genfile.c,
//! dbsize.c, xlogfuncs.c/xlog.c, walsummary.c/blkreftable.c, snapmgr.c,
//! signalfuncs.c, pg_controldata.c, hbafuncs.c, mcxtfuncs.c).
//!
//! Determinism is the whole challenge here: most of these functions return
//! environment-specific data (sizes, LSNs, paths, directory contents), so
//! every family projects its output onto an engine-stable compare surface
//! before it reaches the differ. The projection disciplines (hand-verified
//! byte-identical on both engines 2026-08-11, scratch decks hv1-hv5 in the
//! Q5 lane; A = pinned cpg-ref REL_18_3, B = pgrust @ origin/main):
//!
//!   - object SIZES are never compared raw (the two engines store data
//!     differently — a size mismatch is rig noise, not a finding): size
//!     functions run for coverage but are wrapped in boolean probes
//!     (`>= 0`, `IS NULL` for the missing-oid NULL path, cross-function
//!     inequalities like total >= relation that hold on any engine);
//!   - `pg_size_pretty`/`pg_size_bytes`/`pg_column_size` over FIXED
//!     literals are deterministic and compared raw (verified identical,
//!     incl. `pg_column_size` — same tuple layout both engines);
//!   - directory listings are projected to `count(*) > 0` / `count(*) >= 0`
//!     probes or fixed-name existence booleans (`'PG_VERSION' IN ...`);
//!     `pg_ls_waldir()`'s FIRST segment name is deterministic on the fresh
//!     rig pair (verified) but later segments are not, so only bounded
//!     probes are emitted;
//!   - file CONTENT is comparable when the stream itself creates the file:
//!     the file family first COPYies a fixed generate_series stream to a
//!     datadir-relative fixture file, then reads it back (full, sliced,
//!     binary) and compares raw; `pg_stat_file` compares only the
//!     deterministic columns (size, isdir);
//!   - LSN-bearing functions compare structure, not values (`IS NOT NULL`,
//!     monotonic inequalities); `pg_walfile_name`/`pg_walfile_name_offset`
//!     /`pg_wal_lsn_diff` over FIXED literals are deterministic (timeline 1
//!     on both fresh clusters) and compared raw;
//!   - not-in-recovery is a rich MATCHED surface: replay/promote functions
//!     error identically and the `pg_last_*` probes return NULL identically
//!     on a primary (all compared raw);
//!   - the WAL-summary bracket flips `summarize_wal` on via ALTER SYSTEM +
//!     pg_reload_conf (identically on both sides), feeds the summarizer
//!     (INSERT + pg_switch_wal + CHECKPOINT), probes through timing-proof
//!     `count(*) >= 0` projections (summary AVAILABILITY is asynchronous —
//!     never compared), and RESETs the GUC in the same group;
//!   - signal functions only ever target pid 1 (never a real backend: a
//!     self-cancel races its own delivery) — returns false identically;
//!     `pg_log_backend_memory_contexts(pg_backend_pid())` logs server-side
//!     only and returns true (verified matched);
//!   - `pg_control_*` compare only the layout/constant/primary-quiescent
//!     columns (block sizes, versions, timeline 1, recovery zeros) — never
//!     LSNs, times, or the system identifier; `pg_hba_file_rules` compares
//!     everything except `file_name` (the datadir path) — both clusters
//!     initdb from the same C reference so the rule set is identical;
//!   - snapshot-import error paths (`SET TRANSACTION SNAPSHOT` with bogus /
//!     absent identifiers, wrong isolation level, not-first-query) are
//!     matched errors; the exported snapshot NAME is per-engine and only
//!     ever probed `IS NOT NULL` (cross-engine import is impossible by
//!     construction — the differ replays one text on both sides).
//!
//! Deliberate error fuel rides an `admin:ok`/`admin:err` weight pair biased
//! away from both-sides-error per the findings-budget rule. Known
//! message-text (not SQLSTATE) divergence banked in the lane findings doc:
//! `pg_relation_size(rel, 'bogus')` spells the invalid-fork ereport
//! differently (both 22023, so the differ matches it).

use crate::stmt::{Gen, StmtKind};

/// Statement shapes (top-level weighted pick).
const SHAPES: &[&str] = &[
    "admin:size",
    "admin:pretty",
    "admin:file",
    "admin:ls",
    "admin:wal",
    "admin:backup",
    "admin:recovery",
    "admin:snap",
    "admin:sig",
    "admin:control",
    "admin:summ",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_admin_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin");
    match g.weights.pick(g.rng, SHAPES) {
        "admin:size" => gen_size(g),
        "admin:pretty" => gen_pretty(g),
        "admin:file" => gen_file(g),
        "admin:ls" => gen_ls(g),
        "admin:wal" => gen_wal(g),
        "admin:backup" => gen_backup(g),
        "admin:recovery" => gen_recovery(g),
        "admin:snap" => gen_snap(g),
        "admin:sig" => gen_sig(g),
        "admin:control" => gen_control(g),
        "admin:summ" => gen_summ(g),
        other => unreachable!("unknown admin shape {other}"),
    }
}

/// One-knob error-fuel bias: err arms host deliberate matched errors.
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["admin:ok", "admin:err"]) == "admin:err" {
        g.fire("admin:err");
        true
    } else {
        false
    }
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

// ------------------------------------------------------------- family 1 ----
// Object sizes (dbsize.c): run for coverage, compare booleans only.

fn gen_size(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:size");
    let err = err_arm(g);
    let t = g.pick_table().name.clone();
    let sql = match g.rng.below(8) {
        0 => {
            if err {
                // Matched 22023 (message text differs, SQLSTATE matches —
                // banked as a lane observation, not differ-visible).
                format!("SELECT pg_relation_size('{}', 'bogus');", t)
            } else {
                let fork = pick_str(g, &["main", "vm", "fsm", "init"]);
                format!("SELECT pg_relation_size('{}', '{}') >= 0;", t, fork)
            }
        }
        1 => format!(
            "SELECT pg_relation_size('{}') >= 0, pg_table_size('{}') >= 0, pg_indexes_size('{}') >= 0;",
            t, t, t
        ),
        2 => format!(
            "SELECT pg_total_relation_size('{}') >= pg_relation_size('{}');",
            t, t
        ),
        3 => {
            if err {
                "SELECT pg_database_size('fz_no_such_db');".to_string()
            } else if g.rng.chance(1, 2) {
                "SELECT pg_database_size(current_database()) > 0;".to_string()
            } else {
                "SELECT pg_database_size((SELECT oid FROM pg_database WHERE datname = current_database())) > 0;".to_string()
            }
        }
        4 => {
            if g.rng.chance(1, 2) {
                "SELECT pg_tablespace_size('pg_default') >= 0;".to_string()
            } else {
                "SELECT pg_tablespace_size((SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')) >= 0;".to_string()
            }
        }
        5 => {
            // Missing-oid NULL path: deterministic NULL on both engines.
            "SELECT pg_relation_size(0) IS NULL, pg_table_size(0) IS NULL, pg_total_relation_size(0) IS NULL, pg_indexes_size(0) IS NULL;".to_string()
        }
        6 => {
            // relfilenode machinery; the filenode VALUES differ, the
            // regclass round-trip text does not.
            match g.rng.below(3) {
                0 => format!(
                    "SELECT pg_relation_filenode('{}') IS NOT NULL, pg_relation_filepath('{}') IS NOT NULL;",
                    t, t
                ),
                1 => format!(
                    "SELECT pg_filenode_relation(0, pg_relation_filenode('{}'))::text;",
                    t
                ),
                _ => "SELECT pg_relation_filenode('pg_class_oid_index') IS NOT NULL;".to_string(),
            }
        }
        _ => {
            let f = pick_str(g, &["pg_table_size", "pg_indexes_size", "pg_total_relation_size"]);
            format!("SELECT {}('{}'::regclass) >= 0;", f, t)
        }
    };
    raw(sql)
}

// ------------------------------------------------------------- family 2 ----
// pg_size_pretty / pg_size_bytes / pg_column_size over fixed literals:
// fully deterministic, compared raw (hand-verified identical).

const PRETTY_INT8: &[&str] = &[
    "0", "999", "10239", "10240", "-10241", "1048576", "1073741823", "1099511627776",
    "9223372036854775807", "-9223372036854775808", "5497558138880",
];
const PRETTY_NUM: &[&str] = &[
    "1234567890.5", "'NaN'", "10485760000000000000000", "-0.5", "10240.49", "10240.5",
];
const SIZE_BYTES: &[&str] = &[
    "'1kB'", "'512 MB'", "'-1 PB'", "'1e6 bytes'", "'1.5 GB'", "'.5TB'", "'  100 '", "'3'",
    "'0 TB'", "'-1e-3 MB'",
];

fn gen_pretty(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:pretty");
    let err = err_arm(g);
    let sql = match g.rng.below(4) {
        0 => {
            let a = pick_str(g, PRETTY_INT8);
            let b = pick_str(g, PRETTY_NUM);
            format!(
                "SELECT pg_size_pretty({}::bigint), pg_size_pretty({}::numeric);",
                a, b
            )
        }
        1 => {
            if err {
                format!(
                    "SELECT pg_size_bytes('{}');",
                    pick_str(g, &["bogus", "100 xB", "12.4 kB junk", ""])
                )
            } else {
                let a = pick_str(g, SIZE_BYTES);
                let b = pick_str(g, SIZE_BYTES);
                format!("SELECT pg_size_bytes({}), pg_size_bytes({});", a, b)
            }
        }
        2 => {
            let lit = pick_str(g, &[
                "1::int2", "1::int4", "1::int8", "1.5::float8", "'abc'::text", "''::text",
                "NULL::text", "row(1, 'x')", "ARRAY[1,2,3]", "'2020-01-01'::date",
                "repeat('x', 5000)", "'{\"a\": 1}'::jsonb",
            ]);
            format!("SELECT pg_column_size({});", lit)
        }
        _ => {
            // Compression probes over literals: never stored, NULL both
            // sides; chunk-id NULL for non-toasted fixture values.
            if g.rng.chance(1, 2) {
                "SELECT pg_column_compression('abc'::text) IS NULL, pg_column_compression(repeat('x', 5000)) IS NULL;".to_string()
            } else {
                let t = g.pick_table();
                let tname = t.name.clone();
                let c = t.columns[t.columns.len() - 1].name.clone();
                format!(
                    "SELECT count(*) FROM {} WHERE pg_column_compression({}) IS NOT NULL;",
                    tname, c
                )
            }
        }
    };
    raw(sql)
}

// ------------------------------------------------------------- family 3 ----
// genfile.c reads over a stream-created fixture file: content compared
// raw; error fuel is matched (missing file, traversal, bad offsets).

const FIXTURE: &str = "fz_q5_admin.dat";

fn gen_file(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:file");
    let err = err_arm(g);
    // The fixture write is datadir-relative and byte-deterministic; every
    // group rewrites it so a shared-cluster corpus rig stays consistent.
    let copy = StmtKind::Raw(format!(
        "COPY (SELECT 'fz-q5-line-' || i FROM generate_series(1, 20) i) TO '{}';",
        FIXTURE
    ));
    let read = if err {
        match g.rng.below(5) {
            0 => "SELECT pg_read_file('fz_q5_no_such');".to_string(),
            1 => "SELECT pg_read_file('/etc/passwd');".to_string(),
            2 => "SELECT pg_read_file('../outside.txt');".to_string(),
            3 => format!("SELECT pg_read_file('{}', -5, 10);", FIXTURE),
            _ => format!("SELECT pg_read_file('{}', 0, -1);", FIXTURE),
        }
    } else {
        match g.rng.below(8) {
            0 => format!("SELECT pg_read_file('{}');", FIXTURE),
            1 => {
                let off = g.rng.below(30);
                let len = 1 + g.rng.below(40);
                format!("SELECT pg_read_file('{}', {}, {});", FIXTURE, off, len)
            }
            2 => format!("SELECT pg_read_file('{}', 260, 100);", FIXTURE),
            3 => format!("SELECT pg_read_binary_file('{}')::text;", FIXTURE),
            4 => {
                let off = g.rng.below(30);
                let len = 1 + g.rng.below(40);
                format!(
                    "SELECT pg_read_binary_file('{}', {}, {})::text;",
                    FIXTURE, off, len
                )
            }
            5 => "SELECT pg_read_file('fz_q5_no_such', true) IS NULL, pg_read_binary_file('fz_q5_no_such', 0, 10, true) IS NULL;".to_string(),
            6 => format!("SELECT size, isdir FROM pg_stat_file('{}');", FIXTURE),
            _ => "SELECT isdir FROM pg_stat_file('base');".to_string(),
        }
    };
    vec![copy, StmtKind::Raw(read)]
}

// ------------------------------------------------------------- family 4 ----
// Directory listings projected to deterministic predicates.

fn gen_ls(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:ls");
    let err = err_arm(g);
    let sql = if err {
        if g.rng.chance(1, 2) {
            "SELECT pg_ls_dir('no_such_dir');".to_string()
        } else {
            "SELECT pg_ls_replslotdir('no_such_slot');".to_string()
        }
    } else {
        match g.rng.below(9) {
            0 => "SELECT count(*) > 0 FROM pg_ls_dir('.') a;".to_string(),
            1 => "SELECT count(*) > 0 FROM pg_ls_dir('.', false, false) a;".to_string(),
            2 => format!(
                "SELECT '{}' IN (SELECT pg_ls_dir('.'));",
                pick_str(g, &["PG_VERSION", "postgresql.conf", "base", "pg_wal"])
            ),
            3 => "SELECT count(*) FROM pg_ls_dir('no_such_dir', true, false) a;".to_string(),
            4 => "SELECT count(*) > 0 FROM pg_ls_waldir() a;".to_string(),
            5 => {
                if g.rng.chance(1, 2) {
                    "SELECT count(*) >= 0 FROM pg_ls_tmpdir() a;".to_string()
                } else {
                    "SELECT count(*) >= 0 FROM pg_ls_tmpdir((SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')) a;".to_string()
                }
            }
            6 => {
                let f = pick_str(g, &[
                    "pg_ls_archive_statusdir", "pg_ls_logicalmapdir", "pg_ls_logicalsnapdir",
                    "pg_ls_summariesdir",
                ]);
                format!("SELECT count(*) >= 0 FROM {}() a;", f)
            }
            7 => "SELECT count(*) > 0 FROM pg_ls_dir('base') a;".to_string(),
            _ => "SELECT count(*) FROM pg_ls_logdir() a;".to_string(),
        }
    };
    raw(sql)
}

// ------------------------------------------------------------- family 5 ----
// WAL position/name functions: fixed-literal forms compared raw; live
// LSNs compared structurally.

fn gen_wal(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:wal");
    let err = err_arm(g);
    let sql = if err {
        match g.rng.below(3) {
            0 => "SELECT * FROM pg_split_walfile_name('bogus');".to_string(),
            1 => format!("SELECT pg_create_restore_point(repeat('x', {}));", 80 + g.rng.below(60)),
            _ => "SELECT pg_walfile_name('0/0'::pg_lsn);".to_string(),
        }
    } else {
        match g.rng.below(8) {
            0 => "SELECT pg_current_wal_lsn() IS NOT NULL, pg_current_wal_insert_lsn() >= pg_current_wal_lsn(), pg_current_wal_lsn() >= pg_current_wal_flush_lsn();".to_string(),
            1 => {
                let lsn = pick_str(g, &["A/1000000", "5/7B968D8", "0/1", "FFFFFFFF/FFFFFF00"]);
                format!("SELECT pg_walfile_name('{}'::pg_lsn);", lsn)
            }
            2 => {
                let lsn = pick_str(g, &["5/7B968D8", "A/1000000", "0/2000001"]);
                format!("SELECT * FROM pg_walfile_name_offset('{}'::pg_lsn);", lsn)
            }
            3 => {
                let name = pick_str(g, &[
                    "000000010000000A00000059", "000000010000000000000001", "0000000F00000001000000FF",
                ]);
                format!("SELECT * FROM pg_split_walfile_name('{}');", name)
            }
            4 => {
                let a = pick_str(g, &["0/2000000", "A/1000000", "0/0"]);
                let b = pick_str(g, &["A/1000000", "0/12345", "FFFFFFFF/0"]);
                format!(
                    "SELECT pg_wal_lsn_diff('{}'::pg_lsn, '{}'::pg_lsn);",
                    a, b
                )
            }
            5 => "SELECT pg_switch_wal() IS NOT NULL;".to_string(),
            6 => "SELECT pg_create_restore_point('fz_q5_rp') IS NOT NULL;".to_string(),
            _ => "SELECT pg_log_standby_snapshot() IS NOT NULL;".to_string(),
        }
    };
    raw(sql)
}

// ------------------------------------------------------------- family 6 ----
// Backup bracket: start + stop in one group (exclusive resource), plus
// the matched stop-without-start / double-start error fuel.

fn gen_backup(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:backup");
    if err_arm(g) {
        // Stop without a running backup: matched 55000.
        return raw("SELECT pg_backup_stop(false);".to_string());
    }
    let fast = if g.rng.chance(1, 2) { "true" } else { "false" };
    let mut out = vec![StmtKind::Raw(format!(
        "SELECT lsn IS NOT NULL FROM pg_backup_start('fz_q5_backup', {}) lsn;",
        fast
    ))];
    if g.rng.chance(1, 4) {
        // Double start inside the bracket: matched "already in progress".
        out.push(StmtKind::Raw(
            "SELECT pg_backup_start('fz_q5_dup', true);".to_string(),
        ));
    }
    out.push(StmtKind::Raw(
        "SELECT lsn IS NOT NULL, length(labelfile) > 0, spcmapfile IS NULL FROM pg_backup_stop(false);"
            .to_string(),
    ));
    out
}

// ------------------------------------------------------------- family 7 ----
// Not-in-recovery surface: matched errors + deterministic NULL/false
// probes on a primary.

fn gen_recovery(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:recovery");
    let sql = match g.rng.below(6) {
        0 => "SELECT pg_is_in_recovery();".to_string(),
        1 => "SELECT pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn() IS NULL, pg_last_xact_replay_timestamp() IS NULL;".to_string(),
        2 => "SELECT pg_get_wal_replay_pause_state();".to_string(),
        // The rest are matched not-in-recovery errors (55000).
        3 => "SELECT pg_is_wal_replay_paused();".to_string(),
        4 => format!(
            "SELECT {}();",
            *[
                "pg_wal_replay_pause",
                "pg_wal_replay_resume",
            ]
            .get(g.rng.below_usize(2))
            .unwrap()
        ),
        _ => "SELECT pg_promote();".to_string(),
    };
    raw(sql)
}

// ------------------------------------------------------------- family 8 ----
// Snapshot export/import (snapmgr.c): export probed IS NOT NULL, import
// exercised through its matched error paths only (names are per-engine).

fn gen_snap(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:snap");
    match g.rng.below(5) {
        0 => raw("SELECT pg_export_snapshot() IS NOT NULL;".to_string()),
        1 => vec![
            // Openers stay the literal "BEGIN;" (the stream-wide bracket
            // invariant counts that exact spelling); isolation comes from
            // SET TRANSACTION, which is what the docs' import flow uses.
            StmtKind::Raw("BEGIN;".to_string()),
            StmtKind::Raw("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string()),
            StmtKind::Raw("SELECT pg_export_snapshot() IS NOT NULL;".to_string()),
            StmtKind::Raw("COMMIT;".to_string()),
        ],
        2 => {
            // Identifier parse/lookup error paths (22023): bogus format,
            // well-formed-but-absent, out-of-range xid spelling.
            let ident = pick_str(g, &["bogus", "00000003-00000002-1", "FFFFFFFF-FFFFFFFF-1", "1-2"]);
            let iso = pick_str(g, &["REPEATABLE READ", "SERIALIZABLE"]);
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                StmtKind::Raw(format!("SET TRANSACTION ISOLATION LEVEL {};", iso)),
                StmtKind::Raw(format!("SET TRANSACTION SNAPSHOT '{}';", ident)),
                StmtKind::Raw("ROLLBACK;".to_string()),
            ]
        }
        3 => vec![
            // Wrong isolation level: matched error.
            StmtKind::Raw("BEGIN;".to_string()),
            StmtKind::Raw("SET TRANSACTION SNAPSHOT '00000003-00000002-1';".to_string()),
            StmtKind::Raw("ROLLBACK;".to_string()),
        ],
        _ => vec![
            // Not-first-query: matched error.
            StmtKind::Raw("BEGIN;".to_string()),
            StmtKind::Raw("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string()),
            StmtKind::Raw("SELECT 1;".to_string()),
            StmtKind::Raw("SET TRANSACTION SNAPSHOT '00000003-00000002-1';".to_string()),
            StmtKind::Raw("ROLLBACK;".to_string()),
        ],
    }
}

// ------------------------------------------------------------- family 9 ----
// Signal functions (signalfuncs.c) + memory-context logging (mcxtfuncs.c).
// Targets are pid 1 (never a PostgreSQL process) or the own backend for
// the log function only — self-signaling a cancel races its delivery.

fn gen_sig(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:sig");
    let sql = match g.rng.below(6) {
        0 => "SELECT pg_reload_conf();".to_string(),
        1 => "SELECT pg_cancel_backend(1);".to_string(),
        2 => {
            if g.rng.chance(1, 2) {
                "SELECT pg_terminate_backend(1);".to_string()
            } else {
                "SELECT pg_terminate_backend(1, 25);".to_string()
            }
        }
        3 => "SELECT pg_cancel_backend(999999999);".to_string(),
        4 => {
            // Negative timeout: matched 22023.
            "SELECT pg_terminate_backend(pg_backend_pid(), -1);".to_string()
        }
        _ => {
            if g.rng.chance(1, 3) {
                "SELECT pg_log_backend_memory_contexts(1);".to_string()
            } else {
                "SELECT pg_log_backend_memory_contexts(pg_backend_pid());".to_string()
            }
        }
    };
    raw(sql)
}

// ------------------------------------------------------------ family 10 ----
// Control-file probes (pg_controldata.c) + hba/ident views (hbafuncs.c):
// layout constants and the identical initdb'd rule set compared raw.

fn gen_control(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:control");
    let sql = match g.rng.below(6) {
        0 => "SELECT max_data_alignment, database_block_size, blocks_per_segment, wal_block_size, bytes_per_wal_segment, max_identifier_length, max_index_columns, max_toast_chunk_size, large_object_chunk_size, float8_pass_by_value, data_page_checksum_version FROM pg_control_init();".to_string(),
        1 => "SELECT pg_control_version, catalog_version_no FROM pg_control_system();".to_string(),
        2 => "SELECT timeline_id, prev_timeline_id, full_page_writes, checkpoint_lsn IS NOT NULL, redo_lsn IS NOT NULL, next_multixact_id, next_multi_offset, oldest_multi_xid FROM pg_control_checkpoint();".to_string(),
        3 => "SELECT min_recovery_end_lsn::text, min_recovery_end_timeline, backup_start_lsn::text, backup_end_lsn::text, end_of_backup_record_required FROM pg_control_recovery();".to_string(),
        4 => {
            if g.rng.chance(1, 3) {
                "SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL;".to_string()
            } else {
                "SELECT rule_number, line_number, type, database::text, user_name::text, address, netmask, auth_method, options::text, error FROM pg_hba_file_rules ORDER BY rule_number;".to_string()
            }
        }
        _ => "SELECT count(*) FROM pg_ident_file_mappings;".to_string(),
    };
    raw(sql)
}

// ------------------------------------------------------------ family 11 ----
// WAL-summarizer bracket (walsummary.c / blkreftable.c / summarize_wal):
// GUC flipped identically on both sides within one group; all probes are
// timing-proof projections (availability is asynchronous, never compared).

fn gen_summ(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("admin:summ");
    let mut out = vec![
        StmtKind::Raw("ALTER SYSTEM SET summarize_wal = on;".to_string()),
        StmtKind::Raw("SELECT pg_reload_conf();".to_string()),
        StmtKind::Raw("SELECT pg_switch_wal() IS NOT NULL;".to_string()),
        StmtKind::Raw("CHECKPOINT;".to_string()),
        StmtKind::Raw("SELECT count(*) >= 0 FROM pg_available_wal_summaries() a;".to_string()),
    ];
    match g.rng.below(3) {
        0 => out.push(StmtKind::Raw(
            "SELECT count(*) >= 0 FROM (SELECT pg_wal_summary_contents(s.tli, s.start_lsn, s.end_lsn) FROM pg_available_wal_summaries() s ORDER BY s.end_lsn DESC LIMIT 1) t;".to_string(),
        )),
        1 => out.push(StmtKind::Raw(
            "SELECT count(*) >= 0 FROM pg_ls_summariesdir() a;".to_string(),
        )),
        _ => out.push(StmtKind::Raw(
            "SELECT count(*) >= 0 FROM (SELECT s.tli FROM pg_available_wal_summaries() s LIMIT 1) t;".to_string(),
        )),
    }
    out.push(StmtKind::Raw("ALTER SYSTEM RESET summarize_wal;".to_string()));
    out.push(StmtKind::Raw("SELECT pg_reload_conf();".to_string()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_admin_module(&mut g);
            groups.push(stmts.iter().map(|s| s.to_sql()).collect::<Vec<_>>());
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    #[test]
    fn shapes_and_textual_invariants() {
        let (groups, prods) = gen_groups(0x05AD, 4000, );
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                for banned in ["random(", "now()", "pg_sleep"] {
                    assert!(!sql.contains(banned), "volatile fn in {sql}");
                }
                // Every raw size/lsn/count surface must be projected: no
                // bare environment value may reach the compare surface.
                for f in ["pg_relation_size", "pg_table_size", "pg_total_relation_size",
                          "pg_indexes_size", "pg_database_size", "pg_tablespace_size"] {
                    if sql.contains(f) && !sql.contains("'bogus'") && !sql.contains("fz_no_such_db") {
                        let projected = sql.contains(">=") || sql.contains("> 0")
                            || sql.contains("IS NULL") || sql.contains("IS NOT NULL");
                        assert!(projected, "raw size on compare surface: {sql}");
                    }
                }
                if sql.contains("pg_current_wal") || sql.contains("pg_switch_wal")
                    || sql.contains("pg_export_snapshot")
                {
                    assert!(
                        sql.contains("IS NOT NULL") || sql.contains(">="),
                        "raw live LSN/snapshot on compare surface: {sql}"
                    );
                }
                if sql.contains("pg_available_wal_summaries")
                    || sql.contains("pg_wal_summary_contents")
                    || sql.contains("pg_ls_summariesdir")
                {
                    assert!(sql.contains("count(*) >= 0"), "unprojected summary probe: {sql}");
                }
            }
            // Brackets close within their group.
            if group[0].starts_with("ALTER SYSTEM SET summarize_wal") {
                assert!(
                    group.iter().any(|s| s == "ALTER SYSTEM RESET summarize_wal;"),
                    "unclosed summarize_wal bracket: {group:?}"
                );
                assert_eq!(group.last().unwrap(), "SELECT pg_reload_conf();");
            }
            if group[0].contains("pg_backup_start") {
                assert!(
                    group.last().unwrap().contains("pg_backup_stop"),
                    "unclosed backup bracket: {group:?}"
                );
            }
            if group[0] == "BEGIN;" {
                let last = group.last().unwrap();
                assert!(
                    last == "ROLLBACK;" || last == "COMMIT;",
                    "unclosed txn bracket: {group:?}"
                );
            }
        }
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        assert!(prods.iter().any(|q| q == "admin:err"), "err arm never fired");
    }

    #[test]
    fn generation_is_deterministic() {
        let (a, pa) = gen_groups(5, 300);
        let (b, pb) = gen_groups(5, 300);
        assert_eq!(a, b);
        assert_eq!(pa, pb);
        let (c, _) = gen_groups(6, 300);
        assert_ne!(a, c);
    }
}
