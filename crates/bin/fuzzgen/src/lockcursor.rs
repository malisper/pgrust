//! Locks / savepoints / NOTIFY-error drain module (LOCKCURSOR): the
//! deterministic, single-session, SQL-reachable arms of the lock manager
//! (storage/lmgr/lock.c + lmgr.c, commands/lockcmds.c LockTableCommand),
//! the subtransaction machinery (access/transam/xact.c DefineSavepoint /
//! RollbackToSavepoint / ReleaseSavepoint / AbortSubTransaction /
//! CleanupSubTransaction) and the NOTIFY validation arms of
//! commands/async.c (Async_Notify payload/channel checks + AtAbort_Notify
//! queue discard) — the parts a single session can drive with no
//! cross-session timing.
//!
//! Deliberately out of scope (the gated concurrency phase, not this
//! deterministic coverage lane): cross-session lock waits and deadlocks,
//! NOTIFY *delivery* (PreCommit_Notify/ProcessCompletedNotifies), and
//! LISTEN — none of which are deterministic in a single-session stream.
//! Every valid NOTIFY here is queued inside a bracket that ROLLBACKs, so
//! it is discarded, never delivered; every error arm errors at execution
//! time (before any commit), so the comparand is the error identity.
//!
//! Complementarity: the advisory-lock *introspection* bracket already
//! lives in `adtmisc::gen_locks` (session + xact takes, pg_locks advisory
//! projection, unlock_all); this module adds only the arms it does NOT
//! reach — session-lock reference counting (double take / double release),
//! `pg_advisory_unlock` returning false for an un-held key, and the plain
//! (non-try, non-shared) `pg_advisory_xact_lock`. The cursor/FETCH/MOVE
//! and PREPARE surface is owned by `cursor` (C1); explicit LOCK TABLE, the
//! all-modes drain, savepoint nesting and NOTIFY errors are new here.
//!
//! Determinism discipline (mirrors obs/adtmisc):
//!   - pg_locks is projected without pid/relation-oid: relation locks are
//!     joined to pg_class by the group-local fixed relname, and only
//!     (mode, granted) are selected, ORDER BY mode — stable single-session;
//!   - every relation lock is taken on a fresh, group-local `fz_lc_t`
//!     created and dropped in-group (created COMMITTED before the LOCK
//!     bracket so no CREATE-time AccessExclusiveLock pollutes the mode set,
//!     and the LOCK bracket always ROLLBACKs so nothing persists);
//!   - the savepoint fixture is a group-local `fz_lc_sp`, created and
//!     dropped in-group; the final ORDER BY probe pins result identity;
//!   - error arms use deterministic inputs (fixed key names, `repeat()`
//!     payloads / a fixed-length literal) so both engines see byte-
//!     identical SQL.
//!
//! Every emitted statement is a single line ending in `;` with balanced
//! parens (the stmt-module invariants).

use crate::stmt::{Gen, StmtKind};

/// All eight explicit lock modes, weakest → strongest (the lockcmds.c /
/// lock.c mode-parse + LockAcquire arms).
const LOCK_MODES: &[&str] = &[
    "ACCESS SHARE",
    "ROW SHARE",
    "ROW EXCLUSIVE",
    "SHARE UPDATE EXCLUSIVE",
    "SHARE",
    "SHARE ROW EXCLUSIVE",
    "EXCLUSIVE",
    "ACCESS EXCLUSIVE",
];

const SHAPES: &[&str] = &[
    "lockcursor:locktable",
    "lockcursor:advisory",
    "lockcursor:savepoint",
    "lockcursor:savepoint_err",
    "lockcursor:notify",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_lockcursor_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("lockcursor");
    let stmts = match g.weights.pick(g.rng, SHAPES) {
        "lockcursor:locktable" => gen_locktable(g),
        "lockcursor:advisory" => gen_advisory(g),
        "lockcursor:savepoint" => gen_savepoint(g),
        "lockcursor:savepoint_err" => gen_savepoint_err(g),
        _ => gen_notify(g),
    };
    stmts.into_iter().map(raw).collect()
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

// ---------------------------------------------------------------------
// LOCK TABLE: all eight modes + ONLY + default + NOWAIT + escalation,
// with an in-txn pg_locks introspection, all over a fresh committed
// fixture; the bracket always ROLLBACKs. Optional 42P01 error arm.
// ---------------------------------------------------------------------

fn gen_locktable(g: &mut Gen) -> Vec<String> {
    g.fire("lockcursor:locktable");
    let mut out = vec![
        "DROP TABLE IF EXISTS fz_lc_t;".to_string(),
        "CREATE TABLE fz_lc_t (id int PRIMARY KEY, v text);".to_string(),
        "BEGIN;".to_string(),
    ];

    // Every explicit mode, once — the parse + LockAcquire arm per mode.
    for m in LOCK_MODES {
        g.fire2("lockcursor:lt:mode:", m);
        out.push(format!("LOCK TABLE fz_lc_t IN {m} MODE;"));
    }
    // Re-lock at a weaker mode after a stronger one is already held: the
    // "already hold >= requested" fast return in LockAcquire.
    g.fire("lockcursor:lt:relock_weak");
    out.push("LOCK TABLE fz_lc_t IN ACCESS SHARE MODE;".to_string());
    // ONLY qualifier (RangeVarCallbackForLockTable / find_all_inheritors
    // with recurse=false).
    g.fire("lockcursor:lt:only");
    out.push("LOCK TABLE ONLY fz_lc_t IN ACCESS SHARE MODE;".to_string());
    // NOWAIT (dontWait path — always granted single-session).
    g.fire("lockcursor:lt:nowait");
    out.push("LOCK TABLE fz_lc_t IN EXCLUSIVE MODE NOWAIT;".to_string());
    // Default mode = ACCESS EXCLUSIVE (LOCK with no IN ... MODE clause).
    g.fire("lockcursor:lt:default");
    out.push("LOCK fz_lc_t;".to_string());

    // Introspection: modes held on the fixture, pid/oid-free, mode-ordered.
    out.push(
        "SELECT l.mode, l.granted FROM pg_locks l JOIN pg_class c \
         ON l.relation = c.oid WHERE c.relname = 'fz_lc_t' \
         ORDER BY l.mode;"
            .to_string(),
    );

    out.push("ROLLBACK;".to_string());
    out.push("DROP TABLE fz_lc_t;".to_string());

    // Optional 42P01 arm (autocommit — errors on its own, cannot poison
    // the bracket which has already ended).
    if g.weights.pick(g.rng, &["lockcursor:lt:err", "lockcursor:lt:noerr"])
        == "lockcursor:lt:err"
    {
        g.fire("lockcursor:lt:err");
        out.push("LOCK TABLE fz_lc_nonesuch_xyz IN ACCESS SHARE MODE;".to_string());
    }

    out
}

// ---------------------------------------------------------------------
// Advisory: only the arms adtmisc::gen_locks does not reach.
// ---------------------------------------------------------------------

fn gen_advisory(g: &mut Gen) -> Vec<String> {
    g.fire("lockcursor:advisory");
    // Keys deterministic; offset well away from adtmisc's 0..1000 window
    // so cross-module key overlap within a session is impossible (and
    // groups are atomic + release everything anyway).
    let k = 900_000 + g.rng.below(1000) as i64;
    let k2 = k + 1;

    let mut out = vec![
        // Session-lock reference counting: two takes, three releases.
        format!("SELECT pg_advisory_lock({k});"),
        format!("SELECT pg_advisory_lock({k});"),
        // ref 2 -> 1 (true), 1 -> 0 (true).
        format!("SELECT pg_advisory_unlock({k});"),
        format!("SELECT pg_advisory_unlock({k});"),
        // Un-held: returns false (the "lock not held" return arm).
        format!("SELECT pg_advisory_unlock({k});"),
        format!("SELECT pg_advisory_unlock_shared({k});"),
    ];

    // Plain (non-try, non-shared) xact advisory lock, released by COMMIT.
    out.push("BEGIN;".to_string());
    out.push(format!("SELECT pg_advisory_xact_lock({k2});"));
    out.push(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"
            .to_string(),
    );
    out.push("COMMIT;".to_string());
    out
}

// ---------------------------------------------------------------------
// Savepoint nesting + subtransaction error recovery.
// ---------------------------------------------------------------------

fn gen_savepoint(g: &mut Gen) -> Vec<String> {
    g.fire("lockcursor:savepoint");
    let commit = g.weights.pick(g.rng, &["lockcursor:sp:commit", "lockcursor:sp:rollback"])
        == "lockcursor:sp:commit";
    if commit {
        g.fire("lockcursor:sp:commit");
    } else {
        g.fire("lockcursor:sp:rollback");
    }

    let mut out = vec![
        "DROP TABLE IF EXISTS fz_lc_sp;".to_string(),
        "CREATE TABLE fz_lc_sp (id int);".to_string(),
        "BEGIN;".to_string(),
        "INSERT INTO fz_lc_sp VALUES (1);".to_string(),
        // Nesting: sp1 > sp2 > sp3.
        "SAVEPOINT sp1;".to_string(),
        "INSERT INTO fz_lc_sp VALUES (2);".to_string(),
        "SAVEPOINT sp2;".to_string(),
        "INSERT INTO fz_lc_sp VALUES (3);".to_string(),
        // Roll back to the OUTER savepoint: releases sp2, undoes 2 and 3.
        "ROLLBACK TO SAVEPOINT sp1;".to_string(),
        "INSERT INTO fz_lc_sp VALUES (4);".to_string(),
        // Release the (now single) live savepoint.
        "RELEASE SAVEPOINT sp1;".to_string(),
        // Subxact error recovery: error inside a subxact, then recover.
        "SAVEPOINT sp3;".to_string(),
        "SELECT 1 / 0;".to_string(),
        "ROLLBACK TO SAVEPOINT sp3;".to_string(),
        "RELEASE SAVEPOINT sp3;".to_string(),
        "INSERT INTO fz_lc_sp VALUES (5);".to_string(),
    ];
    // Committed state is {1,4,5} on COMMIT, empty on ROLLBACK — pinned.
    out.push(if commit {
        "COMMIT;".to_string()
    } else {
        "ROLLBACK;".to_string()
    });
    out.push("SELECT id FROM fz_lc_sp ORDER BY id;".to_string());
    out.push("DROP TABLE fz_lc_sp;".to_string());
    out
}

fn gen_savepoint_err(g: &mut Gen) -> Vec<String> {
    g.fire("lockcursor:savepoint_err");
    vec![
        // ROLLBACK TO / RELEASE of a non-existent savepoint: 3B001.
        "BEGIN;".to_string(),
        "SELECT 1;".to_string(),
        "ROLLBACK TO SAVEPOINT fz_lc_nosuch;".to_string(),
        "ROLLBACK;".to_string(),
        "BEGIN;".to_string(),
        "SELECT 1;".to_string(),
        "RELEASE SAVEPOINT fz_lc_nosuch;".to_string(),
        "ROLLBACK;".to_string(),
        // Savepoint verbs outside a transaction block: the
        // RequireTransactionBlock guard arms.
        "SAVEPOINT fz_lc_outside;".to_string(),
        "RELEASE SAVEPOINT fz_lc_outside;".to_string(),
        "ROLLBACK TO SAVEPOINT fz_lc_outside;".to_string(),
    ]
}

// ---------------------------------------------------------------------
// NOTIFY: queue (discarded on rollback) + validation error arms.
// ---------------------------------------------------------------------

fn gen_notify(g: &mut Gen) -> Vec<String> {
    g.fire("lockcursor:notify");
    // 8001-char payload literal: over NOTIFY_PAYLOAD_MAX_LENGTH (8000).
    let long_payload = "a".repeat(8001);
    vec![
        // Valid takes, queued then discarded (AtAbort_Notify) — no delivery.
        "BEGIN;".to_string(),
        "NOTIFY fz_lc_chan;".to_string(),
        "NOTIFY fz_lc_chan, 'payload-one';".to_string(),
        "SELECT pg_notify('fz_lc_chan2', 'via-func');".to_string(),
        // Duplicate (channel,payload) in the same xact: dedup arm.
        "NOTIFY fz_lc_chan, 'payload-one';".to_string(),
        "ROLLBACK;".to_string(),
        // Error arms (autocommit, error at execution):
        //   empty channel name,
        "SELECT pg_notify('', 'x');".to_string(),
        //   payload too long via pg_notify,
        "SELECT pg_notify('fz_lc_chan', repeat('x', 9000));".to_string(),
        //   payload too long via the NOTIFY grammar path.
        format!("NOTIFY fz_lc_chan, '{long_payload}';"),
    ]
}
