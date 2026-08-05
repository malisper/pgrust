//! S1 SpecConflict (H8 — the p3 collision shape): the
//! insert-conflict-specconflict advisory-lock choreography as a generated
//! property. Two inserter sessions race INSERT ... ON CONFLICT over one key
//! through an expression unique index whose function blocks on advisory
//! locks (the pause point INSIDE the speculative-insertion window); the
//! controller (session 0) releases locks in the staged order that forces
//! session 3's speculative tuple to be KILLED (heap_abort_speculative),
//! then seq-scans the page while a REPEATABLE READ horizon pin (session 1)
//! keeps the killed tuple unpruned/unhinted — the scan resolves a raw
//! xmin == 0 through the page-visibility memo path (the p3 plant's
//! debug_assert site).
//!
//! Sessions: 0 = controller, 1 = horizon pin, 2 = inserter s1 (wins),
//! 3 = inserter s2 (spec-kill + DO UPDATE). Every ordering decision is a
//! WaitUntil observable-state gate over pg_locks (never a sleep) or a
//! sync-completion edge; the whole schedule is plan bytes.
//!
//! Detection shapes on the planted engine (revert of c1ca4fcc9): the final
//! seq scan trips XidVisMemo::merge's debug_assert(xid != 0) => SIGABRT on
//! a debug DUT => ConnectionLost => rust-crash P1 (+ the dut-log scrape's
//! panic line). On clean engines the property PASSes byte-identically to C.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Row, Value};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    IsoLevel, Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TxCtl,
};

const SLOT_JOIN_S1: u32 = 0;
const SLOT_JOIN_S2: u32 = 1;
const SLOT_SCAN: u32 = 2;

fn passthrough(sql: String) -> PStep {
    PStep::Sql(SqlStep {
        sql,
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    })
}

fn mutation(sql: String) -> PStep {
    PStep::Sql(SqlStep {
        sql,
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    })
}

fn wait_waiter(classid: u32, objid: u32) -> PStep {
    // Gate: an UNGRANTED advisory waiter exists on (classid, objid) —
    // two-int advisory locks land as classid=key1, objid=key2.
    PStep::WaitUntil(SqlStep {
        sql: format!(
            "SELECT count(*) = 1 FROM pg_locks WHERE locktype = 'advisory' \
             AND classid = {classid} AND objid = {objid} AND NOT granted"
        ),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    })
}

/// The pause-point function: evaluated once per index-key computation. The
/// first evaluation (conflict pre-check) finds (sess,1) held by the
/// controller and parks on (sess,3); the second (speculative index
/// insertion) try-locks (sess,1) successfully and parks on (sess,2) —
/// INSIDE the window between the speculative heap insert and
/// _bt_check_unique. Single-line body (plan-format law).
const BLURT_FN: &str = "CREATE OR REPLACE FUNCTION shp_blurt_lock(text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$ BEGIN IF pg_try_advisory_xact_lock(current_setting('spec.session')::int, 1) THEN PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 2); ELSE PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 3); END IF; RETURN $1; END; $$";

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let t = h::fresh_table(rng, "s1t");

    // Async upsert on the CURRENTLY ACTIVE session (a Session(k) step
    // always precedes each call — the schedule lives in those steps).
    let upsert = |tag: &str| -> PStep {
        PStep::AsyncSql(SqlStep {
            sql: format!(
                "INSERT INTO {t}(key, data) VALUES ('k1', 'ins-{tag}') \
                 ON CONFLICT (shp_blurt_lock(key)) DO UPDATE SET data = {t}.data || ' upd-{tag}'"
            ),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::Opaque),
            stackref: None,
        })
    };

    let steps = vec![
        // Estate (controller). CREATE OR REPLACE: instances are sequential
        // within a plan; the per-seed schema reset owns cleanup.
        mutation(BLURT_FN.to_string()),
        mutation(format!("CREATE TABLE {t}(key text, data text)")),
        mutation(format!(
            "CREATE UNIQUE INDEX {t}_uq ON {t}((shp_blurt_lock(key)))"
        )),
        // Function evaluations during CREATE INDEX / setup run without
        // spec.session set — give the controller one so index build and
        // later controller-side evaluations never error.
        passthrough(format!("SET spec.session = 9")),
        // Horizon pin FIRST (sync completion = snapshot exists before any
        // upsert xid): an open RR snapshot keeps the killed speculative
        // tuple's page from being pruned/hinted before the detector scan.
        PStep::Session(1),
        PStep::Tx(TxCtl::Begin(IsoLevel::RepeatableRead)),
        passthrough(format!("SELECT count(*) >= 0 FROM {t}")),
        PStep::Session(0),
        // Controller grabs the full 2x3 advisory-lock grid (session-scoped;
        // one statement => sync completion = all six granted).
        passthrough(
            "SELECT pg_advisory_lock(sess, lock) FROM generate_series(1,2) a(sess), \
             generate_series(1,3) b(lock)"
                .to_string(),
        ),
        // Inserters: SET their pause identity, then dispatch the upserts
        // WITHOUT waiting (both block inside key evaluation).
        PStep::Session(2),
        passthrough("SET spec.session = 1".to_string()),
        upsert("s1"),
        PStep::Session(3),
        passthrough("SET spec.session = 2".to_string()),
        upsert("s2"),
        PStep::Session(0),
        // Both conflict probes parked mid-key-evaluation on (sess,3).
        wait_waiter(1, 3),
        wait_waiter(2, 3),
        passthrough("SELECT pg_advisory_unlock(1,1), pg_advisory_unlock(2,1)".to_string()),
        passthrough("SELECT pg_advisory_unlock(1,3), pg_advisory_unlock(2,3)".to_string()),
        // Both heap tuples now speculatively inserted; index-key
        // re-evaluations parked on (sess,2) BEFORE _bt_check_unique.
        wait_waiter(1, 2),
        wait_waiter(2, 2),
        // s1 completes: its speculative insert wins and commits.
        passthrough("SELECT pg_advisory_unlock(1,2)".to_string()),
        PStep::Join { session: 2, slot: Some(SLOT_JOIN_S1) },
        PStep::Assert(Check::StmtOk { slot: SLOT_JOIN_S1 }),
        // s2 finds s1's committed entry at _bt_check_unique: kills its
        // speculative tuple, retries, takes the DO UPDATE arm.
        passthrough("SELECT pg_advisory_unlock(2,2)".to_string()),
        PStep::Join { session: 3, slot: Some(SLOT_JOIN_S2) },
        PStep::Assert(Check::StmtOk { slot: SLOT_JOIN_S2 }),
        // THE DETECTOR: seq scan of the page holding the killed speculative
        // tuple while the horizon pin is open (raw xmin == 0 through the
        // page-visibility memo). Compared read + exact-row assert.
        PStep::Sql(SqlStep {
            sql: format!("SELECT key, data FROM {t}"),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::KnownRows {
                rows: vec![Row(vec![
                    Value::Text("k1".into()),
                    Value::Text("ins-s1 upd-s2".into()),
                ])],
            }),
            stackref: Some(SLOT_SCAN),
        }),
        PStep::Assert(Check::RowsEq {
            slot: SLOT_SCAN,
            rows: vec![Row(vec![
                Value::Text("k1".into()),
                Value::Text("ins-s1 upd-s2".into()),
            ])],
        }),
        // Release the pin, then cleanup (session-scoped controller locks).
        PStep::Session(1),
        PStep::Tx(TxCtl::Commit),
        PStep::Session(0),
        passthrough("SELECT pg_advisory_unlock_all()".to_string()),
        mutation(format!("DROP TABLE {t}")),
    ];

    PropertyInstance {
        property: PropertyId::S1SpecConflict,
        steps,
        tables: BTreeSet::from([t]),
    }
}

