//! M2 CrossSessionVisibility (H8 — the property the A1 single-session axiom
//! dropped, reinstated on the multi-session estate): committed-vs-uncommitted
//! visibility across two sessions, asserted ONLY where the serialized
//! schedule makes the answer deterministic (conservative-oracle law).
//!
//! Two arms (seed-drawn):
//!   * RC arm: session 0 holds an uncommitted INSERT open; session 1 (READ
//!     COMMITTED, autocommit reads) must NOT see it; after COMMIT it MUST —
//!     after ROLLBACK it must NOT.
//!   * RR arm: session 1 takes a REPEATABLE READ snapshot FIRST; session 0
//!     commits an INSERT; session 1's in-transaction re-reads must still see
//!     the OLD count (snapshot isolation), and a fresh snapshot after COMMIT
//!     must see the new row.
//!
//! All asserts are slot checks with generation-time-known counts; the
//! single-session ledger is deliberately untouched (probe: Opaque
//! everywhere, ledger_op None — the bridge keeps worker-session txs out of
//! the ledger too).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    IsoLevel, Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TxCtl,
};

fn mutation(sql: String) -> SqlStep {
    SqlStep {
        sql,
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    }
}

fn count_read(sql: String, slot: u32) -> SqlStep {
    SqlStep {
        sql,
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: Some(slot),
    }
}

fn assert_count(slot: u32, want: i64) -> PStep {
    PStep::Assert(Check::ScalarEq { slot, value: Value::Int(want) })
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let t = h::fresh_table(rng, "m2t");
    let n = rng.gen_range(3usize..=8);
    let rows = h::gen_rows(rng, n);
    // A key guaranteed distinct from the generated ones (gen_rows draws in
    // 0..1_000_000).
    let knew = 1_000_000 + rng.gen_range(0i64..1000);
    let n_i = n as i64;

    let mut steps = vec![h::sql(h::create_kv(&t)), h::sql(h::insert_rows(&t, &rows))];

    let rr_arm = rng.gen_range(0u32..2) == 0;
    if rr_arm {
        // RR arm: reader snapshot FIRST, then a committed write, then
        // in-snapshot re-reads.
        steps.extend([
            PStep::Session(1),
            PStep::Tx(TxCtl::Begin(IsoLevel::RepeatableRead)),
            PStep::Sql(count_read(format!("SELECT count(*) FROM {t}"), 0)),
            assert_count(0, n_i), // snapshot taken at n rows
            PStep::Session(0),
            PStep::Sql(mutation(format!(
                "INSERT INTO {t} (k, v) VALUES ({knew}, 1)"
            ))),
            PStep::Session(1),
            // Snapshot isolation: the committed insert is INVISIBLE inside
            // the open RR transaction.
            PStep::Sql(count_read(format!("SELECT count(*) FROM {t}"), 1)),
            assert_count(1, n_i),
            PStep::Sql(count_read(
                format!("SELECT count(*) FROM {t} WHERE k = {knew}"),
                2,
            )),
            assert_count(2, 0),
            PStep::Tx(TxCtl::Commit),
            // Fresh snapshot: visible.
            PStep::Sql(count_read(
                format!("SELECT count(*) FROM {t} WHERE k = {knew}"),
                3,
            )),
            assert_count(3, 1),
            PStep::Session(0),
        ]);
    } else {
        // RC arm: uncommitted-invisibility, then commit-or-rollback.
        let commit = rng.gen_range(0u32..2) == 0;
        steps.extend([
            PStep::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)),
            PStep::Sql(mutation(format!(
                "INSERT INTO {t} (k, v) VALUES ({knew}, 1)"
            ))),
            PStep::Session(1),
            // READ COMMITTED never sees another session's open transaction.
            PStep::Sql(count_read(
                format!("SELECT count(*) FROM {t} WHERE k = {knew}"),
                0,
            )),
            assert_count(0, 0),
            PStep::Sql(count_read(format!("SELECT count(*) FROM {t}"), 1)),
            assert_count(1, n_i),
            PStep::Session(0),
            PStep::Tx(if commit { TxCtl::Commit } else { TxCtl::Rollback }),
            PStep::Session(1),
            PStep::Sql(count_read(
                format!("SELECT count(*) FROM {t} WHERE k = {knew}"),
                2,
            )),
            assert_count(2, if commit { 1 } else { 0 }),
            PStep::Session(0),
        ]);
    }

    steps.push(h::sql(h::drop_table(&t)));

    PropertyInstance {
        property: PropertyId::M2CrossSession,
        steps,
        tables: BTreeSet::from([t]),
    }
}
