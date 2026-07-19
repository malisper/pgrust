//! C2 HoldCursor (H8, the p7 tuplestore surface): SCROLL CURSOR WITH HOLD
//! declared inside a transaction, a seed-drawn pre-commit fetch position,
//! then COMMIT — which drives PersistHoldablePortal: the whole result is
//! materialized into the portal's holdStore TUPLESTORE and the read
//! position is re-established with forward skiptuples over it. Post-commit
//! ops then walk that tuplestore (RunFromStore forward/backward, portal
//! rewinds for ABSOLUTE) with every outcome model-asserted + diff-c
//! compared.
//!
//! Boundary arms drawn deliberately:
//!   * exact-all: pre-commit FETCH FORWARD n (all rows, atEnd not yet
//!     observed) — the skiptuples(portal_pos == count) repositioning
//!     boundary in PersistHoldablePortal;
//!   * at-start: no pre-commit fetch (portal_pos = 0, rescan path);
//!   * overshoot: pre-commit FETCH FORWARD n+2 (atEnd observed — the
//!     skip-to-EOF loop arm);
//!   * partial: anywhere in between.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::cursor::{CursorModel, CursorOp};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    IsoLevel, Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TxCtl,
};

fn passthrough(sql: String) -> SqlStep {
    SqlStep {
        sql,
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    }
}

fn fetch_step(cur: &str, op: CursorOp, expected: &[crate::oracle::check::Row], slot: u32) -> [PStep; 2] {
    [
        PStep::Sql(SqlStep {
            sql: format!("FETCH {} FROM {cur}", op.sql()),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::KnownRows { rows: expected.to_vec() }),
            stackref: Some(slot),
        }),
        PStep::Assert(Check::RowsEq { slot, rows: expected.to_vec() }),
    ]
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let t = h::fresh_table(rng, "c2t");
    let cur = format!("shc_hold_{:05}", rng.gen_range(0u32..100_000));
    let n = rng.gen_range(4usize..=9);
    let rows = h::gen_rows(rng, n);
    let mut model = CursorModel::new(rows.clone());

    let mut steps = vec![
        h::sql(h::create_kv(&t)),
        h::sql(h::insert_rows(&t, &rows)),
        PStep::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)),
        PStep::Sql(passthrough(format!(
            "DECLARE {cur} SCROLL CURSOR WITH HOLD FOR SELECT k, v FROM {t} ORDER BY k"
        ))),
    ];

    let mut slot = 0u32;
    // Pre-commit position arm (the PersistHoldablePortal boundary lever).
    let pre = match rng.gen_range(0u32..5) {
        0 | 1 => Some(CursorOp::Forward(n as u32)), // exact-all boundary (weight 2)
        2 => None,                                  // at-start (rescan path)
        3 => Some(CursorOp::Forward(n as u32 + 2)), // overshoot (atEnd observed)
        _ => Some(CursorOp::Forward(rng.gen_range(1..n.max(2)) as u32)), // partial
    };
    if let Some(op) = pre {
        let expected = model.apply(op);
        steps.extend(fetch_step(&cur, op, &expected, slot));
        slot += 1;
    }

    // COMMIT: the held portal materializes into its tuplestore here.
    steps.push(PStep::Tx(TxCtl::Commit));

    // Post-commit walk over the holdStore (backward fetches read the
    // tuplestore backward; ABSOLUTE triggers DoPortalRewind + forward skips).
    let n_ops = rng.gen_range(3usize..=5);
    let n_i = n as i64;
    for _ in 0..n_ops {
        let op = match rng.gen_range(0u32..6) {
            0 => CursorOp::Backward(rng.gen_range(1..=(n as u32 + 2))),
            1 => CursorOp::BackwardAll,
            2 => CursorOp::Absolute(rng.gen_range(-(n_i + 1)..=n_i + 1)),
            3 => CursorOp::Forward(rng.gen_range(1..=(n as u32 + 2))),
            4 => CursorOp::Last,
            _ => CursorOp::All,
        };
        let expected = model.apply(op);
        steps.extend(fetch_step(&cur, op, &expected, slot));
        slot += 1;
    }

    steps.push(PStep::Sql(passthrough(format!("CLOSE {cur}"))));
    steps.push(h::sql(h::drop_table(&t)));

    PropertyInstance {
        property: PropertyId::C2HoldCursor,
        steps,
        tables: BTreeSet::from([t]),
    }
}
