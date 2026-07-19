//! C1 CursorWalk (H8, the p7 backward-scan surface): DECLARE [NO] SCROLL
//! CURSOR over a fully-known ordered result, then a seed-drawn walk of
//! FETCH/MOVE ops (FORWARD/BACKWARD/ABSOLUTE/RELATIVE/FIRST/LAST/ALL) with
//! every outcome asserted against the cursor position model (RowsEq — exact
//! rows IN ORDER, CmdCountEq for MOVE tags). FETCH steps are Mark::Read, so
//! diff-c cross-checks the model against real C on every step.
//!
//! Reach rationale (h6-reach queue #7): the generator had NO backward-scan
//! or portal-position surface at all — the audit-B1 class (tuplestore
//! backward skips, plpgsql NO_SCROLL portal flags) was structurally
//! invisible. The NO SCROLL arm (1 in 4) exercises the no-scroll portal
//! path with forward-only walks; the SCROLL arm exercises live-executor
//! backward repositioning inside a transaction.

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

/// Draw one cursor op. `scroll` gates the backward family (a NO SCROLL
/// cursor errors on backward movement — that error is NOT the surface this
/// property probes; X-class statement-form noise owns error paths).
fn draw_op(rng: &mut impl Rng, n: usize, scroll: bool) -> CursorOp {
    let n_i = n as i64;
    if !scroll {
        return match rng.gen_range(0u32..3) {
            0 => CursorOp::Forward(rng.gen_range(1..=(n as u32 + 2))),
            1 => CursorOp::All,
            _ => CursorOp::Relative(rng.gen_range(1..=n_i + 1)),
        };
    }
    match rng.gen_range(0u32..8) {
        0 => CursorOp::Forward(rng.gen_range(1..=(n as u32 + 2))),
        1 => CursorOp::Backward(rng.gen_range(1..=(n as u32 + 2))),
        2 => CursorOp::Absolute(rng.gen_range(-(n_i + 1)..=n_i + 1)),
        3 => CursorOp::Relative(rng.gen_range(-(n_i + 1)..=n_i + 1)),
        4 => CursorOp::All,
        5 => CursorOp::BackwardAll,
        6 => CursorOp::First,
        _ => CursorOp::Last,
    }
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let t = h::fresh_table(rng, "c1t");
    let cur = format!("shc_walk_{:05}", rng.gen_range(0u32..100_000));
    let n = rng.gen_range(4usize..=9);
    // gen_rows iterates a BTreeSet of keys: ascending by k, which IS the
    // ORDER BY k sequence — the model's row order.
    let rows = h::gen_rows(rng, n);
    let scroll = rng.gen_range(0u32..4) != 0; // 1 in 4 walks the NO SCROLL portal path
    let mut model = CursorModel::new(rows.clone());

    let mut steps = vec![
        h::sql(h::create_kv(&t)),
        h::sql(h::insert_rows(&t, &rows)),
        PStep::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)),
        PStep::Sql(passthrough(format!(
            "DECLARE {cur} {} CURSOR FOR SELECT k, v FROM {t} ORDER BY k",
            if scroll { "SCROLL" } else { "NO SCROLL" }
        ))),
    ];

    let n_ops = rng.gen_range(3usize..=6);
    for slot in 0..n_ops as u32 {
        let op = draw_op(rng, n, scroll);
        let expected = model.apply(op);
        let is_move = rng.gen_range(0u32..4) == 0;
        if is_move {
            steps.push(PStep::Sql(SqlStep {
                sql: format!("MOVE {} IN {cur}", op.sql()),
                mark: Mark::Read,
                meta: SqlMeta::default(),
                ledger_op: None,
                probe: Some(ProbeSpec::KnownCommand { count: expected.len() as u64 }),
                stackref: Some(slot),
            }));
            steps.push(PStep::Assert(Check::CmdCountEq {
                slot,
                value: expected.len() as u64,
            }));
        } else {
            steps.push(PStep::Sql(SqlStep {
                sql: format!("FETCH {} FROM {cur}", op.sql()),
                mark: Mark::Read,
                meta: SqlMeta::default(),
                ledger_op: None,
                probe: Some(ProbeSpec::KnownRows { rows: expected.clone() }),
                stackref: Some(slot),
            }));
            steps.push(PStep::Assert(Check::RowsEq { slot, rows: expected }));
        }
    }

    steps.push(PStep::Sql(passthrough(format!("CLOSE {cur}"))));
    steps.push(PStep::Tx(TxCtl::Commit));
    steps.push(h::sql(h::drop_table(&t)));

    PropertyInstance {
        property: PropertyId::C1CursorWalk,
        steps,
        tables: BTreeSet::from([t]),
    }
}
