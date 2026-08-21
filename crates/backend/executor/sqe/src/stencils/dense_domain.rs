//! dense-domain group stencil — [famB M1] the hot-shape shape: a derived
//! integer key (TruncMinute) over a date-bounded frame folds into a FLAT
//! minute array (ORDER BY key is free — array order). Ported from
//! kernels_f6::opt_q42 (final2, kernels_f6.rs:713) via
//! fam_frame::group_minute.
//!
//! The minute domain bounds are DATA from the frame's date conjunct
//! (base = (dlo-1) day, width = (dhi-dlo+3)*1440 minutes — the ±1-day
//! margin for EventDate/EventTime timezone skew), with a map for
//! overflow (guarded, never observed). Thread claim: the survivor work
//! is a bounded walk (~2 days of one counter) — elected SERIAL, the
//! same serial_2d_threshold election the zone-order family uses; the
//! parallel twin measured no win (ron hot-shape params).
//!
//! The frame is the shared condition-cache derivation
//! (two_level::frame_granules) — hot-shape keys its OWN entry (different date
//! range than the hot-shape frame).

use crate::answer::{AnswerCol, AnswerSet};
use crate::engine::SqeCtx;
use crate::ir::{CmpOp, KeyExpr, OrderBy, PlanNode};
use crate::kernels_f6::{trunc_minute, FxHasher};
use crate::scan::{CurCache, Scratch};
use crate::stencils::sx;
use crate::stencils::two_level::frame_granules;
use crate::typmeta::TypMeta;
use std::collections::HashMap;

type Fx = std::hash::BuildHasherDefault<FxHasher>;

pub fn run_dense_domain_group(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let bank = ctx.bank; // serial by election (see module docs)
    let pred = node.pred.as_ref().expect("dense_domain: predicate-bearing family");
    let a_et = match node.params.key_exprs.as_slice() {
        [KeyExpr::TruncMinute(c)] => *c,
        other => panic!("dense_domain: unsupported key shape {other:?}"),
    };
    // Emission is key-ASC by construction (array order); OrderBy::None
    // (the server path — order/slice apply shell-side) rides the same
    // deterministic emission with no output-order obligation.
    assert!(
        matches!(node.params.order, OrderBy::KeyAsc | OrderBy::None),
        "dense_domain renders key-ASC"
    );
    let res_terms = pred.residues();
    let res_cols: Vec<u32> = res_terms.iter().map(|t| t.col).collect();
    let res_widths: Vec<u8> = res_cols.iter().map(|&c| super::col_width(bank, c)).collect();

    // Minute domain bounds: the INTERSECTION of every date-interval
    // conjunct (the server lowers `>= lo` / `<= hi` as two one-sided
    // Between terms; the rig authors one closed Range — both shapes
    // intersect to the closed interval). Admission proved both sides
    // bounded; the overflow map still catches anything outside.
    let (mut dlo, mut dhi, mut seen) = (i64::MIN, i64::MAX, false);
    for t in pred.frame().iter().chain(pred.residues().iter()) {
        if t.op == CmpOp::Between && node.is_date(t.col) {
            dlo = dlo.max(t.lo);
            dhi = dhi.min(t.hi);
            seen = true;
        }
    }
    assert!(
        seen && dlo > i64::MIN && dhi < i64::MAX,
        "dense_domain: minute grouping needs a bounded date interval conjunct"
    );
    let base_us: i64 = (dlo - 1) * 86_400_000_000;
    let nmin: usize = (((dhi - dlo).max(0) + 3) as usize) * 1440;

    let granules = frame_granules(ctx, node, pred);

    // Depot-riding decode arenas (serial caller-side — the window_replay
    // convention); cursors are per-engagement, never parked.
    let mut rs: Vec<Scratch> = res_cols.iter().map(|_| crate::scan::scratch_fetch()).collect();
    let mut rc: Vec<CurCache> = res_cols.iter().map(|&a| CurCache::new(a)).collect();
    let mut es = crate::scan::scratch_fetch();
    let mut ec = CurCache::new(a_et);
    let mut counts = vec![0u32; nmin];
    let mut overflow: HashMap<i64, u64, Fx> = Default::default();
    for fg in granules.iter() {
        let n = fg.rows as usize;
        let mut rcols: Vec<&[u64]> = Vec::with_capacity(res_cols.len());
        for (scr, cc) in rs.iter_mut().zip(rc.iter_mut()) {
            let d = scr.decode_full(cc.get(bank, fg.pi), fg.g, n);
            rcols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
        }
        let et = es.decode_full(ec.get(bank, fg.pi), fg.g, n);
        // Term-major residue filter (R2: op match at granule-term grain).
        let mut surv: Vec<u16> = fg.rl.clone();
        for (ti, tm) in res_terms.iter().enumerate() {
            let (d, w) = (rcols[ti], res_widths[ti]);
            tm.filter_sel(&mut surv, |_| true, |r| sx(d[r], w));
        }
        for &r in &surv {
            let m = trunc_minute(et[r as usize] as i64);
            let idx = (m - base_us) / 60_000_000;
            if idx >= 0 && (idx as usize) < nmin {
                counts[idx as usize] += 1;
            } else {
                *overflow.entry(m).or_insert(0) += 1;
            }
        }
    }
    for s in rs {
        crate::scan::scratch_park(s);
    }
    crate::scan::scratch_park(es);

    // Render: distinct minutes ASC (array order IS key order; overflow
    // minutes merge into position), rank window + the kernels_f6 trailer.
    let mut rows: Vec<(i64, u64)> = Vec::new();
    for (i, &c) in counts.iter().enumerate() {
        if c > 0 {
            rows.push((base_us + i as i64 * 60_000_000, c as u64));
        }
    }
    for (m, c) in overflow {
        match rows.binary_search_by_key(&m, |e| e.0) {
            Ok(i) => rows[i].1 += c,
            Err(i) => rows.insert(i, (m, c)),
        }
    }
    let groups = rows.len();
    let total_rows: u64 = rows.iter().map(|e| e.1).sum();
    // Typed emit: TruncMinute keys carry the TIMESTAMP render law through
    // the key column's TypMeta (fmt_timestamp lives in the render seam;
    // keys are trunc_minute-floored, so seconds render as :00).
    let window: Vec<&(i64, u64)> =
        rows.iter().skip(node.params.offset).take(node.params.limit).collect();
    let mut a = AnswerSet::from_cols(vec![
        AnswerCol::i64s(node.ty_of(a_et), window.iter().map(|&&(m, _)| m).collect()),
        AnswerCol::i64s(TypMeta::INT8, window.iter().map(|&&(_, c)| c as i64).collect()),
    ]);
    a.note = Some(crate::render::footer_groups(groups as u64, total_rows as u64));
    a
}
