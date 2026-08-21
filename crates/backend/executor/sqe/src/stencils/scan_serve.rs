//! scan-serve stencil (ScanServe family): row-returning bare scans.
//! Pass 1 (pool, part-grain claim): zone-skip int conjuncts over the flat
//! SMA faces, decode + 3VL-evaluate the conjunction (eval_v — NULL never
//! passes), gather survivor ordinals; under a pushed bound, decode the
//! order keys for survivors only and keep a bounded top-n candidate set
//! per worker (NULL key cells place absolutely per the spec). Pass 2:
//! late materialization — decode ONLY the final rows' output columns
//! (decode_sel), words through the face embed, varlena as payload bytes.
//! Unbounded shapes are admitted only under the scan-answer witness law
//! (planner::check_server_scan); the answer is O(survivors) by contract.

use std::cmp::Ordering;

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::Face;
use crate::engine::SqeCtx;
use crate::ir::{AggOp, PlanNode, TopKKey};
use crate::scan::{byte_payload, CurCache, Scratch};

/// The two byte faces (payload answers/keys); everything else is a word.
#[inline(always)]
fn byte_face(f: Face) -> bool {
    matches!(f, Face::Varlena | Face::Fixed(_))
}

/// One decoded order-key cell (None = SQL NULL). Text cells own their
/// bytes — copies are bounded by the candidate set, never the scan.
enum KeyCell {
    I(Option<i64>),
    B(Option<Vec<u8>>),
}

struct Cand {
    cells: Vec<KeyCell>,
    ui: u32,
    row: u16,
}

fn cmp_cand(a: &Cand, b: &Cand, keys: &[TopKKey]) -> Ordering {
    for (i, k) in keys.iter().enumerate() {
        let ord = match (&a.cells[i], &b.cells[i]) {
            (KeyCell::I(None), KeyCell::I(None)) | (KeyCell::B(None), KeyCell::B(None)) => {
                Ordering::Equal
            }
            (KeyCell::I(None), _) | (KeyCell::B(None), _) => {
                if k.nulls_first { Ordering::Less } else { Ordering::Greater }
            }
            (_, KeyCell::I(None)) | (_, KeyCell::B(None)) => {
                if k.nulls_first { Ordering::Greater } else { Ordering::Less }
            }
            (KeyCell::I(Some(x)), KeyCell::I(Some(y))) => {
                let o = x.cmp(y);
                if k.desc { o.reverse() } else { o }
            }
            (KeyCell::B(Some(x)), KeyCell::B(Some(y))) => {
                // [bpchar-order] trim keys compare the bcTruelen-trimmed
                // images; the (ui, row) tiebreak below keeps selection
                // identical to the boundary's stable sort either way.
                let o = if k.trim {
                    crate::ir::rtrim_blanks(x).cmp(crate::ir::rtrim_blanks(y))
                } else {
                    x.cmp(y)
                };
                if k.desc { o.reverse() } else { o }
            }
            _ => unreachable!("order-key cell class drift (lowering bug)"),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    (a.ui, a.row).cmp(&(b.ui, b.row))
}

struct DecodeCol {
    face: Face,
    scr: Scratch,
    cc: CurCache,
    /// Set per granule after validity+decode.
    all_valid: bool,
}

/// [sqe-topn-gap] Deep-clone a candidate's cells (KeyCell carries owned
/// byte buffers, so derive(Clone) is deliberately absent on Cand).
fn clone_cells(cells: &[KeyCell]) -> Vec<KeyCell> {
    cells
        .iter()
        .map(|c| match c {
            KeyCell::I(v) => KeyCell::I(*v),
            KeyCell::B(v) => KeyCell::B(v.clone()),
        })
        .collect()
}

/// [sqe-topn-gap] Compare a candidate ROW (decoded columns, no cell
/// materialization) against a held candidate under cmp_cand's exact
/// total order — per-key law (validity = NULL lane, desc/nulls_first
/// absolute, byte faces compare payload slices) then the (ui, row)
/// tiebreak — so the bound skip keeps winner selection byte-identical
/// to the unbounded prune.
#[allow(clippy::too_many_arguments)]
fn row_cmp_cand(
    cols: &[DecodeCol],
    key_di: &[usize],
    key_cols: &[(u32, Face)],
    keys: &[TopKKey],
    ui: u32,
    r: usize,
    w: &Cand,
) -> Ordering {
    for (i, k) in keys.iter().enumerate() {
        let c = &cols[key_di[i]];
        let valid = c.all_valid || c.scr.row_valid(r);
        let face = key_cols[i].1;
        let ord = if byte_face(face) {
            let a = valid.then(|| unsafe { byte_payload(face, c.scr.datums[r]) });
            match (&a, &w.cells[i]) {
                (None, KeyCell::B(None)) => Ordering::Equal,
                (None, _) => {
                    if k.nulls_first { Ordering::Less } else { Ordering::Greater }
                }
                (Some(_), KeyCell::B(None)) => {
                    if k.nulls_first { Ordering::Greater } else { Ordering::Less }
                }
                (Some(x), KeyCell::B(Some(y))) => {
                    let o = if k.trim {
                        crate::ir::rtrim_blanks(x).cmp(crate::ir::rtrim_blanks(y))
                    } else {
                        (*x).cmp(y.as_slice())
                    };
                    if k.desc { o.reverse() } else { o }
                }
                _ => unreachable!("order-key cell class drift (lowering bug)"),
            }
        } else {
            let a = valid.then(|| face.word_key(c.scr.datums[r]));
            match (a, &w.cells[i]) {
                (None, KeyCell::I(None)) => Ordering::Equal,
                (None, _) => {
                    if k.nulls_first { Ordering::Less } else { Ordering::Greater }
                }
                (Some(_), KeyCell::I(None)) => {
                    if k.nulls_first { Ordering::Greater } else { Ordering::Less }
                }
                (Some(x), KeyCell::I(Some(y))) => {
                    let o = x.cmp(y);
                    if k.desc { o.reverse() } else { o }
                }
                _ => unreachable!("order-key cell class drift (lowering bug)"),
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    (ui, r as u16).cmp(&(w.ui, w.row))
}

impl DecodeCol {
    fn new(attno: u32, face: Face) -> DecodeCol {
        DecodeCol { face, scr: crate::scan::scratch_fetch(), cc: CurCache::new(attno), all_valid: true }
    }
    fn park(self) {
        crate::scan::scratch_park(self.scr);
    }
}

pub fn run_scan_serve(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let t_all = std::time::Instant::now();
    let emits: Vec<(u32, crate::typmeta::TypMeta)> = node
        .agg
        .iter()
        .map(|a| {
            assert_eq!(a.op, AggOp::EmitMatches, "scan_serve: emit legs only");
            (a.col.expect("emit leg has a column"), a.out)
        })
        .collect();
    let iterms: Vec<crate::ir::PredTerm> =
        node.pred.iter().flat_map(|p| p.terms.iter().cloned()).collect();
    let vterms: Vec<crate::ir::VarPredTerm> =
        node.pred.iter().flat_map(|p| p.var_terms.iter().cloned()).collect();
    let topk = node.params.topk.clone();
    let (keys, bound_n) = match &topk {
        Some(t) => (t.keys.clone(), Some(t.n)),
        None => (Vec::new(), None),
    };
    // Order-key source columns, answer-indexed through the emit list.
    let key_cols: Vec<(u32, Face)> = keys
        .iter()
        .map(|k| {
            let a = emits[k.col as usize].0;
            (a, bank.face(a))
        })
        .collect();

    // Pass-1 decode set: predicate columns first, then order keys.
    let mut dset: Vec<(u32, Face)> = Vec::new();
    let mut push = |dset: &mut Vec<(u32, Face)>, a: u32| {
        if !dset.iter().any(|&(c, _)| c == a) {
            dset.push((a, bank.face(a)));
        }
    };
    for t in &iterms {
        push(&mut dset, t.col);
    }
    for t in &vterms {
        push(&mut dset, t.col);
    }
    for &(a, _) in &key_cols {
        push(&mut dset, a);
    }
    if dset.is_empty() {
        // Predicate-free unkeyed scans still need a walk anchor.
        push(&mut dset, emits[0].0);
    }
    let iterm_di: Vec<usize> = iterms
        .iter()
        .map(|t| dset.iter().position(|&(c, _)| c == t.col).expect("staged"))
        .collect();
    let vterm_di: Vec<usize> = vterms
        .iter()
        .map(|t| dset.iter().position(|&(c, _)| c == t.col).expect("staged"))
        .collect();
    let key_di: Vec<usize> = key_cols
        .iter()
        .map(|&(a, _)| dset.iter().position(|&(c, _)| c == a).expect("staged"))
        .collect();
    let pred_di: Vec<usize> = {
        let mut v: Vec<usize> = iterm_di.iter().chain(&vterm_di).copied().collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    if bound_n == Some(0) {
        return AnswerSet::empty(emits.iter().map(|&(_, ty)| ty).collect());
    }

    let units = ctx.faces.walk(bank, node.cols[0]);
    let smas: Vec<_> = iterms.iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // [psma-consume] §8.2 candidate-slice faces per int conjunct (None
    // under the kill switch / uncovered column — consult degrades).
    let psmas: Vec<_> = iterms.iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    let mut part_units: Vec<(usize, usize)> = vec![(0, 0); bank.parts.len()];
    {
        let mut i = 0usize;
        while i < units.len() {
            let pi = units[i].0;
            let s = i;
            while i < units.len() && units[i].0 == pi {
                i += 1;
            }
            part_units[pi] = (s, i);
        }
    }

    struct W {
        cols: Vec<DecodeCol>,
        sel: Vec<u16>,
        cands: Vec<Cand>,
        locs: Vec<(u32, u16)>,
        /// [sqe-topn-gap] The worker's current n-th-best candidate (set
        /// at each prune): rows that cannot beat it are skipped WITHOUT
        /// the per-row cell materialization — the bounded-selection
        /// down-pass the keyed scan was missing (10M Cand allocations on
        /// a predicate-free ORDER BY+LIMIT scan was the 13.7x cell).
        worst: Option<Cand>,
    }
    let keyed = !keys.is_empty();
    let prune_at = bound_n.map(|n| (2 * n).max(256));
    // [sqe-topn-gap] Part-subdivided claim plane (see pool::part_claim_chunks):
    // shallow part planes starve the claim-depth guard to n/4 workers.
    // cmp_cand is a STRICT total order ((ui, row) tiebreak), so the
    // winner set is unique whatever the claim schedule — byte-identical
    // across grains and widths.
    let chunks = crate::pool::part_claim_chunks(&part_units, pool.threads());
    let chunks = &chunks;
    let t_p1 = std::time::Instant::now();
    let mut states = pool.run_finish(
        chunks.len(),
        |_| W {
            cols: dset.iter().map(|&(a, f)| DecodeCol::new(a, f)).collect(),
            sel: Vec::new(),
            cands: Vec::new(),
            locs: Vec::new(),
            worst: None,
        },
        |s: &mut W, ci| {
            let (pi, u0, u1) = chunks[ci];
            for ui in u0..u1 {
                let (_, g, rows32, _) = units[ui];
                let rows = rows32 as usize;
                if rows == 0 {
                    continue;
                }
                if !keyed {
                    if let Some(n) = bound_n {
                        if s.locs.len() >= n {
                            break;
                        }
                    }
                }
                if iterms
                    .iter()
                    .enumerate()
                    .any(|(ti, t)| !t.zone_may_pass(smas[ti].mins[ui], smas[ti].maxs[ui]))
                {
                    continue;
                }
                // [psma-consume] Zone said maybe: intersect the granule's
                // PSMA candidate slices (one probe per selective-class
                // conjunct, never per-row); rows outside the window are
                // excluded without evaluation.
                let (rlo, rhi) = iterms.iter().enumerate().fold((0usize, rows), |w, (ti, t)| {
                    crate::psmaface::narrow(
                        w,
                        psmas[ti].as_ref().and_then(|pf| {
                            pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                        }),
                    )
                });
                if rlo >= rhi {
                    continue;
                }
                let mut decoded = vec![false; s.cols.len()];
                for &di in &pred_di {
                    if !decoded[di] {
                        let c = &mut s.cols[di];
                        let cur = c.cc.get(bank, pi);
                        c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                        let cur = c.cc.get(bank, pi);
                        c.scr.decode_full(cur, g, rows);
                        decoded[di] = true;
                    }
                }
                // [psma-consume, oracle] slice-complement emptiness: a
                // slice that hid a matching valid row panics loudly.
                #[cfg(feature = "oracle")]
                for (ti, t) in iterms.iter().enumerate() {
                    if let Some(sl) = psmas[ti].as_ref().and_then(|pf| {
                        pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                    }) {
                        let c = &s.cols[iterm_di[ti]];
                        crate::psmaface::oracle_check_complement(
                            t,
                            sl.0 as usize,
                            (sl.1 as usize).min(rows),
                            rows,
                            |r| c.all_valid || c.scr.row_valid(r),
                            |r| c.face.word_key(c.scr.datums[r]),
                        );
                    }
                }
                s.sel.clear();
                'rows: for r in rlo..rhi {
                    for (ti, t) in iterms.iter().enumerate() {
                        let c = &s.cols[iterm_di[ti]];
                        let valid = c.all_valid || c.scr.row_valid(r);
                        if !valid || !t.eval(c.face.word_key(c.scr.datums[r])) {
                            continue 'rows;
                        }
                    }
                    for (ti, t) in vterms.iter().enumerate() {
                        let c = &s.cols[vterm_di[ti]];
                        let valid = c.all_valid || c.scr.row_valid(r);
                        if !valid || !t.eval(unsafe { byte_payload(c.face, c.scr.datums[r]) }) {
                            continue 'rows;
                        }
                    }
                    s.sel.push(r as u16);
                }
                if s.sel.is_empty() {
                    continue;
                }
                if !keyed {
                    let take = match bound_n {
                        Some(n) => (n - s.locs.len()).min(s.sel.len()),
                        None => s.sel.len(),
                    };
                    s.locs.extend(s.sel[..take].iter().map(|&r| (ui as u32, r)));
                    continue;
                }
                for &di in &key_di {
                    if !decoded[di] {
                        let c = &mut s.cols[di];
                        let cur = c.cc.get(bank, pi);
                        c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                        let cur = c.cc.get(bank, pi);
                        c.scr.decode_full(cur, g, rows);
                        decoded[di] = true;
                    }
                }
                for &r in &s.sel {
                    // [sqe-topn-gap] Bound short-circuit: a row that is
                    // not strictly better than the worker's n-th-best
                    // candidate can never be a winner — skip it before
                    // any cell materialization (ties beyond the bound
                    // are cut arbitrarily, exactly as the prune below
                    // already cuts them).
                    if let Some(w) = &s.worst {
                        if row_cmp_cand(
                            &s.cols,
                            &key_di,
                            &key_cols,
                            &keys,
                            ui as u32,
                            r as usize,
                            w,
                        ) != Ordering::Less
                        {
                            continue;
                        }
                    }
                    let cells: Vec<KeyCell> = key_di
                        .iter()
                        .zip(&key_cols)
                        .map(|(&di, &(_, face))| {
                            let c = &s.cols[di];
                            let valid = c.all_valid || c.scr.row_valid(r as usize);
                            if byte_face(face) {
                                KeyCell::B(valid.then(|| unsafe {
                                    byte_payload(face, c.scr.datums[r as usize]).to_vec()
                                }))
                            } else {
                                KeyCell::I(
                                    valid.then(|| face.word_key(c.scr.datums[r as usize])),
                                )
                            }
                        })
                        .collect();
                    s.cands.push(Cand { cells, ui: ui as u32, row: r });
                    if let (Some(p), Some(n)) = (prune_at, bound_n) {
                        if s.cands.len() >= p && n > 0 {
                            s.cands
                                .select_nth_unstable_by(n - 1, |a, b| cmp_cand(a, b, &keys));
                            s.cands.truncate(n);
                            // The retained set's maximum IS the n-th
                            // smallest (select_nth leaves it at n-1).
                            s.worst = Some(Cand {
                                cells: clone_cells(&s.cands[n - 1].cells),
                                ui: s.cands[n - 1].ui,
                                row: s.cands[n - 1].row,
                            });
                        }
                    }
                }
            }
        },
        |mut s| {
            s.cols.drain(..).for_each(DecodeCol::park);
            s
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    // Merge to the final row set (locator-ordered for determinism).
    let t_p2 = std::time::Instant::now();
    let mut finals: Vec<(u32, u16)> = if keyed {
        let mut all: Vec<Cand> = states.iter_mut().flat_map(|s| s.cands.drain(..)).collect();
        let n = bound_n.expect("keyed scans carry a bound");
        if all.len() > n {
            if n > 0 {
                all.select_nth_unstable_by(n - 1, |a, b| cmp_cand(a, b, &keys));
            }
            all.truncate(n);
        }
        all.iter().map(|c| (c.ui, c.row)).collect()
    } else {
        let mut all: Vec<(u32, u16)> =
            states.iter_mut().flat_map(|s| s.locs.drain(..)).collect();
        all.sort_unstable();
        if let Some(n) = bound_n {
            all.truncate(n);
        }
        all
    };
    finals.sort_unstable();
    let mut groups: Vec<(u32, Vec<u16>)> = Vec::new();
    for (ui, r) in finals {
        match groups.last_mut() {
            Some((u, sel)) if *u == ui => sel.push(r),
            _ => groups.push((ui, vec![r])),
        }
    }

    // Pass 2: decode ONLY the final rows' output columns.
    let nrows: usize = groups.iter().map(|(_, s)| s.len()).sum();
    let mut cols_out: Vec<AnswerCol> = Vec::with_capacity(emits.len());
    for &(attno, ty) in &emits {
        let face = bank.face(attno);
        let text = byte_face(face);
        let mut scr = crate::scan::scratch_fetch();
        let mut cc = CurCache::new(attno);
        let mut words: Vec<i64> = Vec::with_capacity(if text { 0 } else { nrows });
        let mut bytes = BytesBuild::new();
        let mut mask: Vec<bool> = Vec::with_capacity(nrows);
        for (ui, sel) in &groups {
            let (pi, g, rows32, _) = units[*ui as usize];
            let rows = rows32 as usize;
            let cur = cc.get(bank, pi);
            let all_valid = scr.validity(cur, g, rows).all_valid();
            let cur = cc.get(bank, pi);
            if sel.len() == rows {
                scr.decode_full(cur, g, rows);
                for r in 0..rows {
                    let valid = all_valid || scr.row_valid(r);
                    mask.push(valid);
                    if text {
                        bytes.push(if valid {
                            unsafe { byte_payload(face, scr.datums[r]) }
                        } else {
                            b""
                        });
                    } else {
                        words.push(if valid { face.word_key(scr.datums[r]) } else { 0 });
                    }
                }
            } else {
                scr.decode_sel(cur, g, sel);
                for (i, &r) in sel.iter().enumerate() {
                    let valid = all_valid || scr.row_valid(r as usize);
                    mask.push(valid);
                    if text {
                        bytes.push(if valid {
                            unsafe { byte_payload(face, scr.datums[i]) }
                        } else {
                            b""
                        });
                    } else {
                        words.push(if valid { face.word_key(scr.datums[i]) } else { 0 });
                    }
                }
            }
        }
        crate::scan::scratch_park(scr);
        let validity = if mask.iter().all(|&v| v) {
            Validity::AllValid
        } else {
            Validity::Mask(mask)
        };
        let data = if text {
            ColData::Bytes { arena: bytes.arena, offs: bytes.offs }
        } else {
            ColData::I64(words)
        };
        cols_out.push(AnswerCol { ty, data, validity });
    }
    let a = AnswerSet { cols: cols_out, nrows, note: None, head_note: None };
    crate::engine::phn(node, "pass2", t_p2);
    crate::engine::phn(node, "total", t_all);
    a
}
