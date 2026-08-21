//! SortGrouped stencil (the tier-2 general-aggregates family): the
//! holistic / order-sensitive aggregates the cell law cannot host —
//! string_agg / array_agg (ORDER BY required in v1) and the
//! percentile_disc / percentile_cont / mode WITHIN GROUP ordered-set
//! class. Design of record: docs/design/sqe/sort-grouped-family.md
//! (Michael's charter, 2026-08-18).
//!
//! Physical law (§2.2): NOT a wider fold cell — a different shape:
//!
//!   pass A (pool, part-grain claim — the ScanServe pass-1 stencil):
//!     zone-skip int conjuncts, decode + 3VL-evaluate the conjunction
//!     (NULL never passes), decode exactly the group-key / sort-key /
//!     agg-input cells of survivors, and scatter
//!     (radix_of(hash(group key)), cells, ingest ordinal) into
//!     per-worker per-partition buffers. Group ownership is a pure
//!     function of the KEY BYTES — width changes who executes a
//!     partition, never which partition a group lives in.
//!
//!   pass B (partition-owned, zero merge): per partition, concatenate
//!     the per-worker slices, sort by
//!         (group key, statement sort spec, ingest ordinal (unit, row))
//!     — the ingest-ordinal tiebreak TOTALIZES the order (zone_order.rs
//!     WKey precedent), so answers are byte-identical at any pool width
//!     (the election-inputs law §1; Michael's tie ruling 2026-08-18:
//!     the engine's deterministic total order is one lawful member of
//!     PG's tie class; oracle gates are tie-tolerant) — then one
//!     run-boundary walk finalizes every aggregate per group.
//!
//! Cross-group answer order: partition index, then in-partition sort
//! order — data-pure. Aggregate laws are C ground truth (§1):
//! string_agg skips NULL values, delimiter between values / NULL delim
//! = concat (varlena.c:5446-5494/5618-5636); array_agg KEEPS NULL
//! elements (array_userfuncs.c:587-591); the ordered-set class excludes
//! NULLs from the sort and N (orderedsetaggs.c:369-372), disc = rank
//! ceil(p·N) (:474), cont = floor/ceil(p·(N−1)) + float8_lerp
//! (:575-603), mode = first maximal run, strictly-greater replacement
//! (:1077-1120); empty/all-NULL input answers NULL.
//!
//! Memory: witnessed byte-budget admission (planner::check_server
//! sortagg arm) — typed refusal over budget, no spill in v1 (§5).

use std::cmp::Ordering;

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::Face;
use crate::engine::SqeCtx;
use crate::grouped::{hash64, hash_bytes, radix_of, RADIX_P};
use crate::ir::{AggDelim, AggOp, PlanNode, TopKKey};
use crate::scan::{varlena_payload, CurCache, Scratch};

/// One decoded survivor cell (None = SQL NULL). Word cells carry the
/// order-preserving AND value-preserving i64 embed (`Face::word_key` —
/// sign-extended ints/dates/bools); byte cells own their payload copy.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    I(Option<i64>),
    B(Option<Vec<u8>>),
}

impl Cell {
    #[inline]
    fn is_null(&self) -> bool {
        matches!(self, Cell::I(None) | Cell::B(None))
    }
}

/// NULL-absolute cell comparator (the scan_serve `cmp_cand` law: Bytes =
/// memcmp under the C-collation admission).
#[inline]
fn cmp_cell(a: &Cell, b: &Cell, desc: bool, nulls_first: bool) -> Ordering {
    let ord = match (a, b) {
        (Cell::I(None), Cell::I(None)) | (Cell::B(None), Cell::B(None)) => return Ordering::Equal,
        (Cell::I(None), _) | (Cell::B(None), _) => {
            return if nulls_first { Ordering::Less } else { Ordering::Greater }
        }
        (_, Cell::I(None)) | (_, Cell::B(None)) => {
            return if nulls_first { Ordering::Greater } else { Ordering::Less }
        }
        (Cell::I(Some(x)), Cell::I(Some(y))) => x.cmp(y),
        (Cell::B(Some(x)), Cell::B(Some(y))) => x.cmp(y),
        _ => unreachable!("cell class drift within one column (lowering bug)"),
    };
    if desc {
        ord.reverse()
    } else {
        ord
    }
}

/// One scattered survivor row: `cells` in the node's decode-set order,
/// `ord` = the ingest ordinal (unit index, row-in-granule) — the total
/// tiebreak byte-identity rides on.
struct SRow {
    cells: Vec<Cell>,
    ord: (u32, u16),
}

/// Group ownership: a pure function of the group-key CELLS (never of
/// scheduling). NULL keys hash to a fixed tag — SQL GROUP BY groups
/// NULLs together.
#[inline]
fn key_hash(cells: &[Cell], gdi: &[usize]) -> u64 {
    let mut h = 0x9E37_79B9_7F4A_7C15u64;
    for &i in gdi {
        h = match &cells[i] {
            Cell::I(None) | Cell::B(None) => hash64(h ^ 0xA5A5_A5A5),
            Cell::I(Some(v)) => hash64(h ^ hash64(*v as u64)),
            Cell::B(Some(b)) => hash64(h ^ hash_bytes(b)),
        };
    }
    h
}

/// One finalized aggregate answer cell.
enum AggCell {
    W(Option<i64>),
    B(Option<Vec<u8>>),
    F(Option<f64>),
    /// array_agg: the group's element cells in run order (None = NULL
    /// array — the ungrouped-empty case only).
    Arr(Option<Vec<Cell>>),
}

struct OutRow {
    keys: Vec<Cell>,
    aggs: Vec<AggCell>,
}

/// Finalize one aggregate over a group's SORTED row slice.
fn finalize(
    op: AggOp,
    frac: Option<f64>,
    delim: &AggDelim,
    ai: usize,
    rows: &[&SRow],
) -> AggCell {
    match op {
        AggOp::StringAgg => {
            // Delimiter BETWEEN values, none leading; NULL values are
            // skipped entirely; all-NULL input answers NULL.
            let mut out: Option<Vec<u8>> = None;
            for r in rows {
                let Cell::B(Some(b)) = &r.cells[ai] else { continue };
                match &mut out {
                    None => out = Some(b.clone()),
                    Some(buf) => {
                        if let AggDelim::Bytes(d) = delim {
                            buf.extend_from_slice(d);
                        }
                        buf.extend_from_slice(b);
                    }
                }
            }
            AggCell::B(out)
        }
        AggOp::ArrayAgg => {
            // NULL elements are KEPT (array_userfuncs.c:587-591).
            AggCell::Arr(Some(rows.iter().map(|r| r.cells[ai].clone()).collect()))
        }
        AggOp::ArrayAggDistinct => {
            // [aggqual] DISTINCT over the sorted run: adjacent-equal
            // skip (the sort key IS the argument — equal values, and
            // the NULL class, are contiguous; two NULLs are NOT
            // DISTINCT, so one NULL element survives).
            let mut out: Vec<Cell> = Vec::new();
            for r in rows {
                let c = &r.cells[ai];
                if out.last() != Some(c) {
                    out.push(c.clone());
                }
            }
            AggCell::Arr(Some(out))
        }
        AggOp::PercentileDisc => {
            // Non-null inputs in run order; rank = ceil(p·N), min 1.
            let vals: Vec<&Cell> =
                rows.iter().map(|r| &r.cells[ai]).filter(|c| !c.is_null()).collect();
            let n = vals.len();
            if n == 0 {
                return match rows.first().map(|r| &r.cells[ai]) {
                    Some(Cell::B(_)) => AggCell::B(None),
                    _ => AggCell::W(None),
                };
            }
            let p = frac.expect("disc without a direct arg");
            let mut rownum = (p * n as f64).ceil() as i64;
            if rownum < 1 {
                rownum = 1;
            }
            match vals[(rownum - 1) as usize] {
                Cell::I(v) => AggCell::W(*v),
                Cell::B(b) => AggCell::B(b.clone()),
            }
        }
        AggOp::PercentileCont => {
            // floor/ceil(p·(N−1)) bracket + C's float8_lerp
            // (orderedsetaggs.c:503-509: lo + pct·(hi − lo)).
            let vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| match &r.cells[ai] {
                    Cell::I(Some(v)) => Some(*v as f64),
                    Cell::I(None) => None,
                    Cell::B(_) => unreachable!("cont over a byte face (admission bug)"),
                })
                .collect();
            let n = vals.len();
            if n == 0 {
                return AggCell::F(None);
            }
            let p = frac.expect("cont without a direct arg");
            let first = (p * (n - 1) as f64).floor();
            let second = (p * (n - 1) as f64).ceil();
            let fv = vals[first as usize];
            let v = if first == second {
                fv
            } else {
                let sv = vals[second as usize];
                let proportion = p * (n - 1) as f64 - first;
                fv + proportion * (sv - fv)
            };
            AggCell::F(Some(v))
        }
        AggOp::Mode => {
            // First maximal run in the deterministic sort order; a new
            // run replaces only on STRICTLY greater count (§1.4).
            let mut mode: Option<(&Cell, usize)> = None;
            let mut last: Option<(&Cell, usize)> = None;
            for r in rows {
                let c = &r.cells[ai];
                if c.is_null() {
                    continue;
                }
                match &mut last {
                    Some((lc, cnt)) if *lc == c => *cnt += 1,
                    _ => last = Some((c, 1)),
                }
                let (lc, cnt) = last.as_ref().expect("just set");
                match &mut mode {
                    Some((_, mcnt)) if *cnt > *mcnt => mode = Some((lc, *cnt)),
                    None => mode = Some((lc, *cnt)),
                    _ => {}
                }
            }
            match mode.map(|(c, _)| c) {
                Some(Cell::I(v)) => AggCell::W(*v),
                Some(Cell::B(b)) => AggCell::B(b.clone()),
                None => match rows.first().map(|r| &r.cells[ai]) {
                    Some(Cell::B(_)) => AggCell::B(None),
                    _ => AggCell::W(None),
                },
            }
        }
        other => unreachable!("sort_grouped: non-tier-2 op {other:?} (admission bug)"),
    }
}

struct DecodeCol {
    face: Face,
    scr: Scratch,
    cc: CurCache,
    all_valid: bool,
}

impl DecodeCol {
    /// Depot-riding constructor (the scratch-init discipline): the decode
    /// arena comes reset from the worker depot; the cursor is
    /// per-engagement — never parked.
    fn fetch(attno: u32, face: Face) -> DecodeCol {
        DecodeCol {
            face,
            scr: crate::scan::scratch_fetch(),
            cc: CurCache::new(attno),
            all_valid: true,
        }
    }
}

pub fn run_sort_grouped(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let t_all = std::time::Instant::now();
    let sa = node.params.sortagg.as_ref().expect("SortGrouped node without a sortagg spec");
    assert!(node.params.key_exprs.is_empty(), "sort_grouped: plain group keys only (v1)");
    assert_eq!(sa.delims.len(), node.agg.len(), "delim slots align with the agg list");
    let gcols: &[u32] = &node.params.group_cols;
    let grouped = !gcols.is_empty();

    // ---- decode set: unique attnos of group keys ++ sort keys ++ inputs ----
    let mut dset: Vec<(u32, Face)> = Vec::new();
    let mut stage = |dset: &mut Vec<(u32, Face)>, a: u32| -> usize {
        match dset.iter().position(|&(c, _)| c == a) {
            Some(i) => i,
            None => {
                dset.push((a, bank.face(a)));
                dset.len() - 1
            }
        }
    };
    let gdi: Vec<usize> = gcols.iter().map(|&c| stage(&mut dset, c)).collect();
    let sdi: Vec<usize> = sa.keys.iter().map(|k| stage(&mut dset, k.col)).collect();
    let adi: Vec<usize> = node
        .agg
        .iter()
        .map(|a| stage(&mut dset, a.col.expect("tier-2 agg carries an input column")))
        .collect();

    let iterms: Vec<crate::ir::PredTerm> =
        node.pred.iter().flat_map(|p| p.terms.iter().cloned()).collect();
    let vterms: Vec<crate::ir::VarPredTerm> =
        node.pred.iter().flat_map(|p| p.var_terms.iter().cloned()).collect();
    // Predicate columns join the decode set (their cells are decoded but
    // not scattered unless a consumer staged them above).
    let mut pset: Vec<(u32, Face)> = dset.clone();
    let mut pstage = |pset: &mut Vec<(u32, Face)>, a: u32| -> usize {
        match pset.iter().position(|&(c, _)| c == a) {
            Some(i) => i,
            None => {
                pset.push((a, bank.face(a)));
                pset.len() - 1
            }
        }
    };
    let iterm_di: Vec<usize> = iterms.iter().map(|t| pstage(&mut pset, t.col)).collect();
    let vterm_di: Vec<usize> = vterms.iter().map(|t| pstage(&mut pset, t.col)).collect();
    let ncells = dset.len();

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

    // ---- pass A: filter -> decode survivor cells -> scatter by key hash ----
    struct W {
        cols: Vec<DecodeCol>,
        sel: Vec<u16>,
        parts: Vec<Vec<SRow>>,
    }
    let t_p1 = std::time::Instant::now();
    let states = pool.run_finish(
        bank.parts.len(),
        |_| W {
            cols: pset.iter().map(|&(a, f)| DecodeCol::fetch(a, f)).collect(),
            sel: Vec::new(),
            parts: (0..RADIX_P).map(|_| Vec::new()).collect(),
        },
        |s: &mut W, pi| {
            let (u0, u1) = part_units[pi];
            for ui in u0..u1 {
                let (_, g, rows32, _) = units[ui];
                let rows = rows32 as usize;
                if rows == 0 {
                    continue;
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
                // conjunct, never per-row); an empty window skips the
                // granule before any decode.
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
                // Decode predicate columns, filter row-major (3VL).
                let mut decoded = vec![false; s.cols.len()];
                for di in iterm_di.iter().chain(vterm_di.iter()) {
                    if !decoded[*di] {
                        let c = &mut s.cols[*di];
                        let cur = c.cc.get(bank, pi);
                        c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                        let cur = c.cc.get(bank, pi);
                        c.scr.decode_full(cur, g, rows);
                        decoded[*di] = true;
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
                        if !valid || !t.eval(unsafe { varlena_payload(c.scr.datums[r]) }) {
                            continue 'rows;
                        }
                    }
                    s.sel.push(r as u16);
                }
                if s.sel.is_empty() {
                    continue;
                }
                // Late materialization: decode the scattered cells for
                // survivors only (decode_sel on partial granules).
                let full = s.sel.len() == rows;
                for di in 0..ncells {
                    if decoded[di] {
                        continue;
                    }
                    let c = &mut s.cols[di];
                    let cur = c.cc.get(bank, pi);
                    c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                    let cur = c.cc.get(bank, pi);
                    if full {
                        c.scr.decode_full(cur, g, rows);
                    } else {
                        c.scr.decode_sel(cur, g, &s.sel);
                    }
                    decoded[di] = true;
                }
                for (i, &r) in s.sel.iter().enumerate() {
                    let cells: Vec<Cell> = (0..ncells)
                        .map(|di| {
                            let c = &s.cols[di];
                            let valid = c.all_valid || c.scr.row_valid(r as usize);
                            // Full decodes (whole granule / predicate
                            // columns) index by row; decode_sel by
                            // selection position (the scan_serve pass-2
                            // convention). Validity always by row.
                            let pred_col =
                                iterm_di.contains(&di) || vterm_di.contains(&di);
                            let d = if full || pred_col {
                                c.scr.datums[r as usize]
                            } else {
                                c.scr.datums[i]
                            };
                            if c.face == Face::Varlena {
                                Cell::B(valid.then(|| unsafe { varlena_payload(d) }.to_vec()))
                            } else {
                                Cell::I(valid.then(|| c.face.word_key(d)))
                            }
                        })
                        .collect();
                    let part = if grouped { radix_of(key_hash(&cells, &gdi)) } else { 0 };
                    s.parts[part].push(SRow { cells, ord: (ui as u32, r) });
                }
            }
        },
        // Worker-side finish: decode arenas park on THIS worker's depot
        // (cursors drop); only the scattered rows cross back.
        |s: W| {
            for c in s.cols {
                crate::scan::scratch_park(c.scr);
            }
            s.parts
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    // ---- pass B: partition-owned total sort + run-boundary walk ------------
    let t_p2 = std::time::Instant::now();
    let gkey_cmp = |a: &SRow, b: &SRow| -> Ordering {
        for &i in &gdi {
            let o = cmp_cell(&a.cells[i], &b.cells[i], false, false);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    };
    let row_cmp = |a: &SRow, b: &SRow, keys: &[TopKKey]| -> Ordering {
        let o = gkey_cmp(a, b);
        if o != Ordering::Equal {
            return o;
        }
        for (ki, k) in keys.iter().enumerate() {
            let o = cmp_cell(&a.cells[sdi[ki]], &b.cells[sdi[ki]], k.desc, k.nulls_first);
            if o != Ordering::Equal {
                return o;
            }
        }
        a.ord.cmp(&b.ord)
    };
    let fracs: Vec<Option<f64>> =
        node.agg.iter().map(|a| a.direct.map(f64::from_bits)).collect();
    let owned: Vec<(usize, Vec<OutRow>)> = pool
        .run(
            RADIX_P,
            |_| Vec::new(),
            |out: &mut Vec<(usize, Vec<OutRow>)>, part| {
                let mut rows: Vec<&SRow> = states
                    .iter()
                    .flat_map(|s| s[part].iter())
                    .collect();
                if rows.is_empty() {
                    return;
                }
                // A total order (ingest-ordinal tiebreak) makes stability
                // irrelevant: any comparison sort is admissible.
                rows.sort_unstable_by(|a, b| row_cmp(a, b, &sa.keys));
                let mut prows: Vec<OutRow> = Vec::new();
                let mut s = 0usize;
                while s < rows.len() {
                    let mut e = s + 1;
                    while e < rows.len() && gkey_cmp(rows[s], rows[e]) == Ordering::Equal {
                        e += 1;
                    }
                    let run = &rows[s..e];
                    prows.push(OutRow {
                        keys: gdi.iter().map(|&i| run[0].cells[i].clone()).collect(),
                        aggs: node
                            .agg
                            .iter()
                            .enumerate()
                            .map(|(x, a)| {
                                finalize(a.op, fracs[x], &sa.delims[x], adi[x], run)
                            })
                            .collect(),
                    });
                    s = e;
                }
                out.push((part, prows));
            },
        )
        .into_iter()
        .flatten()
        .collect();
    let mut by_part: Vec<(usize, Vec<OutRow>)> = owned;
    by_part.sort_by_key(|(p, _)| *p);
    let mut out_rows: Vec<OutRow> = by_part.into_iter().flat_map(|(_, r)| r).collect();

    // Ungrouped over an empty survivor set: ONE all-NULL answer row (the
    // PG plain-agg law).
    if !grouped && out_rows.is_empty() {
        out_rows.push(OutRow {
            keys: Vec::new(),
            aggs: node
                .agg
                .iter()
                .map(|a| match a.op {
                    AggOp::StringAgg => AggCell::B(None),
                    AggOp::ArrayAgg | AggOp::ArrayAggDistinct => AggCell::Arr(None),
                    AggOp::PercentileCont => AggCell::F(None),
                    AggOp::PercentileDisc | AggOp::Mode => {
                        if a.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
                            AggCell::B(None)
                        } else {
                            AggCell::W(None)
                        }
                    }
                    other => unreachable!("sort_grouped: op {other:?}"),
                })
                .collect(),
        });
    }
    crate::engine::phn(node, "pass2", t_p2);

    // ---- typed emit: key columns then agg columns ---------------------------
    let t_e = std::time::Instant::now();
    let nrows = out_rows.len();
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    for (ki, &gc) in gcols.iter().enumerate() {
        let ty = node.ty_of(gc);
        if ty.is_varlena() {
            let mut b = BytesBuild::new();
            let mut mask = Vec::with_capacity(nrows);
            for r in &out_rows {
                match &r.keys[ki] {
                    Cell::B(Some(x)) => {
                        b.push(x);
                        mask.push(true);
                    }
                    _ => {
                        b.push(b"");
                        mask.push(false);
                    }
                }
            }
            let mut c = b.finish(ty);
            if !mask.iter().all(|&v| v) {
                c.validity = Validity::Mask(mask);
            }
            cols_out.push(c);
        } else {
            let v: Vec<Option<i64>> = out_rows
                .iter()
                .map(|r| match &r.keys[ki] {
                    Cell::I(x) => *x,
                    Cell::B(_) => unreachable!("key cell class drift"),
                })
                .collect();
            cols_out.push(AnswerCol::i64s_opt(ty, v));
        }
    }
    for (x, a) in node.agg.iter().enumerate() {
        match a.op {
            AggOp::StringAgg => {
                let mut b = BytesBuild::new();
                let mut mask = Vec::with_capacity(nrows);
                for r in &out_rows {
                    match &r.aggs[x] {
                        AggCell::B(Some(v)) => {
                            b.push(v);
                            mask.push(true);
                        }
                        _ => {
                            b.push(b"");
                            mask.push(false);
                        }
                    }
                }
                let mut c = b.finish(a.out);
                if !mask.iter().all(|&v| v) {
                    c.validity = Validity::Mask(mask);
                }
                cols_out.push(c);
            }
            AggOp::ArrayAgg | AggOp::ArrayAggDistinct => {
                let text = a.in_ty.map(|t| t.is_varlena()).unwrap_or(false);
                let mut offs: Vec<u32> = vec![0];
                let mut mask: Vec<bool> = Vec::with_capacity(nrows);
                let mut ewords: Vec<Option<i64>> = Vec::new();
                let mut ebytes = BytesBuild::new();
                let mut emask: Vec<bool> = Vec::new();
                let mut n_elems = 0u32;
                for r in &out_rows {
                    match &r.aggs[x] {
                        AggCell::Arr(Some(elems)) => {
                            mask.push(true);
                            for c in elems {
                                match c {
                                    Cell::I(v) => ewords.push(*v),
                                    Cell::B(Some(v)) => {
                                        ebytes.push(v);
                                        emask.push(true);
                                    }
                                    Cell::B(None) => {
                                        ebytes.push(b"");
                                        emask.push(false);
                                    }
                                }
                            }
                            n_elems += elems.len() as u32;
                        }
                        AggCell::Arr(None) => mask.push(false),
                        _ => unreachable!("array cell class drift"),
                    }
                    offs.push(n_elems);
                }
                let elems = if text {
                    let mut c = ebytes.finish(a.out);
                    if !emask.iter().all(|&v| v) {
                        c.validity = Validity::Mask(emask);
                    }
                    c
                } else {
                    AnswerCol::i64s_opt(a.out, ewords)
                };
                let validity = if mask.iter().all(|&v| v) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                cols_out.push(AnswerCol {
                    ty: a.out,
                    data: ColData::List { elems: Box::new(elems), offs },
                    validity,
                });
            }
            AggOp::PercentileCont => {
                let mut v: Vec<f64> = Vec::with_capacity(nrows);
                let mut mask: Vec<bool> = Vec::with_capacity(nrows);
                for r in &out_rows {
                    match &r.aggs[x] {
                        AggCell::F(f) => {
                            mask.push(f.is_some());
                            v.push(f.unwrap_or(0.0));
                        }
                        _ => unreachable!("cont cell class drift"),
                    }
                }
                let mut c = AnswerCol::f64s(a.out, v);
                if !mask.iter().all(|&m| m) {
                    c.validity = Validity::Mask(mask);
                }
                cols_out.push(c);
            }
            AggOp::PercentileDisc | AggOp::Mode => {
                if a.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
                    let mut b = BytesBuild::new();
                    let mut mask = Vec::with_capacity(nrows);
                    for r in &out_rows {
                        match &r.aggs[x] {
                            AggCell::B(Some(v)) => {
                                b.push(v);
                                mask.push(true);
                            }
                            _ => {
                                b.push(b"");
                                mask.push(false);
                            }
                        }
                    }
                    let mut c = b.finish(a.out);
                    if !mask.iter().all(|&v| v) {
                        c.validity = Validity::Mask(mask);
                    }
                    cols_out.push(c);
                } else {
                    let v: Vec<Option<i64>> = out_rows
                        .iter()
                        .map(|r| match &r.aggs[x] {
                            AggCell::W(v) => *v,
                            _ => unreachable!("disc/mode cell class drift"),
                        })
                        .collect();
                    cols_out.push(AnswerCol::i64s_opt(a.out, v));
                }
            }
            other => unreachable!("sort_grouped: op {other:?}"),
        }
    }
    let a = AnswerSet::from_cols(cols_out);
    crate::engine::phn(node, "epilogue", t_e);
    crate::engine::phn(node, "total", t_all);
    a
}

// ---------------------------------------------------------------------------
// Unit gates: the C finisher laws over hand-built sorted runs.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn wrow(v: Option<i64>) -> SRow {
        SRow { cells: vec![Cell::I(v)], ord: (0, 0) }
    }
    fn brow(v: Option<&[u8]>) -> SRow {
        SRow { cells: vec![Cell::B(v.map(|b| b.to_vec()))], ord: (0, 0) }
    }
    fn refs(rows: &[SRow]) -> Vec<&SRow> {
        rows.iter().collect()
    }

    #[test]
    fn string_agg_delim_between_nulls_skipped_null_delim_concats() {
        let rows = vec![brow(Some(b"a")), brow(None), brow(Some(b"b")), brow(Some(b"c"))];
        let d = AggDelim::Bytes(b", ".to_vec());
        let AggCell::B(Some(v)) = finalize(AggOp::StringAgg, None, &d, 0, &refs(&rows)) else {
            panic!()
        };
        assert_eq!(v, b"a, b, c");
        let AggCell::B(Some(v)) =
            finalize(AggOp::StringAgg, None, &AggDelim::Null, 0, &refs(&rows))
        else {
            panic!()
        };
        assert_eq!(v, b"abc");
        // all-NULL input answers NULL
        let rows = vec![brow(None), brow(None)];
        assert!(matches!(
            finalize(AggOp::StringAgg, None, &d, 0, &refs(&rows)),
            AggCell::B(None)
        ));
    }

    #[test]
    fn array_agg_keeps_nulls() {
        let rows = vec![wrow(Some(1)), wrow(None), wrow(Some(2))];
        let AggCell::Arr(Some(e)) =
            finalize(AggOp::ArrayAgg, None, &AggDelim::None, 0, &refs(&rows))
        else {
            panic!()
        };
        assert_eq!(e, vec![Cell::I(Some(1)), Cell::I(None), Cell::I(Some(2))]);
    }

    #[test]
    fn percentile_disc_rank_law() {
        // C: rank = ceil(p*N), min 1 — nulls excluded from N.
        let rows: Vec<SRow> =
            [Some(10), None, Some(20), Some(30), Some(40)].map(wrow).into();
        let d = AggDelim::None;
        let at = |p: f64| match finalize(AggOp::PercentileDisc, Some(p), &d, 0, &refs(&rows)) {
            AggCell::W(v) => v,
            _ => panic!(),
        };
        assert_eq!(at(0.0), Some(10)); // rank clamps to 1
        assert_eq!(at(0.25), Some(10)); // ceil(1.0) = 1
        assert_eq!(at(0.26), Some(20));
        assert_eq!(at(0.5), Some(20)); // ceil(2.0) = 2
        assert_eq!(at(1.0), Some(40));
        let empty = vec![wrow(None)];
        assert!(matches!(
            finalize(AggOp::PercentileDisc, Some(0.5), &d, 0, &refs(&empty)),
            AggCell::W(None)
        ));
    }

    #[test]
    fn percentile_cont_lerp_law() {
        // C: first=floor(p*(N-1)), second=ceil, lerp lo + pct*(hi-lo).
        let rows: Vec<SRow> = [Some(10), Some(20), Some(30), Some(41)].map(wrow).into();
        let d = AggDelim::None;
        let at = |p: f64| match finalize(AggOp::PercentileCont, Some(p), &d, 0, &refs(&rows)) {
            AggCell::F(v) => v,
            _ => panic!(),
        };
        assert_eq!(at(0.0), Some(10.0));
        assert_eq!(at(0.5), Some((20.0 + 30.0) / 2.0));
        assert_eq!(at(1.0), Some(41.0));
        // p=0.5 over N=4: p*(N-1) = 1.5 -> 20 + 0.5*(30-20) = 25.
        assert_eq!(at(0.5), Some(25.0));
        // median over N=2: exact midpoint of a lerp.
        let two: Vec<SRow> = [Some(1), Some(2)].map(wrow).into();
        let AggCell::F(Some(v)) =
            finalize(AggOp::PercentileCont, Some(0.5), &d, 0, &refs(&two))
        else {
            panic!()
        };
        assert_eq!(v, 1.5);
    }

    #[test]
    fn mode_first_maximal_run_wins() {
        // Runs: a×2, b×2, c×1 — the FIRST maximal run (a) wins; a later
        // equal count never replaces (strictly-greater law).
        let rows: Vec<SRow> = [
            Some(b"a".as_slice()),
            Some(b"a"),
            None,
            Some(b"b"),
            Some(b"b"),
            Some(b"c"),
        ]
        .map(brow)
        .into();
        let AggCell::B(Some(v)) =
            finalize(AggOp::Mode, None, &AggDelim::None, 0, &refs(&rows))
        else {
            panic!()
        };
        assert_eq!(v, b"a");
        // A strictly longer later run replaces.
        let rows: Vec<SRow> =
            [Some(1), Some(2), Some(2)].map(wrow).into();
        assert!(matches!(
            finalize(AggOp::Mode, None, &AggDelim::None, 0, &refs(&rows)),
            AggCell::W(Some(2))
        ));
    }
}
