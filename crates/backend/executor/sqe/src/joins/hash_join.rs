//! Hash-join stencil (P4-2 phase 1): partitioned build + fused staged
//! probe over two relations. Semantics = the ported row engine's
//! (nodehashjoin): NULL keys never match; `quals` decide matched-ness;
//! LEFT null-extends unmatched probe rows; SEMI emits once on the first
//! passing match; ANTI emits rows with no passing match. Design laws:
//! L2 partition law, per-worker-owned partitions (zero-merge), staged
//! probe sweeps, fp128 identity for text keys (entry_fp128), typed
//! budget refusal instead of spill (the grouped distinct set plane is
//! the exception: armed, it spills and serves).

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::{Bank, Face};
use crate::engine::SqeCtx;
use crate::fold::{combine_cell_fold, minmax_answer, scatter_cell_fold, AccumCell, AggFoldOp};
use crate::grouped::{hash64, Cells64, Cnt64};
use crate::ir::PredSpec;
use crate::joins::ir::{
    CaseTest, DimKey, DimSrc, DimStage, FilterStage, InSetFilter, JoinAggNode, JoinAggOp,
    JoinArith, JoinCaseLeg, JoinNode, JoinOut, JoinQual, JoinRefuse, JoinSide, JoinType,
    KeyXf, NumCellOp, OrTerm, StageFold, StageRows, StageSrc,
};
use crate::planner::partition_count;
use crate::scan::{varlena_payload, CurCache, GranValid, Scratch};
use crate::stencils::part_merge::{build_fps_cached, dict_faces};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Build-side memory ceiling. Placeholder constant pending P4-2d spill.
pub const JOIN_BUILD_BUDGET_BYTES: usize = 256 << 20;

/// The ceiling nodes are constructed with: `PGRUST_SQE_JOIN_BUDGET`
/// override, else the standing constant.
pub fn join_build_budget_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_SQE_JOIN_BUDGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(JOIN_BUILD_BUDGET_BYTES)
    })
}

/// Maximum equi-key lanes of one kind (word/text) per join.
pub const MAX_KEY_LANES: usize = 8;

/// Keyless (nest-loop) pair-product ceiling: the exact post-predicate
/// build cardinality times the probe side's witnessed row total must fit
/// under it, or the node refuses typed (P4-5 witness-gate law — the
/// O(n*m) sweep is only acceptable against a witnessed-small product).
pub const NL_PRODUCT_BUDGET_PAIRS: u64 = 1 << 26;

const STAGE: usize = 512;
const NO_ENTRY: u32 = u32::MAX;
const T_NULL: (u32, u32) = (u32::MAX, 0);

#[inline(always)]
fn key_hash(kw: &[i64], kf: &[u128]) -> u64 {
    let mut h = 0x9E37_79B9_7F4A_7C15u64;
    for &w in kw {
        h = hash64(h ^ hash64(w as u64));
    }
    for &f in kf {
        h = hash64(h ^ (f as u64) ^ hash64((f >> 64) as u64));
    }
    h
}

#[derive(Clone, Copy)]
struct ColPlan {
    attno: u32,
    face: Face,
    nf: bool,
    text: bool,
    needs_bytes: bool,
}

struct SidePlan {
    cols: Vec<ColPlan>,
    key_ci: Vec<usize>,
    qual_ci: Vec<usize>,
    pred_ci: Vec<usize>,
    out_ci: Vec<usize>,
    /// [sqe-mech3] extra decoded word lanes (the build side's dim-probe
    /// key columns), in caller order.
    extra_ci: Vec<usize>,
    /// [crossdim-or]/[semianti-flt] auxiliary lanes (staged-or term
    /// columns, filter host key/qual columns), in caller order; the
    /// bool requests byte decode.
    aux_ci: Vec<usize>,
}

fn side_plan(
    bank: &Bank,
    keys: &[u32],
    quals: &[u32],
    pred: &Option<PredSpec>,
    outs: &[u32],
    extra: &[u32],
    aux: &[(u32, bool)],
) -> SidePlan {
    let mut cols: Vec<ColPlan> = Vec::new();
    let mut add = |attno: u32, needs_bytes: bool| -> usize {
        if let Some(i) = cols.iter().position(|c| c.attno == attno) {
            cols[i].needs_bytes |= needs_bytes;
            return i;
        }
        // [packednum] staging class is the FACE's: a witnessed
        // PackedNumeric column word-stages (mantissa lane); only
        // Face::Varlena byte-stages.
        cols.push(ColPlan {
            attno,
            face: bank.face(attno),
            nf: bank.null_free(attno),
            text: matches!(bank.face(attno), Face::Varlena),
            needs_bytes,
        });
        cols.len() - 1
    };
    let key_ci: Vec<usize> = keys.iter().map(|&a| add(a, false)).collect();
    let qual_ci: Vec<usize> = quals.iter().map(|&a| add(a, false)).collect();
    // [colcmp] term lanes first, then each column pair (a, b) in order.
    let pred_cols: Vec<u32> = pred
        .iter()
        .flat_map(|p| {
            p.terms
                .iter()
                .map(|t| t.col)
                .chain(p.col_terms.iter().flat_map(|t| [t.a, t.b]))
        })
        .collect();
    let mut pred_ci: Vec<usize> = pred_cols.iter().map(|&a| add(a, false)).collect();
    // Varlena conjunct lanes decode full (payload bytes, never codes).
    let var_cols: Vec<u32> =
        pred.iter().flat_map(|p| p.var_terms.iter().map(|t| t.col)).collect();
    pred_ci.extend(var_cols.iter().map(|&a| add(a, true)));
    let out_ci: Vec<usize> = outs
        .iter()
        .map(|&a| {
            let text = matches!(bank.face(a), Face::Varlena);
            add(a, text)
        })
        .collect();
    let extra_ci: Vec<usize> = extra.iter().map(|&a| add(a, false)).collect();
    let aux_ci: Vec<usize> = aux.iter().map(|&(a, nb)| add(a, nb)).collect();
    SidePlan { cols, key_ci, qual_ci, pred_ci, out_ci, extra_ci, aux_ci }
}

/// Per-part entry-fp planes, aligned with `SidePlan::cols` (None = the
/// column decodes full, never as codes).
type FpPlanes = Vec<Option<Arc<Vec<Vec<u128>>>>>;

fn fp_planes(ctx: &SqeCtx, plan: &SidePlan) -> FpPlanes {
    plan.cols
        .iter()
        .map(|c| {
            if c.text && !c.needs_bytes && c.nf && !ctx.bank.parts.is_empty() {
                let pf = dict_faces(ctx, c.attno);
                Some(build_fps_cached(ctx, &pf, c.attno))
            } else {
                None
            }
        })
        .collect()
}

enum Lane<'a> {
    Words(&'a [u64]),
    Codes(&'a [u32]),
}

struct SideDecode {
    scr: Vec<Scratch>,
    cc: Vec<CurCache>,
    codes: Vec<Vec<u32>>,
}

impl SideDecode {
    /// Depot-riding constructor (the scratch-init discipline): decode
    /// arenas come reset from the calling worker's depot; cursors are
    /// built per engagement — NEVER parked (a cursor keyed only by part
    /// index can resurface another bank's or column's stream).
    fn fetch(plan: &SidePlan) -> SideDecode {
        SideDecode {
            scr: plan.cols.iter().map(|_| crate::scan::scratch_fetch()).collect(),
            cc: plan.cols.iter().map(|c| CurCache::new(c.attno)).collect(),
            codes: plan.cols.iter().map(|_| Vec::new()).collect(),
        }
    }

    /// Park the decode arenas back on this worker's depot (run_finish
    /// runs ON the worker thread); cursors and code buffers drop here.
    fn park(self) {
        for s in self.scr {
            crate::scan::scratch_park(s);
        }
    }

    /// Decode every planned column for the granule. Text columns with an
    /// fp plane and a dict in this part decode as codes (fp at entry
    /// grain); everything else decodes full. The returned slices live in
    /// this state's scratch until the next granule (raw-parts law).
    fn granule<'a>(
        &mut self,
        bank: &Bank,
        plan: &SidePlan,
        fps: &FpPlanes,
        pi: usize,
        g: u32,
        rows: usize,
    ) -> (Vec<Lane<'a>>, Vec<GranValid>) {
        let mut lanes: Vec<Lane<'a>> = Vec::with_capacity(plan.cols.len());
        let mut gvs: Vec<GranValid> = Vec::with_capacity(plan.cols.len());
        for (ci, c) in plan.cols.iter().enumerate() {
            let cur = self.cc[ci].get(bank, pi);
            let dictish = fps[ci].as_ref().map(|f| !f[pi].is_empty()).unwrap_or(false);
            if dictish {
                let buf = &mut self.codes[ci];
                if buf.len() < rows {
                    buf.resize(rows, 0);
                }
                cur.decode_codes(g, &mut buf[..rows]).expect("codes");
                gvs.push(GranValid::AllValid);
                lanes.push(Lane::Codes(unsafe {
                    std::slice::from_raw_parts(buf.as_ptr(), rows)
                }));
            } else {
                let gv = if c.nf {
                    GranValid::AllValid
                } else {
                    self.scr[ci].validity(cur, g, rows)
                };
                let d = self.scr[ci].decode_full(cur, g, rows);
                gvs.push(gv);
                lanes.push(Lane::Words(unsafe {
                    std::slice::from_raw_parts(d.as_ptr(), d.len())
                }));
            }
        }
        (lanes, gvs)
    }

}

/// The decoded word slice of a full-decode lane (code lanes never reach
/// the word/bytes routes).
#[inline(always)]
fn words_of<'a>(lane: &Lane<'a>) -> &'a [u64] {
    match lane {
        Lane::Words(d) => *d,
        Lane::Codes(_) => unreachable!("word lane decoded as codes"),
    }
}

/// Term-major side-predicate pass (R2): seeds `sel` with every granule
/// row, then filters by each conjunct in order — the CmpOp match runs
/// ONCE per (granule, term) and drives a monomorphic compaction loop
/// (`PredTerm::filter_sel`). Conjunct order and 3VL are bit-identical to
/// the per-row short-circuit walk this replaces.
fn pred_select(
    pred: &Option<PredSpec>,
    plan: &SidePlan,
    dec: &SideDecode,
    lanes: &[Lane<'_>],
    gvs: &[GranValid],
    rows: usize,
    sel: &mut Vec<u16>,
) {
    sel.clear();
    sel.extend((0..rows).map(|r| r as u16));
    let Some(p) = pred else { return };
    for (ti, t) in p.terms.iter().enumerate() {
        let ci = plan.pred_ci[ti];
        let face = plan.cols[ci].face;
        let d = words_of(&lanes[ci]);
        if plan.cols[ci].nf || gvs[ci].all_valid() {
            t.filter_sel(sel, |_| true, |r| face.word_key(d[r]));
        } else {
            let scr = &dec.scr[ci];
            t.filter_sel(sel, |r| scr.row_valid(r), |r| face.word_key(d[r]));
        }
    }
    // [colcmp] column-vs-column residues: both lanes decoded, 3VL.
    let nt = p.terms.len();
    for (ti, t) in p.col_terms.iter().enumerate() {
        let (cia, cib) = (plan.pred_ci[nt + 2 * ti], plan.pred_ci[nt + 2 * ti + 1]);
        let (fa, fb) = (plan.cols[cia].face, plan.cols[cib].face);
        let (da, db) = (words_of(&lanes[cia]), words_of(&lanes[cib]));
        let a_nf = plan.cols[cia].nf || gvs[cia].all_valid();
        let b_nf = plan.cols[cib].nf || gvs[cib].all_valid();
        let (sa, sb) = (&dec.scr[cia], &dec.scr[cib]);
        sel.retain(|&r| {
            let r = r as usize;
            let a_ok = a_nf || sa.row_valid(r);
            let b_ok = b_nf || sb.row_valid(r);
            t.eval_v(fa.word_key(da[r]), a_ok, fb.word_key(db[r]), b_ok)
        });
    }
    // Varlena conjuncts: payload-byte eval, 3VL (NULL never passes).
    let base = nt + 2 * p.col_terms.len();
    for (ti, t) in p.var_terms.iter().enumerate() {
        let ci = plan.pred_ci[base + ti];
        let d = words_of(&lanes[ci]);
        let av = plan.cols[ci].nf || gvs[ci].all_valid();
        let scr = &dec.scr[ci];
        sel.retain(|&r| {
            let r = r as usize;
            (av || scr.row_valid(r)) && t.eval(unsafe { varlena_payload(d[r]) })
        });
    }
}

/// [crossdim-or] One term test at scan grain over the decoded word `w`
/// of its column (3VL: a NULL row is never TRUE).
fn case_row(t: &CaseTest, face: Face, w: u64, ok: bool) -> bool {
    match t {
        CaseTest::Word(pt) | CaseTest::Packed(pt, _) => pt.eval_v(face.word_key(w), ok),
        CaseTest::Bytes(vt) => ok && vt.eval(unsafe { varlena_payload(w) }),
        CaseTest::InWords(ws) => ok && ws.binary_search(&face.word_key(w)).is_ok(),
        CaseTest::And(ts) => ts.iter().all(|t| case_row(t, face, w, ok)),
    }
}

/// [crossdim-or] Does the test read payload bytes (byte-decode lane)?
fn test_needs_bytes(t: &CaseTest) -> bool {
    match t {
        CaseTest::Word(_) | CaseTest::Packed(..) | CaseTest::InWords(_) => false,
        CaseTest::Bytes(_) => true,
        CaseTest::And(ts) => ts.iter().any(test_needs_bytes),
    }
}

/// [crossdim-or] Clear each term's arm bit over `sel` rows where the
/// term is not TRUE; `masks` is sel-aligned, seeded with the full mask.
/// `aux_base` = the terms' offset into `plan.aux_ci`.
#[allow(clippy::too_many_arguments)]
fn or_mask_pass(
    terms: &[&OrTerm],
    aux_base: usize,
    plan: &SidePlan,
    dec: &SideDecode,
    lanes: &[Lane<'_>],
    gvs: &[GranValid],
    sel: &[u16],
    masks: &mut [u64],
) {
    for (ti, t) in terms.iter().enumerate() {
        let ci = plan.aux_ci[aux_base + ti];
        let face = plan.cols[ci].face;
        let d = words_of(&lanes[ci]);
        let av = plan.cols[ci].nf || gvs[ci].all_valid();
        let scr = &dec.scr[ci];
        let bit = 1u64 << t.arm;
        for (i, &r) in sel.iter().enumerate() {
            if (masks[i] & bit) == 0 {
                continue;
            }
            let r = r as usize;
            let ok = av || scr.row_valid(r);
            if !case_row(&t.test, face, d[r], ok) {
                masks[i] &= !bit;
            }
        }
    }
}

/// [semianti-flt] Per-side resolved filter stage: aux positions of the
/// host key/qual lanes plus each qual's stage payload lane.
struct HostFilter {
    fi: usize,
    key_aux: Vec<usize>,
    qual_aux: Vec<usize>,
    qual_pay: Vec<usize>,
    /// [mapstage] the Num fold's host-operand aux position.
    num_aux: Option<usize>,
}

/// [semianti-flt] Retain host rows by the stages' (NOT) EXISTS law:
/// key-equal stage entry with every qual TRUE = a match; a NULL host
/// key or qual operand never matches; `anti` inverts the verdict.
fn filter_hosts(
    filters: &[FilterStage],
    built: &[DimBuilt],
    hf: &[HostFilter],
    plan: &SidePlan,
    dec: &SideDecode,
    lanes: &[Lane<'_>],
    gvs: &[GranValid],
    sel: &mut Vec<u16>,
) {
    for h in hf {
        let f = &filters[h.fi];
        let ft = &built[h.fi];
        let nk = ft.nk;
        let mut kw = [0i64; MAX_KEY_LANES];
        sel.retain(|&r16| {
            let r = r16 as usize;
            let mut nullk = false;
            for (li, &pos) in h.key_aux.iter().enumerate() {
                let ci = plan.aux_ci[pos];
                let ok = plan.cols[ci].nf || gvs[ci].all_valid() || dec.scr[ci].row_valid(r);
                if !ok {
                    nullk = true;
                    break;
                }
                kw[li] = plan.cols[ci].face.word_key(words_of(&lanes[ci])[r]);
            }
            let mut hit = false;
            match &f.fold {
                None => {
                    if !nullk {
                        let mut e = ft.first_match(&kw[..nk]);
                        while e != NO_ENTRY {
                            let pass = f.quals.iter().enumerate().all(|(qi, q)| {
                                let ci = plan.aux_ci[h.qual_aux[qi]];
                                let ok = plan.cols[ci].nf
                                    || gvs[ci].all_valid()
                                    || dec.scr[ci].row_valid(r);
                                let hv = plan.cols[ci].face.word_key(words_of(&lanes[ci])[r]);
                                let bi = e as usize * ft.npw + h.qual_pay[qi];
                                JoinQual { probe_col: 0, build_col: 0, op: q.op }
                                    .eval_v(hv, ok, ft.pw[bi], ft.pv[bi])
                            });
                            if pass {
                                hit = true;
                                break;
                            }
                            e = ft.next_match(&kw[..nk], e);
                        }
                    }
                }
                // [mapstage] the grouped stage links one representative
                // per key: read its cell, or the empty-group answer for
                // an absent key (a NULL host key IS an empty group).
                Some(StageFold::Word(bf)) => {
                    let e = if nullk { NO_ENTRY } else { ft.first_match(&kw[..nk]) };
                    let cell: Option<(i64, bool)> = if e != NO_ENTRY {
                        let bi = e as usize * ft.npw + h.qual_pay[0];
                        Some((ft.pw[bi], ft.pv[bi]))
                    } else {
                        bf.missing.map(|m| (m, true))
                    };
                    if let Some((bv, bok)) = cell {
                        hit = f.quals.iter().enumerate().all(|(qi, q)| {
                            let ci = plan.aux_ci[h.qual_aux[qi]];
                            let ok = plan.cols[ci].nf
                                || gvs[ci].all_valid()
                                || dec.scr[ci].row_valid(r);
                            let hv = plan.cols[ci].face.word_key(words_of(&lanes[ci])[r]);
                            JoinQual { probe_col: 0, build_col: 0, op: q.op }
                                .eval_v(hv, ok, bv, bok)
                        });
                    }
                }
                Some(StageFold::Num(nf)) => {
                    let e = if nullk { NO_ENTRY } else { ft.first_match(&kw[..nk]) };
                    if e != NO_ENTRY {
                        let ei = e as usize;
                        if ft.fc[ei] > 0 {
                            let ci = plan.aux_ci[h.num_aux.expect("planned with the fold")];
                            let ok = plan.cols[ci].nf
                                || gvs[ci].all_valid()
                                || dec.scr[ci].row_valid(r);
                            if ok {
                                let hv = plan.cols[ci].face.word_key(words_of(&lanes[ci])[r]);
                                let cs = if matches!(nf.op, NumCellOp::Avg) { ft.fr[ei] } else { nf.scale };
                                hit = crate::joins::numcell::num_cell_pass(
                                    nf.qual.op,
                                    hv,
                                    nf.qual.probe_scale,
                                    nf.qual.k_m,
                                    nf.qual.k_scale,
                                    ft.fs[ei],
                                    cs,
                                );
                            }
                        }
                    }
                }
            }
            hit != f.anti
        });
    }
}

// ---------------------------------------------------------------------------
// [sqe-mech3] dimension build stages (3-way composition)
// ---------------------------------------------------------------------------

/// One built dimension stage: flat entries (key words + payload lanes)
/// under an elected access arm — witnessed-dense single-key domains take
/// the DIRECT-ARRAY head table (the floor's bitset/direct-array idea:
/// index = key - lo, no hashing), everything else the owned open-address
/// map. Duplicate keys chain (`next`); the probe walks all matches (the
/// general hash-join multiplicity law).
struct DimBuilt {
    nk: usize,
    npw: usize,
    npt: usize,
    /// nk key words per entry (Map compare; Dense chains are same-key).
    kw: Vec<i64>,
    pw: Vec<i64>,
    pv: Vec<bool>,
    pt: Vec<(u32, u32)>,
    arena: Vec<u8>,
    next: Vec<u32>,
    /// [crossdim-or] per-entry arm mask (empty = no terms at this site).
    omask: Vec<u64>,
    /// [mapstage] numeric collapse planes (empty unless a Num fold).
    fs: Vec<i128>,
    fc: Vec<u32>,
    fr: Vec<i32>,
    arm: DimArm,
}

enum DimArm {
    /// Single word key, witnessed dense-and-bounded domain: heads
    /// indexed `key - lo` (out-of-domain probes miss by construction).
    Dense { lo: i64, heads: Vec<u32> },
    Map { heads: Vec<u32>, mask: usize },
}

impl DimBuilt {
    #[inline(always)]
    fn skip_nonmatch(&self, kw: &[i64], mut e: u32) -> u32 {
        while e != NO_ENTRY {
            let b = e as usize * self.nk;
            if &self.kw[b..b + self.nk] == kw {
                break;
            }
            e = self.next[e as usize];
        }
        e
    }

    #[inline(always)]
    fn first_match(&self, kw: &[i64]) -> u32 {
        match &self.arm {
            DimArm::Dense { lo, heads } => {
                let idx = kw[0].wrapping_sub(*lo) as u64;
                if idx < heads.len() as u64 { heads[idx as usize] } else { NO_ENTRY }
            }
            DimArm::Map { heads, mask } => {
                if heads.is_empty() {
                    return NO_ENTRY;
                }
                let e = heads[(key_hash(kw, &[]) as usize) & mask];
                self.skip_nonmatch(kw, e)
            }
        }
    }

    #[inline(always)]
    fn next_match(&self, kw: &[i64], e: u32) -> u32 {
        let n = self.next[e as usize];
        match &self.arm {
            DimArm::Dense { .. } => n,
            DimArm::Map { .. } => self.skip_nonmatch(kw, n),
        }
    }

    fn est_bytes(&self) -> usize {
        self.kw.len() * 8
            + self.pw.len() * 9
            + self.pt.len() * 8
            + self.arena.len()
            + self.omask.len() * 8
            + self.fs.len() * 16
            + self.fc.len() * 4
            + self.fr.len() * 4
            + self.next.len() * 4
            + match &self.arm {
                DimArm::Dense { heads, .. } | DimArm::Map { heads, .. } => heads.len() * 4,
            }
    }
}

/// Build one dimension stage: pool-parallel granule scan (word pred +
/// text byte-eq conjuncts + NULL-key drop), per-unit staging merged in
/// unit order (deterministic across pool widths), then the arm election.
fn build_dim(
    dctx: &SqeCtx,
    stage: &DimStage,
    pay_w: &[u32],
    pay_t: &[u32],
    or_terms: &[&OrTerm],
    or_full: u64,
    fold: Option<FoldLane>,
) -> DimBuilt {
    let bank = dctx.bank;
    let key_cols: Vec<u32> = stage.keys.iter().map(|k| k.dim_col).collect();
    let te_cols: Vec<u32> = stage.text_eqs.iter().map(|t| t.col).collect();
    let outs: Vec<u32> = pay_w.iter().chain(pay_t.iter()).copied().collect();
    let aux: Vec<(u32, bool)> =
        or_terms.iter().map(|t| (t.col, test_needs_bytes(&t.test))).collect();
    let plan = side_plan(bank, &key_cols, &te_cols, &stage.pred, &outs, &[], &aux);
    let nk = key_cols.len();
    let (npw, npt) = (pay_w.len(), pay_t.len());
    let fps: FpPlanes = plan.cols.iter().map(|_| None).collect();
    let units = if bank.parts.is_empty() {
        Arc::new(Vec::new())
    } else {
        dctx.faces.walk(bank, key_cols[0])
    };
    struct DU {
        kw: Vec<i64>,
        pw: Vec<i64>,
        pv: Vec<bool>,
        pt: Vec<(u32, u32)>,
        arena: Vec<u8>,
        om: Vec<u64>,
    }
    struct DState {
        dec: SideDecode,
        sel: Vec<u16>,
        out: Vec<(usize, DU)>,
    }
    let planr = &plan;
    let fpsr = &fps;
    let per_worker = dctx.pool.run_finish(
        units.len(),
        |_| DState { dec: SideDecode::fetch(planr), sel: Vec::new(), out: Vec::new() },
        |s: &mut DState, ui| {
            let (pi, g, rows, _) = units[ui];
            let rows = rows as usize;
            let DState { dec, sel, out } = s;
            let (lanes, gvs) = dec.granule(bank, planr, fpsr, pi, g, rows);
            pred_select(&stage.pred, planr, dec, &lanes, &gvs, rows, sel);
            // Text byte-equality conjuncts (3VL: NULL never passes).
            for (ti, t) in stage.text_eqs.iter().enumerate() {
                let ci = planr.qual_ci[ti];
                let d = words_of(&lanes[ci]);
                let av = planr.cols[ci].nf || gvs[ci].all_valid();
                let scr = &dec.scr[ci];
                sel.retain(|&r| {
                    let r = r as usize;
                    (av || scr.row_valid(r))
                        && unsafe { varlena_payload(d[r]) } == &t.bytes[..]
                });
            }
            // NULL dim keys never match: drop them at build.
            for &ci in planr.key_ci.iter() {
                if planr.cols[ci].nf || gvs[ci].all_valid() {
                    continue;
                }
                let scr = &dec.scr[ci];
                sel.retain(|&r| scr.row_valid(r as usize));
            }
            let mut u = DU {
                kw: Vec::with_capacity(sel.len() * nk),
                pw: Vec::with_capacity(sel.len() * npw),
                pv: Vec::with_capacity(sel.len() * npw),
                pt: Vec::with_capacity(sel.len() * npt),
                arena: Vec::new(),
                om: Vec::new(),
            };
            if or_full != 0 {
                u.om.resize(sel.len(), or_full);
                or_mask_pass(or_terms, 0, planr, dec, &lanes, &gvs, sel, &mut u.om);
            }
            for &r16 in sel.iter() {
                let r = r16 as usize;
                for &ci in planr.key_ci.iter() {
                    let d = words_of(&lanes[ci]);
                    u.kw.push(planr.cols[ci].face.word_key(d[r]));
                }
                for (pj, _) in pay_w.iter().enumerate() {
                    let ci = planr.out_ci[pj];
                    let d = words_of(&lanes[ci]);
                    let ok = planr.cols[ci].nf
                        || gvs[ci].all_valid()
                        || dec.scr[ci].row_valid(r);
                    u.pv.push(ok);
                    u.pw.push(if ok { planr.cols[ci].face.word_key(d[r]) } else { 0 });
                }
                for (tj, _) in pay_t.iter().enumerate() {
                    let ci = planr.out_ci[npw + tj];
                    let d = words_of(&lanes[ci]);
                    let ok = planr.cols[ci].nf
                        || gvs[ci].all_valid()
                        || dec.scr[ci].row_valid(r);
                    if ok {
                        let bytes = unsafe { varlena_payload(d[r]) };
                        let off = u.arena.len() as u32;
                        u.arena.extend_from_slice(bytes);
                        u.pt.push((off, bytes.len() as u32));
                    } else {
                        u.pt.push(T_NULL);
                    }
                }
            }
            out.push((ui, u));
        },
        // Worker-side finish: arenas park on THIS worker's depot; only
        // the result-bearing units cross back.
        |s: DState| {
            s.dec.park();
            s.out
        },
    );
    // Assemble in unit order (pool-width independent).
    let mut per_unit: Vec<(usize, DU)> = per_worker.into_iter().flatten().collect();
    per_unit.sort_by_key(|(ui, _)| *ui);
    let mut d = DimBuilt {
        nk,
        npw,
        npt,
        kw: Vec::new(),
        pw: Vec::new(),
        pv: Vec::new(),
        pt: Vec::new(),
        arena: Vec::new(),
        next: Vec::new(),
        omask: Vec::new(),
        fs: Vec::new(),
        fc: Vec::new(),
        fr: Vec::new(),
        arm: DimArm::Map { heads: Vec::new(), mask: 0 },
    };
    for (_, u) in per_unit {
        let base = d.arena.len() as u32;
        d.kw.extend_from_slice(&u.kw);
        d.pw.extend_from_slice(&u.pw);
        d.pv.extend_from_slice(&u.pv);
        d.pt
            .extend(u.pt.iter().map(|&(o, l)| if (o, l) == T_NULL { T_NULL } else { (o + base, l) }));
        d.arena.extend_from_slice(&u.arena);
        d.omask.extend_from_slice(&u.om);
    }
    let n = d.kw.len() / nk.max(1);
    assert!(n < NO_ENTRY as usize, "dim stage exceeds u32 entries");
    d.next = vec![NO_ENTRY; n];
    // Arm election: witnessed dense-and-bounded single-key domain takes
    // the direct-array heads (the floor's bitset/direct-array law); the
    // owned open-address map is the always-sound fallback.
    let dense = (nk == 1 && dctx.faces.cfg.direct_array)
        .then(|| crate::planner::direct_array_domain(bank, dctx.faces, key_cols[0], 0))
        .flatten();
    if let Some(FoldLane::Num { op, .. }) = fold {
        d.fs = vec![0; n];
        d.fc = vec![0; n];
        if matches!(op, NumCellOp::Avg) {
            d.fr = vec![0; n];
        }
    }
    // [mapstage] grouped collapse at link time: a key-equal entry
    // already linked is the group's representative — this entry's cell
    // folds into it and the entry stays unlinked (the grouped-build law
    // at stage grain; Dense chains are same-key by construction).
    let mut folded: Vec<bool> = vec![false; if fold.is_some() { n } else { 0 }];
    match dense {
        Some((lo, dn)) => {
            let mut heads = vec![NO_ENTRY; dn];
            for e in 0..n {
                let idx = d.kw[e].wrapping_sub(lo) as usize;
                assert!(idx < dn, "dim key outside the witnessed domain");
                if stage_fold_entry(&mut d, fold, e, heads[idx]) {
                    folded[e] = true;
                    continue;
                }
                d.next[e] = heads[idx];
                heads[idx] = e as u32;
            }
            d.arm = DimArm::Dense { lo, heads };
        }
        None => {
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut heads = vec![NO_ENTRY; cap];
            for e in 0..n {
                let h = key_hash(&d.kw[e * nk..e * nk + nk], &[]);
                let slot = (h as usize) & mask;
                let g = if fold.is_some() {
                    let mut g = heads[slot];
                    while g != NO_ENTRY {
                        let b = g as usize * nk;
                        if d.kw[b..b + nk] == d.kw[e * nk..e * nk + nk] {
                            break;
                        }
                        g = d.next[g as usize];
                    }
                    g
                } else {
                    NO_ENTRY
                };
                if stage_fold_entry(&mut d, fold, e, g) {
                    folded[e] = true;
                    continue;
                }
                d.next[e] = heads[slot];
                heads[slot] = e as u32;
            }
            d.arm = DimArm::Map { heads, mask };
        }
    }
    // [mapstage] compact to the representatives: folded entries are
    // unreachable garbage, so the sealed stage (and the budget law that
    // reads it) sits at GROUP cardinality, not scan cardinality.
    if fold.is_some() && folded.iter().any(|&f| f) {
        debug_assert!(d.npt == 0 && d.omask.is_empty(), "fold stages are word-payload only");
        let has_num = !d.fs.is_empty();
        let has_r = !d.fr.is_empty();
        let mut m = 0usize;
        for e in 0..n {
            if folded[e] {
                continue;
            }
            if m != e {
                for li in 0..nk {
                    d.kw[m * nk + li] = d.kw[e * nk + li];
                }
                for pj in 0..npw {
                    d.pw[m * npw + pj] = d.pw[e * npw + pj];
                    d.pv[m * npw + pj] = d.pv[e * npw + pj];
                }
                if has_num {
                    d.fs[m] = d.fs[e];
                    d.fc[m] = d.fc[e];
                    if has_r {
                        d.fr[m] = d.fr[e];
                    }
                }
            }
            m += 1;
        }
        d.kw.truncate(m * nk);
        d.pw.truncate(m * npw);
        d.pv.truncate(m * npw);
        if has_num {
            d.fs.truncate(m);
            d.fc.truncate(m);
            if has_r {
                d.fr.truncate(m);
            }
        }
        d.next = vec![NO_ENTRY; m];
        let new_arm = match &d.arm {
            DimArm::Dense { lo, heads } => {
                let mut heads = vec![NO_ENTRY; heads.len()];
                for e in 0..m {
                    let idx = d.kw[e].wrapping_sub(*lo) as usize;
                    d.next[e] = heads[idx];
                    heads[idx] = e as u32;
                }
                DimArm::Dense { lo: *lo, heads }
            }
            DimArm::Map { .. } => {
                let cap = (m * 2).next_power_of_two().max(16);
                let mask = cap - 1;
                let mut heads = vec![NO_ENTRY; cap];
                for e in 0..m {
                    let h = key_hash(&d.kw[e * nk..e * nk + nk], &[]);
                    let slot = (h as usize) & mask;
                    d.next[e] = heads[slot];
                    heads[slot] = e as u32;
                }
                DimArm::Map { heads, mask }
            }
        };
        d.arm = new_arm;
    }
    if let Some(FoldLane::Num { op: NumCellOp::Avg, scale, .. }) = fold {
        for e in 0..d.fc.len() {
            if d.fc[e] > 0 {
                let (a, r) = crate::joins::numcell::pg_avg_cell(d.fs[e], d.fc[e], scale);
                d.fs[e] = a;
                d.fr[e] = r;
            }
        }
    }
    d
}

/// [mapstage] Fold entry `e`'s cell: `g` = the linked key-equal
/// representative (NO_ENTRY = none — `e` starts its group and links).
/// Returns true when `e` folded into `g` and must stay unlinked.
fn stage_fold_entry(d: &mut DimBuilt, fold: Option<FoldLane>, e: usize, g: u32) -> bool {
    let Some(fl) = fold else { return false };
    match fl {
        FoldLane::Word(pj, op) => {
            let ce = e * d.npw + pj;
            if g == NO_ENTRY {
                match op {
                    JoinAggOp::CountStar => {
                        d.pw[ce] = 1;
                        d.pv[ce] = true;
                    }
                    JoinAggOp::CountCol => {
                        d.pw[ce] = d.pv[ce] as i64;
                        d.pv[ce] = true;
                    }
                    JoinAggOp::Sum | JoinAggOp::Min | JoinAggOp::Max => {}
                    JoinAggOp::CountDistinct => {
                        unreachable!("stage folds admit count/sum/min/max only")
                    }
                }
                false
            } else {
                let cg = g as usize * d.npw + pj;
                let (v, ok) = (d.pw[ce], d.pv[ce]);
                match op {
                    JoinAggOp::CountStar => d.pw[cg] += 1,
                    JoinAggOp::CountCol => d.pw[cg] += ok as i64,
                    JoinAggOp::CountDistinct => {
                        unreachable!("stage folds admit count/sum/min/max only")
                    }
                    JoinAggOp::Sum | JoinAggOp::Min | JoinAggOp::Max => {
                        if ok {
                            if !d.pv[cg] {
                                d.pw[cg] = v;
                                d.pv[cg] = true;
                            } else {
                                d.pw[cg] = match op {
                                    JoinAggOp::Sum => d.pw[cg] + v,
                                    JoinAggOp::Min => d.pw[cg].min(v),
                                    _ => d.pw[cg].max(v),
                                };
                            }
                        }
                    }
                }
                true
            }
        }
        FoldLane::Num { pj, op, .. } => {
            let ce = e * d.npw + pj;
            if g == NO_ENTRY {
                if d.pv[ce] {
                    d.fs[e] = d.pw[ce] as i128;
                    d.fc[e] = 1;
                }
                false
            } else {
                if d.pv[ce] {
                    let gi = g as usize;
                    let v = d.pw[ce] as i128;
                    // Sum/Avg accumulate; Min/Max are the order-fold at
                    // one fixed lane scale (mantissa order IS value
                    // order). A representative whose own cell was NULL
                    // starts the group at this entry's value.
                    d.fs[gi] = if d.fc[gi] == 0 {
                        v
                    } else {
                        match op {
                            NumCellOp::Sum | NumCellOp::Avg => d.fs[gi] + v,
                            NumCellOp::Min => d.fs[gi].min(v),
                            NumCellOp::Max => d.fs[gi].max(v),
                        }
                    };
                    d.fc[gi] += 1;
                }
                true
            }
        }
    }
}

/// [mapjoingoal] Seal pre-collapsed rows as a stage table: one entry
/// per group, open-address heads (key equality dedup already happened
/// at the goal's grouped fold; duplicate keys would chain harmlessly —
/// the probe reads the first match).
fn prebuilt_stage(rows: &StageRows) -> DimBuilt {
    let nk = rows.nk.max(1);
    let n = rows.kw.len() / nk;
    debug_assert!(rows.fs.len() == n && rows.fc.len() == n, "map rows planes align");
    let mut d = DimBuilt {
        nk,
        npw: 0,
        npt: 0,
        kw: rows.kw.clone(),
        pw: Vec::new(),
        pv: Vec::new(),
        pt: Vec::new(),
        arena: Vec::new(),
        next: vec![NO_ENTRY; n],
        omask: Vec::new(),
        fs: rows.fs.clone(),
        fc: rows.fc.clone(),
        fr: Vec::new(),
        arm: DimArm::Map { heads: Vec::new(), mask: 0 },
    };
    let cap = (n * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut heads = vec![NO_ENTRY; cap];
    for e in 0..n {
        let h = key_hash(&d.kw[e * nk..e * nk + nk], &[]);
        let slot = (h as usize) & mask;
        d.next[e] = heads[slot];
        heads[slot] = e as u32;
    }
    d.arm = DimArm::Map { heads, mask };
    d
}

/// Which normalized survivor planes each probe column needs, resolved
/// once per run.
struct NormPlan {
    word: Vec<bool>,
    fp: Vec<bool>,
    valid: Vec<bool>,
}

fn norm_plan(plan: &SidePlan, key_text: &[bool]) -> NormPlan {
    let n = plan.cols.len();
    let mut np =
        NormPlan { word: vec![false; n], fp: vec![false; n], valid: vec![false; n] };
    for (ki, &ci) in plan.key_ci.iter().enumerate() {
        if key_text[ki] {
            np.fp[ci] = true;
        } else {
            np.word[ci] = true;
        }
    }
    for &ci in &plan.qual_ci {
        np.word[ci] = true;
        np.valid[ci] = true;
    }
    for &ci in &plan.out_ci {
        np.valid[ci] = true;
        if !plan.cols[ci].text {
            np.word[ci] = true;
        }
    }
    np
}

/// Column-major probe normalization (R2): per (granule, column), resolve
/// the Lane/Face/validity dispatch ONCE and write survivor-indexed flat
/// planes; the probe sweeps and the sink then read plain slices with no
/// per-row enum work. NULL slots write (0, false) — the exact (word, ok)
/// pairs the row-major accessors produced — and fp/bytes reads stay
/// validity-guarded (a NULL row's slot is never dereferenced). `snullk`
/// marks survivors with any NULL key lane (a NULL probe key never
/// matches).
#[allow(clippy::too_many_arguments)]
fn normalize_probe(
    plan: &SidePlan,
    np: &NormPlan,
    dec: &SideDecode,
    lanes: &[Lane<'_>],
    gvs: &[GranValid],
    fps: &FpPlanes,
    pi: usize,
    surv: &[u16],
    nw: &mut [Vec<i64>],
    nv: &mut [Vec<bool>],
    nfp: &mut [Vec<u128>],
    snullk: &mut Vec<bool>,
) {
    for (ci, c) in plan.cols.iter().enumerate() {
        let av = c.nf || gvs[ci].all_valid();
        if np.valid[ci] {
            let v = &mut nv[ci];
            v.clear();
            if av {
                v.resize(surv.len(), true);
            } else {
                let scr = &dec.scr[ci];
                v.extend(surv.iter().map(|&r| scr.row_valid(r as usize)));
            }
        }
        if np.word[ci] {
            let d = words_of(&lanes[ci]);
            let face = c.face;
            let w = &mut nw[ci];
            w.clear();
            if av {
                w.extend(surv.iter().map(|&r| face.word_key(d[r as usize])));
            } else {
                let scr = &dec.scr[ci];
                w.extend(surv.iter().map(|&r| {
                    if scr.row_valid(r as usize) { face.word_key(d[r as usize]) } else { 0 }
                }));
            }
        }
        if np.fp[ci] {
            let f = &mut nfp[ci];
            f.clear();
            match &lanes[ci] {
                Lane::Codes(cz) => {
                    // fp planes exist only for null-free dict lanes.
                    let tbl = &fps[ci].as_ref().expect("codes lane has fp plane")[pi];
                    f.extend(surv.iter().map(|&r| tbl[cz[r as usize] as usize]));
                }
                Lane::Words(d) => {
                    if av {
                        f.extend(surv.iter().map(|&r| {
                            crate::fp::entry_fp128(unsafe { varlena_payload(d[r as usize]) })
                        }));
                    } else {
                        let scr = &dec.scr[ci];
                        f.extend(surv.iter().map(|&r| {
                            if scr.row_valid(r as usize) {
                                crate::fp::entry_fp128(unsafe {
                                    varlena_payload(d[r as usize])
                                })
                            } else {
                                0
                            }
                        }));
                    }
                }
            }
        }
    }
    snullk.clear();
    snullk.resize(surv.len(), false);
    for &ci in &plan.key_ci {
        if plan.cols[ci].nf || gvs[ci].all_valid() {
            continue;
        }
        let scr = &dec.scr[ci];
        for (si, &r) in surv.iter().enumerate() {
            snullk[si] |= !scr.row_valid(r as usize);
        }
    }
}

/// [corrsubq]/[corrnumcell] the collapse lane: payload index + fold law.
#[derive(Clone, Copy)]
enum FoldLane {
    Word(usize, JoinAggOp),
    Num { pj: usize, op: NumCellOp, scale: i32 },
}

#[derive(Default)]
struct Bucket {
    hash: Vec<u64>,
    kw: Vec<i64>,
    kf: Vec<u128>,
    pw: Vec<i64>,
    pv: Vec<bool>,
    pt: Vec<(u32, u32)>,
    arena: Vec<u8>,
    /// [sqe-semi-anti] per-entry NULL-join-key flag: true only for the
    /// RightAnti-retained null-keyed build rows (never chain-linked).
    nullk: Vec<bool>,
}

#[derive(Default)]
struct PartTable {
    hash: Vec<u64>,
    kw: Vec<i64>,
    kf: Vec<u128>,
    pw: Vec<i64>,
    pv: Vec<bool>,
    pt: Vec<(u32, u32)>,
    arena: Vec<u8>,
    /// [corrnumcell] per-entry cells: i128 mantissa, valid count, avg rscale.
    fs: Vec<i128>,
    fc: Vec<u32>,
    fr: Vec<i32>,
    /// [sqe-semi-anti] see `Bucket::nullk`.
    nullk: Vec<bool>,
    heads: Vec<u32>,
    next: Vec<u32>,
    mask: usize,
}

enum OutB {
    W(Vec<i64>, Vec<bool>),
    T(BytesBuild, Vec<bool>),
}

fn new_outs(node: &JoinNode) -> Vec<OutB> {
    node.out_bytes
        .iter()
        .map(|&bytes| {
            if bytes {
                OutB::T(BytesBuild::new(), Vec::new())
            } else {
                OutB::W(Vec::new(), Vec::new())
            }
        })
        .collect()
}

/// `(is_text, payload_index)` per build-side out column, in `out_ci`
/// order.
fn bout_pay_map(bplan: &SidePlan, pay_word: &[usize], pay_text: &[usize]) -> Vec<(bool, usize)> {
    bplan
        .out_ci
        .iter()
        .map(|ci| {
            if bplan.cols[*ci].text {
                (true, pay_text.iter().position(|c| c == ci).unwrap())
            } else {
                (false, pay_word.iter().position(|c| c == ci).unwrap())
            }
        })
        .collect()
}

/// Resolved source of one out column: probe decode lane or build payload.
#[derive(Clone, Copy)]
enum OutRef {
    Probe { ci: usize },
    BuildW { pj: usize },
    BuildT { pj: usize },
}

/// Per-granule row context handed to the probe consumer; `entry = None`
/// is the null-extended build side (LEFT) or a build-free emit
/// (SEMI/ANTI). Probe-side reads are SURVIVOR-indexed flat slices: the
/// column-major normalization pass (`normalize_probe`) already resolved
/// every Lane/Face/validity per (granule, column), so no accessor here
/// matches an enum per row on the probe arm.
struct RowCx<'a, 'l> {
    /// The granule's survivor rowlist (`si` -> row).
    surv: &'a [u16],
    /// Per ci: face-normalized words over survivors (0 when NULL).
    nw: &'a [Vec<i64>],
    /// Per ci: per-survivor validity.
    nv: &'a [Vec<bool>],
    /// Per ci: the raw decoded word slice (empty for code lanes) — text
    /// bytes read through `varlena_payload` with the Lane match hoisted.
    raw: &'a [&'l [u64]],
    tables: &'a [PartTable],
    out_map: &'a [OutRef],
    npw: usize,
    npt: usize,
}

impl RowCx<'_, '_> {
    #[inline(always)]
    fn out_valid(&self, oi: usize, si: usize, entry: Option<(usize, usize)>) -> bool {
        match self.out_map[oi] {
            OutRef::Probe { ci } => self.nv[ci][si],
            OutRef::BuildW { pj } => match entry {
                Some((part, e)) => self.tables[part].pv[e * self.npw + pj],
                None => false,
            },
            OutRef::BuildT { pj } => match entry {
                Some((part, e)) => self.tables[part].pt[e * self.npt + pj] != T_NULL,
                None => false,
            },
        }
    }

    /// Payload bytes of a varlena out column (None when NULL).
    #[inline(always)]
    fn out_bytes(&self, oi: usize, si: usize, entry: Option<(usize, usize)>) -> Option<&[u8]> {
        match self.out_map[oi] {
            OutRef::Probe { ci } => self.nv[ci][si].then(|| {
                let r = self.surv[si] as usize;
                unsafe { varlena_payload(self.raw[ci][r]) }
            }),
            OutRef::BuildT { pj } => {
                let (part, e) = entry?;
                let t = &self.tables[part];
                let (o, l) = t.pt[e * self.npt + pj];
                if (o, l) == T_NULL {
                    None
                } else {
                    Some(&t.arena[o as usize..(o + l) as usize])
                }
            }
            OutRef::BuildW { .. } => unreachable!("bytes out on a word column"),
        }
    }

    /// Word + validity of a non-varlena out column (0 when NULL).
    #[inline(always)]
    fn out_word(&self, oi: usize, si: usize, entry: Option<(usize, usize)>) -> (i64, bool) {
        match self.out_map[oi] {
            OutRef::Probe { ci } => (self.nw[ci][si], self.nv[ci][si]),
            OutRef::BuildW { pj } => match entry {
                Some((part, e)) => {
                    let t = &self.tables[part];
                    (t.pw[e * self.npw + pj], t.pv[e * self.npw + pj])
                }
                None => (0, false),
            },
            OutRef::BuildT { .. } => unreachable!("word out on a text column"),
        }
    }
}

/// Probe-loop consumer (statically dispatched — one instantiation per
/// goal): `row` fires once per joined output row (`si` = the survivor
/// index into `cx.surv`), inside the loop; `unit_end` fires after each
/// granule's probe while the granule's normalized buffers are live.
trait Sink: Send {
    fn unit_start(&mut self, ui: usize);
    fn row(&mut self, cx: &RowCx<'_, '_>, si: usize, entry: Option<(usize, usize)>);
    fn unit_end(&mut self, _cx: &RowCx<'_, '_>) {}
}

/// Row goal: buffer joined rows per unit (concatenated in unit order).
/// `row` only stages (survivor, entry) triples; `unit_end` emits
/// COLUMN-MAJOR — one OutRef/OutB match per (granule, out column), then
/// a flat loop over the staged rows (per-column push order is the row
/// order, byte-identical to the row-major emit this replaces).
struct RowEmit<'a> {
    node: &'a JoinNode,
    units: Vec<(usize, Vec<OutB>)>,
    /// Staged (si, part, entry) — `part == NO_ENTRY` is the entry-less
    /// emit (LEFT null-extension, SEMI/ANTI).
    pend: Vec<(u32, u32, u32)>,
}

impl Sink for RowEmit<'_> {
    fn unit_start(&mut self, ui: usize) {
        self.units.push((ui, new_outs(self.node)));
    }

    fn row(&mut self, _cx: &RowCx<'_, '_>, si: usize, entry: Option<(usize, usize)>) {
        let (part, ei) = match entry {
            Some((part, e)) => (part as u32, e as u32),
            None => (NO_ENTRY, 0),
        };
        self.pend.push((si as u32, part, ei));
    }

    fn unit_end(&mut self, cx: &RowCx<'_, '_>) {
        let outs = &mut self.units.last_mut().expect("unit_start").1;
        for (oi, ob) in outs.iter_mut().enumerate() {
            match (ob, cx.out_map[oi]) {
                (OutB::W(v, m), OutRef::Probe { ci }) => {
                    let (nw, nv) = (&cx.nw[ci][..], &cx.nv[ci][..]);
                    for &(si, _, _) in &self.pend {
                        v.push(nw[si as usize]);
                        m.push(nv[si as usize]);
                    }
                }
                (OutB::W(v, m), OutRef::BuildW { pj }) => {
                    for &(_, part, ei) in &self.pend {
                        if part == NO_ENTRY {
                            v.push(0);
                            m.push(false);
                        } else {
                            let t = &cx.tables[part as usize];
                            let bi = ei as usize * cx.npw + pj;
                            v.push(t.pw[bi]);
                            m.push(t.pv[bi]);
                        }
                    }
                }
                (OutB::T(bb, m), OutRef::Probe { ci }) => {
                    let (nv, raw) = (&cx.nv[ci][..], cx.raw[ci]);
                    for &(si, _, _) in &self.pend {
                        if nv[si as usize] {
                            let r = cx.surv[si as usize] as usize;
                            bb.push(unsafe { varlena_payload(raw[r]) });
                            m.push(true);
                        } else {
                            bb.push(b"");
                            m.push(false);
                        }
                    }
                }
                (OutB::T(bb, m), OutRef::BuildT { pj }) => {
                    for &(_, part, ei) in &self.pend {
                        let bytes = if part == NO_ENTRY {
                            None
                        } else {
                            let t = &cx.tables[part as usize];
                            let (off, l) = t.pt[ei as usize * cx.npt + pj];
                            if (off, l) == T_NULL {
                                None
                            } else {
                                Some(&t.arena[off as usize..(off + l) as usize])
                            }
                        };
                        match bytes {
                            Some(b) => {
                                bb.push(b);
                                m.push(true);
                            }
                            None => {
                                bb.push(b"");
                                m.push(false);
                            }
                        }
                    }
                }
                _ => unreachable!("out column kind mismatches its OutRef"),
            }
        }
        self.pend.clear();
    }
}

/// Per-agg staging source, hoisted at sink construction (None for
/// CountStar — rides the row count). [sqe-mech3] `arith`/`oi2` carry the
/// fused-arithmetic fold shape; the staged (word, valid) pair is the
/// evaluated expression (NULL either operand folds nothing — PG's
/// strict-transition law over a NULL-propagating expression). Shared by
/// the ungrouped and grouped fold sinks.
#[derive(Clone, Copy)]
struct FoldSrc<'a> {
    op: AggFoldOp,
    oi: usize,
    oi2: usize,
    /// [scale-alg] subtracted-product lanes + alignment multipliers.
    oi3: usize,
    oi4: usize,
    align: (i64, i64),
    arith: Option<JoinArith>,
    is_count: bool,
    /// [caseleg] the leg's CASE test + its out lane.
    case: Option<&'a JoinCaseLeg>,
    oic: usize,
}

fn fold_srcs(node: &JoinAggNode) -> Vec<Option<FoldSrc<'_>>> {
    node.aggs
        .iter()
        .enumerate()
        .map(|(ai, a)| {
            a.op.fold_op().map(|op| {
                let (oi3, oi4, align) = match a.arith {
                    Some(JoinArith::PackedMulKSubCC { sa, sb, c, sc, d, sd, .. }) => {
                        let pos = |io: JoinOut| {
                            node.join
                                .out
                                .iter()
                                .position(|o| *o == io)
                                .expect("subtracted-product lanes ride join.out")
                        };
                        let s = (sa + sb).max(sc + sd);
                        (pos(c), pos(d), (pow10_i64(s - sa - sb), pow10_i64(s - sc - sd)))
                    }
                    _ => (usize::MAX, usize::MAX, (1i64, 1i64)),
                };
                FoldSrc {
                    op,
                    oi: node.agg_oi[ai],
                    oi2: node.agg_oi2[ai],
                    oi3,
                    oi4,
                    align,
                    arith: a.arith,
                    is_count: matches!(op, AggFoldOp::CountCol),
                    case: a.case.as_ref(),
                    oic: node.agg_oic[ai],
                }
            })
        })
        .collect()
}

/// Alignment gaps are admission-witnessed <= 18, so this cannot overflow.
#[inline(always)]
fn pow10_i64(d: i32) -> i64 {
    10i64.pow(d as u32)
}

/// [caseleg] Does the CASE test hold at this joined row (3VL: a NULL
/// test column is not TRUE)?
#[inline(always)]
fn case_pass(cx: &RowCx<'_, '_>, si: usize, entry: Option<(usize, usize)>, oic: usize, c: &JoinCaseLeg) -> bool {
    case_test(cx, si, entry, oic, &c.test)
}

fn case_test(cx: &RowCx<'_, '_>, si: usize, entry: Option<(usize, usize)>, oic: usize, t: &CaseTest) -> bool {
    match t {
        CaseTest::Word(t) | CaseTest::Packed(t, _) => {
            let (w, ok) = cx.out_word(oic, si, entry);
            t.eval_v(w, ok)
        }
        CaseTest::Bytes(t) => match cx.out_bytes(oic, si, entry) {
            Some(b) => t.eval(b),
            None => false,
        },
        CaseTest::InWords(ws) => {
            let (w, ok) = cx.out_word(oic, si, entry);
            ok && ws.binary_search(&w).is_ok()
        }
        CaseTest::And(ts) => ts.iter().all(|t| case_test(cx, si, entry, oic, t)),
    }
}

/// The staged (word, valid) pair of one fold source at one joined row —
/// the ONE place fold inputs are read (arith admitted overflow-free by
/// `check_join_agg`, so wrapping ops cannot wrap).
#[inline(always)]
fn stage_val(cx: &RowCx<'_, '_>, si: usize, entry: Option<(usize, usize)>, s: &FoldSrc<'_>) -> (i64, bool) {
    if let Some(c) = s.case {
        if !case_pass(cx, si, entry, s.oic, c) {
            return (0, c.else_zero);
        }
        if s.oi == usize::MAX {
            return (0, true);
        }
    }
    if s.is_count {
        return (0, cx.out_valid(s.oi, si, entry));
    }
    let (a, aok) = cx.out_word(s.oi, si, entry);
    let Some(ar) = s.arith else { return (a, aok) };
    match ar {
        JoinArith::AddK { k, .. } => (a.wrapping_add(k), aok),
        JoinArith::MulCC { .. } => {
            let (b, bok) = cx.out_word(s.oi2, si, entry);
            (a.wrapping_mul(b), aok && bok)
        }
        JoinArith::MulKSub { k, .. } => {
            let (b, bok) = cx.out_word(s.oi2, si, entry);
            (a.wrapping_mul(k.wrapping_sub(b)), aok && bok)
        }
        JoinArith::PackedMulK { k, sub, .. } => {
            let (b, bok) = cx.out_word(s.oi2, si, entry);
            let inner = if sub { k.wrapping_sub(b) } else { k.wrapping_add(b) };
            (a.wrapping_mul(inner), aok && bok)
        }
        JoinArith::PackedMulKSubCC { k, sub, .. } => {
            let (b, bok) = cx.out_word(s.oi2, si, entry);
            let (c, cok) = cx.out_word(s.oi3, si, entry);
            let (d, dok) = cx.out_word(s.oi4, si, entry);
            let inner = if sub { k.wrapping_sub(b) } else { k.wrapping_add(b) };
            let l = a.wrapping_mul(inner).wrapping_mul(s.align.0);
            let r = c.wrapping_mul(d).wrapping_mul(s.align.1);
            (l.wrapping_sub(r), aok && bok && cok && dok)
        }
    }
}

/// Monomorphic ungrouped-flush driver: one fold op, one staged lane, one
/// cell; `f` is the op-constant closure picked by the ONE match per
/// (flush, agg).
#[inline(always)]
fn flush_cell(lane: &[(i64, bool)], cell: &mut AccumCell, f: impl Fn(&mut AccumCell, i64, bool)) {
    for &(w, ok) in lane {
        f(cell, w, ok);
    }
}

/// Agg goal: fold cells in place of emission — no joined-row buffers.
/// Same R2 structure as GroupFold (no fold-op dispatch at row grain):
/// `row` only STAGES each fold agg's (word, valid) pair from the hoisted
/// `srcs` list; `flush` (at STAGE rows and once after the probe loop)
/// matches each agg's op ONCE and runs the monomorphic `flush_cell`
/// loop into the single per-agg cell.
struct AggFold<'a> {
    node: &'a JoinAggNode,
    rows: u64,
    cells: Vec<AccumCell>,
    srcs: Vec<Option<FoldSrc<'a>>>,
    blanes: Vec<Vec<(i64, bool)>>,
    staged: usize,
}

impl<'a> AggFold<'a> {
    fn new(node: &'a JoinAggNode) -> AggFold<'a> {
        let na = node.aggs.len();
        AggFold {
            node,
            rows: 0,
            cells: vec![AccumCell::default(); na],
            srcs: fold_srcs(node),
            blanes: (0..na).map(|_| Vec::with_capacity(STAGE)).collect(),
            staged: 0,
        }
    }

    fn flush(&mut self) {
        if self.staged == 0 {
            return;
        }
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(FoldSrc { op, .. }) = *src else { continue };
            let (lane, cell) = (&self.blanes[ai][..], &mut self.cells[ai]);
            match op {
                AggFoldOp::CountCol => flush_cell(lane, cell, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::CountCol, c, w, ok)
                }),
                AggFoldOp::Sum => flush_cell(lane, cell, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Sum, c, w, ok)
                }),
                AggFoldOp::Min => flush_cell(lane, cell, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Min, c, w, ok)
                }),
                AggFoldOp::Max => flush_cell(lane, cell, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Max, c, w, ok)
                }),
                AggFoldOp::SumSq | AggFoldOp::BitAnd | AggFoldOp::BitOr => {
                    unreachable!("join recognizer admits count/sum/min/max only")
                }
            }
        }
        for l in &mut self.blanes {
            l.clear();
        }
        self.staged = 0;
    }
}

impl Sink for AggFold<'_> {
    fn unit_start(&mut self, _ui: usize) {}

    fn row(&mut self, cx: &RowCx<'_, '_>, r: usize, entry: Option<(usize, usize)>) {
        self.rows += 1;
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(s) = src else { continue };
            self.blanes[ai].push(stage_val(cx, r, entry, s));
        }
        self.staged += 1;
        if self.staged >= STAGE {
            self.flush();
        }
    }
}

/// Grouped count(*) by a probe-side word key (Cnt64 = the count table of
/// record); NULL keys are one SQL group.
struct GroupCount {
    group_oi: usize,
    map: Cnt64,
    nulls: u64,
}

impl Sink for GroupCount {
    fn unit_start(&mut self, _ui: usize) {}

    fn row(&mut self, cx: &RowCx<'_, '_>, r: usize, entry: Option<(usize, usize)>) {
        let (w, ok) = cx.out_word(self.group_oi, r, entry);
        if ok {
            self.map.add(w as u64, 1);
        } else {
            self.nulls += 1;
        }
    }
}

/// Grouped folds keyed by a probe-side word key (Cells64 = the grouped
/// fold table of record — per-group row count + one AccumCell per agg,
/// the fold.rs scatter law). NULL keys are one SQL group whose cells
/// live aside the table (no sentinel key exists in the i64 domain).
///
/// Loop structure (R2: no fold-op interpretation at row grain): `row`
/// only STAGES — the group word plus each fold agg's (word, valid) pair
/// go into SoA buffers. `flush` (at STAGE rows, and once after the probe
/// loop) touches every staged key into the table (all growth happens
/// there, so the slot vector stays live), then dispatches each agg's
/// fold op ONCE and runs a monomorphic single-op loop over the batch.
struct GroupFold<'a> {
    node: &'a JoinAggNode,
    map: Cells64,
    null_rows: u64,
    null_cells: Vec<AccumCell>,
    srcs: Vec<Option<FoldSrc<'a>>>,
    bkeys: Vec<(i64, bool)>,
    blanes: Vec<Vec<(i64, bool)>>,
    slots: Vec<u32>,
}

const NULL_SLOT: u32 = u32::MAX;

/// Monomorphic flush driver: one fold op over one staged lane; `f` is
/// the op-constant fold closure picked by the ONE match per (flush, agg).
#[inline(always)]
fn flush_lane(
    lane: &[(i64, bool)],
    slots: &[u32],
    cells: &mut [AccumCell],
    null_cells: &mut [AccumCell],
    na: usize,
    ai: usize,
    f: impl Fn(&mut AccumCell, i64, bool),
) {
    for (i, &(w, ok)) in lane.iter().enumerate() {
        let c = if slots[i] == NULL_SLOT {
            &mut null_cells[ai]
        } else {
            &mut cells[slots[i] as usize * na + ai]
        };
        f(c, w, ok);
    }
}

impl GroupFold<'_> {
    fn new(node: &JoinAggNode) -> GroupFold<'_> {
        let na = node.aggs.len();
        GroupFold {
            node,
            map: Cells64::new(1024, na),
            null_rows: 0,
            null_cells: vec![AccumCell::default(); na],
            srcs: fold_srcs(node),
            bkeys: Vec::with_capacity(STAGE),
            blanes: (0..na).map(|_| Vec::with_capacity(STAGE)).collect(),
            slots: Vec::with_capacity(STAGE),
        }
    }

    fn flush(&mut self) {
        if self.bkeys.is_empty() {
            return;
        }
        self.slots.clear();
        // reserve_batch first: growth REHASHES, so the whole staged
        // batch's touches must fit without growing or slots recorded
        // earlier in this flush would go stale.
        self.map.reserve_batch(self.bkeys.len());
        for &(k, ok) in &self.bkeys {
            self.slots.push(if ok {
                self.map.touch(k as u64) as u32
            } else {
                self.null_rows += 1;
                NULL_SLOT
            });
        }
        let na = self.node.aggs.len();
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(FoldSrc { op, .. }) = *src else { continue };
            let (lane, slots) = (&self.blanes[ai][..], &self.slots[..]);
            let (cells, nulls) = (&mut self.map.cells[..], &mut self.null_cells[..]);
            match op {
                AggFoldOp::CountCol => flush_lane(lane, slots, cells, nulls, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::CountCol, c, w, ok)
                }),
                AggFoldOp::Sum => flush_lane(lane, slots, cells, nulls, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Sum, c, w, ok)
                }),
                AggFoldOp::Min => flush_lane(lane, slots, cells, nulls, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Min, c, w, ok)
                }),
                AggFoldOp::Max => flush_lane(lane, slots, cells, nulls, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Max, c, w, ok)
                }),
                AggFoldOp::SumSq | AggFoldOp::BitAnd | AggFoldOp::BitOr => {
                    unreachable!("join recognizer admits count/sum/min/max only")
                }
            }
        }
        self.bkeys.clear();
        for l in &mut self.blanes {
            l.clear();
        }
    }
}

impl Sink for GroupFold<'_> {
    fn unit_start(&mut self, _ui: usize) {}

    fn row(&mut self, cx: &RowCx<'_, '_>, r: usize, entry: Option<(usize, usize)>) {
        self.bkeys.push(cx.out_word(self.node.group_oi[0], r, entry));
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(s) = src else { continue };
            self.blanes[ai].push(stage_val(cx, r, entry, s));
        }
        if self.bkeys.len() >= STAGE {
            self.flush();
        }
    }
}

/// The join pipeline: partitioned build (pass 1 scatter, pass 2 one-owner
/// tables), then a staged probe driving the consumer inside the loop.
/// Returns per-worker sinks (empty = no probe units).
fn run_core<K: Sink, MK: Fn() -> K + Sync>(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    node: &JoinNode,
    mk: MK,
) -> Result<Vec<K>, JoinRefuse> {
    let pool = bctx.pool;
    let (bbank, pbank) = (bctx.bank, pctx.bank);
    assert_eq!(dctxs.len(), node.dims.len(), "dim ctxs align with dim stages");
    // [mapjoingoal] goal-fed stages carry their table as data; the
    // bank-scan stages align with the ctx list in stage order.
    let n_bank_stages =
        node.filters.iter().filter(|f| matches!(f.src, StageSrc::Bank)).count();
    if fctxs.len() != n_bank_stages {
        return Err(JoinRefuse::Unsupported { what: "filter-ctx-align" });
    }
    // [crossdim-or] terms split per site; masks accumulate at each
    // site's scan, AND at scatter (build/dim) and probe (probe terms).
    let so = node.staged_or.as_ref();
    let or_full = so.map(|s| s.full_mask()).unwrap_or(0);
    let mut or_probe: Vec<&OrTerm> = Vec::new();
    let mut or_build: Vec<&OrTerm> = Vec::new();
    let mut or_dim: Vec<Vec<&OrTerm>> = vec![Vec::new(); node.dims.len()];
    if let Some(s) = so {
        for t in &s.terms {
            match t.site {
                JoinSide::Probe => or_probe.push(t),
                JoinSide::Build => or_build.push(t),
                JoinSide::Dim(di) => or_dim[di as usize].push(t),
            }
        }
    }
    let or_nonprobe = !or_build.is_empty() || or_dim.iter().any(|v| !v.is_empty());
    // The entry mask lane exists only when BOTH ends carry terms; a
    // one-sided disjunction resolves at its own end (probe survivor
    // filter / scatter prune).
    let or_lane = (or_full != 0 && !or_probe.is_empty() && or_nonprobe) as usize;

    let bkeys: Vec<u32> = node.keys.iter().map(|k| k.build_col).collect();
    let pkeys: Vec<u32> = node.keys.iter().map(|k| k.probe_col).collect();
    let bquals: Vec<u32> = node.quals.iter().map(|q| q.build_col).collect();
    let pquals: Vec<u32> = node.quals.iter().map(|q| q.probe_col).collect();
    let bouts: Vec<u32> =
        node.out.iter().filter(|o| o.side == JoinSide::Build).map(|o| o.col).collect();
    let pouts: Vec<u32> =
        node.out.iter().filter(|o| o.side == JoinSide::Probe).map(|o| o.col).collect();
    // [sqe-mech3] dim payload columns per stage, split word/text (dedup,
    // node.out order); dim-probe key lanes ride the build plan's extras.
    let ndim = node.dims.len();
    let mut dim_pay_w: Vec<Vec<u32>> = vec![Vec::new(); ndim];
    let mut dim_pay_t: Vec<Vec<u32>> = vec![Vec::new(); ndim];
    for o in &node.out {
        if let JoinSide::Dim(di) = o.side {
            let (di, bank) = (di as usize, dctxs[di as usize].bank);
            let dst = if matches!(bank.face(o.col), Face::Varlena) {
                &mut dim_pay_t[di]
            } else {
                &mut dim_pay_w[di]
            };
            if !dst.contains(&o.col) {
                dst.push(o.col);
            }
        }
    }
    // [sqe-join-depth] chained dim keys (src = an earlier dim stage)
    // resolve from the PARENT stage's matched entry: the source column
    // rides the parent's word-payload lanes (internal lanes when the
    // goal never outputs them — constructor-validated word faces).
    for d in &node.dims {
        for k in &d.keys {
            if let DimSrc::Dim(j) = k.src {
                let dst = &mut dim_pay_w[j as usize];
                if !dst.contains(&k.build_col) {
                    dst.push(k.build_col);
                }
            }
        }
    }
    // Build-scan dim-probe lanes: the BUILD-sourced keys only (chained
    // keys never read the build scan).
    let dim_probe_cols: Vec<u32> = node
        .dims
        .iter()
        .flat_map(|d| d.keys.iter())
        .filter(|k| k.src == DimSrc::Build)
        .map(|k| k.build_col)
        .collect();
    // Per-stage per-key resolution: a flat build-scan lane index, or
    // (parent stage, parent payload lane) for chained keys.
    enum DKeyRef {
        Flat(usize),
        Chain { parent: usize, pay: usize },
    }
    let dkey_refs: Vec<Vec<DKeyRef>> = {
        let mut flat = 0usize;
        node.dims
            .iter()
            .map(|d| {
                d.keys
                    .iter()
                    .map(|k| match k.src {
                        DimSrc::Build => {
                            let i = flat;
                            flat += 1;
                            DKeyRef::Flat(i)
                        }
                        DimSrc::Dim(j) => DKeyRef::Chain {
                            parent: j as usize,
                            pay: dim_pay_w[j as usize]
                                .iter()
                                .position(|&c| c == k.build_col)
                                .expect("chained key col planned above"),
                        },
                    })
                    .collect()
            })
            .collect()
    };
    let stage_all_flat: Vec<bool> = dkey_refs
        .iter()
        .map(|ks| ks.iter().all(|r| matches!(r, DKeyRef::Flat(_))))
        .collect();
    // [crossdim-or]/[semianti-flt] aux lanes computed first, then ONE
    // final plan per side; [subset] set-filter columns ride `extra`.
    // [crossdim-or]/[semianti-flt] aux lanes: or terms first (this
    // side's), then per hosted stage its key + qual host columns.
    let mut baux: Vec<(u32, bool)> =
        or_build.iter().map(|t| (t.col, test_needs_bytes(&t.test))).collect();
    let mut paux: Vec<(u32, bool)> =
        or_probe.iter().map(|t| (t.col, test_needs_bytes(&t.test))).collect();
    let mut bhf: Vec<HostFilter> = Vec::new();
    let mut phf: Vec<HostFilter> = Vec::new();
    let mut fq_cols: Vec<Vec<u32>> = Vec::with_capacity(node.filters.len());
    for (fi, f) in node.filters.iter().enumerate() {
        let mut qcols: Vec<u32> = Vec::new();
        for q in &f.quals {
            if !qcols.contains(&q.stage_col) {
                qcols.push(q.stage_col);
            }
        }
        if let Some(StageFold::Num(nf)) = &f.fold {
            if !qcols.contains(&nf.col) {
                qcols.push(nf.col);
            }
        }
        let (aux, hfs) = match f.host {
            JoinSide::Build => (&mut baux, &mut bhf),
            JoinSide::Probe => (&mut paux, &mut phf),
            JoinSide::Dim(_) => {
                return Err(JoinRefuse::Unsupported { what: "filter-host-side" });
            }
        };
        let mut push_aux = |c: u32| {
            aux.push((c, false));
            aux.len() - 1
        };
        let key_aux: Vec<usize> = f.keys.iter().map(|k| push_aux(k.host_col)).collect();
        let qual_aux: Vec<usize> = f.quals.iter().map(|q| push_aux(q.host_col)).collect();
        let num_aux = match &f.fold {
            Some(StageFold::Num(nf)) => Some(push_aux(nf.qual.probe_col)),
            _ => None,
        };
        let qual_pay: Vec<usize> = f
            .quals
            .iter()
            .map(|q| qcols.iter().position(|&c| c == q.stage_col).expect("planned above"))
            .collect();
        hfs.push(HostFilter { fi, key_aux, qual_aux, qual_pay, num_aux });
        fq_cols.push(qcols);
    }
    let bextra: Vec<u32> = dim_probe_cols
        .iter()
        .copied()
        .chain(node.build_in.iter().map(|f| f.col))
        .collect();
    let pextra: Vec<u32> = node.probe_in.iter().map(|f| f.col).collect();
    let mut bplan = side_plan(bbank, &bkeys, &bquals, &node.build_pred, &bouts, &bextra, &baux);
    let mut pplan = side_plan(pbank, &pkeys, &pquals, &node.probe_pred, &pouts, &pextra, &paux);
    // [corrnumcell] cell-qual lanes are planned even when no qual/out names them.
    fn plan_lane(plan: &mut SidePlan, bank: &Bank, attno: u32) -> usize {
        if let Some(i) = plan.cols.iter().position(|c| c.attno == attno) {
            return i;
        }
        plan.cols.push(ColPlan {
            attno,
            face: bank.face(attno),
            nf: bank.null_free(attno),
            text: false,
            needs_bytes: false,
        });
        plan.cols.len() - 1
    }
    let nf_bci = node.num_fold.as_ref().map(|f| plan_lane(&mut bplan, bbank, f.col));
    let nf_pci = node.num_fold.as_ref().map(|f| plan_lane(&mut pplan, pbank, f.qual.probe_col));
    let (bplan, pplan) = (bplan, pplan);

    let key_text: Vec<bool> =
        node.keys.iter().map(|k| bbank.typ(k.build_col).is_varlena()).collect();
    let nkw = key_text.iter().filter(|&&t| !t).count();
    let nkf = key_text.len() - nkw;
    // key ci lists split by kind (kw/kf lane order = key order per kind).
    let split_keys = |plan: &SidePlan| -> (Vec<usize>, Vec<usize>) {
        let mut kw = Vec::new();
        let mut kf = Vec::new();
        for (ki, &ci) in plan.key_ci.iter().enumerate() {
            if key_text[ki] {
                kf.push(ci);
            } else {
                kw.push(ci);
            }
        }
        (kw, kf)
    };
    let (kw_cis_b, kf_cis_b) = split_keys(&bplan);
    let (kw_cis_p, kf_cis_p) = split_keys(&pplan);
    // [sqe-tpch-mech] survivor-set membership filters (agg-result-as-set
    // consumption): resolve each side's filter onto an existing equi-key
    // decode lane (word columns only — typed refusal otherwise).
    let resolve_in = |f: &Option<InSetFilter>,
                      side_keys: &[u32],
                      plan: &SidePlan|
     -> Result<Option<(usize, InSetFilter)>, JoinRefuse> {
        let Some(f) = f else { return Ok(None) };
        // An equi-key lane, or [subset] any planned column of the side.
        let ci = side_keys
            .iter()
            .position(|&c| c == f.col)
            .map(|ki| plan.key_ci[ki])
            .or_else(|| plan.cols.iter().position(|c| c.attno == f.col))
            .ok_or(JoinRefuse::Unsupported { what: "in-set-col-not-key" })?;
        if plan.cols[ci].text {
            return Err(JoinRefuse::Unsupported { what: "in-set-col-text" });
        }
        Ok(Some((ci, f.clone())))
    };
    let bin_set = resolve_in(&node.build_in, &bkeys, &bplan)?;
    let pin_set = resolve_in(&node.probe_in, &pkeys, &pplan)?;
    let (bin_setr, pin_setr) = (&bin_set, &pin_set);
    let mut nplan = norm_plan(&pplan, &key_text);
    if let Some(pci) = nf_pci {
        nplan.word[pci] = true;
        nplan.valid[pci] = true;
    }
    let nplan = nplan;
    assert!(nkw <= MAX_KEY_LANES && nkf <= MAX_KEY_LANES, "key lane cap (constructor-checked)");

    let mut pay_word: Vec<usize> = Vec::new();
    let mut pay_text: Vec<usize> = Vec::new();
    for &ci in bplan.qual_ci.iter().chain(bplan.out_ci.iter()) {
        let dst = if bplan.cols[ci].text { &mut pay_text } else { &mut pay_word };
        if !dst.contains(&ci) {
            dst.push(ci);
        }
    }
    if let Some(ci) = nf_bci {
        if !pay_word.contains(&ci) {
            pay_word.push(ci);
        }
    }
    // [sqe-mech3] the build table's payload lanes = the build side's own
    // (prefix, unchanged) then each dim stage's payload columns — one
    // flat lane space so OutRef::BuildW/BuildT serve every non-probe out.
    let bnpw = pay_word.len();
    let bnpt = pay_text.len();
    let mut dimw_base: Vec<usize> = Vec::with_capacity(ndim);
    let mut dimt_base: Vec<usize> = Vec::with_capacity(ndim);
    {
        let (mut w, mut t) = (bnpw, bnpt);
        for di in 0..ndim {
            dimw_base.push(w);
            dimt_base.push(t);
            w += dim_pay_w[di].len();
            t += dim_pay_t[di].len();
        }
    }
    // [crossdim-or] the entry mask rides one extra word lane at the end
    // of the payload space (always-valid; every existing lane index is
    // below it, so the flat OutRef space is untouched).
    let npw = bnpw + dim_pay_w.iter().map(Vec::len).sum::<usize>() + or_lane;
    let or_pj = npw.saturating_sub(1);
    let npt = bnpt + dim_pay_t.iter().map(Vec::len).sum::<usize>();
    let qual_pay: Vec<usize> = bplan
        .qual_ci
        .iter()
        .map(|ci| pay_word.iter().position(|c| c == ci).unwrap())
        .collect();
    // [corrsubq] the grouped-build collapse lane: the fold column's word
    // payload index (constructor-guaranteed to exist) + the missing law.
    let fold_lane: Option<FoldLane> = match (&node.build_fold, &node.num_fold) {
        (None, None) => None,
        (Some(f), _) => {
            let ci = bplan
                .cols
                .iter()
                .position(|c| c.attno == f.col && !c.text)
                .ok_or(JoinRefuse::Unsupported { what: "build-fold-col" })?;
            let pj = pay_word
                .iter()
                .position(|&c| c == ci)
                .ok_or(JoinRefuse::Unsupported { what: "build-fold-col" })?;
            if !node.dims.is_empty() {
                return Err(JoinRefuse::Unsupported { what: "build-fold-shape" });
            }
            Some(FoldLane::Word(pj, f.op))
        }
        (None, Some(f)) => {
            let ci = nf_bci.expect("num-fold lane planned above");
            let pj = pay_word.iter().position(|&c| c == ci).expect("num-fold lane is payload");
            Some(FoldLane::Num { pj, op: f.op, scale: f.scale })
        }
    };
    let fold_missing: Option<i64> = node.build_fold.as_ref().and_then(|f| f.missing);
    let bout_pay = bout_pay_map(&bplan, &pay_word, &pay_text);
    let out_map: Vec<OutRef> = {
        let (mut bo, mut po) = (0usize, 0usize);
        node.out
            .iter()
            .map(|o| match o.side {
                JoinSide::Probe => {
                    let ci = pplan.out_ci[po];
                    po += 1;
                    OutRef::Probe { ci }
                }
                JoinSide::Build => {
                    let (text, pj) = bout_pay[bo];
                    bo += 1;
                    if text {
                        OutRef::BuildT { pj }
                    } else {
                        OutRef::BuildW { pj }
                    }
                }
                JoinSide::Dim(di) => {
                    let (di, bank) = (di as usize, dctxs[di as usize].bank);
                    if matches!(bank.face(o.col), Face::Varlena) {
                        let pj = dimt_base[di]
                            + dim_pay_t[di].iter().position(|&c| c == o.col).unwrap();
                        OutRef::BuildT { pj }
                    } else {
                        let pj = dimw_base[di]
                            + dim_pay_w[di].iter().position(|&c| c == o.col).unwrap();
                        OutRef::BuildW { pj }
                    }
                }
            })
            .collect()
    };

    let entry_fixed =
        8 + 8 * nkw + 16 * nkf + 9 * npw + 8 * npt + if node.num_fold.is_some() { 28 } else { 0 };
    if node.build_pred.is_none()
        && node.dims.is_empty()
        && node.filters.is_empty()
        && so.is_none()
    {
        let est = bbank.rows_total().saturating_mul(entry_fixed as u64);
        if est > node.build_budget_bytes as u64 {
            return Err(JoinRefuse::BuildExceedsBudget {
                est_bytes: est,
                budget: node.build_budget_bytes as u64,
            });
        }
    }

    // [sqe-mech3] dimension stages build FIRST (small sides; the floor's
    // staged pipeline) — their footprint charges the join budget.
    let dims_built: Vec<DimBuilt> = node
        .dims
        .iter()
        .enumerate()
        .map(|(di, st)| {
            build_dim(dctxs[di], st, &dim_pay_w[di], &dim_pay_t[di], &or_dim[di], or_full, None)
        })
        .collect();
    // [semianti-flt] membership stages build like dims: keys + the
    // quals' stage columns as word payload; scan conjuncts on the pass.
    let mut filters_built: Vec<DimBuilt> = Vec::with_capacity(node.filters.len());
    {
        let mut fbi = 0usize;
        for (fi, f) in node.filters.iter().enumerate() {
            // [mapjoingoal] pre-collapsed rows seal directly — the goal
            // already grouped, so the entries ARE the representatives.
            if let StageSrc::Rows(r) = &f.src {
                // nk 0 = the lowering placeholder: a run path that never
                // swapped the goal's answer in fails CLOSED here.
                if r.nk == 0 {
                    return Err(JoinRefuse::Unsupported { what: "map-goal-rows-missing" });
                }
                filters_built.push(prebuilt_stage(r));
                continue;
            }
            let synth = DimStage {
                keys: f
                    .keys
                    .iter()
                    .map(|k| DimKey { dim_col: k.stage_col, build_col: 0, src: DimSrc::Build })
                    .collect(),
                pred: f.pred.clone(),
                text_eqs: f.text_eqs.clone(),
                rows_hint: f.rows_hint,
            };
            let fl = f.fold.as_ref().map(|sf| match sf {
                StageFold::Word(bf) => FoldLane::Word(
                    fq_cols[fi].iter().position(|&c| c == bf.col).expect("planned above"),
                    bf.op,
                ),
                StageFold::Num(nf) => FoldLane::Num {
                    pj: fq_cols[fi].iter().position(|&c| c == nf.col).expect("planned above"),
                    op: nf.op,
                    scale: nf.scale,
                },
            });
            filters_built.push(build_dim(fctxs[fbi], &synth, &fq_cols[fi], &[], &[], 0, fl));
            fbi += 1;
        }
    }
    {
        let dim_bytes: usize = dims_built
            .iter()
            .chain(filters_built.iter())
            .map(DimBuilt::est_bytes)
            .sum();
        if dim_bytes > node.build_budget_bytes {
            return Err(JoinRefuse::BuildExceedsBudget {
                est_bytes: dim_bytes as u64,
                budget: node.build_budget_bytes as u64,
            });
        }
    }
    let bfps = fp_planes(bctx, &bplan);
    let pfps = fp_planes(pctx, &pplan);

    // ndv upper bound = build rows; phase 2 refines from real stats.
    let p = partition_count(bbank.rows_total().max(1) as usize, 64, node.l2_bytes, pool.threads());
    let pbits = p.trailing_zeros();

    // Granule-walk lane per side: the first key lane, or (keyless
    // nest-loop nodes) the first planned column (constructor-guaranteed).
    let bwalk_col = node.keys.first().map(|k| k.build_col).unwrap_or_else(|| bplan.cols[0].attno);
    let pwalk_col = node.keys.first().map(|k| k.probe_col).unwrap_or_else(|| pplan.cols[0].attno);
    let bunits = if bbank.parts.is_empty() {
        Arc::new(Vec::new())
    } else {
        bctx.faces.walk(bbank, bwalk_col)
    };
    let acc = AtomicU64::new(0);
    let abort = AtomicBool::new(false);
    struct BS {
        dec: SideDecode,
        buckets: Vec<Bucket>,
        sel: Vec<u16>,
        /// [sqe-semi-anti] per-survivor NULL-join-key flags (RightAnti
        /// null retention; empty-equivalent all-false otherwise).
        bnull: Vec<bool>,
        /// Column-major survivor planes: key words/fps, payload
        /// words/validity, text-payload validity.
        bkw: Vec<Vec<i64>>,
        bkf: Vec<Vec<u128>>,
        bpw: Vec<Vec<i64>>,
        bpv: Vec<Vec<bool>>,
        btv: Vec<Vec<bool>>,
        /// [sqe-mech3] dim-probe key lanes (words + validity) over sel.
        dkw: Vec<Vec<i64>>,
        dkv: Vec<Vec<bool>>,
        /// [crossdim-or] build-scan arm masks over sel.
        bom: Vec<u64>,
    }
    static PARKB: std::sync::Mutex<Vec<Vec<Bucket>>> = std::sync::Mutex::new(Vec::new());
    let bplanr = &bplan;
    let bfpsr = &bfps;
    let dims_builtr = &dims_built;
    let dkey_refsr = &dkey_refs;
    let stage_all_flatr = &stage_all_flat;
    let filters_builtr = &filters_built;
    let (bhfr, phfr) = (&bhf, &phf);
    let (or_buildr, or_prober) = (&or_build, &or_probe);
    let ndklanes = dim_probe_cols.len();
    let pass1 = pool.run_finish(
        bunits.len(),
        |_| {
            let mut buckets = PARKB.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Bucket::default()).collect();
            } else {
                for b in buckets.iter_mut() {
                    b.hash.clear();
                    b.kw.clear();
                    b.kf.clear();
                    b.pw.clear();
                    b.pv.clear();
                    b.pt.clear();
                    b.arena.clear();
                    b.nullk.clear();
                }
            }
            BS {
                dec: SideDecode::fetch(bplanr),
                buckets,
                sel: Vec::new(),
                bnull: Vec::new(),
                bkw: vec![Vec::new(); nkw],
                bkf: vec![Vec::new(); nkf],
                bpw: vec![Vec::new(); bnpw],
                bpv: vec![Vec::new(); bnpw],
                btv: vec![Vec::new(); bnpt],
                dkw: vec![Vec::new(); ndklanes],
                dkv: vec![Vec::new(); ndklanes],
                bom: Vec::new(),
            }
        },
        |s: &mut BS, i| {
            if abort.load(Ordering::Relaxed) {
                return;
            }
            let (pi, g, rows, _) = bunits[i];
            let rows = rows as usize;
            let BS { dec, buckets, sel, bnull, bkw, bkf, bpw, bpv, btv, dkw, dkv, bom } = s;
            let (lanes, gvs) = dec.granule(bbank, bplanr, bfpsr, pi, g, rows);
            let mut grain = 0usize;
            pred_select(&node.build_pred, bplanr, dec, &lanes, &gvs, rows, sel);
            // [semianti-flt] host-side membership filters (WHERE grain).
            filter_hosts(&node.filters, filters_builtr, bhfr, bplanr, dec, &lanes, &gvs, sel);
            // NULL build keys never match: filter survivors per key lane
            // (column-major AND across key lanes — the same final set as
            // the per-row key-order walk). [sqe-semi-anti] RightAnti
            // RETAINS null-keyed rows (they emit — NOT EXISTS over a
            // NULL key is TRUE): flags ride `bnull`, entries unlinked.
            let keep_null =
                matches!(node.join_type, JoinType::RightAnti | JoinType::Right);
            if !keep_null {
                for &ci in bplanr.key_ci.iter() {
                    if bplanr.cols[ci].nf || gvs[ci].all_valid() {
                        continue;
                    }
                    let scr = &dec.scr[ci];
                    sel.retain(|&r| scr.row_valid(r as usize));
                }
            }
            // [sqe-tpch-mech]/[subset] the filter's own 3VL decides.
            if let Some((ci, f)) = bin_setr {
                let face = bplanr.cols[*ci].face;
                let d = words_of(&lanes[*ci]);
                let np = f.null_passes();
                let nfok = bplanr.cols[*ci].nf || gvs[*ci].all_valid();
                let scr = &dec.scr[*ci];
                sel.retain(|&r| {
                    let r = r as usize;
                    if nfok || scr.row_valid(r) {
                        f.word_passes(face.word_key(d[r]))
                    } else {
                        np
                    }
                });
            }
            // [crossdim-or] build-scan arm masks (dim masks AND in at
            // scatter, per match combination).
            bom.clear();
            if or_full != 0 && or_nonprobe {
                bom.resize(sel.len(), or_full);
                or_mask_pass(or_buildr, 0, bplanr, dec, &lanes, &gvs, sel, bom);
            }
            bnull.clear();
            bnull.resize(sel.len(), false);
            if keep_null {
                for &ci in bplanr.key_ci.iter() {
                    if bplanr.cols[ci].nf || gvs[ci].all_valid() {
                        continue;
                    }
                    let scr = &dec.scr[ci];
                    for (i, &r) in sel.iter().enumerate() {
                        bnull[i] |= !scr.row_valid(r as usize);
                    }
                }
            }
            // Column-major normalization: one Lane/Face/validity dispatch
            // per (granule, column); the scatter loop reads flat slices.
            for (iw, &ci) in kw_cis_b.iter().enumerate() {
                let face = bplanr.cols[ci].face;
                let d = words_of(&lanes[ci]);
                bkw[iw].clear();
                bkw[iw].extend(sel.iter().map(|&r| face.word_key(d[r as usize])));
            }
            for (jf, &ci) in kf_cis_b.iter().enumerate() {
                bkf[jf].clear();
                match &lanes[ci] {
                    Lane::Codes(cz) if !keep_null => {
                        let tbl = &bfpsr[ci].as_ref().expect("codes lane has fp plane")[pi];
                        bkf[jf].extend(sel.iter().map(|&r| tbl[cz[r as usize] as usize]));
                    }
                    // survivors hold non-NULL keys (filtered above), so
                    // the payload deref is safe.
                    Lane::Words(d) if !keep_null => {
                        bkf[jf].extend(sel.iter().map(|&r| {
                            crate::fp::entry_fp128(unsafe { varlena_payload(d[r as usize]) })
                        }))
                    }
                    // [sqe-semi-anti] RightAnti retains null-keyed rows:
                    // their key slots are never dereferenced (fp = 0;
                    // the entry stays unlinked, so no compare sees it).
                    Lane::Codes(cz) => {
                        let tbl = &bfpsr[ci].as_ref().expect("codes lane has fp plane")[pi];
                        bkf[jf].extend(sel.iter().enumerate().map(|(i, &r)| {
                            if bnull[i] { 0 } else { tbl[cz[r as usize] as usize] }
                        }));
                    }
                    Lane::Words(d) => bkf[jf].extend(sel.iter().enumerate().map(|(i, &r)| {
                        if bnull[i] {
                            0
                        } else {
                            crate::fp::entry_fp128(unsafe { varlena_payload(d[r as usize]) })
                        }
                    })),
                }
            }
            for (pj, &ci) in pay_word.iter().enumerate() {
                let face = bplanr.cols[ci].face;
                let d = words_of(&lanes[ci]);
                bpw[pj].clear();
                bpv[pj].clear();
                if bplanr.cols[ci].nf || gvs[ci].all_valid() {
                    bpw[pj].extend(sel.iter().map(|&r| face.word_key(d[r as usize])));
                    bpv[pj].resize(sel.len(), true);
                } else {
                    let scr = &dec.scr[ci];
                    bpv[pj].extend(sel.iter().map(|&r| scr.row_valid(r as usize)));
                    bpw[pj].extend(sel.iter().zip(bpv[pj].iter()).map(|(&r, &ok)| {
                        if ok { face.word_key(d[r as usize]) } else { 0 }
                    }));
                }
            }
            let mut traw: Vec<&[u64]> = Vec::with_capacity(bnpt);
            for (pj, &ci) in pay_text.iter().enumerate() {
                traw.push(words_of(&lanes[ci]));
                btv[pj].clear();
                if bplanr.cols[ci].nf || gvs[ci].all_valid() {
                    btv[pj].resize(sel.len(), true);
                } else {
                    let scr = &dec.scr[ci];
                    btv[pj].extend(sel.iter().map(|&r| scr.row_valid(r as usize)));
                }
            }
            // [sqe-mech3] dim-probe key lanes (NULL keys never match —
            // validity rides `dkv`; the scatter drops the row). ONLY the
            // first `ndklanes` extra lanes are dim-probe keys — a
            // trailing [sqe-semijoin] `build_in` filter lane rides
            // `extra_ci` for decode staging only (consumed by the
            // in-set retain above, never by the dim scatter).
            for (fl, &ci) in bplanr.extra_ci.iter().take(ndklanes).enumerate() {
                let face = bplanr.cols[ci].face;
                let d = words_of(&lanes[ci]);
                dkw[fl].clear();
                dkv[fl].clear();
                if bplanr.cols[ci].nf || gvs[ci].all_valid() {
                    dkw[fl].extend(sel.iter().map(|&r| face.word_key(d[r as usize])));
                    dkv[fl].resize(sel.len(), true);
                } else {
                    let scr = &dec.scr[ci];
                    dkv[fl].extend(sel.iter().map(|&r| scr.row_valid(r as usize)));
                    dkw[fl].extend(sel.iter().zip(dkv[fl].iter()).map(|(&r, &ok)| {
                        if ok { face.word_key(d[r as usize]) } else { 0 }
                    }));
                }
            }
            // Scatter: flat-slice row loop (no per-row enum dispatch).
            // [sqe-mech3] each row resolves its dim matches first
            // (INNER: no match or a NULL key drops the row), then emits
            // one entry per match COMBINATION — the general hash-join
            // multiplicity law; dim payload lanes append after build's.
            let mut kw = [0i64; MAX_KEY_LANES];
            let mut kf = [0u128; MAX_KEY_LANES];
            let mut dkeys = [[0i64; MAX_KEY_LANES]; crate::joins::MAX_DIM_STAGES];
            'rows: for (i, &r16) in sel.iter().enumerate() {
                let r = r16 as usize;
                for (iw, col) in bkw.iter().enumerate() {
                    kw[iw] = col[i];
                }
                for (jf, col) in bkf.iter().enumerate() {
                    kf[jf] = col[i];
                }
                // Build-sourced dim keys fill from the build scan's extra
                // lanes (a NULL key drops the row — INNER law); all-flat
                // stages pre-resolve their first match so an empty stage
                // still drops the row before any emit ([sqe-mech3]).
                let mut prefirst = [NO_ENTRY; crate::joins::MAX_DIM_STAGES];
                for (di, dt) in dims_builtr.iter().enumerate() {
                    for (li, kr) in dkey_refsr[di].iter().enumerate() {
                        if let DKeyRef::Flat(fl) = kr {
                            if !dkv[*fl][i] {
                                continue 'rows;
                            }
                            dkeys[di][li] = dkw[*fl][i];
                        }
                    }
                    if stage_all_flatr[di] {
                        prefirst[di] = dt.first_match(&dkeys[di][..dt.nk]);
                        if prefirst[di] == NO_ENTRY {
                            continue 'rows;
                        }
                    }
                }
                let h = key_hash(&kw[..nkw], &kf[..nkf]);
                let mut emit = |des: &[u32]| {
                    // [crossdim-or] accumulate this combination's mask;
                    // an all-clear mask can never pass any probe row —
                    // the entry is pruned here, never materialized.
                    let mut om = u64::MAX;
                    if or_full != 0 && or_nonprobe {
                        om = bom[i];
                        for (di, dt) in dims_builtr.iter().enumerate() {
                            if !dt.omask.is_empty() {
                                om &= dt.omask[des[di] as usize];
                            }
                        }
                        if om == 0 {
                            return;
                        }
                    }
                    let b = &mut buckets[(h >> (64 - pbits)) as usize];
                    b.hash.push(h);
                    b.nullk.push(bnull[i]);
                    b.kw.extend_from_slice(&kw[..nkw]);
                    b.kf.extend_from_slice(&kf[..nkf]);
                    for pj in 0..bnpw {
                        b.pw.push(bpw[pj][i]);
                        b.pv.push(bpv[pj][i]);
                    }
                    for (di, dt) in dims_builtr.iter().enumerate() {
                        let e = des[di] as usize;
                        for k in 0..dt.npw {
                            b.pw.push(dt.pw[e * dt.npw + k]);
                            b.pv.push(dt.pv[e * dt.npw + k]);
                        }
                    }
                    if or_lane == 1 {
                        b.pw.push(om as i64);
                        b.pv.push(true);
                    }
                    for pj in 0..bnpt {
                        if btv[pj][i] {
                            let bytes = unsafe { varlena_payload(traw[pj][r]) };
                            let off = b.arena.len() as u32;
                            b.arena.extend_from_slice(bytes);
                            b.pt.push((off, bytes.len() as u32));
                            grain += bytes.len();
                        } else {
                            b.pt.push(T_NULL);
                        }
                    }
                    for (di, dt) in dims_builtr.iter().enumerate() {
                        let e = des[di] as usize;
                        for k in 0..dt.npt {
                            let (o, l) = dt.pt[e * dt.npt + k];
                            if (o, l) == T_NULL {
                                b.pt.push(T_NULL);
                            } else {
                                let bytes = &dt.arena[o as usize..(o + l) as usize];
                                let off = b.arena.len() as u32;
                                b.arena.extend_from_slice(bytes);
                                b.pt.push((off, l));
                                grain += bytes.len();
                            }
                        }
                    }
                    grain += entry_fixed;
                };
                // [sqe-join-depth] the generalized match-combination walk:
                // depth-first over the stages in order; entering a stage
                // resolves its CHAINED keys from the parents' current
                // matches (a NULL parent payload prunes the branch, never
                // the row), emits one entry per full combination, then
                // backtracks — N-stage product, 0/1/2 identical to the
                // fixed loops it replaces.
                let ndimb = dims_builtr.len();
                if ndimb == 0 {
                    emit(&[]);
                } else {
                    let mut des = [NO_ENTRY; crate::joins::MAX_DIM_STAGES];
                    let mut lvl = 0usize;
                    'walk: loop {
                        while lvl < ndimb {
                            let dt = &dims_builtr[lvl];
                            let mut ok = true;
                            for (li, kr) in dkey_refsr[lvl].iter().enumerate() {
                                if let DKeyRef::Chain { parent, pay } = kr {
                                    let dp = &dims_builtr[*parent];
                                    let e = des[*parent] as usize;
                                    if !dp.pv[e * dp.npw + pay] {
                                        ok = false;
                                        break;
                                    }
                                    dkeys[lvl][li] = dp.pw[e * dp.npw + pay];
                                }
                            }
                            let e0 = if !ok {
                                NO_ENTRY
                            } else if stage_all_flatr[lvl] {
                                prefirst[lvl]
                            } else {
                                dt.first_match(&dkeys[lvl][..dt.nk])
                            };
                            if e0 == NO_ENTRY {
                                // Backtrack to the deepest stage with
                                // another match; none left = row done.
                                loop {
                                    if lvl == 0 {
                                        continue 'rows;
                                    }
                                    lvl -= 1;
                                    let dtp = &dims_builtr[lvl];
                                    let ne =
                                        dtp.next_match(&dkeys[lvl][..dtp.nk], des[lvl]);
                                    if ne != NO_ENTRY {
                                        des[lvl] = ne;
                                        lvl += 1;
                                        break;
                                    }
                                }
                                continue;
                            }
                            des[lvl] = e0;
                            lvl += 1;
                        }
                        emit(&des[..ndimb]);
                        loop {
                            lvl -= 1;
                            let dt = &dims_builtr[lvl];
                            let ne = dt.next_match(&dkeys[lvl][..dt.nk], des[lvl]);
                            if ne != NO_ENTRY {
                                des[lvl] = ne;
                                lvl += 1;
                                break;
                            }
                            if lvl == 0 {
                                break 'walk;
                            }
                        }
                    }
                }
            }
            if acc.fetch_add(grain as u64, Ordering::Relaxed) + grain as u64
                > node.build_budget_bytes as u64
            {
                abort.store(true, Ordering::Relaxed);
            }
        },
        // Worker-side finish: decode arenas park on THIS worker's depot
        // (also on the budget-abort path); only the scattered buckets
        // cross back — the staging lanes drop with the state.
        |s: BS| {
            s.dec.park();
            s.buckets
        },
    );
    if abort.load(Ordering::Relaxed) {
        let est = acc.load(Ordering::Relaxed);
        let mut park = PARKB.lock().unwrap();
        for buckets in pass1 {
            park.push(buckets);
        }
        return Err(JoinRefuse::BuildExceedsBudget {
            est_bytes: est,
            budget: node.build_budget_bytes as u64,
        });
    }

    let scattered: Vec<&Vec<Bucket>> = pass1.iter().collect();
    let scatteredr = &scattered;
    let owned = pool.run(
        p,
        |_| Vec::<(usize, PartTable)>::new(),
        |out: &mut Vec<(usize, PartTable)>, part| {
            let n: usize = scatteredr.iter().map(|s| s[part].hash.len()).sum();
            let mut t = PartTable::default();
            if n > 0 {
                assert!(n < NO_ENTRY as usize, "build partition exceeds u32 entries");
                t.hash.reserve(n);
                t.kw.reserve(n * nkw);
                t.kf.reserve(n * nkf);
                t.pw.reserve(n * npw);
                t.pv.reserve(n * npw);
                t.pt.reserve(n * npt);
                for s in scatteredr.iter() {
                    let b = &s[part];
                    let base = t.arena.len() as u32;
                    t.hash.extend_from_slice(&b.hash);
                    t.nullk.extend_from_slice(&b.nullk);
                    t.kw.extend_from_slice(&b.kw);
                    t.kf.extend_from_slice(&b.kf);
                    t.pw.extend_from_slice(&b.pw);
                    t.pv.extend_from_slice(&b.pv);
                    t.pt.extend(
                        b.pt.iter()
                            .map(|&(o, l)| if (o, l) == T_NULL { T_NULL } else { (o + base, l) }),
                    );
                    t.arena.extend_from_slice(&b.arena);
                }
                let cap = (n * 2).next_power_of_two().max(16);
                t.mask = cap - 1;
                t.heads = vec![NO_ENTRY; cap];
                t.next = vec![NO_ENTRY; n];
                if let Some(FoldLane::Num { op, .. }) = fold_lane {
                    t.fs = vec![0; n];
                    t.fc = vec![0; n];
                    if op == NumCellOp::Avg {
                        t.fr = vec![0; n];
                    }
                }
                for e in 0..n {
                    // [sqe-semi-anti] RightAnti-retained null-key rows
                    // stay UNLINKED: no probe compare ever sees them,
                    // so they emit as never-matched entries.
                    if t.nullk[e] {
                        continue;
                    }
                    let slot = (t.hash[e] as usize) & t.mask;
                    // [corrsubq] grouped build: a key-equal entry already
                    // linked is the group's representative — fold this
                    // entry's cell into it and leave this entry unlinked
                    // (unreachable by any probe); else this entry starts
                    // the group with its own cell.
                    if let Some(fl) = fold_lane {
                        let mut g = t.heads[slot];
                        while g != NO_ENTRY {
                            let gi = g as usize;
                            if t.hash[gi] == t.hash[e]
                                && t.kw[gi * nkw..gi * nkw + nkw] == t.kw[e * nkw..e * nkw + nkw]
                                && t.kf[gi * nkf..gi * nkf + nkf] == t.kf[e * nkf..e * nkf + nkf]
                            {
                                break;
                            }
                            g = t.next[gi];
                        }
                        match fl {
                            FoldLane::Word(pj, op) => {
                                let ce = e * npw + pj;
                                if g == NO_ENTRY {
                                    match op {
                                        JoinAggOp::CountStar => {
                                            t.pw[ce] = 1;
                                            t.pv[ce] = true;
                                        }
                                        JoinAggOp::CountCol => {
                                            t.pw[ce] = t.pv[ce] as i64;
                                            t.pv[ce] = true;
                                        }
                                        JoinAggOp::Sum | JoinAggOp::Min | JoinAggOp::Max => {}
                                        JoinAggOp::CountDistinct => unreachable!(
                                            "stage folds admit count/sum/min/max only"
                                        ),
                                    }
                                } else {
                                    let cg = g as usize * npw + pj;
                                    let (v, ok) = (t.pw[ce], t.pv[ce]);
                                    match op {
                                        JoinAggOp::CountStar => t.pw[cg] += 1,
                                        JoinAggOp::CountCol => t.pw[cg] += ok as i64,
                                        JoinAggOp::CountDistinct => unreachable!(
                                            "stage folds admit count/sum/min/max only"
                                        ),
                                        JoinAggOp::Sum | JoinAggOp::Min | JoinAggOp::Max => {
                                            if ok {
                                                if !t.pv[cg] {
                                                    t.pw[cg] = v;
                                                    t.pv[cg] = true;
                                                } else {
                                                    t.pw[cg] = match op {
                                                        JoinAggOp::Sum => t.pw[cg] + v,
                                                        JoinAggOp::Min => t.pw[cg].min(v),
                                                        _ => t.pw[cg].max(v),
                                                    };
                                                }
                                            }
                                        }
                                    }
                                    continue;
                                }
                            }
                            FoldLane::Num { pj, op, .. } => {
                                let ce = e * npw + pj;
                                if g == NO_ENTRY {
                                    if t.pv[ce] {
                                        t.fs[e] = t.pw[ce] as i128;
                                        t.fc[e] = 1;
                                    }
                                } else {
                                    let gi = g as usize;
                                    if t.pv[ce] {
                                        let v = t.pw[ce] as i128;
                                        t.fs[gi] = match op {
                                            NumCellOp::Sum | NumCellOp::Avg => t.fs[gi] + v,
                                            NumCellOp::Min if t.fc[gi] > 0 => t.fs[gi].min(v),
                                            NumCellOp::Max if t.fc[gi] > 0 => t.fs[gi].max(v),
                                            NumCellOp::Min | NumCellOp::Max => v,
                                        };
                                        t.fc[gi] += 1;
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                    t.next[e] = t.heads[slot];
                    t.heads[slot] = e as u32;
                }
                if let Some(FoldLane::Num { op: NumCellOp::Avg, scale, .. }) = fold_lane {
                    for e in 0..n {
                        if t.fc[e] > 0 {
                            let (a, r) = crate::joins::numcell::pg_avg_cell(t.fs[e], t.fc[e], scale);
                            t.fs[e] = a;
                            t.fr[e] = r;
                        }
                    }
                }
            }
            out.push((part, t));
        },
    );
    drop(scattered);
    {
        let mut park = PARKB.lock().unwrap();
        for buckets in pass1 {
            park.push(buckets);
        }
    }
    let mut tables: Vec<PartTable> = (0..p).map(|_| PartTable::default()).collect();
    for w in owned {
        for (part, t) in w {
            tables[part] = t;
        }
    }
    let tablesr = &tables;

    // Keyless (nest-loop) product witness: every probe row sweeps every
    // build entry, so the pair product must be provably small. Both
    // factors are exact witnesses (the built tables, the bank row total).
    if node.keys.is_empty() {
        let bn: u64 = tables.iter().map(|t| t.hash.len() as u64).sum();
        let est = bn.saturating_mul(pbank.rows_total());
        if est > NL_PRODUCT_BUDGET_PAIRS {
            return Err(JoinRefuse::ProductExceedsBudget {
                est_pairs: est,
                budget: NL_PRODUCT_BUDGET_PAIRS,
            });
        }
    }

    let punits = if pbank.parts.is_empty() {
        Arc::new(Vec::new())
    } else {
        pctx.faces.walk(pbank, pwalk_col)
    };
    // [sqe-semi-anti] right classes emit BUILD entries after the probe
    // pass — an empty probe side still emits (RightAnti: every entry;
    // [rightouter] Right: every entry, probe side null-extended).
    let right = matches!(
        node.join_type,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::Right
    );
    if punits.is_empty() && !right {
        return Ok(Vec::new());
    }
    struct PS<K> {
        dec: SideDecode,
        surv: Vec<u16>,
        /// Column-major survivor planes (per ci): normalized words,
        /// validity, text-key fps; plus the per-survivor NULL-key flags.
        nw: Vec<Vec<i64>>,
        nv: Vec<Vec<bool>>,
        nfp: Vec<Vec<u128>>,
        snullk: Vec<bool>,
        skw: Vec<i64>,
        skf: Vec<u128>,
        sh: Vec<u64>,
        shead: Vec<u32>,
        /// [sqe-semi-anti] per-partition build-entry match bitmaps
        /// (right classes only; OR-merged across workers after the run).
        mrk: Vec<Vec<u64>>,
        /// [crossdim-or] probe-side arm masks over survivors.
        pom: Vec<u64>,
        sink: K,
    }
    let pplanr = &pplan;
    let pfpsr = &pfps;
    let out_mapr = &out_map;
    let nplanr = &nplan;
    let pncols = pplan.cols.len();
    let probe_res = pool.run_finish(
        punits.len(),
        |_| PS {
            dec: SideDecode::fetch(pplanr),
            surv: Vec::new(),
            nw: vec![Vec::new(); pncols],
            nv: vec![Vec::new(); pncols],
            nfp: vec![Vec::new(); pncols],
            snullk: Vec::new(),
            skw: vec![0; STAGE * nkw],
            skf: vec![0; STAGE * nkf],
            sh: vec![0; STAGE],
            shead: vec![NO_ENTRY; STAGE],
            mrk: if right {
                tablesr.iter().map(|t| vec![0u64; t.hash.len().div_ceil(64)]).collect()
            } else {
                Vec::new()
            },
            pom: Vec::new(),
            sink: mk(),
        },
        |s: &mut PS<K>, ui| {
            let PS { dec, surv, nw, nv, nfp, snullk, skw, skf, sh, shead, mrk, pom, sink } = s;
            let (pi, g, rows, _) = punits[ui];
            let rows = rows as usize;
            let (lanes, gvs) = dec.granule(pbank, pplanr, pfpsr, pi, g, rows);
            pred_select(&node.probe_pred, pplanr, dec, &lanes, &gvs, rows, surv);
            // [semianti-flt] host-side membership filters (WHERE grain).
            filter_hosts(&node.filters, filters_builtr, phfr, pplanr, dec, &lanes, &gvs, surv);
            // [sqe-tpch-mech]/[subset] the filter's own 3VL decides.
            if let Some((ci, f)) = pin_setr {
                let face = pplanr.cols[*ci].face;
                let d = words_of(&lanes[*ci]);
                let np = f.null_passes();
                let nfok = pplanr.cols[*ci].nf || gvs[*ci].all_valid();
                let scr = &dec.scr[*ci];
                surv.retain(|&r| {
                    let r = r as usize;
                    if nfok || scr.row_valid(r) {
                        f.word_passes(face.word_key(d[r]))
                    } else {
                        np
                    }
                });
            }
            // [crossdim-or] probe-scan arm masks: an all-clear row can
            // never satisfy any arm — drop it with its mask slot.
            pom.clear();
            if or_full != 0 && !or_prober.is_empty() {
                pom.resize(surv.len(), or_full);
                or_mask_pass(or_prober, 0, pplanr, dec, &lanes, &gvs, surv, pom);
                let mut w = 0usize;
                for i in 0..surv.len() {
                    if pom[i] != 0 {
                        surv[w] = surv[i];
                        pom[w] = pom[i];
                        w += 1;
                    }
                }
                surv.truncate(w);
                pom.truncate(w);
            }
            normalize_probe(
                pplanr, nplanr, dec, &lanes, &gvs, pfpsr, pi, surv, nw, nv, nfp, snullk,
            );
            let raw: Vec<&[u64]> = lanes
                .iter()
                .map(|l| match l {
                    Lane::Words(d) => *d,
                    Lane::Codes(_) => &[][..],
                })
                .collect();
            sink.unit_start(ui);
            let cx = RowCx {
                surv: &surv[..],
                nw: &nw[..],
                nv: &nv[..],
                raw: &raw,
                tables: tablesr,
                out_map: out_mapr,
                npw,
                npt,
            };
            let mut base = 0usize;
            while base < surv.len() {
                let take = (surv.len() - base).min(STAGE);
                // sweep 1: key gather from the flat normalized planes +
                // hash + partition + head load
                for si in 0..take {
                    let sg = base + si;
                    if snullk[sg] {
                        shead[si] = NO_ENTRY;
                        continue;
                    }
                    for (iw, &ci) in kw_cis_p.iter().enumerate() {
                        skw[si * nkw + iw] = nw[ci][sg];
                    }
                    for (jf, &ci) in kf_cis_p.iter().enumerate() {
                        skf[si * nkf + jf] = nfp[ci][sg];
                    }
                    let h = key_hash(
                        &skw[si * nkw..si * nkw + nkw],
                        &skf[si * nkf..si * nkf + nkf],
                    );
                    sh[si] = h;
                    let t = &tablesr[(h >> (64 - pbits)) as usize];
                    shead[si] = if t.heads.is_empty() {
                        NO_ENTRY
                    } else {
                        t.heads[(h as usize) & t.mask]
                    };
                }
                // sweep 2: chain walk + quals + consume
                for si in 0..take {
                    let sg = base + si;
                    let mut matched = false;
                    let mut keyhit = false;
                    if !snullk[sg] {
                        let h = sh[si];
                        let part = (h >> (64 - pbits)) as usize;
                        let t = &tablesr[part];
                        let mut e = shead[si];
                        'chain: while e != NO_ENTRY {
                            let ei = e as usize;
                            if t.hash[ei] == h
                                && t.kw[ei * nkw..ei * nkw + nkw]
                                    == skw[si * nkw..si * nkw + nkw]
                                && t.kf[ei * nkf..ei * nkf + nkf]
                                    == skf[si * nkf..si * nkf + nkf]
                            {
                                keyhit = true;
                                let mut pass = true;
                                for (qi, q) in node.quals.iter().enumerate() {
                                    let pci = pplanr.qual_ci[qi];
                                    let p_ok = nv[pci][sg];
                                    let pv = nw[pci][sg];
                                    let bi = ei * npw + qual_pay[qi];
                                    if !q.eval_v(pv, p_ok, t.pw[bi], t.pv[bi]) {
                                        pass = false;
                                        break;
                                    }
                                }
                                // [crossdim-or] some arm alive across
                                // both ends of the joined row.
                                if pass && or_lane == 1 {
                                    let m = t.pw[ei * npw + or_pj] as u64;
                                    pass = (pom[sg] & m) != 0;
                                }
                                if pass {
                                    if let (Some(f), Some(pci)) =
                                        (node.num_fold.as_ref(), nf_pci)
                                    {
                                        let cs = if f.op == NumCellOp::Avg {
                                            t.fr[ei]
                                        } else {
                                            f.scale
                                        };
                                        pass = nv[pci][sg]
                                            && t.fc[ei] > 0
                                            && crate::joins::numcell::num_cell_pass(
                                                f.qual.op,
                                                nw[pci][sg],
                                                f.qual.probe_scale,
                                                f.qual.k_m,
                                                f.qual.k_scale,
                                                t.fs[ei],
                                                cs,
                                            );
                                    }
                                }
                                if pass {
                                    matched = true;
                                    match node.join_type {
                                        JoinType::Inner | JoinType::Left => {
                                            sink.row(&cx, sg, Some((part, ei)))
                                        }
                                        JoinType::Right => {
                                            sink.row(&cx, sg, Some((part, ei)));
                                            mrk[part][ei >> 6] |= 1u64 << (ei & 63);
                                        }
                                        JoinType::Semi => {
                                            sink.row(&cx, sg, None);
                                            break 'chain;
                                        }
                                        JoinType::Anti => break 'chain,
                                        // [sqe-semi-anti] right classes:
                                        // mark the BUILD entry; the walk
                                        // continues (duplicate build keys
                                        // each mark), probe emits nothing.
                                        JoinType::RightSemi | JoinType::RightAnti => {
                                            mrk[part][ei >> 6] |= 1u64 << (ei & 63);
                                        }
                                    }
                                }
                            }
                            e = t.next[ei];
                        }
                    }
                    // [corrsubq] a probe key with NO group (incl. a NULL
                    // key) reads the fold's missing value through every
                    // qual; a Semi then emits (a Left emits below, NULL).
                    if !keyhit && node.join_type == JoinType::Semi {
                        if let Some(d) = fold_missing {
                            let pass = node.quals.iter().enumerate().all(|(qi, q)| {
                                let pci = pplanr.qual_ci[qi];
                                q.eval_v(nw[pci][sg], nv[pci][sg], d, true)
                            });
                            if pass {
                                matched = true;
                                sink.row(&cx, sg, None);
                            }
                        }
                    }
                    if !matched
                        && matches!(node.join_type, JoinType::Left | JoinType::Anti)
                    {
                        sink.row(&cx, sg, None);
                    }
                }
                base += take;
            }
            sink.unit_end(&cx);
        },
        // Worker-side finish: probe decode arenas park on THIS worker's
        // depot; only the result-bearing sink (plus the right-class
        // match bitmaps) crosses back.
        |s: PS<K>| {
            s.dec.park();
            (s.sink, s.mrk)
        },
    );
    let (mut sinks, mrks): (Vec<K>, Vec<Vec<Vec<u64>>>) = probe_res.into_iter().unzip();
    if right {
        // [sqe-semi-anti] the build-emit sweep: OR-merge the per-worker
        // match bitmaps, then emit each matched (RightSemi) / unmatched
        // (RightAnti) build entry ONCE, in (partition, entry) order, as
        // one extra unit AFTER every probe unit (`ui = punits.len()`).
        // Entry order within a partition follows the pass-1 worker
        // merge; a row goal needing an order carries a Sort above.
        let mut merged: Vec<Vec<u64>> =
            tables.iter().map(|t| vec![0u64; t.hash.len().div_ceil(64)]).collect();
        for m in &mrks {
            for (part, words) in m.iter().enumerate() {
                for (j, &w) in words.iter().enumerate() {
                    merged[part][j] |= w;
                }
            }
        }
        // [rightouter] sweep rows read the probe side as ONE null row
        // (si = 0), so every sink's ordinary null law applies.
        let outer = node.join_type == JoinType::Right;
        let null_surv: Vec<u16> = if outer { vec![0] } else { Vec::new() };
        let null_nw: Vec<Vec<i64>> = if outer { vec![vec![0]; pncols] } else { Vec::new() };
        let null_nv: Vec<Vec<bool>> = if outer { vec![vec![false]; pncols] } else { Vec::new() };
        let null_raw: Vec<&[u64]> = if outer { vec![&[]; pncols] } else { Vec::new() };
        let cx = RowCx {
            surv: &null_surv,
            nw: &null_nw,
            nv: &null_nv,
            raw: &null_raw,
            tables: tablesr,
            out_map: out_mapr,
            npw,
            npt,
        };
        let want = node.join_type == JoinType::RightSemi;
        let mut sink = mk();
        sink.unit_start(punits.len());
        for (part, t) in tables.iter().enumerate() {
            for e in 0..t.hash.len() {
                let m = (merged[part][e >> 6] >> (e & 63)) & 1 == 1;
                if m == want {
                    sink.row(&cx, 0, Some((part, e)));
                }
            }
        }
        sink.unit_end(&cx);
        sinks.push(sink);
    }
    Ok(sinks)
}

/// Row goal: joined rows as an `AnswerSet`, per-unit buffers concatenated
/// in unit order (deterministic across pool widths).
pub fn run_hash_join(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    node: &JoinNode,
) -> Result<AnswerSet, JoinRefuse> {
    run_hash_join_flt(bctx, pctx, dctxs, &[], node)
}

/// [semianti-flt] Row goal with membership-stage contexts (`fctxs`
/// aligns with `node.filters`; the filter-less entry passes none).
pub fn run_hash_join_flt(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    node: &JoinNode,
) -> Result<AnswerSet, JoinRefuse> {
    let sinks = run_core(bctx, pctx, dctxs, fctxs, node, || RowEmit {
        node,
        units: Vec::new(),
        pend: Vec::new(),
    })?;
    if sinks.is_empty() {
        return Ok(AnswerSet::empty(node.out_tys.clone()));
    }
    let mut per_unit: Vec<(usize, Vec<OutB>)> =
        sinks.into_iter().flat_map(|s| s.units).collect();
    per_unit.sort_by_key(|(ui, _)| *ui);
    let mut cols: Vec<AnswerCol> = Vec::with_capacity(node.out.len());
    for (oi, ty) in node.out_tys.iter().enumerate() {
        if node.out_bytes[oi] {
            let mut bb = BytesBuild::new();
            let mut mask: Vec<bool> = Vec::new();
            for (_, outs) in per_unit.iter() {
                if let OutB::T(b, m) = &outs[oi] {
                    for i in 0..b.len() {
                        bb.push(&b.arena[b.offs[i] as usize..b.offs[i + 1] as usize]);
                    }
                    mask.extend_from_slice(m);
                }
            }
            let mut c = bb.finish(*ty);
            if !mask.iter().all(|&x| x) {
                c.validity = Validity::Mask(mask);
            }
            cols.push(c);
        } else {
            let mut v: Vec<i64> = Vec::new();
            let mut mask: Vec<bool> = Vec::new();
            for (_, outs) in per_unit.iter() {
                if let OutB::W(w, m) = &outs[oi] {
                    v.extend_from_slice(w);
                    mask.extend_from_slice(m);
                }
            }
            let validity = if mask.iter().all(|&x| x) {
                Validity::AllValid
            } else {
                Validity::Mask(mask)
            };
            cols.push(AnswerCol { ty: *ty, data: ColData::I64(v), validity });
        }
    }
    Ok(AnswerSet::from_cols(cols))
}

/// Agg goal: the probe loop folds into accumulator cells (produce/consume
/// law — no joined-row materialization); cells merge across workers by
/// the one combine law, so the answer is pool-width independent.
pub fn run_hash_join_agg(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<AnswerSet, JoinRefuse> {
    run_hash_join_agg_flt(bctx, pctx, dctxs, &[], anode)
}

/// [semianti-flt] Agg goal with membership-stage contexts.
pub fn run_hash_join_agg_flt(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<AnswerSet, JoinRefuse> {
    // [sqe-mech3] fused-arithmetic admission (no-overflow witness law).
    check_join_agg(bctx, pctx, dctxs, anode)?;
    let has_distinct = anode.aggs.iter().any(|a| a.op == JoinAggOp::CountDistinct);
    if !anode.groups.is_empty() {
        // [sqe-mech3] the composite sink serves multi-key groups, text
        // keys, and non-probe key sides; the single probe-side word key
        // keeps the proven Cnt64/Cells64/direct-array arms.
        let single_word_probe = anode.groups.len() == 1
            && anode.groups[0].side == JoinSide::Probe
            && !pctx.bank.typ(anode.groups[0].col).is_varlena()
            && anode.group_xf.iter().all(|x| x.is_none());
        // Distinct legs carry per-group set planes only the composite
        // sink owns — any mix with one routes there.
        if !single_word_probe || has_distinct {
            return run_grouped_multi(bctx, pctx, dctxs, fctxs, anode);
        }
        // Pure grouped count(*) keeps the Cnt64 count table; any other
        // agg mix — and any fused HAVING — rides the grouped-fold sinks
        // (sqe-grpfold; [sqe-tpch-mech] the fold path owns the filter).
        if anode.aggs.len() == 1
            && anode.aggs[0].op == crate::joins::ir::JoinAggOp::CountStar
            && anode.having.is_none()
        {
            return run_grouped_count(bctx, pctx, dctxs, fctxs, anode);
        }
        return run_grouped_fold(bctx, pctx, dctxs, fctxs, anode);
    }
    if anode.having.is_some() {
        return Err(JoinRefuse::Unsupported { what: "having-ungrouped" });
    }
    let n = anode.aggs.len();
    let mut sinks = run_core(bctx, pctx, dctxs, fctxs, &anode.join, || AggFold::new(anode))?;
    for s in &mut sinks {
        s.flush(); // staged residue -> the cells
    }
    // combine consumes the same hoisted per-agg op list as the sinks.
    let srcs = fold_srcs(anode);
    let mut rows = 0u64;
    let mut cells = vec![AccumCell::default(); n];
    for s in sinks {
        rows += s.rows;
        for (ai, src) in srcs.iter().enumerate() {
            if let Some(FoldSrc { op, .. }) = *src {
                combine_cell_fold(op, &mut cells[ai], &s.cells[ai]);
            }
        }
    }
    let cols: Vec<AnswerCol> = anode
        .aggs
        .iter()
        .zip(&cells)
        .map(|(a, c)| {
            use crate::joins::ir::JoinAggOp;
            match a.op {
                JoinAggOp::CountStar => AnswerCol::i64s(a.out, vec![rows as i64]),
                JoinAggOp::CountCol => AnswerCol::i64s(a.out, vec![c.b]),
                JoinAggOp::CountDistinct => {
                    unreachable!("distinct is grouped-only by admission")
                }
                JoinAggOp::Sum => {
                    let mut col = AnswerCol::i128s(a.out, vec![c.a]);
                    if c.b == 0 {
                        col.validity = Validity::Mask(vec![false]);
                    }
                    col
                }
                JoinAggOp::Min | JoinAggOp::Max => {
                    AnswerCol::i64s_opt(a.out, vec![minmax_answer(c)])
                }
            }
        })
        .collect();
    Ok(AnswerSet::from_cols(cols))
}

/// Grouped count(*): (key, count) rows, key-ascending with the NULL group
/// last (deterministic across pool widths by the sort, not the tables).
fn run_grouped_count(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<AnswerSet, JoinRefuse> {
    let goi = anode.group_oi[0];
    let sinks = run_core(bctx, pctx, dctxs, fctxs, &anode.join, || GroupCount {
        group_oi: goi,
        map: Cnt64::new(1024),
        nulls: 0,
    })?;
    let mut nulls = 0u64;
    let mut pairs: Vec<(i64, u64)> = Vec::new();
    let mut tmp: Vec<(u64, u64)> = Vec::new();
    for s in sinks {
        nulls += s.nulls;
        tmp.clear();
        s.map.drain_into(&mut tmp);
        pairs.extend(tmp.iter().map(|&(k, c)| (k as i64, c)));
    }
    pairs.sort_unstable_by_key(|&(k, _)| k);
    let mut keys: Vec<i64> = Vec::new();
    let mut cnts: Vec<i64> = Vec::new();
    for (k, c) in pairs {
        if keys.last() == Some(&k) {
            *cnts.last_mut().unwrap() += c as i64;
        } else {
            keys.push(k);
            cnts.push(c as i64);
        }
    }
    let mut mask = vec![true; keys.len()];
    if nulls > 0 {
        keys.push(0);
        cnts.push(nulls as i64);
        mask.push(false);
    }
    let kvalid = if mask.iter().all(|&x| x) {
        Validity::AllValid
    } else {
        Validity::Mask(mask)
    };
    Ok(AnswerSet::from_cols(vec![
        AnswerCol { ty: anode.join.out_tys[goi], data: ColData::I64(keys), validity: kvalid },
        AnswerCol::i64s(anode.aggs[0].out, cnts),
    ]))
}

/// Grouped folds (sqe-grpfold): (key, agg...) rows, key-ascending with
/// the NULL group last. Per-worker Cells64 tables merge under the one
/// combine law, so the answer is pool-width independent. 3VL: NULL fold
/// inputs fold nothing; count(col) skips them; a group with only NULL
/// fold inputs answers SUM/MIN/MAX NULL (cell.b == 0 / valid == 0); an
/// empty group cannot exist by construction (hash grouping).
fn run_grouped_fold(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<AnswerSet, JoinRefuse> {
    // [sqe-tpch-mech] direct-array grouped state (mechanism 1, join-agg
    // GroupFold sink): witnessed dense-and-bounded probe-side key domain
    // elects the shared atomic array; the post-run overflow audit falls
    // back to the hash arm below (answers identical either arm).
    if let Some((lo, dn)) = dense_group_domain(bctx, pctx, dctxs, anode) {
        if let Some(a) = run_grouped_fold_dense(bctx, pctx, dctxs, fctxs, anode, lo, dn)? {
            return Ok(a);
        }
    }
    let na = anode.aggs.len();
    let mut sinks = run_core(bctx, pctx, dctxs, fctxs, &anode.join, || GroupFold::new(anode))?;
    for s in &mut sinks {
        s.flush(); // staged residue -> the tables
    }
    // combine consumes the same hoisted per-agg op list as the sinks.
    let srcs = fold_srcs(anode);
    let combine = |into: &mut [AccumCell], from: &[AccumCell]| {
        for (ai, src) in srcs.iter().enumerate() {
            if let Some(FoldSrc { op, .. }) = *src {
                combine_cell_fold(op, &mut into[ai], &from[ai]);
            }
        }
    };
    // flat cell arena (na cells per group): million-group answers carry
    // no per-group heap blocks; merge combines in place by index.
    let mut groups: Vec<(i64, u64, u32)> = Vec::new();
    let mut arena: Vec<AccumCell> = Vec::new();
    let mut null_rows = 0u64;
    let mut null_cells = vec![AccumCell::default(); na];
    for s in &sinks {
        null_rows += s.null_rows;
        combine(&mut null_cells, &s.null_cells);
        s.map.for_each(|k, rows, cells| {
            groups.push((k as i64, rows, (arena.len() / na.max(1)) as u32));
            arena.extend_from_slice(cells);
        });
    }
    groups.sort_unstable_by_key(|g| g.0);
    let mut merged: Vec<(i64, u64, u32)> = Vec::new();
    for g in groups {
        match merged.last_mut() {
            Some(m) if m.0 == g.0 => {
                m.1 += g.1;
                let (mb, gb) = (m.2 as usize * na, g.2 as usize * na);
                for (ai, src) in srcs.iter().enumerate() {
                    if let Some(FoldSrc { op, .. }) = *src {
                        let from = arena[gb + ai];
                        combine_cell_fold(op, &mut arena[mb + ai], &from);
                    }
                }
            }
            _ => merged.push(g),
        }
    }
    Ok(grouped_fold_answer(anode, merged, arena, null_rows, null_cells))
}

/// [sqe-tpch-mech] Fused-HAVING group filter for the join grouped fold:
/// the aggregate's ANSWER value per group from the fold cells (NULL
/// aggregate never passes — HavingCmp::keep's 3VL law). The NULL-key
/// group filters by its own aggregate value like any other group.
fn jhaving_keep(
    anode: &JoinAggNode,
    h: &crate::ir::HavingCmp,
    rows: u64,
    cells: &[AccumCell],
) -> bool {
    use crate::joins::ir::JoinAggOp;
    let ai = h.agg as usize;
    let v: Option<i128> = match anode.aggs[ai].op {
        JoinAggOp::CountStar => Some(rows as i128),
        JoinAggOp::CountCol => Some(cells[ai].b as i128),
        // The shell refuses HAVING onto a distinct leg; never passes.
        JoinAggOp::CountDistinct => None,
        JoinAggOp::Sum => (cells[ai].b > 0).then(|| cells[ai].a),
        JoinAggOp::Min | JoinAggOp::Max => minmax_answer(&cells[ai]).map(|x| x as i128),
    };
    h.keep(v)
}

/// Shared answer tail of the grouped-fold arms (hash Cells64 AND the
/// direct-array sink): NULL group appended last, fused-HAVING filter,
/// then the typed column renders — pool-width independent either arm.
/// `arena` holds `na` cells per group at `merged[i].2 * na`.
fn grouped_fold_answer(
    anode: &JoinAggNode,
    mut merged: Vec<(i64, u64, u32)>,
    mut arena: Vec<AccumCell>,
    null_rows: u64,
    null_cells: Vec<AccumCell>,
) -> AnswerSet {
    use crate::joins::ir::JoinAggOp;
    let na = anode.aggs.len();
    let goi = anode.group_oi[0];
    let mut kmask = vec![true; merged.len()];
    if null_rows > 0 {
        merged.push((0, null_rows, (arena.len() / na.max(1)) as u32));
        arena.extend_from_slice(&null_cells);
        kmask.push(false);
    }
    if let Some(h) = &anode.having {
        let keep: Vec<bool> = merged
            .iter()
            .map(|g| jhaving_keep(anode, h, g.1, &arena[g.2 as usize * na..(g.2 as usize + 1) * na]))
            .collect();
        let mut it = keep.iter();
        merged.retain(|_| *it.next().unwrap());
        let mut it = keep.iter();
        kmask.retain(|_| *it.next().unwrap());
    }
    let kvalid = if kmask.iter().all(|&x| x) {
        Validity::AllValid
    } else {
        Validity::Mask(kmask)
    };
    let cell = |g: &(i64, u64, u32), ai: usize| -> AccumCell { arena[g.2 as usize * na + ai] };
    let keys: Vec<i64> = merged.iter().map(|g| g.0).collect();
    let mut cols = Vec::with_capacity(1 + na);
    cols.push(AnswerCol { ty: anode.join.out_tys[goi], data: ColData::I64(keys), validity: kvalid });
    for (ai, a) in anode.aggs.iter().enumerate() {
        cols.push(match a.op {
            JoinAggOp::CountStar => {
                AnswerCol::i64s(a.out, merged.iter().map(|g| g.1 as i64).collect())
            }
            JoinAggOp::CountCol => {
                AnswerCol::i64s(a.out, merged.iter().map(|g| cell(g, ai).b).collect())
            }
            JoinAggOp::CountDistinct => {
                unreachable!("distinct rides the composite sink")
            }
            JoinAggOp::Sum => {
                let mask: Vec<bool> = merged.iter().map(|g| cell(g, ai).b > 0).collect();
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol {
                    ty: a.out,
                    data: ColData::I128(merged.iter().map(|g| cell(g, ai).a).collect()),
                    validity,
                }
            }
            JoinAggOp::Min | JoinAggOp::Max => {
                AnswerCol::i64s_opt(
                    a.out,
                    merged.iter().map(|g| minmax_answer(&cell(g, ai))).collect(),
                )
            }
        });
    }
    AnswerSet::from_cols(cols)
}

/// [sqe-tpch-mech] Direct-array election for the join grouped fold: the
/// probe-side group key's domain must be witnessed dense-and-bounded
/// (planner::direct_array_domain — exact part-record min/max, budget as
/// a function of the fold-lane count), and every Sum input must carry an
/// exact input-domain witness (the post-run overflow audit's |x| bound).
/// NULL group keys need no witness: they fold aside in the sinks.
fn dense_group_domain(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Option<(i64, usize)> {
    use crate::joins::ir::JoinAggOp;
    if !pctx.faces.cfg.direct_array {
        return None;
    }
    if bctx.bank.parts.is_empty() || pctx.bank.parts.is_empty() {
        return None;
    }
    // Side-pred election law (measured, Q3-core vs Q18-core): a filtered
    // side collapses the surviving group census far below the DOMAIN
    // witness, and the cache-resident hash tables win there (the dense
    // array pays domain-sized lanes + gather for a survivor set the
    // filter already shrank). Unfiltered joins keep the dense arm.
    if anode.join.probe_pred.is_some()
        || anode.join.build_pred.is_some()
        || anode.join.staged_or.is_some()
        || !anode.join.filters.is_empty()
    {
        return None;
    }
    let g = *anode.groups.first()?;
    let nfold = anode.aggs.iter().filter(|a| a.op.fold_op().is_some()).count();
    // [sqe-hugedom] answer-bound budget law (the single-table election's
    // twin): a fused HAVING bounds the ANSWER to the survivors, so the
    // accumulator prices under the occupancy-budget — admitted only with
    // all-zero-init lanes (Sum/Count), where alloc_zeroed keeps resident
    // bytes at occupancy. Min/Max sentinel fills and unbounded answers
    // keep the resident-sized budget.
    let zero_init = anode.aggs.iter().all(|a| {
        matches!(
            a.op.fold_op(),
            None | Some(crate::fold::AggFoldOp::Sum) | Some(crate::fold::AggFoldOp::CountCol)
        )
    });
    let bounded_answer = anode.having.is_some();
    let cap = crate::planner::direct_array_bytes_cap(zero_init && bounded_answer);
    let dom = crate::planner::direct_array_domain_capped(pctx.bank, pctx.faces, g.col, nfold, cap)?;
    for a in &anode.aggs {
        // [caseleg] the dense count lane reads validity directly: keep
        // predicated legs on the staged hash arm.
        if a.case.is_some() {
            return None;
        }
        if a.op == JoinAggOp::Sum {
            // Arith fold inputs carry no single-column |x| bound for the
            // atomic-lane audit: keep the exact-i128 hash arm.
            if a.arith.is_some() {
                return None;
            }
            let io = a.input.expect("sum carries an input");
            let cx = side_ctx(bctx, pctx, dctxs, io.side);
            cx.faces.stats(cx.bank, io.col).minmax_exact()?;
        }
    }
    Some(dom)
}

/// The shared atomic grouped-fold state (join direct-array arm): a u64
/// count lane plus per-fold-agg (value, non-null count) atomic lanes,
/// indexed `key - lo`. Sum lanes fold with wrapping fetch_add — SOUND
/// only because the post-run audit re-derives an overflow bound from the
/// EXACT per-group non-null counts and the witnessed |input| bound, and
/// discards the whole run for the hash arm when any group could have
/// wrapped (detection never depends on the possibly-wrapped values).
struct DenseTab {
    lo: i64,
    dn: usize,
    counts: Vec<std::sync::atomic::AtomicU64>,
    /// Per agg index: None for CountStar (rides `counts`).
    lanes: Vec<Option<(Vec<std::sync::atomic::AtomicI64>, Vec<std::sync::atomic::AtomicU64>)>>,
}

struct GroupFoldDense<'a> {
    node: &'a JoinAggNode,
    tab: Arc<DenseTab>,
    srcs: Vec<Option<FoldSrc<'a>>>,
    null_rows: u64,
    null_cells: Vec<AccumCell>,
}

impl Sink for GroupFoldDense<'_> {
    fn unit_start(&mut self, _ui: usize) {}

    fn row(&mut self, cx: &RowCx<'_, '_>, r: usize, entry: Option<(usize, usize)>) {
        use std::sync::atomic::Ordering::Relaxed;
        let (k, ok) = cx.out_word(self.node.group_oi[0], r, entry);
        if !ok {
            // NULL keys are one SQL group folded aside (exact cells —
            // the fold.rs law, no atomics, no audit).
            self.null_rows += 1;
            for (ai, src) in self.srcs.iter().enumerate() {
                let Some(s) = src else { continue };
                let (w, okv) = stage_val(cx, r, entry, s);
                scatter_cell_fold(s.op, &mut self.null_cells[ai], w, okv);
            }
            return;
        }
        let idx = k.wrapping_sub(self.tab.lo) as usize;
        assert!(idx < self.tab.dn, "dense group fold: key outside the witnessed domain");
        self.tab.counts[idx].fetch_add(1, Relaxed);
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(s) = src else { continue };
            let (vals, bs) = self.tab.lanes[ai].as_ref().expect("fold agg lane");
            if s.is_count {
                if cx.out_valid(s.oi, r, entry) {
                    bs[idx].fetch_add(1, Relaxed);
                }
                continue;
            }
            let (w, okv) = stage_val(cx, r, entry, s);
            if !okv {
                continue;
            }
            match s.op {
                AggFoldOp::Sum => {
                    vals[idx].fetch_add(w, Relaxed);
                    bs[idx].fetch_add(1, Relaxed);
                }
                AggFoldOp::Min => {
                    vals[idx].fetch_min(w, Relaxed);
                    bs[idx].fetch_add(1, Relaxed);
                }
                AggFoldOp::Max => {
                    vals[idx].fetch_max(w, Relaxed);
                    bs[idx].fetch_add(1, Relaxed);
                }
                _ => unreachable!("join recognizer admits count/sum/min/max only"),
            }
        }
    }
}

/// Direct-array grouped fold over the join probe loop. Ok(None) = the
/// overflow audit failed — the caller reruns the hash arm.
fn run_grouped_fold_dense(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    anode: &JoinAggNode,
    lo: i64,
    dn: usize,
) -> Result<Option<AnswerSet>, JoinRefuse> {
    use crate::joins::ir::JoinAggOp;
    use std::sync::atomic::Ordering::Relaxed;
    use crate::grouped::{atomic_fill_i64, atomic_zeros_u64};
    let na = anode.aggs.len();
    let srcs = fold_srcs(anode);
    let tab = Arc::new(DenseTab {
        lo,
        dn,
        counts: atomic_zeros_u64(dn),
        lanes: srcs
            .iter()
            .map(|src| {
                src.map(|s| {
                    let init = match s.op {
                        AggFoldOp::Min => i64::MAX,
                        AggFoldOp::Max => i64::MIN,
                        _ => 0,
                    };
                    (atomic_fill_i64(dn, init), atomic_zeros_u64(dn))
                })
            })
            .collect(),
    });
    let mk_tab = Arc::clone(&tab);
    let sinks = run_core(bctx, pctx, dctxs, fctxs, &anode.join, || GroupFoldDense {
        node: anode,
        tab: Arc::clone(&mk_tab),
        srcs: fold_srcs(anode),
        null_rows: 0,
        null_cells: vec![AccumCell::default(); na],
    })?;
    let mut null_rows = 0u64;
    let mut null_cells = vec![AccumCell::default(); na];
    for s in &sinks {
        null_rows += s.null_rows;
        for (ai, src) in srcs.iter().enumerate() {
            if let Some(FoldSrc { op, .. }) = *src {
                combine_cell_fold(op, &mut null_cells[ai], &s.null_cells[ai]);
            }
        }
    }
    // Overflow-audit bounds (Sum lanes): the EXACT per-group non-null
    // count times the witnessed |input| bound must stay inside i64 —
    // else the atomic lane could have wrapped and the whole run is
    // discarded for the hash arm. [sqe-hugedom] the audit rides the
    // domain-partitioned gather sweep below (per-chunk max non-null
    // count, one extra load already paid) instead of a serial O(dn)
    // pre-pass — at multi-million-key domains the serial pass was the
    // sweep's own cost again. A discarded run pays one wasted sweep
    // (rare by construction: the election pre-bounds rows_total).
    let mut audit: Vec<Option<u64>> = vec![None; na]; // per-agg max count bound
    for (ai, a) in anode.aggs.iter().enumerate() {
        if a.op != JoinAggOp::Sum {
            continue;
        }
        let io = a.input.expect("sum carries an input");
        let cx = side_ctx(bctx, pctx, dctxs, io.side);
        let (l, h) = cx
            .faces
            .stats(cx.bank, io.col)
            .minmax_exact()
            .expect("dense election witnessed sum inputs");
        let max_abs = (l.unsigned_abs().max(h.unsigned_abs()) as u128).max(1);
        // b_limit: the largest per-group non-null count that PROVABLY
        // cannot wrap: b * max_abs < i64::MAX.
        audit[ai] = Some(((i64::MAX as u128 - 1) / max_abs).min(u64::MAX as u128) as u64);
    }
    let auditr = &audit;
    // Gather (pool-parallel over contiguous slot ranges; chunk order
    // restored = key ASC, the hash arm's order). A fused HAVING filters
    // HERE — non-surviving groups never materialize.
    let pool = bctx.pool;
    let nch = (pool.threads().max(1) * 8).min(dn.max(1));
    let chunk = dn.div_ceil(nch);
    let (srcsr, tabr, anoder) = (&srcs, &tab, &anode);
    type JPart = Vec<(usize, Vec<(i64, u64, u32)>, Vec<AccumCell>, bool)>;
    let parts: Vec<JPart> = pool.run(
        nch,
        |_| JPart::new(),
        |acc: &mut JPart, ci| {
            let (s0, s1) = (ci * chunk, ((ci + 1) * chunk).min(dn));
            let mut rows: Vec<(i64, u64, u32)> = Vec::new();
            let mut arena: Vec<AccumCell> = Vec::new();
            let mut cells = vec![AccumCell::default(); na];
            let mut wrapped = false;
            for i in s0..s1 {
                let c = tabr.counts[i].load(Relaxed);
                if c == 0 {
                    continue;
                }
                for (ai, lim) in auditr.iter().enumerate() {
                    if let Some(lim) = lim {
                        let (_, bs) = tabr.lanes[ai].as_ref().expect("sum lane");
                        if bs[i].load(Relaxed) > *lim {
                            wrapped = true;
                        }
                    }
                }
                for (ai, src) in srcsr.iter().enumerate() {
                    let Some(FoldSrc { op, .. }) = *src else { continue };
                    let (vals, bs) = tabr.lanes[ai].as_ref().expect("fold agg lane");
                    let b = bs[i].load(Relaxed) as i64;
                    let v = vals[i].load(Relaxed);
                    cells[ai] = match op {
                        AggFoldOp::CountCol => AccumCell { a: 0, a2: 0, b, valid: 0 },
                        AggFoldOp::Sum => AccumCell { a: v as i128, a2: 0, b, valid: 0 },
                        AggFoldOp::Min | AggFoldOp::Max => AccumCell {
                            a: if b > 0 { v as i128 } else { 0 },
                            a2: 0,
                            b,
                            valid: (b > 0) as u8,
                        },
                        _ => unreachable!("join recognizer admits count/sum/min/max only"),
                    };
                }
                if let Some(h) = &anoder.having {
                    if !jhaving_keep(anoder, h, c, &cells) {
                        continue;
                    }
                }
                rows.push((lo.wrapping_add(i as i64), c, rows.len() as u32));
                arena.extend_from_slice(&cells);
            }
            acc.push((ci, rows, arena, wrapped));
        },
    );
    let mut flat: Vec<(usize, Vec<(i64, u64, u32)>, Vec<AccumCell>, bool)> =
        parts.into_iter().flatten().collect();
    // Overflow audit verdict: any group whose non-null count exceeds the
    // provable no-wrap bound discards the whole run for the hash arm
    // (detection never depends on the possibly-wrapped values).
    if flat.iter().any(|p| p.3) {
        return Ok(None);
    }
    flat.sort_unstable_by_key(|p| p.0);
    let mut merged: Vec<(i64, u64, u32)> = Vec::new();
    let mut arena: Vec<AccumCell> = Vec::new();
    for (_, rows, ar, _) in flat {
        let base = (arena.len() / na.max(1)) as u32;
        merged.extend(rows.into_iter().map(|(k, c, at)| (k, c, at + base)));
        arena.extend(ar);
    }
    Ok(Some(grouped_fold_answer(anode, merged, arena, null_rows, null_cells)))
}

// ---------------------------------------------------------------------------
// [sqe-mech3] fused-arithmetic admission + the composite grouped sink
// ---------------------------------------------------------------------------

/// The bank/faces pair a `JoinOut` side resolves against at run.
fn side_ctx<'a>(
    bctx: &'a SqeCtx<'a>,
    pctx: &'a SqeCtx<'a>,
    dctxs: &[&'a SqeCtx<'a>],
    side: JoinSide,
) -> &'a SqeCtx<'a> {
    match side {
        JoinSide::Build => bctx,
        JoinSide::Probe => pctx,
        JoinSide::Dim(i) => dctxs[i as usize],
    }
}

/// The i64 interval every fold-input row of `io` provably lies in: the
/// exact stats witness when the membrane grants it, else the column
/// FACE's type range (an honest superset of any filtered stream).
fn arith_domain(cx: &SqeCtx, io: JoinOut) -> (i128, i128) {
    let (bank, faces, col) = (cx.bank, cx.faces, io.col);
    if let Some(w) = crate::witness::Witness::exact_domain(&faces.stats(bank, col)) {
        let (lo, hi) = w.value();
        return (lo as i128, hi as i128);
    }
    use crate::bank::Face;
    match bank.face(col) {
        Face::SignedWord(2) => (i16::MIN as i128, i16::MAX as i128),
        Face::SignedWord(4) => (i32::MIN as i128, i32::MAX as i128),
        Face::UnsignedWord(w) if w <= 4 => (0, (1i128 << (8 * w as u32)) - 1),
        Face::Bool => (0, 1),
        _ => (i64::MIN as i128, i64::MAX as i128),
    }
}

/// [scale-alg] Mantissa interval of packed `io`: face at the authored
/// scale AND an exact stats domain (numeric has no type-range fallback)
/// — else the typed refusal, never a wrapped fold.
/// [tpch-expr] Mixed lane: authored scale 0 over an int column rides
/// the int word domain; numeric lanes keep the packed law below.
fn packed_or_word_domain(
    cx: &SqeCtx,
    io: JoinOut,
    authored: i32,
) -> Result<(i128, i128), JoinRefuse> {
    if authored == 0
        && matches!(
            cx.bank.typ(io.col).oid,
            crate::typmeta::oids::INT2 | crate::typmeta::oids::INT4 | crate::typmeta::oids::INT8
        )
    {
        return Ok(arith_domain(cx, io));
    }
    packed_domain(cx, io, authored)
}

fn packed_domain(cx: &SqeCtx, io: JoinOut, authored: i32) -> Result<(i128, i128), JoinRefuse> {
    let (bank, faces, col) = (cx.bank, cx.faces, io.col);
    match bank.face(col) {
        crate::bank::Face::PackedNumeric { scale } if scale == authored => {}
        _ => return Err(JoinRefuse::Face { attno: col, what: "packed-arith-scale" }),
    }
    let Some(w) = crate::witness::Witness::exact_domain(&faces.stats(bank, col)) else {
        return Err(JoinRefuse::Face { attno: col, what: "numeric-scale-witness" });
    };
    let (lo, hi) = w.value();
    Ok((lo as i128, hi as i128))
}

/// Does the interval fit the PG op's result width (2/4/8)? PG errors on
/// per-row overflow of the OPERATOR's result type — an unproven shape is
/// a typed refusal, never a wrapped fold (identical answers include
/// identical errors; this engine never errors mid-fold).
fn arith_fits(w: u8, lo: i128, hi: i128) -> bool {
    let (tl, th) = match w {
        2 => (i16::MIN as i128, i16::MAX as i128),
        4 => (i32::MIN as i128, i32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    };
    lo >= tl && hi <= th
}

fn arith_mul_iv((al, ah): (i128, i128), (bl, bh): (i128, i128)) -> (i128, i128) {
    let cs = [al * bl, al * bh, ah * bl, ah * bh];
    (*cs.iter().min().expect("4 corners"), *cs.iter().max().expect("4 corners"))
}

/// [sqe-mech3] Admission for the fused-arithmetic fold legs (the P4-1
/// overflow law composed onto the join): every arith leg's per-row value
/// must provably fit its PG op's RESULT width — else the typed refusal
/// (`agg-arith-overflow-unwitnessed`). Called by the seam at lowering
/// AND at the runner entry (one law, two gates).
pub fn check_join_agg(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<(), JoinRefuse> {
    let refuse = || JoinRefuse::Unsupported { what: "agg-arith-overflow-unwitnessed" };
    // [yearkey] a DATE out with a FINITE witnessed domain.
    if !anode.group_xf.is_empty() && anode.group_xf.len() != anode.groups.len() {
        return Err(JoinRefuse::Unsupported { what: "agg-group-xf-shape" });
    }
    for (j, xf) in anode.group_xf.iter().enumerate() {
        let Some(xf) = xf else { continue };
        let g = anode.groups[j];
        match xf {
            KeyXf::Year => {
                if anode.join.out_tys[anode.group_oi[j]].oid != crate::typmeta::oids::DATE {
                    return Err(JoinRefuse::Face { attno: g.col, what: "year-key-source" });
                }
                let (lo, hi) = arith_domain(side_ctx(bctx, pctx, dctxs, g.side), g);
                if lo <= i32::MIN as i128 || hi >= i32::MAX as i128 {
                    return Err(JoinRefuse::Face { attno: g.col, what: "date-finite-witness" });
                }
            }
            // [textslice] a C-collation varlena lane whose stored images
            // provably hold single-byte chars — the byte prefix IS the
            // char prefix; bare bpchar (no declared width) refuses.
            KeyXf::TextSlice { from, len } => {
                if *from != 1 || !(1..=64).contains(len) {
                    return Err(JoinRefuse::Unsupported { what: "text-slice-shape" });
                }
                let cx = side_ctx(bctx, pctx, dctxs, g.side);
                let ty = cx.bank.typ(g.col);
                let src_ok = anode.join.out_bytes[anode.group_oi[j]]
                    && matches!(cx.bank.face(g.col), crate::bank::Face::Varlena)
                    && ty.collation == crate::typmeta::COLLATION_C;
                if !src_ok {
                    return Err(JoinRefuse::Face { attno: g.col, what: "text-slice-source" });
                }
                let bpchar = ty.oid == crate::typmeta::oids::BPCHAR;
                let pad = crate::typmeta::bpchar_declared_chars(ty.typmod).map(|n| n as u32);
                if bpchar && pad.is_none() {
                    return Err(JoinRefuse::Face { attno: g.col, what: "text-slice-source" });
                }
                let stats = cx.faces.stats(cx.bank, g.col);
                if !stats.single_byte_chars(if bpchar { pad } else { None }) {
                    return Err(JoinRefuse::Face { attno: g.col, what: "text-slice-char-witness" });
                }
            }
        }
    }
    for a in &anode.aggs {
        let Some(ar) = a.arith else { continue };
        let io = a.input.expect("arith carries an input");
        let da = arith_domain(side_ctx(bctx, pctx, dctxs, io.side), io);
        match ar {
            JoinArith::AddK { k, w } => {
                if !arith_fits(w, da.0 + k as i128, da.1 + k as i128) {
                    return Err(refuse());
                }
            }
            JoinArith::MulCC { w } => {
                let io2 = a.input2.expect("mul carries input2");
                let db = arith_domain(side_ctx(bctx, pctx, dctxs, io2.side), io2);
                let (lo, hi) = arith_mul_iv(da, db);
                if !arith_fits(w, lo, hi) {
                    return Err(refuse());
                }
            }
            JoinArith::MulKSub { k, w, wi } => {
                let io2 = a.input2.expect("mul carries input2");
                let (bl, bh) = arith_domain(side_ctx(bctx, pctx, dctxs, io2.side), io2);
                // inner op: (k - b) must fit ITS result type on every row…
                let inner = (k as i128 - bh, k as i128 - bl);
                if !arith_fits(wi, inner.0, inner.1) {
                    return Err(refuse());
                }
                // …then the product must fit the mul op's.
                let (lo, hi) = arith_mul_iv(da, inner);
                if !arith_fits(w, lo, hi) {
                    return Err(refuse());
                }
            }
            JoinArith::PackedMulK { k, sub, sa, sb } => {
                let io2 = a.input2.expect("mul carries input2");
                let da = packed_or_word_domain(side_ctx(bctx, pctx, dctxs, io.side), io, sa)?;
                let (bl, bh) =
                    packed_or_word_domain(side_ctx(bctx, pctx, dctxs, io2.side), io2, sb)?;
                let unwitnessed =
                    || JoinRefuse::Face { attno: io.col, what: "numeric-scale-witness" };
                let inner = if sub { (k as i128 - bh, k as i128 - bl) } else { (k as i128 + bl, k as i128 + bh) };
                if !arith_fits(8, inner.0, inner.1) {
                    return Err(unwitnessed());
                }
                let (lo, hi) = arith_mul_iv(da, inner);
                if !arith_fits(8, lo, hi) {
                    return Err(unwitnessed());
                }
            }
            JoinArith::PackedMulKSubCC { k, sub, sa, sb, c, sc, d, sd } => {
                let io2 = a.input2.expect("mul carries input2");
                let da = packed_or_word_domain(side_ctx(bctx, pctx, dctxs, io.side), io, sa)?;
                let (bl, bh) =
                    packed_or_word_domain(side_ctx(bctx, pctx, dctxs, io2.side), io2, sb)?;
                let dc = packed_or_word_domain(side_ctx(bctx, pctx, dctxs, c.side), c, sc)?;
                let dd = packed_or_word_domain(side_ctx(bctx, pctx, dctxs, d.side), d, sd)?;
                let unwitnessed =
                    || JoinRefuse::Face { attno: io.col, what: "numeric-scale-witness" };
                let inner = if sub { (k as i128 - bh, k as i128 - bl) } else { (k as i128 + bl, k as i128 + bh) };
                if !arith_fits(8, inner.0, inner.1) {
                    return Err(unwitnessed());
                }
                // Products fit i64 raw AND aligned, then the difference.
                let pa = arith_mul_iv(da, inner);
                let pb = arith_mul_iv(dc, dd);
                if !arith_fits(8, pa.0, pa.1) || !arith_fits(8, pb.0, pb.1) {
                    return Err(unwitnessed());
                }
                let s = (sa + sb).max(sc + sd);
                let (ga, gb) = (s - sa - sb, s - sc - sd);
                if ga > 18 || gb > 18 {
                    return Err(unwitnessed());
                }
                let (ma, mb) = (10i128.pow(ga as u32), 10i128.pow(gb as u32));
                let pa = (pa.0 * ma, pa.1 * ma);
                let pb = (pb.0 * mb, pb.1 * mb);
                if !arith_fits(8, pa.0, pa.1) || !arith_fits(8, pb.0, pb.1) {
                    return Err(unwitnessed());
                }
                if !arith_fits(8, pa.0 - pb.1, pa.1 - pb.0) {
                    return Err(unwitnessed());
                }
            }
        }
    }
    Ok(())
}

#[inline(always)]
fn hash_bytes(mut h: u64, b: &[u8]) -> u64 {
    h = hash64(h ^ b.len() as u64);
    for c in b.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        h = hash64(h ^ u64::from_le_bytes(w));
    }
    h
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GKeyKind {
    Word,
    Text,
}

type DSet = std::collections::HashSet<
    u64,
    std::hash::BuildHasherDefault<crate::kernels_f6::FxHasher>,
>;

/// Charged bytes per resident distinct-set entry (slot + load headroom).
const DSET_ENTRY_BYTES: u64 = 16;

/// Distinct set-plane spill engagement census (rig gates prove the legs
/// ran): worker set drains, finalize table drains, finalize merges.
pub static JDSPILL_SCATTERS: AtomicU64 = AtomicU64::new(0);
pub static JDSPILL_DRAINS: AtomicU64 = AtomicU64::new(0);
pub static JDSPILL_MERGES: AtomicU64 = AtomicU64::new(0);

/// The armed set-plane spill: one statement store, worker file ordinals.
struct JdSpill {
    store: Arc<dyn crate::spill::SpillStore>,
    wid: AtomicUsize,
}

/// Shared distinct-set byte meter: every worker's set growth folds in.
/// Unarmed (`spill: None`), crossing `budget` latches `over` — the sets
/// freeze and the run returns the typed refusal after the pool drains
/// (never an OOM). Armed, `over` never latches: workers drain their
/// resident sets to spill records instead and uncharge the meter.
struct DistinctGauge {
    bytes: AtomicU64,
    budget: u64,
    over: AtomicBool,
    spill: Option<JdSpill>,
}

impl DistinctGauge {
    fn charge(&self, entries: u64) -> bool {
        if entries > 0 {
            let b = self.bytes.fetch_add(entries * DSET_ENTRY_BYTES, Ordering::Relaxed)
                + entries * DSET_ENTRY_BYTES;
            if b > self.budget && self.spill.is_none() {
                self.over.store(true, Ordering::Relaxed);
            }
        }
        !self.over.load(Ordering::Relaxed)
    }
}

/// [sqe-mech3] Composite-key grouped table: one owned open-address map
/// per worker keyed on the FULL key tuple (word lanes compared by value,
/// text lanes by bytes — the byte-grouping identity C collation grants;
/// NULL components group as equal, the SQL GROUP BY law). Group indices
/// are stable across growth (only the bucket heads rebuild), so staged
/// slots never go stale.
struct MultiTab {
    nk: usize,
    nkt: usize,
    na: usize,
    /// Distinct legs per group: `dsets[g * nd + di]` is leg di's set.
    nd: usize,
    hash: Vec<u64>,
    kw: Vec<i64>,
    kv: Vec<bool>,
    kt: Vec<(u32, u32)>,
    arena: Vec<u8>,
    rows: Vec<u64>,
    cells: Vec<AccumCell>,
    dsets: Vec<DSet>,
    heads: Vec<u32>,
    next: Vec<u32>,
    mask: usize,
}

impl MultiTab {
    fn new(nk: usize, nkt: usize, na: usize, nd: usize) -> MultiTab {
        MultiTab {
            nk,
            nkt,
            na,
            nd,
            hash: Vec::new(),
            kw: Vec::new(),
            kv: Vec::new(),
            kt: Vec::new(),
            arena: Vec::new(),
            rows: Vec::new(),
            cells: Vec::new(),
            dsets: Vec::new(),
            heads: vec![NO_ENTRY; 1024],
            next: Vec::new(),
            mask: 1023,
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    fn text_eq(&self, g: usize, texts: &[Option<&[u8]>]) -> bool {
        for (j, t) in texts.iter().enumerate() {
            let (o, l) = self.kt[g * self.nkt + j];
            let stored = if (o, l) == T_NULL {
                None
            } else {
                Some(&self.arena[o as usize..(o + l) as usize])
            };
            if stored != *t {
                return false;
            }
        }
        true
    }

    /// Find-or-insert the group of one key tuple; returns its stable
    /// index. `kw`/`kv` are the nk word/validity lanes (text lanes carry
    /// word 0), `texts` the nkt text lanes in text-key order.
    fn touch(&mut self, h: u64, kw: &[i64], kv: &[bool], texts: &[Option<&[u8]>]) -> u32 {
        let mut e = self.heads[(h as usize) & self.mask];
        while e != NO_ENTRY {
            let g = e as usize;
            if self.hash[g] == h
                && &self.kw[g * self.nk..(g + 1) * self.nk] == kw
                && &self.kv[g * self.nk..(g + 1) * self.nk] == kv
                && self.text_eq(g, texts)
            {
                return e;
            }
            e = self.next[g];
        }
        // insert (grow first so the new entry's bucket is current).
        let n = self.rows.len();
        if (n + 1) * 2 > self.heads.len() {
            let cap = self.heads.len() * 2;
            self.mask = cap - 1;
            self.heads = vec![NO_ENTRY; cap];
            for g in 0..n {
                let slot = (self.hash[g] as usize) & self.mask;
                self.next[g] = self.heads[slot];
                self.heads[slot] = g as u32;
            }
        }
        assert!(n < NO_ENTRY as usize, "composite group table exceeds u32 entries");
        self.hash.push(h);
        self.kw.extend_from_slice(kw);
        self.kv.extend_from_slice(kv);
        for t in texts {
            match t {
                Some(b) => {
                    let off = self.arena.len() as u32;
                    self.arena.extend_from_slice(b);
                    self.kt.push((off, b.len() as u32));
                }
                None => self.kt.push(T_NULL),
            }
        }
        self.rows.push(0);
        self.cells.extend(std::iter::repeat(AccumCell::default()).take(self.na));
        self.dsets.extend((0..self.nd).map(|_| DSet::default()));
        let slot = (h as usize) & self.mask;
        self.next.push(self.heads[slot]);
        self.heads[slot] = n as u32;
        n as u32
    }
}

/// [sqe-mech3] Composite grouped folds over the join probe loop. Same R2
/// staging structure as `GroupFold` (slot resolution per row, fold-op
/// dispatch once per flush); the answer merges worker tables under a
/// canonical key-tuple encoding, so it is pool-width independent.
struct GroupFoldMulti<'a> {
    node: &'a JoinAggNode,
    kinds: &'a [GKeyKind],
    tab: MultiTab,
    srcs: Vec<Option<FoldSrc<'a>>>,
    /// Per distinct leg: its `join.out` lane (aligned with `dlanes`).
    dsrcs: Vec<usize>,
    gauge: Arc<DistinctGauge>,
    /// Armed-spill state: this worker's run file, its (offset, records)
    /// chunks, and its resident set-entry census.
    dspill: Option<crate::stencils::hash_group::BW>,
    druns: Vec<(u64, u64)>,
    dres: u64,
    bslots: Vec<u32>,
    blanes: Vec<Vec<(i64, bool)>>,
    dlanes: Vec<Vec<(i64, bool)>>,
}

impl GroupFoldMulti<'_> {
    /// Drain this worker's resident sets to one chunk of 16 B
    /// (group<<8|leg, word) records and uncharge the gauge; the finalize
    /// dedupe-merge makes cross-drain duplicates harmless.
    fn drain_sets(&mut self) {
        if self.dres == 0 {
            return;
        }
        JDSPILL_SCATTERS.fetch_add(1, Ordering::Relaxed);
        let sp = self.gauge.spill.as_ref().expect("drain implies the armed store");
        let bw = self.dspill.get_or_insert_with(|| {
            let w = sp.wid.fetch_add(1, Ordering::Relaxed);
            crate::stencils::hash_group::BW::new(&*sp.store, "jdset-scatter", w)
        });
        let nd = self.tab.nd;
        let mut n = 0u64;
        bw.begin();
        for g in 0..self.tab.len() {
            for di in 0..nd {
                let s = std::mem::take(&mut self.tab.dsets[g * nd + di]);
                for w in s {
                    bw.push(&(((g as u64) << 8) | di as u64).to_ne_bytes());
                    bw.push(&w.to_ne_bytes());
                    n += 1;
                }
            }
        }
        let (off, _len) = bw.end();
        self.druns.push((off, n));
        self.gauge.bytes.fetch_sub(n * DSET_ENTRY_BYTES, Ordering::Relaxed);
        self.dres = 0;
    }

    fn flush(&mut self) {
        if self.bslots.is_empty() {
            return;
        }
        // Distinct planes: valid words insert into the slot's set; NULLs
        // never count. Over budget the sets freeze (the run refuses) —
        // or, spill-armed, this worker's sets drain to records instead.
        if !self.dsrcs.is_empty() && !self.gauge.over.load(Ordering::Relaxed) {
            let nd = self.tab.nd;
            let mut fresh = 0u64;
            for (di, lane) in self.dlanes.iter().enumerate() {
                for (i, &(w, ok)) in lane.iter().enumerate() {
                    if ok
                        && self.tab.dsets[self.bslots[i] as usize * nd + di].insert(w as u64)
                    {
                        fresh += 1;
                    }
                }
            }
            self.dres += fresh;
            self.gauge.charge(fresh);
            if self.gauge.spill.is_some()
                && self.gauge.bytes.load(Ordering::Relaxed) > self.gauge.budget
            {
                self.drain_sets();
            }
        }
        let na = self.node.aggs.len();
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(FoldSrc { op, .. }) = *src else { continue };
            let (lane, slots) = (&self.blanes[ai][..], &self.bslots[..]);
            let cells = &mut self.tab.cells[..];
            #[inline(always)]
            fn mrun(
                lane: &[(i64, bool)],
                slots: &[u32],
                cells: &mut [AccumCell],
                na: usize,
                ai: usize,
                f: impl Fn(&mut AccumCell, i64, bool),
            ) {
                for (i, &(w, ok)) in lane.iter().enumerate() {
                    f(&mut cells[slots[i] as usize * na + ai], w, ok);
                }
            }
            match op {
                AggFoldOp::CountCol => mrun(lane, slots, cells, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::CountCol, c, w, ok)
                }),
                AggFoldOp::Sum => mrun(lane, slots, cells, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Sum, c, w, ok)
                }),
                AggFoldOp::Min => mrun(lane, slots, cells, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Min, c, w, ok)
                }),
                AggFoldOp::Max => mrun(lane, slots, cells, na, ai, |c, w, ok| {
                    scatter_cell_fold(AggFoldOp::Max, c, w, ok)
                }),
                AggFoldOp::SumSq | AggFoldOp::BitAnd | AggFoldOp::BitOr => {
                    unreachable!("join recognizer admits count/sum/min/max only")
                }
            }
        }
        self.bslots.clear();
        for l in &mut self.blanes {
            l.clear();
        }
        for l in &mut self.dlanes {
            l.clear();
        }
    }
}

impl Sink for GroupFoldMulti<'_> {
    fn unit_start(&mut self, _ui: usize) {}

    fn row(&mut self, cx: &RowCx<'_, '_>, r: usize, entry: Option<(usize, usize)>) {
        let nk = self.node.groups.len();
        let mut kw = [0i64; 8];
        let mut kv = [false; 8];
        let mut texts: [Option<&[u8]>; 8] = [None; 8];
        let mut nt = 0usize;
        let mut h = 0x9E37_79B9_7F4A_7C15u64;
        for j in 0..nk {
            let goi = self.node.group_oi[j];
            match self.kinds[j] {
                GKeyKind::Word => {
                    let (mut w, ok) = cx.out_word(goi, r, entry);
                    if let Some(Some(xf)) = self.node.group_xf.get(j) {
                        w = xf.apply(w);
                    }
                    kw[j] = w;
                    kv[j] = ok;
                    h = hash64(h ^ ok as u64 ^ hash64(w as u64));
                }
                GKeyKind::Text => {
                    let mut b = cx.out_bytes(goi, r, entry);
                    if let Some(Some(xf)) = self.node.group_xf.get(j) {
                        b = b.map(|b| xf.apply_bytes(b));
                    }
                    kv[j] = b.is_some();
                    h = match b {
                        Some(b) => hash_bytes(hash64(h ^ 1), b),
                        None => hash64(h),
                    };
                    texts[nt] = b;
                    nt += 1;
                }
            }
        }
        let slot = self.tab.touch(h, &kw[..nk], &kv[..nk], &texts[..nt]);
        self.tab.rows[slot as usize] += 1;
        self.bslots.push(slot);
        for (ai, src) in self.srcs.iter().enumerate() {
            let Some(s) = src else { continue };
            self.blanes[ai].push(stage_val(cx, r, entry, s));
        }
        for (di, &oi) in self.dsrcs.iter().enumerate() {
            self.dlanes[di].push(cx.out_word(oi, r, entry));
        }
        if self.bslots.len() >= STAGE {
            self.flush();
        }
    }
}

/// Canonical (injective, order-deterministic) encoding of one group's
/// key tuple — the merge identity across workers. Per key: 0x00 = NULL;
/// 0x01 + sign-biased BE word, or 0x01 + BE length + bytes for text.
fn multi_key_encode(tab: &MultiTab, kinds: &[GKeyKind], g: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut tj = 0usize;
    for (j, kind) in kinds.iter().enumerate() {
        let valid = tab.kv[g * tab.nk + j];
        match kind {
            GKeyKind::Word => {
                if !valid {
                    out.push(0);
                } else {
                    out.push(1);
                    let w = tab.kw[g * tab.nk + j] as u64 ^ (1u64 << 63);
                    out.extend_from_slice(&w.to_be_bytes());
                }
            }
            GKeyKind::Text => {
                let (o, l) = tab.kt[g * tab.nkt + tj];
                tj += 1;
                if (o, l) == T_NULL {
                    out.push(0);
                } else {
                    out.push(1);
                    out.extend_from_slice(&l.to_be_bytes());
                    out.extend_from_slice(&tab.arena[o as usize..(o + l) as usize]);
                }
            }
        }
    }
    out
}

/// Composite-key grouped answer: merge worker tables by canonical key
/// tuple, apply the fused HAVING, render [keys…, aggs…] — key columns
/// typed from `join.out_tys`, aggregate columns by the one fold law.
fn run_grouped_multi(
    bctx: &SqeCtx,
    pctx: &SqeCtx,
    dctxs: &[&SqeCtx],
    fctxs: &[&SqeCtx],
    anode: &JoinAggNode,
) -> Result<AnswerSet, JoinRefuse> {
    let na = anode.aggs.len();
    let nk = anode.groups.len();
    let kinds: Vec<GKeyKind> = anode
        .group_oi
        .iter()
        .map(|&goi| {
            if anode.join.out_bytes[goi] { GKeyKind::Text } else { GKeyKind::Word }
        })
        .collect();
    let nkt = kinds.iter().filter(|k| **k == GKeyKind::Text).count();
    let kindsr = &kinds[..];
    // Distinct legs: `di_of[ai]` = the leg's set plane (usize::MAX for
    // fold legs); `dsrcs[di]` = its `join.out` lane.
    let mut di_of = vec![usize::MAX; na];
    let mut dsrcs: Vec<usize> = Vec::new();
    for (ai, a) in anode.aggs.iter().enumerate() {
        if a.op == crate::joins::ir::JoinAggOp::CountDistinct {
            di_of[ai] = dsrcs.len();
            dsrcs.push(anode.agg_oi[ai]);
        }
    }
    let nd = dsrcs.len();
    assert!(nd < 256, "distinct legs exceed the record tag byte");
    // Arm the set-plane spill (fail-closed: kill switch or no substrate
    // keeps the freeze-then-refuse law verbatim).
    let spill = (nd > 0 && bctx.faces.cfg.spill)
        .then(crate::spill::new_store)
        .flatten()
        .map(|store| JdSpill { store, wid: AtomicUsize::new(0) });
    let gauge = Arc::new(DistinctGauge {
        bytes: AtomicU64::new(0),
        budget: anode.join.build_budget_bytes as u64,
        over: AtomicBool::new(false),
        spill,
    });
    let dsrcsr = &dsrcs[..];
    let mk_gauge = Arc::clone(&gauge);
    let mut sinks = run_core(bctx, pctx, dctxs, fctxs, &anode.join, || GroupFoldMulti {
        node: anode,
        kinds: kindsr,
        tab: MultiTab::new(nk, nkt, na, nd),
        srcs: fold_srcs(anode),
        dsrcs: dsrcsr.to_vec(),
        gauge: Arc::clone(&mk_gauge),
        dspill: None,
        druns: Vec::new(),
        dres: 0,
        bslots: Vec::with_capacity(STAGE),
        blanes: (0..na).map(|_| Vec::with_capacity(STAGE)).collect(),
        dlanes: (0..nd).map(|_| Vec::with_capacity(STAGE)).collect(),
    })?;
    for s in &mut sinks {
        s.flush();
    }
    if gauge.over.load(Ordering::Relaxed) {
        return Err(JoinRefuse::DistinctExceedsBudget {
            bytes: gauge.bytes.load(Ordering::Relaxed),
            budget: gauge.budget,
        });
    }
    let spilled = sinks.iter().any(|s| !s.druns.is_empty());
    let srcs = fold_srcs(anode);
    // Merge under the canonical tuple encoding (pool-width independent).
    struct MG {
        enc: Vec<u8>,
        sink: usize,
        g: usize,
        rows: u64,
    }
    let mut all: Vec<MG> = Vec::new();
    for (si, s) in sinks.iter().enumerate() {
        for g in 0..s.tab.len() {
            all.push(MG {
                enc: multi_key_encode(&s.tab, &kinds, g),
                sink: si,
                g,
                rows: s.tab.rows[g],
            });
        }
    }
    all.sort_unstable_by(|a, b| a.enc.cmp(&b.enc));
    // merged groups: representative (sink, g) + combined cells; distinct
    // planes union across workers (the sets move out of the sinks).
    struct MGrp {
        sink: usize,
        g: usize,
        rows: u64,
        cells: Vec<AccumCell>,
        dsets: Vec<DSet>,
        dcnt: Vec<i64>,
        enc: Vec<u8>,
    }
    let mut merged: Vec<MGrp> = Vec::new();
    // Per (sink, local group) -> merged index (the spilled records'
    // group identity at finalize).
    let mut gmap: Vec<Vec<u32>> = if spilled {
        sinks.iter().map(|s| vec![0u32; s.tab.len()]).collect()
    } else {
        Vec::new()
    };
    for m in all {
        let (msink, mg) = (m.sink, m.g);
        let cells: Vec<AccumCell> =
            sinks[m.sink].tab.cells[m.g * na..(m.g + 1) * na].to_vec();
        let dsets: Vec<DSet> = (0..nd)
            .map(|di| std::mem::take(&mut sinks[m.sink].tab.dsets[m.g * nd + di]))
            .collect();
        match merged.last_mut() {
            Some(last) if last.enc == m.enc => {
                last.rows += m.rows;
                for (ai, src) in srcs.iter().enumerate() {
                    if let Some(FoldSrc { op, .. }) = *src {
                        combine_cell_fold(op, &mut last.cells[ai], &cells[ai]);
                    }
                }
                for (di, s) in dsets.into_iter().enumerate() {
                    last.dsets[di].extend(s);
                }
            }
            _ => merged.push(MGrp {
                sink: m.sink,
                g: m.g,
                rows: m.rows,
                cells,
                dsets,
                dcnt: Vec::new(),
                enc: m.enc,
            }),
        }
        if spilled {
            gmap[msink][mg] = (merged.len() - 1) as u32;
        }
    }
    // Distinct answers: one exact count per (merged group, leg). The
    // resident path reads the unioned sets; the spilled path re-scatters
    // every record and resident residue under the merged identity into a
    // share-capped dedupe table, drains sorted runs, and counts one per
    // distinct entry at a k-way dedupe merge.
    if !spilled {
        for m in &mut merged {
            m.dcnt = m.dsets.iter().map(|s| s.len() as i64).collect();
        }
    } else {
        use crate::stencils::hash_group::BW;
        let sp = gauge.spill.as_ref().expect("spilled runs imply the armed store");
        let share = ((gauge.budget / bctx.faces.cfg.threads.max(1) as u64).max(4096)) as usize;
        let cap_entries = (share / 16).max(128);
        let slots = (cap_entries * 2).next_power_of_two();
        let mut tbl: Vec<u128> = vec![0; slots];
        let mut len = 0usize;
        let mask = slots - 1;
        let mut rw: Option<BW> = None;
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let fw = sinks.len();
        let mut drain = |tbl: &mut Vec<u128>, len: &mut usize, rw: &mut Option<BW>,
                         runs: &mut Vec<(u64, u64)>| {
            JDSPILL_DRAINS.fetch_add(1, Ordering::Relaxed);
            let mut es: Vec<u128> = tbl.iter().copied().filter(|&e| e != 0).collect();
            es.sort_unstable();
            let bw = rw.get_or_insert_with(|| BW::new(&*sp.store, "jdset-runs", fw));
            bw.begin();
            for e in &es {
                bw.push(&e.to_ne_bytes());
            }
            let (off, _l) = bw.end();
            runs.push((off, es.len() as u64));
            tbl.fill(0);
            *len = 0;
        };
        let mut insert = |mtag: u64, word: u64, tbl: &mut Vec<u128>, len: &mut usize,
                          rw: &mut Option<BW>, runs: &mut Vec<(u64, u64)>| {
            let e = (((mtag + 1) as u128) << 64) | word as u128;
            let h = hash64(mtag ^ hash64(word));
            loop {
                let mut slot = (h as usize) & mask;
                loop {
                    let cur = tbl[slot];
                    if cur == 0 {
                        if *len >= cap_entries {
                            break;
                        }
                        tbl[slot] = e;
                        *len += 1;
                        return;
                    }
                    if cur == e {
                        return;
                    }
                    slot = (slot + 1) & mask;
                }
                drain(tbl, len, rw, runs);
            }
        };
        for (si, s) in sinks.iter().enumerate() {
            let Some(bw) = &s.dspill else { continue };
            for &(off, n) in &s.druns {
                let mut cur = crate::spill::ChunkCursor::new(
                    &*bw.m,
                    off,
                    n,
                    16,
                    crate::spill::SLAB_BYTES.min(share),
                );
                while let Some(r) = cur.next() {
                    let tag = u64::from_ne_bytes(r[..8].try_into().unwrap());
                    let word = u64::from_ne_bytes(r[8..].try_into().unwrap());
                    let mi = gmap[si][(tag >> 8) as usize] as u64;
                    insert((mi << 8) | (tag & 255), word, &mut tbl, &mut len, &mut rw, &mut runs);
                }
            }
        }
        for (mi, m) in merged.iter().enumerate() {
            for (di, set) in m.dsets.iter().enumerate() {
                for &w in set {
                    insert(
                        ((mi as u64) << 8) | di as u64,
                        w,
                        &mut tbl,
                        &mut len,
                        &mut rw,
                        &mut runs,
                    );
                }
            }
        }
        let mut dcounts = vec![0i64; merged.len() * nd];
        let mut count = |e: u128, dcounts: &mut Vec<i64>| {
            let mtag = ((e >> 64) as u64) - 1;
            dcounts[(mtag >> 8) as usize * nd + (mtag & 255) as usize] += 1;
        };
        if runs.is_empty() {
            for &e in tbl.iter() {
                if e != 0 {
                    count(e, &mut dcounts);
                }
            }
        } else {
            JDSPILL_MERGES.fetch_add(1, Ordering::Relaxed);
            use std::cmp::Reverse;
            use std::collections::BinaryHeap;
            let mut mem: Vec<u128> = tbl.iter().copied().filter(|&e| e != 0).collect();
            mem.sort_unstable();
            let m = &*rw.as_ref().expect("runs imply a run file").m;
            let nrun = runs.len();
            let slab = (share / (nrun + 1)).clamp(16, crate::spill::SLAB_BYTES);
            let mut curs: Vec<crate::spill::ChunkCursor> = runs
                .iter()
                .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, 16, slab))
                .collect();
            let mut mi = 0usize;
            let mut heap: BinaryHeap<Reverse<(u128, usize)>> =
                BinaryHeap::with_capacity(nrun + 1);
            for (i, c) in curs.iter_mut().enumerate() {
                if let Some(r) = c.next() {
                    heap.push(Reverse((u128::from_ne_bytes(r.try_into().unwrap()), i)));
                }
            }
            if mi < mem.len() {
                heap.push(Reverse((mem[mi], nrun)));
                mi += 1;
            }
            while let Some(Reverse((e, src))) = heap.pop() {
                let mut adv = |heap: &mut BinaryHeap<Reverse<(u128, usize)>>, s: usize| {
                    if s < nrun {
                        if let Some(r) = curs[s].next() {
                            heap.push(Reverse((u128::from_ne_bytes(r.try_into().unwrap()), s)));
                        }
                    } else if mi < mem.len() {
                        heap.push(Reverse((mem[mi], nrun)));
                        mi += 1;
                    }
                };
                adv(&mut heap, src);
                while let Some(&Reverse((e2, s2))) = heap.peek() {
                    if e2 != e {
                        break;
                    }
                    heap.pop();
                    adv(&mut heap, s2);
                }
                count(e, &mut dcounts);
            }
        }
        for (mi, mg) in merged.iter_mut().enumerate() {
            mg.dcnt = (0..nd).map(|di| dcounts[mi * nd + di]).collect();
        }
    }
    if let Some(h) = &anode.having {
        merged.retain(|g| jhaving_keep(anode, h, g.rows, &g.cells));
    }
    // Render: key columns then aggregate columns.
    use crate::joins::ir::JoinAggOp;
    let mut cols: Vec<AnswerCol> = Vec::with_capacity(nk + na);
    let mut tj_of = vec![0usize; nk];
    {
        let mut tj = 0usize;
        for j in 0..nk {
            if kinds[j] == GKeyKind::Text {
                tj_of[j] = tj;
                tj += 1;
            }
        }
    }
    for j in 0..nk {
        let ty = match anode.group_xf.get(j) {
            Some(Some(xf)) => xf.out_ty(),
            _ => anode.join.out_tys[anode.group_oi[j]],
        };
        match kinds[j] {
            GKeyKind::Word => {
                let mut v = Vec::with_capacity(merged.len());
                let mut mask = Vec::with_capacity(merged.len());
                for mg in &merged {
                    let t = &sinks[mg.sink].tab;
                    v.push(t.kw[mg.g * nk + j]);
                    mask.push(t.kv[mg.g * nk + j]);
                }
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                cols.push(AnswerCol { ty, data: ColData::I64(v), validity });
            }
            GKeyKind::Text => {
                let mut bb = BytesBuild::new();
                let mut mask = Vec::with_capacity(merged.len());
                for mg in &merged {
                    let t = &sinks[mg.sink].tab;
                    let (o, l) = t.kt[mg.g * nkt + tj_of[j]];
                    if (o, l) == T_NULL {
                        bb.push(b"");
                        mask.push(false);
                    } else {
                        bb.push(&t.arena[o as usize..(o + l) as usize]);
                        mask.push(true);
                    }
                }
                let mut c = bb.finish(ty);
                if !mask.iter().all(|&x| x) {
                    c.validity = Validity::Mask(mask);
                }
                cols.push(c);
            }
        }
    }
    for (ai, a) in anode.aggs.iter().enumerate() {
        cols.push(match a.op {
            JoinAggOp::CountStar => {
                AnswerCol::i64s(a.out, merged.iter().map(|m| m.rows as i64).collect())
            }
            JoinAggOp::CountCol => {
                AnswerCol::i64s(a.out, merged.iter().map(|m| m.cells[ai].b).collect())
            }
            JoinAggOp::CountDistinct => AnswerCol::i64s(
                a.out,
                merged.iter().map(|m| m.dcnt[di_of[ai]]).collect(),
            ),
            JoinAggOp::Sum => {
                let mask: Vec<bool> = merged.iter().map(|m| m.cells[ai].b > 0).collect();
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol {
                    ty: a.out,
                    data: ColData::I128(merged.iter().map(|m| m.cells[ai].a).collect()),
                    validity,
                }
            }
            JoinAggOp::Min | JoinAggOp::Max => AnswerCol::i64s_opt(
                a.out,
                merged.iter().map(|m| minmax_answer(&m.cells[ai])).collect(),
            ),
        });
    }
    Ok(AnswerSet::from_cols(cols))
}
