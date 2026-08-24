//! Heap-on-sqe v1 (heap-face.md + the 2026-08-18 RULING: R1 no-fallback
//! APPLIES to heap — heap ships shape-by-shape like the columnar ladder).
//!
//! The statement-level heap arm of the sqe dispatch slot, behind
//! `pgrust.sqe_heap` (default OFF — the v1 safety switch). A RECOGNIZED
//! heap shape is sqe's: it serves or ERRORs typed and censused
//! (`heap/*` causes). UNRECOGNIZED heap analytic shapes return `None`
//! and keep routing to the incumbent engines ONLY until their rung
//! lands — the census (`heap-miss/*` engagement rows), not a fallback,
//! drives the ladder.
//!
//! Landed rungs:
//! - v1: ungrouped filtered folds: count(*)/sum/avg/min/max over
//!   admitted word columns, int-conjunct WHERE (plain Agg over a heap
//!   SeqScan);
//! - v1: grouped folds over ONE int2 group key (narrow-word group-count
//!   witness under the 2^20 cap — the witness-collapse law,
//!   heap-face.md §2.3);
//! - rung 2 (topk-group): a peeled const Limit/Sort tail —
//!   Limit(Sort(Agg(SeqScan))) — with ORDER BY over any sortable output
//!   column (key or fold leg; either direction, explicit NULLS), the
//!   slice/permutation applied at the answer boundary. A pushed bound
//!   with offset+count <= 2^20 is the group-count witness for WIDE int
//!   keys (int4/int8/date/ts/tstz — the bounded-answer admission of
//!   ADJUDICATION-20260818, mirrored from the columnar down-pass); the
//!   fold still runs under the GROUP_CAP live-group ceiling and a
//!   runtime breach is the typed `heap/group-cap` error. Unbounded wide
//!   keys keep the `heap/group-count-unwitnessed` refusal.
//! - scan rung: the pin-handoff drive (see the pack arm below).
//!
//! The face (heap-face.md §2.2): granule = fixed 256-block run computed
//! from nblocks (metadata only); fill = the executor's own pagemode scan
//! repositioned per granule (`heap_set_block_range` →
//! `heap_getnextpagebatch`, which runs `heap_prepare_pagescan`'s
//! visibility collection verbatim under the statement snapshot) →
//! row-major deform (`heap_deform_tuple`) scattered into per-statement
//! word/validity lanes. The scan inherits lanev2's laws: per-statement
//! MVCC snapshot, page pins only inside the fill, zero pins at settle
//! (`heap_end_claim_release`, error paths included), the seq-scan
//! BAS_BULKREAD ring via `initscan`'s strategy election. Serial by
//! design in v1 — the fill owns the backend thread (pins/bufmgr are
//! backend state); parallel fill is a later, gated rung.
//!
//! Cache law (heap-face.md §1.3): the engine runs `SqeConfig::heap_v1`
//! — condcache Never, stats/vw/fp caches off; `run_face_fold` REFUSES
//! any other config (the born-RED leg: `PGRUST_SQE_HEAP_SEED_CACHE=1`
//! wrongly arms a cache and the statement MUST fail).

use std::rc::Rc;

use ::tableam::TableScanDesc;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::{Node, NodeList, NodeTag};
use executils::EStateData;
use tcop_dest::DestReceiver;

use sqe::bank::{ColMeta, Face};
use sqe::face::{FaceError, FaceFill, ScanFace};
use sqe::ir::{CmpOp, PredTerm, TopKKey, VarOp, VarPredTerm};
use sqe::planner::{AAgg, APred, AValExpr};
use sqe::stencils::face_fold::{
    run_face_fold, run_face_topn, run_pack_fold, run_pack_topn, FaceFoldErr, FaceFoldSpec,
    FoldLeg, FoldOp, GroupKeyPart, GroupSpec, PackDeform, PackSource, GROUP_CAP,
};
use sqe::typmeta::{TypMeta, COLLATION_C};

use super::refusal::{fnv1a64, HeapDetail, RefuseCause};
use super::seam::{
    catalog_schema, cmp_conjunct, deliver, in_parallel_role, pred_term_of, recognize_aggref,
    resolve_var, sortproc_dir, OutSlot, OutSrc, Render,
};

/// Granule = fixed block run (heap-face.md Q2, blessed): B=256 blocks
/// keeps worst-case rows (256 × MaxHeapTuplesPerPage) under 65536 and
/// the walk a pure function of nblocks.
const GRANULE_BLOCKS: u64 = 256;

/// [b1] Bare top-n bound cap: past it the incumbent owns the sort.
const TOPN_CAP: u64 = 1 << 16;

const INT2OID: u32 = 21;
const INT4OID: u32 = 23;
const INT8OID: u32 = 20;
const DATEOID: u32 = 1082;
const TSOID: u32 = 1114;
const TSTZOID: u32 = 1184;
const TEXTOID: u32 = 25;
const VARCHAROID: u32 = 1043;

const AGG_PLAIN: u32 = 0;
const AGG_SORTED: u32 = 1;
const AGG_HASHED: u32 = 2;

// ---------------------------------------------------------------------------
// Recognition
// ---------------------------------------------------------------------------

/// Unrecognized (rung not landed): route to the incumbent, censused.
pub(super) struct Miss(pub(super) &'static str);

pub(super) enum Rec<T> {
    Ok(T),
    Miss(Miss),
    /// In-rung typed refusal (R1: serve or ERROR).
    Refuse(RefuseCause),
    /// An already-censused refusal from a shared seam recognizer
    /// (cmp_conjunct ticked it at construction — re-ticking would double
    /// the census row).
    Refused(super::refusal::Refusal),
}

struct HeapGoal {
    /// Whether a Sort sits above the Agg (planstate shape witness).
    top_sort: bool,
    /// Whether a Limit sits on top (planstate shape witness).
    top_limit: bool,
    /// Answer-order obligation from a peeled Sort: TopK keys over ANSWER
    /// columns (grouped answers: col 0 = key, 1.. = legs), always closed
    /// with the implicit (key ASC, NULLS LAST) tie-break so the emitted
    /// order is deterministic. Empty = the stencil's native key-asc
    /// emission order stands.
    order: Vec<TopKKey>,
    /// Peeled const LIMIT/OFFSET slice over the (ordered) answers.
    skip: usize,
    take: usize,
    spec: FaceFoldSpec,
    out: Vec<OutSlot>,
    /// Agg-less grouped shape: the hidden-count leg (b3 law ported).
    distinct: bool,
    /// Bare top-n scan (Limit(Sort(SeqScan)), no Agg).
    topn: bool,
}

pub(super) fn key_render(typid: u32) -> Render {
    match typid {
        INT2OID => Render::MinMaxI16,
        INT4OID | DATEOID => Render::MinMaxI32,
        TEXTOID | VARCHAROID => Render::Text,
        _ => Render::MinMaxI64,
    }
}

/// Identity passthrough tlist (positional OUTER vars, non-junk) — the
/// peeled Sort/Limit levels must project the level below unchanged.
fn identity_tlist(tl: &NodeList<'_>) -> bool {
    tl.iter().enumerate().all(|(i, tn)| {
        tn.as_target_entry().is_some_and(|te| {
            !te.resjunk
                && te.expr.as_var().is_some_and(|v| {
                    v.varno == ::types_nodes::primnodes::OUTER_VAR
                        && v.varlevelsup == 0
                        && i32::from(v.varattno) == i as i32 + 1
                })
        })
    })
}

/// [toastscan] Resolve an OUTER var through the scan targetlist (the
/// recognize_aggref `tls` convention): Agg-level expressions reference
/// the SeqScan's emitted columns positionally; the resolved entry's
/// expression carries the scanrelid-grain vars. None = not an OUTER var
/// (or no targetlist to resolve through).
fn resolve_outer<'p>(v: &::types_nodes::primnodes::Var<'_>, outer_tl: Option<&NodeList<'p>>) -> Option<Node<'p>> {
    if v.varno != ::types_nodes::primnodes::OUTER_VAR || v.varlevelsup != 0 || v.varattno <= 0 {
        return None;
    }
    let te = outer_tl?.iter().nth(v.varattno as usize - 1)?;
    Some(te.as_target_entry()?.expr)
}

/// [toastscan] Does this expression subtree reference a varlena-class
/// column of the scan relation (through relabels, operators, functions,
/// and OUTER vars resolved through the scan targetlist)?
fn refs_varlena_var(
    n: Node<'_>,
    scanrelid: u32,
    schema: &[ColMeta],
    outer_tl: Option<&NodeList<'_>>,
) -> bool {
    if let Some(v) = n.as_var() {
        if let Some(inner) = resolve_outer(v, outer_tl) {
            return refs_varlena_var(inner, scanrelid, schema, None);
        }
        return v.varno == scanrelid as i32
            && v.varlevelsup == 0
            && v.varattno > 0
            && schema
                .iter()
                .find(|c| c.attno == v.varattno as u32)
                .is_some_and(|c| matches!(Face::of_class(c.class), Face::Varlena));
    }
    if let Some(f) = n.as_func_expr() {
        return f.args.iter().any(|a| refs_varlena_var(a, scanrelid, schema, outer_tl));
    }
    if let Some(o) = n.as_op_expr() {
        return o.args.iter().any(|a| refs_varlena_var(a, scanrelid, schema, outer_tl));
    }
    if let Some(b) = n.as_bool_expr() {
        return b.args.iter().any(|a| refs_varlena_var(a, scanrelid, schema, outer_tl));
    }
    if let Some(r) = n.as_relabel_type() {
        return refs_varlena_var(r.arg, scanrelid, schema, outer_tl);
    }
    if let Some(te) = n.as_target_entry() {
        return refs_varlena_var(te.expr, scanrelid, schema, outer_tl);
    }
    if let Some(ar) = n.as_aggref() {
        return ar.args.iter().any(|a| refs_varlena_var(a, scanrelid, schema, outer_tl));
    }
    false
}

/// [toastscan] The typed toast-lane witness (R1 loudness, parity class
/// toastscan, 2026-08-20): TRUE when the subtree computes THROUGH a
/// function over a varlena column of the scan relation (`length(t)`,
/// `substr(t, ...)`, ...). Such a shape's recognition bail is the
/// unlanded detoast/varlena-expression rung and must miss TYPED as
/// `heap-toast` — the parity ledger's toastscan class was answered with
/// `sqe engaged=0` under admit-all and only a GENERIC miss class in the
/// census (SILENT-GAP at the capability grain). Bare varlena Var
/// operands stay their own classes (LIKE vocabulary, text keys, the
/// text-out-of-line execution refusal) — only the function-enclosed
/// varlena reference names this rung. `outer_tl` = the scan targetlist
/// for Agg-level expressions (OUTER vars resolve through it, exactly as
/// recognize_aggref's `tls`); None at the scan grain.
fn varlena_func_ref(
    n: Node<'_>,
    scanrelid: u32,
    schema: &[ColMeta],
    outer_tl: Option<&NodeList<'_>>,
) -> bool {
    if let Some(v) = n.as_var() {
        // A bare OUTER var may name a scan-computed expression — the
        // function lives in the scan targetlist entry.
        if let Some(inner) = resolve_outer(v, outer_tl) {
            return varlena_func_ref(inner, scanrelid, schema, None);
        }
        return false;
    }
    if let Some(f) = n.as_func_expr() {
        return f.args.iter().any(|a| {
            refs_varlena_var(a, scanrelid, schema, outer_tl)
                || varlena_func_ref(a, scanrelid, schema, outer_tl)
        });
    }
    if let Some(o) = n.as_op_expr() {
        return o.args.iter().any(|a| varlena_func_ref(a, scanrelid, schema, outer_tl));
    }
    if let Some(b) = n.as_bool_expr() {
        return b.args.iter().any(|a| varlena_func_ref(a, scanrelid, schema, outer_tl));
    }
    if let Some(r) = n.as_relabel_type() {
        return varlena_func_ref(r.arg, scanrelid, schema, outer_tl);
    }
    if let Some(te) = n.as_target_entry() {
        return varlena_func_ref(te.expr, scanrelid, schema, outer_tl);
    }
    if let Some(ar) = n.as_aggref() {
        return ar.args.iter().any(|a| varlena_func_ref(a, scanrelid, schema, outer_tl));
    }
    false
}

/// Lower the scan's WHERE conjuncts (shared by the fold and top-n arms):
/// int cmp terms + [rung 3] text var terms.
pub(super) fn lower_quals(
    qual: &NodeList<'_>,
    scanrelid: u32,
    schema: &[ColMeta],
    estate: &EStateData<'_>,
    fp: u64,
) -> Rec<(Vec<PredTerm>, Vec<VarPredTerm>)> {
    let col = |attno: u32| schema.iter().find(|c| c.attno == attno);
    let word_ok = |attno: u32| -> bool {
        col(attno).is_some_and(|c| c.typ.byval && Face::of_class(c.class).word_foldable())
    };
    let text_ok = |attno: u32| -> bool {
        col(attno).is_some_and(|c| {
            matches!(c.typ.oid, TEXTOID | VARCHAROID)
                && matches!(Face::of_class(c.class), Face::Varlena)
        })
    };
    let mut terms: Vec<PredTerm> = Vec::new();
    let mut var_terms: Vec<VarPredTerm> = Vec::new();
    for q in qual.iter() {
        let (attno, ap, _) = match cmp_conjunct(q, scanrelid, estate.es_query_cxt, fp, &std::collections::HashMap::new()) {
            Ok(t) => t,
            Err(r) => {
                // Collation guards refuse typed (the columnar precedent —
                // cmp_conjunct already censused the refusal); everything
                // else is an unlanded-rung miss.
                match r.cause() {
                    RefuseCause::NonCCollation { .. }
                    | RefuseCause::NondeterministicCollation { .. } => {
                        return Rec::Refused(r)
                    }
                    // [toastscan] a conjunct computing THROUGH a
                    // function over a varlena lane (`length(t) > k`) is
                    // the unlanded detoast/varlena-expression rung —
                    // typed heap-toast, never the generic pred miss
                    // (the parity SILENT-GAP, 2026-08-20).
                    _ if varlena_func_ref(q, scanrelid, schema, None) => {
                        return Rec::Miss(Miss("heap-toast"))
                    }
                    _ => return Rec::Miss(Miss("pred")),
                }
            }
        };
        match &ap {
            // [rung 3] LIKE / NOT LIKE over a text lane: the seam
            // guarded proc + C inputcollid + the encoding `_`-grain law;
            // lowering mirrors the engine's (like.rs): `%x%` normalizes
            // to Contains/NotContains, everything else takes the ported
            // general matcher. A trailing-escape pattern routes incumbent
            // (PG raises its own 22025 there — identical either engine).
            APred::Like { col: c, pattern, not, .. } => {
                if !text_ok(*c) {
                    return Rec::Miss(Miss("type"));
                }
                let ty = col(*c).expect("text_ok checked").typ;
                let pat = pattern.as_bytes();
                if sqe::like::like_valid(pat).is_err() {
                    return Rec::Miss(Miss("pred"));
                }
                let (op, needle) = match sqe::like::classify(pat) {
                    sqe::like::LikeClass::Contains(inner) => (
                        if *not { VarOp::NotContains } else { VarOp::Contains },
                        inner,
                    ),
                    sqe::like::LikeClass::General => (
                        if *not { VarOp::NotLike } else { VarOp::Like },
                        pat.to_vec(),
                    ),
                };
                var_terms.push(VarPredTerm::new(*c, op, needle, ty));
                continue;
            }
            // `col <> ''` (the seam lowers only the empty-const form,
            // C collation guarded there).
            APred::NeEmpty { col: c, .. } => {
                if !text_ok(*c) {
                    return Rec::Miss(Miss("type"));
                }
                let ty = col(*c).expect("text_ok checked").typ;
                var_terms.push(VarPredTerm::new(*c, VarOp::NeEmpty, Vec::new(), ty));
                continue;
            }
            _ => {}
        }
        if !word_ok(attno) {
            return Rec::Miss(Miss("type"));
        }
        let ty = col(attno).expect("word_ok checked").typ;
        // In-lists lower to In2 elsewhere; v1 heap keeps cmp/range only.
        let term = match &ap {
            APred::And { .. } | APred::Not { .. } => return Rec::Miss(Miss("pred")),
            _ => match pred_term_of(&ap, ty) {
                Some(t) => t,
                None => return Rec::Miss(Miss("pred")),
            },
        };
        terms.push(term);
    }
    Rec::Ok((terms, var_terms))
}

/// Recognize the v1 heap shape set from the plan tree. Pure plan walk —
/// no executor state touched.
fn recognize(
    estate: &EStateData<'_>,
    pstmt: &PlannedStmt<'_>,
    fp: u64,
) -> Rec<HeapGoal> {
    let Some(mut top) = pstmt.planTree else {
        return Rec::Miss(Miss("node"));
    };

    // Optional Limit on top (rung 2): plain COUNT option, const bounds,
    // identity passthrough tlist. A pushed bound with offset+count <=
    // 2^20 is the group-count witness for wide int keys (the bounded-
    // answer admission of ADJUDICATION-20260818 — a k-bounded answer
    // cannot truncate), mirrored from the columnar down-pass.
    let mut top_limit = false;
    let mut lim_offset: Option<i64> = None;
    let mut lim_count: Option<i64> = None;
    if top.node_tag() == NodeTag::T_Limit {
        let Some(lim) = top.as_limit() else { return Rec::Miss(Miss("node")) };
        if lim.limitOption != ::types_nodes::LimitOption::LIMIT_OPTION_COUNT
            || lim.uniqNumCols != 0
            || !lim.plan.initPlan.is_nil()
            || lim.plan.righttree.is_some()
            || !identity_tlist(&lim.plan.targetlist)
        {
            return Rec::Miss(Miss("limit"));
        }
        let bound = |n: Option<Node<'_>>| -> Result<Option<i64>, ()> {
            let Some(n) = n else { return Ok(None) };
            let k = (n.node_tag() == NodeTag::T_Const)
                .then(|| n.as_const())
                .flatten()
                .ok_or(())?;
            // NULL bound = no bound (LIMIT ALL / OFFSET 0 — C's
            // recompute_limits semantics).
            Ok((!k.constisnull).then(|| k.constvalue.as_i64()))
        };
        let (Ok(off), Ok(cnt)) = (bound(lim.limitOffset), bound(lim.limitCount)) else {
            return Rec::Miss(Miss("limit"));
        };
        // A negative COUNT raises in the incumbent executor — route it
        // there; a negative OFFSET clamps to 0 (recompute_limits).
        if cnt.is_some_and(|c| c < 0) {
            return Rec::Miss(Miss("limit"));
        }
        let Some(child) = lim.plan.lefttree else { return Rec::Miss(Miss("limit")) };
        lim_offset = off;
        lim_count = cnt;
        top_limit = true;
        top = child;
    }
    let skip = lim_offset.map_or(0usize, |o| o.max(0) as usize);
    let take = lim_count.map_or(usize::MAX, |c| c as usize);
    // The adjudicated bounded-answer witness: offset+count <= GROUP_CAP.
    let bounded = lim_count.is_some() && (skip as u64).saturating_add(take as u64) <= GROUP_CAP;

    // Optional Sort above the Agg (grouped shapes; rung 2: any sortable
    // output column, either direction, explicit NULLS placement).
    let mut sort_node: Option<Node<'_>> = None;
    if top.node_tag() == NodeTag::T_Sort {
        let Some(srt) = top.as_sort() else { return Rec::Miss(Miss("node")) };
        if !srt.plan.initPlan.is_nil() || srt.plan.righttree.is_some() {
            return Rec::Miss(Miss("sort"));
        }
        let Some(child) = srt.plan.lefttree else { return Rec::Miss(Miss("sort")) };
        sort_node = Some(top);
        top = child;
    }
    // A Limit directly over a grouped Agg (no Sort) asks for an
    // ARBITRARY k-subset of groups — the incumbent's pick is its own;
    // route it there (identity with the row engine is not definable).
    if top_limit && sort_node.is_none() && top.node_tag() == NodeTag::T_Agg {
        if top.as_agg().is_some_and(|a| a.numCols != 0) {
            return Rec::Miss(Miss("limit"));
        }
    }

    // [b1] Bare top-n scan: Limit(Sort(SeqScan)) with no Agg level.
    if top.node_tag() == NodeTag::T_SeqScan {
        return recognize_topn(estate, top, sort_node, lim_count, skip, take, fp);
    }
    if top.node_tag() != NodeTag::T_Agg {
        return Rec::Miss(Miss("node"));
    }
    let Some(agg) = top.as_agg() else { return Rec::Miss(Miss("node")) };
    if !agg.groupingSets.is_nil()
        || !agg.chain.is_nil()
        || agg.aggsplit != ::types_nodes::primnodes::AGGSPLIT_SIMPLE
        || !agg.plan.qual.is_nil()
        || !agg.plan.initPlan.is_nil()
        || agg.plan.righttree.is_some()
    {
        return Rec::Miss(Miss("agg"));
    }
    let grouped = agg.numCols != 0;
    let strategy_ok = if grouped {
        agg.aggstrategy == AGG_SORTED || agg.aggstrategy == AGG_HASHED
    } else {
        agg.aggstrategy == AGG_PLAIN && sort_node.is_none()
    };
    if !strategy_ok {
        return Rec::Miss(Miss("agg"));
    }
    let Some(scan_node) = agg.plan.lefttree else { return Rec::Miss(Miss("agg")) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        return Rec::Miss(Miss("node"));
    }
    let Some(scan) = scan_node.as_seq_scan() else { return Rec::Miss(Miss("node")) };
    if !scan.scan.plan.initPlan.is_nil()
        || scan.scan.plan.lefttree.is_some()
        || scan.scan.plan.righttree.is_some()
    {
        return Rec::Miss(Miss("node"));
    }
    let scanrelid = scan.scan.scanrelid;
    let scan_tl: &NodeList<'_> = &scan.scan.plan.targetlist;
    let tls: [&NodeList<'_>; 1] = [scan_tl];

    // The scanned relation must be THE heap relation of the statement.
    let Some(rel) = estate
        .es_relations
        .get(scanrelid as usize - 1)
        .and_then(|r| r.as_ref())
        .filter(|rel| ::tableam::TableAm::of(rel) == Some(::tableam_vocab::TableAm::Heap))
    else {
        return Rec::Miss(Miss("node"));
    };
    let schema = match catalog_schema(rel) {
        Ok(s) => s,
        Err(_) => return Rec::Miss(Miss("type")),
    };
    let col = |attno: u32| schema.iter().find(|c| c.attno == attno);
    // v1 lane law: byval word faces only (varlena/fixed byref lanes land
    // with the scan-serve rung — detoast-at-fill deferred with it).
    let word_ok = |attno: u32| -> bool {
        col(attno).is_some_and(|c| {
            c.typ.byval && Face::of_class(c.class).word_foldable()
        })
    };
    // [rung 3] text byte lanes (LIKE-class conjuncts, text group keys):
    // text/varchar as verbatim varlena. Collation is guarded separately
    // (non-C refuses typed — the columnar precedent).
    let text_ok = |attno: u32| -> bool {
        col(attno).is_some_and(|c| {
            matches!(c.typ.oid, TEXTOID | VARCHAROID)
                && matches!(Face::of_class(c.class), Face::Varlena)
        })
    };

    // --- group keys (0/1 keys; [b1] the int+text composite pair) -------------
    // (attno, typid, child tlist pos)
    let mut gkeys: Vec<(u32, u32, i16)> = Vec::new();
    let mut witness_cap: u64 = 0;
    if grouped {
        let nk = agg.numCols as usize;
        if nk == 0 || nk > 2 || agg.grpColIdx.len() < nk {
            return Rec::Miss(Miss("key"));
        }
        for j in 0..nk {
            let ci = agg.grpColIdx[j];
            let te = (ci > 0)
                .then(|| scan_tl.iter().nth(ci as usize - 1))
                .flatten()
                .and_then(|n| n.as_target_entry());
            let Some(te) = te else { return Rec::Miss(Miss("key")) };
            if te.expr.node_tag() != NodeTag::T_Var {
                return Rec::Miss(Miss("key"));
            }
            let Some((attno, typid)) = resolve_var(te.expr, &[], scanrelid) else {
                return Rec::Miss(Miss("key"));
            };
            gkeys.push((attno, typid, ci));
        }
        let is_text = |t: u32| matches!(t, TEXTOID | VARCHAROID);
        for &(attno, typid, _) in &gkeys {
            if is_text(typid) {
                // Byte-keyed arm; C collation only (memcmp law).
                if !text_ok(attno) {
                    return Rec::Miss(Miss("type"));
                }
                let coll = col(attno).expect("text_ok checked").typ.collation;
                if coll != COLLATION_C {
                    return Rec::Refuse(RefuseCause::NonCCollation { coll_oid: coll });
                }
            } else {
                if !matches!(typid, INT2OID | INT4OID | INT8OID | DATEOID | TSOID | TSTZOID) {
                    return Rec::Miss(Miss("key"));
                }
                if !word_ok(attno) {
                    return Rec::Miss(Miss("type"));
                }
            }
        }
        // Witness: int2 single key = type domain; a pushed bound
        // witnesses the rest — and [heap spill] the engaged spill arm
        // RETIRES the witness need entirely (§15.15): the hash arms
        // serve any group count, correct answer or typed refusal at the
        // E17/E18 limits. Disarmed keeps the pre-spill law verbatim.
        witness_cap = match gkeys.as_slice() {
            [(_, INT2OID, _)] => (1u64 << 16) + 1,
            [_] => {
                if bounded || heap_spill_serves() {
                    GROUP_CAP
                } else {
                    return Rec::Refuse(RefuseCause::Heap(HeapDetail::GroupCountUnwitnessed));
                }
            }
            [k0, k1] => {
                // [b1] one word key beside one text key.
                if is_text(k0.1) == is_text(k1.1) {
                    return Rec::Miss(Miss("key"));
                }
                if bounded || heap_spill_serves() {
                    GROUP_CAP
                } else {
                    return Rec::Refuse(RefuseCause::Heap(HeapDetail::GroupCountUnwitnessed));
                }
            }
            _ => return Rec::Miss(Miss("key")),
        };
    }

    // --- Agg targetlist: key echo + fold legs --------------------------------
    let mut legs: Vec<FoldLeg> = Vec::new();
    let mut renders: Vec<Render> = Vec::new();
    // OutSlot per tlist entry; grouped answers are [key, legs...].
    let mut out: Vec<OutSlot> = Vec::new();
    // Per-output sort admission: F64 answer cells (min/max over float
    // faces) have no total order in the answer comparator — a Sort over
    // one routes incumbent.
    let mut sortable: Vec<bool> = Vec::new();
    let leg_base = gkeys.len();
    for n in agg.plan.targetlist.iter() {
        let Some(te) = n.as_target_entry() else { return Rec::Miss(Miss("proj")) };
        if te.resjunk {
            return Rec::Miss(Miss("proj"));
        }
        if te.expr.node_tag() == NodeTag::T_Aggref {
            let Some(ar) = te.expr.as_aggref() else { return Rec::Miss(Miss("agg")) };
            let rl = match recognize_aggref(ar, scanrelid, &tls, fp, estate.es_query_cxt, None) {
                Ok(rl) => rl,
                // Census-ticked by the constructor; rung not landed.
                Err(r) => {
                    let _ = r.cause();
                    // [toastscan] an aggregate folding THROUGH a
                    // function over a varlena lane (`max(length(t))`)
                    // is the unlanded detoast/varlena-expression rung —
                    // typed heap-toast, never the generic agg miss (the
                    // parity SILENT-GAP, 2026-08-20).
                    if varlena_func_ref(te.expr, scanrelid, &schema, Some(scan_tl)) {
                        return Rec::Miss(Miss("heap-toast"));
                    }
                    return Rec::Miss(Miss("agg"));
                }
            };
            // [aggqual] A FILTER-qualified leg is recognized by the shared
            // seam (the columnar families serve it), but the heap fold
            // plane carries no per-leg filter (FoldLeg has none) — serving
            // here would silently DROP the qualifier and fold every row
            // (the aggregates:s351 wrong-answer, matrix identity gate
            // 2026-08-20). Rung not landed: the incumbent serves, censused.
            if rl.filter.is_some() {
                return Rec::Miss(Miss("agg-filter"));
            }
            let (op, in_col) = match &rl.agg {
                AAgg::CountStar => (FoldOp::CountStar, None),
                AAgg::Sum { e: AValExpr::Col(c) } => (FoldOp::Sum, Some(*c)),
                AAgg::Avg { e: AValExpr::Col(c) } => (FoldOp::Avg, Some(*c)),
                AAgg::Min { e: AValExpr::Col(c) } => (FoldOp::Min, Some(*c)),
                AAgg::Max { e: AValExpr::Col(c) } => (FoldOp::Max, Some(*c)),
                // [toastscan] recognized-vocabulary legs the heap fold
                // plane can't carry: a varlena-derived leg types
                // heap-toast (see varlena_func_ref); the rest stay agg.
                _ if varlena_func_ref(te.expr, scanrelid, &schema, Some(scan_tl)) => {
                    return Rec::Miss(Miss("heap-toast"));
                }
                _ => return Rec::Miss(Miss("agg")),
            };
            let (out_ty, avg_exact) = match in_col {
                None => (TypMeta::INT8, false),
                Some(c) => {
                    if !word_ok(c) {
                        // [toastscan] a varlena fold leg (`max(t)`) is
                        // the unlanded detoast rung — typed heap-toast,
                        // never the generic type miss.
                        if col(c).is_some_and(|cm| {
                            matches!(Face::of_class(cm.class), Face::Varlena)
                        }) {
                            return Rec::Miss(Miss("heap-toast"));
                        }
                        return Rec::Miss(Miss("type"));
                    }
                    let cm = col(c).expect("word_ok checked");
                    let out_ty = match op {
                        FoldOp::Sum | FoldOp::Avg => TypMeta::NUMERIC,
                        _ => cm.typ,
                    };
                    (out_ty, op == FoldOp::Avg && cm.typ.width == 8)
                }
            };
            out.push(OutSlot { src: OutSrc::Col(leg_base + legs.len()), render: rl.render });
            sortable.push(match (op, in_col) {
                // CountStar/Sum/Avg answers are I64/I128/Ratio — total
                // orders in cmp_rows. Min/Max echo the input face: float
                // faces land F64 cells (unsortable there).
                (FoldOp::Min | FoldOp::Max, Some(c)) => {
                    let cm = col(c).expect("word_ok checked");
                    !matches!(Face::of_class(cm.class), Face::F32 | Face::F64)
                }
                _ => true,
            });
            legs.push(FoldLeg { op, col: in_col, out: out_ty, avg_exact });
            renders.push(rl.render);
        } else if te.expr.node_tag() == NodeTag::T_Var {
            // Key echo: the OUTER var must name a group key position.
            let v = te.expr.as_var();
            let echo = v.and_then(|v| {
                (v.varno == ::types_nodes::primnodes::OUTER_VAR && v.varlevelsup == 0)
                    .then(|| {
                        gkeys
                            .iter()
                            .position(|&(_, _, gpos)| i32::from(v.varattno) == i32::from(gpos))
                    })
                    .flatten()
            });
            let Some(j) = echo else { return Rec::Miss(Miss("proj")) };
            out.push(OutSlot { src: OutSrc::Col(j), render: key_render(gkeys[j].1) });
            sortable.push(true);
        } else {
            return Rec::Miss(Miss("proj"));
        }
    }
    let mut distinct = false;
    if legs.is_empty() {
        if gkeys.is_empty() {
            return Rec::Miss(Miss("agg"));
        }
        // Hidden-count leg: no OutSlot references it (b3 law port).
        legs.push(FoldLeg { op: FoldOp::CountStar, col: None, out: TypMeta::INT8, avg_exact: false });
        distinct = true;
    }

    // --- WHERE conjuncts: int cmp terms + [rung 3] text var terms ------------
    let (terms, var_terms) = match lower_quals(&scan.scan.plan.qual, scanrelid, &schema, estate, fp)
    {
        Rec::Ok(t) => t,
        Rec::Miss(m) => return Rec::Miss(m),
        Rec::Refuse(c) => return Rec::Refuse(c),
        Rec::Refused(r) => return Rec::Refused(r),
    };

    // --- Sort above (grouped only, rung 2): any sortable output columns,
    // either direction, explicit NULLS placement — lowered to an answer-
    // boundary permutation (`answer::cmp_rows`), closed with the implicit
    // (key ASC, NULLS LAST) tie-break for a deterministic emission.
    let mut order: Vec<TopKKey> = Vec::new();
    if let Some(sn) = sort_node {
        if gkeys.is_empty() {
            return Rec::Miss(Miss("sort"));
        }
        let srt = sn.as_sort().expect("checked");
        if !identity_tlist(&srt.plan.targetlist) {
            return Rec::Miss(Miss("sort"));
        }
        let ncols = srt.numCols as usize;
        if ncols == 0
            || srt.sortColIdx.len() < ncols
            || srt.sortOperators.len() < ncols
            || srt.nullsFirst.len() < ncols
        {
            return Rec::Miss(Miss("sort"));
        }
        for j in 0..ncols {
            let pos = srt.sortColIdx[j];
            let Some(slot) = (pos > 0).then(|| out.get(pos as usize - 1)).flatten() else {
                return Rec::Miss(Miss("sort"));
            };
            if !sortable[pos as usize - 1] {
                return Rec::Miss(Miss("sort"));
            }
            let OutSrc::Col(c) = slot.src else { return Rec::Miss(Miss("sort")) };
            // [rung 3] a text sort key orders by the engine's byte law —
            // C collation only (an ORDER BY ... COLLATE override changes
            // the plan's sort collation and the emitted order with it).
            if slot.render == Render::Text {
                let coll = srt.collations.get(j).copied().unwrap_or(0);
                if coll != COLLATION_C {
                    return Rec::Refuse(RefuseCause::NonCCollation { coll_oid: coll });
                }
            }
            let Ok(proc_oid) = ::lsyscache::get_opcode(srt.sortOperators[j]) else {
                return Rec::Miss(Miss("sort"));
            };
            let Some(desc) = sortproc_dir(proc_oid) else {
                return Rec::Miss(Miss("sort"));
            };
            order.push(TopKKey { col: c as u32, desc, nulls_first: srt.nullsFirst[j], lo: None, trim: false });
        }
        // Deterministic close: the group keys ASC NULLS LAST (a legal
        // refinement of any requested order; ties never float).
        for j in 0..gkeys.len() {
            order.push(TopKKey { col: j as u32, desc: false, nulls_first: false, lo: None, trim: false });
        }
    }

    let group = (!gkeys.is_empty()).then(|| {
        let (a0, _, _) = gkeys[0];
        GroupSpec {
            col: a0,
            out: col(a0).expect("key checked").typ,
            witness_cap,
            second: (gkeys.len() == 2).then(|| {
                let (a1, _, _) = gkeys[1];
                GroupKeyPart { col: a1, out: col(a1).expect("key checked").typ }
            }),
        }
    });
    debug_assert!(group.as_ref().is_none_or(|g| g.witness_cap <= GROUP_CAP));
    Rec::Ok(HeapGoal {
        top_sort: sort_node.is_some(),
        top_limit,
        order,
        skip,
        take,
        spec: FaceFoldSpec { terms, var_terms, legs, group },
        out,
        distinct,
        topn: false,
    })
}

/// [b1] Bare top-n: Limit(Sort(SeqScan)), plain word-int outputs.
fn recognize_topn(
    estate: &EStateData<'_>,
    scan_node: Node<'_>,
    sort_node: Option<Node<'_>>,
    lim_count: Option<i64>,
    skip: usize,
    take: usize,
    fp: u64,
) -> Rec<HeapGoal> {
    let Some(sn) = sort_node else { return Rec::Miss(Miss("node")) };
    if lim_count.is_none() || (skip as u64).saturating_add(take as u64) > TOPN_CAP {
        return Rec::Miss(Miss("topn"));
    }
    let Some(scan) = scan_node.as_seq_scan() else { return Rec::Miss(Miss("node")) };
    if !scan.scan.plan.initPlan.is_nil()
        || scan.scan.plan.lefttree.is_some()
        || scan.scan.plan.righttree.is_some()
    {
        return Rec::Miss(Miss("node"));
    }
    let scanrelid = scan.scan.scanrelid;
    let Some(rel) = estate
        .es_relations
        .get(scanrelid as usize - 1)
        .and_then(|r| r.as_ref())
        .filter(|rel| ::tableam::TableAm::of(rel) == Some(::tableam_vocab::TableAm::Heap))
    else {
        return Rec::Miss(Miss("node"));
    };
    let schema = match catalog_schema(rel) {
        Ok(s) => s,
        Err(_) => return Rec::Miss(Miss("type")),
    };
    let col = |attno: u32| schema.iter().find(|c| c.attno == attno);
    let word_ok = |attno: u32| -> bool {
        col(attno).is_some_and(|c| c.typ.byval && Face::of_class(c.class).word_foldable())
    };

    let mut legs: Vec<FoldLeg> = Vec::new();
    let mut out: Vec<OutSlot> = Vec::new();
    for n in scan.scan.plan.targetlist.iter() {
        let Some(te) = n.as_target_entry() else { return Rec::Miss(Miss("proj")) };
        if te.resjunk || te.expr.node_tag() != NodeTag::T_Var {
            return Rec::Miss(Miss("proj"));
        }
        let Some((attno, typid)) = resolve_var(te.expr, &[], scanrelid) else {
            return Rec::Miss(Miss("proj"));
        };
        if !matches!(typid, INT2OID | INT4OID | INT8OID | DATEOID | TSOID | TSTZOID)
            || !word_ok(attno)
        {
            return Rec::Miss(Miss("type"));
        }
        let cm = col(attno).expect("word_ok checked");
        out.push(OutSlot { src: OutSrc::Col(legs.len()), render: key_render(typid) });
        legs.push(FoldLeg { op: FoldOp::Min, col: Some(attno), out: cm.typ, avg_exact: false });
    }
    if legs.is_empty() {
        return Rec::Miss(Miss("proj"));
    }

    let (terms, var_terms) = match lower_quals(&scan.scan.plan.qual, scanrelid, &schema, estate, fp)
    {
        Rec::Ok(t) => t,
        Rec::Miss(m) => return Rec::Miss(m),
        Rec::Refuse(c) => return Rec::Refuse(c),
        Rec::Refused(r) => return Rec::Refused(r),
    };

    let srt = sn.as_sort().expect("peeled by the caller");
    if !identity_tlist(&srt.plan.targetlist) {
        return Rec::Miss(Miss("sort"));
    }
    let ncols = srt.numCols as usize;
    if ncols == 0
        || srt.sortColIdx.len() < ncols
        || srt.sortOperators.len() < ncols
        || srt.nullsFirst.len() < ncols
    {
        return Rec::Miss(Miss("sort"));
    }
    let mut order: Vec<TopKKey> = Vec::new();
    for j in 0..ncols {
        let pos = srt.sortColIdx[j];
        if pos <= 0 || pos as usize > out.len() {
            return Rec::Miss(Miss("sort"));
        }
        let Ok(proc_oid) = ::lsyscache::get_opcode(srt.sortOperators[j]) else {
            return Rec::Miss(Miss("sort"));
        };
        let Some(desc) = sortproc_dir(proc_oid) else {
            return Rec::Miss(Miss("sort"));
        };
        order.push(TopKKey { col: pos as u32 - 1, desc, nulls_first: srt.nullsFirst[j], lo: None, trim: false });
    }
    // Total-order close: rows equal under it are byte-identical.
    for c2 in 0..out.len() {
        order.push(TopKKey { col: c2 as u32, desc: false, nulls_first: false, lo: None, trim: false });
    }

    Rec::Ok(HeapGoal {
        top_sort: true,
        top_limit: true,
        order,
        skip,
        take,
        spec: FaceFoldSpec { terms, var_terms, legs, group: None },
        out,
        distinct: false,
        topn: true,
    })
}

// ---------------------------------------------------------------------------
// Varlena payload extraction (text byte lanes, rung 3)
// ---------------------------------------------------------------------------

/// Append one varlena datum's PAYLOAD bytes to `arena`, returning the
/// appended length. Inline images only: short (1B) and 4B-uncompressed
/// payloads copy verbatim; 4B inline-compressed pglz images decompress
/// straight into the arena (pure function of the image bytes — no
/// backend state, worker-legal). Out-of-line TOAST pointers refuse
/// ("text-external": detoast crosses the toast table — backend state,
/// an unlanded rung) as does a non-pglz method ("text-compression";
/// this build carries no LZ4, matching C without USE_LZ4).
///
/// # Safety
/// `p` points at a live varlena image (pinned page or staged page copy)
/// and `avail` is the number of bytes readable from `p` to the end of the
/// containing tuple image (`t_data + t_len`, or the staged page-copy
/// tuple extent). Nothing past `p.add(avail)` is dereferenced.
unsafe fn push_varlena_payload(
    p: *const u8,
    avail: usize,
    arena: &mut Vec<u8>,
) -> Result<u32, &'static str> {
    use ::types_tuple::varatt as va;
    // [idx 5] The varlena header is attacker-influenceable page/catalog
    // bytes (a restored basebackup, storage corruption, or a raw-page
    // writer). Every declared length below is validated against `avail`
    // BEFORE any slice is built, mirroring the deform-bound fix in
    // types_tuple (`varsize_bounded`): the header must fit, the declared
    // size must cover its own header (no unchecked `l - VARHDRSZ`
    // underflow), and the full datum must lie within the containing tuple
    // — otherwise a typed `text-corrupt` (ERRCODE_DATA_CORRUPTED) is
    // raised instead of a massive out-of-bounds read.
    if avail == 0 {
        return Err("text-corrupt");
    }
    if va::varatt_is_1b_e(p) {
        return Err("text-external");
    }
    if va::varatt_is_1b(p) {
        // 1B header (VARHDRSZ_SHORT == 1): `avail >= 1` guarantees the
        // header byte is readable; `varsize_1b` is the total (<= 0x7F).
        let l = va::varsize_1b(p);
        if l < va::VARHDRSZ_SHORT || l > avail {
            return Err("text-corrupt");
        }
        let b = std::slice::from_raw_parts(p.add(va::VARHDRSZ_SHORT), l - va::VARHDRSZ_SHORT);
        arena.extend_from_slice(b);
        return Ok(b.len() as u32);
    }
    if va::varatt_is_4b_u(p) {
        if avail < va::VARHDRSZ {
            return Err("text-corrupt");
        }
        let l = va::varsize_4b(p);
        if l < va::VARHDRSZ || l > avail {
            return Err("text-corrupt");
        }
        let b = std::slice::from_raw_parts(p.add(va::VARHDRSZ), l - va::VARHDRSZ);
        arena.extend_from_slice(b);
        return Ok(b.len() as u32);
    }
    // 4B-C inline compressed (varattrib_4b va_compressed): va_tcinfo =
    // raw payload size | method << 30 (toast_compression.c's law); the
    // compressed stream follows the 8-byte header (4B varlena header +
    // 4B va_tcinfo). Bound the whole datum before reading either.
    if avail < 8 {
        return Err("text-corrupt");
    }
    let vl = va::varsize_4b(p);
    if vl < 8 || vl > avail {
        return Err("text-corrupt");
    }
    let tcinfo = p.add(4).cast::<u32>().read_unaligned();
    let rawsize = (tcinfo & ((1u32 << 30) - 1)) as usize;
    if tcinfo >> 30 != 0 {
        return Err("text-compression");
    }
    let src = std::slice::from_raw_parts(p.add(8), vl - 8);
    arena.reserve(rawsize);
    let base = arena.len();
    let n = ::pglz::pglz_decompress(src, &mut arena.spare_capacity_mut()[..rawsize], true)
        .ok_or("text-corrupt")?;
    // SAFETY: `n` bytes of the spare capacity were initialized.
    arena.set_len(base + n);
    Ok(n as u32)
}

/// Detoast one out-of-line datum into `arena` (leader-only).
///
/// # Safety
/// `p` points at a live varlena image readable through its own header.
unsafe fn detoast_external(
    mcx: ::mcx::Mcx<'_>,
    p: *const u8,
    arena: &mut Vec<u8>,
) -> Result<u32, Box<PgError>> {
    use ::types_tuple::varatt as va;
    let img = std::slice::from_raw_parts(p, va::varsize_any(p));
    let flat = ::detoast::detoast_attr(mcx, img)?;
    let b = &flat[va::VARHDRSZ..];
    arena.extend_from_slice(b);
    Ok(b.len() as u32)
}

// ---------------------------------------------------------------------------
// The face — serial arm (bare count(*), and the atthasmissing fallback:
// missing defaults live on the descriptor, which the detached pack
// deform deliberately lacks)
// ---------------------------------------------------------------------------

pub(super) struct HeapFace<'a, 'mcx> {
    pub(super) scan: &'a mut ::heapam::HeapScanDescData<'mcx>,
    schema: &'a [ColMeta],
    nblocks: u64,
    /// [b1] detoast-at-fill (lever-armed; this face is leader-only).
    mcx: ::mcx::Mcx<'mcx>,
    detoast: bool,
    n_units: usize,
    // Deform scratch (whole-descriptor width; truncate-refill).
    datums: Vec<::datum::Datum>,
    isnull: Vec<bool>,
    /// Varlena payload scratch (byte lanes; truncate-refill).
    vscratch: Vec<u8>,
    /// A real error raised inside fill (I/O, interrupt) — the FaceError
    /// channel carries no payload, so the PgError parks here.
    pub(super) err: Option<Box<PgError>>,
}

impl<'a, 'mcx> HeapFace<'a, 'mcx> {
    pub(super) fn new(
        scan: &'a mut ::heapam::HeapScanDescData<'mcx>,
        schema: &'a [ColMeta],
        nblocks: u64,
        mcx: ::mcx::Mcx<'mcx>,
        detoast: bool,
    ) -> HeapFace<'a, 'mcx> {
        let natts = scan.rs_base.rs_rd.rd_att.compact_attrs.len();
        HeapFace {
            scan,
            schema,
            nblocks,
            mcx,
            detoast,
            n_units: nblocks.div_ceil(GRANULE_BLOCKS) as usize,
            datums: vec![::datum::Datum::null(); natts],
            isnull: vec![false; natts],
            vscratch: Vec::new(),
            err: None,
        }
    }

    fn meta(&self, attno: u32) -> &ColMeta {
        self.schema
            .iter()
            .find(|c| c.attno == attno)
            .expect("recognized column exists in the catalog schema")
    }
}

impl ScanFace for HeapFace<'_, '_> {
    fn n_units(&self) -> usize {
        self.n_units
    }
    fn face(&self, attno: u32) -> Face {
        Face::of_class(self.meta(attno).class)
    }
    fn null_free(&self, attno: u32) -> bool {
        // Catalog constraint = the only witness sound under any snapshot.
        let _ = attno;
        false
    }
    fn rows_total(&self) -> Option<u64> {
        // reltuples is an estimate; heap has no sound rows witness
        // without a scan (MetadataAnswer refuses on heap — §3).
        None
    }
    fn fill(
        &mut self,
        unit: usize,
        cols: &[u32],
        out: &mut FaceFill,
    ) -> Result<(), FaceError> {
        let b0 = unit as u64 * GRANULE_BLOCKS;
        let b1 = (b0 + GRANULE_BLOCKS).min(self.nblocks);
        let io_err = |e: Box<PgError>, err: &mut Option<Box<PgError>>| {
            *err = Some(e);
            FaceError { attno: 0, what: "io" }
        };
        if let Err(e) = ::heapam::heap_set_block_range(self.scan, b0, b1) {
            return Err(io_err(e, &mut self.err));
        }
        // Byte lanes (text columns) fill payload slices; word lanes fill
        // datum words (the v1 law).
        let is_bytes: Vec<bool> = cols
            .iter()
            .map(|&a| matches!(Face::of_class(self.meta(a).class), Face::Varlena))
            .collect();
        let mut rows: u32 = 0;
        let mut vfail: Option<FaceError> = None;
        loop {
            let n = match ::heapam::heap_getnextpagebatch(self.scan) {
                Ok(n) => n,
                Err(e) => return Err(io_err(e, &mut self.err)),
            };
            if n == 0 {
                break;
            }
            rows += n;
            // Bare count(*): the visible-row count IS the answer — no
            // deform (the pagemode collect already ran).
            if cols.is_empty() {
                continue;
            }
            let (mcx, det) = (self.mcx, self.detoast);
            let HeapFace { scan, schema: _, datums, isnull, vscratch, err, .. } = self;
            let tupdesc = scan.rs_base.rs_rd.rd_att.clone();
            // Column-pruned deform: the offset walk stops at the last
            // referenced attno.
            let want = cols.iter().map(|&a| a as usize).max().unwrap_or(0);
            ::heapam::heap_page_visible_tuples(scan, |_, tup| {
                ::types_tuple::heap_deform_tuple_prefix(tup, &tupdesc, datums, isnull, want);
                for (ci, &attno) in cols.iter().enumerate() {
                    let i = attno as usize - 1;
                    if is_bytes[ci] {
                        if isnull[i] {
                            out.push_bytes(ci, None);
                        } else {
                            vscratch.clear();
                            let p = datums[i].as_usize() as *const u8;
                            // [idx 5] Bound the header read by the tuple's
                            // real extent (t_data..t_data+t_len): the datum
                            // points inside this tuple, so bytes to the
                            // tuple's end cap any trusted-header read.
                            let tend = tup.header_ptr() as usize + tup.t_len as usize;
                            let avail = tend.saturating_sub(p as usize);
                            // SAFETY: non-null varlena datum deformed off
                            // a page pinned for the duration of the fill;
                            // `avail` stops the read at the tuple's end.
                            match unsafe { push_varlena_payload(p, avail, vscratch) } {
                                Ok(_) => out.push_bytes(ci, Some(vscratch)),
                                Err("text-external") if det => {
                                    match unsafe { detoast_external(mcx, p, vscratch) } {
                                        Ok(_) => out.push_bytes(ci, Some(vscratch)),
                                        Err(e) => {
                                            if vfail.is_none() {
                                                *err = Some(e);
                                                vfail =
                                                    Some(FaceError { attno, what: "io" });
                                            }
                                            out.push_bytes(ci, None);
                                        }
                                    }
                                }
                                Err(what) => {
                                    if vfail.is_none() {
                                        vfail = Some(FaceError { attno, what });
                                    }
                                    out.push_bytes(ci, None);
                                }
                            }
                        }
                    } else {
                        out.push(ci, datums[i].as_u64(), isnull[i]);
                    }
                }
            });
            if vfail.is_some() {
                break;
            }
        }
        if let Some(e) = vfail {
            return Err(e);
        }
        out.seal(rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The pack arm (the 1.05x flip levers): the leader STAGES detached page
// images per granule (pin + visibility + one BLCKSZ memcpy — no deform
// under the pin), and deform+filter+fold run over the copies — inline at
// width 1, on the sqe pool otherwise (`Pool::run_feed`: the leader keeps
// staging while workers consume). Late materialization and the
// scatter-copy-free static-offset deform live in the pack consumer
// (face_fold::run_pack_fold + HeapDeformer below).
// Scan rung = pin-handoff drive: leader-pinned page addresses, no copy
// (heap-parscan.md). Pins stay on the leader's ledger until re-stage/settle.
// ---------------------------------------------------------------------------

const BLCKSZ: usize = ::types_core::BLCKSZ;
const BLCKSZ_W: usize = BLCKSZ / 8;

const PIN_GRANULE_BLOCKS: u64 = 32;
const PIN_RING_SLACK_BLOCKS: u64 = 64;

struct PackPageMeta {
    /// Granule row ordinal of this page's first visible row.
    base: u32,
    nvis: u32,
    vis_off: u32,
}

/// One staged granule: page images (u64-backed for MAXALIGN) or pinned
/// page addresses (`page_ptrs` = the consumer's view in both drives),
/// visible offsets, page directory; recycled via the feed's free list.
#[derive(Default)]
struct HeapPack {
    id: usize,
    rows: u32,
    pages: Vec<u64>,
    page_ptrs: Vec<usize>,
    meta: Vec<PackPageMeta>,
    vis: Vec<u16>,
}

/// The thread-portable half of a CompactAttribute (the real one carries
/// a `Cell` offset cache and is !Sync; each worker rebuilds its own).
#[derive(Clone, Copy)]
struct AttSeed {
    attlen: i16,
    attbyval: bool,
    attispackable: bool,
    atthasmissing: bool,
    attisdropped: bool,
    attgenerated: bool,
    attnullability: i8,
    attalignby: u8,
}

impl AttSeed {
    fn of(a: &::types_tuple::CompactAttribute) -> AttSeed {
        AttSeed {
            attlen: a.attlen,
            attbyval: a.attbyval,
            attispackable: a.attispackable,
            atthasmissing: a.atthasmissing,
            attisdropped: a.attisdropped,
            attgenerated: a.attgenerated,
            attnullability: a.attnullability,
            attalignby: a.attalignby,
        }
    }
    fn rebuild(&self) -> ::types_tuple::CompactAttribute {
        ::types_tuple::CompactAttribute {
            attcacheoff: std::cell::Cell::new(-1),
            attlen: self.attlen,
            attbyval: self.attbyval,
            attispackable: self.attispackable,
            atthasmissing: self.atthasmissing,
            attisdropped: self.attisdropped,
            attgenerated: self.attgenerated,
            attnullability: self.attnullability,
            attalignby: self.attalignby,
        }
    }
}

/// Statement-constant deform facts, shared read-only across workers.
struct DeformSpec {
    /// Referenced-attno prefix length (the column-pruned walk bound).
    want: usize,
    /// Leading attributes with static offsets (the fixed-width chain up
    /// to the first varlena/dropped attribute).
    fixed: usize,
    offs: Vec<u32>,
    seeds: Vec<AttSeed>,
    relid: ::types_core::Oid,
    /// Deform-arm identity harness lever: force the scalar walk.
    force_scalar: bool,
}

struct HeapPackSource<'a, 'mcx> {
    scan: &'a mut ::heapam::HeapScanDescData<'mcx>,
    schema: &'a [ColMeta],
    nblocks: u64,
    granule: u64,
    n_units: usize,
    spec: std::sync::Arc<DeformSpec>,
    err: Option<Box<PgError>>,
    pins: Option<Vec<Vec<::bufmgr_seams::BufferPin>>>,
    next_id: std::cell::Cell<usize>,
}

impl<'a, 'mcx> HeapPackSource<'a, 'mcx> {
    fn new(
        scan: &'a mut ::heapam::HeapScanDescData<'mcx>,
        schema: &'a [ColMeta],
        nblocks: u64,
        want: usize,
        pin_window: Option<u64>,
    ) -> HeapPackSource<'a, 'mcx> {
        let pinned = pin_window.is_some();
        let granule = if pinned { PIN_GRANULE_BLOCKS } else { GRANULE_BLOCKS };
        if let Some(w) = pin_window {
            if scan.rs_strategy.is_some() {
                let kb = ((w + PIN_RING_SLACK_BLOCKS) * (BLCKSZ as u64 / 1024)) as i32;
                scan.rs_strategy = ::bufmgr_seams::get_access_strategy_with_size::call(
                    ::types_storage::buf::BufferAccessStrategyType::BasBulkread,
                    kb,
                );
            }
        }
        let atts = &scan.rs_base.rs_rd.rd_att.compact_attrs;
        let seeds: Vec<AttSeed> = atts[..want.min(atts.len())].iter().map(AttSeed::of).collect();
        // Static offset chain (the SoaDeformPlan law): fixed-width
        // attributes only; the chain stops at the first varlena/dropped.
        let mut offs = Vec::with_capacity(seeds.len());
        let mut off = 0usize;
        for s in &seeds {
            if s.attlen <= 0 || s.attisdropped {
                break;
            }
            off = ::types_tuple::att_nominal_alignby(off, s.attalignby);
            offs.push(off as u32);
            off += s.attlen as usize;
        }
        let fixed = offs.len();
        let spec = DeformSpec {
            want,
            fixed,
            offs,
            seeds,
            relid: scan.rs_base.rs_rd.rd_id,
            force_scalar: force_scalar_deform(),
        };
        HeapPackSource {
            scan,
            schema,
            nblocks,
            granule,
            n_units: nblocks.div_ceil(granule) as usize,
            spec: std::sync::Arc::new(spec),
            err: None,
            pins: pinned.then(Vec::new),
            next_id: std::cell::Cell::new(0),
        }
    }

    /// The pack deform NULL-fills short tuples; a missing-default column
    /// must take the descriptor-bearing serial face instead.
    fn lawful(&self) -> bool {
        self.spec.seeds.iter().all(|s| !s.atthasmissing)
    }

    fn pinned(&self) -> bool {
        self.pins.is_some()
    }

    fn release_pack_pins(&mut self, id: usize) {
        if let Some(p) = self.pins.as_mut().and_then(|l| l.get_mut(id)) {
            for pin in p.drain(..) {
                pin.release();
            }
        }
    }

    fn release_pins(&mut self) {
        if let Some(l) = self.pins.as_mut() {
            for p in l.iter_mut() {
                for pin in p.drain(..) {
                    pin.release();
                }
            }
        }
    }
}

impl Drop for HeapPackSource<'_, '_> {
    fn drop(&mut self) {
        self.release_pins();
    }
}

impl PackSource for HeapPackSource<'_, '_> {
    type Pack = HeapPack;
    type Deformer = HeapDeformer;

    fn n_units(&self) -> usize {
        self.n_units
    }
    fn face(&self, attno: u32) -> Face {
        let m = self
            .schema
            .iter()
            .find(|c| c.attno == attno)
            .expect("recognized column exists in the catalog schema");
        Face::of_class(m.class)
    }
    fn new_pack(&self) -> HeapPack {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        HeapPack { id, ..HeapPack::default() }
    }
    fn deformer(&self) -> HeapDeformer {
        let spec = std::sync::Arc::clone(&self.spec);
        let atts: Vec<::types_tuple::CompactAttribute> =
            spec.seeds.iter().map(AttSeed::rebuild).collect();
        HeapDeformer {
            atts,
            datums: vec![::datum::Datum::null(); spec.want],
            isnull: vec![false; spec.want],
            data_ptrs: Vec::new(),
            hdrs: Vec::new(),
            spec,
        }
    }
    fn stage(&mut self, unit: usize, pack: &mut HeapPack) -> Result<(), FaceError> {
        let b0 = unit as u64 * self.granule;
        let b1 = (b0 + self.granule).min(self.nblocks);
        pack.rows = 0;
        pack.pages.clear();
        pack.page_ptrs.clear();
        pack.meta.clear();
        pack.vis.clear();
        self.release_pack_pins(pack.id);
        let io_err = |e: Box<PgError>, err: &mut Option<Box<PgError>>| {
            *err = Some(e);
            FaceError { attno: 0, what: "io" }
        };
        if let Err(e) = ::heapam::heap_set_block_range(self.scan, b0, b1) {
            return Err(io_err(e, &mut self.err));
        }
        let pinned = self.pinned();
        loop {
            let n = match ::heapam::heap_getnextpagebatch(self.scan) {
                Ok(n) => n,
                Err(e) => return Err(io_err(e, &mut self.err)),
            };
            if n == 0 {
                break;
            }
            let pi = pack.meta.len();
            let vis_off = pack.vis.len();
            pack.vis.resize(vis_off + n as usize, 0);
            if pinned {
                let (ptr, pin) =
                    ::heapam::heap_pin_staged_page(self.scan, &mut pack.vis[vis_off..]);
                let ledger = self.pins.as_mut().expect("pin drive");
                if ledger.len() <= pack.id {
                    ledger.resize_with(pack.id + 1, Vec::new);
                }
                ledger[pack.id].push(pin);
                pack.page_ptrs.push(ptr as usize);
            } else {
                pack.pages.resize((pi + 1) * BLCKSZ_W, 0);
                // SAFETY: the fresh BLCKSZ_W u64 tail viewed as bytes.
                let bytes: &mut [u8] = unsafe {
                    std::slice::from_raw_parts_mut(
                        pack.pages[pi * BLCKSZ_W..].as_mut_ptr().cast::<u8>(),
                        BLCKSZ,
                    )
                };
                ::heapam::heap_copy_staged_page(self.scan, bytes, &mut pack.vis[vis_off..]);
            }
            pack.meta.push(PackPageMeta { base: pack.rows, nvis: n, vis_off: vis_off as u32 });
            pack.rows += n;
        }
        if !pinned {
            let base = pack.pages.as_ptr() as usize;
            pack.page_ptrs.extend((0..pack.meta.len()).map(|pi| base + pi * BLCKSZ));
        }
        Ok(())
    }
}

/// Per-worker deformer over detached page images: static-offset
/// column-major fetches straight into the granule lanes (no SoA batch,
/// no scatter copy), the scalar prefix walk for null-bearing/short
/// tuples and past-varlena columns, and survivor-only writes at late
/// materialization pass 2.
struct HeapDeformer {
    spec: std::sync::Arc<DeformSpec>,
    atts: Vec<::types_tuple::CompactAttribute>,
    datums: Vec<::datum::Datum>,
    isnull: Vec<bool>,
    /// Per-page tuple data pointers (usize: the vec must stay Send).
    data_ptrs: Vec<usize>,
    hdrs: Vec<(usize, u32)>,
}

impl PackDeform<HeapPack> for HeapDeformer {
    fn rows(&self, pack: &HeapPack) -> u32 {
        pack.rows
    }

    fn deform(
        &mut self,
        pack: &HeapPack,
        cols: &[(usize, u32)],
        sel: Option<&[u16]>,
        out: &mut FaceFill,
    ) -> Result<(), FaceError> {
        if cols.is_empty() {
            return Ok(());
        }
        let spec = std::sync::Arc::clone(&self.spec);
        let spec = &*spec;
        let (relid, want) = (spec.relid, spec.want);
        // Byte lanes (text columns): always the scalar prefix walk (a
        // varlena column never joins the static-offset chain).
        let is_bytes: Vec<bool> =
            cols.iter().map(|&(ci, _)| out.cols[ci].is_bytes()).collect();
        let all_static =
            !spec.force_scalar && cols.iter().all(|&(_, a)| (a as usize) <= spec.fixed);
        let mut s_lo = 0usize;
        for (pi, m) in pack.meta.iter().enumerate() {
            let base = m.base as usize;
            let n = m.nvis as usize;
            // Page-local survivor window (sel is granule-sorted).
            let (ps, pe) = match sel {
                Some(sel) => {
                    while s_lo < sel.len() && (sel[s_lo] as usize) < base {
                        s_lo += 1;
                    }
                    let ps = s_lo;
                    while s_lo < sel.len() && (sel[s_lo] as usize) < base + n {
                        s_lo += 1;
                    }
                    if ps == s_lo {
                        continue;
                    }
                    (ps, s_lo)
                }
                None => (0, 0),
            };
            let page_ptr = pack.page_ptrs[pi] as *mut u8;
            // SAFETY: BLCKSZ image, pinned or copied while checked out.
            let page = unsafe {
                ::types_storage::bufpage::PageRef::from_raw(std::ptr::NonNull::new_unchecked(
                    page_ptr,
                ))
            };
            let vis = &pack.vis[m.vis_off as usize..m.vis_off as usize + n];
            self.data_ptrs.clear();
            self.hdrs.clear();
            let mut fast = all_static;
            for &lineoff in vis {
                // SAFETY: offsets came from page_collect_tuples on this
                // very image (bounds proven at collection).
                let (ptr, len) = unsafe {
                    let lpp = page.item_id_unchecked(lineoff);
                    debug_assert!(lpp.is_normal());
                    page.item_raw_unchecked(lpp)
                };
                // SAFETY: tuple image inside the pack's page image.
                let tup = unsafe {
                    ::types_tuple::HeapTupleData::from_raw_parts(
                        ptr,
                        len,
                        ::types_tuple::ItemPointerData::new(0, lineoff),
                        spec.relid,
                    )
                };
                fast &= !tup.has_nulls() && (tup.t_data().natts() as usize) >= spec.want;
                self.data_ptrs.push(tup.getstruct() as usize);
                self.hdrs.push((ptr as usize, len));
            }
            if fast {
                for &(ci, attno) in cols {
                    let a = attno as usize - 1;
                    let off = spec.offs[a] as usize;
                    let s = &spec.seeds[a];
                    let (attbyval, attlen) = (s.attbyval, s.attlen as i32);
                    let (words, vwords, _) = out.lane_mut(ci);
                    match sel {
                        None => {
                            for (k, &dp) in self.data_ptrs.iter().enumerate() {
                                // SAFETY: static offset inside a kind-0
                                // tuple's data area (fast page proof).
                                words[base + k] = unsafe {
                                    ::types_tuple::fetch_att(
                                        (dp as *const u8).add(off),
                                        attbyval,
                                        attlen,
                                    )
                                }
                                .as_u64();
                            }
                            sqe::face::set_valid_range(vwords, base, base + n);
                        }
                        Some(sel) => {
                            for &r16 in &sel[ps..pe] {
                                let r = r16 as usize;
                                let dp = self.data_ptrs[r - base];
                                // SAFETY: as above.
                                words[r] = unsafe {
                                    ::types_tuple::fetch_att(
                                        (dp as *const u8).add(off),
                                        attbyval,
                                        attlen,
                                    )
                                }
                                .as_u64();
                                vwords[r >> 6] |= 1u64 << (r & 63);
                            }
                        }
                    }
                }
                continue;
            }
            // Scalar arm: the prefix walk per row (null bitmaps, short
            // tuples NULL-fill, varlena-crossing offsets, byte lanes).
            let mut row = |k: usize,
                           this: &mut Self,
                           out: &mut FaceFill|
             -> Result<(), FaceError> {
                let (ptr, len) = this.hdrs[k];
                // SAFETY: as the gather above.
                let tup = unsafe {
                    ::types_tuple::HeapTupleData::from_raw_parts(
                        ptr as *const u8,
                        len,
                        ::types_tuple::ItemPointerData::new(0, vis[k]),
                        relid,
                    )
                };
                ::types_tuple::heap_deform_tuple_prefix_atts(
                    &tup,
                    &this.atts,
                    &mut this.datums,
                    &mut this.isnull,
                    want,
                );
                let r = base + k;
                for (pi, &(ci, attno)) in cols.iter().enumerate() {
                    let a = attno as usize - 1;
                    if is_bytes[pi] {
                        if this.isnull[a] {
                            let (_, _, _, nulls) = out.bytes_lane_mut(ci);
                            *nulls += 1;
                        } else {
                            let p = this.datums[a].as_usize() as *const u8;
                            // [idx 5] Bound the header read by this tuple's
                            // real extent within the staged page image
                            // (`ptr`..`ptr + len`, the item's t_len): the
                            // datum points inside this tuple, so bytes to
                            // its end cap any trusted-header read.
                            let tend = ptr + len as usize;
                            let avail = tend.saturating_sub(p as usize);
                            let (arena, spans, vwords, _) = out.bytes_lane_mut(ci);
                            let off = arena.len() as u32;
                            // SAFETY: non-null varlena datum inside the
                            // pack's page image (stable while the pack is
                            // checked out); `avail` stops the read at the
                            // tuple's end.
                            match unsafe { push_varlena_payload(p, avail, arena) } {
                                Ok(plen) => {
                                    spans[r] = (off, plen);
                                    vwords[r >> 6] |= 1u64 << (r & 63);
                                }
                                Err(what) => return Err(FaceError { attno, what }),
                            }
                        }
                    } else {
                        let (words, vwords, nulls) = out.lane_mut(ci);
                        if this.isnull[a] {
                            *nulls += 1;
                        } else {
                            words[r] = this.datums[a].as_u64();
                            vwords[r >> 6] |= 1u64 << (r & 63);
                        }
                    }
                }
                Ok(())
            };
            match sel {
                None => {
                    for k in 0..n {
                        row(k, self, out)?;
                    }
                }
                Some(sel) => {
                    for &r16 in &sel[ps..pe] {
                        row(r16 as usize - base, self, out)?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn force_scalar_deform() -> bool {
    use pgsync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_SQE_HEAP_FORCE_SCALAR").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Scan-rung defaults: both flip only on the measurement cell (heap-parscan.md).
const PIN_DRIVE_DEFAULT: bool = false;
const EQ_CLASS_ADMITTED: bool = false;
/// [b1] Implemented-pending-cell: each flips on its own parity cut
/// (never by fiat); until then `heap/perf-unadmitted` in production.
const DISTINCT_CLASS_ADMITTED: bool = false;
const MULTIKEY_CLASS_ADMITTED: bool = false;
const TOPN_CLASS_ADMITTED: bool = false;
/// [b1] TOAST posture: typed refusal is the DEFAULT; the pending §6.6
/// ruling flips it either way without moving code.
const DETOAST_FILL_DEFAULT: bool = false;

fn detoast_fill_enabled() -> bool {
    use pgsync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| match std::env::var("PGRUST_SQE_HEAP_DETOAST").as_deref() {
        Ok("1") | Ok("on") => true,
        Ok("0") | Ok("off") => false,
        _ => DETOAST_FILL_DEFAULT,
    })
}

fn pin_drive_enabled() -> bool {
    use pgsync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| match std::env::var("PGRUST_SQE_HEAP_PIN_DRIVE").as_deref() {
        Ok("1") | Ok("on") => true,
        Ok("0") | Ok("off") => false,
        _ => PIN_DRIVE_DEFAULT,
    })
}

fn pin_window(width: usize) -> Option<u64> {
    let w = (width as u64 + 2) * PIN_GRANULE_BLOCKS;
    let nbuf = ::init_small::globals::NBuffers().max(0) as u64;
    (w * 8 <= nbuf).then_some(w)
}

/// The heap fill's resident pool (built once per backend at first pack
/// engagement, `available_parallelism` wide; per-statement engagement
/// width is `pgrust.sqe_threads`-resolved and capped by the pool).
pub(super) fn heap_pool() -> &'static sqe::pool::Pool {
    use pgsync::OnceLock;
    static POOL: OnceLock<sqe::pool::Pool> = OnceLock::new();
    POOL.get_or_init(|| {
        let t = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        sqe::pool::Pool::new(t)
    })
}

/// Statement fill width: `PGRUST_SQE_HEAP_WIDTH` (identity-harness
/// override) else the session's resolved engine width.
fn heap_width() -> usize {
    match std::env::var("PGRUST_SQE_HEAP_WIDTH").ok().and_then(|v| v.parse::<usize>().ok()) {
        Some(w) if w >= 1 => w,
        _ => super::seam::engine_threads(),
    }
}

// ---------------------------------------------------------------------------
// The EXPLAIN census surface (heap twin of seam::statement_verdict)
// ---------------------------------------------------------------------------

/// The statement-level heap EXPLAIN verdict: served family / typed
/// refusal / incumbent miss class, from the SAME recognizer + admission
/// walk the execution arm runs (EXPLAIN-vs-exec disposition consistency).
/// Caller (seam::statement_verdict) has already established: sqe slot
/// active, NO columnar relation, `pgrust.sqe_heap` armed — so heap
/// EXPLAIN output stays byte-identical while the GUC is off (default).
/// None = no heap relation among the opened relations.
pub(crate) fn statement_verdict(
    estate: &mut EStateData<'_>,
) -> Option<(bool, String, String)> {
    heap_relname(estate)?;
    // Spill substrate registration precedes recognition (the verdict's
    // witness table reads the spill posture).
    super::seam::enable_pool_fd_arming();
    let pstmt = estate.es_plannedstmt?;
    let fp = fnv1a64(estate.es_sourceText.map(str::as_bytes).unwrap_or(b""));
    let miss = |what: &'static str| {
        super::stat::tick_engaged("heap-miss", what);
        Some((false, format!("heap-miss/{what}"), String::new()))
    };
    let refused = |cause: RefuseCause| {
        let c = cause.refuse(fp).cause();
        Some((
            false,
            c.census_key(),
            String::from_utf8_lossy(&::types_error::unpack_sqlstate(c.sqlstate())).into_owned(),
        ))
    };
    // The exec-path run-class blockers visible at EXPLAIN grain, in the
    // exec order (the remaining cadences — EPQ, cursors, SPI budgets,
    // instrumentation — are run-time facts a plain EXPLAIN never has).
    if pstmt.parallelModeNeeded {
        return miss("run-parallel");
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty())
        || pstmt.paramExecTypes.iter().next().is_some()
    {
        return miss("run-params");
    }
    // [joins] EXPLAIN mirror of the exec arm order: `maybe_run_heap`
    // tries `heapjoin::try_join` FIRST on join-topped plans, so the
    // verdict must report the join recognizer's disposition exactly as
    // the execution raises it (EXPLAIN-vs-exec disposition consistency —
    // the matrix identity gate reads this surface; its absence EXPLAINed
    // join statements as heap-miss/node while execution refused typed,
    // the 2026-08-20 14-row incumbent IDENTITY-FAIL class). A join-arm
    // miss falls through to the single-relation walk, as at exec.
    if let Some(j) = super::heapjoin::explain_verdict(estate, pstmt, fp) {
        match j {
            super::heapjoin::ExplainJoin::Verdict(engaged, detail, sqlstate) => {
                return Some((engaged, detail, sqlstate));
            }
            super::heapjoin::ExplainJoin::Miss(what) => {
                super::stat::tick_engaged("heap-miss", what);
            }
        }
    }
    match recognize(estate, pstmt, fp) {
        Rec::Ok(goal) => {
            if !shape_admitted(&goal) && !admit_all() {
                return refused(RefuseCause::Heap(HeapDetail::PerfUnadmitted));
            }
            Some((true, heap_family(&goal).to_string(), String::new()))
        }
        Rec::Refuse(cause) => refused(cause),
        // Already censused at the seam (cmp_conjunct's tick) — report
        // without re-ticking.
        Rec::Refused(r) => {
            let c = r.cause();
            Some((
                false,
                c.census_key(),
                String::from_utf8_lossy(&::types_error::unpack_sqlstate(c.sqlstate()))
                    .into_owned(),
            ))
        }
        Rec::Miss(Miss(what)) => miss(what),
    }
}

/// The 1.05x-of-the-row-engine per-shape admission predicate (Michael's
/// ruling 2026-08-18) — ONE definition serving both the execution arm
/// and the EXPLAIN verdict. See the admission table comment at the
/// execution call site.
fn shape_admitted(goal: &HeapGoal) -> bool {
    let spec = &goal.spec;
    let base =
        spec.terms.iter().all(|t| t.op != CmpOp::Eq) || (EQ_CLASS_ADMITTED && pin_drive_enabled());
    let multikey = spec.group.as_ref().is_some_and(|g| g.second.is_some());
    let class_ok = (!goal.distinct || DISTINCT_CLASS_ADMITTED)
        && (!multikey || MULTIKEY_CLASS_ADMITTED)
        && (!goal.topn || TOPN_CLASS_ADMITTED);
    base && class_ok
}

/// Census family of a recognized heap goal.
fn heap_family(goal: &HeapGoal) -> &'static str {
    if goal.topn {
        return "heap-topn";
    }
    if goal.distinct {
        return "heap-distinct";
    }
    if goal.spec.group.as_ref().is_some_and(|g| g.second.is_some()) {
        return "heap-multikey";
    }
    match (&goal.spec.group, goal.top_limit) {
        (None, _) => "heap-filteragg",
        (Some(_), true) => "heap-topkgroup",
        (Some(_), false) => "heap-hashgroup",
    }
}

// ---------------------------------------------------------------------------
// The dispatch arm
// ---------------------------------------------------------------------------

/// The statement-level heap arm. `None` = not a recognized v1 heap shape
/// (censused `heap-miss/<class>`): the caller falls through to the
/// incumbent engines. `Some(r)` = the statement was sqe's: served, or a
/// typed censused error (R1 on heap — no fallback past recognition).
pub(crate) fn maybe_run_heap<'mcx, 'd>(
    estate: &mut EStateData<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    number_tuples: u64,
    use_parallel_mode: bool,
    tup_desc: Option<Rc<::types_tuple::TupleDescData<'static>>>,
    dest: &mut DestReceiver<'d>,
) -> Option<PgResult<()>> {
    let miss = |what: &'static str| {
        super::stat::tick_witness("heap-miss", what);
        None
    };
    // [heap spill] Substrate registration + pool fd arming precede
    // recognition: the witness table reads the spill posture, and armed
    // workers may write runs (idempotent seam).
    super::seam::enable_pool_fd_arming();
    // Run-class gates. [sqe-heap-cursors] §15.10: the bounded-pull
    // cadences (cursor FETCH/MOVE, SPI tcount, Execute(max_rows),
    // plpgsql) ride the P6-4 spool — the SAME law as the columnar face.
    if estate.es_epq_active {
        return miss("run-epq");
    }
    if estate.es_lane_cursor_parked {
        return miss("run-cursor");
    }
    // Spool resume: an earlier bounded run of THIS estate already ran
    // the heap fill to completion and retained the answer plane; every
    // later pull streams its window — the fill never re-runs (its
    // parallel fold order is not a per-run determinism). Probed ahead
    // of every other gate: the answer is a fixed fact on this estate.
    if estate.es_sqe_spool.is_some() {
        return Some(super::seam::spool_resume(estate, number_tuples, dest));
    }
    let spool_on = super::seam::spool_enabled();
    if !spool_on {
        // Kill-world (PGRUST_SQE_CURSOR_SPOOL=0): the pre-§15.10 heap
        // posture verbatim — these cadences route to the incumbent.
        if estate.es_cursor_run_budget.is_some() {
            return miss("run-cursor");
        }
        if number_tuples != 0 {
            return miss("run-ntuples");
        }
        if estate.es_spi_run_budget.is_some() {
            return miss("run-spi");
        }
    }
    if use_parallel_mode || in_parallel_role() {
        return miss("run-parallel");
    }
    if estate.es_instrument != 0 {
        return miss("run-instr");
    }
    // [sqe-heap-cursors] hooked lists (a PL's variable environment) are
    // exempt exactly as at the columnar seam (`bind_params_present`):
    // a generic plan's live Param reaches the qual walk and MISSES to
    // the incumbent; a custom plan folded the true values as Consts.
    if (!estate.es_param_list_hooked
        && estate.es_param_list_info.is_some_and(|p| !p.is_empty()))
        || estate
            .es_plannedstmt
            .is_some_and(|p| p.paramExecTypes.iter().next().is_some())
    {
        return miss("run-params");
    }
    let pstmt = estate.es_plannedstmt?;
    let fp = fnv1a64(estate.es_sourceText.map(str::as_bytes).unwrap_or(b""));

    // [joins] The two-relation rung tries first (its recognizer walks
    // only join-topped plans; misses fall through to the goals below).
    // Bounded pulls skip it (join answers have no spool arm yet); the
    // walk below misses join-topped plans to the incumbent.
    if number_tuples == 0 {
        if let Some(r) =
            super::heapjoin::try_join(estate, planstate, pstmt, fp, tup_desc.as_ref(), dest)
        {
            return Some(r);
        }
    }
    let goal = match recognize(estate, pstmt, fp) {
        Rec::Ok(g) => g,
        Rec::Miss(Miss(what)) => return miss(what),
        Rec::Refuse(cause) => {
            let relname = heap_relname(estate).unwrap_or_default();
            return Some(Err(cause.refuse(fp).into_error(&relname)));
        }
        // Censused at the seam already — no second tick.
        Rec::Refused(r) => {
            let relname = heap_relname(estate).unwrap_or_default();
            return Some(Err(r.into_error(&relname)));
        }
    };
    let relname = heap_relname(estate).unwrap_or_default();
    // The 1.05x-of-the-row-engine per-shape admission gate (Michael's
    // ruling 2026-08-18): only shape classes MEASURED inside the band
    // serve; recognized shapes outside it refuse typed (never serve
    // slow — no-fallback means no escape hatch). Comparator of record
    // (heap-perf lane): the ROW ENGINE (pgrust.lane_executor=off) on its
    // ACTUAL plan — gather elected by the planner at default parallel
    // settings; lanev2 is the lane engine, retired by its own P7-2
    // coverage gate, not this bar. Admitted set (release parity table,
    // 10M-row laptop cut, scripts/sqe-heap-parity.sh; CI cluster re-cut
    // refines): count-star 0.82-0.91x, grouped-int2 0.33-0.40x,
    // minmax-date 0.61-0.63x, filtered-count 0.73-0.88x, filtered-folds
    // 0.45-0.56x — ADMIT. Rung-2 re-cut (2026-08-19, 10M-row laptop,
    // interleaved reps): topk-int2-countdesc 0.34-0.37x, topk-int4-wide
    // 0.44x, topk-int8-100k 0.57-0.62x (after the Fx hash-arm fix — the
    // SipHash entry tax alone held it at 1.25-1.37x), topk-date-avg
    // 0.27-0.34x, topk-offset 0.23-0.31x, sortdesc-nolimit 0.31-0.33x,
    // ungrouped-limit 0.73-0.77x — ALL ADMIT; count-star re-read
    // 0.92-0.96x. Rung-3 cut (2026-08-19, 10M rows, own text fixture —
    // the text column must NOT widen the legacy hb fixture, the
    // scan-bound rows are page-copy contests): like-contains 0.55-0.56x,
    // like-general 0.47-0.51x, notlike 0.50-0.53x, like+int 0.56-0.58x,
    // neempty 0.61-0.64x, topk-text 0.94-1.00x, topk-text-filtered
    // 0.91-1.02x — ALL ADMIT (the text top-k rows are band-edge and only
    // after the hashbrown entry_ref lever — the contains+get_mut double
    // probe measured 1.12-1.37x; watch them at the CI cluster re-cut).
    // The Eq-conjunct class (eq-selective) re-read
    // 1.28-1.37x — the survivor work is nil either way and the serial
    // stage (pin + visibility + page copy) loses to the near-agg-free
    // parallel row scan; REFUSED until the pool-parallel SCAN rung
    // (per-worker scan descriptors, the lanev2-morsel precedent) lands.
    // PGRUST_SQE_HEAP_ADMIT_ALL=1 is the parity/identity HARNESS lever
    // (never a production posture): it serves every recognized shape so
    // the oracle gates keep proving the full set while the fix lane
    // burns the band down.
    let admitted = shape_admitted(&goal);
    if !admitted && !admit_all() {
        return Some(Err(RefuseCause::Heap(HeapDetail::PerfUnadmitted)
            .refuse(fp)
            .into_error(&relname)));
    }
    // [sqe-heap-cursors] Scroll law, the columnar arm verbatim: top
    // REWIND|BACKWARD eflags reach the dispatch only when the portal
    // store is disarmed under a SCROLL cursor; the store is the one
    // backward server (the spool feeds it), so an sqe-owned shape
    // refuses typed at the first drive. Placed AFTER admission: shapes
    // the face does not own keep their incumbent routing.
    if spool_on
        && estate.es_top_eflags
            & (::types_slot::EXEC_FLAG_REWIND | ::types_slot::EXEC_FLAG_BACKWARD)
            != 0
    {
        return Some(Err(RefuseCause::ScrollableCursor.refuse(fp).into_error(&relname)));
    }

    // Navigate the plan state to the SeqScan (shape mirrors the plan).
    let mut node: &mut crate::procnode::PlanStateNode<'mcx> = planstate;
    if goal.top_limit {
        let crate::procnode::PlanStateNode::Limit(lim) = node else {
            return miss("state-shape");
        };
        node = &mut lim.outer;
    }
    if goal.top_sort {
        let crate::procnode::PlanStateNode::Sort(srt) = node else {
            return miss("state-shape");
        };
        node = &mut srt.outer;
    }
    let ss = if goal.topn {
        let crate::procnode::PlanStateNode::SeqScan(ss) = node else {
            return miss("state-shape");
        };
        ss
    } else {
        let crate::procnode::PlanStateNode::Agg(aps) = node else {
            return miss("state-shape");
        };
        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
            return miss("state-shape");
        };
        ss
    };
    if ss.is_parallel() {
        return miss("run-parallel");
    }
    // Opens the scan descriptor under the statement snapshot (the
    // snapshot law: one MVCC snapshot per statement, bound at scan open).
    let nblocks = match ::nodeseqscan::seq_scan_heap_block_geometry(ss, estate) {
        Ok(Some(n)) => n,
        // Empty relation (or non-heap surprise): incumbent answers it.
        Ok(None) => return miss("geometry"),
        Err(e) => return Some(Err(e)),
    };
    let Some(TableScanDesc::Heap(scan)) = ss.ss.ss_currentScanDesc.as_mut() else {
        return miss("geometry");
    };
    // Pagemode is the visibility law's carrier (rs_vistuples collection);
    // a non-MVCC snapshot cleared it — not servable.
    if scan.rs_base.rs_flags & ::tableam_vocab::SO_ALLOW_PAGEMODE == 0 {
        return miss("pagemode");
    }
    let Some(tup_desc) = tup_desc else { return miss("proj") };
    if tup_desc.natts as usize != goal.out.len() {
        return miss("proj");
    }
    let rel = ss
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("heap seq scan has a relation");
    let schema = match catalog_schema(rel) {
        Ok(s) => s,
        Err(e) => return Some(Err(e)),
    };

    // The heap cache-law config (born-RED seed: wrongly arm a cache and
    // the run MUST fail — the discipline is load-bearing, not accidental).
    let mut cfg = sqe::engine::SqeConfig::heap_v1(1);
    if std::env::var("PGRUST_SQE_HEAP_SEED_CACHE").as_deref() == Ok("1") {
        cfg.stats_cache = true;
    }

    let family: &'static str = heap_family(&goal);
    super::stat::tick_engaged(family, "A");

    // Ring witness: initscan elected the BAS_BULKREAD strategy for
    // large-relation seq scans; record its posture for the gate harness.
    super::stat::tick_witness(
        if scan.rs_strategy.is_some() { "heap-ring-armed" } else { "heap-ring-off" },
        "-",
    );

    // Referenced columns decide the drive: no deform => the serial face
    // (bare count(*) needs only the pagemode collect); deform-bearing
    // shapes ride the pack pipeline (late materialization + the
    // pool-parallel fill), unless the descriptor carries missing
    // defaults — those need the descriptor-bearing serial walk.
    let mut refcols: Vec<u32> = Vec::new();
    if let Some(g) = &goal.spec.group {
        refcols.push(g.col);
        if let Some(k2) = &g.second {
            refcols.push(k2.col);
        }
    }
    for t in &goal.spec.terms {
        if !refcols.contains(&t.col) {
            refcols.push(t.col);
        }
    }
    for t in &goal.spec.var_terms {
        if !refcols.contains(&t.col) {
            refcols.push(t.col);
        }
    }
    for l in &goal.spec.legs {
        if let Some(c) = l.col {
            if !refcols.contains(&c) {
                refcols.push(c);
            }
        }
    }
    let want = refcols.iter().map(|&a| a as usize).max().unwrap_or(0);
    let atts = &scan.rs_base.rs_rd.rd_att.compact_attrs;
    // [b1] Lever-armed byte-lane statements take the leader-serial face
    // (the toast relation is backend state — worker-illegal).
    let detoast_fill = detoast_fill_enabled()
        && refcols.iter().any(|&a| {
            schema
                .iter()
                .find(|c| c.attno == a)
                .is_some_and(|c| matches!(Face::of_class(c.class), Face::Varlena))
        });
    let pack_ok = want > 0
        && !detoast_fill
        && atts[..want.min(atts.len())].iter().all(|a| !a.atthasmissing);

    let spill0 = sqe::stencils::face_fold::spill_counters();
    let (result, io_err) = if pack_ok {
        let width = heap_width();
        // The E18 width law prices per engaged worker.
        cfg.threads = width;
        let window = (width > 1 && pin_drive_enabled())
            .then(|| pin_window(width))
            .flatten();
        super::stat::tick_witness(
            match (width > 1, window.is_some()) {
                (true, true) => "heap-fill-pinned",
                (true, false) => "heap-fill-pooled",
                (false, _) => "heap-fill-serial",
            },
            "-",
        );
        let mut src = HeapPackSource::new(scan, &schema, nblocks, want, window);
        let pool = if width > 1 { Some((heap_pool(), width)) } else { None };
        let r = catch_refusal(|| {
            if goal.topn {
                let n = goal.skip.saturating_add(goal.take);
                run_pack_topn(&mut src, &goal.spec, &goal.order, n, &cfg, pool)
            } else {
                run_pack_fold(&mut src, &goal.spec, &cfg, pool)
            }
        });
        let e = src.err.take();
        // Zero pins at claim settle, error paths included (lanev2's law).
        src.release_pins();
        ::heapam::heap_end_claim_release(&mut *src.scan);
        (r, e)
    } else {
        if detoast_fill {
            super::stat::tick_witness("heap-detoast-fill", "-");
        }
        let mut face = HeapFace::new(scan, &schema, nblocks, estate.es_query_cxt, detoast_fill);
        let mut fill = FaceFill::new();
        let r = catch_refusal(|| {
            if goal.topn {
                let n = goal.skip.saturating_add(goal.take);
                run_face_topn(&mut face, &goal.spec, &goal.order, n, &cfg, &mut fill)
            } else {
                run_face_fold(&mut face, &goal.spec, &cfg, &mut fill)
            }
        });
        let e = face.err.take();
        // Zero pins at claim settle, error paths included (lanev2's law).
        ::heapam::heap_end_claim_release(face.scan);
        (r, e)
    };

    // [heap spill] Engagement witnesses for the gate harness.
    let spill1 = sqe::stencils::face_fold::spill_counters();
    if spill1.0 > spill0.0 {
        super::stat::tick_witness("heap-spilled", "-");
    }
    if spill1.1 > spill0.1 {
        super::stat::tick_witness("heap-spill-merged", "-");
    }
    let answers = match result {
        Ok(Ok(a)) => a,
        // Typed runtime refusals off the fold drives (answer-bytes 53400,
        // spill-unavailable, spill I/O) lower onto the shell lattice.
        Err(refuse) => {
            return Some(Err(super::refusal::from_engine(&refuse)
                .refuse(fp)
                .into_error(&relname)));
        }
        Ok(Err(FaceFoldErr::CacheLaw)) => {
            return Some(Err(RefuseCause::Heap(HeapDetail::CacheLaw)
                .refuse(fp)
                .into_error(&relname)));
        }
        Ok(Err(FaceFoldErr::GroupCap { .. })) => {
            return Some(Err(RefuseCause::Heap(HeapDetail::GroupCap)
                .refuse(fp)
                .into_error(&relname)));
        }
        Ok(Err(FaceFoldErr::RowOrdinalOverflow { .. })) => {
            return Some(Err(RefuseCause::Heap(HeapDetail::RowOrdinalOverflow)
                .refuse(fp)
                .into_error(&relname)));
        }
        Ok(Err(FaceFoldErr::Face(fe))) => {
            return Some(Err(match io_err {
                Some(e) => e,
                None => {
                    // [rung 3] typed true causes off the byte-lane fill:
                    // out-of-line TOAST (detoast rung unlanded) and
                    // non-pglz inline compression are capability gaps;
                    // everything else stays the internal face breach.
                    let d = match fe.what {
                        "text-external" => HeapDetail::TextOutOfLine,
                        "text-compression" => HeapDetail::TextCompression,
                        // [idx 5] A crafted/corrupt on-disk varlena (declared
                        // size past the tuple, header undershoot, or a failed
                        // pglz stream) surfaces as ERRCODE_DATA_CORRUPTED.
                        "text-corrupt" => HeapDetail::TextCorrupt,
                        _ => HeapDetail::Face,
                    };
                    RefuseCause::Heap(d).refuse(fp).into_error(&relname)
                }
            }));
        }
    };

    // Answer-order permutation (peeled Sort): `cmp_rows` over the
    // materialized answer columns — G log G on at most GROUP_CAP rows,
    // deterministic (the goal's order closes with the key tie-break).
    let perm: Option<Vec<u32>> = (!goal.order.is_empty()).then(|| {
        let mut idx: Vec<u32> = (0..answers.nrows as u32).collect();
        idx.sort_unstable_by(|&a, &b| {
            sqe::answer::cmp_rows(&answers.cols, &goal.order, a as usize, b as usize)
        });
        idx
    });
    // [sqe-heap-cursors] §15.10: a bounded pull (number_tuples != 0)
    // delivers only its window and retains the plane as the P6-4 estate
    // spool — priced against the E17 answer face BEFORE the first row
    // escapes; count-0 stays the ordinary full delivery, spool-free.
    let total_rows = perm.as_ref().map(|p| p.len()).unwrap_or(answers.nrows);
    let window_end = total_rows.min(goal.skip.saturating_add(goal.take));
    let spool_take = if number_tuples == 0 {
        goal.take
    } else {
        let bytes = super::seam::spool_window_bytes(
            &answers,
            &goal.out,
            perm.as_deref(),
            goal.skip,
            window_end,
        );
        if bytes > cfg.answer_budget_bytes() {
            return Some(Err(RefuseCause::AnswerBytes { what: "cursor-spool" }
                .refuse(fp)
                .into_error(&relname)));
        }
        window_end.saturating_sub(goal.skip).min(number_tuples as usize)
    };
    let r = deliver(
        estate,
        tup_desc.clone(),
        &answers,
        &goal.out,
        &[],
        dest,
        goal.skip,
        spool_take,
        perm.as_deref(),
        None,
    );
    match r {
        Ok((delivered, slot)) => {
            super::stat::tick_completed(family, "A");
            if number_tuples != 0 {
                estate.es_sqe_spool = Some(Box::new(super::seam::CursorSpool::heap(
                    answers,
                    goal.out,
                    perm,
                    goal.skip,
                    window_end,
                    delivered,
                    tup_desc,
                    slot,
                )));
            }
            Some(Ok(()))
        }
        Err(e) => Some(Err(e)),
    }
}

/// [heap spill] The statement's spill posture: the engine arm is on
/// (PGRUST_SQE_SPILL, default ON — must mirror `SqeConfig::default`)
/// and the host registered a store factory. When true, the grouped
/// witness cap is RETIRED (§15.15, the columnar §10f precedent): hash
/// arms write through the substrate and finalize prices the answer
/// plane; when false, the legacy witness-cap law stands verbatim.
fn heap_spill_serves() -> bool {
    !matches!(std::env::var("PGRUST_SQE_SPILL").as_deref(), Ok("0") | Ok("off"))
        && sqe::spill::available()
}

/// Run an engine fold catching the typed RUNTIME refusals (answer-bytes,
/// spill-unavailable, spill I/O) — the `run_engine_typed` twin for the
/// face drives; anything else resumes to its existing handler.
fn catch_refusal<T>(f: impl FnOnce() -> T) -> Result<T, sqe::refuse::Refuse> {
    // unwind-ok: stmt-boundary — typed runtime-refusal transport
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(v) => Ok(v),
        Err(p) => match p.downcast::<sqe::refuse::RunRefusal>() {
            Ok(rr) => Err(rr.0),
            Err(p) => std::panic::resume_unwind(p),
        },
    }
}

/// The parity-harness admission override (dev/test only; see the
/// admission gate above).
pub(super) fn admit_all() -> bool {
    use pgsync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_SQE_HEAP_ADMIT_ALL").as_deref(), Ok("1") | Ok("on"))
    })
}

pub(super) fn heap_relname(estate: &EStateData<'_>) -> Option<String> {
    estate
        .es_relations
        .iter()
        .flatten()
        .find(|rel| ::tableam::TableAm::of(rel) == Some(::tableam_vocab::TableAm::Heap))
        .map(|rel| String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned())
}

#[cfg(test)]
mod varlena_bound_tests {
    //! [idx 5] `push_varlena_payload` must never read past the containing
    //! tuple: a varlena header parsed from crafted on-disk bytes is
    //! untrusted, so a declared size beyond `avail` (or below its own
    //! header) is a typed `text-corrupt` (ERRCODE_DATA_CORRUPTED), not an
    //! out-of-bounds read. Each buffer here is sized to `avail` exactly,
    //! so any read past the declared bound would fault under Miri.
    use super::push_varlena_payload;

    /// Little-endian 4B-uncompressed header word for a total size `total`
    /// (header + payload): low two bits 00, size in bits 2..=31.
    #[cfg(target_endian = "little")]
    fn hdr_4b_u(total: u32) -> [u8; 4] {
        (total << 2).to_le_bytes()
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn valid_4b_uncompressed_copies_payload() {
        // total = 8 (4B header + 4B payload "abcd").
        let mut buf = Vec::new();
        buf.extend_from_slice(&hdr_4b_u(8));
        buf.extend_from_slice(b"abcd");
        let mut arena = Vec::new();
        let n = unsafe { push_varlena_payload(buf.as_ptr(), buf.len(), &mut arena) };
        assert_eq!(n, Ok(4));
        assert_eq!(&arena, b"abcd");
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn lying_oversized_4b_header_is_corrupt_not_oob() {
        // Header declares ~1 GiB but only 8 bytes are readable.
        let mut buf = Vec::new();
        buf.extend_from_slice(&hdr_4b_u(0x3FFF_FFFF));
        buf.extend_from_slice(b"abcd");
        let mut arena = Vec::new();
        // avail == buf.len(): a trusting read would walk ~1 GiB past it.
        let r = unsafe { push_varlena_payload(buf.as_ptr(), buf.len(), &mut arena) };
        assert_eq!(r, Err("text-corrupt"));
        assert!(arena.is_empty());
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn undersized_4b_headers_do_not_underflow() {
        // Declared totals 0 and 3 are below VARHDRSZ (4): the old
        // `l - VARHDRSZ` wrapped to ~usize::MAX. Now typed-corrupt.
        for total in [0u32, 3u32] {
            let mut buf = Vec::new();
            buf.extend_from_slice(&hdr_4b_u(total));
            buf.extend_from_slice(&[0u8; 4]);
            let mut arena = Vec::new();
            let r = unsafe { push_varlena_payload(buf.as_ptr(), buf.len(), &mut arena) };
            assert_eq!(r, Err("text-corrupt"), "total={total}");
            assert!(arena.is_empty());
        }
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn zero_avail_is_corrupt() {
        let buf = [0u8; 4];
        let mut arena = Vec::new();
        let r = unsafe { push_varlena_payload(buf.as_ptr(), 0, &mut arena) };
        assert_eq!(r, Err("text-corrupt"));
    }
}
