//! [joins rung] Keyed heap hash join, ALL sides heap seq scans, riding
//! the columnar join machinery at the BANK seam (heap-face.md JOINS
//! RUNG): each side's granule fill seals into an in-memory bank via the
//! Values-bank law and `run_hash_join{,_agg}` compose unchanged. R1:
//! serve or ERROR typed/censused; residue refuses `heap/join-*`.
//! R2 [sqe-heap-multiway]: 3+-way INNER HashJoin trees flatten by the
//! columnar deep law (fact election, staged dims) and every leaf seals
//! under ONE shared fill budget.

use std::rc::Rc;

use ::tableam::TableScanDesc;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::primnodes::{INNER_VAR, OUTER_VAR};
use ::types_nodes::{Node, NodeList, NodeTag};
use executils::EStateData;
use tcop_dest::DestReceiver;

use pgrc2_write::ingest::RawDatum;
use sqe::bank::{Bank, ColMeta, Face};
use sqe::engine::{Faces, SqeConfig, SqeCtx};
use sqe::face::{FaceFill, ScanFace};
use sqe::ir::{PredSpec, PredTerm};
use sqe::joins::{
    join_agg_node, join_node, run_hash_join, run_hash_join_agg, DimKey, DimSrc, DimStage,
    JoinAggOp, JoinAggReq, JoinKey, JoinOut, JoinSide, JoinType, MAX_DIM_STAGES,
};

use super::heap::{
    admit_all, heap_pool, heap_relname, key_render, lower_quals, HeapFace, Miss, Rec,
};
use super::refusal::{from_join, FamilyKind, HeapDetail, RefuseCause};
use super::seam::{
    catalog_schema, deliver, word_eq_proc, OutSlot, OutSrc, Render, AGG_COUNT_ANY, AGG_COUNT_STAR,
    AGG_MAX_DATE, AGG_MAX_INT2, AGG_MAX_INT4, AGG_MAX_INT8, AGG_MAX_TS, AGG_MAX_TSTZ,
    AGG_MIN_DATE, AGG_MIN_INT2, AGG_MIN_INT4, AGG_MIN_INT8, AGG_MIN_TS, AGG_MIN_TSTZ,
    AGG_SUM_INT2, AGG_SUM_INT4, AGG_SUM_INT8,
};
use super::valuesbank::{BankBuilder, VCol};

/// The admission-table bit for the heap-join class: false until its
/// parity cell admits it against the ruled 1.05x-vs-row-actual bar
/// (the heap-perf pattern — the cut table lands in this comment at the
/// flip). `PGRUST_SQE_HEAP_ADMIT_ALL=1` serves it for the oracle gates.
const JOINS_CLASS_ADMITTED: bool = false;

/// The admission bit for the DEEP (3+-way) heap-join class — its own
/// parity cell, independent of the 2-way rung's.
const JOINS_DEEP_ADMITTED: bool = false;

/// Both sides' staged fill together; breach = the join family's typed
/// resource refusal at engagement cadence, never an OOM.
const JOIN_FILL_BUDGET_BYTES: u64 = 1 << 30;

/// [joins parity, p7-2] Perf-law guard: the ruled fix-or-refuse
/// demotion of the SMALL-BUILD heap-join shape (never serve slow —
/// the 2026-08-18 per-shape admission ruling). The bank-seam join
/// materializes BOTH sides through `fill_side`'s row-at-a-time
/// `append_row` before any join work — a per-execution tax that is
/// LINEAR in probe rows, while the incumbent row engine streams its
/// probe against a built hash with no materialization at all. The
/// p7-2 ledger join cell measured the tax: 10M-row probe x ~5k-row
/// filtered build, sqe 2549ms vs row 474ms (5.37x SLOWER) — ~208ns
/// of fill overhead per probe row against the row engine's ~47ns/row
/// whole cost. With a build side small enough that the row engine's
/// hash stays cache-resident, that tax can NEVER be recouped at any
/// probe scale (both costs are linear in probe rows; sqe's slope is
/// strictly worse). The row probe only degrades once its hash table
/// spills last-level cache — ~32MB on the CI cluster's c8g floor at ~50B
/// an entry = ~650k build entries. Floor = 1M build rows: one power
/// of ten above the measured-slow cell and past that LLC knee; the
/// still-open question ABOVE the floor stays owned by
/// `JOINS_CLASS_ADMITTED` (false until a parity cell lands there).
const JOIN_BUILD_ROWS_FLOOR: u64 = 1_000_000;

/// The guard is admission LAW, not a lever: it fires even under
/// `PGRUST_SQE_HEAP_ADMIT_ALL=1` (the lever forces recognition of
/// unadmitted classes for the oracle gates — it never licenses
/// serving a shape MEASURED slower than the incumbent).
/// `PGRUST_SQE_HEAP_JOIN_PERF_GUARD=0` disarms it for oracle
/// identity runs (the e2e's joins-rung boot).
fn join_perf_guard() -> bool {
    use pgsync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_SQE_HEAP_JOIN_PERF_GUARD").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Small-build demotion at recognition grain (planner estimate — the
/// same currency `build_rows_hint` already carries to the engine).
fn perf_guard_refuses(build_rows_hint: u64) -> bool {
    join_perf_guard() && build_rows_hint < JOIN_BUILD_ROWS_FLOOR
}

const INT2OID: u32 = 21;
const INT4OID: u32 = 23;
const INT8OID: u32 = 20;
const DATEOID: u32 = 1082;
const TSOID: u32 = 1114;
const TSTZOID: u32 = 1184;
const TEXTOID: u32 = 25;
const VARCHAROID: u32 = 1043;
const BPCHAROID: u32 = 1042;

const AGG_PLAIN: u32 = 0;

fn is_word_oid(t: u32) -> bool {
    matches!(t, INT2OID | INT4OID | INT8OID | DATEOID | TSOID | TSTZOID)
}
fn is_text_oid(t: u32) -> bool {
    matches!(t, TEXTOID | VARCHAROID)
}

struct SideRec {
    scanrelid: u32,
    schema: Vec<ColMeta>,
    /// Referenced heap attnos, sorted: bank column = position + 1.
    cols: Vec<u32>,
    /// WHERE conjuncts in heap-attno space (remapped at node build).
    terms: Vec<PredTerm>,
}

impl SideRec {
    fn bank_col(&self, attno: u32) -> u32 {
        self.cols.iter().position(|&a| a == attno).expect("referenced column planned") as u32 + 1
    }
    fn meta(&self, attno: u32) -> &ColMeta {
        self.schema.iter().find(|c| c.attno == attno).expect("recognized column in catalog")
    }
}

struct JGoal {
    build: SideRec,
    probe: SideRec,
    /// (probe attno, build attno) per hashclause.
    keys: Vec<(u32, u32)>,
    /// Row goal: (build side?, attno, type oid) per tlist position.
    routs: Vec<(bool, u32, u32)>,
    /// Agg goal: op + optional input (build side?, attno) + render.
    aggs: Vec<(JoinAggOp, Option<(bool, u32)>, Render)>,
    agg: bool,
    build_rows_hint: u64,
}

enum JRec {
    Ok(Box<JGoal>),
    Deep(Box<DGoal>),
    Miss(Miss),
    Refuse(RefuseCause),
    Refused(super::refusal::Refusal),
}

fn aggref_plain(ar: &::types_nodes::primnodes::Aggref<'_>) -> bool {
    ar.aggsplit == ::types_nodes::primnodes::AGGSPLIT_SIMPLE
        && ar.aggkind == ::types_nodes::primnodes::AGGKIND_NORMAL
        && ar.aggdirectargs.is_nil()
        && ar.aggorder.is_nil()
        && ar.aggdistinct.is_nil()
        && ar.aggfilter.is_none()
        && ar.agglevelsup == 0
}

fn agg_plain(a: &::types_nodes::plannodes::Agg<'_>) -> bool {
    a.aggstrategy == AGG_PLAIN
        && a.numCols == 0
        && a.groupingSets.is_nil()
        && a.plan.qual.is_nil()
        && a.plan.initPlan.is_nil()
}

/// The recognized ungrouped-agg vocabulary: aggfnoid -> (engine op,
/// answer render). `count(*)` is handled apart (no input column).
fn agg_op_render(fnoid: u32) -> Option<(JoinAggOp, Render)> {
    Some(match fnoid {
        AGG_COUNT_ANY => (JoinAggOp::CountCol, Render::CountI64),
        AGG_SUM_INT2 | AGG_SUM_INT4 => (JoinAggOp::Sum, Render::SumI64),
        AGG_SUM_INT8 => (JoinAggOp::Sum, Render::SumNumeric),
        AGG_MIN_INT2 => (JoinAggOp::Min, Render::MinMaxI16),
        AGG_MAX_INT2 => (JoinAggOp::Max, Render::MinMaxI16),
        AGG_MIN_INT4 | AGG_MIN_DATE => (JoinAggOp::Min, Render::MinMaxI32),
        AGG_MAX_INT4 | AGG_MAX_DATE => (JoinAggOp::Max, Render::MinMaxI32),
        AGG_MIN_INT8 | AGG_MIN_TS | AGG_MIN_TSTZ => (JoinAggOp::Min, Render::MinMaxI64),
        AGG_MAX_INT8 | AGG_MAX_TS | AGG_MAX_TSTZ => (JoinAggOp::Max, Render::MinMaxI64),
        _ => return None,
    })
}

/// A join-level Var (OUTER/INNER) chased through the child scan tlist to
/// (build side?, scan attno, type oid). None = outside the vocabulary.
fn join_var<'a>(
    n: Node<'_>,
    ptl: &NodeList<'a>,
    btl: &NodeList<'a>,
    prelid: u32,
    brelid: u32,
) -> Option<(bool, u32, u32)> {
    let v = n.as_var()?;
    if v.varlevelsup != 0 || v.varattno <= 0 {
        return None;
    }
    let (tl, relid, build) = if v.varno == OUTER_VAR {
        (ptl, prelid, false)
    } else if v.varno == INNER_VAR {
        (btl, brelid, true)
    } else {
        return None;
    };
    let te = tl.iter().nth(v.varattno as usize - 1)?.as_target_entry()?;
    let sv = te.expr.as_var()?;
    if sv.varno != relid as i32 || sv.varlevelsup != 0 || sv.varattno <= 0 {
        return None;
    }
    Some((build, sv.varattno as u32, sv.vartype))
}

/// One side's relation + schema; the scanned relation must be heap.
fn side_scan(
    estate: &EStateData<'_>,
    n: Node<'_>,
) -> Option<(u32, Vec<ColMeta>)> {
    let scan = n.as_seq_scan()?;
    if !scan.scan.plan.initPlan.is_nil()
        || scan.scan.plan.lefttree.is_some()
        || scan.scan.plan.righttree.is_some()
    {
        return None;
    }
    let scanrelid = scan.scan.scanrelid;
    let rel = estate
        .es_relations
        .get(scanrelid as usize - 1)
        .and_then(|r| r.as_ref())
        .filter(|rel| ::tableam::TableAm::of(rel) == Some(::tableam_vocab::TableAm::Heap))?;
    let schema = catalog_schema(rel).ok()?;
    Some((scanrelid, schema))
}

fn recognize(
    estate: &EStateData<'_>,
    pstmt: &PlannedStmt<'_>,
    fp: u64,
) -> Option<JRec> {
    let top = pstmt.planTree?;
    // Only join-topped (or Agg-over-join) plans proceed.
    let (agg_node, hj_node) = match top.node_tag() {
        NodeTag::T_HashJoin => (None, top),
        NodeTag::T_Agg => {
            let a = top.as_agg()?;
            let child = a.plan.lefttree?;
            if child.node_tag() != NodeTag::T_HashJoin {
                return None;
            }
            (Some(a), child)
        }
        _ => return None,
    };
    let miss = |what: &'static str| Some(JRec::Miss(Miss(what)));
    let refuse = |c: RefuseCause| Some(JRec::Refuse(c));
    let jrefuse = |what: &'static str| refuse(RefuseCause::Heap(HeapDetail::Join(what)));

    let hj = hj_node.as_hash_join()?;
    let Some(outer_p) = hj.join.plan.lefttree else { return miss("node") };
    let Some(inner_p) = hj.join.plan.righttree else { return miss("node") };
    let join_tag = |t: NodeTag| {
        matches!(t, NodeTag::T_HashJoin | NodeTag::T_NestLoop | NodeTag::T_MergeJoin)
    };
    // Charter residue, refused at the recognized boundary.
    let Some(hash) = inner_p.as_hash() else { return miss("node") };
    if !hash.plan.initPlan.is_nil() || !hash.plan.qual.is_nil() || hash.plan.righttree.is_some() {
        return miss("node");
    }
    let Some(bchild) = hash.plan.lefttree else { return miss("node") };
    if join_tag(outer_p.node_tag()) || join_tag(bchild.node_tag()) {
        // R2: the deep arm owns every nested-join tree.
        return recognize_deep(estate, fp, agg_node.map(|_| top), hj_node);
    }
    if outer_p.node_tag() != NodeTag::T_SeqScan || bchild.node_tag() != NodeTag::T_SeqScan {
        return miss("node");
    }
    let Some((prelid, pschema)) = side_scan(estate, outer_p) else { return miss("node") };
    let Some((brelid, bschema)) = side_scan(estate, bchild) else { return miss("node") };
    if hj.join.jointype != ::types_nodes::jointype::JoinType::JOIN_INNER {
        return jrefuse("join-outer");
    }
    if !hj.join.joinqual.is_nil() || !hj.join.plan.qual.is_nil() {
        return jrefuse("join-non-equi");
    }
    if !hj.join.plan.initPlan.is_nil() {
        return miss("node");
    }

    let ptl = &outer_p.as_seq_scan().expect("tag checked").scan.plan.targetlist;
    let btl = &bchild.as_seq_scan().expect("tag checked").scan.plan.targetlist;
    let pmeta = |a: u32| pschema.iter().find(|c| c.attno == a);
    let bmeta = |a: u32| bschema.iter().find(|c| c.attno == a);
    // Word-lane admission; text = ruled residue, bpchar = pad-law class.
    let word_ref = |build: bool, attno: u32, ty: u32, what: &'static str| -> Result<(), JRec> {
        if is_text_oid(ty) {
            return Err(JRec::Refuse(RefuseCause::Heap(HeapDetail::Join("join-text"))));
        }
        if ty == BPCHAROID {
            return Err(JRec::Refuse(RefuseCause::BpcharLaw { what }));
        }
        let cm = if build { bmeta(attno) } else { pmeta(attno) };
        let ok = is_word_oid(ty)
            && cm.is_some_and(|c| c.typ.byval && Face::of_class(c.class).word_foldable());
        if ok {
            Ok(())
        } else {
            Err(JRec::Miss(Miss("type")))
        }
    };

    // --- equi-keys from the plan's hashclauses -------------------------------
    let mut keys: Vec<(u32, u32)> = Vec::new();
    for cn in hj.hashclauses.iter() {
        let Some(op) = cn.as_op_expr() else { return miss("pred") };
        if op.args.len() != 2 {
            return miss("pred");
        }
        let mut it = op.args.iter();
        let (a, b) = (it.next()?, it.next()?);
        let (Some(ra), Some(rb)) = (
            join_var(a, ptl, btl, prelid, brelid),
            join_var(b, ptl, btl, prelid, brelid),
        ) else {
            return miss("pred");
        };
        let ((_, pa, pt), (_, ba, bt)) = match (ra.0, rb.0) {
            (false, true) => (ra, rb),
            (true, false) => (rb, ra),
            _ => return miss("pred"),
        };
        if let Err(r) = word_ref(false, pa, pt, "join-key") {
            return Some(r);
        }
        if let Err(r) = word_ref(true, ba, bt, "join-key") {
            return Some(r);
        }
        if !word_eq_proc(op.opfuncid) {
            return miss("pred");
        }
        keys.push((pa, ba));
    }
    if keys.is_empty() {
        return jrefuse("join-non-equi");
    }

    // --- outputs / agg legs ---------------------------------------------------
    let mut routs: Vec<(bool, u32, u32)> = Vec::new();
    let mut aggs: Vec<(JoinAggOp, Option<(bool, u32)>, Render)> = Vec::new();
    let agg = if let Some(a) = agg_node {
        if !agg_plain(a) {
            return miss("agg");
        }
        let hjtl = &hj.join.plan.targetlist;
        for tn in a.plan.targetlist.iter() {
            let Some(te) = tn.as_target_entry() else { return miss("proj") };
            if te.resjunk || te.expr.node_tag() != NodeTag::T_Aggref {
                return miss("proj");
            }
            let ar = te.expr.as_aggref().expect("tag checked");
            if !aggref_plain(ar) {
                return miss("agg");
            }
            if ar.aggfnoid == AGG_COUNT_STAR {
                aggs.push((JoinAggOp::CountStar, None, Render::CountI64));
                continue;
            }
            let Some((op, render)) = agg_op_render(ar.aggfnoid) else { return miss("agg") };
            if ar.args.len() != 1 {
                return miss("agg");
            }
            let Some(ate) = ar.args.iter().next().and_then(|x| x.as_target_entry()) else {
                return miss("agg");
            };
            // Agg-level Vars index the JOIN tlist (one hop).
            let hop = || -> Option<(bool, u32, u32)> {
                let v = ate.expr.as_var()?;
                if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno <= 0 {
                    return None;
                }
                let jte = hjtl.iter().nth(v.varattno as usize - 1)?.as_target_entry()?;
                join_var(jte.expr, ptl, btl, prelid, brelid)
            };
            let Some((build, attno, ty)) = hop() else {
                return miss("agg");
            };
            if let Err(r) = word_ref(build, attno, ty, "join-out") {
                return Some(r);
            }
            aggs.push((op, Some((build, attno)), render));
        }
        if aggs.is_empty() {
            return miss("agg");
        }
        true
    } else {
        for tn in hj.join.plan.targetlist.iter() {
            let Some(te) = tn.as_target_entry() else { return miss("proj") };
            if te.resjunk {
                return miss("proj");
            }
            let Some((build, attno, ty)) = join_var(te.expr, ptl, btl, prelid, brelid) else {
                return miss("proj");
            };
            if let Err(r) = word_ref(build, attno, ty, "join-out") {
                return Some(r);
            }
            routs.push((build, attno, ty));
        }
        if routs.is_empty() {
            return miss("proj");
        }
        false
    };

    // --- side WHERE conjuncts (shared lowering; text terms = residue) --------
    let side_terms = |tl_node: Node<'_>, relid: u32, schema: &[ColMeta]| -> Result<Vec<PredTerm>, JRec> {
        let scan = tl_node.as_seq_scan().expect("tag checked");
        match lower_quals(&scan.scan.plan.qual, relid, schema, estate, fp) {
            Rec::Ok((terms, var_terms)) => {
                if !var_terms.is_empty() {
                    return Err(JRec::Refuse(RefuseCause::Heap(HeapDetail::Join("join-text"))));
                }
                Ok(terms)
            }
            Rec::Miss(m) => Err(JRec::Miss(m)),
            Rec::Refuse(c) => Err(JRec::Refuse(c)),
            Rec::Refused(r) => Err(JRec::Refused(r)),
        }
    };
    let pterms = match side_terms(outer_p, prelid, &pschema) {
        Ok(t) => t,
        Err(r) => return Some(r),
    };
    let bterms = match side_terms(bchild, brelid, &bschema) {
        Ok(t) => t,
        Err(r) => return Some(r),
    };
    // Qual lanes must be in the bank-ingest vocabulary (six word oids).
    for t in pterms.iter() {
        let Some(cm) = pmeta(t.col) else { return miss("type") };
        if !is_word_oid(cm.typ.oid)
            || !(cm.typ.byval && Face::of_class(cm.class).word_foldable())
        {
            return miss("type");
        }
    }
    for t in bterms.iter() {
        let Some(cm) = bmeta(t.col) else { return miss("type") };
        if !is_word_oid(cm.typ.oid)
            || !(cm.typ.byval && Face::of_class(cm.class).word_foldable())
        {
            return miss("type");
        }
    }

    // --- referenced sets -> side records --------------------------------------
    let mut pcols: Vec<u32> = Vec::new();
    let mut bcols: Vec<u32> = Vec::new();
    let add = |v: &mut Vec<u32>, a: u32| {
        if !v.contains(&a) {
            v.push(a);
        }
    };
    for &(pa, ba) in &keys {
        add(&mut pcols, pa);
        add(&mut bcols, ba);
    }
    for t in &pterms {
        add(&mut pcols, t.col);
    }
    for t in &bterms {
        add(&mut bcols, t.col);
    }
    for &(build, attno, _) in &routs {
        add(if build { &mut bcols } else { &mut pcols }, attno);
    }
    for (_, input, _) in &aggs {
        if let Some((build, attno)) = input {
            add(if *build { &mut bcols } else { &mut pcols }, *attno);
        }
    }
    pcols.sort_unstable();
    bcols.sort_unstable();

    let build_rows_hint = hash.plan.plan_rows.max(0.0) as u64;
    Some(JRec::Ok(Box::new(JGoal {
        build: SideRec { scanrelid: brelid, schema: bschema, cols: bcols, terms: bterms },
        probe: SideRec { scanrelid: prelid, schema: pschema, cols: pcols, terms: pterms },
        keys,
        routs,
        aggs,
        agg,
        build_rows_hint,
    })))
}

enum Fail {
    Miss(&'static str),
    Err(Box<PgError>),
    Refuse(RefuseCause),
}

/// One side's fill into a sealed in-memory bank (serial-face pin law:
/// zero pins at settle, error paths included).
fn fill_side<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    side: &SideRec,
    budget: &mut u64,
) -> Result<Bank, Fail> {
    let internal = |m: String| Fail::Err(Box::new(PgError::error(format!("sqe heap join: {m}"))));
    let nblocks = match ::nodeseqscan::seq_scan_heap_block_geometry(ss, estate) {
        Ok(Some(n)) => n,
        Ok(None) => return Err(Fail::Miss("geometry")),
        Err(e) => return Err(Fail::Err(e)),
    };
    let Some(TableScanDesc::Heap(scan)) = ss.ss.ss_currentScanDesc.as_mut() else {
        return Err(Fail::Miss("geometry"));
    };
    if scan.rs_base.rs_flags & ::tableam_vocab::SO_ALLOW_PAGEMODE == 0 {
        return Err(Fail::Miss("pagemode"));
    }
    let vcols: Vec<VCol> = side
        .cols
        .iter()
        .map(|&a| VCol { oid: side.meta(a).typ.oid, collation: 0 })
        .collect();
    let mut builder = match BankBuilder::open("sqe-heap-join-side", &vcols) {
        Ok(b) => b,
        Err(m) => return Err(internal(m)),
    };
    let row_bytes = 8u64 * side.cols.len().max(1) as u64;
    let mut face = HeapFace::new(scan, &side.schema, nblocks, estate.es_query_cxt, false);
    let mut fill = FaceFill::new();
    let fill_cols: Vec<(u32, Face)> =
        side.cols.iter().map(|&a| (a, face.face(a))).collect();
    let mut datums: Vec<RawDatum<'static>> = Vec::with_capacity(side.cols.len());
    let mut res: Result<(), Fail> = Ok(());
    'units: for u in 0..face.n_units() {
        if let Err(e) = crate::cfi() {
            res = Err(Fail::Err(e));
            break;
        }
        fill.reset(&fill_cols);
        if face.fill(u, &side.cols, &mut fill).is_err() {
            res = Err(match face.err.take() {
                Some(e) => Fail::Err(e),
                None => Fail::Refuse(RefuseCause::Heap(HeapDetail::Face)),
            });
            break;
        }
        for r in 0..fill.rows as usize {
            *budget += row_bytes;
            if *budget > JOIN_FILL_BUDGET_BYTES {
                res = Err(Fail::Refuse(RefuseCause::MemoryBudget {
                    family: FamilyKind::Join,
                }));
                break 'units;
            }
            datums.clear();
            for c in fill.cols.iter() {
                if c.all_valid() || c.row_valid(r) {
                    datums.push(RawDatum::Word(c.words[r]));
                } else {
                    datums.push(RawDatum::Null);
                }
            }
            if let Err(m) = builder.append_row(&datums) {
                res = Err(internal(m));
                break 'units;
            }
        }
    }
    ::heapam::heap_end_claim_release(face.scan);
    res?;
    builder.finish().map_err(internal)
}

/// The join arm's EXPLAIN-grain disposition (heap.rs statement_verdict
/// consumes it ahead of the single-relation walk — the SAME order the
/// execution arm runs `try_join`).
pub(super) enum ExplainJoin {
    /// Final verdict tuple: (engaged, family-or-cause, sqlstate).
    Verdict(bool, String, String),
    /// Join-arm miss: the caller ticks the census and falls through to
    /// the single-relation recognizer, exactly as `maybe_run_heap` does.
    Miss(&'static str),
}

/// EXPLAIN mirror of `try_join` at recognition + admission grain
/// (EXPLAIN-vs-exec disposition consistency — the matrix identity
/// gate's premise; regressed 2026-08-20: join-topped statements
/// EXPLAINed as `heap-miss/node` while execution raised the typed
/// `heap/join-*` refusals, ledgered as 14 incumbent IDENTITY-FAILs).
/// Planstate/fill-time facts (state-shape, proj, run-parallel at the
/// scan, budget breach) stay run-time-only, like the columnar surface.
/// None = not a join-topped plan: the single-relation walk owns the
/// verdict. Refusal probes tick the census at `refuse()` — one tick per
/// EXPLAIN, the execution cadence; `Refused` was ticked at construction.
pub(super) fn explain_verdict(
    estate: &EStateData<'_>,
    pstmt: &PlannedStmt<'_>,
    fp: u64,
) -> Option<ExplainJoin> {
    let rec = recognize(estate, pstmt, fp)?;
    let refused = |cause: RefuseCause| {
        let c = cause.refuse(fp).cause();
        ExplainJoin::Verdict(
            false,
            c.census_key(),
            String::from_utf8_lossy(&::types_error::unpack_sqlstate(c.sqlstate())).into_owned(),
        )
    };
    Some(match rec {
        JRec::Ok(g) => {
            if !JOINS_CLASS_ADMITTED && !admit_all() {
                refused(RefuseCause::Heap(HeapDetail::PerfUnadmitted))
            } else if perf_guard_refuses(g.build_rows_hint) {
                // Small-build demotion (p7-2 SLOWER cell) — the SAME
                // verdict the execution arm raises, at the same grain.
                refused(RefuseCause::Heap(HeapDetail::PerfUnadmitted))
            } else {
                let fam = if g.agg { "heap-joinagg" } else { "heap-join" };
                ExplainJoin::Verdict(true, fam.to_string(), String::new())
            }
        }
        JRec::Deep(g) => {
            if !JOINS_DEEP_ADMITTED && !admit_all() {
                refused(RefuseCause::Heap(HeapDetail::PerfUnadmitted))
            } else if perf_guard_refuses(g.build_rows_hint) {
                refused(RefuseCause::Heap(HeapDetail::PerfUnadmitted))
            } else {
                let fam = if g.agg { "heap-joindeepagg" } else { "heap-joindeep" };
                ExplainJoin::Verdict(true, fam.to_string(), String::new())
            }
        }
        JRec::Miss(Miss(what)) => ExplainJoin::Miss(what),
        JRec::Refuse(cause) => refused(cause),
        JRec::Refused(r) => {
            let c = r.cause();
            ExplainJoin::Verdict(
                false,
                c.census_key(),
                String::from_utf8_lossy(&::types_error::unpack_sqlstate(c.sqlstate())).into_owned(),
            )
        }
    })
}

/// The heap-join arm of the dispatch: Some(result) = the statement is
/// this rung's (served or typed error); None = miss, the single-relation
/// ladder and the incumbents proceed.
pub(crate) fn try_join<'mcx, 'd>(
    estate: &mut EStateData<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    fp: u64,
    tup_desc: Option<&Rc<::types_tuple::TupleDescData<'static>>>,
    dest: &mut DestReceiver<'d>,
) -> Option<PgResult<()>> {
    let miss = |what: &'static str| {
        super::stat::tick_witness("heap-miss", what);
        None
    };
    let rec = recognize(estate, pstmt, fp)?;
    let relname = heap_relname(estate).unwrap_or_default();
    let goal = match rec {
        JRec::Ok(g) => g,
        JRec::Deep(g) => {
            if !JOINS_DEEP_ADMITTED && !admit_all() {
                return Some(Err(RefuseCause::Heap(HeapDetail::PerfUnadmitted)
                    .refuse(fp)
                    .into_error(&relname)));
            }
            if perf_guard_refuses(g.build_rows_hint) {
                super::stat::tick_witness("heap-join-perf-guard", "-");
                return Some(Err(RefuseCause::Heap(HeapDetail::PerfUnadmitted)
                    .refuse(fp)
                    .into_error(&relname)));
            }
            return run_deep(estate, planstate, g, fp, tup_desc, dest, &relname);
        }
        JRec::Miss(Miss(what)) => return miss(what),
        JRec::Refuse(cause) => {
            return Some(Err(cause.refuse(fp).into_error(&relname)));
        }
        JRec::Refused(r) => {
            return Some(Err(r.into_error(&relname)));
        }
    };
    if !JOINS_CLASS_ADMITTED && !admit_all() {
        return Some(Err(RefuseCause::Heap(HeapDetail::PerfUnadmitted)
            .refuse(fp)
            .into_error(&relname)));
    }
    if perf_guard_refuses(goal.build_rows_hint) {
        // The p7-2 demotion: a small-build heap join is measured
        // slower than the streamed row plan at every probe scale —
        // refuse typed so the incumbent answers (witnessed here and
        // by the census tick inside `refuse`).
        super::stat::tick_witness("heap-join-perf-guard", "-");
        return Some(Err(RefuseCause::Heap(HeapDetail::PerfUnadmitted)
            .refuse(fp)
            .into_error(&relname)));
    }
    let Some(td) = tup_desc else { return miss("proj") };
    let natts = if goal.agg { goal.aggs.len() } else { goal.routs.len() };
    if td.natts as usize != natts {
        return miss("proj");
    }

    let mut node: &mut crate::procnode::PlanStateNode<'mcx> = planstate;
    if goal.agg {
        let crate::procnode::PlanStateNode::Agg(aps) = node else {
            return miss("state-shape");
        };
        node = &mut aps.outer;
    }
    let crate::procnode::PlanStateNode::HashJoin(hj) = node else {
        return miss("state-shape");
    };

    let family: &'static str = if goal.agg { "heap-joinagg" } else { "heap-join" };
    super::stat::tick_engaged(family, "A");
    super::stat::tick_witness("heap-join-membank", "-");

    // Build then probe, one open scan at a time, one statement snapshot.
    let mut budget: u64 = 0;
    let fail = |f: Fail, fp: u64, relname: &str| -> Option<PgResult<()>> {
        match f {
            Fail::Miss(what) => miss(what),
            Fail::Err(e) => Some(Err(e)),
            Fail::Refuse(c) => Some(Err(c.refuse(fp).into_error(relname))),
        }
    };
    let bbank = {
        let bnode: &mut crate::procnode::PlanStateNode<'mcx> = &mut hj.hash.child;
        let crate::procnode::PlanStateNode::SeqScan(ss) = bnode else {
            return miss("state-shape");
        };
        if ss.is_parallel() {
            return miss("run-parallel");
        }
        match fill_side(ss, estate, &goal.build, &mut budget) {
            Ok(b) => b,
            Err(f) => return fail(f, fp, &relname),
        }
    };
    let pbank = {
        let pnode: &mut crate::procnode::PlanStateNode<'mcx> = &mut hj.outer;
        let crate::procnode::PlanStateNode::SeqScan(ss) = pnode else {
            return miss("state-shape");
        };
        if ss.is_parallel() {
            return miss("run-parallel");
        }
        match fill_side(ss, estate, &goal.probe, &mut budget) {
            Ok(b) => b,
            Err(f) => return fail(f, fp, &relname),
        }
    };

    // Cache law: per-statement faces; the shared heap pool runs both.
    let bfaces = Faces::new(SqeConfig::heap_v1(1));
    let pfaces = Faces::new(SqeConfig::heap_v1(1));
    let pool = heap_pool();
    let bctx = SqeCtx { bank: &bbank, pool, faces: &bfaces };
    let pctx = SqeCtx { bank: &pbank, pool, faces: &pfaces };

    let jerr = |r: &sqe::joins::JoinRefuse| from_join(r).refuse(fp).into_error(&relname);
    let keys: Vec<JoinKey> = goal
        .keys
        .iter()
        .map(|&(pa, ba)| JoinKey {
            build_col: goal.build.bank_col(ba),
            probe_col: goal.probe.bank_col(pa),
        })
        .collect();
    let remap = |side: &SideRec, terms: &[PredTerm]| -> Option<PredSpec> {
        if terms.is_empty() {
            return None;
        }
        Some(PredSpec::all(
            terms
                .iter()
                .map(|t| {
                    PredTerm::new(side.bank_col(t.col), t.op, t.lo, t.hi, side.meta(t.col).typ)
                })
                .collect(),
        ))
    };
    let build_pred = remap(&goal.build, &goal.build.terms);
    let probe_pred = remap(&goal.probe, &goal.probe.terms);

    let answers = if goal.agg {
        let reqs: Vec<JoinAggReq> = goal
            .aggs
            .iter()
            .map(|&(op, input, _)| {
                JoinAggReq::col(
                    op,
                    input.map(|(build, attno)| JoinOut {
                        side: if build { JoinSide::Build } else { JoinSide::Probe },
                        col: if build {
                            goal.build.bank_col(attno)
                        } else {
                            goal.probe.bank_col(attno)
                        },
                    }),
                )
            })
            .collect();
        let anode = match join_agg_node(
            &bbank,
            &pbank,
            &[],
            0,
            JoinType::Inner,
            keys,
            Vec::new(),
            build_pred,
            probe_pred,
            Vec::new(),
            reqs,
            Vec::new(),
            goal.build_rows_hint,
        ) {
            Ok(n) => n,
            Err(e) => return Some(Err(jerr(&e))),
        };
        match run_hash_join_agg(&bctx, &pctx, &[], &anode) {
            Ok(a) => a,
            Err(e) => return Some(Err(jerr(&e))),
        }
    } else {
        let jouts: Vec<JoinOut> = goal
            .routs
            .iter()
            .map(|&(build, attno, _)| JoinOut {
                side: if build { JoinSide::Build } else { JoinSide::Probe },
                col: if build {
                    goal.build.bank_col(attno)
                } else {
                    goal.probe.bank_col(attno)
                },
            })
            .collect();
        let node = match join_node(
            &bbank,
            &pbank,
            &[],
            0,
            JoinType::Inner,
            keys,
            Vec::new(),
            build_pred,
            probe_pred,
            Vec::new(),
            jouts,
            goal.build_rows_hint,
            false,
        ) {
            Ok(n) => n,
            Err(e) => return Some(Err(jerr(&e))),
        };
        let a = match run_hash_join(&bctx, &pctx, &[], &node) {
            Ok(a) => a,
            Err(e) => return Some(Err(jerr(&e))),
        };
        // Joined-row emit cap: the grouped-answer bound, typed at breach.
        if a.nrows as u64 > sqe::planner::GROUP_ROW_CAP {
            return Some(Err(RefuseCause::MemoryBudget { family: FamilyKind::Join }
                .refuse(fp)
                .into_error(&relname)));
        }
        a
    };

    let out: Vec<OutSlot> = if goal.agg {
        goal.aggs
            .iter()
            .enumerate()
            .map(|(i, (_, _, render))| OutSlot { src: OutSrc::Col(i), render: *render })
            .collect()
    } else {
        goal.routs
            .iter()
            .enumerate()
            .map(|(i, &(_, _, ty))| OutSlot { src: OutSrc::Col(i), render: key_render(ty) })
            .collect()
    };
    let r = deliver(
        estate,
        td.clone(),
        &answers,
        &out,
        &[],
        dest,
        0,
        usize::MAX,
        None,
        None,
    );
    if r.is_ok() {
        super::stat::tick_completed(family, "A");
    }
    Some(r.map(|_| ()))
}

// [sqe-heap-multiway] R2: 3+-way INNER HashJoin trees over heap scans.
// The flatten + fact/build/dim election mirror the columnar deep
// recognizer (seam.rs admit_deep_tree); every leaf seals through the
// same Values-bank fill under the ONE shared budget, and the engine's
// dim stages compose over the sealed banks.

struct DLeaf<'p> {
    node: Node<'p>,
    scanrelid: u32,
    schema: Vec<ColMeta>,
    rows: f64,
    /// Rows hint when this leaf is a Hash child; 0 on the outer spine.
    hash_rows: u64,
}

/// Var-resolution mirror of the flattened tree, in LEAF-INDEX space
/// (roles are elected after the walk).
enum HRes<'p> {
    Leaf {
        leaf: usize,
        tl: &'p NodeList<'p>,
        scanrelid: u32,
    },
    Join {
        tl: &'p NodeList<'p>,
        hash_tl: &'p NodeList<'p>,
        outer: Box<HRes<'p>>,
        inner: Box<HRes<'p>>,
    },
}

/// Resolve `n`, an expr AT `res`'s level, to (leaf, attno, type oid).
fn hres_at<'p>(res: &HRes<'p>, n: Node<'p>) -> Option<(usize, u32, u32)> {
    match res {
        HRes::Leaf { leaf, scanrelid, .. } => {
            let v = n.as_var()?;
            if v.varno != *scanrelid as i32 || v.varlevelsup != 0 || v.varattno <= 0 {
                return None;
            }
            Some((*leaf, v.varattno as u32, v.vartype))
        }
        HRes::Join { hash_tl, outer, inner, .. } => {
            let v = n.as_var()?;
            if v.varlevelsup != 0 || v.varattno <= 0 {
                return None;
            }
            match v.varno {
                OUTER_VAR => hres_out(outer, v.varattno as usize),
                INNER_VAR => {
                    let e = hash_tl.iter().nth(v.varattno as usize - 1)?.as_target_entry()?.expr;
                    let v2 = e.as_var()?;
                    if v2.varno != OUTER_VAR || v2.varlevelsup != 0 || v2.varattno <= 0 {
                        return None;
                    }
                    hres_out(inner, v2.varattno as usize)
                }
                _ => None,
            }
        }
    }
}

/// Resolve OUTPUT column `attno` of `res` to (leaf, attno, type oid).
fn hres_out<'p>(res: &HRes<'p>, attno: usize) -> Option<(usize, u32, u32)> {
    let tl = match res {
        HRes::Leaf { tl, .. } => tl,
        HRes::Join { tl, .. } => tl,
    };
    let e = tl.iter().nth(attno - 1)?.as_target_entry()?.expr;
    hres_at(res, e)
}

struct DFlat<'p> {
    leaves: Vec<DLeaf<'p>>,
    /// Resolved equi-clauses, both ends (leaf, attno, type oid).
    clauses: Vec<((usize, u32, u32), (usize, u32, u32))>,
}

/// Post-order structural flatten; typed residue refuses at the walk
/// (outer joins, residual quals, non-hash join nodes).
fn dwalk<'p>(
    estate: &EStateData<'_>,
    n: Node<'p>,
    hash_rows: u64,
    st: &mut DFlat<'p>,
) -> Result<HRes<'p>, JRec> {
    let miss = |w: &'static str| JRec::Miss(Miss(w));
    let jref = |w: &'static str| JRec::Refuse(RefuseCause::Heap(HeapDetail::Join(w)));
    match n.node_tag() {
        NodeTag::T_SeqScan => {
            let Some((scanrelid, schema)) = side_scan(estate, n) else {
                return Err(miss("node"));
            };
            let scan = n.as_seq_scan().expect("tag checked");
            let leaf = st.leaves.len();
            st.leaves.push(DLeaf {
                node: n,
                scanrelid,
                schema,
                rows: scan.scan.plan.plan_rows,
                hash_rows,
            });
            Ok(HRes::Leaf { leaf, tl: &scan.scan.plan.targetlist, scanrelid })
        }
        NodeTag::T_HashJoin => {
            let hj = n.as_hash_join().expect("tag checked");
            if hj.join.jointype != ::types_nodes::jointype::JoinType::JOIN_INNER {
                return Err(jref("join-outer"));
            }
            if !hj.join.joinqual.is_nil() || !hj.join.plan.qual.is_nil() {
                return Err(jref("join-non-equi"));
            }
            if !hj.join.plan.initPlan.is_nil() {
                return Err(miss("node"));
            }
            let (Some(outer_p), Some(inner_p)) = (hj.join.plan.lefttree, hj.join.plan.righttree)
            else {
                return Err(miss("node"));
            };
            let Some(hash) = inner_p.as_hash() else { return Err(miss("node")) };
            if !hash.plan.initPlan.is_nil()
                || !hash.plan.qual.is_nil()
                || hash.plan.righttree.is_some()
            {
                return Err(miss("node"));
            }
            let Some(bchild) = hash.plan.lefttree else { return Err(miss("node")) };
            let outer = dwalk(estate, outer_p, 0, st)?;
            let inner = dwalk(estate, bchild, hash.plan.plan_rows.max(0.0) as u64, st)?;
            let level = HRes::Join {
                tl: &hj.join.plan.targetlist,
                hash_tl: &hash.plan.targetlist,
                outer: Box::new(outer),
                inner: Box::new(inner),
            };
            for cn in hj.hashclauses.iter() {
                let Some(op) = cn.as_op_expr() else { return Err(miss("pred")) };
                if op.args.len() != 2 {
                    return Err(miss("pred"));
                }
                let mut it = op.args.iter();
                let (Some(a), Some(b)) = (it.next(), it.next()) else {
                    return Err(miss("pred"));
                };
                let (Some(ra), Some(rb)) = (hres_at(&level, a), hres_at(&level, b)) else {
                    return Err(miss("pred"));
                };
                if ra.0 == rb.0 || !word_eq_proc(op.opfuncid) {
                    return Err(miss("pred"));
                }
                st.clauses.push((ra, rb));
            }
            Ok(level)
        }
        NodeTag::T_NestLoop | NodeTag::T_MergeJoin => Err(jref("join-multiway")),
        _ => Err(miss("node")),
    }
}

struct DGoal {
    /// One rec per leaf, in plan-walk (fill) order.
    leaves: Vec<SideRec>,
    fact: usize,
    build: usize,
    /// Stage index -> leaf index.
    order: Vec<usize>,
    /// (probe attno, build attno) in heap-attno space.
    keys: Vec<(u32, u32)>,
    /// Per stage: (dim attno, source attno, source side).
    dim_keys: Vec<Vec<(u32, u32, DimSrc)>>,
    routs: Vec<(usize, u32, u32)>,
    aggs: Vec<(JoinAggOp, Option<(usize, u32)>, Render)>,
    agg: bool,
    build_rows_hint: u64,
    dim_rows: Vec<u64>,
}

fn recognize_deep<'p>(
    estate: &EStateData<'_>,
    fp: u64,
    agg_top: Option<Node<'p>>,
    hj_node: Node<'p>,
) -> Option<JRec> {
    let miss = |w: &'static str| Some(JRec::Miss(Miss(w)));
    let jrefuse = |w: &'static str| Some(JRec::Refuse(RefuseCause::Heap(HeapDetail::Join(w))));
    let mut st = DFlat { leaves: Vec::new(), clauses: Vec::new() };
    let root = match dwalk(estate, hj_node, 0, &mut st) {
        Ok(r) => r,
        Err(j) => return Some(j),
    };
    let DFlat { leaves, clauses } = st;
    let nl = leaves.len();
    if nl > 2 + MAX_DIM_STAGES {
        return jrefuse("join-deep-stage-depth");
    }
    let word_ref = |leaf: usize, attno: u32, ty: u32, what: &'static str| -> Result<(), JRec> {
        if is_text_oid(ty) {
            return Err(JRec::Refuse(RefuseCause::Heap(HeapDetail::Join("join-text"))));
        }
        if ty == BPCHAROID {
            return Err(JRec::Refuse(RefuseCause::BpcharLaw { what }));
        }
        let cm = leaves[leaf].schema.iter().find(|c| c.attno == attno);
        let ok = is_word_oid(ty)
            && cm.is_some_and(|c| c.typ.byval && Face::of_class(c.class).word_foldable());
        if ok {
            Ok(())
        } else {
            Err(JRec::Miss(Miss("type")))
        }
    };
    for &(a, b) in &clauses {
        if let Err(r) = word_ref(a.0, a.1, a.2, "join-key") {
            return Some(r);
        }
        if let Err(r) = word_ref(b.0, b.1, b.2, "join-key") {
            return Some(r);
        }
    }
    // --- clause graph, fact election, stage order (the columnar law) ---------
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); nl];
    for (ci, c) in clauses.iter().enumerate() {
        adj[c.0 .0].push(ci);
        adj[c.1 .0].push(ci);
    }
    let partner = |ci: usize, me: usize| -> usize {
        let c = &clauses[ci];
        if c.0 .0 == me {
            c.1 .0
        } else {
            c.0 .0
        }
    };
    let reaches_all = |from: usize, skip: usize| -> bool {
        let mut seen = vec![false; nl];
        seen[from] = true;
        seen[skip] = true;
        let mut q = std::collections::VecDeque::from([from]);
        while let Some(u) = q.pop_front() {
            for &ci in &adj[u] {
                let v = partner(ci, u);
                if v != skip && !seen[v] {
                    seen[v] = true;
                    q.push_back(v);
                }
            }
        }
        seen.iter().all(|&s| s)
    };
    let mut fact: Option<(usize, usize)> = None;
    for li in 0..nl {
        let mut partners = adj[li].iter().map(|&ci| partner(ci, li));
        let Some(first) = partners.next() else { continue };
        if !partners.all(|p| p == first) {
            continue;
        }
        if !reaches_all(first, li) {
            continue;
        }
        if fact.is_none_or(|(f, _)| leaves[li].rows > leaves[f].rows) {
            fact = Some((li, first));
        }
    }
    let Some((fact, build_leaf)) = fact else {
        return jrefuse("join-deep-fact-dim-key");
    };
    let mut stage_of: Vec<Option<usize>> = vec![None; nl];
    let mut order: Vec<usize> = Vec::new();
    {
        let mut seen = vec![false; nl];
        seen[fact] = true;
        seen[build_leaf] = true;
        let mut q = std::collections::VecDeque::from([build_leaf]);
        while let Some(u) = q.pop_front() {
            for &ci in &adj[u] {
                let v = partner(ci, u);
                if !seen[v] {
                    seen[v] = true;
                    stage_of[v] = Some(order.len());
                    order.push(v);
                    q.push_back(v);
                }
            }
        }
        if !seen.iter().all(|&s| s) {
            return jrefuse("join-deep-no-key");
        }
    }
    // --- keys: fact<->build lanes; the rest stage as dim keys ----------------
    let mut keys: Vec<(u32, u32)> = Vec::new();
    let mut dim_keys: Vec<Vec<(u32, u32, DimSrc)>> = vec![Vec::new(); order.len()];
    for c in &clauses {
        if c.0 .0 == fact || c.1 .0 == fact {
            let ((_, pa, _), (_, ba, _)) = if c.0 .0 == fact { (c.0, c.1) } else { (c.1, c.0) };
            keys.push((pa, ba));
            continue;
        }
        let stg = |l: usize| stage_of[l];
        let ((ss, scol), (ds, dcol)) = match (stg(c.0 .0), stg(c.1 .0)) {
            (None, Some(d)) => ((None, c.0 .1), (d, c.1 .1)),
            (Some(d), None) => ((None, c.1 .1), (d, c.0 .1)),
            (Some(x), Some(y)) if x < y => ((Some(x), c.0 .1), (y, c.1 .1)),
            (Some(x), Some(y)) if x > y => ((Some(y), c.1 .1), (x, c.0 .1)),
            _ => return miss("pred"),
        };
        let src = match ss {
            None => DimSrc::Build,
            Some(j) => DimSrc::Dim(j as u8),
        };
        dim_keys[ds].push((dcol, scol, src));
    }
    if keys.is_empty() {
        return jrefuse("join-non-equi");
    }
    // --- outputs / agg legs ---------------------------------------------------
    let hj = hj_node.as_hash_join().expect("walked as HashJoin");
    let mut routs: Vec<(usize, u32, u32)> = Vec::new();
    let mut aggs: Vec<(JoinAggOp, Option<(usize, u32)>, Render)> = Vec::new();
    let agg = if let Some(atop) = agg_top {
        let a = atop.as_agg().expect("tag checked");
        if !agg_plain(a) {
            return miss("agg");
        }
        for tn in a.plan.targetlist.iter() {
            let Some(te) = tn.as_target_entry() else { return miss("proj") };
            if te.resjunk || te.expr.node_tag() != NodeTag::T_Aggref {
                return miss("proj");
            }
            let ar = te.expr.as_aggref().expect("tag checked");
            if !aggref_plain(ar) {
                return miss("agg");
            }
            if ar.aggfnoid == AGG_COUNT_STAR {
                aggs.push((JoinAggOp::CountStar, None, Render::CountI64));
                continue;
            }
            let Some((op, render)) = agg_op_render(ar.aggfnoid) else { return miss("agg") };
            if ar.args.len() != 1 {
                return miss("agg");
            }
            let Some(ate) = ar.args.iter().next().and_then(|x| x.as_target_entry()) else {
                return miss("agg");
            };
            // Agg-level Vars index the top join tlist (one hop).
            let hop = || -> Option<(usize, u32, u32)> {
                let v = ate.expr.as_var()?;
                if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno <= 0 {
                    return None;
                }
                hres_out(&root, v.varattno as usize)
            };
            let Some((leaf, attno, ty)) = hop() else {
                return miss("agg");
            };
            if let Err(r) = word_ref(leaf, attno, ty, "join-out") {
                return Some(r);
            }
            aggs.push((op, Some((leaf, attno)), render));
        }
        if aggs.is_empty() {
            return miss("agg");
        }
        true
    } else {
        for tn in hj.join.plan.targetlist.iter() {
            let Some(te) = tn.as_target_entry() else { return miss("proj") };
            if te.resjunk {
                return miss("proj");
            }
            let Some((leaf, attno, ty)) = hres_at(&root, te.expr) else {
                return miss("proj");
            };
            if let Err(r) = word_ref(leaf, attno, ty, "join-out") {
                return Some(r);
            }
            routs.push((leaf, attno, ty));
        }
        if routs.is_empty() {
            return miss("proj");
        }
        false
    };
    // --- per-leaf WHERE conjuncts (shared lowering; word lanes only) ---------
    let mut terms: Vec<Vec<PredTerm>> = Vec::with_capacity(nl);
    for l in leaves.iter() {
        let scan = l.node.as_seq_scan().expect("walked as SeqScan");
        let t = match lower_quals(&scan.scan.plan.qual, l.scanrelid, &l.schema, estate, fp) {
            Rec::Ok((t, var_terms)) => {
                if !var_terms.is_empty() {
                    return jrefuse("join-text");
                }
                t
            }
            Rec::Miss(m) => return Some(JRec::Miss(m)),
            Rec::Refuse(c) => return Some(JRec::Refuse(c)),
            Rec::Refused(r) => return Some(JRec::Refused(r)),
        };
        for pt in t.iter() {
            let Some(cm) = l.schema.iter().find(|c| c.attno == pt.col) else {
                return miss("type");
            };
            if !is_word_oid(cm.typ.oid)
                || !(cm.typ.byval && Face::of_class(cm.class).word_foldable())
            {
                return miss("type");
            }
        }
        terms.push(t);
    }
    // --- referenced sets -> leaf records --------------------------------------
    let mut cols: Vec<Vec<u32>> = vec![Vec::new(); nl];
    let add = |v: &mut Vec<u32>, a: u32| {
        if !v.contains(&a) {
            v.push(a);
        }
    };
    for &(pa, ba) in &keys {
        add(&mut cols[fact], pa);
        add(&mut cols[build_leaf], ba);
    }
    for (si, dk) in dim_keys.iter().enumerate() {
        for &(dcol, scol, src) in dk {
            add(&mut cols[order[si]], dcol);
            match src {
                DimSrc::Build => add(&mut cols[build_leaf], scol),
                DimSrc::Dim(j) => add(&mut cols[order[j as usize]], scol),
            }
        }
    }
    for (li, t) in terms.iter().enumerate() {
        for pt in t {
            add(&mut cols[li], pt.col);
        }
    }
    for &(leaf, attno, _) in &routs {
        add(&mut cols[leaf], attno);
    }
    for (_, input, _) in &aggs {
        if let Some((leaf, attno)) = input {
            add(&mut cols[*leaf], *attno);
        }
    }
    let dim_rows: Vec<u64> = order.iter().map(|&li| leaves[li].hash_rows).collect();
    let build_rows_hint = leaves[build_leaf].hash_rows;
    let mut recs: Vec<SideRec> = Vec::with_capacity(nl);
    for (li, l) in leaves.into_iter().enumerate() {
        let mut c = std::mem::take(&mut cols[li]);
        c.sort_unstable();
        recs.push(SideRec {
            scanrelid: l.scanrelid,
            schema: l.schema,
            cols: c,
            terms: std::mem::take(&mut terms[li]),
        });
    }
    Some(JRec::Deep(Box::new(DGoal {
        leaves: recs,
        fact,
        build: build_leaf,
        order,
        keys,
        dim_keys,
        routs,
        aggs,
        agg,
        build_rows_hint,
        dim_rows,
    })))
}

/// Seal every leaf in plan-walk order (outer spine first, then each
/// Hash child) — the same order `dwalk` numbered them.
fn fill_tree<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaves: &[SideRec],
    next: &mut usize,
    budget: &mut u64,
    banks: &mut Vec<Bank>,
) -> Result<(), Fail> {
    match node {
        crate::procnode::PlanStateNode::HashJoin(hj) => {
            fill_tree(&mut hj.outer, estate, leaves, next, budget, banks)?;
            fill_tree(&mut hj.hash.child, estate, leaves, next, budget, banks)
        }
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            if *next >= leaves.len() {
                return Err(Fail::Miss("state-shape"));
            }
            if ss.is_parallel() {
                return Err(Fail::Miss("run-parallel"));
            }
            let b = fill_side(ss, estate, &leaves[*next], budget)?;
            banks.push(b);
            *next += 1;
            Ok(())
        }
        _ => Err(Fail::Miss("state-shape")),
    }
}

/// The deep arm of the dispatch (admission already gated).
fn run_deep<'mcx, 'd>(
    estate: &mut EStateData<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    goal: Box<DGoal>,
    fp: u64,
    tup_desc: Option<&Rc<::types_tuple::TupleDescData<'static>>>,
    dest: &mut DestReceiver<'d>,
    relname: &str,
) -> Option<PgResult<()>> {
    let miss = |what: &'static str| {
        super::stat::tick_witness("heap-miss", what);
        None
    };
    let Some(td) = tup_desc else { return miss("proj") };
    let natts = if goal.agg { goal.aggs.len() } else { goal.routs.len() };
    if td.natts as usize != natts {
        return miss("proj");
    }
    let mut node: &mut crate::procnode::PlanStateNode<'mcx> = planstate;
    if goal.agg {
        let crate::procnode::PlanStateNode::Agg(aps) = node else {
            return miss("state-shape");
        };
        node = &mut aps.outer;
    }
    if !matches!(node, crate::procnode::PlanStateNode::HashJoin(_)) {
        return miss("state-shape");
    }
    let family: &'static str = if goal.agg { "heap-joindeepagg" } else { "heap-joindeep" };
    super::stat::tick_engaged(family, "A");
    super::stat::tick_witness("heap-join-membank", "-");

    // All seals share ONE budget; breach = the typed join refusal.
    let mut budget: u64 = 0;
    let mut banks: Vec<Bank> = Vec::with_capacity(goal.leaves.len());
    let mut next = 0usize;
    if let Err(f) = fill_tree(node, estate, &goal.leaves, &mut next, &mut budget, &mut banks) {
        return match f {
            Fail::Miss(what) => miss(what),
            Fail::Err(e) => Some(Err(e)),
            Fail::Refuse(c) => Some(Err(c.refuse(fp).into_error(relname))),
        };
    }
    if next != goal.leaves.len() {
        return miss("state-shape");
    }

    let faces: Vec<Faces> = banks.iter().map(|_| Faces::new(SqeConfig::heap_v1(1))).collect();
    let pool = heap_pool();
    let ctxs: Vec<SqeCtx> = banks
        .iter()
        .zip(faces.iter())
        .map(|(b, f)| SqeCtx { bank: b, pool, faces: f })
        .collect();
    let bctx = &ctxs[goal.build];
    let pctx = &ctxs[goal.fact];
    let dctxs: Vec<&SqeCtx> = goal.order.iter().map(|&li| &ctxs[li]).collect();
    let dim_banks: Vec<&Bank> = goal.order.iter().map(|&li| &banks[li]).collect();

    let jerr = |r: &sqe::joins::JoinRefuse| from_join(r).refuse(fp).into_error(relname);
    let rec_of = |li: usize| -> &SideRec { &goal.leaves[li] };
    let keys: Vec<JoinKey> = goal
        .keys
        .iter()
        .map(|&(pa, ba)| JoinKey {
            build_col: rec_of(goal.build).bank_col(ba),
            probe_col: rec_of(goal.fact).bank_col(pa),
        })
        .collect();
    let pred_of = |li: usize| -> Option<PredSpec> {
        let side = rec_of(li);
        if side.terms.is_empty() {
            return None;
        }
        Some(PredSpec::all(
            side.terms
                .iter()
                .map(|t| {
                    PredTerm::new(side.bank_col(t.col), t.op, t.lo, t.hi, side.meta(t.col).typ)
                })
                .collect(),
        ))
    };
    let dims: Vec<DimStage> = goal
        .order
        .iter()
        .enumerate()
        .map(|(si, &li)| DimStage {
            keys: goal.dim_keys[si]
                .iter()
                .map(|&(dcol, scol, src)| DimKey {
                    dim_col: rec_of(li).bank_col(dcol),
                    build_col: match src {
                        DimSrc::Build => rec_of(goal.build).bank_col(scol),
                        DimSrc::Dim(j) => rec_of(goal.order[j as usize]).bank_col(scol),
                    },
                    src,
                })
                .collect(),
            pred: pred_of(li),
            text_eqs: Vec::new(),
            rows_hint: goal.dim_rows[si],
        })
        .collect();
    let side_of = |li: usize| -> JoinSide {
        if li == goal.fact {
            JoinSide::Probe
        } else if li == goal.build {
            JoinSide::Build
        } else {
            let s = goal.order.iter().position(|&x| x == li).expect("staged leaf");
            JoinSide::Dim(s as u8)
        }
    };
    let out_of =
        |li: usize, attno: u32| JoinOut { side: side_of(li), col: rec_of(li).bank_col(attno) };
    let build_pred = pred_of(goal.build);
    let probe_pred = pred_of(goal.fact);

    let answers = if goal.agg {
        let reqs: Vec<JoinAggReq> = goal
            .aggs
            .iter()
            .map(|&(op, input, _)| JoinAggReq::col(op, input.map(|(li, a)| out_of(li, a))))
            .collect();
        let anode = match join_agg_node(
            &banks[goal.build],
            &banks[goal.fact],
            &dim_banks,
            0,
            JoinType::Inner,
            keys,
            Vec::new(),
            build_pred,
            probe_pred,
            dims,
            reqs,
            Vec::new(),
            goal.build_rows_hint,
        ) {
            Ok(n) => n,
            Err(e) => return Some(Err(jerr(&e))),
        };
        match run_hash_join_agg(bctx, pctx, &dctxs, &anode) {
            Ok(a) => a,
            Err(e) => return Some(Err(jerr(&e))),
        }
    } else {
        let jouts: Vec<JoinOut> = goal.routs.iter().map(|&(li, a, _)| out_of(li, a)).collect();
        let jnode = match join_node(
            &banks[goal.build],
            &banks[goal.fact],
            &dim_banks,
            0,
            JoinType::Inner,
            keys,
            Vec::new(),
            build_pred,
            probe_pred,
            dims,
            jouts,
            goal.build_rows_hint,
            false,
        ) {
            Ok(n) => n,
            Err(e) => return Some(Err(jerr(&e))),
        };
        let a = match run_hash_join(bctx, pctx, &dctxs, &jnode) {
            Ok(a) => a,
            Err(e) => return Some(Err(jerr(&e))),
        };
        // Joined-row emit cap: the grouped-answer bound, typed at breach.
        if a.nrows as u64 > sqe::planner::GROUP_ROW_CAP {
            return Some(Err(RefuseCause::MemoryBudget { family: FamilyKind::Join }
                .refuse(fp)
                .into_error(relname)));
        }
        a
    };

    let out: Vec<OutSlot> = if goal.agg {
        goal.aggs
            .iter()
            .enumerate()
            .map(|(i, (_, _, render))| OutSlot { src: OutSrc::Col(i), render: *render })
            .collect()
    } else {
        goal.routs
            .iter()
            .enumerate()
            .map(|(i, &(_, _, ty))| OutSlot { src: OutSrc::Col(i), render: key_render(ty) })
            .collect()
    };
    let r = deliver(
        estate,
        td.clone(),
        &answers,
        &out,
        &[],
        dest,
        0,
        usize::MAX,
        None,
        None,
    );
    if r.is_ok() {
        super::stat::tick_completed(family, "A");
    }
    Some(r.map(|_| ()))
}
