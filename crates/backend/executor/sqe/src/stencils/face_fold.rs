//! Fold stencil over the [`crate::face::ScanFace`] seam (heap-on-sqe,
//! heap-face.md §2/§3): ungrouped and grouped filtered scalar folds where
//! the source fills word/validity lanes per granule and the fold law runs
//! over the filled lanes.
//!
//! Two drives share one fold plan/state:
//! - [`run_face_fold`]: the v1 serial drive — the source fills EVERY
//!   referenced lane per granule on the caller thread (the fill owns the
//!   source's thread affinity: heap pins/bufmgr are backend state).
//! - [`run_pack_fold`]: the perf drive — the source STAGES detached
//!   granule packs on the caller thread (pins released before hand-off)
//!   and deform+filter+fold run over the packs, either inline or on the
//!   pool ([`Pool::run_feed`]: the leader stages while workers consume).
//!   Late materialization is structural here: pass 1 deforms only the
//!   predicate lanes, the filter runs, and pass 2 deforms the fold/key
//!   lanes for SURVIVORS only (dense when every row survived). Answers
//!   are width-independent: every partial is an exact fold (i128 sums,
//!   min/max, counts) merged associatively and emitted in normalized
//!   key order.
//!
//! Laws carried verbatim from the sealed-bank folds:
//! - 3VL: a NULL predicate operand fails the row (`PredTerm::filter_sel`
//!   via the validity lane); a NULL agg input among survivors is skipped
//!   (strict aggs); zero survivors render count 0 / NULL folds.
//! - Grouping: NULL keys form ONE group (SQL law); grouped answers emit
//!   key-ascending, NULL group last (any order is legal without a sort
//!   obligation; this one also satisfies the ASC NULLS LAST default).
//! - The cache law (heap-face.md §1.3): the run REFUSES a config with any
//!   persistent plane armed — nothing may outlive the statement.

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, Validity};
use crate::bank::Face;
use crate::face::{heap_lawful, FaceError, FaceFill, ScanFace};
use crate::fold::f64_from_key;
use crate::ir::{PredTerm, TopKKey, VarPredTerm};
use crate::pool::Pool;
use crate::spill::{ByteCursor, ChunkCursor, SpillMedium, SpillStore, SLAB_BYTES};
use crate::stencils::fused_filter_agg::ColAcc;
use crate::typmeta::TypMeta;
use crate::kernels_f6::FxHasher;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::sync::Arc;

/// The grouped hash arm's hasher: FxHash (the dense_domain/two_level
/// precedent) — word keys, per-row entry probes; SipHash here is pure
/// tax on the 1.05x heap admission band.
type Fx = std::hash::BuildHasherDefault<FxHasher>;
use std::sync::{Condvar, Mutex};

/// The grouped emit cap of record (ADJUDICATION-20260818 §down-pass: an
/// admitted bound `offset+count <= 2^20` needs no witness; narrow-word
/// key domains witness under the same cap).
pub const GROUP_CAP: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldOp {
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Copy, Debug)]
pub struct FoldLeg {
    pub op: FoldOp,
    /// None only for CountStar.
    pub col: Option<u32>,
    /// Answer column type (render currency).
    pub out: TypMeta,
    /// AVG exact-ratio law (width-8 inputs sum past 2^53).
    pub avg_exact: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct GroupKeyPart {
    pub col: u32,
    pub out: TypMeta,
}

#[derive(Clone, Copy, Debug)]
pub struct GroupSpec {
    pub col: u32,
    pub out: TypMeta,
    /// The admitted group-count witness (narrow-word type domain or a
    /// pushed bound), `<= GROUP_CAP`. A runtime breach is a hard error —
    /// the witness was wrong, never truncate silently.
    pub witness_cap: u64,
    /// [heap b1] Second key: the int+text composite on the byte-keyed
    /// arm (constant-offset word zone + text tail; NULLs encode in-key).
    pub second: Option<GroupKeyPart>,
}

#[derive(Clone, Debug)]
pub struct FaceFoldSpec {
    pub terms: Vec<PredTerm>,
    /// [heap rung 3] Varlena conjuncts (LIKE / NOT LIKE / Contains /
    /// NeEmpty — the engine's VarPredTerm currency) over byte lanes.
    /// Always part of the filter pass (never zone-prunable — heap has no
    /// zone plane anyway).
    pub var_terms: Vec<VarPredTerm>,
    pub legs: Vec<FoldLeg>,
    pub group: Option<GroupSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaceFoldErr {
    /// A caching plane was armed on a per-statement source (born-RED).
    CacheLaw,
    /// The face has no lane law for a touched column.
    Face(FaceError),
    /// Live groups exceeded the admitted witness (witness bug).
    GroupCap { cap: u64 },
}

// ---------------------------------------------------------------------------
// Plan + state (shared by both drives; parallel partials merge exactly)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum FoldNeed {
    Sum,
    MinMax,
    All,
}

/// The resolved fold plan: distinct referenced columns (lane order),
/// per-term/leg lane bindings, and the pass split for late
/// materialization (`pred_pairs` = predicate lanes, pass 1; `late_pairs`
/// = every other lane, deformed for survivors at pass 2).
pub struct FoldPlan {
    dcols: Vec<u32>,
    term_di: Vec<usize>,
    /// [heap rung 3] lane index per varlena conjunct (byte lanes).
    var_di: Vec<usize>,
    leg_di: Vec<Option<usize>>,
    gdi: Option<usize>,
    gdi2: Option<usize>,
    faces: Vec<Face>,
    fill_cols: Vec<(u32, Face)>,
    fold_dis: Vec<usize>,
    need_of: Vec<FoldNeed>,
    /// (lane, attno) pairs: predicate lanes, then the rest.
    pred_pairs: Vec<(usize, u32)>,
    late_pairs: Vec<(usize, u32)>,
}

impl FoldPlan {
    pub fn build(spec: &FaceFoldSpec, face_of: &dyn Fn(u32) -> Face) -> Result<FoldPlan, FaceFoldErr> {
        let mut dcols: Vec<u32> = Vec::new();
        let mut idx_of = |c: u32| -> usize {
            match dcols.iter().position(|&x| x == c) {
                Some(i) => i,
                None => {
                    dcols.push(c);
                    dcols.len() - 1
                }
            }
        };
        let gdi = spec.group.as_ref().map(|g| idx_of(g.col));
        let gdi2 = spec.group.as_ref().and_then(|g| g.second).map(|k| idx_of(k.col));
        let term_di: Vec<usize> = spec.terms.iter().map(|t| idx_of(t.col)).collect();
        let var_di: Vec<usize> = spec.var_terms.iter().map(|t| idx_of(t.col)).collect();
        let leg_di: Vec<Option<usize>> =
            spec.legs.iter().map(|l| l.col.map(&mut idx_of)).collect();
        let faces: Vec<Face> = dcols.iter().map(|&c| face_of(c)).collect();
        // Per-use face law (belt — admission upstream already gates):
        // int terms and fold legs need word faces; varlena conjuncts need
        // byte lanes; the group key may be either (word arms or the
        // byte-keyed hash arm).
        for &di in &term_di {
            if !faces[di].word_foldable() {
                return Err(FaceFoldErr::Face(FaceError { attno: dcols[di], what: "word-face" }));
            }
        }
        for &di in &var_di {
            if !matches!(faces[di], Face::Varlena) {
                return Err(FaceFoldErr::Face(FaceError { attno: dcols[di], what: "bytes-face" }));
            }
        }
        for di in leg_di.iter().flatten() {
            if !faces[*di].word_foldable() {
                return Err(FaceFoldErr::Face(FaceError { attno: dcols[*di], what: "word-face" }));
            }
        }
        if let Some(gdi) = gdi {
            if !faces[gdi].word_foldable() && !matches!(faces[gdi], Face::Varlena) {
                return Err(FaceFoldErr::Face(FaceError { attno: dcols[gdi], what: "key-face" }));
            }
        }
        if let (Some(g1), Some(g2)) = (gdi, gdi2) {
            // Composite law: one word zone + one byte tail, either order.
            let ok = (faces[g1].word_foldable() && matches!(faces[g2], Face::Varlena))
                || (matches!(faces[g1], Face::Varlena) && faces[g2].word_foldable());
            if !ok {
                return Err(FaceFoldErr::Face(FaceError { attno: dcols[g2], what: "key-face" }));
            }
        }
        // Sum/Avg stay integer-domain (float sums have no exact i64 fold
        // law here; admission refuses them upstream — belt).
        for (leg, di) in spec.legs.iter().zip(&leg_di) {
            if matches!(leg.op, FoldOp::Sum | FoldOp::Avg) {
                let f = faces[di.expect("sum/avg has a column")];
                if matches!(f, Face::F32 | Face::F64) {
                    return Err(FaceFoldErr::Face(FaceError {
                        attno: leg.col.unwrap(),
                        what: "float-sum",
                    }));
                }
            }
        }
        let fill_cols: Vec<(u32, Face)> =
            dcols.iter().zip(&faces).map(|(&c, &f)| (c, f)).collect();
        let mut fold_dis: Vec<usize> = leg_di.iter().flatten().copied().collect();
        fold_dis.sort_unstable();
        fold_dis.dedup();
        // Op-specialized fold election per column (law 11: the row loop
        // does only the work a leg reads — SUM/AVG want {sum,n}, MIN/MAX
        // want the extrema; the full ColAcc fold runs only for mixed asks).
        let need_of: Vec<FoldNeed> = fold_dis
            .iter()
            .map(|&di| {
                let mut sum = false;
                let mut mm = false;
                for (leg, ldi) in spec.legs.iter().zip(&leg_di) {
                    if *ldi == Some(di) {
                        match leg.op {
                            FoldOp::Sum | FoldOp::Avg => sum = true,
                            FoldOp::Min | FoldOp::Max => mm = true,
                            FoldOp::CountStar => {}
                        }
                    }
                }
                match (sum, mm) {
                    (true, false) => FoldNeed::Sum,
                    (false, true) => FoldNeed::MinMax,
                    _ => FoldNeed::All,
                }
            })
            .collect();
        let mut pred_dis: Vec<usize> = term_di.clone();
        pred_dis.extend_from_slice(&var_di);
        pred_dis.sort_unstable();
        pred_dis.dedup();
        let pred_pairs: Vec<(usize, u32)> = pred_dis.iter().map(|&di| (di, dcols[di])).collect();
        let late_pairs: Vec<(usize, u32)> = (0..dcols.len())
            .filter(|di| !pred_dis.contains(di))
            .map(|di| (di, dcols[di]))
            .collect();
        Ok(FoldPlan {
            dcols,
            term_di,
            var_di,
            leg_di,
            gdi,
            gdi2,
            faces,
            fill_cols,
            fold_dis,
            need_of,
            pred_pairs,
            late_pairs,
        })
    }

    pub fn ncols(&self) -> usize {
        self.dcols.len()
    }
}

struct GroupAcc {
    stars: u64,
    accs: Vec<ColAcc>,
}

/// Grouped accumulator: narrow word domains fold into a DENSE direct
/// array indexed `key - lo` (no per-row hash, no per-group allocation;
/// the last slot is the NULL group), wider witnessed domains keep the
/// hash arm; [heap rung 3] Varlena keys take the byte-keyed hash arm
/// (owned key copies; per-row probes borrow the lane slice — a key
/// allocates only on first sight). Answers are identical every arm.
enum Groups {
    Dense {
        lo: i64,
        n: usize,
        stars: Vec<u64>,
        /// `accs[di_pos * (n + 1) + slot]` for `fold_dis[di_pos]`.
        accs: Vec<ColAcc>,
    },
    Hash(HashMap<Option<i64>, GroupAcc, Fx>),
    BytesHash {
        /// hashbrown for `entry_ref`: ONE hash+probe per row on the hot
        /// arm (std's entry would force an owned key per probe; the
        /// contains+get_mut spelling costs a second lookup — measured
        /// 1.12-1.23x on topk-text, over the band).
        map: hashbrown::HashMap<Vec<u8>, GroupAcc, Fx>,
        /// The one NULL group (SQL law), kept out of the map so probes
        /// borrow `&[u8]` keys.
        null: Option<GroupAcc>,
    },
}

impl Groups {
    fn for_face(f: Face, nfold: usize) -> Groups {
        let dom = match f {
            Face::Bool => Some((0i64, 2usize)),
            Face::SignedWord(1) => Some((i8::MIN as i64, 256)),
            Face::SignedWord(2) => Some((i16::MIN as i64, 65536)),
            Face::UnsignedWord(1) => Some((0, 256)),
            Face::UnsignedWord(2) => Some((0, 65536)),
            Face::Varlena => {
                return Groups::BytesHash { map: hashbrown::HashMap::default(), null: None }
            }
            _ => None,
        };
        match dom {
            Some((lo, n)) => Groups::Dense {
                lo,
                n,
                stars: vec![0u64; n + 1],
                accs: vec![ColAcc::default(); nfold * (n + 1)],
            },
            None => Groups::Hash(HashMap::default()),
        }
    }
    fn len(&self) -> usize {
        match self {
            Groups::Dense { stars, .. } => stars.iter().filter(|&&s| s > 0).count(),
            Groups::Hash(m) => m.len(),
            Groups::BytesHash { map, null } => map.len() + null.is_some() as usize,
        }
    }
}

// ---------------------------------------------------------------------------
// [heap spill] The grouped hash arms' E18 write-through (spill-design.md
// extended to the face folds, RULING §15.15): a partial whose resident
// map crosses its budget share drains to a per-partial SORTED run
// through the spill substrate; finalize k-way-merges runs + residents
// and prices the TRUE answer plane against the E17 face — the witness
// cap retires for these arms (correct answer or typed refusal at real
// resource limits only). The Dense arm is domain-bounded and keeps its
// arm; spill disarmed (kill switch / no substrate) keeps the legacy
// witness-cap behavior verbatim.
// ---------------------------------------------------------------------------

static FACE_SPILL_FLUSHES: AtomicU64 = AtomicU64::new(0);
static FACE_SPILL_MERGES: AtomicU64 = AtomicU64::new(0);

/// Census snapshot: (sorted-run flushes, finalize k-way merges).
pub fn spill_counters() -> (u64, u64) {
    (FACE_SPILL_FLUSHES.load(AtOrd::Relaxed), FACE_SPILL_MERGES.load(AtOrd::Relaxed))
}

/// Serialized accumulator: sum/sumsq i128, n u64, four tagged Option<i64>.
const ACC_REC: usize = 16 + 16 + 8 + 4 * 9;
/// Word-key run record head: [tag u8][key i64][stars u64]; accs follow.
const WREC_HDR: usize = 1 + 8 + 8;
/// Resident-accounting constants: per-entry overhead beside the
/// accumulators, and one in-memory accumulator.
const ENTRY_MEM: usize = 64;
const ACC_MEM: usize = std::mem::size_of::<ColAcc>();

fn push_acc(buf: &mut Vec<u8>, a: &ColAcc) {
    buf.extend_from_slice(&a.sum.to_le_bytes());
    buf.extend_from_slice(&a.sumsq.to_le_bytes());
    buf.extend_from_slice(&a.n.to_le_bytes());
    for v in [a.min, a.max, a.band, a.bor] {
        buf.push(v.is_some() as u8);
        buf.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
    }
}

fn read_acc(b: &[u8]) -> ColAcc {
    let i128_at = |o: usize| i128::from_le_bytes(b[o..o + 16].try_into().expect("acc rec"));
    let opt_at = |o: usize| {
        (b[o] != 0).then(|| i64::from_le_bytes(b[o + 1..o + 9].try_into().expect("acc rec")))
    };
    ColAcc {
        sum: i128_at(0),
        sumsq: i128_at(16),
        n: u64::from_le_bytes(b[32..40].try_into().expect("acc rec")),
        min: opt_at(40),
        max: opt_at(49),
        band: opt_at(58),
        bor: opt_at(67),
    }
}

/// Only the fold-read lanes spill (R2: the record layout is a constant
/// of the arm that wrote it — `fold_dis` is a plan fact).
fn push_group(buf: &mut Vec<u8>, stars: u64, accs: &[ColAcc], fold_dis: &[usize]) {
    buf.extend_from_slice(&stars.to_le_bytes());
    for &di in fold_dis {
        push_acc(buf, &accs[di]);
    }
}

fn read_group(b: &[u8], ndc: usize, fold_dis: &[usize]) -> GroupAcc {
    let stars = u64::from_le_bytes(b[..8].try_into().expect("group rec"));
    let mut accs = vec![ColAcc::default(); ndc];
    for (i, &di) in fold_dis.iter().enumerate() {
        accs[di] = read_acc(&b[8 + i * ACC_REC..8 + (i + 1) * ACC_REC]);
    }
    GroupAcc { stars, accs }
}

fn merge_group(a: &mut GroupAcc, b: &GroupAcc) {
    a.stars += b.stars;
    for (x, y) in a.accs.iter_mut().zip(&b.accs) {
        x.merge(y);
    }
}

/// The word-key emit order (key ASC, NULL group last).
fn cmp_word_key(a: &Option<i64>, b: &Option<i64>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(x), Some(y)) => x.cmp(y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

/// Per-statement spill engagement facts for the grouped hash arms.
pub struct GroupSpillParams {
    store: Arc<dyn SpillStore>,
    /// Per-partial resident allowance, bytes (the E18 width law).
    share: usize,
    /// The E17 answer-plane budget finalize prices against.
    answer_budget: u64,
}

struct SpillRun {
    m: Arc<dyn SpillMedium>,
    off: u64,
    /// Word arm: record count. Byte arm: extent length in bytes.
    n: u64,
}

struct GroupSpill {
    store: Arc<dyn SpillStore>,
    share: usize,
    answer_budget: u64,
    worker: usize,
    m: Option<Arc<dyn SpillMedium>>,
    runs: Vec<SpillRun>,
    accounted: usize,
}

impl GroupSpill {
    fn medium(&mut self) -> Arc<dyn SpillMedium> {
        if self.m.is_none() {
            let f = self
                .store
                .file("face-group", self.worker)
                .unwrap_or_else(|e| crate::spill::io_fail("create", e));
            self.m = Some(Arc::from(f));
        }
        self.m.as_ref().expect("just seeded").clone()
    }
}

/// Drain a word-key resident map into one sorted committed run.
#[cold]
fn drain_word(sp: &mut GroupSpill, map: &mut HashMap<Option<i64>, GroupAcc, Fx>, fold_dis: &[usize]) {
    if map.is_empty() {
        return;
    }
    let mut rows: Vec<(Option<i64>, GroupAcc)> = map.drain().collect();
    rows.sort_unstable_by(|a, b| cmp_word_key(&a.0, &b.0));
    let m = sp.medium();
    let mut buf: Vec<u8> = Vec::new();
    let mut off: Option<u64> = None;
    for (k, v) in &rows {
        buf.push(k.is_none() as u8);
        buf.extend_from_slice(&k.unwrap_or(0).to_le_bytes());
        push_group(&mut buf, v.stars, &v.accs, fold_dis);
        if buf.len() >= SLAB_BYTES {
            let o = m.append(&buf).unwrap_or_else(|e| crate::spill::io_fail("append", e));
            off.get_or_insert(o);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        let o = m.append(&buf).unwrap_or_else(|e| crate::spill::io_fail("append", e));
        off.get_or_insert(o);
    }
    sp.runs.push(SpillRun { m, off: off.expect("nonempty run"), n: rows.len() as u64 });
    sp.accounted = 0;
    FACE_SPILL_FLUSHES.fetch_add(1, AtOrd::Relaxed);
}

/// Drain a byte-key resident map into one sorted committed run
/// (self-delimiting records: [klen u32][key][stars u64][accs]).
#[cold]
fn drain_bytes(
    sp: &mut GroupSpill,
    map: &mut hashbrown::HashMap<Vec<u8>, GroupAcc, Fx>,
    fold_dis: &[usize],
) {
    if map.is_empty() {
        return;
    }
    let mut rows: Vec<(Vec<u8>, GroupAcc)> = map.drain().collect();
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let m = sp.medium();
    let mut buf: Vec<u8> = Vec::new();
    let mut off: Option<u64> = None;
    let mut len: u64 = 0;
    for (k, v) in &rows {
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        push_group(&mut buf, v.stars, &v.accs, fold_dis);
        if buf.len() >= SLAB_BYTES {
            len += buf.len() as u64;
            let o = m.append(&buf).unwrap_or_else(|e| crate::spill::io_fail("append", e));
            off.get_or_insert(o);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        len += buf.len() as u64;
        let o = m.append(&buf).unwrap_or_else(|e| crate::spill::io_fail("append", e));
        off.get_or_insert(o);
    }
    sp.runs.push(SpillRun { m, off: off.expect("nonempty run"), n: len });
    sp.accounted = 0;
    FACE_SPILL_FLUSHES.fetch_add(1, AtOrd::Relaxed);
}

/// The finalize answer-bytes meter (the E17 face, the columnar
/// `check_answer_budget` twin): counts the TRUE answer plane group by
/// group during the merge and raises the typed runtime refusal the
/// moment the budget is crossed — never a truncation, never an OOM.
struct AnswerMeter {
    on: bool,
    got: u64,
    budget: u64,
    per_row: u64,
}

impl AnswerMeter {
    fn new(spill: &Option<GroupSpill>, per_row: u64) -> AnswerMeter {
        match spill {
            Some(sp) => AnswerMeter { on: true, got: 0, budget: sp.answer_budget, per_row },
            None => AnswerMeter { on: false, got: 0, budget: 0, per_row: 0 },
        }
    }

    fn add(&mut self, extra: u64) {
        if !self.on {
            return;
        }
        self.got += self.per_row + extra;
        if self.got > self.budget {
            crate::refuse::raise_runtime(crate::refuse::Refuse::GroupAnswerOverBudget {
                got: self.got,
                budget: self.budget,
            });
        }
    }
}

/// Rendered answer bytes per group row (keys always nullable on this
/// face; per-leg widths mirror the columnar answer pricing).
fn render_row_bytes(spec: &FaceFoldSpec, nkeys: u64) -> u64 {
    let mut b = nkeys * 9;
    for leg in &spec.legs {
        b += match leg.op {
            FoldOp::CountStar | FoldOp::Min | FoldOp::Max => 8,
            FoldOp::Sum => 16,
            FoldOp::Avg => 24,
        };
    }
    b
}

/// Decide + mint the statement's spill engagement for a grouped fold:
/// `None` = the shape has no spill arm (dense/ungrouped) or spill is
/// disarmed (kill switch or no registered substrate — the legacy
/// witness-cap arm then stands). A registered substrate that cannot
/// mint a store at statement time refuses typed (fail-closed).
fn group_spill_params(
    spec: &FaceFoldSpec,
    plan: &FoldPlan,
    cfg: &crate::engine::SqeConfig,
    width: usize,
) -> Option<GroupSpillParams> {
    let g = spec.group.as_ref()?;
    if !cfg.spill {
        return None;
    }
    let gdi = plan.gdi?;
    let dense = matches!(
        plan.faces[gdi],
        Face::Bool
            | Face::SignedWord(1)
            | Face::SignedWord(2)
            | Face::UnsignedWord(1)
            | Face::UnsignedWord(2)
    ) && g.second.is_none();
    if dense || !crate::spill::available() {
        return None;
    }
    let budget = cfg.grouped_budget_bytes();
    match crate::spill::new_store() {
        Some(store) => Some(GroupSpillParams {
            store,
            share: (budget / width.max(1) as u64).max(1) as usize,
            answer_budget: cfg.answer_budget_bytes(),
        }),
        None => crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
            what: "face-grouped",
            est: 0,
            budget,
        }),
    }
}

/// One drive's (or one worker's) exact partial fold.
pub struct FoldState {
    stars: u64,
    accs: Vec<ColAcc>,
    groups: Groups,
    /// Composite-key encode scratch (truncate-refill, law 11).
    kscratch: Vec<u8>,
    /// [heap spill] Armed only on the Hash/BytesHash grouped arms.
    spill: Option<GroupSpill>,
}

impl FoldState {
    fn new(
        plan: &FoldPlan,
        spec: &FaceFoldSpec,
        spill: Option<&GroupSpillParams>,
        worker: usize,
    ) -> FoldState {
        let groups = match (&spec.group, plan.gdi) {
            (Some(g), Some(_)) if g.second.is_some() => {
                Groups::BytesHash { map: hashbrown::HashMap::default(), null: None }
            }
            (Some(_), Some(gdi)) => Groups::for_face(plan.faces[gdi], plan.fold_dis.len()),
            _ => Groups::Hash(HashMap::default()),
        };
        let spill = match (&groups, spec.group.is_some(), spill) {
            (Groups::Hash(_) | Groups::BytesHash { .. }, true, Some(p)) => Some(GroupSpill {
                store: p.store.clone(),
                share: p.share,
                answer_budget: p.answer_budget,
                worker,
                m: None,
                runs: Vec::new(),
                accounted: 0,
            }),
            _ => None,
        };
        FoldState {
            stars: 0,
            accs: vec![ColAcc::default(); plan.dcols.len()],
            groups,
            kscratch: Vec::new(),
            spill,
        }
    }

    /// Fold the granule's survivors into this partial. `sel` is the
    /// survivor set; lanes only need defined cells at `sel` rows.
    fn absorb(
        &mut self,
        plan: &FoldPlan,
        spec: &FaceFoldSpec,
        fill: &FaceFill,
        sel: &[u16],
    ) -> Result<(), FaceFoldErr> {
        if sel.is_empty() {
            return Ok(());
        }
        match (&spec.group, plan.gdi) {
            (None, _) => {
                self.stars += sel.len() as u64;
                for (dp, &di) in plan.fold_dis.iter().enumerate() {
                    let c = &fill.cols[di];
                    let f = c.face;
                    let acc = &mut self.accs[di];
                    let av = c.all_valid();
                    match plan.need_of[dp] {
                        FoldNeed::Sum => {
                            for &r16 in sel {
                                let r = r16 as usize;
                                if av || c.row_valid(r) {
                                    acc.sum += f.word_key(c.words[r]) as i128;
                                    acc.n += 1;
                                }
                            }
                        }
                        FoldNeed::MinMax => {
                            for &r16 in sel {
                                let r = r16 as usize;
                                if av || c.row_valid(r) {
                                    let k = f.word_key(c.words[r]);
                                    acc.n += 1;
                                    acc.min = Some(acc.min.map_or(k, |m| m.min(k)));
                                    acc.max = Some(acc.max.map_or(k, |m| m.max(k)));
                                }
                            }
                        }
                        FoldNeed::All => {
                            for &r16 in sel {
                                let r = r16 as usize;
                                if av || c.row_valid(r) {
                                    acc.fold(f.word_key(c.words[r]), false);
                                }
                            }
                        }
                    }
                }
            }
            (Some(g), Some(gdi)) if g.second.is_some() => {
                let gdi2 = plan.gdi2.expect("composite key joins the fill set");
                let (wdi, tdi) =
                    if plan.faces[gdi].word_foldable() { (gdi, gdi2) } else { (gdi2, gdi) };
                let wc = &fill.cols[wdi];
                let tc = &fill.cols[tdi];
                let wf = wc.face;
                let nlegs = plan.dcols.len();
                let FoldState { groups, kscratch, spill, .. } = self;
                let Groups::BytesHash { map, .. } = groups else {
                    unreachable!("composite keys elect the byte-keyed arm")
                };
                let len0 = map.len();
                let mut new_key_bytes = 0usize;
                for &r16 in sel {
                    let r = r16 as usize;
                    // [wflag][8B biased BE word][tflag][text tail].
                    kscratch.clear();
                    if wc.all_valid() || wc.row_valid(r) {
                        kscratch.push(0);
                        let k = (wf.word_key(wc.words[r]) as u64) ^ (1u64 << 63);
                        kscratch.extend_from_slice(&k.to_be_bytes());
                    } else {
                        kscratch.push(1);
                        kscratch.extend_from_slice(&[0u8; 8]);
                    }
                    if tc.all_valid() || tc.row_valid(r) {
                        kscratch.push(0);
                        kscratch.extend_from_slice(tc.bytes_at(r));
                    } else {
                        kscratch.push(1);
                    }
                    let klen = kscratch.len();
                    let e = map.entry_ref(kscratch.as_slice()).or_insert_with(|| {
                        new_key_bytes += klen;
                        GroupAcc { stars: 0, accs: vec![ColAcc::default(); nlegs] }
                    });
                    e.stars += 1;
                    for &di in &plan.fold_dis {
                        let c = &fill.cols[di];
                        if c.all_valid() || c.row_valid(r) {
                            e.accs[di].fold(c.face.word_key(c.words[r]), false);
                        }
                    }
                }
                match spill {
                    Some(sp) => {
                        sp.accounted +=
                            new_key_bytes + (map.len() - len0) * (ENTRY_MEM + nlegs * ACC_MEM);
                        if sp.accounted > sp.share {
                            drain_bytes(sp, map, &plan.fold_dis);
                        }
                    }
                    None => {
                        if map.len() as u64 > g.witness_cap {
                            return Err(FaceFoldErr::GroupCap { cap: g.witness_cap });
                        }
                    }
                }
            }
            (Some(g), Some(gdi)) => {
                let kc = &fill.cols[gdi];
                let kf = kc.face;
                let FoldState { groups, spill, .. } = self;
                match groups {
                    Groups::Dense { lo, n, stars, accs } => {
                        let (lo, n) = (*lo, *n);
                        let kav = kc.all_valid();
                        for &r16 in sel {
                            let r = r16 as usize;
                            let slot = if kav || kc.row_valid(r) {
                                (kf.word_key(kc.words[r]) - lo) as usize
                            } else {
                                n
                            };
                            stars[slot] += 1;
                            for (dp, &di) in plan.fold_dis.iter().enumerate() {
                                let c = &fill.cols[di];
                                if c.all_valid() || c.row_valid(r) {
                                    let acc = &mut accs[dp * (n + 1) + slot];
                                    let k = c.face.word_key(c.words[r]);
                                    match plan.need_of[dp] {
                                        FoldNeed::Sum => {
                                            acc.sum += k as i128;
                                            acc.n += 1;
                                        }
                                        FoldNeed::MinMax => {
                                            acc.n += 1;
                                            acc.min = Some(acc.min.map_or(k, |m| m.min(k)));
                                            acc.max = Some(acc.max.map_or(k, |m| m.max(k)));
                                        }
                                        FoldNeed::All => acc.fold(k, false),
                                    }
                                }
                            }
                        }
                    }
                    Groups::Hash(map) => {
                        let nlegs = plan.dcols.len();
                        for &r16 in sel {
                            let r = r16 as usize;
                            let key = (kc.all_valid() || kc.row_valid(r))
                                .then(|| kf.word_key(kc.words[r]));
                            let e = map.entry(key).or_insert_with(|| GroupAcc {
                                stars: 0,
                                accs: vec![ColAcc::default(); nlegs],
                            });
                            e.stars += 1;
                            for &di in &plan.fold_dis {
                                let c = &fill.cols[di];
                                if c.all_valid() || c.row_valid(r) {
                                    e.accs[di].fold(c.face.word_key(c.words[r]), false);
                                }
                            }
                        }
                        match spill {
                            Some(sp) => {
                                sp.accounted = map.len() * (ENTRY_MEM + nlegs * ACC_MEM);
                                if sp.accounted > sp.share {
                                    drain_word(sp, map, &plan.fold_dis);
                                }
                            }
                            None => {
                                if map.len() as u64 > g.witness_cap {
                                    return Err(FaceFoldErr::GroupCap { cap: g.witness_cap });
                                }
                            }
                        }
                    }
                    Groups::BytesHash { map, null } => {
                        let nlegs = plan.dcols.len();
                        let len0 = map.len();
                        let mut new_key_bytes = 0usize;
                        for &r16 in sel {
                            let r = r16 as usize;
                            let e = if kc.all_valid() || kc.row_valid(r) {
                                // entry_ref: one hash+probe; the owned key
                                // materializes only on first sight.
                                let kb = kc.bytes_at(r);
                                let klen = kb.len();
                                map.entry_ref(kb).or_insert_with(|| {
                                    new_key_bytes += klen;
                                    GroupAcc {
                                        stars: 0,
                                        accs: vec![ColAcc::default(); nlegs],
                                    }
                                })
                            } else {
                                null.get_or_insert_with(|| GroupAcc {
                                    stars: 0,
                                    accs: vec![ColAcc::default(); nlegs],
                                })
                            };
                            e.stars += 1;
                            for &di in &plan.fold_dis {
                                let c = &fill.cols[di];
                                if c.all_valid() || c.row_valid(r) {
                                    e.accs[di].fold(c.face.word_key(c.words[r]), false);
                                }
                            }
                        }
                        match spill {
                            Some(sp) => {
                                sp.accounted += new_key_bytes
                                    + (map.len() - len0) * (ENTRY_MEM + nlegs * ACC_MEM);
                                if sp.accounted > sp.share {
                                    drain_bytes(sp, map, &plan.fold_dis);
                                }
                            }
                            None => {
                                if (map.len() + null.is_some() as usize) as u64 > g.witness_cap {
                                    return Err(FaceFoldErr::GroupCap { cap: g.witness_cap });
                                }
                            }
                        }
                    }
                }
            }
            (Some(_), None) => unreachable!("group key joins the fill set"),
        }
        Ok(())
    }

    /// Merge another partial in (associative + exact: i128 sums, counts,
    /// extrema, per-slot/per-key accumulator merges — answers are
    /// width-independent by construction).
    fn merge(&mut self, o: FoldState) {
        self.stars += o.stars;
        for (a, b) in self.accs.iter_mut().zip(&o.accs) {
            a.merge(b);
        }
        // Committed runs travel with the merge; resident maps combine in
        // memory (bounded by the statement's E18 budget across partials).
        if let Some(b) = o.spill {
            debug_assert!(self.spill.is_some(), "spill arm is a plan fact");
            if let Some(a) = &mut self.spill {
                a.runs.extend(b.runs);
            }
        }
        match (&mut self.groups, o.groups) {
            (
                Groups::Dense { stars: sa, accs: aa, .. },
                Groups::Dense { stars: sb, accs: ab, .. },
            ) => {
                for (a, b) in sa.iter_mut().zip(&sb) {
                    *a += *b;
                }
                for (a, b) in aa.iter_mut().zip(&ab) {
                    a.merge(b);
                }
            }
            (Groups::Hash(ma), Groups::Hash(mb)) => {
                for (k, v) in mb {
                    match ma.entry(k) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(v);
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            let a = e.get_mut();
                            a.stars += v.stars;
                            for (x, y) in a.accs.iter_mut().zip(&v.accs) {
                                x.merge(y);
                            }
                        }
                    }
                }
            }
            (
                Groups::BytesHash { map: ma, null: na },
                Groups::BytesHash { map: mb, null: nb },
            ) => {
                let merge_acc = |a: &mut GroupAcc, v: &GroupAcc| {
                    a.stars += v.stars;
                    for (x, y) in a.accs.iter_mut().zip(&v.accs) {
                        x.merge(y);
                    }
                };
                for (k, v) in mb {
                    match ma.entry(k) {
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            e.insert(v);
                        }
                        hashbrown::hash_map::Entry::Occupied(mut e) => {
                            merge_acc(e.get_mut(), &v);
                        }
                    }
                }
                if let Some(v) = nb {
                    match na {
                        Some(a) => merge_acc(a, &v),
                        None => *na = Some(v),
                    }
                }
            }
            _ => unreachable!("group arm is a plan fact, identical across partials"),
        }
    }

    fn into_answers(self, plan: &FoldPlan, spec: &FaceFoldSpec) -> Result<AnswerSet, FaceFoldErr> {
        // Un-embed a word min/max per the column's face (the float answer
        // rides the F64 class; Fixed/varlena refused above).
        let word_answer = |out: TypMeta, f: Face, keys: Vec<Option<i64>>| -> AnswerCol {
            match f {
                Face::F32 | Face::F64 => {
                    let mask: Vec<bool> = keys.iter().map(|k| k.is_some()).collect();
                    let v: Vec<f64> =
                        keys.iter().map(|k| k.map(f64_from_key).unwrap_or(0.0)).collect();
                    let mut c = AnswerCol::f64s(out, v);
                    if mask.iter().any(|&b| !b) {
                        c.validity = crate::answer::Validity::Mask(mask);
                    }
                    c
                }
                _ => AnswerCol::i64s_opt(out, keys),
            }
        };

        let FoldState { stars: tstars, accs: taccs, groups, spill, .. } = self;
        let Some(g) = &spec.group else {
            let out_cols: Vec<AnswerCol> = spec
                .legs
                .iter()
                .zip(&plan.leg_di)
                .map(|(leg, di)| leg_answer(leg, *di, tstars, &taccs, &plan.faces, &word_answer))
                .collect();
            return Ok(AnswerSet::from_cols(out_cols));
        };
        // [heap spill] With the spill arm engaged the witness cap is
        // RETIRED — the finalize answer meter is the replacement law;
        // disarmed keeps the legacy cap verbatim.
        if spill.is_none() && groups.len() as u64 > g.witness_cap {
            return Err(FaceFoldErr::GroupCap { cap: g.witness_cap });
        }
        // Normalize every arm into key-ascending rows, NULL group last
        // (satisfies the ASC NULLS LAST obligation and is a legal
        // arbitrary order otherwise). accs indexed by di.
        let ndc = plan.dcols.len();
        let rows_v: Vec<(Option<i64>, u64, Vec<ColAcc>)> = match groups {
            // [heap rung 3] Byte-keyed arm: key-ascending BYTE order
            // (memcmp — the C-collation law; Rust slice cmp IS PG's
            // C-collation text order: memcmp then length), NULL group
            // last. The key column emits Bytes (Render::Text).
            Groups::BytesHash { map, null } => {
                let nkeys = if g.second.is_some() { 2 } else { 1 };
                let stage =
                    (std::mem::size_of::<(Vec<u8>, GroupAcc)>() + ndc * ACC_MEM) as u64;
                let mut meter =
                    AnswerMeter::new(&spill, render_row_bytes(spec, nkeys) + stage);
                let (ks, null) = bytes_groups(map, null, spill, plan, ndc, &mut meter);
                return Ok(bytes_answers(g, spec, plan, ks, null, &word_answer));
            }
            Groups::Dense { lo, n, stars, accs } => {
                let mut out = Vec::new();
                for slot in 0..=n {
                    if stars[slot] == 0 {
                        continue;
                    }
                    let key = (slot < n).then(|| lo + slot as i64);
                    let mut by_di = vec![ColAcc::default(); ndc];
                    for (dp, &di) in plan.fold_dis.iter().enumerate() {
                        by_di[di] = accs[dp * (n + 1) + slot];
                    }
                    out.push((key, stars[slot], by_di));
                }
                out
            }
            Groups::Hash(map) => {
                let stage = (std::mem::size_of::<(Option<i64>, u64, Vec<ColAcc>)>()
                    + ndc * ACC_MEM) as u64;
                let mut meter = AnswerMeter::new(&spill, render_row_bytes(spec, 1) + stage);
                word_groups(map, spill, plan, ndc, &mut meter)
            }
        };
        let keys: Vec<Option<i64>> = rows_v.iter().map(|r| r.0).collect();
        let mut cols: Vec<AnswerCol> = Vec::with_capacity(1 + spec.legs.len());
        cols.push(AnswerCol::i64s_opt(g.out, keys));
        for (leg, di) in spec.legs.iter().zip(&plan.leg_di) {
            cols.push(grouped_leg_answer(leg, *di, &rows_v, &plan.faces, &word_answer));
        }
        Ok(AnswerSet::from_cols(cols))
    }
}

/// Reset `sel` to the granule's survivors under `spec.terms` +
/// `spec.var_terms` (3VL: a NULL operand fails the row — VarPredTerm's
/// `eval_v`, critically for NOT LIKE / NeEmpty).
fn apply_terms(plan: &FoldPlan, spec: &FaceFoldSpec, fill: &FaceFill, sel: &mut Vec<u16>) {
    let rows = fill.rows as usize;
    sel.clear();
    sel.extend((0..rows).map(|r| r as u16));
    for (ti, t) in spec.terms.iter().enumerate() {
        let c = &fill.cols[plan.term_di[ti]];
        let f = c.face;
        if c.all_valid() {
            t.filter_sel(sel, |_| true, |r| f.word_key(c.words[r]));
        } else {
            t.filter_sel(sel, |r| c.row_valid(r), |r| f.word_key(c.words[r]));
        }
    }
    for (vi, t) in spec.var_terms.iter().enumerate() {
        let c = &fill.cols[plan.var_di[vi]];
        let av = c.all_valid();
        let mut w = 0usize;
        for i in 0..sel.len() {
            let r = sel[i] as usize;
            let keep = t.eval_v(c.bytes_at(r), av || c.row_valid(r));
            sel[w] = sel[i];
            w += keep as usize;
        }
        sel.truncate(w);
    }
}

/// Resolve the word-keyed groups to emit-ordered rows, k-way-merging
/// committed runs when the partials spilled; the meter prices each
/// emitted group against the E17 answer face.
fn word_groups(
    map: HashMap<Option<i64>, GroupAcc, Fx>,
    spill: Option<GroupSpill>,
    plan: &FoldPlan,
    ndc: usize,
    meter: &mut AnswerMeter,
) -> Vec<(Option<i64>, u64, Vec<ColAcc>)> {
    let mut res: Vec<(Option<i64>, GroupAcc)> = map.into_iter().collect();
    res.sort_unstable_by(|a, b| cmp_word_key(&a.0, &b.0));
    let (runs, share) = match spill {
        Some(s) => (s.runs, s.share),
        None => (Vec::new(), 0),
    };
    if runs.is_empty() {
        return res
            .into_iter()
            .map(|(k, v)| {
                meter.add(0);
                (k, v.stars, v.accs)
            })
            .collect();
    }
    FACE_SPILL_MERGES.fetch_add(1, AtOrd::Relaxed);
    let rec = WREC_HDR + plan.fold_dis.len() * ACC_REC;
    let slab = (share / (runs.len() + 1)).clamp(rec, SLAB_BYTES);
    let mut curs: Vec<ChunkCursor> =
        runs.iter().map(|r| ChunkCursor::new(&*r.m, r.off, r.n, rec, slab)).collect();
    let pull = |c: &mut ChunkCursor| -> Option<(Option<i64>, GroupAcc)> {
        c.next().map(|b| {
            let k = (b[0] == 0).then(|| i64::from_le_bytes(b[1..9].try_into().expect("wrec")));
            (k, read_group(&b[9..], ndc, &plan.fold_dis))
        })
    };
    let mut heads: Vec<Option<(Option<i64>, GroupAcc)>> =
        curs.iter_mut().map(|c| pull(c)).collect();
    let mut res_it = res.into_iter().peekable();
    let mut out: Vec<(Option<i64>, u64, Vec<ColAcc>)> = Vec::new();
    loop {
        let mut best: Option<Option<i64>> = res_it.peek().map(|(k, _)| *k);
        for h in heads.iter().flatten() {
            if best.is_none_or(|b| cmp_word_key(&h.0, &b) == std::cmp::Ordering::Less) {
                best = Some(h.0);
            }
        }
        let Some(bk) = best else { break };
        let mut acc: Option<GroupAcc> = None;
        let mut take = |v: GroupAcc, acc: &mut Option<GroupAcc>| match acc {
            Some(a) => merge_group(a, &v),
            None => *acc = Some(v),
        };
        while res_it.peek().is_some_and(|(k, _)| *k == bk) {
            let (_, v) = res_it.next().expect("peeked");
            take(v, &mut acc);
        }
        for (i, h) in heads.iter_mut().enumerate() {
            while h.as_ref().is_some_and(|(k, _)| *k == bk) {
                let (_, v) = h.take().expect("checked");
                take(v, &mut acc);
                *h = pull(&mut curs[i]);
            }
        }
        meter.add(0);
        let a = acc.expect("best came from a stream");
        out.push((bk, a.stars, a.accs));
    }
    out
}

/// The byte-keyed twin of [`word_groups`] (self-delimiting run records;
/// the resident NULL group never spills and re-joins at emit).
fn bytes_groups(
    map: hashbrown::HashMap<Vec<u8>, GroupAcc, Fx>,
    null: Option<GroupAcc>,
    spill: Option<GroupSpill>,
    plan: &FoldPlan,
    ndc: usize,
    meter: &mut AnswerMeter,
) -> (Vec<(Vec<u8>, GroupAcc)>, Option<GroupAcc>) {
    let mut res: Vec<(Vec<u8>, GroupAcc)> = map.into_iter().collect();
    res.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let (runs, share) = match spill {
        Some(s) => (s.runs, s.share),
        None => (Vec::new(), 0),
    };
    let out = if runs.is_empty() {
        for (k, _) in &res {
            meter.add(k.len() as u64);
        }
        res
    } else {
        FACE_SPILL_MERGES.fetch_add(1, AtOrd::Relaxed);
        let grp = 8 + plan.fold_dis.len() * ACC_REC;
        let slab = (share / (runs.len() + 1)).clamp(64, SLAB_BYTES);
        let mut curs: Vec<ByteCursor> =
            runs.iter().map(|r| ByteCursor::new(&*r.m, r.off, r.n, slab)).collect();
        let pull = |c: &mut ByteCursor| -> Option<(Vec<u8>, GroupAcc)> {
            let lb = c.take(4)?;
            let klen = u32::from_le_bytes(lb.try_into().expect("brec")) as usize;
            let key = c.take(klen).expect("writer law").to_vec();
            let gb = c.take(grp).expect("writer law");
            Some((key, read_group(gb, ndc, &plan.fold_dis)))
        };
        let mut heads: Vec<Option<(Vec<u8>, GroupAcc)>> =
            curs.iter_mut().map(|c| pull(c)).collect();
        let mut res_it = res.into_iter().peekable();
        let mut out: Vec<(Vec<u8>, GroupAcc)> = Vec::new();
        loop {
            let mut best: Option<Vec<u8>> = res_it.peek().map(|(k, _)| k.clone());
            for h in heads.iter().flatten() {
                if best.as_ref().is_none_or(|b| h.0 < *b) {
                    best = Some(h.0.clone());
                }
            }
            let Some(bk) = best else { break };
            let mut acc: Option<GroupAcc> = None;
            let mut take = |v: GroupAcc, acc: &mut Option<GroupAcc>| match acc {
                Some(a) => merge_group(a, &v),
                None => *acc = Some(v),
            };
            while res_it.peek().is_some_and(|(k, _)| *k == bk) {
                let (_, v) = res_it.next().expect("peeked");
                take(v, &mut acc);
            }
            for (i, h) in heads.iter_mut().enumerate() {
                while h.as_ref().is_some_and(|(k, _)| *k == bk) {
                    let (_, v) = h.take().expect("checked");
                    take(v, &mut acc);
                    *h = pull(&mut curs[i]);
                }
            }
            meter.add(bk.len() as u64);
            out.push((bk, acc.expect("best came from a stream")));
        }
        out
    };
    if null.is_some() {
        meter.add(0);
    }
    (out, null)
}

/// Byte-keyed answer build over emit-ordered owned rows (single text
/// key or the [b1] composite; the arms above already normalized order).
fn bytes_answers(
    g: &GroupSpec,
    spec: &FaceFoldSpec,
    plan: &FoldPlan,
    ks: Vec<(Vec<u8>, GroupAcc)>,
    null: Option<GroupAcc>,
    word_answer: &impl Fn(TypMeta, Face, Vec<Option<i64>>) -> AnswerCol,
) -> AnswerSet {
    if let Some(g2) = &g.second {
        // [b1] Composite decode by constant-offset zones.
        let mut wkeys: Vec<Option<i64>> = Vec::with_capacity(ks.len());
        let mut tb = BytesBuild::new();
        let mut rows_v: Vec<(Option<i64>, u64, Vec<ColAcc>)> = Vec::with_capacity(ks.len());
        for (k, v) in ks {
            wkeys.push((k[0] == 0).then(|| {
                let z: [u8; 8] = k[1..9].try_into().expect("fixed zone");
                (u64::from_be_bytes(z) ^ (1u64 << 63)) as i64
            }));
            tb.push_opt((k[9] == 0).then(|| &k[10..]));
            rows_v.push((None, v.stars, v.accs));
        }
        let word_first = plan.faces[plan.gdi.expect("grouped plan")].word_foldable();
        let (wout, tout) = if word_first { (g.out, g2.out) } else { (g2.out, g.out) };
        let wcol = AnswerCol::i64s_opt(wout, wkeys);
        let tcol = tb.finish(tout);
        let mut cols: Vec<AnswerCol> = Vec::with_capacity(2 + spec.legs.len());
        if word_first {
            cols.push(wcol);
            cols.push(tcol);
        } else {
            cols.push(tcol);
            cols.push(wcol);
        }
        for (leg, di) in spec.legs.iter().zip(&plan.leg_di) {
            cols.push(grouped_leg_answer(leg, *di, &rows_v, &plan.faces, word_answer));
        }
        debug_assert!(null.is_none(), "composite NULL combos live in the map");
        return AnswerSet::from_cols(cols);
    }
    let nrows = ks.len() + null.is_some() as usize;
    let mut kb = BytesBuild::new();
    let mut rows_v: Vec<(Option<i64>, u64, Vec<ColAcc>)> = Vec::with_capacity(nrows);
    for (k, v) in ks {
        kb.push(&k);
        rows_v.push((None, v.stars, v.accs));
    }
    let mut key_col = kb.finish(g.out);
    if let Some(nv) = null {
        // NULL group last: empty key bytes + a validity mask.
        if let crate::answer::ColData::Bytes { offs, .. } = &mut key_col.data {
            offs.push(*offs.last().expect("BytesBuild seeds offs"));
        }
        let mut mask = vec![true; nrows];
        mask[nrows - 1] = false;
        key_col.validity = Validity::Mask(mask);
        rows_v.push((None, nv.stars, nv.accs));
    }
    let mut cols: Vec<AnswerCol> = Vec::with_capacity(1 + spec.legs.len());
    cols.push(key_col);
    for (leg, di) in spec.legs.iter().zip(&plan.leg_di) {
        cols.push(grouped_leg_answer(leg, *di, &rows_v, &plan.faces, word_answer));
    }
    AnswerSet::from_cols(cols)
}

// ---------------------------------------------------------------------------
// Serial drive over ScanFace (the v1 fill-everything contract)
// ---------------------------------------------------------------------------

pub fn run_face_fold(
    face: &mut dyn ScanFace,
    spec: &FaceFoldSpec,
    cfg: &crate::engine::SqeConfig,
    fill: &mut FaceFill,
) -> Result<AnswerSet, FaceFoldErr> {
    if !heap_lawful(cfg) {
        return Err(FaceFoldErr::CacheLaw);
    }
    let plan = FoldPlan::build(spec, &|c| face.face(c))?;
    let spill = group_spill_params(spec, &plan, cfg, 1);
    let mut state = FoldState::new(&plan, spec, spill.as_ref(), 0);
    let mut sel: Vec<u16> = Vec::new();
    for u in 0..face.n_units() {
        crate::cancel::checkpoint();
        fill.reset(&plan.fill_cols);
        face.fill(u, &plan.dcols, fill).map_err(FaceFoldErr::Face)?;
        if fill.rows == 0 {
            continue;
        }
        apply_terms(&plan, spec, fill, &mut sel);
        state.absorb(&plan, spec, fill, &sel)?;
    }
    state.into_answers(&plan, spec)
}

// ---------------------------------------------------------------------------
// Pack drive: staged detached granules; late materialization; the pool
// ---------------------------------------------------------------------------

/// A worker-side deformer over detached packs: pure memory, no backend
/// state — the Send bound is the whole point.
pub trait PackDeform<P>: Send {
    /// Rows staged in the pack.
    fn rows(&self, pack: &P) -> u32;
    /// Deform `cols` (lane index, attno) into `out` at their pack row
    /// positions (`FaceFill::begin_rows` was called). `sel = None` =
    /// every row; `Some(sel)` = only the listed ordinals must be defined
    /// (late materialization pass 2 — undeformed cells stay invalid and
    /// no consumer reads them).
    fn deform(
        &mut self,
        pack: &P,
        cols: &[(usize, u32)],
        sel: Option<&[u16]>,
        out: &mut FaceFill,
    ) -> Result<(), FaceError>;
}

/// A source whose granules can be staged into detached packs on the
/// leader thread (pins released before hand-off) and consumed anywhere.
pub trait PackSource {
    type Pack: Send;
    type Deformer: PackDeform<Self::Pack>;
    fn n_units(&self) -> usize;
    fn face(&self, attno: u32) -> Face;
    fn new_pack(&self) -> Self::Pack;
    fn deformer(&self) -> Self::Deformer;
    /// Leader-side stage of granule `unit` into `pack` (truncate-refill;
    /// zero pins held at return, error paths included).
    fn stage(&mut self, unit: usize, pack: &mut Self::Pack) -> Result<(), FaceError>;
}

/// Consumer seam: fold and top-n share the stage/feed/late-mat law.
trait PackSinkKind: Sync {
    type State: Send;
    /// `worker` names the partial for the spill file naming law.
    fn new_state(&self, plan: &FoldPlan, worker: usize) -> Self::State;
    fn absorb(
        &self,
        st: &mut Self::State,
        plan: &FoldPlan,
        fill: &FaceFill,
        sel: &[u16],
    ) -> Result<(), FaceFoldErr>;
    fn merge(&self, a: &mut Self::State, b: Self::State);
}

struct FoldKind<'a> {
    spec: &'a FaceFoldSpec,
    spill: Option<GroupSpillParams>,
}

impl PackSinkKind for FoldKind<'_> {
    type State = FoldState;
    fn new_state(&self, plan: &FoldPlan, worker: usize) -> FoldState {
        FoldState::new(plan, self.spec, self.spill.as_ref(), worker)
    }
    fn absorb(
        &self,
        st: &mut FoldState,
        plan: &FoldPlan,
        fill: &FaceFill,
        sel: &[u16],
    ) -> Result<(), FaceFoldErr> {
        st.absorb(plan, self.spec, fill, sel)
    }
    fn merge(&self, a: &mut FoldState, b: FoldState) {
        a.merge(b);
    }
}

/// Deform + filter + absorb one staged pack into `state` (the worker
/// body; also the inline drive's body — identical code, hence identical
/// answers at every width).
fn process_pack<P, D: PackDeform<P>, K: PackSinkKind>(
    d: &mut D,
    pack: &P,
    plan: &FoldPlan,
    spec: &FaceFoldSpec,
    kind: &K,
    fill: &mut FaceFill,
    sel: &mut Vec<u16>,
    state: &mut K::State,
) -> Result<(), FaceFoldErr> {
    let rows = d.rows(pack);
    if rows == 0 {
        return Ok(());
    }
    fill.reset(&plan.fill_cols);
    fill.begin_rows(rows);
    d.deform(pack, &plan.pred_pairs, None, fill).map_err(FaceFoldErr::Face)?;
    apply_terms(plan, spec, fill, sel);
    if sel.is_empty() {
        return Ok(());
    }
    if !plan.late_pairs.is_empty() {
        // Adaptive: full-survival granules deform dense (the column-major
        // arm); partial survival deforms survivors only.
        let s = if sel.len() == rows as usize { None } else { Some(&sel[..]) };
        d.deform(pack, &plan.late_pairs, s, fill).map_err(FaceFoldErr::Face)?;
    }
    kind.absorb(state, plan, fill, sel)
}

struct FeedQ<P> {
    work: VecDeque<P>,
    free: Vec<P>,
    closed: bool,
    err: Option<FaceFoldErr>,
}

struct Feed<P> {
    m: Mutex<FeedQ<P>>,
    work_cv: Condvar,
    free_cv: Condvar,
}

impl<P> Feed<P> {
    fn fail(&self, e: FaceFoldErr) {
        let mut q = self.m.lock().unwrap();
        if q.err.is_none() {
            q.err = Some(e);
        }
        q.closed = true;
        drop(q);
        self.work_cv.notify_all();
        self.free_cv.notify_all();
    }
    fn close(&self) {
        let mut q = self.m.lock().unwrap();
        q.closed = true;
        drop(q);
        self.work_cv.notify_all();
        self.free_cv.notify_all();
    }
}

/// Close-on-drop belt: a leader unwind (cancel checkpoint, stage panic)
/// must still close the feed, or the generation join deadlocks.
struct CloseGuard<'a, P>(&'a Feed<P>);
impl<P> Drop for CloseGuard<'_, P> {
    fn drop(&mut self) {
        self.0.close();
    }
}

pub fn run_pack_fold<S: PackSource>(
    src: &mut S,
    spec: &FaceFoldSpec,
    cfg: &crate::engine::SqeConfig,
    pool: Option<(&Pool, usize)>,
) -> Result<AnswerSet, FaceFoldErr> {
    if !heap_lawful(cfg) {
        return Err(FaceFoldErr::CacheLaw);
    }
    let plan = FoldPlan::build(spec, &|c| src.face(c))?;
    let width =
        pool.map_or(1, |(p, w)| w.min(p.threads())).min(src.n_units().max(1)).max(1);
    let kind = FoldKind { spec, spill: group_spill_params(spec, &plan, cfg, width) };
    let state = run_pack_drive(src, spec, &plan, pool, &kind)?;
    state.into_answers(&plan, spec)
}

fn run_pack_drive<S: PackSource, K: PackSinkKind>(
    src: &mut S,
    spec: &FaceFoldSpec,
    plan: &FoldPlan,
    pool: Option<(&Pool, usize)>,
    kind: &K,
) -> Result<K::State, FaceFoldErr> {
    let n = src.n_units();
    let width = pool.map_or(1, |(p, w)| w.min(p.threads())).min(n.max(1));
    // Inline drive: one pack, leader does everything (also the no-deform
    // shapes — nothing for workers to do there).
    if width <= 1 || plan.ncols() == 0 {
        let mut state = kind.new_state(plan, 0);
        let mut d = src.deformer();
        let mut pack = src.new_pack();
        let mut fill = FaceFill::new();
        let mut sel: Vec<u16> = Vec::new();
        for u in 0..n {
            crate::cancel::checkpoint();
            src.stage(u, &mut pack).map_err(FaceFoldErr::Face)?;
            process_pack(&mut d, &pack, plan, spec, kind, &mut fill, &mut sel, &mut state)?;
        }
        return Ok(state);
    }

    let (pool, _) = pool.expect("width > 1 has a pool");
    let feed: Feed<S::Pack> = Feed {
        m: Mutex::new(FeedQ {
            work: VecDeque::with_capacity(width + 2),
            free: (0..width + 2).map(|_| src.new_pack()).collect(),
            closed: false,
            err: None,
        }),
        work_cv: Condvar::new(),
        free_cv: Condvar::new(),
    };
    let slots: Vec<Mutex<Option<K::State>>> =
        (0..pool.threads()).map(|_| Mutex::new(None)).collect();
    let cancel = crate::cancel::current();
    // Deformers are built leader-side (source access is leader-only);
    // each engaged worker pops its own.
    let deformers: Mutex<Vec<S::Deformer>> =
        Mutex::new((0..width).map(|_| src.deformer()).collect());
    {
        let plan_r = plan;
        let feed_r = &feed;
        let slots_r = &slots;
        let cancel_r = &cancel;
        let deformers_r = &deformers;
        let kind_r = kind;
        let gen = pool.run_feed(width, move |t| {
            let _inh = crate::cancel::inherit(cancel_r);
            let mut d = deformers_r
                .lock()
                .unwrap()
                .pop()
                .expect("one deformer per engaged worker");
            let mut fill = FaceFill::new();
            let mut sel: Vec<u16> = Vec::new();
            let mut state = kind_r.new_state(plan_r, t);
            loop {
                if crate::cancel::fired_of(cancel_r) {
                    break;
                }
                let pack = {
                    let mut q = feed_r.m.lock().unwrap();
                    loop {
                        if q.err.is_some() {
                            break None;
                        }
                        if let Some(p) = q.work.pop_front() {
                            break Some(p);
                        }
                        if q.closed {
                            break None;
                        }
                        q = feed_r.work_cv.wait(q).unwrap();
                    }
                };
                let Some(pack) = pack else { break };
                let r =
                    process_pack(&mut d, &pack, plan_r, spec, kind_r, &mut fill, &mut sel, &mut state);
                {
                    let mut q = feed_r.m.lock().unwrap();
                    q.free.push(pack);
                }
                feed_r.free_cv.notify_one();
                if let Err(e) = r {
                    feed_r.fail(e);
                    break;
                }
            }
            *slots_r[t].lock().unwrap() = Some(state);
        });
        let _close = CloseGuard(&feed);
        // Leader loop: stage granules into free packs, feed the workers.
        for u in 0..n {
            crate::cancel::checkpoint();
            let pack = {
                let mut q = feed.m.lock().unwrap();
                loop {
                    if q.err.is_some() || q.closed {
                        break None;
                    }
                    if let Some(p) = q.free.pop() {
                        break Some(p);
                    }
                    q = feed.free_cv.wait(q).unwrap();
                }
            };
            let Some(mut pack) = pack else { break };
            if let Err(e) = src.stage(u, &mut pack) {
                feed.fail(FaceFoldErr::Face(e));
                break;
            }
            {
                let mut q = feed.m.lock().unwrap();
                q.work.push_back(pack);
            }
            feed.work_cv.notify_one();
        }
        drop(_close); // close the feed; workers drain and finish
        gen.join();
    }
    if let Some(e) = feed.m.lock().unwrap().err {
        return Err(e);
    }
    let mut state = kind.new_state(plan, 0);
    for s in &slots {
        if let Some(p) = s.lock().unwrap().take() {
            kind.merge(&mut state, p);
        }
    }
    Ok(state)
}

// ---------------------------------------------------------------------------
// Top-n sink: bounded candidates per drive/worker, merged exactly. The
// caller closes `keys` over every leg, so the comparator is total and
// the kept set width/schedule-independent (equal rows are identical).
// ---------------------------------------------------------------------------

fn cmp_cells(keys: &[TopKKey], a: &[Option<i64>], b: &[Option<i64>]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for k in keys {
        let ord = match (a[k.col as usize], b[k.col as usize]) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => {
                if k.nulls_first { Ordering::Less } else { Ordering::Greater }
            }
            (Some(_), None) => {
                if k.nulls_first { Ordering::Greater } else { Ordering::Less }
            }
            (Some(x), Some(y)) => {
                let o = x.cmp(&y);
                if k.desc { o.reverse() } else { o }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub struct TopNState {
    n: usize,
    /// Max-heap by `keys` (the worst kept row at the root).
    cands: Vec<Vec<Option<i64>>>,
    scratch: Vec<Option<i64>>,
}

impl TopNState {
    fn new(n: usize) -> TopNState {
        TopNState { n, cands: Vec::new(), scratch: Vec::new() }
    }

    /// Offer `scratch` into the bounded set.
    fn offer(&mut self, keys: &[TopKKey]) {
        use std::cmp::Ordering;
        if self.n == 0 {
            return;
        }
        if self.cands.len() == self.n {
            if cmp_cells(keys, &self.scratch, &self.cands[0]) != Ordering::Less {
                return;
            }
            self.cands[0].clear();
            self.cands[0].extend_from_slice(&self.scratch);
            let mut i = 0usize;
            loop {
                let (l, r) = (2 * i + 1, 2 * i + 2);
                let mut m = i;
                if l < self.cands.len()
                    && cmp_cells(keys, &self.cands[l], &self.cands[m]) == Ordering::Greater
                {
                    m = l;
                }
                if r < self.cands.len()
                    && cmp_cells(keys, &self.cands[r], &self.cands[m]) == Ordering::Greater
                {
                    m = r;
                }
                if m == i {
                    break;
                }
                self.cands.swap(i, m);
                i = m;
            }
        } else {
            self.cands.push(self.scratch.clone());
            let mut i = self.cands.len() - 1;
            while i > 0 {
                let p = (i - 1) / 2;
                if cmp_cells(keys, &self.cands[i], &self.cands[p]) == Ordering::Greater {
                    self.cands.swap(i, p);
                    i = p;
                } else {
                    break;
                }
            }
        }
    }

    fn absorb(&mut self, plan: &FoldPlan, keys: &[TopKKey], fill: &FaceFill, sel: &[u16]) {
        for &r16 in sel {
            let r = r16 as usize;
            self.scratch.clear();
            for di in &plan.leg_di {
                let di = di.expect("top-n legs carry columns");
                let c = &fill.cols[di];
                let v = (c.all_valid() || c.row_valid(r)).then(|| c.face.word_key(c.words[r]));
                self.scratch.push(v);
            }
            self.offer(keys);
        }
    }

    fn merge(&mut self, keys: &[TopKKey], o: TopNState) {
        for c in o.cands {
            self.scratch.clear();
            self.scratch.extend_from_slice(&c);
            self.offer(keys);
        }
    }

    fn into_answers(mut self, spec: &FaceFoldSpec, keys: &[TopKKey]) -> AnswerSet {
        self.cands.sort_unstable_by(|a, b| cmp_cells(keys, a, b));
        let cols: Vec<AnswerCol> = spec
            .legs
            .iter()
            .enumerate()
            .map(|(j, leg)| {
                AnswerCol::i64s_opt(leg.out, self.cands.iter().map(|c| c[j]).collect())
            })
            .collect();
        AnswerSet::from_cols(cols)
    }
}

struct TopNKind<'a> {
    keys: &'a [TopKKey],
    n: usize,
}

impl PackSinkKind for TopNKind<'_> {
    type State = TopNState;
    fn new_state(&self, _plan: &FoldPlan, _worker: usize) -> TopNState {
        TopNState::new(self.n)
    }
    fn absorb(
        &self,
        st: &mut TopNState,
        plan: &FoldPlan,
        fill: &FaceFill,
        sel: &[u16],
    ) -> Result<(), FaceFoldErr> {
        st.absorb(plan, self.keys, fill, sel);
        Ok(())
    }
    fn merge(&self, a: &mut TopNState, b: TopNState) {
        a.merge(self.keys, b);
    }
}

pub fn run_face_topn(
    face: &mut dyn ScanFace,
    spec: &FaceFoldSpec,
    keys: &[TopKKey],
    n: usize,
    cfg: &crate::engine::SqeConfig,
    fill: &mut FaceFill,
) -> Result<AnswerSet, FaceFoldErr> {
    if !heap_lawful(cfg) {
        return Err(FaceFoldErr::CacheLaw);
    }
    let plan = FoldPlan::build(spec, &|c| face.face(c))?;
    let mut state = TopNState::new(n);
    let mut sel: Vec<u16> = Vec::new();
    for u in 0..face.n_units() {
        crate::cancel::checkpoint();
        fill.reset(&plan.fill_cols);
        face.fill(u, &plan.dcols, fill).map_err(FaceFoldErr::Face)?;
        if fill.rows == 0 {
            continue;
        }
        apply_terms(&plan, spec, fill, &mut sel);
        state.absorb(&plan, keys, fill, &sel);
    }
    Ok(state.into_answers(spec, keys))
}

pub fn run_pack_topn<S: PackSource>(
    src: &mut S,
    spec: &FaceFoldSpec,
    keys: &[TopKKey],
    n: usize,
    cfg: &crate::engine::SqeConfig,
    pool: Option<(&Pool, usize)>,
) -> Result<AnswerSet, FaceFoldErr> {
    if !heap_lawful(cfg) {
        return Err(FaceFoldErr::CacheLaw);
    }
    let plan = FoldPlan::build(spec, &|c| src.face(c))?;
    let kind = TopNKind { keys, n };
    let state = run_pack_drive(src, spec, &plan, pool, &kind)?;
    Ok(state.into_answers(spec, keys))
}

fn leg_answer(
    leg: &FoldLeg,
    di: Option<usize>,
    stars: u64,
    accs: &[ColAcc],
    faces: &[Face],
    word_answer: &impl Fn(TypMeta, Face, Vec<Option<i64>>) -> AnswerCol,
) -> AnswerCol {
    match leg.op {
        FoldOp::CountStar => AnswerCol::i64s(leg.out, vec![stars as i64]),
        FoldOp::Sum => {
            let a = &accs[di.expect("sum has a column")];
            let mut c = AnswerCol::i128s(leg.out, vec![a.sum]);
            if a.n == 0 {
                c.validity = crate::answer::Validity::Mask(vec![false]);
            }
            c
        }
        FoldOp::Avg => {
            let a = &accs[di.expect("avg has a column")];
            AnswerCol::ratios(leg.out, vec![(a.sum, a.n as i64)], leg.avg_exact)
        }
        FoldOp::Min => {
            let di = di.expect("min has a column");
            word_answer(leg.out, faces[di], vec![accs[di].min])
        }
        FoldOp::Max => {
            let di = di.expect("max has a column");
            word_answer(leg.out, faces[di], vec![accs[di].max])
        }
    }
}

fn grouped_leg_answer(
    leg: &FoldLeg,
    di: Option<usize>,
    rows_v: &[(Option<i64>, u64, Vec<ColAcc>)],
    faces: &[Face],
    word_answer: &impl Fn(TypMeta, Face, Vec<Option<i64>>) -> AnswerCol,
) -> AnswerCol {
    match leg.op {
        FoldOp::CountStar => {
            AnswerCol::i64s(leg.out, rows_v.iter().map(|r| r.1 as i64).collect())
        }
        FoldOp::Sum => {
            let di = di.expect("sum has a column");
            let pairs: Vec<(i128, u64)> =
                rows_v.iter().map(|r| (r.2[di].sum, r.2[di].n)).collect();
            let mut c =
                AnswerCol::i128s(leg.out, pairs.iter().map(|&(s, _)| s).collect());
            if pairs.iter().any(|&(_, n)| n == 0) {
                c.validity = crate::answer::Validity::Mask(
                    pairs.iter().map(|&(_, n)| n > 0).collect(),
                );
            }
            c
        }
        FoldOp::Avg => {
            let di = di.expect("avg has a column");
            AnswerCol::ratios(
                leg.out,
                rows_v.iter().map(|r| (r.2[di].sum, r.2[di].n as i64)).collect(),
                leg.avg_exact,
            )
        }
        FoldOp::Min => {
            let di = di.expect("min has a column");
            word_answer(leg.out, faces[di], rows_v.iter().map(|r| r.2[di].min).collect())
        }
        FoldOp::Max => {
            let di = di.expect("max has a column");
            word_answer(leg.out, faces[di], rows_v.iter().map(|r| r.2[di].max).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answer::ColData;
    use crate::engine::SqeConfig;
    use crate::ir::CmpOp;

    /// A word-lane test face: fixed granules of (col -> rows) data with
    /// explicit NULLs. Proves the seam contract without any storage.
    struct VecFace {
        // per unit, per col: (attno, face, rows of Option<i64>)
        units: Vec<Vec<(u32, Face, Vec<Option<i64>>)>>,
    }

    impl ScanFace for VecFace {
        fn n_units(&self) -> usize {
            self.units.len()
        }
        fn face(&self, attno: u32) -> Face {
            self.units[0]
                .iter()
                .find(|c| c.0 == attno)
                .map(|c| c.1)
                .expect("attno")
        }
        fn null_free(&self, _attno: u32) -> bool {
            false
        }
        fn rows_total(&self) -> Option<u64> {
            None
        }
        fn fill(
            &mut self,
            unit: usize,
            cols: &[u32],
            out: &mut FaceFill,
        ) -> Result<(), FaceError> {
            let u = &self.units[unit];
            let rows = u[0].2.len();
            for (ci, &attno) in cols.iter().enumerate() {
                let col = u.iter().find(|c| c.0 == attno).expect("attno");
                for r in 0..rows {
                    match col.2[r] {
                        Some(v) => out.push(ci, v as u64, false),
                        None => out.push(ci, 0, true),
                    }
                }
            }
            out.seal(rows as u32);
            Ok(())
        }
    }

    /// The same data as a pack source: pack = the granule's rows,
    /// deform = positional writes (sel-aware), proving the pack drive's
    /// late-mat + width identity against the serial drive.
    struct VecPacks {
        units: Vec<Vec<(u32, Face, Vec<Option<i64>>)>>,
    }

    #[derive(Default)]
    struct VecPack {
        cols: Vec<(u32, Vec<Option<i64>>)>,
        rows: u32,
    }

    struct VecDeformer;

    impl PackDeform<VecPack> for VecDeformer {
        fn rows(&self, pack: &VecPack) -> u32 {
            pack.rows
        }
        fn deform(
            &mut self,
            pack: &VecPack,
            cols: &[(usize, u32)],
            sel: Option<&[u16]>,
            out: &mut FaceFill,
        ) -> Result<(), FaceError> {
            for &(ci, attno) in cols {
                let col = pack.cols.iter().find(|c| c.0 == attno).expect("attno");
                let write = |out: &mut FaceFill, r: usize| {
                    let (words, vwords, nulls) = out.lane_mut(ci);
                    match col.1[r] {
                        Some(v) => {
                            words[r] = v as u64;
                            vwords[r >> 6] |= 1u64 << (r & 63);
                        }
                        None => *nulls += 1,
                    }
                };
                match sel {
                    None => {
                        for r in 0..pack.rows as usize {
                            write(out, r);
                        }
                    }
                    Some(sel) => {
                        for &r in sel {
                            write(out, r as usize);
                        }
                    }
                }
            }
            Ok(())
        }
    }

    impl PackSource for VecPacks {
        type Pack = VecPack;
        type Deformer = VecDeformer;
        fn n_units(&self) -> usize {
            self.units.len()
        }
        fn face(&self, attno: u32) -> Face {
            self.units[0]
                .iter()
                .find(|c| c.0 == attno)
                .map(|c| c.1)
                .expect("attno")
        }
        fn new_pack(&self) -> VecPack {
            VecPack::default()
        }
        fn deformer(&self) -> VecDeformer {
            VecDeformer
        }
        fn stage(&mut self, unit: usize, pack: &mut VecPack) -> Result<(), FaceError> {
            let u = &self.units[unit];
            pack.cols.clear();
            pack.cols
                .extend(u.iter().map(|(a, _, v)| (*a, v.clone())));
            pack.rows = u[0].2.len() as u32;
            Ok(())
        }
    }

    fn leg(op: FoldOp, col: Option<u32>) -> FoldLeg {
        FoldLeg { op, col, out: TypMeta::INT8, avg_exact: false }
    }

    fn corpus_units(nunits: usize, rows: usize) -> Vec<Vec<(u32, Face, Vec<Option<i64>>)>> {
        (0..nunits)
            .map(|u| {
                let f = |i: usize, m: i64, nz: usize| -> Option<i64> {
                    let x = (u * rows + i) as i64;
                    (x as usize % nz != 0).then_some(x % m)
                };
                vec![
                    (1, Face::SignedWord(4), (0..rows).map(|i| f(i, 997, 13)).collect()),
                    (2, Face::SignedWord(8), (0..rows).map(|i| f(i, 100_000, 7)).collect()),
                    (3, Face::SignedWord(2), (0..rows).map(|i| f(i, 37, 11)).collect()),
                ]
            })
            .collect()
    }

    fn spec_corpus() -> Vec<FaceFoldSpec> {
        vec![
            FaceFoldSpec {
                terms: vec![],
                var_terms: vec![],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
                group: None,
            },
            FaceFoldSpec {
                terms: vec![PredTerm::new(1, CmpOp::Eq, 7, 0, TypMeta::INT4)],
                var_terms: vec![],
                legs: vec![
                    leg(FoldOp::CountStar, None),
                    leg(FoldOp::Sum, Some(2)),
                    leg(FoldOp::Min, Some(2)),
                    leg(FoldOp::Max, Some(2)),
                ],
                group: None,
            },
            FaceFoldSpec {
                terms: vec![PredTerm::new(1, CmpOp::Ne, 5, 0, TypMeta::INT4)],
                var_terms: vec![],
                legs: vec![leg(FoldOp::Avg, Some(2)), leg(FoldOp::Min, Some(1))],
                group: None,
            },
            FaceFoldSpec {
                terms: vec![PredTerm::new(1, CmpOp::Between, -1_000_000, 399, TypMeta::INT4)],
                var_terms: vec![],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
                group: Some(GroupSpec { col: 3, out: TypMeta::INT8, witness_cap: 1 << 16, second: None }),
            },
        ]
    }

    fn assert_answers_eq(a: &AnswerSet, b: &AnswerSet, what: &str) {
        assert_eq!(a.nrows, b.nrows, "{what}: nrows");
        assert_eq!(a.cols.len(), b.cols.len(), "{what}: ncols");
        for (i, (x, y)) in a.cols.iter().zip(&b.cols).enumerate() {
            assert_eq!(x.data, y.data, "{what}: col {i} data");
            for r in 0..a.nrows {
                assert_eq!(
                    x.validity.is_valid(r),
                    y.validity.is_valid(r),
                    "{what}: col {i} row {r} validity"
                );
            }
        }
    }

    /// The pack drive (inline AND pooled at several widths, late-mat
    /// engaged by the filtered specs) answers identically to the serial
    /// ScanFace drive — the width-independence + late-mat identity law.
    #[test]
    fn pack_drive_width_and_latemat_identity() {
        let units = corpus_units(7, 300);
        let cfg = SqeConfig::heap_v1(1);
        for (si, spec) in spec_corpus().iter().enumerate() {
            let mut fill = FaceFill::new();
            let oracle = run_face_fold(
                &mut VecFace { units: units.clone() },
                spec,
                &cfg,
                &mut fill,
            )
            .unwrap();
            let inline = run_pack_fold(
                &mut VecPacks { units: units.clone() },
                spec,
                &cfg,
                None,
            )
            .unwrap();
            assert_answers_eq(&oracle, &inline, &format!("spec {si} inline"));
            for width in [2usize, 3, 8] {
                let pool = Pool::new(width);
                let pooled = run_pack_fold(
                    &mut VecPacks { units: units.clone() },
                    spec,
                    &cfg,
                    Some((&pool, width)),
                )
                .unwrap();
                assert_answers_eq(&oracle, &pooled, &format!("spec {si} width {width}"));
            }
        }
    }

    #[test]
    fn pack_drive_cache_law_born_red() {
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.stats_cache = true;
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None)],
            group: None,
        };
        assert_eq!(
            run_pack_fold(&mut VecPacks { units: vec![] }, &spec, &cfg, None),
            Err(FaceFoldErr::CacheLaw)
        );
    }

    #[test]
    fn pack_drive_group_cap_breach_is_hard_error_pooled() {
        // 64 distinct int8 keys against a witness cap of 4: the hash arm
        // must fail (worker-side or at the post-merge belt), never emit.
        let units: Vec<Vec<(u32, Face, Vec<Option<i64>>)>> = (0..4)
            .map(|u| {
                vec![(
                    1,
                    Face::SignedWord(8),
                    (0..64).map(|i| Some((u * 64 + i) as i64)).collect(),
                )]
            })
            .collect();
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None)],
            group: Some(GroupSpec { col: 1, out: TypMeta::INT8, witness_cap: 4, second: None }),
        };
        let pool = Pool::new(3);
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.spill = false;
        let r = run_pack_fold(&mut VecPacks { units }, &spec, &cfg, Some((&pool, 3)));
        assert_eq!(r, Err(FaceFoldErr::GroupCap { cap: 4 }));
    }

    #[test]
    fn ungrouped_filtered_fold_3vl() {
        // col 1 (filter), col 2 (agg input); NULLs in both.
        let mut f = VecFace {
            units: vec![
                vec![
                    (1, Face::SignedWord(8), vec![Some(1), Some(2), None, Some(1)]),
                    (2, Face::SignedWord(8), vec![Some(10), Some(20), Some(30), None]),
                ],
                vec![
                    (1, Face::SignedWord(8), vec![Some(1)]),
                    (2, Face::SignedWord(8), vec![Some(5)]),
                ],
            ],
        };
        let spec = FaceFoldSpec {
            terms: vec![PredTerm::new(1, CmpOp::Eq, 1, 0, TypMeta::INT8)],
            var_terms: vec![],
            legs: vec![
                leg(FoldOp::CountStar, None),
                leg(FoldOp::Sum, Some(2)),
                leg(FoldOp::Min, Some(2)),
                leg(FoldOp::Max, Some(2)),
            ],
            group: None,
        };
        let mut fill = FaceFill::new();
        let a =
            run_face_fold(&mut f, &spec, &SqeConfig::heap_v1(1), &mut fill).unwrap();
        // survivors: rows with col1 = 1 (NULL fails): 2 in unit 0, 1 in unit 1.
        assert_eq!(a.cols[0].data, ColData::I64(vec![3]));
        // sum over non-NULL survivor inputs: 10 + 5 (the NULL input row skipped).
        assert_eq!(a.cols[1].data, ColData::I128(vec![15]));
        assert_eq!(a.cols[2].data, ColData::I64(vec![5]));
        assert_eq!(a.cols[3].data, ColData::I64(vec![10]));
    }

    #[test]
    fn empty_fold_renders_null_min() {
        let mut f = VecFace {
            units: vec![vec![(1, Face::SignedWord(8), vec![Some(5)])]],
        };
        let spec = FaceFoldSpec {
            terms: vec![PredTerm::new(1, CmpOp::Eq, 99, 0, TypMeta::INT8)],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Min, Some(1))],
            group: None,
        };
        let mut fill = FaceFill::new();
        let a =
            run_face_fold(&mut f, &spec, &SqeConfig::heap_v1(1), &mut fill).unwrap();
        assert_eq!(a.cols[0].data, ColData::I64(vec![0]));
        assert!(!a.cols[1].validity.is_valid(0), "MIN over empty is NULL");
    }

    #[test]
    fn grouped_fold_null_key_one_group_key_asc() {
        let mut f = VecFace {
            units: vec![vec![
                (1, Face::SignedWord(2), vec![Some(2), None, Some(1), None, Some(2)]),
                (2, Face::SignedWord(8), vec![Some(10), Some(1), Some(3), Some(2), Some(30)]),
            ]],
        };
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
            group: Some(GroupSpec { col: 1, out: TypMeta::INT8, witness_cap: 1 << 16, second: None }),
        };
        let mut fill = FaceFill::new();
        let a =
            run_face_fold(&mut f, &spec, &SqeConfig::heap_v1(1), &mut fill).unwrap();
        assert_eq!(a.nrows, 3);
        // keys ascending, NULL group last.
        assert_eq!(a.cols[0].data, ColData::I64(vec![1, 2, 0]));
        assert!(a.cols[0].validity.is_valid(0));
        assert!(!a.cols[0].validity.is_valid(2));
        assert_eq!(a.cols[1].data, ColData::I64(vec![1, 2, 2]));
        assert_eq!(a.cols[2].data, ColData::I128(vec![3, 40, 3]));
    }

    #[test]
    fn cache_law_born_red() {
        let mut f = VecFace { units: vec![] };
        let spec =
            FaceFoldSpec { terms: vec![], var_terms: vec![], legs: vec![leg(FoldOp::CountStar, None)], group: None };
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.stats_cache = true; // wrongly enabled cache plane
        let mut fill = FaceFill::new();
        assert_eq!(
            run_face_fold(&mut f, &spec, &cfg, &mut fill),
            Err(FaceFoldErr::CacheLaw),
            "a seeded persistent plane MUST fail the run (born-RED)"
        );
    }

    #[test]
    fn group_cap_breach_is_a_hard_error() {
        let mut f = VecFace {
            units: vec![vec![(
                1,
                Face::SignedWord(8),
                (0..10).map(|i| Some(i)).collect(),
            )]],
        };
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None)],
            group: Some(GroupSpec { col: 1, out: TypMeta::INT8, witness_cap: 4, second: None }),
        };
        let mut fill = FaceFill::new();
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.spill = false;
        assert_eq!(
            run_face_fold(&mut f, &spec, &cfg, &mut fill),
            Err(FaceFoldErr::GroupCap { cap: 4 })
        );
    }

    // -----------------------------------------------------------------------
    // [heap rung 3] byte lanes: varlena conjuncts + text group keys
    // -----------------------------------------------------------------------

    use crate::ir::{VarOp, VarPredTerm};

    /// Mixed word/byte test face: word columns as VecFace, plus text
    /// columns carrying Option<bytes> rows.
    #[derive(Clone)]
    struct MixUnit {
        words: Vec<(u32, Face, Vec<Option<i64>>)>,
        texts: Vec<(u32, Vec<Option<Vec<u8>>>)>,
    }

    #[derive(Clone)]
    struct MixFace {
        units: Vec<MixUnit>,
    }

    impl MixFace {
        fn face_of(&self, attno: u32) -> Face {
            let u = &self.units[0];
            if u.texts.iter().any(|c| c.0 == attno) {
                Face::Varlena
            } else {
                u.words.iter().find(|c| c.0 == attno).map(|c| c.1).expect("attno")
            }
        }
    }

    impl ScanFace for MixFace {
        fn n_units(&self) -> usize {
            self.units.len()
        }
        fn face(&self, attno: u32) -> Face {
            self.face_of(attno)
        }
        fn null_free(&self, _attno: u32) -> bool {
            false
        }
        fn rows_total(&self) -> Option<u64> {
            None
        }
        fn fill(
            &mut self,
            unit: usize,
            cols: &[u32],
            out: &mut FaceFill,
        ) -> Result<(), FaceError> {
            let u = &self.units[unit];
            let rows = u
                .words
                .first()
                .map(|c| c.2.len())
                .or_else(|| u.texts.first().map(|c| c.1.len()))
                .unwrap_or(0);
            for (ci, &attno) in cols.iter().enumerate() {
                if let Some(tc) = u.texts.iter().find(|c| c.0 == attno) {
                    for r in 0..rows {
                        out.push_bytes(ci, tc.1[r].as_deref());
                    }
                } else {
                    let col = u.words.iter().find(|c| c.0 == attno).expect("attno");
                    for r in 0..rows {
                        match col.2[r] {
                            Some(v) => out.push(ci, v as u64, false),
                            None => out.push(ci, 0, true),
                        }
                    }
                }
            }
            out.seal(rows as u32);
            Ok(())
        }
    }

    /// The same data as a pack source: positional deform through
    /// `lane_mut`/`bytes_lane_mut`, sel-aware — proving byte-lane late
    /// materialization + width identity against the serial drive.
    struct MixPacks {
        units: Vec<MixUnit>,
    }

    #[derive(Default)]
    struct MixPack {
        u: Option<MixUnit>,
    }

    struct MixDeformer;

    impl PackDeform<MixPack> for MixDeformer {
        fn rows(&self, pack: &MixPack) -> u32 {
            let u = pack.u.as_ref().expect("staged");
            u.words
                .first()
                .map(|c| c.2.len())
                .or_else(|| u.texts.first().map(|c| c.1.len()))
                .unwrap_or(0) as u32
        }
        fn deform(
            &mut self,
            pack: &MixPack,
            cols: &[(usize, u32)],
            sel: Option<&[u16]>,
            out: &mut FaceFill,
        ) -> Result<(), FaceError> {
            let u = pack.u.as_ref().expect("staged");
            let rows = self.rows(pack) as usize;
            for &(ci, attno) in cols {
                if let Some(tc) = u.texts.iter().find(|c| c.0 == attno) {
                    let write = |out: &mut FaceFill, r: usize| {
                        let (arena, spans, vwords, nulls) = out.bytes_lane_mut(ci);
                        match &tc.1[r] {
                            Some(b) => {
                                let off = arena.len() as u32;
                                arena.extend_from_slice(b);
                                spans[r] = (off, b.len() as u32);
                                vwords[r >> 6] |= 1u64 << (r & 63);
                            }
                            None => *nulls += 1,
                        }
                    };
                    match sel {
                        None => (0..rows).for_each(|r| write(out, r)),
                        Some(sel) => sel.iter().for_each(|&r| write(out, r as usize)),
                    }
                } else {
                    let col = u.words.iter().find(|c| c.0 == attno).expect("attno");
                    let write = |out: &mut FaceFill, r: usize| {
                        let (words, vwords, nulls) = out.lane_mut(ci);
                        match col.2[r] {
                            Some(v) => {
                                words[r] = v as u64;
                                vwords[r >> 6] |= 1u64 << (r & 63);
                            }
                            None => *nulls += 1,
                        }
                    };
                    match sel {
                        None => (0..rows).for_each(|r| write(out, r)),
                        Some(sel) => sel.iter().for_each(|&r| write(out, r as usize)),
                    }
                }
            }
            Ok(())
        }
    }

    impl PackSource for MixPacks {
        type Pack = MixPack;
        type Deformer = MixDeformer;
        fn n_units(&self) -> usize {
            self.units.len()
        }
        fn face(&self, attno: u32) -> Face {
            let u = &self.units[0];
            if u.texts.iter().any(|c| c.0 == attno) {
                Face::Varlena
            } else {
                u.words.iter().find(|c| c.0 == attno).map(|c| c.1).expect("attno")
            }
        }
        fn new_pack(&self) -> MixPack {
            MixPack::default()
        }
        fn deformer(&self) -> MixDeformer {
            MixDeformer
        }
        fn stage(&mut self, unit: usize, pack: &mut MixPack) -> Result<(), FaceError> {
            pack.u = Some(self.units[unit].clone());
            Ok(())
        }
    }

    fn mix_units(nunits: usize, rows: usize) -> Vec<MixUnit> {
        (0..nunits)
            .map(|u| {
                let f = |i: usize, m: i64, nz: usize| -> Option<i64> {
                    let x = (u * rows + i) as i64;
                    (x as usize % nz != 0).then_some(x % m)
                };
                let t = |i: usize| -> Option<Vec<u8>> {
                    let x = u * rows + i;
                    if x % 13 == 0 {
                        return None; // NULL text (3VL legs)
                    }
                    Some(match x % 5 {
                        0 => format!("http://google.com/x{}", x % 7).into_bytes(),
                        1 => format!("http://example.com/p{}", x % 11).into_bytes(),
                        2 => Vec::new(), // empty string (NeEmpty legs)
                        3 => "日本語テキスト".as_bytes().to_vec(),
                        _ => format!("s{}", x % 4).into_bytes(),
                    })
                };
                MixUnit {
                    words: vec![
                        (1, Face::SignedWord(4), (0..rows).map(|i| f(i, 997, 17)).collect()),
                        (2, Face::SignedWord(8), (0..rows).map(|i| f(i, 100_000, 7)).collect()),
                    ],
                    texts: vec![(3, (0..rows).map(t).collect())],
                }
            })
            .collect()
    }

    fn text_spec_corpus() -> Vec<FaceFoldSpec> {
        let vt = |op: VarOp, needle: &[u8]| {
            VarPredTerm::new(3, op, needle.to_vec(), TypMeta::TEXT_C)
        };
        vec![
            // LIKE '%google%' (Contains class) filter-agg.
            FaceFoldSpec {
                terms: vec![],
                var_terms: vec![vt(VarOp::Contains, b"google")],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
                group: None,
            },
            // NOT LIKE general pattern + int conjunct (3VL: NULL text fails).
            FaceFoldSpec {
                terms: vec![PredTerm::new(1, CmpOp::Ne, 5, 0, TypMeta::INT4)],
                var_terms: vec![vt(VarOp::NotLike, b"http://%.com/p_")],
                legs: vec![
                    leg(FoldOp::CountStar, None),
                    leg(FoldOp::Min, Some(2)),
                    leg(FoldOp::Max, Some(2)),
                ],
                group: None,
            },
            // General matcher over multibyte text (`_` char-grain).
            FaceFoldSpec {
                terms: vec![],
                var_terms: vec![vt(VarOp::Like, "日本語____".as_bytes())],
                legs: vec![leg(FoldOp::CountStar, None)],
                group: None,
            },
            // col <> '' (NeEmpty).
            FaceFoldSpec {
                terms: vec![],
                var_terms: vec![vt(VarOp::NeEmpty, b"")],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Avg, Some(2))],
                group: None,
            },
            // TEXT group key (byte-keyed hash arm) + LIKE filter.
            FaceFoldSpec {
                terms: vec![],
                var_terms: vec![vt(VarOp::Contains, b".com")],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
                group: Some(GroupSpec { col: 3, out: TypMeta::TEXT_C, witness_cap: 1 << 16, second: None }),
            },
            // TEXT group key, unfiltered (NULL key group + empty-string key).
            FaceFoldSpec {
                terms: vec![PredTerm::new(1, CmpOp::Between, -10, 800, TypMeta::INT4)],
                var_terms: vec![],
                legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Min, Some(2))],
                group: Some(GroupSpec { col: 3, out: TypMeta::TEXT_C, witness_cap: 1 << 16, second: None }),
            },
        ]
    }

    /// Byte lanes answer identically across the serial drive, the inline
    /// pack drive, and the pooled pack drive at several widths — LIKE/
    /// Contains/NeEmpty conjuncts (3VL) and byte-keyed text grouping.
    #[test]
    fn byte_lane_width_and_latemat_identity() {
        let units = mix_units(7, 300);
        let cfg = SqeConfig::heap_v1(1);
        for (si, spec) in text_spec_corpus().iter().enumerate() {
            let mut fill = FaceFill::new();
            let oracle = run_face_fold(
                &mut MixFace { units: units.clone() },
                spec,
                &cfg,
                &mut fill,
            )
            .unwrap();
            let inline = run_pack_fold(
                &mut MixPacks { units: units.clone() },
                spec,
                &cfg,
                None,
            )
            .unwrap();
            assert_answers_eq(&oracle, &inline, &format!("text spec {si} inline"));
            for width in [2usize, 3, 8] {
                let pool = Pool::new(width);
                let pooled = run_pack_fold(
                    &mut MixPacks { units: units.clone() },
                    spec,
                    &cfg,
                    Some((&pool, width)),
                )
                .unwrap();
                assert_answers_eq(&oracle, &pooled, &format!("text spec {si} width {width}"));
            }
        }
    }

    /// Hand-checked byte-keyed grouping: key-ascending byte order, the
    /// empty string sorts first, the NULL key group lands last, LIKE 3VL
    /// (NULL text fails the row).
    #[test]
    fn text_group_key_hand_checked() {
        let units = vec![MixUnit {
            words: vec![(2, Face::SignedWord(8), vec![Some(10), Some(20), None, Some(40), Some(50), Some(60)])],
            texts: vec![(
                3,
                vec![
                    Some(b"b".to_vec()),
                    Some(b"a".to_vec()),
                    Some(b"b".to_vec()),
                    None,
                    Some(Vec::new()),
                    None,
                ],
            )],
        }];
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
            group: Some(GroupSpec { col: 3, out: TypMeta::TEXT_C, witness_cap: 1 << 16, second: None }),
        };
        let mut fill = FaceFill::new();
        let a = run_face_fold(
            &mut MixFace { units: units.clone() },
            &spec,
            &SqeConfig::heap_v1(1),
            &mut fill,
        )
        .unwrap();
        assert_eq!(a.nrows, 4);
        // keys: "" < "a" < "b" < NULL(last)
        assert_eq!(a.cols[0].data.bytes_at(0), b"");
        assert_eq!(a.cols[0].data.bytes_at(1), b"a");
        assert_eq!(a.cols[0].data.bytes_at(2), b"b");
        assert!(a.cols[0].validity.is_valid(2));
        assert!(!a.cols[0].validity.is_valid(3), "NULL key group last");
        assert_eq!(a.cols[1].data, ColData::I64(vec![1, 1, 2, 2]));
        // sums: ""=50, "a"=20, "b"=10 (NULL input skipped), NULL=40+60.
        assert_eq!(a.cols[2].data, ColData::I128(vec![50, 20, 10, 100]));
        // The pack drive agrees.
        let p = run_pack_fold(&mut MixPacks { units }, &spec, &SqeConfig::heap_v1(1), None)
            .unwrap();
        assert_answers_eq(&a, &p, "text group hand-checked pack");
    }

    /// A text group key past the witness cap is the typed GroupCap error
    /// on every drive (never a truncated answer).
    #[test]
    fn text_group_cap_breach_is_hard_error() {
        let units = vec![MixUnit {
            words: vec![],
            texts: vec![(
                3,
                (0..64).map(|i| Some(format!("k{i}").into_bytes())).collect(),
            )],
        }];
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None)],
            group: Some(GroupSpec { col: 3, out: TypMeta::TEXT_C, witness_cap: 4, second: None }),
        };
        let mut fill = FaceFill::new();
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.spill = false;
        assert_eq!(
            run_face_fold(&mut MixFace { units: units.clone() }, &spec, &cfg, &mut fill),
            Err(FaceFoldErr::GroupCap { cap: 4 })
        );
        let pool = Pool::new(3);
        assert_eq!(
            run_pack_fold(&mut MixPacks { units }, &spec, &cfg, Some((&pool, 3))),
            Err(FaceFoldErr::GroupCap { cap: 4 })
        );
    }

    // -----------------------------------------------------------------------
    // [heap spill] the grouped hash arms' E18 write-through
    // -----------------------------------------------------------------------

    fn spill_cfg(budget: u64) -> SqeConfig {
        crate::spill::register_std_store();
        let mut cfg = SqeConfig::heap_v1(1);
        cfg.spill = true;
        cfg.grouped_budget_override = Some(budget);
        cfg
    }

    /// A forced-tiny budget drains every granule to sorted runs; the
    /// k-way merged answer is identical to the disarmed oracle on every
    /// drive and width (NULL keys and NULL fold inputs included).
    #[test]
    fn word_spill_identity_every_drive() {
        let mk = |u: i64| {
            vec![
                (
                    1,
                    Face::SignedWord(8),
                    (0..64i64).map(|i| (i % 7 != 0).then_some((i * 3 + u) % 96)).collect(),
                ),
                (
                    2,
                    Face::SignedWord(8),
                    (0..64i64).map(|i| (i % 5 != 0).then_some(i + u)).collect(),
                ),
            ]
        };
        let units: Vec<_> = (0..4).map(mk).collect();
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![
                leg(FoldOp::CountStar, None),
                leg(FoldOp::Sum, Some(2)),
                leg(FoldOp::Min, Some(2)),
                leg(FoldOp::Avg, Some(2)),
            ],
            group: Some(GroupSpec {
                col: 1,
                out: TypMeta::INT8,
                witness_cap: 1 << 16,
                second: None,
            }),
        };
        let mut oracle_cfg = SqeConfig::heap_v1(1);
        oracle_cfg.spill = false;
        let mut fill = FaceFill::new();
        let oracle =
            run_face_fold(&mut VecFace { units: units.clone() }, &spec, &oracle_cfg, &mut fill)
                .unwrap();
        let (f0, m0) = spill_counters();
        let cfg = spill_cfg(1);
        let mut fill2 = FaceFill::new();
        let spilled =
            run_face_fold(&mut VecFace { units: units.clone() }, &spec, &cfg, &mut fill2)
                .unwrap();
        assert_answers_eq(&oracle, &spilled, "word spill serial");
        let (f1, m1) = spill_counters();
        assert!(f1 > f0 && m1 > m0, "the run spilled and merged");
        let pool = Pool::new(3);
        let pooled =
            run_pack_fold(&mut VecPacks { units }, &spec, &cfg, Some((&pool, 3))).unwrap();
        assert_answers_eq(&oracle, &pooled, "word spill pooled");
    }

    /// Byte-keyed twin: text keys spill as self-delimiting records; the
    /// NULL key group stays resident and lands last, identically.
    #[test]
    fn text_spill_identity_every_drive() {
        let units = vec![MixUnit {
            words: vec![(2, Face::SignedWord(8), (0..64).map(Some).collect())],
            texts: vec![(
                3,
                (0..64)
                    .map(|i| (i % 9 != 0).then(|| format!("key-{:03}", i % 40).into_bytes()))
                    .collect(),
            )],
        }];
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None), leg(FoldOp::Sum, Some(2))],
            group: Some(GroupSpec {
                col: 3,
                out: TypMeta::TEXT_C,
                witness_cap: 1 << 16,
                second: None,
            }),
        };
        let mut oracle_cfg = SqeConfig::heap_v1(1);
        oracle_cfg.spill = false;
        let mut fill = FaceFill::new();
        let oracle = run_face_fold(
            &mut MixFace { units: units.clone() },
            &spec,
            &oracle_cfg,
            &mut fill,
        )
        .unwrap();
        let cfg = spill_cfg(1);
        let mut fill2 = FaceFill::new();
        let spilled = run_face_fold(
            &mut MixFace { units: units.clone() },
            &spec,
            &cfg,
            &mut fill2,
        )
        .unwrap();
        assert_answers_eq(&oracle, &spilled, "text spill serial");
        let pool = Pool::new(3);
        let pooled =
            run_pack_fold(&mut MixPacks { units }, &spec, &cfg, Some((&pool, 3))).unwrap();
        assert_answers_eq(&oracle, &pooled, "text spill pooled");
    }

    /// The finalize answer meter (E17): a tiny answer budget refuses
    /// typed through the RunRefusal unwind — never a truncated answer.
    #[test]
    fn answer_meter_refuses_typed_over_budget() {
        let units = vec![vec![(
            1,
            Face::SignedWord(8),
            (0..64i64).map(Some).collect::<Vec<_>>(),
        )]];
        let spec = FaceFoldSpec {
            terms: vec![],
            var_terms: vec![],
            legs: vec![leg(FoldOp::CountStar, None)],
            group: Some(GroupSpec {
                col: 1,
                out: TypMeta::INT8,
                witness_cap: 1 << 16,
                second: None,
            }),
        };
        let mut cfg = spill_cfg(1 << 20);
        cfg.answer_budget_override = Some(16);
        // unwind-ok: asserting the typed refusal transport
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut fill = FaceFill::new();
            let _ = run_face_fold(&mut VecFace { units }, &spec, &cfg, &mut fill);
        }));
        let e = r.expect_err("over-budget answer must refuse");
        let rr = e.downcast::<crate::refuse::RunRefusal>().expect("typed refusal payload");
        assert!(matches!(
            rr.0,
            crate::refuse::Refuse::GroupAnswerOverBudget { .. }
        ));
    }
}
