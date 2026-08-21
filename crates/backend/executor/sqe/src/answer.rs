//! The typed answer currency (port-study/abi-kill-list.md §5) — replaces
//! the PoC's `Vec<String>` stencil ABI. Column-major, typed, with a real
//! validity leg so NULL is representable (MIN/MAX over an empty domain is
//! `NULL`, never a fabricated 0 / i64::MAX — the oracle.rs:357 bug class,
//! fixed here and in the oracle together).
//!
//! Shape follows the lx4 `AggAnswer` vocabulary (lanev4
//! lx4_pipe/src/grouped.rs:1419-1431): Min/Max carry Option-ness via the
//! validity bitmap; Sum/Avg carry `{sum, count}` (`ColData::Ratio`) so
//! AVG-over-empty and exact-decimal rendering are decidable at answer
//! time, not fold time.
//!
//! Text leaves the engine ONLY through `render::to_lines` (the one render
//! seam, used by the rig for byte-identity) — at P2-1 that seam is swapped
//! for the typed DestReceiver emitter and this module is the currency it
//! receives.

use crate::typmeta::TypMeta;

/// Which variance-family finisher a Moments column elects (PG's
/// numeric_stddev_internal parameterization: variance × sample).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MomentKind {
    VarSamp,
    VarPop,
    StddevSamp,
    StddevPop,
}

impl MomentKind {
    /// (variance, sample) — the C numeric_stddev_internal flag pair.
    pub fn flags(self) -> (bool, bool) {
        match self {
            MomentKind::VarSamp => (true, true),
            MomentKind::VarPop => (true, false),
            MomentKind::StddevSamp => (false, true),
            MomentKind::StddevPop => (false, false),
        }
    }
    pub fn of_op(op: crate::ir::AggOp) -> Option<MomentKind> {
        use crate::ir::AggOp;
        match op {
            AggOp::VarSamp => Some(MomentKind::VarSamp),
            AggOp::VarPop => Some(MomentKind::VarPop),
            AggOp::StddevSamp => Some(MomentKind::StddevSamp),
            AggOp::StddevPop => Some(MomentKind::StddevPop),
            _ => None,
        }
    }
}

/// Column-major typed payload of one answer column.
#[derive(Clone, Debug, PartialEq)]
pub enum ColData {
    I64(Vec<i64>),
    F64(Vec<f64>),
    I128(Vec<i128>),
    /// Variance-family payloads: exact `{n, sum, sumsq}` triples plus the
    /// elected finisher. The finisher (PG's N·Σx² − (Σx)² closed form)
    /// runs at the render seam; n==0 rows (and n<=1 for the sample
    /// finishers) must be marked invalid by the producer — the NULL law.
    Moments { kind: MomentKind, trips: Vec<(i64, i128, i128)> },
    /// AVG payloads: exact `{sum, count}` pairs, rendered through PG's
    /// numeric avg finisher at the seam (`render::avg_numeric`). `exact`
    /// records the width-8 producer law; it no longer elects a render.
    /// count==0 rows must be marked invalid by the producer.
    Ratio { pairs: Vec<(i128, i64)>, exact: bool },
    /// Byte-string answers (text keys, MinBytes). Owned copies — the
    /// varlena arena/dict regions do not outlive the query, the answer
    /// does (risks.md §5).
    Bytes { arena: Vec<u8>, offs: Vec<u32> },
    /// [sortgrp v1] Nested (array) answers — the ONE two-level nested-data
    /// currency (sort-grouped-family.md §6, aligned with the P5-6 shred
    /// representation): row `i`'s elements are `elems` rows
    /// `offs[i]..offs[i+1]`. Two-level validity: the OUTER column's
    /// `Validity` marks NULL arrays; `elems.validity` marks NULL elements.
    /// Unsortable answer class (typed sort-key refusal at lowering).
    List { elems: Box<AnswerCol>, offs: Vec<u32> },
}

impl ColData {
    pub fn len(&self) -> usize {
        match self {
            ColData::I64(v) => v.len(),
            ColData::F64(v) => v.len(),
            ColData::I128(v) => v.len(),
            ColData::Moments { trips, .. } => trips.len(),
            ColData::Ratio { pairs, .. } => pairs.len(),
            ColData::Bytes { offs, .. } => offs.len().saturating_sub(1),
            ColData::List { offs, .. } => offs.len().saturating_sub(1),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Row `i` of a Bytes column.
    pub fn bytes_at(&self, i: usize) -> &[u8] {
        match self {
            ColData::Bytes { arena, offs } => {
                &arena[offs[i] as usize..offs[i + 1] as usize]
            }
            _ => panic!("bytes_at on non-Bytes column"),
        }
    }
}

/// Per-row validity of one answer column. `AllValid` is the fast path
/// (answers over NOT NULL data never allocate a mask).
#[derive(Clone, Debug, PartialEq)]
pub enum Validity {
    AllValid,
    /// false = SQL NULL for that row.
    Mask(Vec<bool>),
}

impl Validity {
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        match self {
            Validity::AllValid => true,
            Validity::Mask(m) => m[i],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnswerCol {
    pub ty: TypMeta,
    pub data: ColData,
    pub validity: Validity,
}

impl AnswerCol {
    pub fn i64s(ty: TypMeta, v: Vec<i64>) -> AnswerCol {
        AnswerCol { ty, data: ColData::I64(v), validity: Validity::AllValid }
    }
    /// Option-typed i64 column: None = NULL (the MIN/MAX-over-empty law).
    pub fn i64s_opt(ty: TypMeta, v: Vec<Option<i64>>) -> AnswerCol {
        let mask: Vec<bool> = v.iter().map(|x| x.is_some()).collect();
        let data = ColData::I64(v.into_iter().map(|x| x.unwrap_or(0)).collect());
        let validity = if mask.iter().all(|&b| b) {
            Validity::AllValid
        } else {
            Validity::Mask(mask)
        };
        AnswerCol { ty, data, validity }
    }
    pub fn f64s(ty: TypMeta, v: Vec<f64>) -> AnswerCol {
        AnswerCol { ty, data: ColData::F64(v), validity: Validity::AllValid }
    }
    pub fn i128s(ty: TypMeta, v: Vec<i128>) -> AnswerCol {
        AnswerCol { ty, data: ColData::I128(v), validity: Validity::AllValid }
    }
    /// Moments column; the NULL law rides the finisher: n==0 rows are
    /// NULL for every kind, n<=1 rows are NULL for the sample finishers
    /// (PG numeric_stddev_internal's early-outs, mirrored as validity).
    pub fn moments(ty: TypMeta, kind: MomentKind, trips: Vec<(i64, i128, i128)>) -> AnswerCol {
        let (_, sample) = kind.flags();
        let mask: Vec<bool> =
            trips.iter().map(|&(n, ..)| if sample { n > 1 } else { n > 0 }).collect();
        let validity = if mask.iter().all(|&b| b) {
            Validity::AllValid
        } else {
            Validity::Mask(mask)
        };
        AnswerCol { ty, data: ColData::Moments { kind, trips }, validity }
    }
    /// Ratio column; count==0 rows become NULL (AVG over empty = NULL).
    pub fn ratios(ty: TypMeta, pairs: Vec<(i128, i64)>, exact: bool) -> AnswerCol {
        let mask: Vec<bool> = pairs.iter().map(|&(_, c)| c > 0).collect();
        let validity = if mask.iter().all(|&b| b) {
            Validity::AllValid
        } else {
            Validity::Mask(mask)
        };
        AnswerCol { ty, data: ColData::Ratio { pairs, exact }, validity }
    }
}

/// The typed answer of one plan execution. Columns are answer-ordered
/// (projection order); rows are final row order (ordering/limit applied
/// by the producing stencil — the one ordering law).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AnswerSet {
    pub cols: Vec<AnswerCol>,
    pub nrows: usize,
    /// Rig-visible answer trailer (the hot-shape `-- N rows` row-count
    /// witness the answers of record carry). Rendered as a final line by
    /// `render::to_lines`; the P2-1 DestReceiver ignores it. Never answer
    /// data.
    pub note: Option<String>,
    /// Rig-visible answer HEADER (the hot-shape `-- predicate matches: N`
    /// witness) — rendered before the rows. Same contract as `note`.
    pub head_note: Option<String>,
}

impl AnswerSet {
    pub fn from_cols(cols: Vec<AnswerCol>) -> AnswerSet {
        let nrows = cols.first().map(|c| c.data.len()).unwrap_or(0);
        for c in &cols {
            debug_assert_eq!(c.data.len(), nrows, "ragged AnswerSet");
        }
        AnswerSet { cols, nrows, note: None, head_note: None }
    }
    /// Zero-row answer with a declared column shape.
    pub fn empty(tys: Vec<TypMeta>) -> AnswerSet {
        AnswerSet {
            cols: tys
                .into_iter()
                .map(|ty| AnswerCol { ty, data: ColData::I64(Vec::new()), validity: Validity::AllValid })
                .collect(),
            nrows: 0,
            note: None,
            head_note: None,
        }
    }
}

/// Compare two answer rows under a pushed-down key spec: NULLs place
/// absolutely (`nulls_first`), values by the column's total order (Bytes
/// = memcmp — the C-collation law; unsortable classes are refused at
/// lowering, loud here).
/// Exact rational comparison of `an/ad` vs `bn/bd` (denominators > 0) by
/// Euclidean descent — the AVG answer is an exact {sum, count} pair and a
/// pushed bound over it must select exactly.
pub fn cmp_ratio(an: i128, ad: i128, bn: i128, bd: i128) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    debug_assert!(ad > 0 && bd > 0);
    let (mut an, mut ad, mut bn, mut bd) = (an, ad, bn, bd);
    loop {
        let (qa, ra) = (an.div_euclid(ad), an.rem_euclid(ad));
        let (qb, rb) = (bn.div_euclid(bd), bn.rem_euclid(bd));
        match qa.cmp(&qb) {
            Ordering::Equal => {}
            o => return o,
        }
        match (ra == 0, rb == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => (an, ad, bn, bd) = (bd, rb, ad, ra),
        }
    }
}

fn span_at(cols: &[AnswerCol], hi: u32, lo: u32, i: usize) -> Option<i128> {
    let (h, l) = (&cols[hi as usize], &cols[lo as usize]);
    if !h.validity.is_valid(i) || !l.validity.is_valid(i) {
        return None;
    }
    match (&h.data, &l.data) {
        (ColData::I64(x), ColData::I64(y)) => Some(x[i] as i128 - y[i] as i128),
        _ => unreachable!("span key over non-word answer columns (bug)"),
    }
}

pub fn cmp_rows(cols: &[AnswerCol], keys: &[crate::ir::TopKKey], a: usize, b: usize) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for k in keys {
        if let Some(lo) = k.lo {
            let ord = match (span_at(cols, k.col, lo, a), span_at(cols, k.col, lo, b)) {
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
            continue;
        }
        let col = &cols[k.col as usize];
        // A Ratio cell with count 0 renders NULL (empty-fold AVG): it
        // orders through the null lane like the server emitter's sort.
        let null_at = |i: usize| {
            !col.validity.is_valid(i)
                || matches!(&col.data, ColData::Ratio { pairs, .. } if pairs[i].1 == 0)
        };
        let (va, vb) = (!null_at(a), !null_at(b));
        let ord = match (va, vb) {
            (false, false) => Ordering::Equal,
            (false, true) => {
                if k.nulls_first { Ordering::Less } else { Ordering::Greater }
            }
            (true, false) => {
                if k.nulls_first { Ordering::Greater } else { Ordering::Less }
            }
            (true, true) => {
                let o = match &col.data {
                    ColData::I64(v) => v[a].cmp(&v[b]),
                    ColData::I128(v) => v[a].cmp(&v[b]),
                    ColData::Bytes { .. } if k.trim => crate::ir::rtrim_blanks(
                        col.data.bytes_at(a),
                    )
                    .cmp(crate::ir::rtrim_blanks(col.data.bytes_at(b))),
                    ColData::Bytes { .. } => col.data.bytes_at(a).cmp(col.data.bytes_at(b)),
                    ColData::Ratio { pairs, .. } => {
                        let ((an, ad), (bn, bd)) = (pairs[a], pairs[b]);
                        cmp_ratio(an, ad as i128, bn, bd as i128)
                    }
                    ColData::F64(_) | ColData::Moments { .. } | ColData::List { .. } => {
                        unreachable!("unsortable answer class under a pushed bound (bug)")
                    }
                };
                if k.desc { o.reverse() } else { o }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Bound an answer to its pushed-down top-`n` groups: an O(G) selection
/// (never a full sort) keeps exactly the spec's top rows; an empty spec
/// keeps the first `n` in the producer's merge order. Row order within
/// the kept set is NOT asserted — the server emitter owns final order.
pub fn apply_topk(a: &mut AnswerSet, t: &crate::ir::TopK) {
    if a.nrows <= t.n {
        return;
    }
    let mut idx: Vec<u32> = (0..a.nrows as u32).collect();
    if !t.keys.is_empty() && t.n > 0 {
        idx.select_nth_unstable_by(t.n - 1, |&x, &y| {
            cmp_rows(&a.cols, &t.keys, x as usize, y as usize)
        });
    }
    idx.truncate(t.n);
    for col in a.cols.iter_mut() {
        col.data = match &col.data {
            ColData::I64(v) => ColData::I64(idx.iter().map(|&i| v[i as usize]).collect()),
            ColData::F64(v) => ColData::F64(idx.iter().map(|&i| v[i as usize]).collect()),
            ColData::I128(v) => ColData::I128(idx.iter().map(|&i| v[i as usize]).collect()),
            ColData::Ratio { pairs, exact } => ColData::Ratio {
                pairs: idx.iter().map(|&i| pairs[i as usize]).collect(),
                exact: *exact,
            },
            ColData::Moments { kind, trips } => ColData::Moments {
                kind: *kind,
                trips: idx.iter().map(|&i| trips[i as usize]).collect(),
            },
            ColData::Bytes { .. } => {
                let mut b = BytesBuild::new();
                for &i in &idx {
                    b.push(col.data.bytes_at(i as usize));
                }
                ColData::Bytes { arena: b.arena, offs: b.offs }
            }
            ColData::List { elems, offs } => {
                let mut eidx: Vec<u32> = Vec::new();
                let mut noffs: Vec<u32> = vec![0];
                for &i in &idx {
                    eidx.extend(offs[i as usize]..offs[i as usize + 1]);
                    noffs.push(eidx.len() as u32);
                }
                ColData::List { elems: Box::new(gather_col(elems, &eidx)), offs: noffs }
            }
        };
        col.validity = match &col.validity {
            Validity::AllValid => Validity::AllValid,
            Validity::Mask(m) => Validity::Mask(idx.iter().map(|&i| m[i as usize]).collect()),
        };
    }
    a.nrows = idx.len();
}

/// [sqe-tpch-mech] Aggregate-result-as-set materialization: the valid
/// (non-NULL) i64 word keys of answer column 0 — for a grouped fused-
/// HAVING answer, the surviving group-key set — SORTED for binary-search
/// membership (the `joins::InSetFilter` consumption hook's currency).
pub fn survivor_keyset(a: &AnswerSet) -> Vec<i64> {
    let col = &a.cols[0];
    let ColData::I64(v) = &col.data else {
        panic!("survivor_keyset: key column is not a word lane")
    };
    let mut out: Vec<i64> =
        (0..a.nrows).filter(|&i| col.validity.is_valid(i)).map(|i| v[i]).collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// [sortgrp v1] Gather rows of a SCALAR answer column by index (the List
/// permutation helper — v1 elements are scalar classes; nested Lists have
/// no producer).
pub fn gather_col(col: &AnswerCol, idx: &[u32]) -> AnswerCol {
    let data = match &col.data {
        ColData::I64(v) => ColData::I64(idx.iter().map(|&i| v[i as usize]).collect()),
        ColData::F64(v) => ColData::F64(idx.iter().map(|&i| v[i as usize]).collect()),
        ColData::I128(v) => ColData::I128(idx.iter().map(|&i| v[i as usize]).collect()),
        ColData::Bytes { .. } => {
            let mut b = BytesBuild::new();
            for &i in idx {
                b.push(col.data.bytes_at(i as usize));
            }
            ColData::Bytes { arena: b.arena, offs: b.offs }
        }
        other => panic!("gather_col: non-scalar element class {other:?} (bug)"),
    };
    let validity = match &col.validity {
        Validity::AllValid => Validity::AllValid,
        Validity::Mask(m) => Validity::Mask(idx.iter().map(|&i| m[i as usize]).collect()),
    };
    AnswerCol { ty: col.ty, data, validity }
}

/// Row-append builder for stencils that fill several columns in step.
pub struct BytesBuild {
    pub arena: Vec<u8>,
    pub offs: Vec<u32>,
    /// Row ordinals pushed as SQL NULL (empty = the AllValid fast path).
    nulls: Vec<u32>,
}

impl BytesBuild {
    pub fn new() -> BytesBuild {
        BytesBuild { arena: Vec::new(), offs: vec![0], nulls: Vec::new() }
    }
    #[inline]
    pub fn push(&mut self, b: &[u8]) {
        self.arena.extend_from_slice(b);
        self.offs.push(self.arena.len() as u32);
    }
    /// Append one SQL-NULL row (an empty image under the validity mask).
    #[inline]
    pub fn push_null(&mut self) {
        self.nulls.push(self.len() as u32);
        self.offs.push(self.arena.len() as u32);
    }
    #[inline]
    pub fn push_opt(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => self.push(b),
            None => self.push_null(),
        }
    }
    pub fn finish(self, ty: TypMeta) -> AnswerCol {
        let validity = if self.nulls.is_empty() {
            Validity::AllValid
        } else {
            let mut m = vec![true; self.offs.len() - 1];
            for &i in &self.nulls {
                m[i as usize] = false;
            }
            Validity::Mask(m)
        };
        AnswerCol { ty, data: ColData::Bytes { arena: self.arena, offs: self.offs }, validity }
    }
    pub fn len(&self) -> usize {
        self.offs.len() - 1
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for BytesBuild {
    fn default() -> Self {
        Self::new()
    }
}
