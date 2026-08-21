//! sqe physical IR — the interface contract, conformed to PLANNER-SPEC
//! and ENGINE-PLAN, with the P1-1 typed-currency reshape
//! (port-study/currency-insertion.md):
//!
//!   - predicate leaves carry STRUCTURED typed fingerprints
//!     (`Fingerprint`: attno + type oid + collation oid + op tag +
//!     canonical post-parse constant) — the condition-cache key, shared
//!     across queries exactly when attno+type+collation+op+const all
//!     match (the collation-illegal-sharing fix). The PoC's formatted
//!     string keys are dead; a Display form survives for EXPLAIN/census.
//!   - the goal/request vector (`Goal`) carries every context a subplan's
//!     cost depends on — never the cost side;
//!   - aggregate specs carry input/output `TypMeta` — `MinDate/MaxDate/
//!     AvgExact` died into `Min/Max/Avg` + type identity (render law
//!     lives in the ONE render seam, not in op variants).
//!
//! CONTRACT (conform exactly — family lanes integrate mechanically):
//!   PlanNode { family, q, cols, pred, agg, params }
//!   stencil entry: `pub fn run_<family>(ctx, node) -> AnswerSet`
//!   elections are FUNCTIONS over bank stats — never constants.

use crate::typmeta::{oids, TypMeta};

/// Stencil family menu (ENGINE-PLAN §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Family {
    MetadataAnswer,
    FusedFilterAgg,
    DenseDomainGroup,
    TwoLevelCodeAgg,
    HashPlaneOwnedGroup,
    DistinctPipeline,
    VerdictBitmapContains,
    ZoneOrderWalk,
    FrameWalkCodeGroup,
    WindowReplay,
    SurvivorGather,
    DerivedKeyFold,
    /// Row-returning bare scans: filter -> survivor gather -> (bounded
    /// top-k | witnessed full) emission of projected columns.
    ScanServe,
    /// [sortgrp v1] The tier-2 general-aggregates family (holistic /
    /// order-sensitive: string_agg/array_agg with ORDER BY, the
    /// WITHIN GROUP ordered-set aggs). Pass A: scan/filter/scatter by
    /// hash(group key) into single-owner partitions; pass B: per-partition
    /// total sort (group key, statement sort spec, ingest ordinal) +
    /// run-boundary emission (docs/design/sqe/sort-grouped-family.md).
    SortGrouped,
    /// [winserve v1] SQL window functions (`OVER (...)`) over a served
    /// scan child, default-frame semantics ONLY. Pass A: scan/filter/
    /// scatter by hash(PARTITION BY key) into single-owner partitions
    /// (the SortGrouped pass-A stencil); pass B: per-partition total
    /// sort (partition key, window ORDER BY keys, ingest ordinal) + a
    /// peer-group run walk emitting ONE answer row per input row
    /// (nodeWindowAgg.c ground truth: rank family windowfuncs.c:49-213;
    /// default RANGE frame peer-inclusion nodeWindowAgg.c:1441-1476).
    WindowServe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CmpOp {
    Eq,
    Ne,
    /// lo <= v <= hi (inclusive both ends).
    Between,
    /// v ∈ {lo, hi} (the two-member IN list — hot-shape shape; longer lists
    /// lower to a disjunction when one appears).
    In2,
}

/// Fingerprint op tags: int comparison ops + varlena ops, one namespace
/// so a conjunction's identity is a plain sorted set of leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpTag {
    Eq,
    Ne,
    Range,
    In2,
    Contains,
    NotContains,
    NeEmpty,
    /// [R6d] general LIKE pattern (backslash escape convention).
    Like,
    NotLike,
    /// [type-vocab] byte-order comparison against a constant image
    /// (memcmp law: name fixed images; C-collated text payloads).
    BytesEq,
    BytesNe,
    BytesLt,
    BytesLe,
    BytesGt,
    BytesGe,
    /// [bpchar-order] trimmed-basis comparison (bcTruelen trim, then
    /// the memcmp law) — a distinct verdict family from the padded
    /// byte-identity tags above.
    BytesTrimEq,
    BytesTrimNe,
    BytesTrimLt,
    BytesTrimLe,
    BytesTrimGt,
    BytesTrimGe,
    /// [sqe-bpchar] byte-equality IN list (needle = the length-prefixed
    /// image concatenation — see `VarOp::InBytes`).
    BytesIn,
    /// [tpch-expr] byte-prefix IN list.
    BytesInPrefix,
    /// [colcmp] column-vs-column word compare; canon = (other attno, op).
    ColCmp,
}

/// The canonical constant: the TYPED, post-parse datum image (e.g. the
/// pg_date i32 for a date literal, never its source text) — removes the
/// text/number canonical-form fragility between RON-authored and
/// SQL-lowered plans.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CanonConst {
    /// Eq/Ne carry (v, 0); Range carries (lo, hi); In2 carries (a, b).
    Ints(i64, i64),
    /// Varlena needle bytes (empty for NeEmpty).
    Bytes(Vec<u8>),
}

/// One predicate leaf's identity (PLANNER-SPEC §3.2, typed form). This is
/// the condition-cache / verdict-plane key currency: two predicates share
/// a cached verdict plane iff their Fingerprints are equal — which now
/// REQUIRES equal type resolution and equal collation (the PoC's string
/// keys would replay byte-identical needles across collations).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fingerprint {
    pub attno: u32,
    pub ty_oid: u32,
    pub collation_oid: u32,
    pub op: OpTag,
    pub canon: CanonConst,
}

impl std::fmt::Display for Fingerprint {
    /// Human form for EXPLAIN/census: the PoC string shape plus the type
    /// and collation identity. NOT a key — identity is the struct.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.op {
            OpTag::Eq => "eq",
            OpTag::Ne => "ne",
            OpTag::Range => "range",
            OpTag::In2 => "in",
            OpTag::Contains => "contains",
            OpTag::NotContains => "not_contains",
            OpTag::NeEmpty => "ne:empty",
            OpTag::Like => "like",
            OpTag::NotLike => "not_like",
            OpTag::BytesEq => "beq",
            OpTag::BytesNe => "bne",
            OpTag::BytesLt => "blt",
            OpTag::BytesLe => "ble",
            OpTag::BytesGt => "bgt",
            OpTag::BytesGe => "bge",
            OpTag::BytesTrimEq => "bteq",
            OpTag::BytesTrimNe => "btne",
            OpTag::BytesTrimLt => "btlt",
            OpTag::BytesTrimLe => "btle",
            OpTag::BytesTrimGt => "btgt",
            OpTag::BytesTrimGe => "btge",
            OpTag::BytesIn => "bin",
            OpTag::BytesInPrefix => "bipfx",
            OpTag::ColCmp => "colcmp",
        };
        write!(f, "c{}:{op}:", self.attno)?;
        match &self.canon {
            CanonConst::Ints(a, b) => match self.op {
                OpTag::Eq | OpTag::Ne => write!(f, "{a}")?,
                OpTag::Range => write!(f, "{a}:{b}")?,
                _ => write!(f, "{a},{b}")?,
            },
            CanonConst::Bytes(b) => write!(f, "{}", String::from_utf8_lossy(b))?,
        }
        write!(f, ":oid={}:coll={}", self.ty_oid, self.collation_oid)
    }
}

/// A conjunction's identity: the SORTED set of leaf fingerprints
/// (conjunction order does not change identity). The condition-cache map
/// key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ConjFp(pub Vec<Fingerprint>);

impl ConjFp {
    pub fn new(mut leaves: Vec<Fingerprint>) -> ConjFp {
        leaves.sort_unstable();
        ConjFp(leaves)
    }
}

impl std::fmt::Display for ConjFp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, fp) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "&")?;
            }
            write!(f, "{fp}")?;
        }
        Ok(())
    }
}

/// One integer-column comparison term (byval columns). `fp` is the typed
/// canonical fingerprint of THIS conjunct.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PredTerm {
    pub col: u32,
    pub op: CmpOp,
    /// Eq/Ne use `lo`; Between uses [lo, hi].
    pub lo: i64,
    pub hi: i64,
    pub fp: Fingerprint,
}

impl PredTerm {
    /// `ty` = the column's TypMeta (the operator's type resolution rides
    /// the fingerprint; byval int ops carry collation 0).
    pub fn new(col: u32, op: CmpOp, lo: i64, hi: i64, ty: TypMeta) -> PredTerm {
        let (tag, canon) = match op {
            CmpOp::Eq => (OpTag::Eq, CanonConst::Ints(lo, 0)),
            CmpOp::Ne => (OpTag::Ne, CanonConst::Ints(lo, 0)),
            CmpOp::Between => (OpTag::Range, CanonConst::Ints(lo, hi)),
            CmpOp::In2 => (OpTag::In2, CanonConst::Ints(lo, hi)),
        };
        let fp = Fingerprint {
            attno: col,
            ty_oid: ty.oid,
            collation_oid: ty.collation,
            op: tag,
            canon,
        };
        PredTerm { col, op, lo, hi, fp }
    }
    #[inline(always)]
    pub fn eval(&self, v: i64) -> bool {
        match self.op {
            CmpOp::Eq => v == self.lo,
            CmpOp::Ne => v != self.lo,
            CmpOp::Between => v >= self.lo && v <= self.hi,
            CmpOp::In2 => v == self.lo || v == self.hi,
        }
    }
    /// 3VL WHERE-clause evaluation: a NULL operand makes EVERY comparison
    /// UNKNOWN, and UNKNOWN does not pass — including the negative ops
    /// (`Ne` of a NULL row is NOT true; `!eval(garbage)` on a null slot
    /// is the wrong-answer machine this replaces). Null-free lanes pass a
    /// hoisted `valid = true` (constant-folds to `eval` — law 11).
    #[inline(always)]
    pub fn eval_v(&self, v: i64, valid: bool) -> bool {
        valid && self.eval(v)
    }
    /// Term-major survivor filter (R2: no op interpretation at row
    /// grain): retains the rows of `sel` this conjunct passes, matching
    /// the CmpOp ONCE per call and running a monomorphic compaction loop
    /// over the selection (the fold_lane idiom). `valid`/`word` receive
    /// the row index stored in `sel`; callers hoist face/width/validity
    /// resolution into them. `word` is only consulted on valid rows
    /// (3VL: a NULL operand never passes — eval_v semantics), and the
    /// compaction is stable, so filtering the terms of a conjunction in
    /// order is bit-identical to the row-major short-circuit walk.
    #[inline]
    pub fn filter_sel(
        &self,
        sel: &mut Vec<u16>,
        valid: impl Fn(usize) -> bool,
        word: impl Fn(usize) -> i64,
    ) {
        #[inline(always)]
        fn run(
            sel: &mut Vec<u16>,
            valid: impl Fn(usize) -> bool,
            word: impl Fn(usize) -> i64,
            cmp: impl Fn(i64) -> bool,
        ) {
            let mut w = 0usize;
            for i in 0..sel.len() {
                let r = sel[i] as usize;
                let keep = valid(r) && cmp(word(r));
                sel[w] = sel[i];
                w += keep as usize;
            }
            sel.truncate(w);
        }
        let (lo, hi) = (self.lo, self.hi);
        match self.op {
            CmpOp::Eq => run(sel, valid, word, |v| v == lo),
            CmpOp::Ne => run(sel, valid, word, |v| v != lo),
            CmpOp::Between => run(sel, valid, word, |v| v >= lo && v <= hi),
            CmpOp::In2 => run(sel, valid, word, |v| v == lo || v == hi),
        }
    }
    /// Can a granule with exact zone [zlo, zhi] contain a passing row?
    #[inline(always)]
    pub fn zone_may_pass(&self, zlo: i64, zhi: i64) -> bool {
        match self.op {
            CmpOp::Eq => self.lo >= zlo && self.lo <= zhi,
            CmpOp::Ne => !(zlo == self.lo && zhi == self.lo),
            CmpOp::Between => self.hi >= zlo && self.lo <= zhi,
            CmpOp::In2 => {
                (self.lo >= zlo && self.lo <= zhi) || (self.hi >= zlo && self.hi <= zhi)
            }
        }
    }
    /// Does the zone PROVE every row passes? SOUND ONLY over granules
    /// proven all-nonnull: zone min/max are facts about the NON-NULL
    /// values, and a NULL row never passes — callers either hold the
    /// column's null-freedom proof (the lowering admission for the
    /// zone-consuming families) or gate with `zone_all_pass_v`.
    #[inline(always)]
    pub fn zone_all_pass(&self, zlo: i64, zhi: i64) -> bool {
        match self.op {
            CmpOp::Eq => zlo == self.lo && zhi == self.lo,
            CmpOp::Ne => self.lo < zlo || self.lo > zhi,
            CmpOp::Between => zlo >= self.lo && zhi <= self.hi,
            CmpOp::In2 => {
                (zlo == self.lo && zhi == self.lo) || (zlo == self.hi && zhi == self.hi)
            }
        }
    }
    /// Null-aware all-pass: a granule with ANY null can never all-pass
    /// (currency-insertion.md §1.2). `all_nonnull` comes from the stats
    /// face's nonnull count or the column's null-freedom proof.
    #[inline(always)]
    pub fn zone_all_pass_v(&self, zlo: i64, zhi: i64, all_nonnull: bool) -> bool {
        all_nonnull && self.zone_all_pass(zlo, zhi)
    }
}

/// [famB M1] Varlena predicate op (dict-entry verdict lowering). Never
/// zone-prunable; always part of the FRAME identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VarOp {
    /// LIKE '%needle%'.
    Contains,
    /// NOT LIKE '%needle%'.
    NotContains,
    /// col <> '' (needle empty).
    NeEmpty,
    /// [R6d] col LIKE <general pattern> (`needle` = the full pattern
    /// bytes, backslash escape convention, C collation — like.rs). The
    /// `%x%` class never reaches this op: lowering normalizes it to
    /// Contains (fingerprint stability, §3.2).
    Like,
    /// [R6d] col NOT LIKE <general pattern> — the verdict complement
    /// under the null-free witness (NullableDict refusal upstream).
    NotLike,
    /// [type-vocab] byte-order comparison against a constant image:
    /// `memcmp(col_bytes, needle)` decides. SOUND only where memcmp IS
    /// the type's comparison law — name fixed images (NUL-padded 64-byte
    /// buffers: memcmp == strncmp == PG's namecmp under C collation) and
    /// C-collated text payloads. Lowering gates the face + collation.
    CmpBytes(BytesCmp),
    /// [bpchar-order] bpchar comparison against a pre-trimmed constant
    /// image: `memcmp(rtrim_blanks(col_bytes), needle)` decides —
    /// bpcharlt/le/ge/gt ARE varstr_cmp over the bcTruelen-trimmed
    /// sides, which under the byte-order collation class is exactly
    /// this compare, for padded and bare stored images alike (every
    /// stored image trims to the value bpcharcmp compares). Lowering
    /// gates the varlena face + collation and the trimmed needle.
    CmpBytesTrim(BytesCmp),
    /// [sqe-bpchar] `col IN (const, ...)` under the byte-equality law:
    /// TRUE iff the column bytes equal ANY listed image. The needle is
    /// the CANONICAL encoding `encode_in_needles` produces (sorted,
    /// deduped, u32-LE length-prefixed concatenation) — fingerprint
    /// identity is list-order independent by construction. Same
    /// soundness gate as CmpBytes(Eq): byte equality must BE the type's
    /// equality (C-collated text; the seam's pad-aware bpchar law).
    InBytes,
    /// [tpch-expr] byte-prefix membership (the substring char-prefix
    /// law, seam-proven); InBytes' canonical needle encoding.
    InPrefix,
}

/// [sqe-bpchar] Canonical multi-needle image for `VarOp::InBytes`:
/// sort + dedup the images, then concatenate `[u32-LE len][bytes]`.
/// Sorting makes the fingerprint list-order independent; dedup keeps
/// the eval walk minimal. Decode with [`decode_in_needles`].
pub fn encode_in_needles(mut needles: Vec<Vec<u8>>) -> Vec<u8> {
    needles.sort_unstable();
    needles.dedup();
    let mut out = Vec::with_capacity(needles.iter().map(|n| n.len() + 4).sum());
    for n in &needles {
        out.extend_from_slice(&(n.len() as u32).to_le_bytes());
        out.extend_from_slice(n);
    }
    out
}

/// Iterate the images of an `encode_in_needles` needle. The encoding is
/// produced only by `encode_in_needles`; a malformed tail yields no
/// further images (never panics on hostile bytes).
pub fn decode_in_needles(needle: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = needle;
    std::iter::from_fn(move || {
        if rest.len() < 4 {
            return None;
        }
        let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        if rest.len() < 4 + len {
            return None;
        }
        let (img, tail) = rest[4..].split_at(len);
        rest = tail;
        Some(img)
    })
}

/// bcTruelen's exact trim: trailing 0x20 bytes ONLY (never other
/// whitespace, never interior bytes; UTF-8 continuation bytes are
/// >= 0x80, so the byte trim is char-safe).
pub fn rtrim_blanks(b: &[u8]) -> &[u8] {
    let mut n = b.len();
    while n > 0 && b[n - 1] == b' ' {
        n -= 1;
    }
    &b[..n]
}

/// The six comparison verdicts of the CmpBytes law.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BytesCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl BytesCmp {
    #[inline]
    pub fn verdict(self, o: std::cmp::Ordering) -> bool {
        match self {
            BytesCmp::Eq => o == std::cmp::Ordering::Equal,
            BytesCmp::Ne => o != std::cmp::Ordering::Equal,
            BytesCmp::Lt => o == std::cmp::Ordering::Less,
            BytesCmp::Le => o != std::cmp::Ordering::Greater,
            BytesCmp::Gt => o == std::cmp::Ordering::Greater,
            BytesCmp::Ge => o != std::cmp::Ordering::Less,
        }
    }
}

/// One varlena-column predicate term. The fingerprint carries the
/// column's collation: byte-contains verdicts computed under C collation
/// never serve another collation's predicate.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VarPredTerm {
    pub col: u32,
    pub op: VarOp,
    pub needle: Vec<u8>,
    pub fp: Fingerprint,
}

impl VarPredTerm {
    pub fn new(col: u32, op: VarOp, needle: Vec<u8>, ty: TypMeta) -> VarPredTerm {
        let tag = match op {
            VarOp::Contains => OpTag::Contains,
            VarOp::NotContains => OpTag::NotContains,
            VarOp::NeEmpty => OpTag::NeEmpty,
            VarOp::Like => OpTag::Like,
            VarOp::NotLike => OpTag::NotLike,
            VarOp::CmpBytes(BytesCmp::Eq) => OpTag::BytesEq,
            VarOp::CmpBytes(BytesCmp::Ne) => OpTag::BytesNe,
            VarOp::CmpBytes(BytesCmp::Lt) => OpTag::BytesLt,
            VarOp::CmpBytes(BytesCmp::Le) => OpTag::BytesLe,
            VarOp::CmpBytes(BytesCmp::Gt) => OpTag::BytesGt,
            VarOp::CmpBytes(BytesCmp::Ge) => OpTag::BytesGe,
            VarOp::CmpBytesTrim(BytesCmp::Eq) => OpTag::BytesTrimEq,
            VarOp::CmpBytesTrim(BytesCmp::Ne) => OpTag::BytesTrimNe,
            VarOp::CmpBytesTrim(BytesCmp::Lt) => OpTag::BytesTrimLt,
            VarOp::CmpBytesTrim(BytesCmp::Le) => OpTag::BytesTrimLe,
            VarOp::CmpBytesTrim(BytesCmp::Gt) => OpTag::BytesTrimGt,
            VarOp::CmpBytesTrim(BytesCmp::Ge) => OpTag::BytesTrimGe,
            VarOp::InBytes => OpTag::BytesIn,
            VarOp::InPrefix => OpTag::BytesInPrefix,
        };
        let fp = Fingerprint {
            attno: col,
            ty_oid: ty.oid,
            collation_oid: ty.collation,
            op: tag,
            canon: CanonConst::Bytes(if tag == OpTag::NeEmpty { Vec::new() } else { needle.clone() }),
        };
        VarPredTerm { col, op, needle, fp }
    }
    #[inline]
    pub fn eval(&self, bytes: &[u8]) -> bool {
        match self.op {
            VarOp::Contains => crate::simd::contains_scalar(bytes, &self.needle),
            VarOp::NotContains => !crate::simd::contains_scalar(bytes, &self.needle),
            VarOp::NeEmpty => !bytes.is_empty(),
            VarOp::Like => crate::like::like_match(bytes, &self.needle),
            VarOp::NotLike => !crate::like::like_match(bytes, &self.needle),
            VarOp::CmpBytes(c) => c.verdict(bytes.cmp(&self.needle[..])),
            VarOp::CmpBytesTrim(c) => c.verdict(rtrim_blanks(bytes).cmp(&self.needle[..])),
            VarOp::InBytes => decode_in_needles(&self.needle).any(|img| img == bytes),
            VarOp::InPrefix => decode_in_needles(&self.needle)
                .any(|img| bytes.len() >= img.len() && &bytes[..img.len()] == img),
        }
    }
    /// 3VL form: NULL never passes — critically for `NotContains` and
    /// `NeEmpty`, whose 2VL negations would pass a NULL row's slot bytes.
    #[inline]
    pub fn eval_v(&self, bytes: &[u8], valid: bool) -> bool {
        valid && self.eval(bytes)
    }
}

/// [colcmp] The six verdicts of a word column-vs-column compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColCmpOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl ColCmpOp {
    #[inline(always)]
    pub fn eval(self, a: i64, b: i64) -> bool {
        match self {
            ColCmpOp::Lt => a < b,
            ColCmpOp::Le => a <= b,
            ColCmpOp::Gt => a > b,
            ColCmpOp::Ge => a >= b,
            ColCmpOp::Eq => a == b,
            ColCmpOp::Ne => a != b,
        }
    }
    pub fn code(self) -> i64 {
        match self {
            ColCmpOp::Lt => 0,
            ColCmpOp::Le => 1,
            ColCmpOp::Gt => 2,
            ColCmpOp::Ge => 3,
            ColCmpOp::Eq => 4,
            ColCmpOp::Ne => 5,
        }
    }
}

/// [colcmp] `a OP b` between two WORD columns of one scan (3VL: a NULL
/// operand never passes). Always a row-grain residue — never zone-pruned
/// or condition-cached; single-relation lowering refuses it typed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColCmpTerm {
    pub a: u32,
    pub b: u32,
    pub op: ColCmpOp,
    pub fp: Fingerprint,
}

impl ColCmpTerm {
    pub fn new(a: u32, b: u32, op: ColCmpOp, ty: TypMeta) -> ColCmpTerm {
        let fp = Fingerprint {
            attno: a,
            ty_oid: ty.oid,
            collation_oid: ty.collation,
            op: OpTag::ColCmp,
            canon: CanonConst::Ints(b as i64, op.code()),
        };
        ColCmpTerm { a, b, op, fp }
    }
    #[inline(always)]
    pub fn eval_v(&self, av: i64, a_ok: bool, bv: i64, b_ok: bool) -> bool {
        a_ok && b_ok && self.op.eval(av, bv)
    }
}

/// A conjunction of terms after CNF split. The first `frame_terms` terms
/// are the FRAME predicate (zone-prunable prefix — a stats election):
/// cached at granule grain under `frame_fingerprint()` — hot-shape share
/// one entry. The remaining terms are residues, evaluated on frame
/// survivors inside the stencil.
#[derive(Clone, Debug)]
pub struct PredSpec {
    pub terms: Vec<PredTerm>,
    pub frame_terms: usize,
    /// [famB M1] Varlena conjuncts. Always FRAME.
    pub var_terms: Vec<VarPredTerm>,
    /// [colcmp] Column-vs-column word conjuncts. Always row-grain
    /// residues; families without the lane refuse when non-empty.
    pub col_terms: Vec<ColCmpTerm>,
}

impl PredSpec {
    pub fn all(terms: Vec<PredTerm>) -> PredSpec {
        let n = terms.len();
        PredSpec { terms, frame_terms: n, var_terms: Vec::new(), col_terms: Vec::new() }
    }
    /// The condition-cache key of the frame conjunct set (int frame terms
    /// + ALL varlena terms), canonicalized by ConjFp's sort.
    pub fn frame_fingerprint(&self) -> ConjFp {
        let mut fps: Vec<Fingerprint> =
            self.terms[..self.frame_terms].iter().map(|t| t.fp.clone()).collect();
        fps.extend(self.var_terms.iter().map(|t| t.fp.clone()));
        ConjFp::new(fps)
    }
    /// [sqe-m2] The condition-cache key of the WHOLE conjunction (frame +
    /// residues + varlena terms): the identity of the FINAL survivor
    /// plane.
    pub fn full_fingerprint(&self) -> ConjFp {
        let mut fps: Vec<Fingerprint> = self.terms.iter().map(|t| t.fp.clone()).collect();
        fps.extend(self.var_terms.iter().map(|t| t.fp.clone()));
        fps.extend(self.col_terms.iter().map(|t| t.fp.clone()));
        ConjFp::new(fps)
    }
    pub fn frame(&self) -> &[PredTerm] {
        &self.terms[..self.frame_terms]
    }
    pub fn residues(&self) -> &[PredTerm] {
        &self.terms[self.frame_terms..]
    }
    /// [oracle, ruling Q4 2026-08-18] The canonical structural key of the
    /// FRAME conjunct set — the exact term set `frame_fingerprint()` is
    /// minted from.
    #[cfg(feature = "oracle")]
    pub fn frame_structural(&self) -> StructuralPred {
        StructuralPred::new(
            self.terms[..self.frame_terms].to_vec(),
            self.var_terms.clone(),
        )
    }
    /// [oracle] The canonical structural key of the WHOLE conjunction —
    /// the exact term set `full_fingerprint()` is minted from.
    #[cfg(feature = "oracle")]
    pub fn full_structural(&self) -> StructuralPred {
        StructuralPred::new(self.terms.clone(), self.var_terms.clone())
    }
    /// [oracle] The structural key matching an explicit cache fingerprint:
    /// every condition-cache fp is this predicate's full or frame identity
    /// (exec.rs replay/publish contract). None = the fp is not mintable
    /// from this predicate — itself an identity-plumbing bug the caller
    /// reports loudly.
    #[cfg(feature = "oracle")]
    pub fn structural_for(&self, fp: &ConjFp) -> Option<StructuralPred> {
        if *fp == self.full_fingerprint() {
            Some(self.full_structural())
        } else if *fp == self.frame_fingerprint() {
            Some(self.frame_structural())
        } else {
            None
        }
    }
}

/// [oracle, ruling Q4 2026-08-18] The canonical STRUCTURAL key a ConjFp
/// was minted from: the full predicate term set, canonically sorted, with
/// DERIVE-BASED equality — every semantic field of PredTerm/VarPredTerm
/// participates by construction, so a field added to a term type but left
/// out of the Fingerprint encoding is caught the moment two such
/// predicates meet on a cache hit. Production builds never carry this
/// type (hash-as-identity stays the production key); oracle/CI builds
/// store it alongside every fingerprint-keyed cache entry and compare on
/// every hit — mismatch = fingerprint collision or encoder omission.
#[cfg(feature = "oracle")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralPred {
    pub int_terms: Vec<PredTerm>,
    pub var_terms: Vec<VarPredTerm>,
}

#[cfg(feature = "oracle")]
impl StructuralPred {
    /// Canonicalize by the terms' FULL derived order (never by the
    /// fingerprint alone — a fingerprint collision must not be able to
    /// reorder structurally distinct terms into a false mismatch or a
    /// false match).
    pub fn new(mut int_terms: Vec<PredTerm>, mut var_terms: Vec<VarPredTerm>) -> StructuralPred {
        int_terms.sort_unstable();
        var_terms.sort_unstable();
        StructuralPred { int_terms, var_terms }
    }
}

/// Aggregate physical ops. The PoC's render-smuggling variants are dead:
/// `MinDate/MaxDate` → `Min/Max` + DATE output TypMeta; `AvgExact` →
/// `Avg` + the exact-numeric render law elected from the input width
/// (currency-insertion.md §5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggOp {
    CountStar,
    Sum,
    Avg,
    /// Emit the matching rows' values of cols[0] (filter scans: hot-shape).
    EmitMatches,
    /// MIN over an int-word column. Over an empty domain: NULL (the
    /// validity leg of the answer — never a sentinel).
    Min,
    Max,
    /// MIN over a varlena column's bytes (dict-entry candidates).
    MinBytes,
    /// COUNT(DISTINCT col).
    CountDistinct,
    /// SUM(col + k) — `AggSpec.k` carries the shift.
    SumShifted,
    /// AVG(octet_length(col)) — varlena length average (hot-shape; over
    /// rows with len != 0 when the plan carries the `<> ''` conjunct).
    AvgLen,
    /// AVG(length(col)) — varlena CHARACTER-count average (the official
    /// ClickBench q27/q28 text: `length(text)` = textlen, chars not
    /// bytes). Identical shape/fold law to AvgLen; only the per-value
    /// length kernel differs (UTF-8 char count — continuation bytes
    /// excluded). len != 0 gates coincide: a value has 0 chars iff it
    /// has 0 bytes.
    AvgCharLen,
    /// var_samp/variance over an int word column ({count,sum,sumsq}
    /// decomposition — fold.rs AggFoldOp::SumSq; finisher = PG's
    /// numeric_poly_stddev_internal closed form at the answer seam).
    VarSamp,
    VarPop,
    StddevSamp,
    StddevPop,
    /// bit_and/bit_or over an int word column (int2/4/8). Bitwise ops are
    /// width-local: sign-extended words fold correctly and the render
    /// truncates back to the input width.
    BitAnd,
    BitOr,
    /// [sortgrp v1] string_agg(col, const-delim ORDER BY keys): delimiter
    /// BETWEEN values (none leading), NULL values skipped, NULL delimiter
    /// = plain concatenation (the observable varlena.c:5446-5494/5618-5636
    /// law). The delimiter rides `Params.sortagg.delims[agg_idx]`.
    StringAgg,
    /// [sortgrp v1] array_agg(col ORDER BY keys): NULL elements are KEPT
    /// (array_userfuncs.c:587-591); empty input answers NULL.
    ArrayAgg,
    /// [sortgrp v1] percentile_disc(p) WITHIN GROUP (ORDER BY col):
    /// rank ceil(p·N) over the group's NON-NULL inputs in sort order
    /// (orderedsetaggs.c:427-492); N=0 answers NULL. `AggSpec.direct`
    /// carries p as f64 bits.
    PercentileDisc,
    /// [sortgrp v1] percentile_cont(p) WITHIN GROUP (ORDER BY float8-cast
    /// col): floor/ceil(p·(N−1)) bracketing + float8_lerp
    /// (orderedsetaggs.c:501-608). Answers float8.
    PercentileCont,
    /// [sortgrp v1] mode() WITHIN GROUP (ORDER BY col): the FIRST maximal
    /// run in the engine's deterministic sort order (a new run replaces
    /// only on strictly greater count — orderedsetaggs.c:1077-1120).
    Mode,
    /// [aggqual] SUM(DISTINCT col): the per-group distinct VALUE set
    /// feeds the sum fold — NULLs never enter the set (strict), empty
    /// set answers NULL. Distinct ops are their own variants (never a
    /// flag on Sum/Avg) so an arm without the dedup machinery fails
    /// closed at its exhaustive match.
    SumDistinct,
    /// [aggqual] AVG(DISTINCT col) = distinct-sum / distinct-count.
    AvgDistinct,
    /// [aggqual] array_agg(DISTINCT col ORDER BY col): ArrayAgg with
    /// run-boundary dedup over the sorted group slice (adjacent-equal
    /// skip; two NULLs are NOT DISTINCT — one NULL element kept).
    ArrayAggDistinct,
}

impl AggOp {
    /// Both varlena length averages (byte and char kernels) — the shape
    /// gates that route AvgLen route AvgCharLen identically; only the
    /// per-value length kernel differs at the accumulation sites.
    #[inline]
    pub fn is_avglen(self) -> bool {
        matches!(self, AggOp::AvgLen | AggOp::AvgCharLen)
    }
}

/// [P4-1] Fused arithmetic on a fold INPUT: the CLOSED monomorphic
/// shape vocabulary, evaluated per surviving row into the fold's i64
/// word — never a per-row expression tree (the de-interpretation law:
/// the stencil matches the shape ONCE per granule and runs a closed
/// loop). The primary operand is `AggSpec.col`; the shapes carry the
/// second operand / constant. `a + k` does NOT live here — it lowers to
/// the answer-time algebra (`AggOp::SumShifted`: Σ(a+k) = Σa + k·n).
///
/// OVERFLOW LAW (the honest decision, the variance-witness precedent):
/// PG evaluates the row expression in the OPERATOR's result type and
/// ERRORS on overflow — identical answers include identical errors, and
/// this engine never errors mid-fold (the refuse-and-replay law is
/// future work; div/mod stay excluded for the same reason). So the
/// planner admits a shape only under a witnessed proof that NO evaluated
/// row can overflow the PG op's result type (interval arithmetic over
/// exact stats domains, type ranges as the fallback evidence) — anything
/// unproven is a TYPED refusal, never a wrapped fold. Inside that proof
/// every per-row word satisfies |w| < 2^63, so the i128 sum accumulation
/// stays exact for any n < 2^64 rows (the SumSq exactness argument).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldExpr {
    /// col * b — both signed word columns. `w` = the PG mul op's result
    /// width (4 or 8), the overflow-proof obligation.
    MulCC { b: u32, w: u8 },
    /// col * (k - b) — the TPC-H revenue shape. `wi` = the inner
    /// subtraction op's result width, `w` = the outer mul's; BOTH ops
    /// carry the no-overflow proof obligation.
    MulKSub { k: i64, b: u32, w: u8, wi: u8 },
    /// [scale-alg] Packed-mantissa product `col · (k ± b)` over TWO
    /// PackedNumeric word lanes (the WITNESSED FIXED-POINT ALGEBRA
    /// ruling, 2026-08-19): the per-row word is the EXACT mantissa of
    /// PG's numeric product at scale sa+sb — mul adds scales, and `k`
    /// is authored as an exact mantissa at b's scale (add/sub aligns
    /// scales; const dscale <= sb so PG's inner dscale IS sb). Plain
    /// `a·b` authors k=0, sub=false. PG numeric arithmetic never
    /// overflows, so the ONLY proof obligation is representability:
    /// the planner's witnessed mantissa domains must prove every
    /// per-row product fits the i64 fold word (else typed refusal —
    /// never a wrapped fold). The render scale (sa+sb) rides the
    /// answer seam, not this shape.
    PackedMulK { k: i64, sub: bool, b: u32 },
    /// [scale-alg] `col · (k1 ± b) · (k2 ± c)` — the revenue×tax shape
    /// (`l_extendedprice·(1−l_discount)·(1+l_tax)`), THREE packed
    /// lanes, render scale sa+sb+sc. Evaluation order is the kernel's
    /// left-fold: (col·(k1±b)) first — the admission witness proves
    /// the INTERMEDIATE product fits i64 too, so the i64 evaluation
    /// equals PG's exact product whenever admitted.
    PackedMulK2 { k1: i64, sub1: bool, b: u32, k2: i64, sub2: bool, c: u32 },
}

impl FoldExpr {
    /// The second input column (fold NULL law: a NULL in ANY operand
    /// makes the row's input NULL — the fold skips it).
    pub fn col2(&self) -> u32 {
        match self {
            FoldExpr::MulCC { b, .. }
            | FoldExpr::MulKSub { b, .. }
            | FoldExpr::PackedMulK { b, .. }
            | FoldExpr::PackedMulK2 { b, .. } => *b,
        }
    }
    /// [scale-alg] The third input column (PackedMulK2 only) — same
    /// staging/decode/NULL obligations as col2.
    pub fn col3(&self) -> Option<u32> {
        match self {
            FoldExpr::PackedMulK2 { c, .. } => Some(*c),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggSpec {
    pub op: AggOp,
    pub col: Option<u32>,
    /// Op-specific constant (SumShifted shift, stored as i64 bits —
    /// consumers read `k as i64`). 0 otherwise.
    pub k: u64,
    /// [P4-1] fused arithmetic on the fold input (None = the input is
    /// the bare column word). Sum/Avg legs only; admission in planner.
    pub expr: Option<FoldExpr>,
    /// [sortgrp v1] The ordered-set aggs' CONST direct argument (the
    /// percentile fraction) as f64 bits, resolved at lowering; variable
    /// direct args refuse. None for every other op.
    pub direct: Option<u64>,
    /// Input column type (None for CountStar).
    pub in_ty: Option<TypMeta>,
    /// Output type — computed at lowering from input TypMeta + agg
    /// semantics (PG's rule set); what the P2-1 DestReceiver binds
    /// out-functions from.
    pub out: TypMeta,
}

impl AggSpec {
    pub fn new(op: AggOp, col: Option<u32>, in_ty: Option<TypMeta>) -> AggSpec {
        let out = Self::out_ty(op, in_ty);
        AggSpec { op, col, k: 0, expr: None, direct: None, in_ty, out }
    }
    /// [P4-1] attach a fused-arithmetic fold-input shape.
    pub fn with_expr(mut self, e: FoldExpr) -> AggSpec {
        self.expr = Some(e);
        self
    }
    /// [sortgrp v1] attach the const direct argument (percentile fraction).
    pub fn with_direct(mut self, frac: f64) -> AggSpec {
        self.direct = Some(frac.to_bits());
        self
    }
    /// The resolved direct argument (panics when absent — lowering bug).
    pub fn direct_f64(&self) -> f64 {
        f64::from_bits(self.direct.expect("ordered-set agg without a direct arg"))
    }
    /// The second input column of an expr leg (staging/decode currency).
    pub fn col2(&self) -> Option<u32> {
        self.expr.map(|e| e.col2())
    }
    /// [scale-alg] The third input column of an expr leg (PackedMulK2).
    pub fn col3(&self) -> Option<u32> {
        self.expr.and_then(|e| e.col3())
    }
    pub fn out_ty(op: AggOp, in_ty: Option<TypMeta>) -> TypMeta {
        match op {
            AggOp::CountStar | AggOp::CountDistinct => TypMeta::INT8,
            // sum(int2/int4)→int8, sum(int8)→numeric; the i128 answer lane
            // renders both correctly.
            AggOp::Sum | AggOp::SumShifted | AggOp::SumDistinct => TypMeta::NUMERIC,
            AggOp::Avg | AggOp::AvgLen | AggOp::AvgCharLen | AggOp::AvgDistinct => TypMeta::NUMERIC,
            // PG: variance/stddev over any int family answer numeric.
            AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                TypMeta::NUMERIC
            }
            AggOp::Min | AggOp::Max | AggOp::EmitMatches => {
                in_ty.expect("Min/Max/Emit need an input type")
            }
            // PG: bit_and(intN)/bit_or(intN) answer intN.
            AggOp::BitAnd | AggOp::BitOr => in_ty.expect("BitAnd/BitOr need an input type"),
            AggOp::MinBytes => in_ty.unwrap_or(TypMeta::TEXT_C),
            // [sortgrp v1] string_agg(text,text) answers text; disc/mode
            // answer the input type; cont answers float8; array_agg's
            // `out` carries the ELEMENT type (Render::ArrayOf wraps it).
            AggOp::StringAgg => in_ty.unwrap_or(TypMeta::TEXT_C),
            AggOp::ArrayAgg | AggOp::ArrayAggDistinct | AggOp::PercentileDisc | AggOp::Mode => {
                in_ty.expect("tier-2 agg needs an input type")
            }
            AggOp::PercentileCont => TypMeta::FLOAT8,
        }
    }
    /// The exact-decimal AVG law (hot-shape UserID): width-8 integer inputs sum
    /// past 2^53 — render from the exact {sum,count} pair.
    pub fn avg_exact(&self) -> bool {
        matches!(self.op, AggOp::Avg) && self.in_ty.map(|t| t.width == 8).unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderBy {
    CountDesc,
    KeyAsc,
    None,
    /// ORDER BY <col> ASC (zone-order walks: the order column).
    ColAsc(u32),
    /// ORDER BY <col a> ASC, <col b> ASC.
    ColThenColAsc(u32, u32),
    /// ORDER BY agg[idx] DESC (hot-shape ORDER BY l DESC).
    AggDesc(u32),
}

/// [famB M1] Group-key expression (beyond plain columns).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyExpr {
    Col(u32),
    /// EventTime truncated to the minute.
    TruncMinute(u32),
    /// Minute-of-hour bucket.
    Minute(u32),
    /// Host extraction over a URL-ish column (hot-shape derived key;
    /// hand-implemented in derived_key.rs — no regex crate).
    HostRegex(u32),
    /// col - k.
    MinusConst(u32, i64),
    /// The hot-shape CASE source expression (se/adv/referer).
    CaseSrc { se: u32, adv: u32, referer: u32 },
    Const1,
    /// Hour of day of `to_timestamp(col / 1e6)` under a constant offset (s east).
    HourBucket { col: u32, off_s: i64 },
}

/// `EXTRACT(HOUR FROM to_timestamp(us / 1000000))` hop for hop (int84div,
/// float8_timestamptz's rounded product, the local floor modulo).
pub fn hour_bucket(us: i64, off_s: i64) -> u8 {
    let q = us / 1_000_000;
    let t = ((q as f64 - 946_684_800.0) * 1_000_000.0).round_ties_even() as i64;
    let local = t as i128 + off_s as i128 * 1_000_000;
    (local.rem_euclid(86_400_000_000) / 3_600_000_000) as u8
}

/// [sortgrp v1] One aggregate's delimiter slot (aligned with `PlanNode.agg`).
/// `None` = the op takes no delimiter; `Null` = SQL NULL delimiter (plain
/// concatenation — varlena.c's skipped-append law); `Bytes` = the const
/// delimiter's payload bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggDelim {
    None,
    Null,
    Bytes(Vec<u8>),
}

/// [sortgrp v1] The SortGrouped family's node data: ONE statement-level
/// sort spec over INPUT columns (`TopKKey.col` = attno, NOT an answer
/// slot — the family sorts survivor rows, not answers) plus the per-agg
/// delimiter slots. Mixed per-agg specs refuse at lowering (the
/// one-sort-spec v1 law, sort-grouped-family.md §3).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SortAggSpec {
    pub keys: Vec<TopKKey>,
    pub delims: Vec<AggDelim>,
}

/// [winframes v2] Frame vocabulary — C's frameOptions distilled
/// (nodeWindowAgg.c update_frameheadpos/update_frametailpos). Offsets
/// are non-negative (negative offsets refuse at lowering — C errors at
/// executor start, and refusal hands the statement to that executor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameMode {
    /// Positional bounds: head = cur - n, tail = cur + m + 1, clamped.
    Rows,
    /// Value bounds over ONE window ORDER BY key (in_range law); the
    /// CURRENT ROW bounds are peer-group edges (the v1 default law).
    Range,
    /// Peer-group-count bounds (currentgroup +/- n, group edges).
    Groups,
}

/// One frame bound. `Preceding`/`Following` carry the offset in the
/// mode's domain: rows (Rows), peer groups (Groups), or the SCALED
/// order-key embed delta (Range — key embeds x `FrameSpec::scale`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i128),
    CurrentRow,
    Following(i128),
    UnboundedFollowing,
}

/// [winv3] The EXCLUDE clause (C row_is_in_frame's exclusion leg,
/// nodeWindowAgg.c:1511-1533): a sub-range of the [head, tail) frame is
/// subtracted per row. `CurrentRow` excludes exactly the current row;
/// `Group` excludes its whole peer group; `Ties` excludes the peers but
/// KEEPS the current row. Without a window ORDER BY every partition row
/// is a peer (Group excludes everything, Ties keeps only self).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameExclusion {
    None,
    CurrentRow,
    Group,
    Ties,
}

/// [winv3] One calendar interval frame offset (month-carrying) for
/// Range bounds over date/timestamp keys: C's exact timestamp +/-
/// interval (timestamp.c timestamp_pl_interval — add months via
/// j2date/date2j with end-of-month clamping, then days, then usec; for
/// tz-free keys the day+time legs are fixed usec, folded into `usecs`).
/// The matching `FrameBound` offset carries the CONSERVATIVE linear
/// magnitude (months <= 31 days) — band admission only; the walk uses
/// this exact form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalOff {
    pub months: i32,
    /// day * USECS_PER_DAY + time (exact for tz-free keys).
    pub usecs: i64,
}

/// [winframes v2] The frame law of one WindowAgg. `scale` multiplies
/// Range order-key embeds into the offset domain (date keys x usec/day
/// under interval offsets; 1 elsewhere). `band` = the valid SCALED
/// range of the key domain (datetime keys: usec timestamps): admission
/// refuses when any
/// witnessed key +/- offset could leave it — outside the band C's
/// in_range arithmetic errors or goes non-linear (infinities), and the
/// refused statement reproduces that on the heap executor. i128 math
/// reproduces C's saturating int in_range exactly (int.c:698-730: the
/// overflowed sum still answers the true comparison).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameSpec {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
    pub scale: i128,
    pub band: Option<(i128, i128)>,
    /// [winv3] The EXCLUDE clause (None = no exclusion).
    pub exclusion: FrameExclusion,
    /// [winv3] Calendar (month-carrying) start/end offsets: Some
    /// replaces the matching bound's LINEAR offset in the walk (the
    /// bound then carries the conservative magnitude for band
    /// admission only). Range mode + datetime key only.
    pub cal_start: Option<CalOff>,
    pub cal_end: Option<CalOff>,
}

impl Default for FrameSpec {
    /// The DEFAULT frame: RANGE UNBOUNDED PRECEDING .. CURRENT ROW.
    fn default() -> FrameSpec {
        FrameSpec {
            mode: FrameMode::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
            scale: 1,
            band: None,
            exclusion: FrameExclusion::None,
            cal_start: None,
            cal_end: None,
        }
    }
}

impl FrameSpec {
    /// Either bound is an OFFSET bound (offset 0 included — still the
    /// in_range law, not the CURRENT ROW peer law).
    pub fn has_offsets(&self) -> bool {
        matches!(self.start, FrameBound::Preceding(_) | FrameBound::Following(_))
            || matches!(self.end, FrameBound::Preceding(_) | FrameBound::Following(_))
    }

    /// Largest offset magnitude either bound carries (band admission).
    pub fn max_offset(&self) -> i128 {
        let of = |b: &FrameBound| match b {
            FrameBound::Preceding(o) | FrameBound::Following(o) => *o,
            _ => 0,
        };
        of(&self.start).max(of(&self.end))
    }
}

/// [winserve v1] Window function physical ops. The rank family and
/// lead/lag are frame-insensitive; the aggregate and first/last/nth
/// ops answer over the `FrameSpec` frame ([winframes v2] explicit
/// ROWS/RANGE/GROUPS bounds, [winv3] EXCLUDE + calendar offsets) —
/// the DEFAULT frame is RANGE UNBOUNDED PRECEDING..CURRENT ROW (peer
/// rows share the value — nodeWindowAgg.c:1441-1476 row_is_in_frame's
/// are_peers leg); without a window ORDER BY every partition row is a
/// peer, so the value is the whole partition's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WinOp {
    /// row_number(): partition position + 1 (windowfuncs.c:84-91).
    RowNumber,
    /// rank(): peer-group head position + 1 (windowfuncs.c:138-151).
    Rank,
    /// dense_rank(): peer-group ordinal (windowfuncs.c:200-213).
    DenseRank,
    /// count(*) OVER: frame row count (all rows, NULLs included).
    CountStar,
    /// count(col) OVER: frame non-null count.
    Count,
    /// sum(int2/4/8) OVER: exact i128 frame sum; all-NULL frame = NULL.
    Sum,
    Min,
    Max,
    /// avg(int2/4/8) OVER: exact {sum,count} ratio per row (the grouped
    /// AvgNumeric render law).
    Avg,
    /// [winframes v2] lead(col[, n]): partition-positional read at
    /// position + off (out of partition = NULL; frame-independent —
    /// WinGetFuncArgInPartition).
    Lead,
    /// lag(col[, n]): position - off, same law.
    Lag,
    /// first_value(col): the frame-head row's cell (empty frame = NULL).
    FirstValue,
    /// last_value(col): the frame-tail row's cell.
    LastValue,
    /// nth_value(col, n): head + n - 1 when in frame (n >= 1 admitted).
    NthValue,
}

/// [winserve v1] One window function column: op + input column (None for
/// the rank family and count(*)) + type facts resolved at lowering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WinFuncSpec {
    pub op: WinOp,
    pub col: Option<u32>,
    /// Input column type (None when col is None).
    pub in_ty: Option<TypMeta>,
    /// Output type (PG's window-agg output rules; rank family = int8).
    pub out: TypMeta,
    /// [winframes v2] lead/lag offset (lead/lag default 1; lead/lag
    /// admit negatives — C reads the other direction) or nth_value's
    /// n (>= 1 admitted). 0 for every other op.
    pub off: i64,
}

impl WinFuncSpec {
    pub fn new(op: WinOp, col: Option<u32>, in_ty: Option<TypMeta>) -> WinFuncSpec {
        let off = match op {
            WinOp::Lead | WinOp::Lag | WinOp::NthValue => 1,
            _ => 0,
        };
        Self::with_off(op, col, in_ty, off)
    }

    pub fn with_off(op: WinOp, col: Option<u32>, in_ty: Option<TypMeta>, off: i64) -> WinFuncSpec {
        let out = match op {
            // The rank family and both counts answer int8.
            WinOp::RowNumber | WinOp::Rank | WinOp::DenseRank | WinOp::CountStar
            | WinOp::Count => TypMeta::INT8,
            // PG: sum(int2/int4) OVER -> int8; sum(int8) OVER -> numeric.
            WinOp::Sum => {
                if in_ty.map(|t| t.width == 8).unwrap_or(false) {
                    TypMeta::NUMERIC
                } else {
                    TypMeta::INT8
                }
            }
            WinOp::Avg => TypMeta::NUMERIC,
            // Value functions answer the input type.
            WinOp::Min | WinOp::Max | WinOp::Lead | WinOp::Lag | WinOp::FirstValue
            | WinOp::LastValue | WinOp::NthValue => {
                in_ty.expect("value functions need an input type")
            }
        };
        WinFuncSpec { op, col, in_ty, out, off }
    }
}

/// [winv4] One runCondition comparison op, normalized wfunc-on-left
/// (`funcs[func] OP val`). The seam commutes `const OP wfunc` forms
/// before authoring, exactly as C's opexpr carries either arrangement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunCmp {
    Lt,
    Le,
    Eq,
    Ge,
    Gt,
}

impl RunCmp {
    #[inline]
    pub fn eval(self, v: i64, rhs: i64) -> bool {
        match self {
            RunCmp::Lt => v < rhs,
            RunCmp::Le => v <= rhs,
            RunCmp::Eq => v == rhs,
            RunCmp::Ge => v >= rhs,
            RunCmp::Gt => v > rhs,
        }
    }
}

/// [winv4] One WindowAgg runCondition leg (nodeWindowAgg.c:2404-2462):
/// the plan-level qual PG attaches for provably-monotonic window
/// functions (row_number/rank/dense_rank/count under find_window_run_
/// conditions, allpaths.c:2266-2441). The engine's law is C's exact
/// executor law for the TOP window: per partition, rows are emitted in
/// walk order WHILE every leg passes; the FIRST failing row ends the
/// partition's emission (STRICT pass-through when PARTITION BY exists,
/// WINDOWAGG_DONE otherwise — either way the failing row and everything
/// after it in the partition is excluded). `func` indexes the COMBINED
/// function list; the target must render `FVal::N` (rank/count class).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WinRunCond {
    pub func: usize,
    pub op: RunCmp,
    pub val: i64,
}

/// [winv4] The 2-chain upper window spec (stacked WindowAggs sharing
/// ONE input sort): same PARTITION BY columns as the bottom spec, ORDER
/// BY = the first `n_ord` keys of the bottom spec's `order` (the
/// shared-sort-prefix law — PG only stacks WindowAggs without an
/// intermediate Sort when the upper spec's sort requirements are a
/// prefix of the delivered order). `funcs` append AFTER the bottom
/// spec's in the answer (emit ++ funcs ++ chain.funcs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WinChain {
    pub n_ord: usize,
    pub frame: FrameSpec,
    pub funcs: Vec<WinFuncSpec>,
}

/// [winserve v1] The WindowServe family's node data. `emit` lists the
/// pass-through output columns (attnos, answer order); the answer is
/// `emit` columns then `funcs` columns ([winv4]: then `chain` function
/// columns), one row per surviving input row. `order` keys name INPUT
/// columns (`TopKKey.col` = attno), with the child Sort's
/// desc/nulls_first — the peer-group law reads THESE keys' equality.
/// Empty `part_cols` = one whole-statement partition; empty `order` =
/// whole-partition frames, all rows peers.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WindowSpec {
    pub part_cols: Vec<u32>,
    pub order: Vec<TopKKey>,
    pub funcs: Vec<WinFuncSpec>,
    pub emit: Vec<u32>,
    /// [winframes v2] The frame law (None on Default derive would be
    /// wrong — Default::default() IS the SQL default frame).
    pub frame: FrameSpec,
    /// [winv4] runCondition legs (ANDed); empty = no run condition.
    /// Only lawful without `chain` (C's pass-through modes for non-top
    /// windows are outside the served vocabulary — the seam refuses).
    pub run_conds: Vec<WinRunCond>,
    /// [winv4] The stacked upper window spec (2-chain), None = single.
    pub chain: Option<WinChain>,
}

impl WindowSpec {
    /// [winv4] Every function column in ANSWER order: the bottom spec's
    /// then the chain's.
    pub fn all_funcs(&self) -> impl Iterator<Item = &WinFuncSpec> {
        self.funcs.iter().chain(self.chain.iter().flat_map(|c| c.funcs.iter()))
    }

    /// [winv4] Combined function-column count.
    pub fn n_funcs(&self) -> usize {
        self.funcs.len() + self.chain.as_ref().map(|c| c.funcs.len()).unwrap_or(0)
    }
}

/// One pushed-down sort key over ANSWER columns (grouped answers are
/// key-cols-then-agg-cols). `nulls_first` is absolute (PG semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopKKey {
    pub col: u32,
    pub desc: bool,
    pub nulls_first: bool,
    /// [bpchar-order] compare Bytes cells on the bcTruelen-trimmed
    /// images — the pushed bound's comparator must mirror the answer
    /// boundary's basis exactly.
    pub trim: bool,
    /// `Some(l)`: the key is the word span `cols[col] - cols[l]` (NULL if either is).
    pub lo: Option<u32>,
}

/// A pushed-down bounded-answer goal for grouped shapes: aggregation
/// folds every row and finalizes every group exactly as before; only the
/// ANSWER is bounded to the top `n` groups under `keys` (empty = any `n`
/// groups). `native` marks specs a (count DESC, key ASC)-ordered arm
/// selection serves directly; non-native specs ride the collector arms
/// and are bounded at the answer boundary (`answer::apply_topk`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopK {
    pub keys: Vec<TopKKey>,
    pub n: usize,
    pub native: bool,
}

/// [sqe-tpch-mech] Generalized HAVING vocabulary: ONE aggregate-comparison
/// conjunct (`agg(col) <op> const`) fused into the grouped fold — evaluated
/// per group AT THE ANSWER BOUNDARY from the already-folded cells (never a
/// second pass over rows). `agg` indexes the node's aggregate list
/// (`PlanNode.agg` / `JoinAggNode.aggs`); the referenced aggregate need not
/// be a delivered output column (a HAVING-only aggregate rides as a hidden
/// answer column the emitter drops). SQL 3VL: a NULL aggregate value (an
/// empty fold) never passes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HvOp {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HavingCmp {
    pub agg: u32,
    pub op: HvOp,
    /// The typed comparison constant in the aggregate's answer word
    /// domain (i64 consts; sum answers compare exactly in i128).
    pub rhs: i64,
}

impl HavingCmp {
    /// The group-filter law: None = NULL aggregate = UNKNOWN = drop.
    #[inline]
    pub fn keep(&self, v: Option<i128>) -> bool {
        let Some(v) = v else { return false };
        let r = self.rhs as i128;
        match self.op {
            HvOp::Gt => v > r,
            HvOp::Ge => v >= r,
            HvOp::Lt => v < r,
            HvOp::Le => v <= r,
            HvOp::Eq => v == r,
            HvOp::Ne => v != r,
        }
    }
}

/// Reject group-key rows whose (varlena) key is the empty string.
pub const F_DROP_EMPTY_KEY: u32 = 1 << 0;
/// Executor-set: honest arm — the condition cache is bypassed for READS
/// but still populated as the run proceeds (fill-on-cold).
pub const F_HONEST: u32 = 1 << 1;
/// [R6d] Guard-lowered row-grain survivor plane: the entry-grain verdict
/// build (VerdictWords over dict entries) is SKIPPED and every varlena
/// conjunct evaluates per row (the hash-plane-class exhaustive path).
/// Set by the planner's grain election (`planner::elect_entry_grain`)
/// when a stats-witnessed guard fails — NEVER a refusal; answers are
/// identical either grain. Consumed by the WindowReplay var lane.
pub const F_ROW_GRAIN: u32 = 1 << 2;

/// Thread-claim class per family (PLANNER-SPEC §1.6). The concrete count
/// is elected at execution from survivor-work stats — the CLASS is a plan
/// property, the COUNT is not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClaimClass {
    /// Bandwidth-bound folds: full pool width.
    Counting,
    /// Survivor granules << width: min(width, survivor_granules).
    SkipDominated,
    /// Scatter/dedupe pairs: ~width/2.
    PairDistinct,
    /// Zone-order walks, dict-head: serial.
    BoundedWalk,
}

/// The GOAL/REQUEST vector: every piece of context a subplan's cost
/// depends on goes HERE, never through a cost function. Canonicalized
/// before any memoization.
#[derive(Clone, Debug, Default)]
pub struct Goal {
    /// Typed conjunction fingerprints this plan can replay (sorted).
    pub fingerprints: Vec<ConjFp>,
    pub claim_class: Option<ClaimClass>,
}

impl Goal {
    pub fn canonicalize(&mut self) {
        self.fingerprints.sort_unstable();
        self.fingerprints.dedup();
    }
    /// Partial-order covers() test.
    pub fn covers(&self, other: &Goal) -> bool {
        other.fingerprints.iter().all(|f| self.fingerprints.binary_search(f).is_ok())
            && (other.claim_class.is_none() || other.claim_class == self.claim_class)
    }
}

/// Physical attributes: partition-law inputs, thread claim, flags, and
/// the goal vector. Pre-elected values are FUNCTIONS of bank stats
/// computed by the planner; 0 = "elect at execution from stats".
#[derive(Clone, Debug)]
pub struct Params {
    pub group_cols: Vec<u32>,
    pub order: OrderBy,
    pub offset: usize,
    /// usize::MAX = no LIMIT.
    pub limit: usize,
    /// Partition-law input: per-partition accumulator budget (bytes).
    pub l2_bytes: usize,
    /// Partition-law input: accumulator slot cost (bytes) at <=1/2 load.
    pub slot_bytes: usize,
    /// Thread claim; 0 = elect from stats at execution.
    pub thread_claim: usize,
    pub flags: u32,
    pub goal: Goal,
    /// Group-key expressions when any key is not a plain column
    /// (empty = `group_cols` are the keys, in order).
    pub key_exprs: Vec<KeyExpr>,
    /// Varlena columns required <> ''.
    pub ne_empty_cols: Vec<u32>,
    /// HAVING COUNT(*) > k (0 = none; hot-shape).
    pub having_min_count: u64,
    /// Bounded-answer goal lowered from an ORDER/LIMIT above the group
    /// (None = full answer, the emit-cap law applies).
    pub topk: Option<TopK>,
    /// [sqe-tpch-mech] Generalized HAVING fused into the grouped fold
    /// (None = no group filter; `having_min_count` stays the legacy
    /// count(*)-only authoring spelling).
    pub having: Option<HavingCmp>,
    /// [sortgrp v1] The SortGrouped family's statement sort spec + per-agg
    /// delimiters (None for every other family).
    pub sortagg: Option<SortAggSpec>,
    /// [winserve v1] The WindowServe family's window spec (None for
    /// every other family).
    pub window: Option<WindowSpec>,
    /// [aggqual] Per-aggregate FILTER (WHERE ...) predicates, index-
    /// aligned with `PlanNode.agg` when non-empty (empty = no leg
    /// filters). Each is an int-conjunct PredSpec in the SAME term
    /// vocabulary as the statement predicate; a leg's fold input is
    /// (word, valid && filter_pass) — 3VL: only TRUE passes. Serving
    /// arms without the composition refuse at admission (fail-closed).
    pub agg_filters: Vec<Option<PredSpec>>,
}

impl Params {
    /// Emission bound for grouped arms whose bounded selection runs
    /// under (count DESC, key ASC). A native pushed bound is served by
    /// that selection directly; a non-native bound must see the FULL
    /// group set (collector mode) and is trimmed at the answer boundary.
    pub fn emit_cap(&self) -> usize {
        match &self.topk {
            Some(t) if t.native => t.n,
            Some(_) => usize::MAX,
            None => self.offset + self.limit.min(1 << 20),
        }
    }
}

impl Default for Params {
    fn default() -> Params {
        Params {
            group_cols: Vec::new(),
            order: OrderBy::None,
            offset: 0,
            limit: usize::MAX,
            l2_bytes: 512 * 1024,
            slot_bytes: 32,
            thread_claim: 0,
            flags: 0,
            goal: Goal::default(),
            key_exprs: Vec::new(),
            ne_empty_cols: Vec::new(),
            having_min_count: 0,
            topk: None,
            having: None,
            sortagg: None,
            window: None,
            agg_filters: Vec::new(),
        }
    }
}

/// One stencil instance. `cols` lists every column the hot path touches;
/// `col_tys` carries their TypMeta resolved at lowering (index-aligned
/// with `cols`) — execution never re-derives type facts by name.
#[derive(Clone, Debug)]
pub struct PlanNode {
    pub family: Family,
    pub q: u32,
    pub cols: Vec<u32>,
    pub col_tys: Vec<TypMeta>,
    pub pred: Option<PredSpec>,
    pub agg: Vec<AggSpec>,
    pub params: Params,
}

impl PlanNode {
    /// TypMeta of a touched column by attno.
    pub fn ty_of(&self, attno: u32) -> TypMeta {
        self.cols
            .iter()
            .position(|&c| c == attno)
            .map(|i| self.col_tys[i])
            .unwrap_or_else(|| panic!("attno {attno} not in node cols"))
    }
    /// Is the column a DATE lane? (The typed replacement for the PoC's
    /// `is_date_col` name sniff.)
    pub fn is_date(&self, attno: u32) -> bool {
        self.ty_of(attno).oid == oids::DATE
    }
}
