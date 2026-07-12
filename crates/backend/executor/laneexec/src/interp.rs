// The lane qual interpreter (translate.rs's v1 backend): a self-contained
// harvest of the proven batchexec engine's qual subset — SelVec, Raw/Scalar
// lanes, the step vocabulary translate emits, the fail-closed clause
// classifier and the bitmap kernels. Deliberately NOT lanestitch: routing
// stitchable shapes to the stitcher (and growing its vocabulary with
// dict-code steps) is a later tranche; this module keeps the staged/prewhere
// drive independent of stitcher availability.
//
// Semantics contract: a qual is an implicitly ANDed clause list evaluated in
// clause order per row (production ExecQual); NULL clause result = row
// fails. The row-loop tier runs the whole program loop-inside, so clause
// order, short-circuit and error positions are per-row identical to the
// production evaluator by construction. The vectorized tier reorders
// evaluation ACROSS rows only for clauses proven non-erroring/non-volatile/
// allocation-free — the same legality argument as production's
// qual_bitmap_cmp_const.
use datum::{Datum, NullableDatum};
use types_error::PgResult;

pub const MAX_ROWS: usize = 1024;
pub const SEL_WORDS: usize = MAX_ROWS / 64;
pub const MAX_REGS: usize = 16;

/// Selection vector over one staged batch: bit i set = row i selected.
#[derive(Clone)]
pub struct SelVec {
    pub words: [u64; SEL_WORDS],
    pub nrows: u32,
}

impl SelVec {
    pub fn all(nrows: u32) -> SelVec {
        debug_assert!(nrows as usize <= MAX_ROWS);
        let mut words = [0u64; SEL_WORDS];
        let full = nrows as usize / 64;
        words[..full].fill(!0u64);
        let rem = nrows as usize % 64;
        if rem != 0 {
            words[full] = (1u64 << rem) - 1;
        }
        SelVec { words, nrows }
    }

    pub fn none(nrows: u32) -> SelVec {
        SelVec { words: [0; SEL_WORDS], nrows }
    }

    #[inline(always)]
    pub fn contains(&self, i: u32) -> bool {
        self.words[(i / 64) as usize] & (1u64 << (i % 64)) != 0
    }

    #[inline(always)]
    pub fn set(&mut self, i: u32) {
        self.words[(i / 64) as usize] |= 1u64 << (i % 64);
    }

    #[inline(always)]
    pub fn clear(&mut self, i: u32) {
        self.words[(i / 64) as usize] &= !(1u64 << (i % 64));
    }

    pub fn count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }

    #[inline]
    pub fn iter(&self) -> SelIter<'_> {
        SelIter { sel: self, word: 0, bits: self.words[0] }
    }
}

pub struct SelIter<'a> {
    sel: &'a SelVec,
    word: usize,
    bits: u64,
}

impl Iterator for SelIter<'_> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        loop {
            if self.bits != 0 {
                let bit = self.bits.trailing_zeros();
                self.bits &= self.bits - 1;
                return Some((self.word as u32) * 64 + bit);
            }
            if self.word + 1 >= SEL_WORDS {
                return None;
            }
            self.word += 1;
            self.bits = self.sel.words[self.word];
        }
    }
}

/// Per-column view of one staged batch. Raw is the SoA deform currency
/// (canonically sign-extended Datums + isnull bytes); Scalar covers const
/// columns / correlated params. Dict-coded columns never reach this
/// interpreter — the dict tier (dict.rs) owns them.
pub enum LaneRef<'a> {
    Raw { values: &'a [Datum], isnull: &'a [bool] },
    Scalar { value: Datum, isnull: bool },
}

impl LaneRef<'_> {
    #[inline(always)]
    pub fn read(&self, i: u32) -> NullableDatum {
        match self {
            LaneRef::Raw { values, isnull } => {
                NullableDatum { value: values[i as usize], isnull: isnull[i as usize] }
            }
            LaneRef::Scalar { value, isnull } => {
                NullableDatum { value: *value, isnull: *isnull }
            }
        }
    }
}

/// One staged batch: a view over the adapter's lane storage.
pub struct Batch<'a> {
    pub nrows: u32,
    pub lanes: Vec<LaneRef<'a>>,
}

#[derive(Clone, Copy, Debug)]
pub enum BStep {
    /// reg[out] = lane[col] row value.
    LoadLane { col: u16, out: u8 },
    LoadConst { k: u16, out: u8 },
    /// Strict comparison: NULL if either input NULL.
    Cmp { op: CmpOp, a: u8, b: u8, out: u8 },
    IsNull { a: u8, out: u8 },
    IsNotNull { a: u8, out: u8 },
    /// BooleanTest (non-strict, EEOP_BOOLTEST_* parity):
    /// out = isnull(a) ? null_result : (bool(a) ^ negate), never NULL.
    BoolTest { null_result: bool, negate: bool, a: u8, out: u8 },
    /// col <op> ANY(consts[k..k+n]) with OR semantics over a strict
    /// comparator (ExecEvalScalarArrayOp useOr parity): NULL input -> NULL;
    /// any element compare true -> true; else NULL if any element NULL,
    /// else false. Non-erroring because op is.
    InListAnyConst { col: u16, op: CmpOp, k: u16, n: u16, out: u8 },
    /// Clause boundary: reg[a] NULL or false fails the row (short-circuit:
    /// later clauses never evaluate for this row).
    Qual { a: u8 },
}

pub struct Program {
    pub steps: Vec<BStep>,
    pub consts: Vec<NullableDatum>,
}

impl Program {
    pub fn new() -> Program {
        Program { steps: Vec::new(), consts: Vec::new() }
    }

    pub fn push_const(&mut self, nd: NullableDatum) -> u16 {
        self.consts.push(nd);
        (self.consts.len() - 1) as u16
    }
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

/// General tier: the whole program per row, steps in order, first failing
/// Qual step exits the row.
#[inline(always)]
pub fn eval_row(prog: &Program, batch: &Batch<'_>, i: u32) -> PgResult<bool> {
    eval_row_steps(prog, &prog.steps, batch, i)
}

#[inline(always)]
fn eval_row_steps(prog: &Program, steps: &[BStep], batch: &Batch<'_>, i: u32) -> PgResult<bool> {
    let mut regs = [NullableDatum::null(); MAX_REGS];
    for step in steps {
        match *step {
            BStep::LoadLane { col, out } => {
                regs[out as usize] = batch.lanes[col as usize].read(i);
            }
            BStep::LoadConst { k, out } => {
                regs[out as usize] = prog.consts[k as usize];
            }
            BStep::Cmp { op, a, b, out } => {
                let (a, b) = (regs[a as usize], regs[b as usize]);
                regs[out as usize] = if a.isnull || b.isnull {
                    NullableDatum::null()
                } else {
                    NullableDatum {
                        value: Datum::from_bool(op.eval(a.value, b.value)),
                        isnull: false,
                    }
                };
            }
            BStep::IsNull { a, out } => {
                regs[out as usize] = NullableDatum {
                    value: Datum::from_bool(regs[a as usize].isnull),
                    isnull: false,
                };
            }
            BStep::IsNotNull { a, out } => {
                regs[out as usize] = NullableDatum {
                    value: Datum::from_bool(!regs[a as usize].isnull),
                    isnull: false,
                };
            }
            BStep::BoolTest { null_result, negate, a, out } => {
                let r = regs[a as usize];
                let v = if r.isnull { null_result } else { r.value.as_bool() ^ negate };
                regs[out as usize] = NullableDatum { value: Datum::from_bool(v), isnull: false };
            }
            BStep::InListAnyConst { col, op, k, n, out } => {
                let v = batch.lanes[col as usize].read(i);
                regs[out as usize] = if v.isnull {
                    NullableDatum::null()
                } else {
                    // Element order + first-match short-circuit per C's
                    // ExecEvalScalarArrayOp (unobservable for pure ops, kept
                    // anyway); NULL element -> NULL on miss, not false.
                    let mut saw_null = false;
                    let mut hit = false;
                    for c in &prog.consts[k as usize..(k + n) as usize] {
                        if c.isnull {
                            saw_null = true;
                        } else if op.eval(v.value, c.value) {
                            hit = true;
                            break;
                        }
                    }
                    if hit {
                        NullableDatum { value: Datum::from_bool(true), isnull: false }
                    } else if saw_null {
                        NullableDatum::null()
                    } else {
                        NullableDatum { value: Datum::from_bool(false), isnull: false }
                    }
                };
            }
            BStep::Qual { a } => {
                let r = regs[a as usize];
                if r.isnull || !r.value.as_bool() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// One clause of the compiled plan. RowLoop is the always-correct general
/// tier; the vectorized shapes are proven non-erroring so cross-row
/// reordering is unobservable.
enum ClausePlan {
    /// steps[lo..hi] (ending in Qual) loop-inside over current survivors.
    RowLoop { lo: usize, hi: usize },
    /// !isnull(lane) && cmp(lane, konst) as one bitmap pass.
    BitmapCmpConst { col: u16, op: CmpOp, konst: Datum },
    /// col IS [NOT] NULL as one pass over the isnull lane (never NULL-valued,
    /// never erroring — NullTest is not strict).
    NullBitmap { col: u16, want_null: bool },
    /// Bare boolean Var clause: !isnull(lane) && bool(lane) as one pass.
    BoolVarBitmap { col: u16 },
    /// col IS [NOT] TRUE/FALSE as one pass over both lanes (non-strict).
    BoolTestBitmap { col: u16, null_result: bool, negate: bool },
    /// col <op> ANY(consts): one bitmap pass per non-null element, OR-folded.
    /// NULL elements never set a bit (they can only turn a false result
    /// NULL, and both fail a Qual), so skipping them is qual-exact.
    InListBitmap { col: u16, op: CmpOp, k: u16, n: u16 },
}

pub struct QualPlan {
    clauses: Vec<ClausePlan>,
}

/// Fail-closed clause classifier: exhaustive over BStep with no wildcard —
/// a new step variant fails to compile until classified. Unrecognized
/// clauses stay on the RowLoop tier.
pub fn compile_qual(prog: &Program) -> QualPlan {
    let mut clauses = Vec::new();
    let mut lo = 0usize;
    for (ix, step) in prog.steps.iter().enumerate() {
        let BStep::Qual { .. } = step else { continue };
        let hi = ix + 1;
        let clause = &prog.steps[lo..hi];
        clauses.push(classify_clause(prog, clause, lo, hi));
        lo = hi;
    }
    debug_assert!(lo == prog.steps.len(), "qual program must end each clause with Qual");
    QualPlan { clauses }
}

fn classify_clause(prog: &Program, clause: &[BStep], lo: usize, hi: usize) -> ClausePlan {
    // Every step must be provably non-erroring for the vectorized shapes;
    // exhaustive match, no wildcard (fail-closed doctrine).
    for step in clause {
        match step {
            BStep::LoadLane { .. }
            | BStep::LoadConst { .. }
            | BStep::Cmp { .. }
            | BStep::IsNull { .. }
            | BStep::IsNotNull { .. }
            | BStep::BoolTest { .. }
            | BStep::InListAnyConst { .. }
            | BStep::Qual { .. } => {}
        }
    }
    match clause {
        [BStep::LoadLane { col, out: r0 }, BStep::LoadConst { k, out: r1 }, BStep::Cmp { op, a, b, out }, BStep::Qual { a: q }]
            if a == r0 && b == r1 && q == out =>
        {
            let konst = prog.consts[*k as usize];
            if konst.isnull {
                return ClausePlan::RowLoop { lo, hi };
            }
            ClausePlan::BitmapCmpConst { col: *col, op: *op, konst: konst.value }
        }
        [BStep::LoadLane { col, out: r0 }, BStep::IsNull { a, out }, BStep::Qual { a: q }]
            if a == r0 && q == out =>
        {
            ClausePlan::NullBitmap { col: *col, want_null: true }
        }
        [BStep::LoadLane { col, out: r0 }, BStep::IsNotNull { a, out }, BStep::Qual { a: q }]
            if a == r0 && q == out =>
        {
            ClausePlan::NullBitmap { col: *col, want_null: false }
        }
        [BStep::LoadLane { col, out: r0 }, BStep::Qual { a: q }] if q == r0 => {
            ClausePlan::BoolVarBitmap { col: *col }
        }
        [BStep::LoadLane { col, out: r0 }, BStep::BoolTest { null_result, negate, a, out }, BStep::Qual { a: q }]
            if a == r0 && q == out =>
        {
            ClausePlan::BoolTestBitmap { col: *col, null_result: *null_result, negate: *negate }
        }
        [BStep::InListAnyConst { col, op, k, n, out }, BStep::Qual { a: q }] if q == out => {
            ClausePlan::InListBitmap { col: *col, op: *op, k: *k, n: *n }
        }
        _ => ClausePlan::RowLoop { lo, hi },
    }
}

/// Evaluate the compiled qual over the staged batch: `sel` in/out. Clause
/// order is preserved; clause k sees only survivors of clauses 0..k, so
/// short-circuit semantics hold per row on every tier.
pub fn eval_qual(
    plan: &mut QualPlan,
    prog: &Program,
    batch: &Batch<'_>,
    sel: &mut SelVec,
) -> PgResult<()> {
    let mut scratch = [0u64; SEL_WORDS];
    for clause in &mut plan.clauses {
        if sel.count() == 0 {
            return Ok(());
        }
        match clause {
            ClausePlan::BitmapCmpConst { col, op, konst } => {
                match &batch.lanes[*col as usize] {
                    LaneRef::Raw { values, isnull } => {
                        bitmap_cmp_const(*op, *konst, values, isnull, &mut scratch);
                        for (w, s) in sel.words.iter_mut().zip(scratch.iter()) {
                            *w &= *s;
                        }
                    }
                    LaneRef::Scalar { value, isnull } => {
                        if *isnull || !op.eval(*value, *konst) {
                            sel.words = [0; SEL_WORDS];
                        }
                    }
                }
            }
            ClausePlan::BoolVarBitmap { col } => match &batch.lanes[*col as usize] {
                LaneRef::Raw { values, isnull } => {
                    bitmap_bool_var(values, isnull, &mut scratch);
                    for (w, s) in sel.words.iter_mut().zip(scratch.iter()) {
                        *w &= *s;
                    }
                }
                LaneRef::Scalar { value, isnull } => {
                    if *isnull || !value.as_bool() {
                        sel.words = [0; SEL_WORDS];
                    }
                }
            },
            ClausePlan::BoolTestBitmap { col, null_result, negate } => {
                match &batch.lanes[*col as usize] {
                    LaneRef::Raw { values, isnull } => {
                        bitmap_bool_test(values, isnull, *null_result, *negate, &mut scratch);
                        for (w, s) in sel.words.iter_mut().zip(scratch.iter()) {
                            *w &= *s;
                        }
                    }
                    LaneRef::Scalar { value, isnull } => {
                        let bit = if *isnull { *null_result } else { value.as_bool() ^ *negate };
                        if !bit {
                            sel.words = [0; SEL_WORDS];
                        }
                    }
                }
            }
            ClausePlan::InListBitmap { col, op, k, n } => {
                let consts = &prog.consts[*k as usize..(*k + *n) as usize];
                match &batch.lanes[*col as usize] {
                    LaneRef::Raw { values, isnull } => {
                        // OR of per-element bitmap passes; each pass already
                        // masks !isnull, so the fold is !isnull && any-match.
                        let mut acc = [0u64; SEL_WORDS];
                        for c in consts.iter().filter(|c| !c.isnull) {
                            bitmap_cmp_const(*op, c.value, values, isnull, &mut scratch);
                            for (a, s) in acc.iter_mut().zip(scratch.iter()) {
                                *a |= *s;
                            }
                        }
                        for (w, a) in sel.words.iter_mut().zip(acc.iter()) {
                            *w &= *a;
                        }
                    }
                    LaneRef::Scalar { value, isnull } => {
                        let hit = !*isnull
                            && consts.iter().any(|c| !c.isnull && op.eval(*value, c.value));
                        if !hit {
                            sel.words = [0; SEL_WORDS];
                        }
                    }
                }
            }
            ClausePlan::NullBitmap { col, want_null } => match &batch.lanes[*col as usize] {
                LaneRef::Raw { isnull, .. } => {
                    bitmap_null_test(isnull, *want_null, &mut scratch);
                    for (w, s) in sel.words.iter_mut().zip(scratch.iter()) {
                        *w &= *s;
                    }
                }
                LaneRef::Scalar { isnull, .. } => {
                    if *isnull != *want_null {
                        sel.words = [0; SEL_WORDS];
                    }
                }
            },
            ClausePlan::RowLoop { lo, hi } => {
                let steps = &prog.steps[*lo..*hi];
                let snapshot = sel.clone();
                for i in snapshot.iter() {
                    if !eval_row_steps(prog, steps, batch, i)? {
                        sel.clear(i);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Uncompiled reference tier: the whole program loop-inside per row. The
/// vectorized plan is parity-checked against this in tests.
pub fn eval_qual_rowloop(prog: &Program, batch: &Batch<'_>, sel: &mut SelVec) -> PgResult<()> {
    debug_assert!(batch.nrows as usize <= MAX_ROWS);
    let snapshot = sel.clone();
    for i in snapshot.iter() {
        if !eval_row(prog, batch, i)? {
            sel.clear(i);
        }
    }
    Ok(())
}

// Comparator vocabulary: the full int2/int4/int8 + cross-width families
// (int.c parity: widen the int2 side), unsigned oid, and the float families
// with C float.h NaN ordering. Copied from the proven batchexec cmp module;
// monomorphized comparator bodies + the bitmap kernel shape mirror the
// production execexpr steps.rs kernels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CmpOp {
    Int4Eq,
    Int4Ne,
    Int4Lt,
    Int4Le,
    Int4Gt,
    Int4Ge,
    Int8Eq,
    Int8Ne,
    Int8Lt,
    Int8Le,
    Int8Gt,
    Int8Ge,
    Int2Eq,
    Int2Ne,
    Int2Lt,
    Int2Le,
    Int2Gt,
    Int2Ge,
    Int84Eq,
    Int84Ne,
    Int84Lt,
    Int84Le,
    Int84Gt,
    Int84Ge,
    Int48Eq,
    Int48Ne,
    Int48Lt,
    Int48Le,
    Int48Gt,
    Int48Ge,
    Int24Eq,
    Int24Ne,
    Int24Lt,
    Int24Le,
    Int24Gt,
    Int24Ge,
    Int42Eq,
    Int42Ne,
    Int42Lt,
    Int42Le,
    Int42Gt,
    Int42Ge,
    // Oid is unsigned; the interpreter truncates to u32 (extension-blind).
    // Translation still canonicalizes oid konsts to sign-extension (the
    // lane deform contract) so a later SIMD tier stays exact.
    OidEq,
    OidNe,
    OidLt,
    OidLe,
    OidGt,
    OidGe,
    // Float families with C float.h NaN ordering (NaN = NaN, NaN > any
    // non-NaN); comparisons never error, so they vectorize. f32 operands
    // promote to f64 (exact and order-preserving), so one predicate set
    // covers all four width families — btfloat48cmp promotes the same way.
    Float4Eq,
    Float4Ne,
    Float4Lt,
    Float4Le,
    Float4Gt,
    Float4Ge,
    Float8Eq,
    Float8Ne,
    Float8Lt,
    Float8Le,
    Float8Gt,
    Float8Ge,
    Float48Eq,
    Float48Ne,
    Float48Lt,
    Float48Le,
    Float48Gt,
    Float48Ge,
    Float84Eq,
    Float84Ne,
    Float84Lt,
    Float84Le,
    Float84Gt,
    Float84Ge,
}

// C float.h float8_eq/lt/le/gt/ge parity (utils/float.h): NaN compares
// equal to NaN and greater than every non-NaN value.
#[inline(always)]
pub fn pgf_eq(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

#[inline(always)]
pub fn pgf_lt(a: f64, b: f64) -> bool {
    !a.is_nan() && (b.is_nan() || a < b)
}

#[inline(always)]
pub fn pgf_le(a: f64, b: f64) -> bool {
    b.is_nan() || (!a.is_nan() && a <= b)
}

#[inline(always)]
pub fn pgf_gt(a: f64, b: f64) -> bool {
    !b.is_nan() && (a.is_nan() || a > b)
}

#[inline(always)]
pub fn pgf_ge(a: f64, b: f64) -> bool {
    a.is_nan() || (!b.is_nan() && a >= b)
}

#[inline(always)]
fn f4(d: Datum) -> f64 {
    d.as_f32() as f64
}

impl CmpOp {
    #[inline(always)]
    pub fn eval(self, a: Datum, b: Datum) -> bool {
        match self {
            CmpOp::Int4Eq => a.as_i32() == b.as_i32(),
            CmpOp::Int4Ne => a.as_i32() != b.as_i32(),
            CmpOp::Int4Lt => a.as_i32() < b.as_i32(),
            CmpOp::Int4Le => a.as_i32() <= b.as_i32(),
            CmpOp::Int4Gt => a.as_i32() > b.as_i32(),
            CmpOp::Int4Ge => a.as_i32() >= b.as_i32(),
            CmpOp::Int8Eq => a.as_i64() == b.as_i64(),
            CmpOp::Int8Ne => a.as_i64() != b.as_i64(),
            CmpOp::Int8Lt => a.as_i64() < b.as_i64(),
            CmpOp::Int8Le => a.as_i64() <= b.as_i64(),
            CmpOp::Int8Gt => a.as_i64() > b.as_i64(),
            CmpOp::Int8Ge => a.as_i64() >= b.as_i64(),
            CmpOp::Int2Eq => a.as_i16() == b.as_i16(),
            CmpOp::Int2Ne => a.as_i16() != b.as_i16(),
            CmpOp::Int2Lt => a.as_i16() < b.as_i16(),
            CmpOp::Int2Le => a.as_i16() <= b.as_i16(),
            CmpOp::Int2Gt => a.as_i16() > b.as_i16(),
            CmpOp::Int2Ge => a.as_i16() >= b.as_i16(),
            CmpOp::Int84Eq => a.as_i64() == b.as_i32() as i64,
            CmpOp::Int84Ne => a.as_i64() != b.as_i32() as i64,
            CmpOp::Int84Lt => a.as_i64() < b.as_i32() as i64,
            CmpOp::Int84Le => a.as_i64() <= b.as_i32() as i64,
            CmpOp::Int84Gt => a.as_i64() > b.as_i32() as i64,
            CmpOp::Int84Ge => a.as_i64() >= b.as_i32() as i64,
            CmpOp::Int48Eq => (a.as_i32() as i64) == b.as_i64(),
            CmpOp::Int48Ne => (a.as_i32() as i64) != b.as_i64(),
            CmpOp::Int48Lt => (a.as_i32() as i64) < b.as_i64(),
            CmpOp::Int48Le => (a.as_i32() as i64) <= b.as_i64(),
            CmpOp::Int48Gt => (a.as_i32() as i64) > b.as_i64(),
            CmpOp::Int48Ge => (a.as_i32() as i64) >= b.as_i64(),
            CmpOp::Int24Eq => (a.as_i16() as i32) == b.as_i32(),
            CmpOp::Int24Ne => (a.as_i16() as i32) != b.as_i32(),
            CmpOp::Int24Lt => (a.as_i16() as i32) < b.as_i32(),
            CmpOp::Int24Le => (a.as_i16() as i32) <= b.as_i32(),
            CmpOp::Int24Gt => (a.as_i16() as i32) > b.as_i32(),
            CmpOp::Int24Ge => (a.as_i16() as i32) >= b.as_i32(),
            CmpOp::Int42Eq => a.as_i32() == b.as_i16() as i32,
            CmpOp::Int42Ne => a.as_i32() != b.as_i16() as i32,
            CmpOp::Int42Lt => a.as_i32() < b.as_i16() as i32,
            CmpOp::Int42Le => a.as_i32() <= b.as_i16() as i32,
            CmpOp::Int42Gt => a.as_i32() > b.as_i16() as i32,
            CmpOp::Int42Ge => a.as_i32() >= b.as_i16() as i32,
            CmpOp::OidEq => a.as_u32() == b.as_u32(),
            CmpOp::OidNe => a.as_u32() != b.as_u32(),
            CmpOp::OidLt => a.as_u32() < b.as_u32(),
            CmpOp::OidLe => a.as_u32() <= b.as_u32(),
            CmpOp::OidGt => a.as_u32() > b.as_u32(),
            CmpOp::OidGe => a.as_u32() >= b.as_u32(),
            CmpOp::Float4Eq => pgf_eq(f4(a), f4(b)),
            CmpOp::Float4Ne => !pgf_eq(f4(a), f4(b)),
            CmpOp::Float4Lt => pgf_lt(f4(a), f4(b)),
            CmpOp::Float4Le => pgf_le(f4(a), f4(b)),
            CmpOp::Float4Gt => pgf_gt(f4(a), f4(b)),
            CmpOp::Float4Ge => pgf_ge(f4(a), f4(b)),
            CmpOp::Float8Eq => pgf_eq(a.as_f64(), b.as_f64()),
            CmpOp::Float8Ne => !pgf_eq(a.as_f64(), b.as_f64()),
            CmpOp::Float8Lt => pgf_lt(a.as_f64(), b.as_f64()),
            CmpOp::Float8Le => pgf_le(a.as_f64(), b.as_f64()),
            CmpOp::Float8Gt => pgf_gt(a.as_f64(), b.as_f64()),
            CmpOp::Float8Ge => pgf_ge(a.as_f64(), b.as_f64()),
            CmpOp::Float48Eq => pgf_eq(f4(a), b.as_f64()),
            CmpOp::Float48Ne => !pgf_eq(f4(a), b.as_f64()),
            CmpOp::Float48Lt => pgf_lt(f4(a), b.as_f64()),
            CmpOp::Float48Le => pgf_le(f4(a), b.as_f64()),
            CmpOp::Float48Gt => pgf_gt(f4(a), b.as_f64()),
            CmpOp::Float48Ge => pgf_ge(f4(a), b.as_f64()),
            CmpOp::Float84Eq => pgf_eq(a.as_f64(), f4(b)),
            CmpOp::Float84Ne => !pgf_eq(a.as_f64(), f4(b)),
            CmpOp::Float84Lt => pgf_lt(a.as_f64(), f4(b)),
            CmpOp::Float84Le => pgf_le(a.as_f64(), f4(b)),
            CmpOp::Float84Gt => pgf_gt(a.as_f64(), f4(b)),
            CmpOp::Float84Ge => pgf_ge(a.as_f64(), f4(b)),
        }
    }
}

/// Batched cmp-against-const over a lane: sel bit = !isnull && cmp(v, k).
/// Legal because comparisons are non-erroring and allocation-free, so
/// evaluation order across rows is unobservable.
pub fn bitmap_cmp_const(
    cmp: CmpOp,
    konst: Datum,
    values: &[Datum],
    isnull: &[bool],
    sel: &mut [u64],
) {
    macro_rules! lanes {
        ($pred:expr) => {
            bitmap_loop(values, isnull, sel, $pred)
        };
    }
    match cmp {
        CmpOp::Int4Eq => lanes!(|v: Datum| v.as_i32() == konst.as_i32()),
        CmpOp::Int4Ne => lanes!(|v: Datum| v.as_i32() != konst.as_i32()),
        CmpOp::Int4Lt => lanes!(|v: Datum| v.as_i32() < konst.as_i32()),
        CmpOp::Int4Le => lanes!(|v: Datum| v.as_i32() <= konst.as_i32()),
        CmpOp::Int4Gt => lanes!(|v: Datum| v.as_i32() > konst.as_i32()),
        CmpOp::Int4Ge => lanes!(|v: Datum| v.as_i32() >= konst.as_i32()),
        CmpOp::Int8Eq => lanes!(|v: Datum| v.as_i64() == konst.as_i64()),
        CmpOp::Int8Ne => lanes!(|v: Datum| v.as_i64() != konst.as_i64()),
        CmpOp::Int8Lt => lanes!(|v: Datum| v.as_i64() < konst.as_i64()),
        CmpOp::Int8Le => lanes!(|v: Datum| v.as_i64() <= konst.as_i64()),
        CmpOp::Int8Gt => lanes!(|v: Datum| v.as_i64() > konst.as_i64()),
        CmpOp::Int8Ge => lanes!(|v: Datum| v.as_i64() >= konst.as_i64()),
        CmpOp::Int2Eq => lanes!(|v: Datum| v.as_i16() == konst.as_i16()),
        CmpOp::Int2Ne => lanes!(|v: Datum| v.as_i16() != konst.as_i16()),
        CmpOp::Int2Lt => lanes!(|v: Datum| v.as_i16() < konst.as_i16()),
        CmpOp::Int2Le => lanes!(|v: Datum| v.as_i16() <= konst.as_i16()),
        CmpOp::Int2Gt => lanes!(|v: Datum| v.as_i16() > konst.as_i16()),
        CmpOp::Int2Ge => lanes!(|v: Datum| v.as_i16() >= konst.as_i16()),
        CmpOp::Int84Eq => lanes!(|v: Datum| v.as_i64() == konst.as_i32() as i64),
        CmpOp::Int84Ne => lanes!(|v: Datum| v.as_i64() != konst.as_i32() as i64),
        CmpOp::Int84Lt => lanes!(|v: Datum| v.as_i64() < konst.as_i32() as i64),
        CmpOp::Int84Le => lanes!(|v: Datum| v.as_i64() <= konst.as_i32() as i64),
        CmpOp::Int84Gt => lanes!(|v: Datum| v.as_i64() > konst.as_i32() as i64),
        CmpOp::Int84Ge => lanes!(|v: Datum| v.as_i64() >= konst.as_i32() as i64),
        CmpOp::Int48Eq => lanes!(|v: Datum| (v.as_i32() as i64) == konst.as_i64()),
        CmpOp::Int48Ne => lanes!(|v: Datum| (v.as_i32() as i64) != konst.as_i64()),
        CmpOp::Int48Lt => lanes!(|v: Datum| (v.as_i32() as i64) < konst.as_i64()),
        CmpOp::Int48Le => lanes!(|v: Datum| (v.as_i32() as i64) <= konst.as_i64()),
        CmpOp::Int48Gt => lanes!(|v: Datum| (v.as_i32() as i64) > konst.as_i64()),
        CmpOp::Int48Ge => lanes!(|v: Datum| (v.as_i32() as i64) >= konst.as_i64()),
        CmpOp::Int24Eq => lanes!(|v: Datum| (v.as_i16() as i32) == konst.as_i32()),
        CmpOp::Int24Ne => lanes!(|v: Datum| (v.as_i16() as i32) != konst.as_i32()),
        CmpOp::Int24Lt => lanes!(|v: Datum| (v.as_i16() as i32) < konst.as_i32()),
        CmpOp::Int24Le => lanes!(|v: Datum| (v.as_i16() as i32) <= konst.as_i32()),
        CmpOp::Int24Gt => lanes!(|v: Datum| (v.as_i16() as i32) > konst.as_i32()),
        CmpOp::Int24Ge => lanes!(|v: Datum| (v.as_i16() as i32) >= konst.as_i32()),
        CmpOp::Int42Eq => lanes!(|v: Datum| v.as_i32() == konst.as_i16() as i32),
        CmpOp::Int42Ne => lanes!(|v: Datum| v.as_i32() != konst.as_i16() as i32),
        CmpOp::Int42Lt => lanes!(|v: Datum| v.as_i32() < konst.as_i16() as i32),
        CmpOp::Int42Le => lanes!(|v: Datum| v.as_i32() <= konst.as_i16() as i32),
        CmpOp::Int42Gt => lanes!(|v: Datum| v.as_i32() > konst.as_i16() as i32),
        CmpOp::Int42Ge => lanes!(|v: Datum| v.as_i32() >= konst.as_i16() as i32),
        CmpOp::OidEq => lanes!(|v: Datum| v.as_u32() == konst.as_u32()),
        CmpOp::OidNe => lanes!(|v: Datum| v.as_u32() != konst.as_u32()),
        CmpOp::OidLt => lanes!(|v: Datum| v.as_u32() < konst.as_u32()),
        CmpOp::OidLe => lanes!(|v: Datum| v.as_u32() <= konst.as_u32()),
        CmpOp::OidGt => lanes!(|v: Datum| v.as_u32() > konst.as_u32()),
        CmpOp::OidGe => lanes!(|v: Datum| v.as_u32() >= konst.as_u32()),
        CmpOp::Float4Eq => lanes!(|v: Datum| pgf_eq(f4(v), f4(konst))),
        CmpOp::Float4Ne => lanes!(|v: Datum| !pgf_eq(f4(v), f4(konst))),
        CmpOp::Float4Lt => lanes!(|v: Datum| pgf_lt(f4(v), f4(konst))),
        CmpOp::Float4Le => lanes!(|v: Datum| pgf_le(f4(v), f4(konst))),
        CmpOp::Float4Gt => lanes!(|v: Datum| pgf_gt(f4(v), f4(konst))),
        CmpOp::Float4Ge => lanes!(|v: Datum| pgf_ge(f4(v), f4(konst))),
        CmpOp::Float8Eq => lanes!(|v: Datum| pgf_eq(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Ne => lanes!(|v: Datum| !pgf_eq(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Lt => lanes!(|v: Datum| pgf_lt(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Le => lanes!(|v: Datum| pgf_le(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Gt => lanes!(|v: Datum| pgf_gt(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Ge => lanes!(|v: Datum| pgf_ge(v.as_f64(), konst.as_f64())),
        CmpOp::Float48Eq => lanes!(|v: Datum| pgf_eq(f4(v), konst.as_f64())),
        CmpOp::Float48Ne => lanes!(|v: Datum| !pgf_eq(f4(v), konst.as_f64())),
        CmpOp::Float48Lt => lanes!(|v: Datum| pgf_lt(f4(v), konst.as_f64())),
        CmpOp::Float48Le => lanes!(|v: Datum| pgf_le(f4(v), konst.as_f64())),
        CmpOp::Float48Gt => lanes!(|v: Datum| pgf_gt(f4(v), konst.as_f64())),
        CmpOp::Float48Ge => lanes!(|v: Datum| pgf_ge(f4(v), konst.as_f64())),
        CmpOp::Float84Eq => lanes!(|v: Datum| pgf_eq(v.as_f64(), f4(konst))),
        CmpOp::Float84Ne => lanes!(|v: Datum| !pgf_eq(v.as_f64(), f4(konst))),
        CmpOp::Float84Lt => lanes!(|v: Datum| pgf_lt(v.as_f64(), f4(konst))),
        CmpOp::Float84Le => lanes!(|v: Datum| pgf_le(v.as_f64(), f4(konst))),
        CmpOp::Float84Gt => lanes!(|v: Datum| pgf_gt(v.as_f64(), f4(konst))),
        CmpOp::Float84Ge => lanes!(|v: Datum| pgf_ge(v.as_f64(), f4(konst))),
    }
}

/// Batched bare-bool-var clause: sel bit = !isnull && value (production Qual
/// semantics on a boolean Var; DatumGetBool = word != 0).
pub fn bitmap_bool_var(values: &[Datum], isnull: &[bool], sel: &mut [u64]) {
    bitmap_loop(values, isnull, sel, |v: Datum| v.as_bool());
}

/// Batched BooleanTest: non-strict (EEOP_BOOLTEST_*): bit =
/// isnull ? null_result : (value ^ negate). IS TRUE = (false, false),
/// IS NOT TRUE = (true, true), IS FALSE = (false, true),
/// IS NOT FALSE = (true, false). Pure function of both lanes.
pub fn bitmap_bool_test(
    values: &[Datum],
    isnull: &[bool],
    null_result: bool,
    negate: bool,
    sel: &mut [u64],
) {
    for (w, (vch, nch)) in values.chunks(64).zip(isnull.chunks(64)).enumerate() {
        let mut word = 0u64;
        if vch.len() == 64 {
            let mut b = [0u8; 64];
            for i in 0..64 {
                // Branchless: (n & null_result) | (!n & (v ^ negate)).
                b[i] = ((nch[i] & null_result) | (!nch[i] & (vch[i].as_bool() ^ negate))) as u8;
            }
            word = movemask64(&b);
        } else {
            for i in 0..vch.len() {
                let bit = if nch[i] { null_result } else { vch[i].as_bool() ^ negate };
                word |= (bit as u64) << i;
            }
        }
        sel[w] = word;
    }
}

/// Batched null test over a lane: sel bit = (isnull == want_null). Pure
/// function of the isnull lane, so cross-row evaluation is unobservable;
/// AND-composed by the caller (non-survivor bits stay clear).
pub fn bitmap_null_test(isnull: &[bool], want_null: bool, sel: &mut [u64]) {
    for (w, nch) in isnull.chunks(64).enumerate() {
        let mut word = 0u64;
        if nch.len() == 64 {
            let mut b = [0u8; 64];
            for i in 0..64 {
                b[i] = (nch[i] == want_null) as u8;
            }
            word = movemask64(&b);
        } else {
            for i in 0..nch.len() {
                word |= ((nch[i] == want_null) as u64) << i;
            }
        }
        sel[w] = word;
    }
}

/// Multiply-movemask over 64 bytes that are exactly 0/1: bit i of the
/// result = b[i]. Exact because bit 56+i of m * 0x0102_0408_1020_4080 is
/// byte i's LSB and the eight contributing powers per output bit are
/// pairwise distinct (no carries). Mirrors production steps.rs.
#[inline(always)]
fn movemask64(b: &[u8; 64]) -> u64 {
    let mut word = 0u64;
    for g in 0..8 {
        let m = u64::from_le_bytes(b[g * 8..g * 8 + 8].try_into().unwrap());
        word |= (m.wrapping_mul(0x0102_0408_1020_4080) >> 56) << (g * 8);
    }
    word
}

#[inline(always)]
fn bitmap_loop(values: &[Datum], isnull: &[bool], sel: &mut [u64], pred: impl Fn(Datum) -> bool) {
    // Branchless `&` + byte-mask/movemask body: mirrors production's
    // bitmap_loop (execexpr steps.rs) — pred is non-erroring by
    // classification so unconditional evaluation on NULL rows' stale
    // Datums is safe; the byte-mask form keeps the fold in vector
    // registers (the `word |= bit << i` fold vectorized the compares but
    // lowered the fold to 64 scalar lane extracts).
    for (w, (vch, nch)) in values.chunks(64).zip(isnull.chunks(64)).enumerate() {
        let mut word = 0u64;
        if vch.len() == 64 {
            let mut b = [0u8; 64];
            for i in 0..64 {
                b[i] = (!nch[i] & pred(vch[i])) as u8;
            }
            word = movemask64(&b);
        } else {
            for i in 0..vch.len() {
                word |= ((!nch[i] & pred(vch[i])) as u64) << i;
            }
        }
        sel[w] = word;
    }
}
