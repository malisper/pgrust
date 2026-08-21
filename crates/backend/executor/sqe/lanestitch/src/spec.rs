// Step-IR Level-2 currency (step-ir.md §2): programs, batches, selection.
// Semantics contract: a qual program is an implicitly ANDed clause list,
// each clause ending in a Qual step, evaluated in clause order per row;
// NULL clause result = row fails. The oracle-feature interpreter is the
// executable specification: every stitched body is contractually
// equivalent to it — same pass bits, same refusal point.
//
// CANONICAL-DATUM CONTRACT (step-ir.md §4.1, load-bearing for the
// stitched compares): lane values and program consts hold canonically
// SIGN-extended Datum images for the integer families — int2/int4 to the
// full word, oid sign-extended from its u32 image. This makes
// truncate-then-widen cross-width semantics equal to one signed compare
// at any covering width, and makes the 2x64 unsigned NEON compares exact
// for oid. Float lanes carry the raw f32 pattern in the low word / the
// f64 pattern in the full word; upper f32 garbage is harmless.

use datum::{Datum, NullableDatum};

pub const MAX_ROWS: usize = 1024;
pub const SEL_WORDS: usize = MAX_ROWS / 64;
pub const MAX_COLS: usize = 8;
pub const MAX_REGS: usize = 16;
pub const MAX_OUTS: usize = 8;

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

    #[inline(always)]
    pub fn contains(&self, i: u32) -> bool {
        self.words[(i / 64) as usize] & (1u64 << (i % 64)) != 0
    }

    #[inline(always)]
    pub fn clear(&mut self, i: u32) {
        self.words[(i / 64) as usize] &= !(1u64 << (i % 64));
    }

    pub fn count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }

    pub fn is_all(&self) -> bool {
        let full = SelVec::all(self.nrows);
        self.words == full.words
    }
}

/// One fixed-width SoA column: canonically extended Datum values plus
/// per-row isnull bytes.
#[derive(Clone, Copy)]
pub struct Lane<'a> {
    pub values: &'a [Datum],
    pub isnull: &'a [bool],
}

/// One staged batch: a view over the adapter's lane storage.
pub struct Batch<'a> {
    pub nrows: u32,
    pub lanes: Vec<Lane<'a>>,
}

/// One mutable output lane of a projection program (`Step::StoreOut`
/// target). Same canonical-datum currency as [`Lane`].
pub struct OutLane<'a> {
    pub values: &'a mut [Datum],
    pub isnull: &'a mut [bool],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithOp {
    Add4,
    Sub4,
    Mul4,
    Div4,
    Add2,
    Sub2,
    Mul2,
    Div2,
    Add8,
    Sub8,
    Mul8,
    Div8,
}

impl ArithOp {
    /// Compute width in bytes (2/4/8): selects the overflow probe and the
    /// register read/write width.
    pub(crate) fn width(self) -> u8 {
        use ArithOp::*;
        match self {
            Add2 | Sub2 | Mul2 | Div2 => 2,
            Add4 | Sub4 | Mul4 | Div4 => 4,
            Add8 | Sub8 | Mul8 | Div8 => 8,
        }
    }
}

/// IS NULL / IS NOT NULL: never-erroring, never-NULL predicate over one
/// register's null flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullTestKind {
    IsNull,
    IsNotNull,
}

/// IS [NOT] TRUE / IS [NOT] FALSE: three-valued collapse — NULL input
/// reads as the "not" arm, result is never NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoolTestKind {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
}

/// The Step vocabulary (step-ir.md §2.2): fixed-width lane loads, the
/// whitelisted comparison families, int arithmetic (the one erroring
/// regime — refuse-and-replay), and the clause-boundary Qual.
#[derive(Clone, Copy, Debug)]
pub enum Step {
    /// reg[out] = lane[col] row value (value + isnull, verbatim).
    LoadLane { col: u16, out: u8 },
    LoadConst { k: u16, out: u8 },
    /// Strict comparison: NULL if either input NULL.
    Cmp { op: CmpOp, a: u8, b: u8, out: u8 },
    /// int2/int4/int8 arithmetic. Erroring step: overflow / zero divisor
    /// takes the body's refuse exit (no error constructed in stitched
    /// code); the driver replays the batch on the error-owning path.
    Arith { op: ArithOp, a: u8, b: u8, out: u8 },
    NullTest { a: u8, out: u8, kind: NullTestKind },
    BoolTest { a: u8, out: u8, kind: BoolTestKind },
    /// reg[out] = reg[a] <op> ANY (const array `arr`), strict-OR
    /// three-valued: NULL scalar, or no match with a NULL element, is
    /// NULL; else the OR of the element matches. Non-erroring.
    SaopAny { a: u8, out: u8, op: CmpOp, arr: u16 },
    /// Clause boundary: reg[a] NULL-or-false fails the row; later clauses
    /// never evaluate for it.
    Qual { a: u8 },
    /// Projection output: out_lane[out][row] = reg[a]. Projection
    /// programs only — the program kinds never mix (step-ir.md §2.3):
    /// a qual program carrying StoreOut, or a projection program carrying
    /// Qual, refuses fail-closed at classification.
    StoreOut { a: u8, out: u16 },
}

pub struct Program {
    pub steps: Vec<Step>,
    pub consts: Vec<NullableDatum>,
    /// Baked const arrays for SaopAny (fixed-width by-value elements;
    /// individual elements may be NULL, the array datum itself never is —
    /// a NULL array keeps the program off the stitcher).
    pub arrays: Vec<Vec<NullableDatum>>,
    /// Programs that must never reorder or stitch (volatile expressions
    /// upstream). Classification refuses them fail-closed; the refusal
    /// surfaces as a TYPED lowering refusal at the caller (production-plan
    /// §3, step-ir.md §9 Q8) — never a slow-path route.
    pub volatile: bool,
}

impl Program {
    pub fn new() -> Program {
        Program { steps: Vec::new(), consts: Vec::new(), arrays: Vec::new(), volatile: false }
    }

    pub fn push_const(&mut self, nd: NullableDatum) -> u16 {
        self.consts.push(nd);
        (self.consts.len() - 1) as u16
    }

    pub fn push_array(&mut self, elems: Vec<NullableDatum>) -> u16 {
        self.arrays.push(elems);
        (self.arrays.len() - 1) as u16
    }
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

// The whitelisted comparator families. Date/timestamp are carrier types
// (step-ir.md §4.1): date rides Int4*, timestamp/tstz ride Int8*, with
// ±infinity as INT_MIN/MAX sentinels that sort as plain signed ints — no
// date-specific comparators exist.
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
    // Oid is unsigned; the scalar tiers truncate to u32 (extension-blind).
    // The 2x64 SIMD tier additionally requires the canonical
    // sign-extension contract (module header).
    OidEq,
    OidNe,
    OidLt,
    OidLe,
    OidGt,
    OidGe,
    // Float families with the production NaN ordering (NaN = NaN, NaN >
    // any non-NaN); never error. f32 promotes to f64 exactly, so one
    // predicate set covers all four width families.
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

/// The ONE float-family predicate every float-special stitch gate keys on
/// (step-ir.md §4.1) so the fences cannot drift.
pub(crate) fn is_float_cmp(op: CmpOp) -> bool {
    use CmpOp::*;
    matches!(
        op,
        Float4Eq | Float4Ne | Float4Lt | Float4Le | Float4Gt | Float4Ge
            | Float8Eq | Float8Ne | Float8Lt | Float8Le | Float8Gt | Float8Ge
            | Float48Eq | Float48Ne | Float48Lt | Float48Le | Float48Gt | Float48Ge
            | Float84Eq | Float84Ne | Float84Lt | Float84Le | Float84Gt | Float84Ge
    )
}
