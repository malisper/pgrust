// The stitcher's program/batch currency. Semantics contract: a qual program
// is an implicitly ANDed clause list, each clause ending in a Qual step,
// evaluated in clause order per row; NULL clause result = row fails
// (production ExecQual). The interpreter in interp.rs evaluates the whole
// program loop-inside (row loop around the step walk) and IS the semantic
// specification: every stitched body is contractually equivalent to it —
// same pass bits, same error, same erroring row.
//
// CANONICAL-DATUM CONTRACT (load-bearing for the stitched compares): Raw
// lane values and program consts must hold canonically SIGN-extended Datum
// images for the integer families (the deform / Datum::from_iN contract) —
// int2/int4 values sign-extended to the full word, oid values sign-extended
// from their u32 image (laneexec canonicalizes oid konsts at translation).
// Canonical sign-extension makes the interpreter's truncate-then-widen
// cross-width semantics equal to one signed compare at any covering width,
// and makes the 2x64 unsigned NEON compares exact for oid (sign-extension
// is injective and order-preserving under unsigned 64-bit compare). Float
// lanes carry the raw f32 bit pattern in the low word (from_f32) / the f64
// pattern in the full word (from_f64); upper garbage bits on f32 lanes are
// harmless (every consumer reads the low word).

use datum::{Datum, NullableDatum};

pub const MAX_ROWS: usize = 1024;
pub const SEL_WORDS: usize = MAX_ROWS / 64;
pub const MAX_COLS: usize = 8;
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

/// One fixed-width SoA column: canonically extended Datum values plus the
/// per-row isnull bytes (the heap-SoA / columnar decode currency).
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithOp {
    // int4 (integer): "integer out of range".
    Add4,
    Sub4,
    Mul4,
    Div4,
    // int2 (smallint): "smallint out of range". C int2pl/mi/mul/div parity.
    Add2,
    Sub2,
    Mul2,
    Div2,
    // int8 (bigint): "bigint out of range". C int8pl/mi/mul/div parity.
    Add8,
    Sub8,
    Mul8,
    Div8,
}

impl ArithOp {
    /// The integer width this op computes at (2, 4, or 8 bytes) — the
    /// interpreter reads/writes the register at that width and the stitched
    /// stencil selects its overflow probe from it.
    pub(crate) fn width(self) -> u8 {
        use ArithOp::*;
        match self {
            Add2 | Sub2 | Mul2 | Div2 => 2,
            Add4 | Sub4 | Mul4 | Div4 => 4,
            Add8 | Sub8 | Mul8 | Div8 => 8,
        }
    }
}

/// IS NULL / IS NOT NULL — never-erroring, never-NULL predicate over one
/// register's null flag (production NullTest).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullTestKind {
    IsNull,
    IsNotNull,
}

/// IS [NOT] TRUE / IS [NOT] FALSE — production BooleanTest three-valued
/// collapse: NULL input reads as the "not" arm, result is never NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoolTestKind {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
}

/// The initial stitch vocabulary: fixed-width lane loads, the whitelisted
/// comparison families, simple int arithmetic (erroring — refuse-and-replay
/// discipline in the stitched tier), and the clause-boundary Qual.
#[derive(Clone, Copy, Debug)]
pub enum Step {
    /// reg[out] = lane[col] row value.
    LoadLane { col: u16, out: u8 },
    LoadConst { k: u16, out: u8 },
    /// Strict comparison: NULL if either input NULL.
    Cmp { op: CmpOp, a: u8, b: u8, out: u8 },
    /// int2/int4/int8 arithmetic with production error behavior (erroring
    /// step — refuse-and-replay for overflow / zero divisor).
    Arith { op: ArithOp, a: u8, b: u8, out: u8 },
    /// reg[out] = (reg[a] IS [NOT] NULL) — non-erroring, non-NULL.
    NullTest { a: u8, out: u8, kind: NullTestKind },
    /// reg[out] = (reg[a] IS [NOT] TRUE/FALSE) — non-erroring, non-NULL.
    BoolTest { a: u8, out: u8, kind: BoolTestKind },
    /// reg[out] = reg[a] <op> ANY (const array `arr`), strict-OR three-valued
    /// (production ScalarArrayOpExpr useOr): non-erroring for the whitelisted
    /// fixed-width by-value comparators. NULL scalar or all-non-matching with
    /// a NULL element yields NULL; else the OR of the element matches.
    SaopAny { a: u8, out: u8, op: CmpOp, arr: u16 },
    /// Clause boundary: reg[a] NULL or false fails the row (short-circuit:
    /// later clauses never evaluate for this row).
    Qual { a: u8 },
}

pub struct Program {
    pub steps: Vec<Step>,
    pub consts: Vec<NullableDatum>,
    /// Baked const arrays for SaopAny steps (fixed-width by-value elements,
    /// individual elements may be NULL). The array datum itself is always
    /// present — a NULL array upstream keeps the program off the stitcher.
    pub arrays: Vec<Vec<NullableDatum>>,
    /// Set by the translator for programs that must never be reordered or
    /// stitched (volatile functions upstream); keeps the program on the
    /// interpreter tier.
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

// The whitelisted comparator families (execexpr steps.rs lineage: full
// int2/int4/int8 + cross-width families, unsigned oid, and the float
// families with C float.h NaN ordering).
//
// DATE / TIMESTAMP carriers (old-lane reuse, no new stencil): date is an
// int4 carrier and timestamp/timestamptz are int8 carriers, all with
// -infinity/+infinity encoded as the type sentinels (INT_MIN/MAX,
// INT64_MIN/MAX). Those sentinels sort as plain signed integers, so date
// comparisons stitch through the Int4* family and timestamp/tstz through
// the Int8* family unchanged — the translator picks the carrier's width and
// the ordering is already exact (see the date_timestamp_carrier_ordering
// parity test).
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
    // Oid is unsigned; interpreter/scalar tiers truncate to u32
    // (extension-blind). The 2x64 SIMD tier additionally requires the
    // canonical sign-extension contract (module header).
    OidEq,
    OidNe,
    OidLt,
    OidLe,
    OidGt,
    OidGe,
    // Float families with C float.h NaN ordering (NaN = NaN, NaN > any
    // non-NaN); comparisons never error. f32 operands promote to f64
    // (exact and order-preserving — btfloat48cmp precedent), so one
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

/// The float comparator families (NaN-ordering semantics). Every stitch
/// gate that treats floats specially keys on this ONE predicate so the
/// fences cannot drift apart as families gain stencils.
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
