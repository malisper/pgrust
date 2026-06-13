//! In-memory working types for the `numeric` arithmetic engine (numeric.c).
//!
//! [`NumericVar`]`<'mcx>` is the idiomatic analogue of C's `NumericVar`: the
//! computation-time representation operated on by `add_var`/`mul_var`/`div_var`/
//! etc. Its digit buffer is a *charged* `mcx::PgVec<'mcx, NumericDigit>`, so the
//! `'mcx` lifetime (the memory context that owns the digits) threads through
//! every numeric family — this is the keystone lifetime.
//!
//! The Vec-bearing aggregate-transition states ([`NumericSumAccum`],
//! [`NumericAggState`], [`GenerateSeriesNumericFctx`]) live here too: they bear
//! charged buffers and so also carry `'mcx`.

use mcx::PgVec;

use crate::{NumericDigit, NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS};

/// Sign / kind tag for a [`NumericVar`].
///
/// Mirrors the C convention where `sign` holds one of `NUMERIC_POS`,
/// `NUMERIC_NEG`, `NUMERIC_NAN`, `NUMERIC_PINF`, or `NUMERIC_NINF`. For a
/// "special" value (NaN/+Inf/-Inf) only the sign matters: `ndigits` should be
/// zero and the weight/dscale fields are ignored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericSign {
    /// Finite, non-negative.
    Pos,
    /// Finite, negative.
    Neg,
    /// Not-a-number.
    NaN,
    /// Positive infinity.
    PInf,
    /// Negative infinity.
    NInf,
}

impl NumericSign {
    /// True for `NaN`, `PInf`, or `NInf`.
    #[inline]
    pub fn is_special(self) -> bool {
        !matches!(self, NumericSign::Pos | NumericSign::Neg)
    }

    /// True for `PInf` or `NInf`.
    #[inline]
    pub fn is_inf(self) -> bool {
        matches!(self, NumericSign::PInf | NumericSign::NInf)
    }

    /// Convert from the on-disk `NUMERIC_*` sign word (as returned by
    /// [`crate::numeric_sign`]). `None` for an unrecognized bit pattern.
    #[inline]
    pub fn from_numeric_word(word: u16) -> Option<Self> {
        match word {
            NUMERIC_POS => Some(NumericSign::Pos),
            NUMERIC_NEG => Some(NumericSign::Neg),
            // NaN uses NUMERIC_SPECIAL (0xC000) as its ext-flagbits value.
            0xC000 => Some(NumericSign::NaN),
            NUMERIC_PINF => Some(NumericSign::PInf),
            NUMERIC_NINF => Some(NumericSign::NInf),
            _ => None,
        }
    }

    /// Convert back to the on-disk `NUMERIC_*` sign word.
    #[inline]
    pub fn to_numeric_word(self) -> u16 {
        match self {
            NumericSign::Pos => NUMERIC_POS,
            NumericSign::Neg => NUMERIC_NEG,
            NumericSign::NaN => 0xC000,
            NumericSign::PInf => NUMERIC_PINF,
            NumericSign::NInf => NUMERIC_NINF,
        }
    }
}

/// The in-memory working representation used for arithmetic — the idiomatic
/// analogue of C's `NumericVar`. NOT `#[repr(C)]`: purely computation-time, never
/// stored on disk.
///
/// The value is `sign * (digits as a base-NBASE number) * NBASE**weight`, where
/// `digits[0]` is the most-significant base-NBASE digit (multiplied by
/// `NBASE**weight`; there are `weight + 1` digits before the decimal point and
/// `weight` may be negative). For a special value only `sign` is meaningful and
/// the logical digit run is empty.
///
/// # Carry-headroom invariant
///
/// C's `NumericVar` keeps the palloc'd buffer (`buf`) separate from the logical
/// digit start (`digits`), normally leaving a zeroed slack digit or two. That
/// slack lets `mul_var`/`div_var` postpone carry propagation and lets a carry
/// out of the top digit be absorbed by decrementing the start (and incrementing
/// `weight`) without reallocating. Here the digits live in a charged
/// [`PgVec`]; the [`headroom`](NumericVar::headroom) field records how many
/// leading entries are reserved zeroed slack and are NOT part of the logical
/// value. The logical digits are `digits[headroom..]`.
#[derive(Clone, Debug)]
pub struct NumericVar<'mcx> {
    /// Sign / kind of the value.
    pub sign: NumericSign,
    /// Weight of the first logical digit (base-NBASE).
    pub weight: i32,
    /// Display scale: decimal digits after the point. Always `>= 0`.
    pub dscale: i32,
    /// Backing digit buffer (charged to `'mcx`). Logical digits are
    /// `digits[headroom..]`; the first `headroom` entries are zeroed carry
    /// slack (see the carry-headroom invariant).
    pub digits: PgVec<'mcx, NumericDigit>,
    /// Number of leading reserved (zeroed) carry-slack digits in `digits`.
    pub headroom: usize,
}

impl<'mcx> NumericVar<'mcx> {
    /// A zero value (`0`, weight 0, dscale 0, no digits) in context `mcx`.
    #[inline]
    pub fn zero(mcx: mcx::Mcx<'mcx>) -> Self {
        NumericVar {
            sign: NumericSign::Pos,
            weight: 0,
            dscale: 0,
            digits: PgVec::new_in(mcx),
            headroom: 0,
        }
    }

    /// A special value (`NaN`/`PInf`/`NInf`); panics on a finite sign.
    #[inline]
    pub fn special(mcx: mcx::Mcx<'mcx>, sign: NumericSign) -> Self {
        debug_assert!(sign.is_special());
        NumericVar {
            sign,
            weight: 0,
            dscale: 0,
            digits: PgVec::new_in(mcx),
            headroom: 0,
        }
    }

    /// The logical significant digits (excluding leading carry slack).
    #[inline]
    pub fn logical_digits(&self) -> &[NumericDigit] {
        &self.digits[self.headroom..]
    }

    /// Number of logical (significant) base-NBASE digits. Can be 0.
    #[inline]
    pub fn ndigits(&self) -> usize {
        self.digits.len() - self.headroom
    }

    /// True if this is a special (`NaN`/`Inf`) value.
    #[inline]
    pub fn is_special(&self) -> bool {
        self.sign.is_special()
    }
}

// ---------------------------------------------------------------------------
// Aggregate-transition working state (numeric.c).
//
// Computation-time (per-call / per-aggcontext) states, NOT on-disk storage and
// NOT shared/cross-process state. They bear charged `PgVec`s and so carry the
// `'mcx` lifetime; they are signature types of this unit's aggregate
// transition/final functions.
// ---------------------------------------------------------------------------

/// `NumericSumAccum` (numeric.c:380-389) -- the fast, lazy sum accumulator.
/// Positive and negative values accumulate separately in `pos_digits`/
/// `neg_digits` as 32-bit limbs, allowing up to `NBASE-1` values before a carry
/// pass; carries propagate only on overflow or final extraction.
#[derive(Clone, Debug)]
pub struct NumericSumAccum<'mcx> {
    /// Number of NBASE limb positions in `pos_digits`/`neg_digits`.
    pub ndigits: i32,
    /// Weight of the first (most-significant) limb position.
    pub weight: i32,
    /// Maximum display scale seen among the summed values.
    pub dscale: i32,
    /// Number of values added since the last carry propagation.
    pub num_uncarried: i32,
    /// True while a leading limb is still reserved for carry-out headroom.
    pub have_carry_space: bool,
    /// Running positive contributions, one `int32` per NBASE limb position.
    pub pos_digits: PgVec<'mcx, i32>,
    /// Running negative contributions, one `int32` per NBASE limb position.
    pub neg_digits: PgVec<'mcx, i32>,
}

impl<'mcx> NumericSumAccum<'mcx> {
    /// A fresh, all-zero accumulator backed by context `mcx`.
    #[inline]
    pub fn new(mcx: mcx::Mcx<'mcx>) -> Self {
        NumericSumAccum {
            ndigits: 0,
            weight: 0,
            dscale: 0,
            num_uncarried: 0,
            have_carry_space: false,
            pos_digits: PgVec::new_in(mcx),
            neg_digits: PgVec::new_in(mcx),
        }
    }
}

/// `NumericAggState` (numeric.c:4913-4926) -- transition state for the SQL
/// `numeric` aggregates (`sum`/`avg`/`variance`/`stddev`). Carries the running
/// `sumX` (and optionally `sumX2`) accumulators, a count `N`, and NaN/+Inf/-Inf
/// counters. The C `agg_context` MemoryContext is elided: the owned
/// accumulators carry their own charged storage.
#[derive(Clone, Debug)]
pub struct NumericAggState<'mcx> {
    /// Whether the second moment (`sumX2`) is being accumulated.
    pub calc_sum_x2: bool,
    /// Count of non-null, non-special inputs.
    pub n: i64,
    /// Sum of the input values.
    pub sum_x: NumericSumAccum<'mcx>,
    /// Sum of the squares of the input values (only when `calc_sum_x2`).
    pub sum_x2: NumericSumAccum<'mcx>,
    /// Maximum display scale among inputs.
    pub max_scale: i32,
    /// Number of inputs that contributed `max_scale`.
    pub max_scale_count: i64,
    /// Count of NaN inputs seen.
    pub nan_count: i64,
    /// Count of +Inf inputs seen.
    pub p_inf_count: i64,
    /// Count of -Inf inputs seen.
    pub n_inf_count: i64,
}

impl<'mcx> NumericAggState<'mcx> {
    /// A fresh transition state backed by context `mcx`.
    #[inline]
    pub fn new(mcx: mcx::Mcx<'mcx>, calc_sum_x2: bool) -> Self {
        NumericAggState {
            calc_sum_x2,
            n: 0,
            sum_x: NumericSumAccum::new(mcx),
            sum_x2: NumericSumAccum::new(mcx),
            max_scale: 0,
            max_scale_count: 0,
            nan_count: 0,
            p_inf_count: 0,
            n_inf_count: 0,
        }
    }

    /// `NA_TOTAL_COUNT(na)` (numeric.c:4928) -- N plus the special counts.
    #[inline]
    pub fn total_count(&self) -> i64 {
        self.n + self.nan_count + self.p_inf_count + self.n_inf_count
    }
}

/// `generate_series_numeric_fctx` (numeric.c:328) -- the cross-call state for
/// `generate_series(numeric, ...)`. Holds the value to emit next, the inclusive
/// stop bound, and the per-iteration step. Per-SRF-call computation state, not
/// on-disk storage; bears `NumericVar`s so it carries `'mcx`.
#[derive(Clone, Debug)]
pub struct GenerateSeriesNumericFctx<'mcx> {
    /// The value to emit on the next call (advanced by `step` each iteration).
    pub current: NumericVar<'mcx>,
    /// The inclusive stop bound.
    pub stop: NumericVar<'mcx>,
    /// The per-iteration step (validated non-zero, non-special).
    pub step: NumericVar<'mcx>,
}
