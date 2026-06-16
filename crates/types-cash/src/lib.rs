//! Vocabulary types for `utils/adt/cash.c` (the `money` datatype).
//!
//! `cash.c` itself is almost entirely self-contained integer arithmetic and
//! string formatting; the only genuinely external *value* type it reads is the
//! monetary subset of libc's `struct lconv`, fetched through `PGLC_localeconv()`
//! (which lives in the not-yet-ported `pg_locale` subsystem). Because that
//! snapshot appears in the [`pglc_localeconv`] seam signature (declared in
//! `backend-utils-adt-pg-locale-seams`, the locale owner), its working type
//! ([`CashLconv`]) lives here in a `types-*` vocabulary crate.
//!
//! [`pglc_localeconv`]: ../backend_utils_adt_pg_locale_seams/fn.pglc_localeconv.html

/// `Cash` (`utils/cash.h`): the on-the-wire representation of `money` — an
/// `int64` count of the smallest currency unit (cents).
pub type Cash = i64;

/// `signed char` maximum — the value libc stuffs into the numeric `struct lconv`
/// members that are "not available" in a locale (notably the `C` locale).
/// `cash.c` deliberately range-checks against `[0,10]` / `[1,6]` instead of
/// testing `== CHAR_MAX` (see the long comment at `cash_in`), so the exact
/// sentinel only needs to be out of those ranges; we use the real `CHAR_MAX`
/// for fidelity.
pub const C_CHAR_MAX: i8 = i8::MAX;

/// The subset of libc's `struct lconv` that `cash.c` reads.
///
/// Field names and types mirror `struct lconv` exactly (the scalar members are
/// `char` in C, i.e. [`i8`] here). The string members are owned [`String`]s
/// holding the database-encoded bytes (`cash.c` treats them as NUL-terminated C
/// strings and never mutates them); an empty string is the C `""` that triggers
/// each "fall back to the hard-coded default" branch. This is the idiomatic
/// *working snapshot* the seam hands back; it is NOT an on-disk / ABI struct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CashLconv {
    /// `decimal_point` — non-monetary radix character (read by the NUM
    /// number-format engine in `formatting.c`).
    pub decimal_point: String,
    /// `thousands_sep` — non-monetary thousands separator (read by the NUM
    /// number-format engine in `formatting.c`).
    pub thousands_sep: String,
    /// `mon_decimal_point` — monetary radix character.
    pub mon_decimal_point: String,
    /// `mon_thousands_sep` — monetary thousands separator.
    pub mon_thousands_sep: String,
    /// `mon_grouping` — digit-grouping spec; only the first byte is consulted.
    pub mon_grouping: Vec<i8>,
    /// `currency_symbol`.
    pub currency_symbol: String,
    /// `positive_sign`.
    pub positive_sign: String,
    /// `negative_sign`.
    pub negative_sign: String,
    /// `frac_digits` — fractional digits for the local monetary format.
    pub frac_digits: i8,
    /// `p_cs_precedes` — currency symbol precedes a non-negative value.
    pub p_cs_precedes: i8,
    /// `p_sep_by_space` — space separation for a non-negative value.
    pub p_sep_by_space: i8,
    /// `n_cs_precedes` — currency symbol precedes a negative value.
    pub n_cs_precedes: i8,
    /// `n_sep_by_space` — space separation for a negative value.
    pub n_sep_by_space: i8,
    /// `p_sign_posn` — sign position for a non-negative value.
    pub p_sign_posn: i8,
    /// `n_sign_posn` — sign position for a negative value.
    pub n_sign_posn: i8,
}

impl CashLconv {
    /// First byte of `mon_grouping` (C reads `*lconvert->mon_grouping`); `0`
    /// when the spec is empty, exactly as the NUL terminator of an empty C
    /// string.
    #[inline]
    pub fn mon_grouping_first(&self) -> i8 {
        self.mon_grouping.first().copied().unwrap_or(0)
    }

    /// The `C`/`POSIX`-locale `struct lconv`, byte-for-byte as libc reports it:
    /// empty strings everywhere, empty `mon_grouping`, and `CHAR_MAX` in every
    /// numeric member.
    pub fn c_locale() -> Self {
        CashLconv {
            decimal_point: String::new(),
            thousands_sep: String::new(),
            mon_decimal_point: String::new(),
            mon_thousands_sep: String::new(),
            mon_grouping: Vec::new(),
            currency_symbol: String::new(),
            positive_sign: String::new(),
            negative_sign: String::new(),
            frac_digits: C_CHAR_MAX,
            p_cs_precedes: C_CHAR_MAX,
            p_sep_by_space: C_CHAR_MAX,
            n_cs_precedes: C_CHAR_MAX,
            n_sep_by_space: C_CHAR_MAX,
            p_sign_posn: C_CHAR_MAX,
            n_sign_posn: C_CHAR_MAX,
        }
    }
}
