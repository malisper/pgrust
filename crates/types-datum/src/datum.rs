//! Idiomatic `Datum`: a single machine word that carries a fixed-width scalar
//! or a pointer-sized value, with safe typed conversions.
//!
//! PostgreSQL's `Datum` is a `uintptr_t` (`postgres.h`). Here it is a newtype
//! over `usize` with explicit, allocation-free conversions in place of the C
//! `*GetDatum` / `DatumGet*` macros. Pointer-carrying datums are represented by
//! the raw word (`from_usize` / `as_usize`); owned payloads live in the typed
//! node structs that reference them, not in the `Datum` itself.

use core::num::NonZeroUsize;

use types_core::{Oid, TransactionId};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Datum(usize);

impl Datum {
    pub const fn null() -> Self {
        Self(0)
    }

    pub const fn from_usize(value: usize) -> Self {
        Self(value)
    }

    pub const fn as_usize(self) -> usize {
        self.0
    }

    pub const fn from_bool(value: bool) -> Self {
        Self(value as usize)
    }

    pub const fn as_bool(self) -> Option<bool> {
        match self.0 {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    pub const fn from_i16(value: i16) -> Self {
        Self(value as usize)
    }

    pub const fn as_i16(self) -> i16 {
        self.0 as i16
    }

    pub const fn from_i32(value: i32) -> Self {
        // PostgreSQL `Int32GetDatum(X)` is `(Datum) X`, which SIGN-extends a
        // negative int32 into the full-width Datum (consistent with `from_i16`).
        Self(value as usize)
    }

    pub const fn as_i32(self) -> i32 {
        self.0 as u32 as i32
    }

    pub const fn from_u32(value: u32) -> Self {
        Self(value as usize)
    }

    pub const fn as_u32(self) -> u32 {
        self.0 as u32
    }

    pub const fn from_oid(value: Oid) -> Self {
        Self::from_u32(value)
    }

    pub const fn as_oid(self) -> Oid {
        self.as_u32()
    }

    pub const fn from_nonzero_word(value: NonZeroUsize) -> Self {
        Self(value.get())
    }

    pub const fn as_nonzero_word(self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(self.0)
    }

    // -----------------------------------------------------------------------
    // Pass-by-value `*GetDatum` / `DatumGet*` codec family (`postgres.h`).
    //
    // Every conversion below carries a pass-by-VALUE scalar inside the single
    // `Datum` word, mirroring the C macros 1:1 on a 64-bit (`SIZEOF_DATUM == 8`,
    // `USE_FLOAT8_BYVAL`) build. A pass-by-REFERENCE type (`text`, `numeric`,
    // every varlena) is NOT representable here — it crosses the fmgr boundary as
    // the typed `RefPayload` side-channel (see `fmgr_boundary`), never as a word.
    // -----------------------------------------------------------------------

    /// C: `CharGetDatum(X)` — a single `char` (PG's `char` type is the signed
    /// 1-byte `c_char`). Sign-extended into the word like `from_i16`/`from_i32`.
    pub const fn from_char(value: i8) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetChar(X)` — read back the low byte as a signed `char`.
    pub const fn as_char(self) -> i8 {
        self.0 as u8 as i8
    }

    /// C: `Int8GetDatum(X)` — PG's `"tinyint"`-shaped 1-byte signed value. (Not
    /// to be confused with the SQL `int8`/`bigint`, which is [`Self::from_i64`].)
    pub const fn from_i8(value: i8) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetInt8(X)` — read back the low byte as a signed 1-byte int.
    pub const fn as_i8(self) -> i8 {
        self.0 as u8 as i8
    }

    /// C: `UInt8GetDatum(X)`.
    pub const fn from_u8(value: u8) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetUInt8(X)`.
    pub const fn as_u8(self) -> u8 {
        self.0 as u8
    }

    /// C: `UInt16GetDatum(X)`.
    pub const fn from_u16(value: u16) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetUInt16(X)`.
    pub const fn as_u16(self) -> u16 {
        self.0 as u16
    }

    /// C: `Int64GetDatum(X)` — the SQL `int8`/`bigint`. On a 64-bit
    /// (`USE_FLOAT8_BYVAL`/`SIZEOF_DATUM == 8`) build this is pass-by-value: the
    /// i64 bit pattern occupies the whole word.
    pub const fn from_i64(value: i64) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetInt64(X)` — read the word back as a signed 64-bit int.
    pub const fn as_i64(self) -> i64 {
        self.0 as u64 as i64
    }

    /// C: `UInt64GetDatum(X)`.
    pub const fn from_u64(value: u64) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetUInt64(X)`.
    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }

    /// C: `Float4GetDatum(X)` — a `float4`/`real`. PG stores the IEEE-754 bit
    /// pattern of the `float`, NOT a numeric cast: `Float4GetDatum` does
    /// `memcpy`-style reinterpret (`Int32GetDatum(*(int32 *) &X)`), so a negative
    /// or NaN value round-trips bit-for-bit. The 32-bit pattern is held in the
    /// low word (consistent with `from_u32`).
    pub const fn from_f32(value: f32) -> Self {
        Self(value.to_bits() as usize)
    }

    /// C: `DatumGetFloat4(X)` — reinterpret the low 32 bits as an IEEE-754
    /// `float`.
    pub const fn as_f32(self) -> f32 {
        f32::from_bits(self.0 as u32)
    }

    /// C: `Float8GetDatum(X)` — a `float8`/`double precision`. On a 64-bit
    /// (`USE_FLOAT8_BYVAL`) build this is pass-by-value: PG reinterprets the
    /// IEEE-754 bits of the `double` into the Datum word (`*(int64 *) &X`), so
    /// the value round-trips bit-for-bit (negatives, NaN, ±Inf included).
    pub const fn from_f64(value: f64) -> Self {
        Self(value.to_bits() as usize)
    }

    /// C: `DatumGetFloat8(X)` — reinterpret the word as an IEEE-754 `double`.
    pub const fn as_f64(self) -> f64 {
        f64::from_bits(self.0 as u64)
    }

    /// C: `TransactionIdGetDatum(X)` — a `TransactionId` (`xid`) is a `uint32`.
    pub const fn from_transaction_id(value: TransactionId) -> Self {
        Self(value as usize)
    }

    /// C: `DatumGetTransactionId(X)`.
    pub const fn as_transaction_id(self) -> TransactionId {
        self.0 as TransactionId
    }
}

/// `NullableDatum` (`postgres.h`) — a `Datum` paired with an explicit `isnull`
/// flag, matching PostgreSQL's `struct NullableDatum { Datum value; bool isnull; }`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}

impl NullableDatum {
    pub const fn null() -> Self {
        Self {
            value: Datum::null(),
            isnull: true,
        }
    }

    pub const fn some(value: Datum) -> Self {
        Self {
            value,
            isnull: false,
        }
    }

    /// Constructor matching PostgreSQL call sites that build a non-null datum.
    pub const fn value(value: Datum) -> Self {
        Self {
            value,
            isnull: false,
        }
    }

    /// `Some(value)` when not null, else `None`.
    pub const fn get(self) -> Option<Datum> {
        if self.isnull {
            None
        } else {
            Some(self.value)
        }
    }
}

/// A datum comparison callback (replaces a C `int (*)(Datum, Datum)` slot).
pub type DatumComparator = fn(Datum, Datum) -> core::cmp::Ordering;
/// A datum transform callback (replaces a C `Datum (*)(Datum)` slot).
pub type DatumTransformer = fn(Datum) -> Datum;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_char_small_int_round_trips() {
        assert_eq!(Datum::from_bool(true).as_bool(), Some(true));
        assert_eq!(Datum::from_bool(false).as_bool(), Some(false));

        // `char` is signed in PG: a negative byte round-trips, and the low byte
        // is masked off (high bits of the word are not read back).
        for v in [i8::MIN, -1, 0, 1, 42, i8::MAX] {
            assert_eq!(Datum::from_char(v).as_char(), v);
            assert_eq!(Datum::from_i8(v).as_i8(), v);
        }
        // `from_char(-1)` sign-extends, but `as_char` only reads the low byte.
        assert_eq!(Datum::from_char(-1).as_char(), -1);
        assert_eq!(Datum::from_u8(0xFF).as_u8(), 0xFF);
        assert_eq!(Datum::from_u16(0xBEEF).as_u16(), 0xBEEF);
    }

    #[test]
    fn int16_int32_negatives_round_trip() {
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            assert_eq!(Datum::from_i16(v).as_i16(), v);
        }
        for v in [i32::MIN, -1, 0, 1, 12345, i32::MAX] {
            assert_eq!(Datum::from_i32(v).as_i32(), v);
        }
        // u32 / oid / transaction id all share the 32-bit lane.
        assert_eq!(Datum::from_u32(0xDEAD_BEEF).as_u32(), 0xDEAD_BEEF);
        assert_eq!(Datum::from_oid(2202).as_oid(), 2202);
        assert_eq!(
            Datum::from_transaction_id(0xFFFF_FFFF).as_transaction_id(),
            0xFFFF_FFFF
        );
    }

    #[test]
    fn int64_uint64_negatives_round_trip() {
        for v in [i64::MIN, -1, 0, 1, 1_000_000_000_000, i64::MAX] {
            assert_eq!(Datum::from_i64(v).as_i64(), v);
        }
        for v in [0u64, 1, u64::MAX, 0x1234_5678_9ABC_DEF0] {
            assert_eq!(Datum::from_u64(v).as_u64(), v);
        }
        // A negative i64 reinterpreted as u64 keeps its two's-complement bits.
        assert_eq!(Datum::from_i64(-1).as_u64(), u64::MAX);
    }

    #[test]
    fn float4_bit_cast_round_trip() {
        for v in [
            0.0f32,
            -0.0,
            1.0,
            -1.5,
            f32::MIN,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
            core::f32::consts::PI,
        ] {
            let back = Datum::from_f32(v).as_f32();
            // Bit-for-bit (covers -0.0 vs 0.0, which `==` would conflate).
            assert_eq!(back.to_bits(), v.to_bits(), "f32 {v} did not round-trip");
        }
        // NaN survives as a NaN bit pattern (a numeric cast could have collapsed
        // it).
        let nan = Datum::from_f32(f32::NAN).as_f32();
        assert!(nan.is_nan());
    }

    #[test]
    fn float8_bit_cast_round_trip() {
        for v in [
            0.0f64,
            -0.0,
            1.0,
            -2.25,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
            core::f64::consts::E,
        ] {
            let back = Datum::from_f64(v).as_f64();
            assert_eq!(back.to_bits(), v.to_bits(), "f64 {v} did not round-trip");
        }
        let nan = Datum::from_f64(f64::NAN).as_f64();
        assert!(nan.is_nan());
        // A negative float reinterpreted as raw word keeps its sign bit set (the
        // C `Float8GetDatum`/`DatumGetFloat8` `*(int64*)&x` reinterpret).
        assert_eq!(Datum::from_f64(-1.0).as_u64(), (-1.0f64).to_bits());
    }
}
