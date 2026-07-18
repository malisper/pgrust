use ::types_core::{Oid, TransactionId};

// The 8-byte C Datum word (SIZEOF_DATUM == 8, USE_FLOAT8_BYVAL). Lane
// invariant: typbyval/typlen are threaded from context, never probed from the
// word; by-ref values cross only as raw pointer words (from_usize/as_usize)
// borrowed from the page/tuple/context that owns them.
//
// SIZEOF_DATUM stays 8 on EVERY target, including 32-bit-pointer wasm32: the
// whole port hardcodes C's 64-bit catalog configuration (int8/float8 byval,
// 8-byte alignment); C's own 32-bit story (pointer-width Datum, float8
// byref) is a different catalog and is NOT what this tree ports. On wasm32
// the word is u64 and pointers occupy the low 32 bits — from_usize
// zero-extends, as_usize truncates back losslessly.
#[cfg(not(target_family = "wasm"))]
type DatumWord = usize;
#[cfg(target_family = "wasm")]
type DatumWord = u64;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Datum(DatumWord);

const _: () = assert!(core::mem::size_of::<Datum>() == 8);

mcx::forget_safe_nodrop!(Datum, NullableDatum);

impl Datum {
    pub const fn null() -> Self {
        Self(0)
    }

    pub const fn from_usize(value: usize) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub const fn from_bool(value: bool) -> Self {
        Self(value as DatumWord)
    }

    // C `DatumGetBool` is `(X != 0)`: any nonzero word reads back true.
    pub const fn as_bool(self) -> bool {
        self.0 != 0
    }

    pub const fn from_i16(value: i16) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_i16(self) -> i16 {
        self.0 as i16
    }

    // Signed *GetDatum sign-extends into the full word, matching C.
    pub const fn from_i32(value: i32) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_i32(self) -> i32 {
        self.0 as u32 as i32
    }

    pub const fn from_u32(value: u32) -> Self {
        Self(value as DatumWord)
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

    pub const fn from_char(value: i8) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_char(self) -> i8 {
        self.0 as u8 as i8
    }

    pub const fn from_i8(value: i8) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_i8(self) -> i8 {
        self.0 as u8 as i8
    }

    pub const fn from_u8(value: u8) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_u8(self) -> u8 {
        self.0 as u8
    }

    pub const fn from_u16(value: u16) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_u16(self) -> u16 {
        self.0 as u16
    }

    pub const fn from_i64(value: i64) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_i64(self) -> i64 {
        self.0 as u64 as i64
    }

    pub const fn from_u64(value: u64) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }

    pub const fn from_f32(value: f32) -> Self {
        Self(value.to_bits() as DatumWord)
    }

    pub const fn as_f32(self) -> f32 {
        f32::from_bits(self.0 as u32)
    }

    pub const fn from_f64(value: f64) -> Self {
        Self(value.to_bits() as DatumWord)
    }

    pub const fn as_f64(self) -> f64 {
        f64::from_bits(self.0 as u64)
    }

    pub const fn from_transaction_id(value: TransactionId) -> Self {
        Self(value as DatumWord)
    }

    pub const fn as_transaction_id(self) -> TransactionId {
        self.0 as TransactionId
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}

impl NullableDatum {
    pub const fn value(value: Datum) -> Self {
        Self {
            value,
            isnull: false,
        }
    }

    pub const fn null() -> Self {
        Self {
            value: Datum::null(),
            isnull: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_char_small_int_round_trips() {
        assert!(Datum::from_bool(true).as_bool());
        assert!(!Datum::from_bool(false).as_bool());
        assert!(Datum::from_usize(2).as_bool());

        for v in [i8::MIN, -1, 0, 1, 42, i8::MAX] {
            assert_eq!(Datum::from_char(v).as_char(), v);
            assert_eq!(Datum::from_i8(v).as_i8(), v);
        }
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
            assert_eq!(back.to_bits(), v.to_bits(), "f32 {v} did not round-trip");
        }
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
        assert_eq!(Datum::from_f64(-1.0).as_u64(), (-1.0f64).to_bits());
    }
}
