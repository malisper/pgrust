//! ABI structs and constants for assorted ADT (abstract data type) modules:
//! `inet`/`cidr`, `macaddr`, `macaddr8`, `uuid`, `bit`/`varbit`, and `cash`.
//!
//! These mirror the on-disk / Datum C declarations in
//! `src/include/utils/inet.h`, `src/include/utils/uuid.h`,
//! `src/include/utils/varbit.h`, and `src/include/utils/cash.h`.
//! Layout is locked down with const-assert size/offset gates so the structs
//! stay ABI-compatible with the C definitions on the same target.

use core::ffi::c_char;

use crate::heaptuple::bits8;

// ---------------------------------------------------------------------------
// const-assert helper
// ---------------------------------------------------------------------------

/// Compile-time assertion: forces a build error when `$cond` is false.
macro_rules! const_assert {
    ($cond:expr) => {
        const _: [(); 0 - !{
            const ASSERT: bool = $cond;
            ASSERT
        } as usize] = [];
    };
}

// ---------------------------------------------------------------------------
// inet / cidr  (utils/inet.h)
// ---------------------------------------------------------------------------

/// Family field values for [`inet_struct`].  `PGSQL_AF_INET` is `AF_INET + 0`
/// and `PGSQL_AF_INET6` is `AF_INET + 1` (utils/inet.h).
///
/// These are on-disk constants for the inet/cidr types: PostgreSQL stores
/// `AF_INET`'s numeric value (2 on every platform it supports) in the inet
/// binary representation, so the value must be stable regardless of build host.
/// On wasm (`target_family = "wasm"`) `libc` does not export `AF_INET`, so we
/// pin the literal `2` — identical to the value `libc::AF_INET` yields natively.
#[cfg(not(target_family = "wasm"))]
pub const PGSQL_AF_INET: u8 = libc::AF_INET as u8;
#[cfg(not(target_family = "wasm"))]
pub const PGSQL_AF_INET6: u8 = (libc::AF_INET + 1) as u8;
#[cfg(target_family = "wasm")]
pub const PGSQL_AF_INET: u8 = 2;
#[cfg(target_family = "wasm")]
pub const PGSQL_AF_INET6: u8 = 3;

/// Internal storage format for IP addresses (both INET and CIDR). (utils/inet.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct inet_struct {
    /// `PGSQL_AF_INET` or `PGSQL_AF_INET6`.
    pub family: u8,
    /// Number of bits in netmask.
    pub bits: u8,
    /// Up to 128 bits of address.
    pub ipaddr: [u8; 16],
}

const_assert!(core::mem::size_of::<inet_struct>() == 18);
const_assert!(core::mem::align_of::<inet_struct>() == 1);
const_assert!(core::mem::offset_of!(inet_struct, family) == 0);
const_assert!(core::mem::offset_of!(inet_struct, bits) == 1);
const_assert!(core::mem::offset_of!(inet_struct, ipaddr) == 2);

/// `inet`/`cidr` varlena: a varlena header followed by an [`inet_struct`]. (utils/inet.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct inet {
    /// Varlena header (do not touch directly!).
    pub vl_len_: [c_char; 4],
    pub inet_data: inet_struct,
}

const_assert!(core::mem::offset_of!(inet, vl_len_) == 0);
const_assert!(core::mem::offset_of!(inet, inet_data) == 4);

// ---------------------------------------------------------------------------
// macaddr / macaddr8  (utils/inet.h)
// ---------------------------------------------------------------------------

/// Internal storage format for MAC addresses (fixed-length pass-by-reference). (utils/inet.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct macaddr {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
}

const_assert!(core::mem::size_of::<macaddr>() == 6);
const_assert!(core::mem::align_of::<macaddr>() == 1);

/// Internal storage format for MAC8 addresses (fixed-length pass-by-reference). (utils/inet.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct macaddr8 {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
    pub g: u8,
    pub h: u8,
}

const_assert!(core::mem::size_of::<macaddr8>() == 8);
const_assert!(core::mem::align_of::<macaddr8>() == 1);

// ---------------------------------------------------------------------------
// uuid  (utils/uuid.h)
// ---------------------------------------------------------------------------

/// Length in bytes of a UUID. (utils/uuid.h)
pub const UUID_LEN: usize = 16;

/// Storage format for the `uuid` type (fixed-length pass-by-reference). (utils/uuid.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct pg_uuid_t {
    pub data: [u8; UUID_LEN],
}

const_assert!(core::mem::size_of::<pg_uuid_t>() == 16);
const_assert!(core::mem::align_of::<pg_uuid_t>() == 1);

// ---------------------------------------------------------------------------
// bit / varbit  (utils/varbit.h)
// ---------------------------------------------------------------------------

/// Bits per byte (c.h).
pub const BITS_PER_BYTE: i32 = 8;
/// Header overhead *in addition to* VARHDRSZ. (utils/varbit.h)
pub const VARBITHDRSZ: usize = core::mem::size_of::<i32>();
/// Maximum number of bits. (utils/varbit.h)
pub const VARBITMAXLEN: i32 = i32::MAX - BITS_PER_BYTE + 1;
/// Mask that will cover exactly one byte. (utils/varbit.h)
pub const BITMASK: u8 = 0xFF;

/// Storage format for `bit` and `bit varying` (toastable varlena). (utils/varbit.h)
///
/// Caution: if `bit_len` is not a multiple of `BITS_PER_BYTE`, the low-order
/// bits of the last byte of `bit_dat[]` are unused and MUST be zeroes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VarBit {
    /// Varlena header (do not touch directly!).
    pub vl_len_: i32,
    /// Number of valid bits.
    pub bit_len: i32,
    /// Bit string, most significant byte first (flexible array member).
    pub bit_dat: [bits8; 0],
}

const_assert!(core::mem::offset_of!(VarBit, vl_len_) == 0);
const_assert!(core::mem::offset_of!(VarBit, bit_len) == 4);
const_assert!(core::mem::offset_of!(VarBit, bit_dat) == 8);

// ---------------------------------------------------------------------------
// cash  (utils/cash.h)
// ---------------------------------------------------------------------------

/// Storage format for the `money` type. (utils/cash.h)
pub type Cash = i64;
