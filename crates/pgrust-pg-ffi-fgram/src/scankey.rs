use core::ffi::c_int;

use crate::{AttrNumber, Datum, FmgrInfo, Oid, StrategyNumber};

pub type ScanKey = *mut ScanKeyData;

pub const C_COLLATION_OID: Oid = 950;

// ScanKey `sk_flags` bits (access/skey.h).
/// `SK_ISNULL` — `sk_argument` is NULL.
pub const SK_ISNULL: c_int = 0x0001;
/// `SK_UNARY` — unary operator (not supported!).
pub const SK_UNARY: c_int = 0x0002;
/// `SK_ROW_HEADER` — row comparison header.
pub const SK_ROW_HEADER: c_int = 0x0004;
/// `SK_ROW_MEMBER` — row comparison member.
pub const SK_ROW_MEMBER: c_int = 0x0008;
/// `SK_ROW_END` — last row comparison member.
pub const SK_ROW_END: c_int = 0x0010;
/// `SK_SEARCHARRAY` — scankey represents ScalarArrayOp.
pub const SK_SEARCHARRAY: c_int = 0x0020;
/// `SK_SEARCHNULL` — scankey represents "col IS NULL".
pub const SK_SEARCHNULL: c_int = 0x0040;
/// `SK_SEARCHNOTNULL` — scankey represents "col IS NOT NULL".
pub const SK_SEARCHNOTNULL: c_int = 0x0080;
/// `SK_ORDER_BY` — scankey is for ORDER BY op.
pub const SK_ORDER_BY: c_int = 0x0100;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ScanKeyData {
    pub sk_flags: c_int,
    pub sk_attno: AttrNumber,
    pub sk_strategy: StrategyNumber,
    pub sk_subtype: Oid,
    pub sk_collation: Oid,
    pub sk_func: FmgrInfo,
    pub sk_argument: Datum,
}

// Compile-time `ScanKeyData` ABI layout asserts (access/skey.h), matching the
// sibling-crate convention (`toast.rs`, `algorithms.rs`, `guc.rs`). These are
// const-evaluated, so any drift from the C 64-bit layout is a build error.
//   int sk_flags; AttrNumber sk_attno; StrategyNumber sk_strategy;
//   Oid sk_subtype; Oid sk_collation; FmgrInfo sk_func; Datum sk_argument;
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_flags) == 0);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_attno) == 4);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_strategy) == 6);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_subtype) == 8);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_collation) == 12);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_func) == 16);
const _: () = assert!(core::mem::offset_of!(ScanKeyData, sk_argument) == 64);
const _: () = assert!(core::mem::size_of::<ScanKeyData>() == 72);
const _: () = assert!(core::mem::align_of::<ScanKeyData>() == 8);

impl ScanKeyData {
    pub const fn empty() -> Self {
        Self {
            sk_flags: 0,
            sk_attno: 0,
            sk_strategy: 0,
            sk_subtype: 0,
            sk_collation: 0,
            sk_func: FmgrInfo::empty(),
            sk_argument: 0,
        }
    }

    pub fn initialize_without_proc(
        &mut self,
        flags: c_int,
        attributeNumber: AttrNumber,
        strategy: StrategyNumber,
        subtype: Oid,
        collation: Oid,
        argument: Datum,
    ) {
        self.sk_flags = flags;
        self.sk_attno = attributeNumber;
        self.sk_strategy = strategy;
        self.sk_subtype = subtype;
        self.sk_collation = collation;
        self.sk_func = FmgrInfo::empty();
        self.sk_argument = argument;
    }
}
