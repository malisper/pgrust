//! Datum / in-memory ABI structs for the expanded-record subsystem.
//!
//! These mirror PostgreSQL 18.3 `src/include/utils/expandedrecord.h`. The
//! `ExpandedRecordHeader` embeds the standard `ExpandedObjectHeader` (whose two
//! TOAST-pointer buffers are part of the Datum ABI), so it carries `#[repr(C)]`
//! and compile-time layout assertions so its layout cannot drift from the C ABI.

use core::ffi::c_char;
use core::ffi::c_int;

use crate::toast::{int32, ExpandedObjectHeader};
use crate::uint64;
use crate::{Datum, HeapTuple, MemoryContext, MemoryContextCallback, Oid, Size, TupleDesc};

/// `#define ER_MAGIC 1384727874` — ID for debugging crosschecks.
pub const ER_MAGIC: c_int = 1384727874;

// Assorted flag bits (`ExpandedRecordHeader.flags`).
/// `ER_FLAG_FVALUE_VALID` — fvalue is up to date?
pub const ER_FLAG_FVALUE_VALID: c_int = 0x0001;
/// `ER_FLAG_FVALUE_ALLOCED` — fvalue is local storage?
pub const ER_FLAG_FVALUE_ALLOCED: c_int = 0x0002;
/// `ER_FLAG_DVALUES_VALID` — dvalues/dnulls are up to date?
pub const ER_FLAG_DVALUES_VALID: c_int = 0x0004;
/// `ER_FLAG_DVALUES_ALLOCED` — any field values local storage?
pub const ER_FLAG_DVALUES_ALLOCED: c_int = 0x0008;
/// `ER_FLAG_HAVE_EXTERNAL` — any field values are external?
pub const ER_FLAG_HAVE_EXTERNAL: c_int = 0x0010;
/// `ER_FLAG_TUPDESC_ALLOCED` — tupdesc is local storage?
pub const ER_FLAG_TUPDESC_ALLOCED: c_int = 0x0020;
/// `ER_FLAG_IS_DOMAIN` — er_decltypeid is domain?
pub const ER_FLAG_IS_DOMAIN: c_int = 0x0040;
/// `ER_FLAG_IS_DUMMY` — this header is dummy.
pub const ER_FLAG_IS_DUMMY: c_int = 0x0080;
/// `ER_FLAGS_NON_DATA` — flag bits that are not to be cleared when replacing
/// tuple data.
pub const ER_FLAGS_NON_DATA: c_int = ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY;

/// `struct ExpandedRecordHeader` from expandedrecord.h: the control structure of
/// an expanded record. The leading `hdr: ExpandedObjectHeader` carries the
/// standard read-write / read-only TOAST pointers, so the whole struct lives in
/// the object's private memory context and is part of the Datum ABI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExpandedRecordHeader {
    /// Standard header for expanded objects.
    pub hdr: ExpandedObjectHeader,
    /// Magic value identifying an expanded record (for debugging only).
    pub er_magic: c_int,
    /// Assorted flag bits.
    pub flags: c_int,
    /// Declared type of the record variable (could be a domain type).
    pub er_decltypeid: Oid,
    /// Type OID of the composite type.
    pub er_typeid: Oid,
    /// Typmod of the composite type.
    pub er_typmod: int32,
    /// Tuple descriptor, if we have one, else NULL.
    pub er_tupdesc: TupleDesc,
    /// Unique-within-process identifier for the tupdesc.
    pub er_tupdesc_id: uint64,
    /// Array of Datums (Datum-array representation), or NULL.
    pub dvalues: *mut Datum,
    /// Array of is-null flags for Datums, or NULL.
    pub dnulls: *mut bool,
    /// Length of the dvalues/dnulls arrays.
    pub nfields: c_int,
    /// Current space requirement for the flat equivalent, if known, else 0.
    pub flat_size: Size,
    /// Data len within flat_size.
    pub data_len: Size,
    /// Header offset.
    pub hoff: c_int,
    /// Null bitmap needed?
    pub hasnull: bool,
    /// Points to the flat representation if we have one, else NULL.
    pub fvalue: HeapTuple,
    /// Start of the flat representation's data area.
    pub fstartptr: *mut c_char,
    /// End+1 of the flat representation's data area.
    pub fendptr: *mut c_char,
    /// Short-term memory context.
    pub er_short_term_cxt: MemoryContext,
    /// Dummy record header (used for domain checking).
    pub er_dummy_header: *mut ExpandedRecordHeader,
    /// Cache space for `domain_check()`.
    pub er_domaininfo: *mut core::ffi::c_void,
    /// Callback info (active if `er_mcb.arg` is not NULL).
    pub er_mcb: MemoryContextCallback,
}

/// `struct ExpandedRecordFieldInfo` from expandedrecord.h: information returned
/// by `expanded_record_lookup_field()`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExpandedRecordFieldInfo {
    /// Field's attr number in record.
    pub fnumber: c_int,
    /// Field's type OID.
    pub ftypeid: Oid,
    /// Field's typmod.
    pub ftypmod: int32,
    /// Field's collation if any.
    pub fcollation: Oid,
}

// Compile-time ABI guard: the header must lead with the standard expanded-object
// header at offset 0 (the embedded TOAST pointers must be the first thing a
// Datum reference resolves to).
const _: () = assert!(core::mem::offset_of!(ExpandedRecordHeader, hdr) == 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn er_magic_and_flags() {
        assert_eq!(ER_MAGIC, 1384727874);
        assert_eq!(ER_FLAG_FVALUE_VALID, 0x0001);
        assert_eq!(ER_FLAG_FVALUE_ALLOCED, 0x0002);
        assert_eq!(ER_FLAG_DVALUES_VALID, 0x0004);
        assert_eq!(ER_FLAG_DVALUES_ALLOCED, 0x0008);
        assert_eq!(ER_FLAG_HAVE_EXTERNAL, 0x0010);
        assert_eq!(ER_FLAG_TUPDESC_ALLOCED, 0x0020);
        assert_eq!(ER_FLAG_IS_DOMAIN, 0x0040);
        assert_eq!(ER_FLAG_IS_DUMMY, 0x0080);
        assert_eq!(
            ER_FLAGS_NON_DATA,
            ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY
        );
    }

    #[test]
    fn expanded_record_header_starts_with_hdr() {
        assert_eq!(core::mem::offset_of!(ExpandedRecordHeader, hdr), 0);
    }
}
