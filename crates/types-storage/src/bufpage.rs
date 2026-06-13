//! Page / line-pointer sizing vocabulary (`storage/bufpage.h`,
//! `storage/off.h`, `storage/itemid.h`, `access/htup_details.h`), trimmed to
//! the sizing constants ports consume.

use types_core::{uint32, BLCKSZ, OffsetNumber};

/// `ItemIdData` (`storage/itemid.h`) — a line pointer: a 4-byte packed
/// `(lp_off:15, lp_flags:2, lp_len:15)` word. Only its 4-byte width is
/// consumed here (for `MaxOffsetNumber`); the packed-field accessors are not
/// reproduced until a consumer needs them.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemIdData {
    raw: uint32,
}

/// `MaxOffsetNumber` (`storage/off.h`) — `(OffsetNumber)(BLCKSZ /
/// sizeof(ItemIdData))`.
pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / core::mem::size_of::<ItemIdData>()) as u16;

/// `SizeOfPageHeaderData` (`storage/bufpage.h`) —
/// `offsetof(PageHeaderData, pd_linp)` == 24 bytes on the supported build.
pub const SizeOfPageHeaderData: usize = 24;

/// `SizeofHeapTupleHeader` (`access/htup_details.h`) —
/// `offsetof(HeapTupleHeaderData, t_bits)`.
pub const SizeofHeapTupleHeader: usize = 23;

/// `MaxHeapTuplesPerPage` (`access/htup_details.h`):
/// `(BLCKSZ - SizeOfPageHeaderData) / (MAXALIGN(SizeofHeapTupleHeader) +
/// sizeof(ItemIdData))`. `MAXALIGN(23) == 24` on the 8-byte-aligned build.
pub const MaxHeapTuplesPerPage: usize =
    (BLCKSZ - SizeOfPageHeaderData) / (24 + core::mem::size_of::<ItemIdData>());
