//! RowId — the first-class 64-bit row-identity currency (C1): late
//! materialization, top-N refetch, and EPQ capture all trade in it.

use ::datum::Datum;
use ::types_core::{BlockNumber, OffsetNumber};
use ::types_tuple::itemptr::ItemPointerData;

/// First-class row identity, one Datum-width word per row (C1). Encodings
/// per source:
///
/// - **heap** (M1, this crate): `(block << 16) | offset` — the TID packed
///   order-preserving (block-major, then offset), occupying the low 48
///   bits. CONTRACT-NOTE: the charter fixes "heap TID mapping now" without
///   naming the split; this is the natural order-embedding of
///   `ItemPointerData` into one word and is frozen here.
/// - **columnar** (part, granule, row): bit split 32/19/13, FROZEN by
///   `pgrc2_format::rowid` (spec §10) — [`RowId::from_columnar`] delegates
///   (the M3-G declared lx edit; `lanev3-m1-chunks.md` §5).
/// - **ducklake**: `(file << 48) | (rg << 32) | row` — already chartered
///   (C1), packed by [`RowId::from_ducklake`]; its `lx_source` implementor
///   arrives at M6.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

impl RowId {
    /// Heap TID packing: order-preserving `(block << 16) | offset`.
    #[inline]
    pub const fn from_heap(block: BlockNumber, offset: OffsetNumber) -> RowId {
        RowId(((block as u64) << 16) | offset as u64)
    }

    #[inline]
    pub const fn heap_block(self) -> BlockNumber {
        (self.0 >> 16) as BlockNumber
    }

    #[inline]
    pub const fn heap_offset(self) -> OffsetNumber {
        self.0 as OffsetNumber
    }

    #[inline]
    pub fn from_item_pointer(ip: &ItemPointerData) -> RowId {
        RowId::from_heap(
            ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(ip),
            ::types_tuple::itemptr::ItemPointerGetOffsetNumberNoCheck(ip),
        )
    }

    #[inline]
    pub fn to_item_pointer(self) -> ItemPointerData {
        ItemPointerData::new(self.heap_block(), self.heap_offset())
    }

    /// Columnar (part, granule, row) packing — the M3-G declared lx edit
    /// filling the M1-A typed constructor hole (`lanev3-m1-chunks.md` §5).
    ///
    /// The bit split (32/19/13) is FROZEN by `pgrc2_format::rowid`
    /// (`docs/design/pgrc2-format.md` §10; O-8 via the O-M3-5 confirmed
    /// core); this is a pure delegation — the split is never restated here.
    /// Order-embedding: within a part, RowId order == row order; across
    /// parts, part_no (publish) order. Field ranges are the callee's
    /// contract (debug-asserted there).
    #[inline]
    pub const fn from_columnar(part: u64, granule: u64, row: u64) -> RowId {
        RowId(::pgrc2_format::rowid::pack_rowid(part as u32, granule as u32, row as u32))
    }

    /// Columnar part_no (delegates to the frozen `pgrc2_format` split).
    #[inline]
    pub const fn columnar_part(self) -> u32 {
        ::pgrc2_format::rowid::rowid_part(self.0)
    }

    /// Columnar granule ordinal within the part.
    #[inline]
    pub const fn columnar_granule(self) -> u32 {
        ::pgrc2_format::rowid::rowid_granule(self.0)
    }

    /// Columnar row ordinal within the granule.
    #[inline]
    pub const fn columnar_row(self) -> u32 {
        ::pgrc2_format::rowid::rowid_row(self.0)
    }

    /// Ducklake packing, chartered in C1: `(file << 48) | (rg << 32) | row`.
    #[inline]
    pub const fn from_ducklake(file: u16, rg: u16, row: u32) -> RowId {
        RowId(((file as u64) << 48) | ((rg as u64) << 32) | row as u64)
    }

    #[inline]
    pub const fn ducklake_file(self) -> u16 {
        (self.0 >> 48) as u16
    }

    #[inline]
    pub const fn ducklake_rg(self) -> u16 {
        (self.0 >> 32) as u16
    }

    #[inline]
    pub const fn ducklake_row(self) -> u32 {
        self.0 as u32
    }

    /// RowId columns stage their packed word directly in the datum cells
    /// ([`crate::ColRep::RowId`]).
    #[inline]
    pub const fn as_datum(self) -> Datum {
        Datum::from_u64(self.0)
    }

    #[inline]
    pub const fn from_datum(d: Datum) -> RowId {
        RowId(d.as_u64())
    }
}
