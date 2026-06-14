//! BRIN tuple/descriptor vocabulary (`access/brin_internal.h`,
//! `access/brin_tuple.h`): the in-memory shapes the BRIN tuple codec
//! (`access/brin/brin_tuple.c`) operates on, plus the on-disk `bt_info`
//! constants. Owned by `brin.c` (`brin_build_desc`) / the BRIN opclasses, which
//! are not ported yet; defined here (trimmed to consumed fields, verified
//! against the C headers) so the codec and its seam signatures can name them.

#![no_std]
#![allow(non_snake_case)]
#![forbid(unsafe_code)]

extern crate alloc;

use mcx::{Mcx, PgBox, PgVec};
use types_core::{AttrNumber, BlockNumber};
use types_error::PgResult;
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;
use types_typcache::TypeCacheEntry;

// ---------------------------------------------------------------------------
// On-disk BrinTuple header layout (brin_tuple.h).
// ---------------------------------------------------------------------------

/// `SizeOfBrinTuple` (`brin_tuple.h`): `offsetof(BrinTuple, bt_info) +
/// sizeof(uint8)` — a `BlockNumber bt_blkno` (offset 0) then a `uint8 bt_info`.
pub const SIZE_OF_BRIN_TUPLE: usize = 5;

/// Byte offset of `bt_info` within the on-disk header.
pub const BT_INFO_OFFSET: usize = 4;

/// `BRIN_OFFSET_MASK` (`brin_tuple.h`): bits 4..0 of `bt_info` hold the data
/// offset.
pub const BRIN_OFFSET_MASK: u8 = 0x1F;
/// `BRIN_EMPTY_RANGE_MASK` (`brin_tuple.h`).
pub const BRIN_EMPTY_RANGE_MASK: u8 = 0x20;
/// `BRIN_PLACEHOLDER_MASK` (`brin_tuple.h`).
pub const BRIN_PLACEHOLDER_MASK: u8 = 0x40;
/// `BRIN_NULLS_MASK` (`brin_tuple.h`).
pub const BRIN_NULLS_MASK: u8 = 0x80;

// ---------------------------------------------------------------------------
// BrinOpcInfo / BrinDesc (brin_internal.h).
// ---------------------------------------------------------------------------

/// `BrinOpcInfo` (`brin_internal.h`): the struct returned by an opclass'
/// `OpcInfo` amproc, describing the on-disk layout of one indexed column.
#[derive(Debug)]
pub struct BrinOpcInfo<'mcx> {
    /// `oi_nstored`: number of columns stored in an index column of this
    /// opclass.
    pub oi_nstored: u16,
    /// `oi_regular_nulls`: regular processing of NULLs in `BrinValues`?
    pub oi_regular_nulls: bool,
    /// `void *oi_opaque`: opaque pointer for the opclass' private use. A
    /// genuinely heterogeneous extension slot (C `void *`); `None` when unset.
    pub oi_opaque: Option<PgBox<'mcx, OpaqueOpcInfo>>,
    /// `oi_typcache[oi_nstored]`: the type-cache entries of the stored columns.
    pub oi_typcache: PgVec<'mcx, TypeCacheEntry>,
}

impl BrinOpcInfo<'_> {
    /// `oi_nstored` as a `usize`.
    #[inline]
    pub fn nstored(&self) -> usize {
        self.oi_nstored as usize
    }
}

/// Placeholder payload for `BrinOpcInfo::oi_opaque` — the opclass-private blob
/// (C `void *`). The opclass that allocates it (e.g. `brin_bloom.c`) owns its
/// real shape; until those land it is an opaque byte buffer.
pub type OpaqueOpcInfo = [u8];

/// `BrinDesc` (`brin_internal.h`): descriptor that enables decoding a BRIN
/// tuple from on-disk to in-memory and back.
///
/// The C struct caches the on-disk tuple descriptor in `bd_disktdesc`; here the
/// codec recomputes it from `bd_info`'s type-cache entries on demand (the cache
/// is purely an optimization, behaviorally identical), so no cache field is
/// carried.
#[derive(Debug)]
pub struct BrinDesc<'mcx> {
    /// `Relation bd_index`: the index relation itself.
    pub bd_index: Relation<'mcx>,
    /// `TupleDesc bd_tupdesc`: tuple descriptor of the index relation. Its
    /// `natts` is the number of indexed columns.
    pub bd_tupdesc: PgBox<'mcx, TupleDescData<'mcx>>,
    /// `bd_totalstored`: total number of `Datum` entries stored on-disk for all
    /// columns.
    pub bd_totalstored: i32,
    /// `bd_info[bd_tupdesc->natts]`: per-column opclass info.
    pub bd_info: PgVec<'mcx, PgBox<'mcx, BrinOpcInfo<'mcx>>>,
}

impl BrinDesc<'_> {
    /// `brdesc->bd_tupdesc->natts` — number of indexed columns.
    #[inline]
    pub fn natts(&self) -> usize {
        self.bd_tupdesc.natts as usize
    }
}

// ---------------------------------------------------------------------------
// BrinValues / BrinMemTuple (brin_tuple.h).
// ---------------------------------------------------------------------------

/// `BrinValues` (`brin_tuple.h`): per-column accumulated values inside a
/// [`BrinMemTuple`].
///
/// `bv_values` carries each stored datum as a [`Datum`] (the codec's
/// faithful `Datum` model — by-value scalars and by-reference byte
/// images), matching `access/common/heaptuple.c`'s form/deform model.
#[derive(Debug)]
pub struct BrinValues<'mcx> {
    /// `bv_attno`: index attribute number (1-based).
    pub bv_attno: AttrNumber,
    /// `bv_hasnulls`: are there any nulls in the page range?
    pub bv_hasnulls: bool,
    /// `bv_allnulls`: are all values nulls in the page range?
    pub bv_allnulls: bool,
    /// `bv_values[oi_nstored]`: current accumulated values.
    pub bv_values: PgVec<'mcx, Datum<'mcx>>,
    /// `bv_mem_value`: opclass-expanded accumulated value (`Datum` of an
    /// expanded object in C); `None` is C's `PointerGetDatum(NULL)`.
    pub bv_mem_value: Option<Datum<'mcx>>,
    /// Whether a `bv_serialize` opclass callback is registered for this column
    /// (`brin_serialize_callback_type`; `false` is the C NULL pointer). The
    /// callback itself is opclass-owned and invoked through the brin-tuple
    /// `brin_serialize` seam keyed by the column index.
    pub bv_has_serialize: bool,
}

/// `BrinMemTuple` (`brin_tuple.h`): the in-memory (deformed) BRIN tuple.
///
/// The C single `palloc` block (header + `BrinValues[natts]` + trailing `Datum`
/// areas) plus the per-tuple `bt_context` become owned `PgVec`s here; the
/// codec's `MemoryContextReset` maps to clearing/rebuilding `bt_columns`.
#[derive(Debug)]
pub struct BrinMemTuple<'mcx> {
    /// `bt_placeholder`: this is a placeholder tuple.
    pub bt_placeholder: bool,
    /// `bt_empty_range`: range represents no tuples.
    pub bt_empty_range: bool,
    /// `bt_blkno`: heap block number the tuple is for.
    pub bt_blkno: BlockNumber,
    /// `bt_columns[bd_tupdesc->natts]`: per-column values.
    pub bt_columns: PgVec<'mcx, BrinValues<'mcx>>,
}

// ---------------------------------------------------------------------------
// The opclass serialize callback (brin_serialize_callback_type, brin_tuple.h).
// ---------------------------------------------------------------------------

/// `brin_serialize_callback_type` (`brin_tuple.h`): an opclass-registered
/// serializer `void (*)(BrinDesc *bdesc, Datum src, Datum *dst)`. The opclass
/// owns the function; the codec stores only its presence on each column (see
/// [`BrinValues::bv_has_serialize`]) and dispatches through the brin-tuple
/// `brin_serialize` seam.
///
/// The seam takes the in-memory expanded value (`src`) and fills the
/// destination `dst` slice (the column's `bv_values`), allocating any
/// by-reference output in `mcx`.
pub type BrinSerializeFn =
    for<'mcx> fn(Mcx<'mcx>, &Datum<'_>, &mut [Datum<'mcx>]) -> PgResult<()>;
