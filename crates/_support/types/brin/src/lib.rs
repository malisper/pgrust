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
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::TupleDescData;
use ::types_typcache::TypeCacheEntry;

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
    /// `void *oi_opaque`: opaque pointer for the opclass' private use. The
    /// opclass support procedures own the value; `None` when unset.
    pub oi_opaque: Option<OpaqueOpcInfo>,
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

/// `BTMaxStrategyNumber` (`stratnum.h`): the number of B-tree strategies that
/// the minmax opclass caches a comparison procinfo for.
pub const BT_MAX_STRATEGY_NUMBER: usize = 5;

/// `RTMaxStrategyNumber` (`stratnum.h`): the number of R-tree strategies that
/// the inclusion opclass caches a comparison procinfo for.
pub const RT_MAX_STRATEGY_NUMBER: usize = 30;

/// `INCLUSION_MAX_PROCNUMS` (`brin_inclusion.c`): the number of optional/required
/// inclusion support procedures (`PROCNUM_MERGE`/`MERGEABLE`/`CONTAINS`/`EMPTY`),
/// cached in [`InclusionOpaque::extra_procinfos`].
pub const INCLUSION_MAX_PROCNUMS: usize = 4;

/// `BLOOM_MAX_PROCNUMS` (`brin_bloom.c`): the number of bloom support procedures
/// (just `PROCNUM_HASH`), cached in [`BloomOpaque::extra_procinfos`].
pub const BLOOM_MAX_PROCNUMS: usize = 1;

/// `MINMAX_MAX_PROCNUMS` (`brin_minmax_multi.c`): the number of optional
/// minmax-multi support procedures (just `PROCNUM_DISTANCE`), cached in
/// [`MinmaxMultiOpaque::extra_procinfos`].
pub const MINMAX_MULTI_MAX_PROCNUMS: usize = 1;

/// Payload for `BrinOpcInfo::oi_opaque` — the opclass-private blob (C
/// `void *oi_opaque`). In C each opclass `palloc0`s its own private struct in
/// the tail of the `BrinOpcInfo` allocation (`MinmaxOpaque`, `InclusionOpaque`,
/// the bloom/minmax-multi caches). Modeled here as a closed typed enum, one
/// variant per built-in opclass; the genuinely heterogeneous `void *` of an
/// extension opclass is not representable and is not used by the built-ins.
///
/// The per-strategy procinfo caches are lazily filled by the opclass support
/// procedures, which the BRIN AM dispatches through a `&BrinDesc` (immutable);
/// the cache slots therefore use `Cell`/[`MinmaxOpaque`]-interior mutability so
/// the lazy fill matches C's mutation through `bdesc->bd_info[]->oi_opaque`.
#[derive(Debug)]
pub enum OpaqueOpcInfo {
    /// `brin_minmax.c` `MinmaxOpaque` — the per-attribute strategy-procinfo
    /// cache.
    Minmax(MinmaxOpaque),
    /// `brin_inclusion.c` `InclusionOpaque` — the per-attribute support- and
    /// strategy-procinfo cache.
    Inclusion(InclusionOpaque),
    /// `brin_bloom.c` `BloomOpaque` — the per-attribute hash-procinfo cache.
    Bloom(BloomOpaque),
    /// `brin_minmax_multi.c` `MinmaxMultiOpaque` — the per-attribute distance
    /// support-procinfo + B-tree strategy-procinfo cache.
    MinmaxMulti(MinmaxMultiOpaque),
}

/// `MinmaxMultiOpaque` (`brin_minmax_multi.c`): the per-attribute support- and
/// strategy-procinfo cache.
///
/// C: `{ FmgrInfo extra_procinfos[MINMAX_MAX_PROCNUMS]; Oid cached_subtype;
///        FmgrInfo strategy_procinfos[BTMaxStrategyNumber]; }`.
///
/// As in [`MinmaxOpaque`] each cached `FmgrInfo` is reduced to the resolved
/// function's `Oid` (the BRIN fmgr-call seam re-resolves by OID). An `Oid` of
/// `InvalidOid` (0) marks an uninitialized slot, exactly as `palloc0` leaves it.
/// The `Cell`s give interior mutability so the cache fills lazily through the
/// `&BrinDesc` the AM passes (C mutates the same struct through a pointer).
#[derive(Debug, Default)]
pub struct MinmaxMultiOpaque {
    /// `extra_procinfos[MINMAX_MAX_PROCNUMS]`: the resolved distance-support
    /// function `Oid` (`InvalidOid` marks an uninitialized slot).
    pub extra_procinfos:
        [core::cell::Cell<::types_core::primitive::Oid>; MINMAX_MULTI_MAX_PROCNUMS],
    /// `cached_subtype`.
    pub cached_subtype: core::cell::Cell<::types_core::primitive::Oid>,
    /// `strategy_procinfos[BTMaxStrategyNumber]`: each slot's resolved
    /// comparison function `Oid` (`InvalidOid` marks an uninitialized slot).
    pub strategy_procinfos:
        [core::cell::Cell<::types_core::primitive::Oid>; BT_MAX_STRATEGY_NUMBER],
}

/// `MinmaxOpaque` (`brin_minmax.c`): the per-attribute strategy-procinfo cache.
///
/// C: `{ Oid cached_subtype; FmgrInfo strategy_procinfos[BTMaxStrategyNumber]; }`.
/// Each cached `FmgrInfo` is represented by the resolved comparison function's
/// `Oid` (its `fn_oid`); the BRIN fmgr-call seam re-resolves by OID, so the
/// `Oid` is the whole callable identity. An `Oid` of `InvalidOid` (0) marks an
/// uninitialized slot, exactly as `palloc0` leaves it.
///
/// `Cell`s give interior mutability so the cache fills lazily through the
/// `&BrinDesc` the AM passes (C mutates the same struct through a pointer).
#[derive(Debug, Default)]
pub struct MinmaxOpaque {
    /// `cached_subtype`.
    pub cached_subtype: core::cell::Cell<::types_core::primitive::Oid>,
    /// `strategy_procinfos[BTMaxStrategyNumber]`: each slot's resolved
    /// comparison function `Oid` (`InvalidOid` marks an uninitialized slot).
    pub strategy_procinfos: [core::cell::Cell<::types_core::primitive::Oid>; BT_MAX_STRATEGY_NUMBER],
}

/// `InclusionOpaque` (`brin_inclusion.c`): the per-attribute support- and
/// strategy-procinfo cache.
///
/// C: `{ FmgrInfo extra_procinfos[INCLUSION_MAX_PROCNUMS];
///        bool extra_proc_missing[INCLUSION_MAX_PROCNUMS];
///        Oid cached_subtype;
///        FmgrInfo strategy_procinfos[RTMaxStrategyNumber]; }`.
///
/// As in [`MinmaxOpaque`] each cached `FmgrInfo` is reduced to the resolved
/// function's `Oid` (the BRIN fmgr-call seam re-resolves by OID). An `Oid` of
/// `InvalidOid` (0) marks an uninitialized slot, exactly as `palloc0` leaves it;
/// `extra_proc_missing[i]` records a support procedure that was looked up and
/// found absent, so it is not searched again. The `Cell`s give interior
/// mutability so the cache fills lazily through the `&BrinDesc` the AM passes (C
/// mutates the same struct through a pointer).
#[derive(Debug, Default)]
pub struct InclusionOpaque {
    /// `extra_procinfos[INCLUSION_MAX_PROCNUMS]`: each optional support
    /// procedure's resolved function `Oid` (`InvalidOid` marks an
    /// uninitialized slot).
    pub extra_procinfos: [core::cell::Cell<::types_core::primitive::Oid>; INCLUSION_MAX_PROCNUMS],
    /// `extra_proc_missing[INCLUSION_MAX_PROCNUMS]`: a support procedure looked
    /// up and found absent (do not search again).
    pub extra_proc_missing: [core::cell::Cell<bool>; INCLUSION_MAX_PROCNUMS],
    /// `cached_subtype`.
    pub cached_subtype: core::cell::Cell<::types_core::primitive::Oid>,
    /// `strategy_procinfos[RTMaxStrategyNumber]`: each slot's resolved
    /// comparison function `Oid` (`InvalidOid` marks an uninitialized slot).
    pub strategy_procinfos: [core::cell::Cell<::types_core::primitive::Oid>; RT_MAX_STRATEGY_NUMBER],
}

/// `BloomOpaque` (`brin_bloom.c`): the per-attribute hash-procinfo cache.
///
/// C: `{ FmgrInfo extra_procinfos[BLOOM_MAX_PROCNUMS]; }`. As in
/// [`MinmaxOpaque`] / [`InclusionOpaque`] each cached `FmgrInfo` is reduced to
/// the resolved function's `Oid` (the BRIN fmgr-call seam re-resolves by OID).
/// An `Oid` of `InvalidOid` (0) marks an uninitialized slot, exactly as
/// `palloc0` leaves it. The `Cell` gives interior mutability so the cache fills
/// lazily through the `&BrinDesc` the AM passes (C mutates the same struct
/// through a pointer).
#[derive(Debug, Default)]
pub struct BloomOpaque {
    /// `extra_procinfos[BLOOM_MAX_PROCNUMS]`: the resolved hash function `Oid`
    /// (`InvalidOid` marks an uninitialized slot).
    pub extra_procinfos: [core::cell::Cell<::types_core::primitive::Oid>; BLOOM_MAX_PROCNUMS],
}

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
    ///
    /// C carries this as a bare `Datum` pointing at an opclass-private expanded
    /// object. The only built-in opclass that uses it (`brin_minmax_multi`)
    /// keeps a live in-memory [`MinmaxMultiRanges`] struct across many
    /// `add_value` calls and serializes it once at `brin_form_tuple` time
    /// through the `bv_serialize` callback. Modeled here as a typed enum so the
    /// live struct can be named (mirrors [`OpaqueOpcInfo`]); the [`Datum`] arm
    /// keeps C's finished-datum case representable.
    pub bv_mem_value: Option<BrinMemValue<'mcx>>,
    /// Whether a `bv_serialize` opclass callback is registered for this column
    /// (`brin_serialize_callback_type`; `false` is the C NULL pointer). The
    /// callback itself is opclass-owned and invoked through the brin-tuple
    /// `brin_serialize` seam keyed by the column index.
    pub bv_has_serialize: bool,
}

/// Payload for [`BrinValues::bv_mem_value`] — the opclass-private expanded
/// accumulated value (C's `Datum bv_mem_value`). Modeled as a typed enum, one
/// arm per shape the built-in opclasses store (mirrors [`OpaqueOpcInfo`]); the
/// genuinely heterogeneous `void *` of an extension opclass is not used by the
/// built-ins.
#[derive(Debug)]
pub enum BrinMemValue<'mcx> {
    /// A finished by-value/by-reference `Datum` expanded object (the C
    /// `bv_mem_value = PointerGetDatum(x)` case for opclasses that store a
    /// plain expanded datum). Unused by the built-in opclasses, kept so the
    /// contract stays faithful to C.
    Datum(Datum<'mcx>),
    /// `brin_minmax_multi.c`: a live in-memory [`MinmaxMultiRanges`] insert
    /// buffer, accumulated across `add_value` and compacted/serialized once by
    /// the `bv_serialize` callback at `brin_form_tuple` time.
    MinmaxMultiRanges(MinmaxMultiRanges<'mcx>),
}

/// `Ranges` (`brin_minmax_multi.c`): the in-memory minmax-multi summary — an
/// oversized insert buffer of boundary values, accumulated across many
/// `add_value` calls and compacted to `target_maxvalues` once at serialize
/// time.
///
/// The `values` array stores `2*nranges` regular-range boundary values first,
/// then `nvalues` single-point values (`nsorted` of which are sorted). The
/// cached `FmgrInfo *cmp` is reduced to the comparison function's `Oid` (the
/// BRIN fmgr-call seam re-resolves by OID).
#[derive(Debug)]
pub struct MinmaxMultiRanges<'mcx> {
    /// `typid`: the indexed column's type Oid.
    pub typid: ::types_core::primitive::Oid,
    /// `colloid`: the collation Oid.
    pub colloid: ::types_core::primitive::Oid,
    /// `attno`: the indexed attribute number (1-based).
    pub attno: AttrNumber,
    /// `cmp`: the cached less-than comparison function `Oid` (`InvalidOid` when
    /// not yet resolved).
    pub cmp: ::types_core::primitive::Oid,
    /// `nranges`: number of regular ranges in `values`.
    pub nranges: i32,
    /// `nsorted`: number of `nvalues` point values that are sorted.
    pub nsorted: i32,
    /// `nvalues`: number of single-point values in `values`.
    pub nvalues: i32,
    /// `maxvalues`: number of elements allocated in `values` (the oversized
    /// insert-buffer capacity).
    pub maxvalues: i32,
    /// `target_maxvalues`: the requested (`values_per_range`) number of values
    /// to compact down to before serializing.
    pub target_maxvalues: i32,
    /// `values[]`: boundary values — `2*nranges` regular-range bounds followed
    /// by `nvalues` single-point values.
    pub values: PgVec<'mcx, Datum<'mcx>>,
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

// ---------------------------------------------------------------------------
// BrinStatsData (brin.h) — index statistics read from the metapage.
// ---------------------------------------------------------------------------

/// `BrinStatsData` (`brin.h`): the BRIN index statistics `brinGetStats` reads
/// from the metapage, used by `brincostestimate` (selfuncs.c).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BrinStatsData {
    /// `pagesPerRange`.
    pub pages_per_range: BlockNumber,
    /// `revmapNumPages`.
    pub revmap_num_pages: BlockNumber,
}
