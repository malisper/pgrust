//! Port of `src/backend/access/spgist/spgutils.c` (PostgreSQL 18.3): the
//! various support functions for SP-GiST.
//!
//! Scope (SP-GiST F0): `initSpGistState` / `spgGetCache` / `fillTypeDesc` /
//! `getSpGistTupleDesc` / `GetIndexInputType`, the page-management routines
//! (`SpGistInitPage` / `SpGistInitBuffer` / `SpGistInitMetapage` /
//! `SpGistNewBuffer` / `allocNewBuffer` / `SpGistGetBuffer` /
//! `SpGistSetLastUsedPage` / `SpGistUpdateMetaPage` / `SpGistPageAddNewItem`),
//! the index-tuple builders (`spgFormLeafTuple` / `spgFormInnerTuple` /
//! `spgFormNodeTuple` / `spgFormDeadTuple` / `spgDeformLeafTuple`), the inner
//! datum helpers (`SpGistGetInnerTypeSize` / `memcpyInnerDatum`),
//! `spgExtractNodeLabels`, `spgoptions`, and `spgproperty`.
//!
//! NOT in scope (later families): the `spghandler` IndexAmRoutine vtable (F6),
//! and the scan/insert/vacuum/validate code.
//!
//! ## Memory model
//!
//! The C `Relation` (an `index->rd_index`/`rd_opcintype`/... bundle) is the
//! owned [`Relation`](types_rel::Relation) value, deref'd to read its
//! `rd_index` / `rd_opcintype` / `rd_indcollation` / `rd_att` / `rd_rel`
//! fields directly (no per-field seam — those values are already projected onto
//! the relcache entry). The functions thread `&Relation<'mcx>` exactly where C
//! passes `Relation index`, even though [`SpGistState::index`](types_spgist::SpGistState)
//! stores only the relation OID (the modelled form of the C `Relation` pointer
//! it caches).
//!
//! The C on-disk tuple pointers (`SpGistLeafTuple` / `SpGistInnerTuple` /
//! `SpGistNodeTuple` / `SpGistDeadTuple`, all `palloc`'d byte buffers) become
//! owned byte images: the builders return [`mcx::PgVec`]`<u8>` (or, for
//! `spgFormDeadTuple`, write into `state.deadTupleStorage`). Each header struct
//! is `#[repr(C)]` in `types-spgist`, so the image is assembled by writing the
//! header bytes at offset 0 followed by the prefix/nodes/data area, matching the
//! C `palloc` + field-store + `memcpy` layout exactly.
//!
//! `Datum` is the owned [`types_tuple` value](types_tuple::backend_access_common_heaptuple::Datum):
//! the by-value arm is the raw machine word (C's `Datum`), the by-reference arm
//! is the verbatim on-disk bytes. `memcpyInnerDatum` writes the 8-byte word for
//! a pass-by-value type and the value bytes for a pass-by-reference type, as in
//! the C `memcpy(target, &datum, sizeof(Datum))` / `memcpy(target,
//! DatumGetPointer(datum), size)`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};

use backend_access_common_heaptuple::{heap_compute_data_size, heap_fill_tuple, varsize_any};
use backend_access_common_indextuple::index_deform_tuple_internal;
use backend_access_common_tupdesc::{populate_compact_attribute, CreateTupleDescCopy};
use backend_storage_page::{
    ItemPointerSet, ItemPointerSetInvalid, PageAddItemExtended, PageGetExactFreeSpace, PageGetItem,
    PageGetItemId, PageGetMaxOffsetNumber, PageIndexTupleDelete, PageInit, PageIsEmpty, PageIsNew,
    PageMut, PageRef,
};
use backend_utils_error::{ereport, PgError};
use types_error::error::{ERROR, PANIC};
use types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_error::PgResult;

use types_core::primitive::{
    AttrNumber, BlockNumber, ForkNumber, InvalidBlockNumber, InvalidOid, OffsetNumber, Oid, Size,
    BLCKSZ,
};
use types_rel::Relation;
use types_storage::buf::{Buffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{
    ItemPointerData, TupleDescData, INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
    FIRST_OFFSET_NUMBER as FirstOffsetNumber,
};

use types_spgist::{
    spgConfigIn, GBUF_INNER_PARITY, GBUF_LEAF, GBUF_NULLS, GBUF_PARITY_MASK, GBUF_REQ_LEAF,
    GBUF_REQ_NULLS, SpGistBlockIsFixed, SpGistCache, SpGistDeadTupleData, SpGistInnerTupleData,
    SpGistLastUsedPage, SpGistLeafTupleData, SpGistMetaPageData, SpGistNodeTupleData,
    SpGistPageOpaqueData, SpGistState, SpGistTypeDesc, SPGIST_CACHED_PAGES, SPGIST_COMPRESS_PROC,
    SPGIST_CONFIG_PROC, SPGIST_DEFAULT_FILLFACTOR, SPGIST_LEAF, SPGIST_MAGIC_NUMBER,
    SPGIST_META, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_PAGE_ID, SPGIST_PLACEHOLDER,
    SPGIST_REDIRECT, SGITMAXNNODES, SGITMAXPREFIXSIZE, SGITMAXSIZE, spgFirstIncludeColumn,
    spgKeyColumn,
};

// ===========================================================================
// MAXALIGN and the spgist_private.h size macros.
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
#[inline]
pub(crate) const fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}

/// `MAXALIGN_DOWN(x)` (c.h).
#[inline]
pub(crate) const fn MAXALIGN_DOWN(x: usize) -> usize {
    x & !7
}

/// `sizeof(SpGistInnerTupleData)` (8 bytes on a 64-bit build), the inner-tuple
/// header size already MAXALIGN'd.
const SIZEOF_SPGIST_INNER_TUPLE_DATA: usize = core::mem::size_of::<SpGistInnerTupleData>();
/// `sizeof(SpGistLeafTupleData)` (12 bytes), the leaf-tuple header.
pub(crate) const SIZEOF_SPGIST_LEAF_TUPLE_DATA: usize = core::mem::size_of::<SpGistLeafTupleData>();
/// `sizeof(SpGistNodeTupleData)` == `sizeof(IndexTupleData)` (8 bytes).
const SIZEOF_SPGIST_NODE_TUPLE_DATA: usize = core::mem::size_of::<SpGistNodeTupleData>();
/// `sizeof(SpGistDeadTupleData)` (16 bytes).
const SIZEOF_SPGIST_DEAD_TUPLE_DATA: usize = core::mem::size_of::<SpGistDeadTupleData>();
/// `sizeof(SpGistPageOpaqueData)` (8 bytes).
const SIZEOF_SPGIST_PAGE_OPAQUE_DATA: usize = core::mem::size_of::<SpGistPageOpaqueData>();
/// `sizeof(ItemIdData)` (4 bytes).
pub(crate) const SIZEOF_ITEM_ID_DATA: usize = 4;
/// `sizeof(Datum)` (8 bytes on a 64-bit build).
pub(crate) const SIZEOF_DATUM: usize = 8;
/// `sizeof(IndexAttributeBitMapData)` (`(INDEX_MAX_KEYS + 8 - 1) / 8` = 4 bytes
/// for `INDEX_MAX_KEYS == 32`).
const SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA: usize = 4;

/// `SGITHDRSZ` (spgist_private.h): `MAXALIGN(sizeof(SpGistInnerTupleData))`.
pub(crate) const SGITHDRSZ: usize = MAXALIGN(SIZEOF_SPGIST_INNER_TUPLE_DATA);
/// `SGDTSIZE` (spgist_private.h): `MAXALIGN(sizeof(SpGistDeadTupleData))`.
pub(crate) const SGDTSIZE: usize = MAXALIGN(SIZEOF_SPGIST_DEAD_TUPLE_DATA);

/// `SGLTHDRSZ(hasnulls)` (spgist_private.h):
/// `MAXALIGN(sizeof(SpGistLeafTupleData) + (hasnulls ?
/// sizeof(IndexAttributeBitMapData) : 0))`.
#[inline]
pub(crate) const fn SGLTHDRSZ(hasnulls: bool) -> usize {
    if hasnulls {
        MAXALIGN(SIZEOF_SPGIST_LEAF_TUPLE_DATA + SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA)
    } else {
        MAXALIGN(SIZEOF_SPGIST_LEAF_TUPLE_DATA)
    }
}

/// `SPGIST_PAGE_CAPACITY` (spgist_private.h):
/// `MAXALIGN_DOWN(BLCKSZ - SizeOfPageHeaderData -
/// MAXALIGN(sizeof(SpGistPageOpaqueData)))`.
pub(crate) const SPGIST_PAGE_CAPACITY: usize = MAXALIGN_DOWN(
    BLCKSZ as usize - SizeOfPageHeaderData as usize - MAXALIGN(SIZEOF_SPGIST_PAGE_OPAQUE_DATA),
);

/// `INDEX_SIZE_MASK` (itup.h): the 13 bits of `t_info` that hold the index
/// tuple size.
pub(crate) const INDEX_SIZE_MASK: u16 = 0x1FFF;
/// `INDEX_NULL_MASK` (itup.h): the `t_info` bit marking a NULL index attribute.
pub(crate) const INDEX_NULL_MASK: u16 = 0x8000;

// ===========================================================================
// Page-opaque / metapage byte accessors against the BLCKSZ page bytes.
//
// The SP-GiST special area (`SpGistPageOpaqueData`, 8 bytes) occupies the last
// MAXALIGN-sized element of the page: `[BLCKSZ - 8 .. BLCKSZ]`. The metapage's
// `SpGistMetaPageData` starts at `PageGetContents` = MAXALIGN(SizeOfPageHeaderData).
// ===========================================================================

/// Byte offset of the `SpGistPageOpaqueData` special area on a BLCKSZ page.
const OPAQUE_OFFSET: usize = BLCKSZ as usize - MAXALIGN(SIZEOF_SPGIST_PAGE_OPAQUE_DATA);
/// Byte offset of `pd_lower` within `PageHeaderData` (the uint16 at offset 12).
const OFF_PD_LOWER: usize = 12;
/// `PageGetContents(page)` offset: the MAXALIGN'd page-header size, where
/// `SpGistMetaPageData` (`SpGistPageGetMeta`) starts.
const META_OFFSET: usize = MAXALIGN(SizeOfPageHeaderData as usize);

/// `SpGistPageGetOpaque(page)->flags` (read).
#[inline]
pub(crate) fn opaque_flags(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OPAQUE_OFFSET], page[OPAQUE_OFFSET + 1]])
}

/// `SpGistPageIsLeaf(page)` (spgist_private.h): `flags & SPGIST_LEAF`.
#[inline]
pub(crate) fn SpGistPageIsLeaf(page: &[u8]) -> bool {
    opaque_flags(page) & SPGIST_LEAF != 0
}

/// `SpGistPageStoresNulls(page)`: `flags & SPGIST_NULLS`.
#[inline]
pub(crate) fn SpGistPageStoresNulls(page: &[u8]) -> bool {
    opaque_flags(page) & SPGIST_NULLS != 0
}

/// `SpGistPageIsDeleted(page)`: `flags & SPGIST_DELETED`.
#[inline]
pub(crate) fn SpGistPageIsDeleted(page: &[u8]) -> bool {
    opaque_flags(page) & types_spgist::SPGIST_DELETED != 0
}

/// `SpGistPageGetOpaque(page)->nPlaceholder` (read).
#[inline]
pub(crate) fn opaque_n_placeholder(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OPAQUE_OFFSET + 4], page[OPAQUE_OFFSET + 5]])
}

/// `SpGistPageGetOpaque(page)->nPlaceholder = v`.
#[inline]
pub(crate) fn set_opaque_n_placeholder(page: &mut [u8], v: u16) {
    page[OPAQUE_OFFSET + 4..OPAQUE_OFFSET + 6].copy_from_slice(&v.to_ne_bytes());
}

// ===========================================================================
// fillTypeDesc / GetIndexInputType
// ===========================================================================

/// `IsPolymorphicType(typid)` (pg_type.h) — is `typid` a polymorphic
/// pseudotype? Inlined here (the C macro), as the parser's copy is private.
#[inline]
fn IsPolymorphicType(typid: Oid) -> bool {
    use types_tuple::heaptuple::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID,
        ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYNONARRAYOID, ANYRANGEOID,
    };
    // IsPolymorphicTypeFamily1 ∪ IsPolymorphicTypeFamily2 (pg_type.h). The
    // multirange members are part of family 1/2; reference them through the
    // type-OID module if present, else fall back to the literal OIDs.
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYMULTIRANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `ANYMULTIRANGEOID` (pg_type.dat OID 4451).
const ANYMULTIRANGEOID: Oid = 4451;
/// `ANYCOMPATIBLEMULTIRANGEOID` (pg_type.dat OID 4538).
const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

/// `GetIndexInputType(index, indexcol)` (spgutils.c:120) — the nominal input
/// data type for an index column.
///
/// `indexcol` is 1-based (the C `AttrNumber`). The polymorphic + EXPRESSION-key
/// branch needs `RelationGetIndexExpressions` (unseamed: relcache `derived.rs`
/// builds the cached expression list); it is reached through the
/// `get_index_input_type_expr` owner seam (in `spg-core-seams`) and panics
/// until relcache installs it. SP-GiST has exactly one key column, so the simple-column
/// polymorphic path (`indkey[indexcol-1] != 0`) covers every supported case.
fn GetIndexInputType(index: &Relation<'_>, indexcol: AttrNumber) -> PgResult<Oid> {
    let rd_index = index
        .rd_index
        .as_ref()
        .expect("GetIndexInputType: index->rd_index is NULL");
    debug_assert!(indexcol > 0 && indexcol <= rd_index.indnkeyatts as AttrNumber);

    let opcintype = index.rd_opcintype[(indexcol - 1) as usize];
    if !IsPolymorphicType(opcintype) {
        return Ok(opcintype);
    }

    // SP-GiST has a single key column; the first (and only) key column's heap
    // column number is `indkey0`. A nonzero value is a simple index column.
    let heapcol = if indexcol == 1 {
        rd_index.indkey0
    } else {
        // SP-GiST never reaches here (indnkeyatts == 1), but mirror the C: the
        // trimmed FormData_pg_index only carries indkey0, so any further column
        // would be an expression-key lookup we cannot perform without the full
        // indkey vector — which only matters for the unsupported multi-key case.
        0
    };

    if heapcol != 0 {
        // Simple index column.
        let atttype =
            backend_utils_cache_lsyscache_seams::get_atttype::call(rd_index.indrelid, heapcol)?;
        return backend_utils_cache_lsyscache_seams::get_base_type::call(atttype);
    }

    // Expression column: needs RelationGetIndexExpressions + exprType, reached
    // through the owner seam (relcache/plancat install it later).
    backend_access_spg_core_seams::get_index_input_type_expr::call(index.rd_id, indexcol)
}

/// `fillTypeDesc(desc, type)` (spgutils.c:165) — fill a [`SpGistTypeDesc`] with
/// info about the specified data type.
fn fillTypeDesc(type_oid: Oid) -> PgResult<SpGistTypeDesc> {
    let tp = backend_utils_cache_lsyscache_seams::lookup_pg_type::call(type_oid)?
        .ok_or_else(|| elog_internal(ERROR, format!("cache lookup failed for type {type_oid}")))?;
    Ok(SpGistTypeDesc {
        type_: type_oid,
        attlen: tp.typlen,
        attbyval: tp.typbyval,
        attalign: tp.typalign,
        attstorage: tp.typstorage,
    })
}

// ===========================================================================
// spgGetCache
// ===========================================================================

/// `spgGetCache(index)` (spgutils.c:187) — fetch the local cache of AM-specific
/// info about the index, initializing it if necessary.
///
/// The C `SpGistCache *` returned (a pointer into `index->rd_amcache`) is
/// modelled as the owned [`SpGistCache`] value: it is read from the relcache
/// entry's `rd_amcache` slot via [`rd_amcache_spgist`], and rebuilt + installed
/// via [`set_rd_amcache_spgist`] when absent. The value is `Copy`, so callers
/// receive a snapshot — the lastUsedPages updates the page-management routines
/// perform are written back through `set_rd_amcache_spgist` (mirroring the C
/// in-place mutation of the cached struct).
pub fn spgGetCache<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<SpGistCache> {
    if let Some(cache) = backend_utils_cache_relcache_seams::rd_amcache_spgist::call(index.rd_id)? {
        // assume it's up to date
        return Ok(cache);
    }

    let mut cache = SpGistCache::default();

    let rd_index = index
        .rd_index
        .as_ref()
        .expect("spgGetCache: index->rd_index is NULL");
    // SPGiST must have one key column and can also have INCLUDE columns.
    debug_assert_eq!(rd_index.indnkeyatts, 1);

    // Get the actual (well, nominal) data type of the key column. We pass this
    // to the opclass config function so that polymorphic opclasses are possible.
    let atttype = GetIndexInputType(index, spgKeyColumn as AttrNumber + 1)?;

    // Call the config function to get config info for the opclass.
    let cfgin = spgConfigIn { attType: atttype };
    let config_proc_oid =
        backend_utils_cache_relcache_seams::index_getprocid::call(index, 1, SPGIST_CONFIG_PROC as u16)?;
    backend_access_spg_core_seams::spg_config::call(config_proc_oid, &cfgin, &mut cache.config)?;

    // If leafType isn't specified, use the declared index column type, which
    // index.c will have derived from the opclass's opcintype.
    if !OidIsValid(cache.config.leafType) {
        cache.config.leafType = index.rd_att.attr(spgKeyColumn as usize).atttypid;

        // If index column type is binary-coercible to atttype (for example, it's
        // a domain over atttype), treat it as plain atttype to avoid thinking we
        // need to compress.
        if cache.config.leafType != atttype
            && backend_parser_coerce_seams::is_binary_coercible::call(
                cache.config.leafType,
                atttype,
            )?
        {
            cache.config.leafType = atttype;
        }
    }

    // Get the information we need about each relevant datatype.
    cache.attType = fillTypeDesc(atttype)?;

    if cache.config.leafType != atttype {
        if !OidIsValid(backend_utils_cache_relcache_seams::index_getprocid::call(
            index,
            1,
            SPGIST_COMPRESS_PROC as u16,
        )?) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(
                    "compress method must be defined when leaf type is different from input type",
                )
                .into_error());
        }

        cache.attLeafType = fillTypeDesc(cache.config.leafType)?;
    } else {
        // Save lookups in this common case.
        cache.attLeafType = cache.attType;
    }

    cache.attPrefixType = fillTypeDesc(cache.config.prefixType)?;
    cache.attLabelType = fillTypeDesc(cache.config.labelType)?;

    // Finally, if it's a real index (not a partitioned one), get the
    // lastUsedPages data from the metapage.
    if index.rd_rel.relkind != types_tuple::access::RELKIND_PARTITIONED_INDEX {
        let metabuffer =
            backend_storage_buffer_bufmgr_seams::read_buffer::call(index, SPGIST_METAPAGE_BLKNO)?;
        backend_storage_buffer_bufmgr_seams::lock_buffer::call(metabuffer, BUFFER_LOCK_SHARE)?;

        let metapage = backend_storage_buffer_bufmgr_seams::buffer_get_page::call(mcx, metabuffer)?;
        let metadata = read_meta(&metapage);

        if metadata.magicNumber != SPGIST_MAGIC_NUMBER {
            backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(metabuffer);
            return Err(elog_internal(
                ERROR,
                format!("index \"{}\" is not an SP-GiST index", index.name()),
            ));
        }

        cache.lastUsedPages = metadata.lastUsedPages;

        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(metabuffer);
    }

    // index->rd_amcache = cache (install on the relcache entry).
    backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;

    Ok(cache)
}

/// `SpGistPageGetMeta(page)` — read the `SpGistMetaPageData` out of the page
/// content bytes (magicNumber + the per-class lastUsedPages cache).
fn read_meta(page: &[u8]) -> SpGistMetaPageData {
    let mut m = SpGistMetaPageData::default();
    let mut off = META_OFFSET;
    m.magicNumber = u32::from_ne_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]]);
    off += 4; // magicNumber (4) — lastUsedPages starts here (no padding: align 4)
    for i in 0..SPGIST_CACHED_PAGES {
        let blkno =
            BlockNumber::from_ne_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]]);
        let free = i32::from_ne_bytes([
            page[off + 4],
            page[off + 5],
            page[off + 6],
            page[off + 7],
        ]);
        m.lastUsedPages.cachedPage[i] = SpGistLastUsedPage {
            blkno,
            freeSpace: free,
        };
        off += 8;
    }
    m
}

/// `SpGistPageGetMeta(page)` mutate: write the `SpGistMetaPageData` into the
/// page content bytes and set `pd_lower` just past the metadata (so xlog.c page
/// compression keeps the metadata).
fn write_meta(page: &mut [u8], m: &SpGistMetaPageData) {
    let mut off = META_OFFSET;
    page[off..off + 4].copy_from_slice(&m.magicNumber.to_ne_bytes());
    off += 4;
    for i in 0..SPGIST_CACHED_PAGES {
        let slot = &m.lastUsedPages.cachedPage[i];
        page[off..off + 4].copy_from_slice(&slot.blkno.to_ne_bytes());
        page[off + 4..off + 8].copy_from_slice(&slot.freeSpace.to_ne_bytes());
        off += 8;
    }
    // ((PageHeader) page)->pd_lower = (metadata + sizeof(SpGistMetaPageData)) - page.
    let pd_lower = (META_OFFSET + core::mem::size_of::<SpGistMetaPageData>()) as u16;
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&pd_lower.to_ne_bytes());
}

// ===========================================================================
// getSpGistTupleDesc / initSpGistState
// ===========================================================================

/// `getSpGistTupleDesc(index, keyType)` (spgutils.c:314) — compute a tuple
/// descriptor for leaf tuples whose key column matches `keyType`.
///
/// Returns either a clone of the relcache descriptor (when the key column type
/// already matches), or a palloc'd copy adjusted to the key type. We always
/// return an owned [`TupleDescData`] (the C "pointer to the relcache tupdesc"
/// case becomes a clone — `SpGistState.leafTupDesc` owns its descriptor).
pub fn getSpGistTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    keyType: &SpGistTypeDesc,
) -> PgResult<TupleDescData<'mcx>> {
    if keyType.type_ == index.rd_att.attr(spgKeyColumn as usize).atttypid {
        // The relcache's tupdesc is suitable as-is; clone it into mcx (the
        // owned model can't alias the relcache descriptor by pointer).
        return index.rd_att.clone_in(mcx);
    }

    let mut out_tup_desc = CreateTupleDescCopy(mcx, &index.rd_att)?;
    {
        let att = out_tup_desc.attr_mut(spgKeyColumn as usize);
        // It's sufficient to update the type-dependent fields of the column.
        att.atttypid = keyType.type_;
        att.atttypmod = -1;
        att.attlen = keyType.attlen;
        att.attbyval = keyType.attbyval;
        att.attalign = keyType.attalign;
        att.attstorage = keyType.attstorage;
        // We shouldn't need to bother with making these valid:
        att.attcompression = types_tuple::heaptuple::InvalidCompressionMethod;
        att.attcollation = InvalidOid;
    }
    // In case we changed typlen, we'd better reset following offsets.
    let natts = out_tup_desc.natts as usize;
    for i in spgFirstIncludeColumn as usize..natts {
        out_tup_desc.compact_attrs[i].attcacheoff = -1;
    }

    populate_compact_attribute(&mut out_tup_desc, spgKeyColumn as usize)?;

    Ok(out_tup_desc)
}

/// `initSpGistState(state, index)` (spgutils.c:347) — initialize a
/// [`SpGistState`] for working with the given index.
pub fn initSpGistState<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<SpGistState<'mcx>> {
    // Get cached static information about index.
    let cache = spgGetCache(mcx, index)?;

    // Ensure we have a valid descriptor for leaf tuples.
    let leaf_tup_desc = getSpGistTupleDesc(mcx, index, &cache.attLeafType)?;

    // Make workspace for constructing dead tuples (palloc0(SGDTSIZE)).
    let dead_tuple_storage = alloc::vec![0u8; SGDTSIZE];

    // Set horizon XID to use in redirection tuples. Use our own XID if we have
    // one, else InvalidTransactionId.
    let redirect_xid = backend_access_transam_xact_seams::get_top_transaction_id_if_any::call();

    Ok(SpGistState {
        index: index.rd_id,
        config: cache.config,
        attType: cache.attType,
        attLeafType: cache.attLeafType,
        attPrefixType: cache.attPrefixType,
        attLabelType: cache.attLabelType,
        leafTupDesc: Some(mcx::alloc_in(mcx, leaf_tup_desc)?),
        deadTupleStorage: Some(dead_tuple_storage),
        redirectXid: redirect_xid,
        // Assume we're not in an index build (spgbuild will override).
        isBuild: false,
    })
}

// ===========================================================================
// Buffer / page management
// ===========================================================================

/// `SpGistNewBuffer(index)` (spgutils.c:393) — allocate a new page (by
/// recycling, or by extending the index file). Returns a pinned,
/// exclusive-locked buffer; caller initializes the page via `SpGistInitBuffer`.
pub fn SpGistNewBuffer<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<Buffer> {
    // First, try to get a page from FSM.
    loop {
        let blkno = backend_storage_freespace_seams::get_free_index_page::call(index)?;

        if blkno == InvalidBlockNumber {
            break; // nothing known to FSM
        }

        // The fixed pages shouldn't ever be listed in FSM, but just in case one
        // is, ignore it.
        if SpGistBlockIsFixed(blkno) {
            continue;
        }

        let buffer = backend_storage_buffer_bufmgr_seams::read_buffer::call(index, blkno)?;

        // We have to guard against the possibility that someone else already
        // recycled this page; the buffer may be locked if so.
        if backend_storage_buffer_bufmgr_seams::conditional_lock_buffer::call(buffer)? {
            let page = backend_storage_buffer_bufmgr_seams::buffer_get_page::call(mcx, buffer)?;

            let page_ref = PageRef::new(&page)?;
            if PageIsNew(&page_ref) {
                return Ok(buffer); // OK to use, if never initialized
            }

            if SpGistPageIsDeleted(&page) || PageIsEmpty(&page_ref) {
                return Ok(buffer); // OK to use
            }

            backend_storage_buffer_bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
        }

        // Can't use it, so release buffer and try again.
        backend_storage_buffer_bufmgr_seams::release_buffer::call(buffer);
    }

    let buffer = backend_storage_buffer_bufmgr_seams::extend_buffered_rel::call(
        index,
        ForkNumber::MAIN_FORKNUM,
    )?;

    Ok(buffer)
}

/// `SpGistUpdateMetaPage(index)` (spgutils.c:449) — update the index metapage's
/// `lastUsedPages` info from the local cache, if the conditional lock succeeds.
pub fn SpGistUpdateMetaPage<'mcx>(index: &Relation<'mcx>) -> PgResult<()> {
    let cache = match backend_utils_cache_relcache_seams::rd_amcache_spgist::call(index.rd_id)? {
        Some(c) => c,
        None => return Ok(()),
    };

    let metabuffer =
        backend_storage_buffer_bufmgr_seams::read_buffer::call(index, SPGIST_METAPAGE_BLKNO)?;

    if backend_storage_buffer_bufmgr_seams::conditional_lock_buffer::call(metabuffer)? {
        backend_storage_buffer_bufmgr_seams::with_buffer_page::call(
            metabuffer,
            &mut |metapage: &mut [u8]| {
                let mut metadata = read_meta(metapage);
                metadata.lastUsedPages = cache.lastUsedPages;
                // write_meta also sets pd_lower just past the metadata.
                write_meta(metapage, &metadata);
                Ok(())
            },
        )?;

        backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(metabuffer);
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(metabuffer);
    } else {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(metabuffer);
    }
    Ok(())
}

/// `GET_LUP(cache, flags)` index (spgutils.c:490): the cache slot for `flags`,
/// masked with `SPGIST_CACHED_PAGES` for paranoia's sake.
#[inline]
pub(crate) fn get_lup_index(flags: i32) -> usize {
    (flags as u32 as usize) % SPGIST_CACHED_PAGES
}

/// `allocNewBuffer(index, flags)` (spgutils.c:512) — allocate and initialize a
/// new buffer of the type/parity specified by `flags`. The lastUsedPages cache
/// (`cache`) is updated for wrong-parity inner pages and written back.
fn allocNewBuffer<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    flags: i32,
    cache: &mut SpGistCache,
) -> PgResult<Buffer> {
    let mut pageflags: u16 = 0;
    if GBUF_REQ_LEAF(flags) {
        pageflags |= SPGIST_LEAF;
    }
    if GBUF_REQ_NULLS(flags) {
        pageflags |= SPGIST_NULLS;
    }

    loop {
        let buffer = SpGistNewBuffer(mcx, index)?;
        SpGistInitBuffer(buffer, pageflags)?;

        if pageflags & SPGIST_LEAF != 0 {
            // Leaf pages have no parity concerns, so just use it.
            return Ok(buffer);
        }

        let blkno = backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buffer);
        let mut blk_flags = GBUF_INNER_PARITY(blkno);

        if (flags & GBUF_PARITY_MASK) == blk_flags {
            // Page has right parity, use it.
            return Ok(buffer);
        }

        // Page has wrong parity, record it in cache and try again.
        if pageflags & SPGIST_NULLS != 0 {
            blk_flags |= GBUF_NULLS;
        }
        let free_space = {
            let page = backend_storage_buffer_bufmgr_seams::buffer_get_page::call(mcx, buffer)?;
            PageGetExactFreeSpace(&PageRef::new(&page)?)
        };
        cache.lastUsedPages.cachedPage[blk_flags as usize].blkno = blkno;
        cache.lastUsedPages.cachedPage[blk_flags as usize].freeSpace = free_space as i32;
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buffer);
    }
}

/// `SpGistGetTargetPageFreeSpace(index)` (spgist_private.h):
/// `BLCKSZ * (100 - RelationGetFillFactor(index, SPGIST_DEFAULT_FILLFACTOR)) / 100`.
#[inline]
pub(crate) fn SpGistGetTargetPageFreeSpace(index: &Relation<'_>) -> usize {
    BLCKSZ as usize * (100 - index.get_fillfactor(SPGIST_DEFAULT_FILLFACTOR) as usize) / 100
}

/// `SpGistGetBuffer(index, flags, needSpace, isNew)` (spgutils.c:568) — get a
/// buffer of the type/parity specified by `flags` with at least `needSpace`
/// free, reusing the lastUsedPages cache when possible. Returns the buffer and
/// `isNew` (true if the page was initialized here).
///
/// The caller-visible C in/out `cache` mutation rides through the
/// `rd_amcache_spgist` read + `set_rd_amcache_spgist` write-back: the cache is
/// loaded, mutated in place, and stored before return.
pub fn SpGistGetBuffer<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    flags: i32,
    mut need_space: i32,
    is_new: &mut bool,
) -> PgResult<Buffer> {
    // spgGetCache installs the cache if absent; we mutate our snapshot and
    // write it back at each return point that touched lastUsedPages.
    let mut cache = spgGetCache(mcx, index)?;

    // Bail out if even an empty page wouldn't meet the demand.
    if need_space as usize > SPGIST_PAGE_CAPACITY {
        return Err(elog_internal(
            ERROR,
            "desired SPGiST tuple size is too big".into(),
        ));
    }

    // If possible, increase the space request to include relation's fillfactor.
    need_space += SpGistGetTargetPageFreeSpace(index) as i32;
    need_space = need_space.min(SPGIST_PAGE_CAPACITY as i32);

    // Get the cache entry for this flags setting.
    let lup_idx = get_lup_index(flags);

    // If we have nothing cached, just turn it over to allocNewBuffer.
    if cache.lastUsedPages.cachedPage[lup_idx].blkno == InvalidBlockNumber {
        *is_new = true;
        let buffer = allocNewBuffer(mcx, index, flags, &mut cache)?;
        backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;
        return Ok(buffer);
    }

    // fixed pages should never be in cache.
    debug_assert!(!SpGistBlockIsFixed(cache.lastUsedPages.cachedPage[lup_idx].blkno));

    // If cached freeSpace isn't enough, don't bother looking at the page.
    if cache.lastUsedPages.cachedPage[lup_idx].freeSpace >= need_space {
        let blkno = cache.lastUsedPages.cachedPage[lup_idx].blkno;
        let buffer = backend_storage_buffer_bufmgr_seams::read_buffer::call(index, blkno)?;

        if !backend_storage_buffer_bufmgr_seams::conditional_lock_buffer::call(buffer)? {
            // buffer is locked by another process, so return a new buffer.
            backend_storage_buffer_bufmgr_seams::release_buffer::call(buffer);
            *is_new = true;
            let nb = allocNewBuffer(mcx, index, flags, &mut cache)?;
            backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;
            return Ok(nb);
        }

        let page = backend_storage_buffer_bufmgr_seams::buffer_get_page::call(mcx, buffer)?;
        let page_ref = PageRef::new(&page)?;

        if PageIsNew(&page_ref) || SpGistPageIsDeleted(&page) || PageIsEmpty(&page_ref) {
            // OK to initialize the page.
            let mut pageflags: u16 = 0;
            if GBUF_REQ_LEAF(flags) {
                pageflags |= SPGIST_LEAF;
            }
            if GBUF_REQ_NULLS(flags) {
                pageflags |= SPGIST_NULLS;
            }
            SpGistInitBuffer(buffer, pageflags)?;
            let free = PageGetExactFreeSpace(&page_ref) as i32;
            cache.lastUsedPages.cachedPage[lup_idx].freeSpace = free - need_space;
            *is_new = true;
            backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;
            return Ok(buffer);
        }

        // Check that page is of right type and has enough space. We must recheck
        // this since our cache isn't necessarily up to date.
        let type_ok = if GBUF_REQ_LEAF(flags) {
            SpGistPageIsLeaf(&page)
        } else {
            !SpGistPageIsLeaf(&page)
        };
        let nulls_ok = if GBUF_REQ_NULLS(flags) {
            SpGistPageStoresNulls(&page)
        } else {
            !SpGistPageStoresNulls(&page)
        };
        if type_ok && nulls_ok {
            let free_space = PageGetExactFreeSpace(&page_ref) as i32;
            if free_space >= need_space {
                // Success, update freespace info and return the buffer.
                cache.lastUsedPages.cachedPage[lup_idx].freeSpace = free_space - need_space;
                *is_new = false;
                backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(
                    index.rd_id,
                    cache,
                )?;
                return Ok(buffer);
            }
        }

        // fallback to allocation of new buffer.
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buffer);
    }

    // No success with cache, so return a new buffer.
    *is_new = true;
    let buffer = allocNewBuffer(mcx, index, flags, &mut cache)?;
    backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;
    Ok(buffer)
}

/// `SpGistSetLastUsedPage(index, buffer)` (spgutils.c:672) — update the
/// lastUsedPages cache when done modifying a page.
pub fn SpGistSetLastUsedPage<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    let mut cache = spgGetCache(mcx, index)?;

    let blkno = backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buffer);

    // Never enter fixed pages (root pages) in cache, though.
    if SpGistBlockIsFixed(blkno) {
        return Ok(());
    }

    let page = backend_storage_buffer_bufmgr_seams::buffer_get_page::call(mcx, buffer)?;

    let mut flags = if SpGistPageIsLeaf(&page) {
        GBUF_LEAF
    } else {
        GBUF_INNER_PARITY(blkno)
    };
    if SpGistPageStoresNulls(&page) {
        flags |= GBUF_NULLS;
    }

    let lup_idx = get_lup_index(flags);
    let free_space = PageGetExactFreeSpace(&PageRef::new(&page)?) as i32;

    let lup = &mut cache.lastUsedPages.cachedPage[lup_idx];
    if lup.blkno == InvalidBlockNumber || lup.blkno == blkno || lup.freeSpace < free_space {
        lup.blkno = blkno;
        lup.freeSpace = free_space;
        backend_utils_cache_relcache_seams::set_rd_amcache_spgist::call(index.rd_id, cache)?;
    }
    Ok(())
}

/// `SpGistInitPage(page, f)` (spgutils.c:707) — initialize an SP-GiST page to
/// empty, with the specified flags, against the raw page bytes.
pub fn SpGistInitPage(page: &mut [u8], f: u16) -> PgResult<()> {
    PageInit(page, BLCKSZ as Size, SIZEOF_SPGIST_PAGE_OPAQUE_DATA)?;
    // opaque->flags = f; opaque->spgist_page_id = SPGIST_PAGE_ID;
    page[OPAQUE_OFFSET..OPAQUE_OFFSET + 2].copy_from_slice(&f.to_ne_bytes());
    // nRedirection (2) + nPlaceholder (2) were zeroed by PageInit's special-area
    // clear; spgist_page_id is at offset 6 in the opaque.
    page[OPAQUE_OFFSET + 6..OPAQUE_OFFSET + 8].copy_from_slice(&SPGIST_PAGE_ID.to_ne_bytes());
    Ok(())
}

/// `SpGistInitBuffer(b, f)` (spgutils.c:721) — initialize a buffer's page to
/// empty, with the specified flags.
pub fn SpGistInitBuffer(b: Buffer, f: u16) -> PgResult<()> {
    backend_storage_buffer_bufmgr_seams::with_buffer_page::call(b, &mut |page: &mut [u8]| {
        SpGistInitPage(page, f)
    })
}

/// `SpGistInitMetapage(page)` (spgutils.c:731) — initialize the metadata page
/// against the raw page bytes.
pub fn SpGistInitMetapage(page: &mut [u8]) -> PgResult<()> {
    SpGistInitPage(page, SPGIST_META)?;
    let mut metadata = SpGistMetaPageData {
        magicNumber: SPGIST_MAGIC_NUMBER,
        ..SpGistMetaPageData::default()
    };
    // initialize last-used-page cache to empty.
    for i in 0..SPGIST_CACHED_PAGES {
        metadata.lastUsedPages.cachedPage[i].blkno = InvalidBlockNumber;
    }
    // write_meta also sets pd_lower just past the metadata.
    write_meta(page, &metadata);
    Ok(())
}

// ===========================================================================
// spgoptions
// ===========================================================================

/// `spgoptions(reloptions, validate)` (spgutils.c:758) — reloptions processing
/// for SP-GiST (the only option is `fillfactor`). Delegates to the reloptions
/// owner's `build_reloptions_spgist` seam.
pub fn spgoptions(reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<Vec<u8>>> {
    backend_access_common_reloptions_seams::build_reloptions_spgist::call(reloptions, validate)
}

// ===========================================================================
// Inner-datum helpers + tuple builders
// ===========================================================================

/// `SpGistGetInnerTypeSize(att, datum)` (spgutils.c:778) — the MAXALIGN'd space
/// needed to store a non-null datum of the given type in an inner tuple (prefix
/// or node label). Pass-by-val types are stored in their Datum representation.
pub fn SpGistGetInnerTypeSize(att: &SpGistTypeDesc, datum: &Datum<'_>) -> usize {
    let size = if att.attbyval {
        SIZEOF_DATUM
    } else if att.attlen > 0 {
        att.attlen as usize
    } else {
        varsize_any(datum.as_ref_bytes())
    };
    MAXALIGN(size)
}

/// `memcpyInnerDatum(target, att, datum)` (spgutils.c:796) — copy the given
/// non-null datum to `target`, in the inner-tuple case.
pub(crate) fn memcpyInnerDatum(target: &mut [u8], att: &SpGistTypeDesc, datum: &Datum<'_>) {
    if att.attbyval {
        // memcpy(target, &datum, sizeof(Datum)) — the raw machine word.
        target[..SIZEOF_DATUM].copy_from_slice(&datum.as_usize().to_ne_bytes());
    } else {
        let size = if att.attlen > 0 {
            att.attlen as usize
        } else {
            varsize_any(datum.as_ref_bytes())
        };
        target[..size].copy_from_slice(&datum.as_ref_bytes()[..size]);
    }
}

/// `SpGistGetLeafTupleSize(tupleDescriptor, datums, isnulls)` (spgutils.c:817)
/// — compute the space required for a leaf tuple holding the given data.
pub fn SpGistGetLeafTupleSize(
    tuple_descriptor: &TupleDescData<'_>,
    datums: &[Datum<'_>],
    isnulls: &[bool],
) -> PgResult<Size> {
    let natts = tuple_descriptor.natts as usize;

    // Decide whether we need a nulls bitmask. If there is only a key attribute
    // (natts == 1), never use a bitmask; otherwise need one if any attr is null.
    let mut needs_null_mask = false;
    if natts > 1 {
        for &isnull in isnulls.iter().take(natts) {
            if isnull {
                needs_null_mask = true;
                break;
            }
        }
    }

    // Calculate size of the data part; same as for heap tuples.
    let data_size = heap_compute_data_size(tuple_descriptor, datums, isnulls)?;

    // Compute total size.
    let mut size = SGLTHDRSZ(needs_null_mask);
    size += data_size;
    size = MAXALIGN(size);

    // Ensure that we can replace the tuple with a dead tuple later.
    if size < SGDTSIZE {
        size = SGDTSIZE;
    }

    Ok(size)
}

/// `spgFormLeafTuple(state, heapPtr, datums, isnulls)` (spgutils.c:870) —
/// construct a leaf tuple containing the given heap TID and datum values.
/// Returns the owned on-disk byte image.
pub fn spgFormLeafTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    heap_ptr: &ItemPointerData,
    datums: &[Datum<'_>],
    isnulls: &[bool],
) -> PgResult<PgVec<'mcx, u8>> {
    let tuple_descriptor: &TupleDescData<'_> = state
        .leafTupDesc
        .as_ref()
        .expect("spgFormLeafTuple: state->leafTupDesc is NULL");
    let natts = tuple_descriptor.natts as usize;

    // Decide whether we need a nulls bitmask.
    let mut needs_null_mask = false;
    if natts > 1 {
        for &isnull in isnulls.iter().take(natts) {
            if isnull {
                needs_null_mask = true;
                break;
            }
        }
    }

    // Calculate size of the data part; same as for heap tuples.
    let data_size = heap_compute_data_size(tuple_descriptor, datums, isnulls)?;

    // Compute total size.
    let hoff = SGLTHDRSZ(needs_null_mask);
    let mut size = hoff + data_size;
    size = MAXALIGN(size);

    // Ensure that we can replace the tuple with a dead tuple later.
    if size < SGDTSIZE {
        size = SGDTSIZE;
    }

    // OK, form the tuple (palloc0).
    let mut tup = vec_with_capacity_in(mcx, size)?;
    tup.resize(size, 0u8);

    // Build the SpGistLeafTupleData header.
    let mut header = SpGistLeafTupleData::default();
    header.set_size(size as u32);
    header.set_nextOffset(InvalidOffsetNumber); // SGLT_SET_NEXTOFFSET
    header.heapPtr = *heap_ptr;

    if needs_null_mask {
        // Set nullmask presence bit in SpGistLeafTuple header.
        header.set_hasNullMask(true);
        // Fill the data area and null mask.
        let filled = heap_fill_tuple(mcx, tuple_descriptor, datums, isnulls, data_size, true)?;
        // tp = (char *) tup + hoff;
        tup[hoff..hoff + data_size].copy_from_slice(&filled.data);
        // bp = (bits8 *) ((char *) tup + sizeof(SpGistLeafTupleData));
        let bp_off = SIZEOF_SPGIST_LEAF_TUPLE_DATA;
        tup[bp_off..bp_off + filled.bits.len()].copy_from_slice(&filled.bits);
    } else if natts > 1 || !isnulls[spgKeyColumn as usize] {
        // Fill data area only.
        let filled = heap_fill_tuple(mcx, tuple_descriptor, datums, isnulls, data_size, false)?;
        tup[hoff..hoff + data_size].copy_from_slice(&filled.data);
    }
    // otherwise we have no data, nor a bitmap, to fill.

    // Write the header bytes at offset 0.
    write_leaf_header(&mut tup, &header);

    Ok(tup)
}

/// Serialize a [`SpGistLeafTupleData`] header into the first 12 bytes of `tup`.
pub(crate) fn write_leaf_header(tup: &mut [u8], header: &SpGistLeafTupleData) {
    tup[0..4].copy_from_slice(&header.bits.to_ne_bytes());
    tup[4..6].copy_from_slice(&header.t_info.to_ne_bytes());
    write_item_pointer(&mut tup[6..12], &header.heapPtr);
}

/// `spgFormNodeTuple(state, label, isnull)` (spgutils.c:959) — construct a node
/// tuple (to go into an inner tuple) containing the given label. The downlink
/// (`t_tid`) is set invalid; the caller fills it in later.
pub fn spgFormNodeTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    label: &Datum<'_>,
    isnull: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut infomask: u16 = 0;

    // Compute space needed (note result is already maxaligned).
    let mut size = SGNTHDRSZ();
    if !isnull {
        size += SpGistGetInnerTypeSize(&state.attLabelType, label);
    }

    // Make sure the size will fit in the field reserved for it in t_info.
    if (size as u16 & INDEX_SIZE_MASK) as usize != size {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "index row requires {size} bytes, maximum size is {}",
                INDEX_SIZE_MASK
            ))
            .into_error());
    }

    let mut tup = vec_with_capacity_in(mcx, size)?;
    tup.resize(size, 0u8);

    if isnull {
        infomask |= INDEX_NULL_MASK;
    }
    // we don't bother setting the INDEX_VAR_MASK bit.
    infomask |= size as u16;

    // The node tuple reuses the IndexTupleData header (t_tid + t_info).
    let mut node_header = SpGistNodeTupleData {
        t_info: infomask,
        ..SpGistNodeTupleData::default()
    };
    // The TID field will be filled in later.
    ItemPointerSetInvalid(&mut node_header.t_tid);

    if !isnull {
        // memcpyInnerDatum(SGNTDATAPTR(tup), &state->attLabelType, label).
        let data_off = SGNTHDRSZ();
        memcpyInnerDatum(&mut tup[data_off..], &state.attLabelType, label);
    }

    write_node_header(&mut tup, &node_header);

    Ok(tup)
}

/// `SGNTHDRSZ` (spgist_private.h): `MAXALIGN(sizeof(SpGistNodeTupleData))`.
#[inline]
pub(crate) const fn SGNTHDRSZ() -> usize {
    MAXALIGN(SIZEOF_SPGIST_NODE_TUPLE_DATA)
}

/// Serialize a [`SpGistNodeTupleData`] header into the first 8 bytes of `tup`.
pub(crate) fn write_node_header(tup: &mut [u8], header: &SpGistNodeTupleData) {
    write_item_pointer(&mut tup[0..6], &header.t_tid);
    tup[6..8].copy_from_slice(&header.t_info.to_ne_bytes());
}

/// `IndexTupleSize` of a node tuple (an `IndexTupleData`): `t_info &
/// INDEX_SIZE_MASK`.
#[inline]
pub(crate) fn node_tuple_size(node: &[u8]) -> usize {
    let t_info = u16::from_ne_bytes([node[6], node[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `IndexTupleHasNulls` of a node tuple: `t_info & INDEX_NULL_MASK`.
#[inline]
pub(crate) fn node_tuple_has_nulls(node: &[u8]) -> bool {
    let t_info = u16::from_ne_bytes([node[6], node[7]]);
    t_info & INDEX_NULL_MASK != 0
}

/// `spgFormInnerTuple(state, hasPrefix, prefix, nNodes, nodes)`
/// (spgutils.c:1001) — construct an inner tuple containing the given prefix and
/// node array. `nodes` are the owned node-tuple byte images.
pub fn spgFormInnerTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    has_prefix: bool,
    prefix: &Datum<'_>,
    nodes: &[PgVec<'mcx, u8>],
) -> PgResult<PgVec<'mcx, u8>> {
    let n_nodes = nodes.len();

    // Compute size needed.
    let prefix_size = if has_prefix {
        SpGistGetInnerTypeSize(&state.attPrefixType, prefix)
    } else {
        0
    };

    let mut size = SGITHDRSZ + prefix_size;

    // Note: we rely on node tuple sizes to be maxaligned already.
    for node in nodes {
        size += node_tuple_size(node);
    }

    // Ensure that we can replace the tuple with a dead tuple later.
    if size < SGDTSIZE {
        size = SGDTSIZE;
    }

    // Inner tuple should be small enough to fit on a page.
    if size > SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "SP-GiST inner tuple size {size} exceeds maximum {}",
                SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA
            ))
            .errhint("Values larger than a buffer page cannot be indexed.")
            .into_error());
    }

    // Check for overflow of header fields.
    if size as u32 > SGITMAXSIZE
        || prefix_size as u32 > SGITMAXPREFIXSIZE
        || n_nodes as u32 > SGITMAXNNODES
    {
        return Err(elog_internal(
            ERROR,
            "SPGiST inner tuple header field is too small".into(),
        ));
    }

    // OK, form the tuple (palloc0).
    let mut tup = vec_with_capacity_in(mcx, size)?;
    tup.resize(size, 0u8);

    let mut header = SpGistInnerTupleData::default();
    header.set_nNodes(n_nodes as u32);
    header.set_prefixSize(prefix_size as u32);
    header.size = size as u16;

    if has_prefix {
        // memcpyInnerDatum(SGITDATAPTR(tup), &state->attPrefixType, prefix).
        memcpyInnerDatum(&mut tup[SGITHDRSZ..], &state.attPrefixType, prefix);
    }

    // ptr = (char *) SGITNODEPTR(tup): the nodes start after the header + prefix.
    let mut ptr = SGITHDRSZ + prefix_size;
    for node in nodes {
        let nsz = node_tuple_size(node);
        tup[ptr..ptr + nsz].copy_from_slice(&node[..nsz]);
        ptr += nsz;
    }

    write_inner_header(&mut tup, &header);

    Ok(tup)
}

/// Serialize a [`SpGistInnerTupleData`] header into the first 8 bytes of `tup`
/// (the 4-byte packed bit word followed by the uint16 size).
pub(crate) fn write_inner_header(tup: &mut [u8], header: &SpGistInnerTupleData) {
    tup[0..4].copy_from_slice(&header.bits.to_ne_bytes());
    tup[4..6].copy_from_slice(&header.size.to_ne_bytes());
    // offsets 6..8 are padding (already zeroed by palloc0).
}

/// `spgFormDeadTuple(state, tupstate, blkno, offnum)` (spgutils.c:1084) —
/// construct a "dead" tuple to replace a tuple being deleted, built in the
/// preallocated `state.deadTupleStorage` (no palloc; called in critical
/// sections). Returns the built [`SpGistDeadTupleData`] value (a snapshot of
/// the storage); the storage itself holds the serialized image.
pub fn spgFormDeadTuple(
    state: &mut SpGistState<'_>,
    tupstate: u32,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> SpGistDeadTupleData {
    let mut tuple = SpGistDeadTupleData::default();

    tuple.set_tupstate(tupstate);
    tuple.set_size(SGDTSIZE as u32);
    // SGLT_SET_NEXTOFFSET(tuple, InvalidOffsetNumber) — dead tuples share the
    // leaf t_info layout; nextOffset is the low 14 bits of t_info (here 0).
    set_dead_next_offset(&mut tuple, InvalidOffsetNumber);

    if tupstate == SPGIST_REDIRECT {
        ItemPointerSet(&mut tuple.pointer, blkno, offnum);
        tuple.xid = state.redirectXid;
    } else {
        ItemPointerSetInvalid(&mut tuple.pointer);
        tuple.xid = types_core::xact::InvalidTransactionId;
    }

    // Serialize into the preallocated storage (state->deadTupleStorage).
    let storage = state
        .deadTupleStorage
        .as_mut()
        .expect("spgFormDeadTuple: deadTupleStorage is NULL");
    write_dead_tuple(storage, &tuple);

    tuple
}

/// `SGLT_SET_NEXTOFFSET` on a dead tuple's `t_info` (low 14 bits).
#[inline]
fn set_dead_next_offset(tuple: &mut SpGistDeadTupleData, offset: OffsetNumber) {
    tuple.t_info = (tuple.t_info & 0xC000) | (offset & 0x3FFF);
}

/// Serialize a [`SpGistDeadTupleData`] into the first 16 bytes of `storage`.
fn write_dead_tuple(storage: &mut [u8], tuple: &SpGistDeadTupleData) {
    storage[0..4].copy_from_slice(&tuple.bits.to_ne_bytes());
    storage[4..6].copy_from_slice(&tuple.t_info.to_ne_bytes());
    write_item_pointer(&mut storage[6..12], &tuple.pointer);
    storage[12..16].copy_from_slice(&tuple.xid.to_ne_bytes());
}

/// `spgDeformLeafTuple(tup, tupleDescriptor, datums, isnulls, keyColumnIsNull)`
/// (spgutils.c:1114) — convert an SP-GiST leaf tuple into datum/isnull arrays.
///
/// `tup` is the leaf tuple's on-disk bytes. Returns the per-column
/// `(Datum, isnull)` pairs.
pub fn spgDeformLeafTuple<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &[u8],
    tuple_descriptor: &TupleDescData<'_>,
    key_column_is_null: bool,
) -> PgResult<PgVec<'mcx, (Datum<'mcx>, bool)>> {
    // SGLT_GET_HASNULLMASK(tup): bit 0x8000 of t_info (the uint16 at offset 4).
    let t_info = u16::from_ne_bytes([tup[4], tup[5]]);
    let has_nulls_mask = t_info & 0x8000 != 0;

    if key_column_is_null && tuple_descriptor.natts == 1 {
        // Trivial case: only the key attribute, and we're in a nulls tree. The
        // hasNullsMask bit should not be set; the result is NULL.
        debug_assert!(!has_nulls_mask);
        let mut out = vec_with_capacity_in(mcx, 1)?;
        out.push((Datum::null(), true));
        return Ok(out);
    }

    // tp = (char *) tup + SGLTHDRSZ(hasNullsMask);
    let tp = &tup[SGLTHDRSZ(has_nulls_mask)..];
    // bp = (bits8 *) ((char *) tup + sizeof(SpGistLeafTupleData));
    let bp: Option<&[u8]> = if has_nulls_mask {
        Some(&tup[SIZEOF_SPGIST_LEAF_TUPLE_DATA..])
    } else {
        None
    };

    let cols = index_deform_tuple_internal(mcx, tuple_descriptor, tp, bp)?;

    // Key column isnull value from the tuple should match keyColumnIsNull.
    debug_assert_eq!(key_column_is_null, cols[spgKeyColumn as usize].1);

    Ok(cols)
}

/// `spgExtractNodeLabels(state, innerTuple)` (spgutils.c:1159) — extract the
/// label datums of the nodes within `inner_tuple` (the on-disk bytes). Returns
/// `None` if the label datums are all NULL.
pub fn spgExtractNodeLabels<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    inner_tuple: &[u8],
) -> PgResult<Option<PgVec<'mcx, Datum<'mcx>>>> {
    let header = read_inner_header(inner_tuple);
    let n_nodes = header.nNodes() as usize;
    let prefix_size = header.prefixSize() as usize;

    // SGITNODEPTR(innerTuple): the node array starts after header + prefix.
    let nodes_off = SGITHDRSZ + prefix_size;

    // Either all the labels must be NULL, or none. Check the first node.
    let first_node = &inner_tuple[nodes_off..];
    if node_tuple_has_nulls(first_node) {
        // Iterate to verify all are null.
        let mut off = nodes_off;
        for _ in 0..n_nodes {
            let node = &inner_tuple[off..];
            if !node_tuple_has_nulls(node) {
                return Err(elog_internal(
                    ERROR,
                    "some but not all node labels are null in SPGiST inner tuple".into(),
                ));
            }
            off += node_tuple_size(node);
        }
        // They're all null, so just return NULL.
        return Ok(None);
    }

    let mut node_labels = vec_with_capacity_in(mcx, n_nodes)?;
    let mut off = nodes_off;
    for _ in 0..n_nodes {
        let node = &inner_tuple[off..];
        if node_tuple_has_nulls(node) {
            return Err(elog_internal(
                ERROR,
                "some but not all node labels are null in SPGiST inner tuple".into(),
            ));
        }
        // nodeLabels[i] = SGNTDATUM(node, state): the inner datum after the node
        // header, decoded per attLabelType.
        let label = read_inner_datum(mcx, &state.attLabelType, &node[SGNTHDRSZ()..])?;
        node_labels.push(label);
        off += node_tuple_size(node);
    }
    Ok(Some(node_labels))
}

/// `SGNTDATUM(node, state)` (spgist_private.h): fetch an inner datum (label or
/// prefix) of the given type from the bytes following the inner/node header.
/// Pass-by-value types are stored in their Datum representation.
pub(crate) fn read_inner_datum<'mcx>(
    mcx: Mcx<'mcx>,
    att: &SpGistTypeDesc,
    data: &[u8],
) -> PgResult<Datum<'mcx>> {
    if att.attbyval {
        // The raw machine word.
        let mut word = [0u8; SIZEOF_DATUM];
        word.copy_from_slice(&data[..SIZEOF_DATUM]);
        Ok(Datum::ByVal(usize::from_ne_bytes(word)))
    } else {
        let size = if att.attlen > 0 {
            att.attlen as usize
        } else {
            varsize_any(data)
        };
        let mut bytes = vec_with_capacity_in(mcx, size)?;
        bytes.extend_from_slice(&data[..size]);
        Ok(Datum::ByRef(bytes))
    }
}

/// Read a [`SpGistInnerTupleData`] header from the inner tuple bytes.
pub(crate) fn read_inner_header(tup: &[u8]) -> SpGistInnerTupleData {
    SpGistInnerTupleData {
        bits: u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]),
        size: u16::from_ne_bytes([tup[4], tup[5]]),
    }
}

// ===========================================================================
// SpGistPageAddNewItem
// ===========================================================================

/// `SpGistPageAddNewItem(state, page, item, size, startOffset, errorOK)`
/// (spgutils.c:1202) — add a new item to the page, replacing a PLACEHOLDER item
/// if possible. Returns the offset it was inserted at, or `InvalidOffsetNumber`
/// on failure. `start_offset`, if `Some`, is the search hint, updated in place.
pub fn SpGistPageAddNewItem(
    _state: &SpGistState<'_>,
    page: &mut [u8],
    item: &[u8],
    size: Size,
    start_offset: Option<&mut OffsetNumber>,
    error_ok: bool,
) -> PgResult<OffsetNumber> {
    // Read placeholder count + free space from the page.
    let n_placeholder = opaque_n_placeholder(page);
    let free_space = PageGetExactFreeSpace(&PageRef::new(page)?);

    let mut start_offset = start_offset;

    if n_placeholder > 0 && (free_space as usize + SGDTSIZE) >= MAXALIGN(size) {
        // Try to replace a placeholder.
        let maxoff = PageGetMaxOffsetNumber(&PageRef::new(page)?);
        let mut offnum = InvalidOffsetNumber;

        loop {
            let mut i = match start_offset.as_deref() {
                Some(&so) if so != InvalidOffsetNumber => so,
                _ => FirstOffsetNumber,
            };
            while i <= maxoff {
                let it_tupstate = {
                    let page_ref = PageRef::new(page)?;
                    let item_id = PageGetItemId(&page_ref, i)?;
                    let it = PageGetItem(&page_ref, &item_id)?;
                    // SpGistDeadTuple->tupstate is the low 2 bits of bits[0..4].
                    let bits = u32::from_ne_bytes([it[0], it[1], it[2], it[3]]);
                    bits & 0x3
                };
                if it_tupstate == SPGIST_PLACEHOLDER {
                    offnum = i;
                    break;
                }
                i += 1;
            }

            // Done if we found a placeholder.
            if offnum != InvalidOffsetNumber {
                break;
            }

            if let Some(so) = start_offset.as_deref() {
                if *so != InvalidOffsetNumber {
                    // Hint was no good, re-search from beginning.
                    if let Some(so) = start_offset.as_deref_mut() {
                        *so = InvalidOffsetNumber;
                    }
                    continue;
                }
            }

            // Hmm, no placeholder found?
            set_opaque_n_placeholder(page, 0);
            break;
        }

        if offnum != InvalidOffsetNumber {
            // Replace the placeholder tuple.
            {
                let mut page_mut = PageMut::new(page)?;
                PageIndexTupleDelete(&mut page_mut, offnum)?;
            }

            let added = {
                let mut page_mut = PageMut::new(page)?;
                PageAddItemExtended(&mut page_mut, item, offnum, 0)?
            };

            // We should not have failed given the size check at the top. If we
            // did, PANIC because we've already deleted the placeholder tuple.
            if added != InvalidOffsetNumber {
                debug_assert!(opaque_n_placeholder(page) > 0);
                set_opaque_n_placeholder(page, opaque_n_placeholder(page) - 1);
                if let Some(so) = start_offset.as_deref_mut() {
                    *so = added + 1;
                }
            } else {
                return Err(elog_internal(
                    PANIC,
                    format!("failed to add item of size {size} to SPGiST index page"),
                ));
            }

            return Ok(added);
        }
    }

    // No luck replacing a placeholder, so just add it to the page.
    let offnum = {
        let mut page_mut = PageMut::new(page)?;
        PageAddItemExtended(&mut page_mut, item, InvalidOffsetNumber, 0)?
    };

    if offnum == InvalidOffsetNumber && !error_ok {
        return Err(elog_internal(
            ERROR,
            format!("failed to add item of size {size} to SPGiST index page"),
        ));
    }

    Ok(offnum)
}

// ===========================================================================
// spgproperty
// ===========================================================================

/// `spgproperty(index_oid, attno, prop, propname, res, isnull)`
/// (spgutils.c:1297) — check boolean properties of indexes. SP-GiST overrides
/// the core property code for `AMPROP_DISTANCE_ORDERABLE`.
///
/// Returns `(handled, res, isnull)`: `handled` is the C boolean return (whether
/// this routine answered the inquiry), `res`/`isnull` the out-params.
pub fn spgproperty<'mcx>(
    mcx: Mcx<'mcx>,
    index_oid: Oid,
    attno: i32,
    prop: IndexAMProperty,
) -> PgResult<(bool, bool, bool)> {
    // Only answer column-level inquiries.
    if attno == 0 {
        return Ok((false, false, false));
    }

    match prop {
        IndexAMProperty::DistanceOrderable => {}
        _ => return Ok((false, false, false)),
    }

    // First we need to know the column's opclass.
    let opclass = backend_utils_cache_lsyscache_seams::get_index_column_opclass::call(
        index_oid, attno,
    )?;
    if !OidIsValid(opclass) {
        // isnull = true; return true.
        return Ok((true, false, true));
    }

    // Now look up the opclass family and input datatype.
    let (opfamily, opcintype) =
        match backend_utils_cache_lsyscache_seams::get_opclass_opfamily_and_input_type::call(
            opclass,
        )? {
            Some(pair) => pair,
            None => return Ok((true, false, true)),
        };

    // And now we can check whether the operator is provided.
    let catlist = backend_utils_cache_syscache_seams::search_amop_list::call(mcx, opfamily)?;

    let mut res = false;

    for amopform in &catlist {
        if amopform.amoppurpose == types_opclass::AMOP_ORDER
            && (amopform.amoplefttype == opcintype || amopform.amoprighttype == opcintype)
            && backend_access_index_amvalidate_seams::opfamily_can_sort_type::call(
                amopform.amopsortfamily,
                backend_utils_cache_lsyscache_seams::get_op_rettype::call(amopform.amopopr)?,
            )?
        {
            res = true;
            break;
        }
    }

    // isnull = false; return true.
    Ok((true, res, false))
}

/// `IndexAMProperty` (amapi.h) — the boolean/text property inquiry kinds. Only
/// the `AMPROP_DISTANCE_ORDERABLE` arm matters to SP-GiST; the rest are folded
/// into `Other` (the C `default:` that returns false).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexAMProperty {
    /// `AMPROP_DISTANCE_ORDERABLE`.
    DistanceOrderable,
    /// Any other `IndexAMProperty` value (the C `default:` case).
    Other,
}

// ===========================================================================
// Small helpers
// ===========================================================================

/// `OidIsValid(oid)` (c.h): `oid != InvalidOid`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// Serialize an [`ItemPointerData`] (6 bytes: block hi/lo + offset).
pub(crate) fn write_item_pointer(dst: &mut [u8], ip: &ItemPointerData) {
    dst[0..2].copy_from_slice(&ip.ip_blkid.bi_hi.to_ne_bytes());
    dst[2..4].copy_from_slice(&ip.ip_blkid.bi_lo.to_ne_bytes());
    dst[4..6].copy_from_slice(&ip.ip_posid.to_ne_bytes());
}

/// `elog(level, "...")` (an internal-message error/panic, no SQLSTATE).
fn elog_internal(level: types_error::ErrorLevel, msg: alloc::string::String) -> PgError {
    ereport(level).errmsg_internal(msg).into_error()
}

/// This crate owns no inward seams: its page-management / tuple-builder entry
/// points are plain `pub fn`s consumed directly by spgdoinsert / spgscan /
/// spgvacuum, and the three seams it *calls out through* are owned elsewhere —
/// `conditional_lock_buffer` (bufmgr), `get_index_input_type_expr` (the
/// SP-GiST core seam crate, installed by relcache/plancat later), and
/// `opfamily_can_sort_type` (lsyscache). So `init_seams()` is empty, like
/// `backend-access-gin-ginutil`'s.
pub fn init_seams() {}

pub mod spgdoinsert;
pub use spgdoinsert::{spgPageIndexMultiDelete, spgUpdateNodeLink, spgdoinsert};

#[cfg(test)]
mod tests;
