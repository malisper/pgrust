//! PostgreSQL 18.3 `src/backend/access/heap/heaptoast.c` — heap-specific
//! definitions for external and compressed storage of variable size
//! attributes.
//!
//! INTERFACE ROUTINES
//!   - [`heap_toast_insert_or_update`] — try to make a tuple fit on one page
//!     by compressing or moving off attributes;
//!   - [`heap_toast_delete`] — reclaim TOAST storage when a tuple is deleted;
//!   - [`toast_flatten_tuple`] / [`toast_flatten_tuple_to_datum`] /
//!     [`toast_build_flattened_tuple`] — remove out-of-line fields;
//!   - [`heap_fetch_toast_slice`] — fetch a TOAST slice from a heap table.
//!
//! Model notes (vs. the C pointer model):
//!
//!   * a tuple travels as [`FormedTuple`] (owned header + user-data area); a
//!     per-attribute `Datum` is [`TupleValue`] (`ByVal` scalar / `ByRef`
//!     verbatim datum bytes, varlena header included);
//!   * relations cross as [`types_rel::Relation`] handles; `rel.h` field
//!     reads are plain field reads on the trimmed `RelationData` (foreign
//!     seams that key on the relation still take `rd_id`);
//!   * the TOAST pass context is the transparent owned
//!     [`ToastTupleContext`], a local value threaded `&mut` through the
//!     `toast_helper.c` seams exactly as C threads `&ttc`;
//!   * C's `pfree` of temp values (`toast_free[]` bookkeeping) is Rust drop —
//!     replaced values are dropped on overwrite, the arrays at scope end.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

#[cfg(test)]
mod tests;

use alloc::format;

use backend_access_common_heaptuple::{
    heap_compute_data_size, heap_deform_tuple, heap_form_tuple, nocachegetattr, FormedTuple,
    HeapTupleError, TupleValue,
};
use backend_utils_error::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{AttrNumber, Oid};
use types_rel::Relation;
use types_datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_TOO_MANY_COLUMNS, ERROR,
};
use types_scan::scankey::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTLessEqualStrategyNumber, ScanKeyData,
    StrategyNumber,
};
use types_scan::sdir::ForwardScanDirection;
use types_storage::storage::AccessShareLock;
use types_tuple::heap::SizeofHeapTupleHeader;
use types_tuple::heaptuple::{
    HeapTupleHeaderSetNatts, TupleDescData, BITMAPLEN, HEAP2_XACT_MASK, HEAP_HASEXTERNAL,
    HEAP_HASNULL, HEAP_HASVARWIDTH, HEAP_XACT_MASK, MaxHeapAttributeNumber,
    MaxTupleAttributeNumber, TYPSTORAGE_EXTENDED,
};
use types_tuple::toast_helper::{
    ToastAttrInfo, ToastTupleContext, TOASTCOL_INCOMPRESSIBLE, TOAST_HAS_NULLS,
    TOAST_NEEDS_CHANGE,
};

use backend_access_common_detoast_seams as detoast_seams;
use backend_access_common_toast_internals_seams as toast_internals_seams;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_toast_helper_seams as toast_helper_seams;

// ---------------------------------------------------------------------------
// heaptoast.h — TOAST thresholds (derived exactly as the C macros derive
// them from the page layout).
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` (64-bit build).
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)` (c.h).
#[inline]
const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN_DOWN(LEN)` (c.h).
#[inline]
const fn MAXALIGN_DOWN(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `BLCKSZ` (pg_config.h, default build).
const BLCKSZ: usize = 8192;
/// `SizeOfPageHeaderData` == `offsetof(PageHeaderData, pd_linp)` (24).
const SizeOfPageHeaderData: usize = 24;
/// `sizeof(ItemIdData)` — a packed 32-bit line pointer.
const SIZEOF_ITEM_ID_DATA: usize = 4;

/// `MaximumBytesPerTuple(tuplesPerPage)` (heaptoast.h).
const fn MaximumBytesPerTuple(tuples_per_page: usize) -> usize {
    MAXALIGN_DOWN(
        (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + tuples_per_page * SIZEOF_ITEM_ID_DATA))
            / tuples_per_page,
    )
}

/// `TOAST_TUPLES_PER_PAGE` (heaptoast.h).
pub const TOAST_TUPLES_PER_PAGE: usize = 4;
/// `TOAST_TUPLE_THRESHOLD` == `TOAST_TUPLE_TARGET` (heaptoast.h). 2032 on the
/// default 8 KiB page; the per-relation `toast_tuple_target` reloption
/// overrides it via `RelationGetToastTupleTarget`.
pub const TOAST_TUPLE_THRESHOLD: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE);
pub const TOAST_TUPLE_TARGET: i32 = TOAST_TUPLE_THRESHOLD as i32;
/// `TOAST_TUPLES_PER_PAGE_MAIN` (heaptoast.h).
pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
/// `TOAST_TUPLE_TARGET_MAIN` (heaptoast.h). 8160 on the default page.
pub const TOAST_TUPLE_TARGET_MAIN: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE_MAIN);
/// `EXTERN_TUPLES_PER_PAGE` (heaptoast.h).
pub const EXTERN_TUPLES_PER_PAGE: usize = 4;
/// `EXTERN_TUPLE_MAX_SIZE` (heaptoast.h).
pub const EXTERN_TUPLE_MAX_SIZE: usize = MaximumBytesPerTuple(EXTERN_TUPLES_PER_PAGE);
/// `TOAST_MAX_CHUNK_SIZE` (heaptoast.h): `EXTERN_TUPLE_MAX_SIZE -
/// MAXALIGN(SizeofHeapTupleHeader) - sizeof(Oid) - sizeof(int32) - VARHDRSZ`.
/// 1996 on the default page.
pub const TOAST_MAX_CHUNK_SIZE: i32 = (EXTERN_TUPLE_MAX_SIZE
    - MAXALIGN(SizeofHeapTupleHeader)
    - core::mem::size_of::<Oid>()
    - core::mem::size_of::<i32>()
    - VARHDRSZ as usize) as i32;

/// `VARHDRSZ` (c.h) — varlena 4-byte length header size.
const VARHDRSZ: i32 = 4;
/// `VARHDRSZ_SHORT` (varatt.h) == `offsetof(varattrib_1b, va_data)` == 1.
const VARHDRSZ_SHORT: i32 = 1;

/// `F_OIDEQ` / `F_INT4EQ` / `F_INT4GE` / `F_INT4LE` (fmgroids.h).
const F_OIDEQ: Oid = 184;
const F_INT4EQ: Oid = 65;
const F_INT4GE: Oid = 150;
const F_INT4LE: Oid = 149;

/// `HEAP_INSERT_SPECULATIVE` (heapam.h) — cleared from `options` on entry.
const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

const InvalidOid: Oid = 0;

/// `C_COLLATION_OID` (pg_collation.dat oid 950) — `ScanKeyInit` always sets
/// `sk_collation` to the C collation; it is ignored for non-collatable
/// columns such as the toast (valueid, chunkidx) keys.
const C_COLLATION_OID: Oid = 950;

// ---------------------------------------------------------------------------
// varatt.h predicates over verbatim datum bytes (the value's on-disk image,
// exactly what `DatumGetPointer` would dereference; little-endian build).
// ---------------------------------------------------------------------------

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`: `va_header == 0x01`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_COMPRESSED(PTR)` == `VARATT_IS_4B_C(PTR)`: low two bits `0b10`.
#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

/// `VARATT_IS_4B(PTR)`: low two bits `0b00`.
#[inline]
fn varatt_is_4b(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x00
}

/// `VARATT_IS_1B(PTR)`: low bit set.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_EXTENDED(PTR)` == `!VARATT_IS_4B_U(PTR)`.
#[inline]
fn varatt_is_extended(b: &[u8]) -> bool {
    !varatt_is_4b(b)
}

/// `VARATT_IS_SHORT(PTR)` == `VARATT_IS_1B(PTR)`.
#[inline]
fn varatt_is_short(b: &[u8]) -> bool {
    varatt_is_1b(b)
}

/// `VARSIZE(PTR)` == `VARSIZE_4B(PTR)`: the 4-byte length word `>> 2`.
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let word = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    (word >> 2) & 0x3fff_ffff
}

/// `VARSIZE_SHORT(PTR)` == `VARSIZE_1B(PTR)`: `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7f) as u32
}

// ---------------------------------------------------------------------------
// ScanKeyInit (access/common/scankey.c) — plain field initialization. C's
// `fmgr_info(procedure, &entry->sk_func)` resolves the function eagerly; the
// trimmed `FmgrInfo` records only `fn_oid`, deferring the lookup to the scan
// code that consumes the key (behind the genam seam).
// ---------------------------------------------------------------------------

fn ScanKeyInit(
    entry: &mut ScanKeyData,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    procedure: Oid,
    argument: Datum,
) {
    entry.sk_flags = 0;
    entry.sk_attno = attribute_number;
    entry.sk_strategy = strategy;
    entry.sk_subtype = InvalidOid;
    entry.sk_collation = C_COLLATION_OID;
    entry.sk_argument = argument;
    entry.sk_func = types_core::fmgr::FmgrInfo { fn_oid: procedure };
}

// ---------------------------------------------------------------------------
// heap_toast_delete
// ---------------------------------------------------------------------------

/// `heap_toast_delete(rel, oldtup, is_speculative)` — cascaded delete of
/// toast-entries on DELETE. `mcx` holds the deform temporaries.
pub fn heap_toast_delete(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    oldtup: &FormedTuple<'_>,
    is_speculative: bool,
) -> PgResult<()> {
    // We should only ever be called for tuples of plain relations or
    // materialized views --- recursing on a toast rel is bad news.
    // Assert(rel->rd_rel->relkind == RELKIND_RELATION || RELKIND_MATVIEW);

    // Get the tuple descriptor and break down the tuple into fields.
    //
    // NOTE: it's debatable whether to use heap_deform_tuple() here or just
    // heap_getattr() only the varlena columns. heap_deform_tuple costs only
    // O(N) while the heap_getattr way would cost O(N^2) if there are many
    // varlena columns, so it seems better to err on the side of linear cost.
    let tuple_desc = &rel.rd_att;

    debug_assert!(tuple_desc.natts <= MaxHeapAttributeNumber);
    let (toast_values, toast_isnull) = deform_split(mcx, oldtup, tuple_desc)?;

    // Do the real work.
    toast_internals_seams::toast_delete_external::call(
        rel.rd_id,
        &toast_values,
        &toast_isnull,
        is_speculative,
    )
}

// ---------------------------------------------------------------------------
// heap_toast_insert_or_update
// ---------------------------------------------------------------------------

/// `heap_toast_insert_or_update(rel, newtup, oldtup, options)` — delete
/// no-longer-used toast-entries and create new ones to make the new tuple fit
/// on INSERT or UPDATE.
///
/// `oldtup` is the old row version for UPDATE, or `None` for INSERT.
/// `options` is passed to `heap_insert()` for toast rows. Returns `None` when
/// no toasting was needed (C returns `newtup` itself), or `Some` — a freshly
/// built modified tuple allocated in `mcx`. Neither input tuple is modified.
pub fn heap_toast_insert_or_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    newtup: &FormedTuple<'_>,
    oldtup: Option<&FormedTuple<'_>>,
    mut options: i32,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Ignore the INSERT_SPECULATIVE option. Speculative insertions/super
    // deletions just normally insert/delete the toast values.
    options &= !HEAP_INSERT_SPECULATIVE;

    // We should only ever be called for tuples of plain relations or
    // materialized views --- recursing on a toast rel is bad news.
    // Assert(rel->rd_rel->relkind == RELKIND_RELATION || RELKIND_MATVIEW);

    // Get the tuple descriptor and break down the tuple(s) into fields.
    let tuple_desc = &rel.rd_att;
    let num_attrs = tuple_desc.natts;

    debug_assert!(num_attrs <= MaxHeapAttributeNumber);
    let (toast_values, toast_isnull) = deform_split(mcx, newtup, tuple_desc)?;
    let (toast_oldvalues, toast_oldisnull) = match oldtup {
        Some(otup) => {
            let (v, n) = deform_split(mcx, otup, tuple_desc)?;
            (Some(v), Some(n))
        }
        None => (None, None),
    };

    // rel->rd_rel->reltoastrelid
    let reltoastrelid = rel.rd_rel.reltoastrelid;

    // Prepare for toasting.
    let mut ttc_attr: PgVec<'_, ToastAttrInfo<'_>> =
        vec_with_capacity_in(mcx, num_attrs.max(0) as usize)?;
    for _ in 0..num_attrs.max(0) {
        ttc_attr.push(ToastAttrInfo::empty());
    }
    let mut ttc = ToastTupleContext {
        ttc_rel: rel.rd_id,
        ttc_values: toast_values,
        ttc_isnull: toast_isnull,
        ttc_oldvalues: toast_oldvalues,
        ttc_oldisnull: toast_oldisnull,
        ttc_flags: 0,
        ttc_attr,
    };
    toast_helper_seams::toast_tuple_init::call(&mut ttc)?;

    // Compress and/or save external until data fits into target length:
    //   1: inline-compress attributes with attstorage EXTENDED, and store
    //      very large EXTENDED/EXTERNAL attributes external immediately;
    //   2: store EXTENDED/EXTERNAL attributes external;
    //   3: inline-compress attributes with attstorage MAIN;
    //   4: store attributes of type MAIN external.

    // Compute header overhead --- this should match heap_form_tuple().
    let mut hoff: usize = SizeofHeapTupleHeader;
    if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
        hoff += BITMAPLEN(num_attrs) as usize;
    }
    hoff = MAXALIGN(hoff);
    // Now convert to a limit on the tuple data size. C performs the
    // subtraction in unsigned `Size`, so wrap rather than checked-subtract.
    let mut max_data_len: usize =
        (rel.get_toast_tuple_target(TOAST_TUPLE_TARGET) as usize)
            .wrapping_sub(hoff);

    // Round 1: compress EXTENDED attributes; push very large EXTENDED /
    // EXTERNAL attributes out to the toast table immediately.
    while heap_compute_data_size(&tuple_desc, &ttc.ttc_values, &ttc.ttc_isnull)? > max_data_len {
        let biggest_attno =
            toast_helper_seams::toast_tuple_find_biggest_attribute::call(&ttc, true, false)?;
        if biggest_attno < 0 {
            break;
        }

        // Attempt to compress it inline, if it has attstorage EXTENDED.
        if tuple_desc.attr(biggest_attno as usize).attstorage == TYPSTORAGE_EXTENDED {
            toast_helper_seams::toast_tuple_try_compression::call(&mut ttc, biggest_attno)?;
        } else {
            // Has attstorage EXTERNAL: ignore on subsequent compression passes.
            ttc.ttc_attr[biggest_attno as usize].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
        }

        // If this value is by itself more than maxDataLen (after compression
        // if any), push it out to the toast table immediately, if possible.
        // This avoids uselessly compressing other fields in the common case
        // where we have one long field and several short ones.
        if ttc.ttc_attr[biggest_attno as usize].tai_size as usize > max_data_len
            && reltoastrelid != InvalidOid
        {
            toast_helper_seams::toast_tuple_externalize::call(&mut ttc, biggest_attno, options)?;
        }
    }

    // Round 2: EXTENDED/EXTERNAL attributes still inline become external.
    // Skip if there's no toast table to push them to.
    while heap_compute_data_size(&tuple_desc, &ttc.ttc_values, &ttc.ttc_isnull)? > max_data_len
        && reltoastrelid != InvalidOid
    {
        let biggest_attno =
            toast_helper_seams::toast_tuple_find_biggest_attribute::call(&ttc, false, false)?;
        if biggest_attno < 0 {
            break;
        }
        toast_helper_seams::toast_tuple_externalize::call(&mut ttc, biggest_attno, options)?;
    }

    // Round 3: this time we take attributes with storage MAIN into compression.
    while heap_compute_data_size(&tuple_desc, &ttc.ttc_values, &ttc.ttc_isnull)? > max_data_len {
        let biggest_attno =
            toast_helper_seams::toast_tuple_find_biggest_attribute::call(&ttc, true, true)?;
        if biggest_attno < 0 {
            break;
        }
        toast_helper_seams::toast_tuple_try_compression::call(&mut ttc, biggest_attno)?;
    }

    // Round 4: store attributes of type MAIN externally. At this point we
    // increase the target tuple size, so that MAIN attributes aren't stored
    // externally unless really necessary.
    max_data_len = TOAST_TUPLE_TARGET_MAIN.wrapping_sub(hoff);

    while heap_compute_data_size(&tuple_desc, &ttc.ttc_values, &ttc.ttc_isnull)? > max_data_len
        && reltoastrelid != InvalidOid
    {
        let biggest_attno =
            toast_helper_seams::toast_tuple_find_biggest_attribute::call(&ttc, false, true)?;
        if biggest_attno < 0 {
            break;
        }
        toast_helper_seams::toast_tuple_externalize::call(&mut ttc, biggest_attno, options)?;
    }

    // In the case we toasted any values, we need to build a new heap tuple
    // with the changed values.
    let result = if (ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0 {
        // C: palloc0 a new tuple, memcpy the old header, adjust natts and
        // t_hoff, heap_fill_tuple the data. heap_form_tuple performs the same
        // size/hoff/fill computation (note: the old tuple's t_hoff need not
        // equal the new header length — ALTER TABLE ADD COLUMN may have grown
        // natts since it was stored); the header memcpy then becomes copying
        // the old header fields over the formed header, with heap_fill_tuple's
        // recomputed HASNULL/HASVARWIDTH/HASEXTERNAL bits kept.
        let mut formed = heap_form_tuple(mcx, &tuple_desc, &ttc.ttc_values, &ttc.ttc_isnull)
            .map_err(map_heaptuple_error)?;

        formed.tuple.t_self = newtup.tuple.t_self;
        formed.tuple.t_tableOid = newtup.tuple.t_tableOid;

        let old_hdr = newtup
            .tuple
            .t_data
            .as_ref()
            .expect("heap_toast_insert_or_update: newtup has no t_data");
        if let Some(new_hdr) = formed.tuple.t_data.as_mut() {
            // memcpy(new_data, olddata, SizeofHeapTupleHeader):
            new_hdr.t_choice = old_hdr.t_choice.clone();
            new_hdr.t_ctid = old_hdr.t_ctid;
            // heap_fill_tuple owns HASNULL/HASVARWIDTH/HASEXTERNAL; the rest
            // of t_infomask carries over from the old header.
            new_hdr.t_infomask = (old_hdr.t_infomask
                & !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL))
                | (new_hdr.t_infomask & (HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL));
            // HeapTupleHeaderSetNatts(new_data, numAttrs) over the old bits.
            new_hdr.t_infomask2 = old_hdr.t_infomask2;
            HeapTupleHeaderSetNatts(new_hdr, num_attrs as u16);
            // t_hoff and t_bits stay as heap_form_tuple computed them
            // (new_data->t_hoff = new_header_len).
        }

        Some(formed)
    } else {
        None
    };

    toast_helper_seams::toast_tuple_cleanup::call(&mut ttc)?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// toast_flatten_tuple
// ---------------------------------------------------------------------------

/// `toast_flatten_tuple(tup, tupleDesc)` — "flatten" a tuple to contain no
/// out-of-line toasted fields. (This does not eliminate compressed or
/// short-header datums.) The caller already checked
/// `HeapTupleHasExternal(tup)`, so there is no short-circuit path.
pub fn toast_flatten_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let num_attrs = tuple_desc.natts;
    debug_assert!(num_attrs <= MaxTupleAttributeNumber);

    // Break down the tuple into fields.
    let (mut toast_values, toast_isnull) = deform_split(mcx, tup, tuple_desc)?;

    for i in 0..num_attrs.max(0) as usize {
        // Look at non-null varlena attributes.
        if !toast_isnull[i] && tuple_desc.compact_attrs[i].attlen == -1 {
            if let TupleValue::ByRef(bytes) = &toast_values[i] {
                if varatt_is_external(bytes) {
                    let detoasted = detoast_seams::detoast_external_attr::call(mcx, bytes)?;
                    toast_values[i] = TupleValue::ByRef(detoasted);
                }
            }
        }
    }

    // Form the reconfigured tuple.
    let mut new_tuple = heap_form_tuple(mcx, tuple_desc, &toast_values, &toast_isnull)
        .map_err(map_heaptuple_error)?;

    // Be sure to copy the tuple's identity fields. We also make a point of
    // copying visibility info, just in case anybody looks at those fields in
    // a syscache entry.
    new_tuple.tuple.t_self = tup.tuple.t_self;
    new_tuple.tuple.t_tableOid = tup.tuple.t_tableOid;

    let old_hdr = tup
        .tuple
        .t_data
        .as_ref()
        .expect("toast_flatten_tuple: tuple has no t_data");
    if let Some(new_hdr) = new_tuple.tuple.t_data.as_mut() {
        new_hdr.t_choice = old_hdr.t_choice.clone();
        new_hdr.t_ctid = old_hdr.t_ctid;
        new_hdr.t_infomask &= !HEAP_XACT_MASK;
        new_hdr.t_infomask |= old_hdr.t_infomask & HEAP_XACT_MASK;
        new_hdr.t_infomask2 &= !HEAP2_XACT_MASK;
        new_hdr.t_infomask2 |= old_hdr.t_infomask2 & HEAP2_XACT_MASK;
    }

    Ok(new_tuple)
}

// ---------------------------------------------------------------------------
// toast_flatten_tuple_to_datum
// ---------------------------------------------------------------------------

/// `toast_flatten_tuple_to_datum(tup, tup_len, tupleDesc)` — "flatten" a
/// tuple containing out-of-line toasted fields into a composite Datum,
/// decompressing compressed fields too (in-line short-header varlenas are
/// left alone — they'd just get changed back within `heap_fill_tuple`).
///
/// C takes the bare `HeapTupleHeader` + `tup_len`; the [`FormedTuple`]
/// carries exactly that (header, data area, `tuple.t_len`). The result is the
/// flattened composite tuple allocated in `mcx`, its header carrying the
/// `t_datum` length/typeid/typmod fields
/// (`HeapTupleHeaderSetDatumLength/TypeId/TypMod`) — the owned stand-in for
/// C's `PointerGetDatum(new_data)`.
pub fn toast_flatten_tuple_to_datum<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let num_attrs = tuple_desc.natts;
    debug_assert!(num_attrs <= MaxTupleAttributeNumber);

    // C builds a temporary HeapTupleData control structure (invalid t_self /
    // t_tableOid) just to deform; the deformer here reads only header + data.
    let (mut toast_values, toast_isnull) = deform_split(mcx, tup, tuple_desc)?;

    let mut has_nulls = false;
    for i in 0..num_attrs.max(0) as usize {
        // Look at non-null varlena attributes.
        if toast_isnull[i] {
            has_nulls = true;
        } else if tuple_desc.compact_attrs[i].attlen == -1 {
            if let TupleValue::ByRef(bytes) = &toast_values[i] {
                if varatt_is_external(bytes) || varatt_is_compressed(bytes) {
                    let detoasted = detoast_seams::detoast_attr::call(mcx, bytes)?;
                    toast_values[i] = TupleValue::ByRef(detoasted);
                }
            }
        }
    }
    // heap_form_tuple derives `hasnull` from the isnull array — equal to the
    // C-tracked has_nulls — and performs the same new_header_len /
    // new_data_len computation, sets natts, t_hoff, and the composite-Datum
    // header fields (SetDatumLength/TypeId/TypMod).
    debug_assert_eq!(
        has_nulls,
        toast_isnull[..num_attrs.max(0) as usize].iter().any(|&n| n)
    );

    let mut new_tuple = heap_form_tuple(mcx, tuple_desc, &toast_values, &toast_isnull)
        .map_err(map_heaptuple_error)?;

    // C: memcpy(new_data, tup, SizeofHeapTupleHeader) before the natts/hoff/
    // Datum-field adjustments — t_ctid and the non-fill infomask bits carry
    // over from the source header (t_choice is then overwritten by the
    // composite-Datum fields heap_form_tuple already set).
    let old_hdr = tup
        .tuple
        .t_data
        .as_ref()
        .expect("toast_flatten_tuple_to_datum: tuple has no t_data");
    if let Some(new_hdr) = new_tuple.tuple.t_data.as_mut() {
        new_hdr.t_ctid = old_hdr.t_ctid;
        new_hdr.t_infomask = (old_hdr.t_infomask
            & !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL))
            | (new_hdr.t_infomask & (HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL));
        // HeapTupleHeaderSetNatts(new_data, numAttrs) over the old bits.
        new_hdr.t_infomask2 = old_hdr.t_infomask2;
        HeapTupleHeaderSetNatts(new_hdr, num_attrs as u16);
    }

    Ok(new_tuple)
}

// ---------------------------------------------------------------------------
// toast_build_flattened_tuple
// ---------------------------------------------------------------------------

/// `toast_build_flattened_tuple(tupleDesc, values, isnull)` — build a tuple
/// containing no out-of-line toasted fields. Essentially `heap_form_tuple`,
/// except that it expands any external-data pointers beforehand. (It does not
/// decompress in-line compressed datums.)
pub fn toast_build_flattened_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_desc: &TupleDescData<'_>,
    values: &[TupleValue<'_>],
    isnull: &[bool],
) -> PgResult<FormedTuple<'mcx>> {
    let num_attrs = tuple_desc.natts;
    debug_assert!(num_attrs <= MaxTupleAttributeNumber);

    // We can pass the caller's isnull array directly to heap_form_tuple, but
    // we potentially need to modify the values array (C memcpy's the Datum
    // array; the owned model copies the values into `mcx`).
    let mut new_values: PgVec<'mcx, TupleValue<'mcx>> =
        vec_with_capacity_in(mcx, num_attrs.max(0) as usize)?;
    for v in &values[..num_attrs.max(0) as usize] {
        new_values.push(v.clone_in(mcx)?);
    }

    for i in 0..num_attrs.max(0) as usize {
        // Look at non-null varlena attributes.
        if !isnull[i] && tuple_desc.compact_attrs[i].attlen == -1 {
            if let TupleValue::ByRef(bytes) = &new_values[i] {
                if varatt_is_external(bytes) {
                    let detoasted = detoast_seams::detoast_external_attr::call(mcx, bytes)?;
                    new_values[i] = TupleValue::ByRef(detoasted);
                }
            }
        }
    }

    // Form the reconfigured tuple.
    heap_form_tuple(mcx, tuple_desc, &new_values, isnull).map_err(map_heaptuple_error)
}

// ---------------------------------------------------------------------------
// heap_fetch_toast_slice
// ---------------------------------------------------------------------------

/// `heap_fetch_toast_slice(toastrel, valueid, attrsize, sliceoffset,
/// slicelength, result)` — fetch a TOAST slice from a heap table.
///
/// `toastrel` is the relation from which chunks are fetched; `valueid`
/// identifies the TOAST value; `attrsize` is the total size of the TOAST
/// value; `sliceoffset`/`slicelength` delimit the slice. `result` is the
/// `VARDATA` region of the caller-allocated result varlena (`slicelength`
/// bytes), which this fills. `mcx` holds the scan temporaries.
pub fn heap_fetch_toast_slice(
    mcx: Mcx<'_>,
    toastrel: &Relation<'_>,
    valueid: Oid,
    attrsize: i32,
    sliceoffset: i32,
    slicelength: i32,
    result: &mut [u8],
) -> PgResult<()> {
    let mut toastkey: [ScanKeyData; 3] = [
        ScanKeyData::empty(),
        ScanKeyData::empty(),
        ScanKeyData::empty(),
    ];
    let toasttup_desc = &toastrel.rd_att;

    // C: ((attrsize - 1) / TOAST_MAX_CHUNK_SIZE) + 1; TOAST_MAX_CHUNK_SIZE is
    // a Size expression, so the arithmetic is unsigned in C — wrap to match.
    let totalchunks: i32 = ((attrsize - 1) as usize)
        .wrapping_div(TOAST_MAX_CHUNK_SIZE as usize)
        .wrapping_add(1) as i32;

    // Look for the valid index of toast relation.
    let (toastidxs, valid_index) =
        toast_internals_seams::toast_open_indexes::call(mcx, toastrel.rd_id, AccessShareLock)?;

    let startchunk: i32 =
        (sliceoffset as usize).wrapping_div(TOAST_MAX_CHUNK_SIZE as usize) as i32;
    let endchunk: i32 = ((sliceoffset + slicelength - 1) as usize)
        .wrapping_div(TOAST_MAX_CHUNK_SIZE as usize) as i32;
    debug_assert!(endchunk <= totalchunks);

    // Set up a scan key to fetch from the index.
    ScanKeyInit(
        &mut toastkey[0],
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(valueid),
    );

    // No additional condition if fetching all chunks. Otherwise, use an
    // equality condition for one chunk, and a range condition otherwise.
    let nscankeys: usize = if startchunk == 0 && endchunk == totalchunks - 1 {
        1
    } else if startchunk == endchunk {
        ScanKeyInit(
            &mut toastkey[1],
            2 as AttrNumber,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Datum::from_i32(startchunk),
        );
        2
    } else {
        ScanKeyInit(
            &mut toastkey[1],
            2 as AttrNumber,
            BTGreaterEqualStrategyNumber,
            F_INT4GE,
            Datum::from_i32(startchunk),
        );
        ScanKeyInit(
            &mut toastkey[2],
            2 as AttrNumber,
            BTLessEqualStrategyNumber,
            F_INT4LE,
            Datum::from_i32(endchunk),
        );
        3
    };

    // Prepare for scan.
    let snapshot = toast_internals_seams::get_toast_snapshot::call()?;
    let toastscan = genam_seams::systable_beginscan_ordered::call(
        toastrel.rd_id,
        toastidxs[valid_index as usize],
        snapshot,
        &toastkey[..nscankeys],
    )?;

    // Read the chunks by index.
    //
    // The index is on (valueid, chunkidx) so they will come in order.
    let mut expectedchunk: i32 = startchunk;
    while let Some(ttup) =
        genam_seams::systable_getnext_ordered::call(mcx, toastscan, ForwardScanDirection)?
    {
        // Have a chunk, extract the sequence number and the data.
        let (cur_value, isnull) = fastgetattr(mcx, &ttup, 2, toasttup_desc)?;
        debug_assert!(!isnull);
        // DatumGetInt32(...): the chunk-index column is int4 (by value).
        let curchunk: i32 = match cur_value {
            TupleValue::ByVal(d) => d.as_i32(),
            TupleValue::ByRef(_) => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg_internal(format!(
                        "toast chunk-index column is not by-value for toast value {} in {}",
                        valueid,
                        toastrel.name()
                    ))
                    .into_error());
            }
        };
        // DatumGetPointer(...): the chunk-data column is bytea (by reference).
        let (chunk_value, isnull) = fastgetattr(mcx, &ttup, 3, &toasttup_desc)?;
        debug_assert!(!isnull);
        let chunk: &[u8] = match &chunk_value {
            TupleValue::ByRef(bytes) => bytes,
            TupleValue::ByVal(_) => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg_internal(format!(
                        "toast chunk-data column is not by-reference for toast value {} in {}",
                        valueid,
                        toastrel.name()
                    ))
                    .into_error());
            }
        };

        let (chunksize, chunkdata): (i32, &[u8]) = if !varatt_is_extended(chunk) {
            // chunksize = VARSIZE(chunk) - VARHDRSZ; chunkdata = VARDATA(chunk)
            (
                (varsize_4b(chunk) as i32) - VARHDRSZ,
                &chunk[VARHDRSZ as usize..],
            )
        } else if varatt_is_short(chunk) {
            // could happen due to heap_form_tuple doing its thing
            (
                (varsize_1b(chunk) as i32) - VARHDRSZ_SHORT,
                &chunk[VARHDRSZ_SHORT as usize..],
            )
        } else {
            // should never happen
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "found toasted toast chunk for toast value {} in {}",
                    valueid,
                    toastrel.name()
                ))
                .into_error());
        };

        // Some checks on the data we've found.
        if curchunk != expectedchunk {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg_internal(format!(
                    "unexpected chunk number {} (expected {}) for toast value {} in {}",
                    curchunk,
                    expectedchunk,
                    valueid,
                    toastrel.name()
                ))
                .into_error());
        }
        if curchunk > endchunk {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg_internal(format!(
                    "unexpected chunk number {} (out of range {}..{}) for toast value {} in {}",
                    curchunk,
                    startchunk,
                    endchunk,
                    valueid,
                    toastrel.name()
                ))
                .into_error());
        }
        let expected_size: i32 = if curchunk < totalchunks - 1 {
            TOAST_MAX_CHUNK_SIZE
        } else {
            // attrsize - ((totalchunks - 1) * TOAST_MAX_CHUNK_SIZE), with the
            // product formed in unsigned Size as C does.
            (attrsize as usize).wrapping_sub(
                ((totalchunks - 1) as usize).wrapping_mul(TOAST_MAX_CHUNK_SIZE as usize),
            ) as i32
        };
        if chunksize != expected_size {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg_internal(format!(
                    "unexpected chunk size {} (expected {}) in chunk {} of {} for toast value {} in {}",
                    chunksize,
                    expected_size,
                    curchunk,
                    totalchunks,
                    valueid,
                    toastrel.name()
                ))
                .into_error());
        }

        // Copy the data into proper place in our result.
        let mut chcpystrt: i32 = 0;
        let mut chcpyend: i32 = chunksize - 1;
        if curchunk == startchunk {
            chcpystrt = (sliceoffset as usize).wrapping_rem(TOAST_MAX_CHUNK_SIZE as usize) as i32;
        }
        if curchunk == endchunk {
            chcpyend = ((sliceoffset + slicelength - 1) as usize)
                .wrapping_rem(TOAST_MAX_CHUNK_SIZE as usize) as i32;
        }

        // memcpy(VARDATA(result) + curchunk * TOAST_MAX_CHUNK_SIZE -
        //        sliceoffset + chcpystrt, chunkdata + chcpystrt,
        //        (chcpyend - chcpystrt) + 1);
        let dst_start = ((curchunk as usize).wrapping_mul(TOAST_MAX_CHUNK_SIZE as usize) as isize
            - sliceoffset as isize
            + chcpystrt as isize) as usize;
        let copy_len = (chcpyend - chcpystrt + 1) as usize;
        let src_off = chcpystrt as usize;
        result[dst_start..dst_start + copy_len]
            .copy_from_slice(&chunkdata[src_off..src_off + copy_len]);

        expectedchunk += 1;
    }

    // Final checks that we successfully fetched the datum.
    if expectedchunk != endchunk + 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg_internal(format!(
                "missing chunk number {} for toast value {} in {}",
                expectedchunk,
                valueid,
                toastrel.name()
            ))
            .into_error());
    }

    // End scan and close indexes.
    genam_seams::systable_endscan_ordered::call(toastscan)?;
    toast_internals_seams::toast_close_indexes::call(&toastidxs, AccessShareLock)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

/// `fastgetattr(tup, attnum, tupleDesc, &isnull)` (access/htup_details.h) —
/// fetch one user attribute (1-based, `attnum > 0`). The C inline's
/// `attcacheoff` fast path is a pure optimization over `nocachegetattr`; the
/// general path computes the identical value.
fn fastgetattr<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    attnum: i32,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<(TupleValue<'mcx>, bool)> {
    debug_assert!(attnum > 0);
    let hdr = tup
        .tuple
        .t_data
        .as_ref()
        .expect("fastgetattr: tuple has no t_data");
    // !HeapTupleNoNulls(tup) && att_isnull(attnum - 1, t_bits)
    if (hdr.t_infomask & HEAP_HASNULL) != 0 && att_isnull((attnum - 1) as usize, &hdr.t_bits) {
        return Ok((TupleValue::ByVal(Datum::null()), true));
    }
    Ok((
        nocachegetattr(mcx, &tup.tuple, attnum, tuple_desc, &tup.data)?,
        false,
    ))
}

/// `att_isnull(ATT, BITS)` (access/tupmacs.h): bit clear means null.
#[inline]
fn att_isnull(att: usize, bits: &[u8]) -> bool {
    (bits[att >> 3] & (1 << (att & 7))) == 0
}

/// Split the deformed columns into the parallel `(values, isnull)` arrays C's
/// `heap_deform_tuple` fills.
fn deform_split<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<(PgVec<'mcx, TupleValue<'mcx>>, PgVec<'mcx, bool>)> {
    let columns = heap_deform_tuple(mcx, &tup.tuple, tuple_desc, &tup.data)?;
    let mut values = vec_with_capacity_in(mcx, columns.len())?;
    let mut isnull = vec_with_capacity_in(mcx, columns.len())?;
    for (v, n) in columns {
        values.push(v);
        isnull.push(n);
    }
    Ok((values, isnull))
}

/// Map [`HeapTupleError`] to the `PgError` C raises at the same site.
fn map_heaptuple_error(err: HeapTupleError) -> PgError {
    match err {
        // ereport(ERROR, errcode(ERRCODE_TOO_MANY_COLUMNS), ...)
        HeapTupleError::TooManyColumns { columns, limit } => ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "number of columns ({columns}) exceeds limit ({limit})"
            ))
            .into_error(),
        // heap_form_tuple never raises this (it belongs to
        // heap_modify_tuple_by_cols); mapped for exhaustiveness.
        HeapTupleError::InvalidColumnNumber { attnum } => ereport(ERROR)
            .errmsg_internal(format!("invalid column number {attnum}"))
            .into_error(),
        HeapTupleError::Pg(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam declared in `backend-access-heap-heaptoast-seams`.
pub fn init_seams() {
    backend_access_heap_heaptoast_seams::toast_flatten_tuple_to_datum::set(
        toast_flatten_tuple_to_datum,
    );
    backend_access_heap_heaptoast_seams::toast_flatten_tuple::set(toast_flatten_tuple);
}
