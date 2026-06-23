//! `backend-access-common-indextuple` — the in-memory index-tuple
//! (de)serialization core of `src/backend/access/common/indextuple.c`.
//!
//! Ports every top-level routine of `indextuple.c`:
//!
//! * [`index_form_tuple`] / [`index_form_tuple_context`] — turn a
//!   `(values, isnull)` pair into an on-disk [`FormedIndexTuple`], detoasting
//!   external varlena keys and optionally compressing over-size in-line varlenas
//!   (`TOAST_INDEX_HACK`);
//! * [`index_deform_tuple`] / [`index_deform_tuple_internal`] — break an index
//!   tuple's data area back into per-column `(value, isnull)` pairs;
//! * [`nocache_index_getattr`] — fetch a single (1-based) attribute;
//! * [`CopyIndexTuple`] — a verbatim copy;
//! * [`index_truncate_tuple`] — drop trailing key columns to build a pivot
//!   tuple (the nbtree suffix-truncation primitive).
//!
//! ## The byte / `Datum` model (shared with `heaptuple`)
//!
//! In C an index tuple is one contiguous `palloc` chunk: an `IndexTupleData`
//! header, then (when there are nulls) an `IndexAttributeBitMapData` null bitmap,
//! then `MAXALIGN`-padded user data starting at `IndexInfoFindDataOffset(t_info)`.
//! `index_form_tuple` builds it with `heap_compute_data_size` / `heap_fill_tuple`,
//! and the deform routines are raw pointer arithmetic over the data area.
//!
//! This port keeps the arithmetic identical but, exactly like
//! [`heaptuple`], represents the user-data area as a
//! `PgVec<u8>` and a per-attribute value as a [`Datum`]
//! (`ByVal(Datum)` / `ByRef(PgVec<u8>)`): the header is an owned
//! [`IndexTupleData`], the optional null bitmap travels alongside as
//! [`FormedIndexTuple::bits`], and the `MAXALIGN`-padded user data as
//! [`FormedIndexTuple::data`].
//!
//! ## Genuine externals (seamed, panic until owner lands)
//!
//! * `detoast_external_attr` (`access/common/detoast.c`) — fetch an external
//!   on-disk varlena into an in-line image;
//! * `toast_compress_datum` (`access/common/toast_internals.c`) — compress an
//!   over-size in-line varlena.
//!
//! `CreateTupleDescTruncatedCopy` (`access/common/tupdesc.c`) is now a ported
//! direct dependency.
//!
//! ## Seams this crate INSTALLS (its `init_seams`)
//!
//! `backend-access-common-indextuple-seams` declares the two cross-subsystem
//! adapters consumed by nbtree / nodeIndexonlyscan:
//!
//! * `index_form_tuple(mcx, rel, values, isnull, ht_ctid)` — form from the
//!   index relation's descriptor + bare `Datum` arrays, stamp `t_tid`, return
//!   the on-disk bytes;
//! * `index_deform_tuple(estate, slot, itup, itupdesc)` — deform into a scan
//!   slot's `tts_values`/`tts_isnull`.
//!
//! Their bodies live here as real functions ([`index_form_tuple_seam`] /
//! [`index_deform_tuple_seam`]); `init_seams()` only `set()`s them. The bare-
//! `Datum` <-> by-reference-value bridge is the unresolved slot-payload model
//! frontier: a by-reference column there panics loudly (mirroring
//! `execTuples`), never a silent stub.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;

use ::heaptuple::{heap_compute_data_size, heap_fill_tuple};
use ::utils_error::ereport;
use ::mcx::{slice_in, vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{Size, INDEX_MAX_KEYS};
use ::types_error::{
    PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_TOO_MANY_COLUMNS, ERROR,
};
// The one canonical value type (Datum-unification keystone). The crate's own
// form/deform model is `Datum<'mcx>` (ByVal/ByRef); the former `Datum`
// spelling was a transitional alias for exactly this enum.
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::{
    bits8, CompactAttribute, IndexTupleData, ItemPointerData, TupleDescData,
    HEAP_HASEXTERNAL, HEAP_HASVARWIDTH, INDEX_NULL_MASK, INDEX_SIZE_MASK, INDEX_VAR_MASK,
    TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN,
};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Compile-time ABI constants (mirroring access/itup.h on the 64-bit catalog ABI)
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` on a standard 64-bit build.
const MAXIMUM_ALIGNOF: usize = 8;
/// `sizeof(IndexTupleData)` on the 64-bit ABI (`t_tid` 6 + `t_info` 2 == 8).
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;
/// `sizeof(IndexAttributeBitMapData)` == `INDEX_MAX_KEYS / 8` rounded up == 4.
const SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA: usize = (INDEX_MAX_KEYS as usize + 7) / 8;

/// `BLCKSZ` (default 8 KiB).
const BLCKSZ: usize = 8192;
/// `SizeOfPageHeaderData` (`storage/bufpage.h`) on the 64-bit ABI.
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// `sizeof(ItemIdData)` (`storage/itemid.h`).
const SIZE_OF_ITEM_ID_DATA: usize = 4;
/// `MaxHeapTupleSize` (`access/htup_details.h`):
/// `BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData))`.
const MAX_HEAP_TUPLE_SIZE: usize =
    BLCKSZ - maxalign_const(SIZE_OF_PAGE_HEADER_DATA + SIZE_OF_ITEM_ID_DATA);
/// `TOAST_INDEX_TARGET` (`indextuple.c`): `MaxHeapTupleSize / 16`.
const TOAST_INDEX_TARGET: usize = MAX_HEAP_TUPLE_SIZE / 16;

const fn maxalign_const(value: usize) -> usize {
    (value + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

#[inline]
fn maxalign(value: usize) -> usize {
    maxalign_const(value)
}

/// `IndexInfoFindDataOffset(t_info)` (`access/itup.h`): the data-area offset
/// within an index tuple — `MAXALIGN(sizeof(IndexTupleData))` with no nulls,
/// `MAXALIGN(sizeof(IndexTupleData) + sizeof(IndexAttributeBitMapData))` with.
#[inline]
fn index_info_find_data_offset(t_info: u16) -> Size {
    if (t_info & INDEX_NULL_MASK) == 0 {
        maxalign(SIZEOF_INDEX_TUPLE_DATA)
    } else {
        maxalign(SIZEOF_INDEX_TUPLE_DATA + SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA)
    }
}

// ---------------------------------------------------------------------------
// FormedIndexTuple — the owned on-disk index tuple
// ---------------------------------------------------------------------------

/// A fully-formed index tuple: the owned [`IndexTupleData`] header, its optional
/// null bitmap, and the user-data area bytes.
///
/// In C the header, optional null bitmap, and `MAXALIGN`-padded user data are
/// one contiguous `palloc` chunk; here the header lives in
/// [`FormedIndexTuple::header`], the bitmap (the `IndexAttributeBitMapData`
/// region, empty when there are no nulls) in [`FormedIndexTuple::bits`], and the
/// user data in [`FormedIndexTuple::data`].
#[derive(Debug)]
pub struct FormedIndexTuple<'mcx> {
    /// The index-tuple header (`t_tid` + `t_info`).
    pub header: IndexTupleData,
    /// The null bitmap (`IndexAttributeBitMapData` bytes), empty when the tuple
    /// has no nulls (C: `bp == NULL`).
    pub bits: PgVec<'mcx, bits8>,
    /// The user-data area: the `MAXALIGN`-padded bytes at `tuple + hoff`.
    pub data: PgVec<'mcx, u8>,
}

impl<'mcx> FormedIndexTuple<'mcx> {
    /// `IndexTupleSize(itup)` (`access/itup.h`).
    #[inline]
    pub fn size(&self) -> Size {
        (self.header.t_info & INDEX_SIZE_MASK) as Size
    }

    /// `IndexTupleHasNulls(itup)`.
    #[inline]
    pub fn has_nulls(&self) -> bool {
        (self.header.t_info & INDEX_NULL_MASK) != 0
    }

    /// `IndexTupleHasVarwidths(itup)`.
    #[inline]
    pub fn has_varwidths(&self) -> bool {
        (self.header.t_info & INDEX_VAR_MASK) != 0
    }

    /// `IndexInfoFindDataOffset(itup->t_info)`.
    #[inline]
    pub fn data_offset(&self) -> Size {
        index_info_find_data_offset(self.header.t_info)
    }

    /// The null bitmap as a slice, or `None` when the tuple has no nulls.
    #[inline]
    fn null_bitmap(&self) -> Option<&[bits8]> {
        if self.has_nulls() {
            Some(&self.bits)
        } else {
            None
        }
    }

    /// Parse a contiguous on-disk index-tuple byte image (exactly the layout
    /// [`on_disk_image`](Self::on_disk_image) produces, i.e. what nbtree carries
    /// on a page) into the owned field split: the 8-byte header, the null
    /// bitmap (when `INDEX_NULL_MASK` is set), then the user-data area at
    /// `IndexInfoFindDataOffset(t_info)`. The inverse of `on_disk_image`.
    pub fn from_on_disk_image<'b>(mcx: Mcx<'b>, itup: &[u8]) -> PgResult<FormedIndexTuple<'b>> {
        if itup.len() < SIZEOF_INDEX_TUPLE_DATA {
            return Err(PgError::error("index tuple image shorter than its header"));
        }
        let mut t_tid = ItemPointerData::default();
        t_tid.ip_blkid.bi_hi = u16::from_ne_bytes([itup[0], itup[1]]);
        t_tid.ip_blkid.bi_lo = u16::from_ne_bytes([itup[2], itup[3]]);
        t_tid.ip_posid = u16::from_ne_bytes([itup[4], itup[5]]);
        let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
        let header = IndexTupleData { t_tid, t_info };

        let total = (t_info & INDEX_SIZE_MASK) as usize;
        if itup.len() < total {
            return Err(PgError::error("index tuple image shorter than its size field"));
        }
        let data_off = index_info_find_data_offset(t_info);

        let bits: PgVec<'b, bits8> = if (t_info & INDEX_NULL_MASK) != 0 {
            let nb = data_off.saturating_sub(SIZEOF_INDEX_TUPLE_DATA);
            slice_in(mcx, &itup[SIZEOF_INDEX_TUPLE_DATA..SIZEOF_INDEX_TUPLE_DATA + nb])?
        } else {
            slice_in(mcx, &[])?
        };
        let data: PgVec<'b, u8> = slice_in(mcx, &itup[data_off..total])?;

        Ok(FormedIndexTuple { header, bits, data })
    }

    /// A deep copy of the tuple into `mcx` (the owned analogue of C's single
    /// `memcpy(result, source, IndexTupleSize(source))`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FormedIndexTuple<'b>> {
        Ok(FormedIndexTuple {
            header: self.header,
            bits: slice_in(mcx, &self.bits)?,
            data: slice_in(mcx, &self.data)?,
        })
    }

    /// The tuple's on-disk byte image: the 8-byte `IndexTupleData` header
    /// (`t_tid` 6 + `t_info` 2), the null bitmap when present, zero pad to
    /// `IndexInfoFindDataOffset(t_info)`, then the user data. In C this layout
    /// IS the `IndexTuple` palloc'd by `index_form_tuple`; the owned model
    /// splits it into fields and this is the inverse.
    pub fn on_disk_image<'b>(&self, mcx: Mcx<'b>) -> PgResult<PgVec<'b, u8>> {
        let total = self.size();
        let mut out = vec_with_capacity_in(mcx, total)?;
        out.resize(total, 0);
        out[0..2].copy_from_slice(&self.header.t_tid.ip_blkid.bi_hi.to_ne_bytes());
        out[2..4].copy_from_slice(&self.header.t_tid.ip_blkid.bi_lo.to_ne_bytes());
        out[4..6].copy_from_slice(&self.header.t_tid.ip_posid.to_ne_bytes());
        out[6..8].copy_from_slice(&self.header.t_info.to_ne_bytes());
        let data_off = self.data_offset();
        if !self.bits.is_empty() {
            let nb = self.bits.len().min(data_off.saturating_sub(SIZEOF_INDEX_TUPLE_DATA));
            out[SIZEOF_INDEX_TUPLE_DATA..SIZEOF_INDEX_TUPLE_DATA + nb]
                .copy_from_slice(&self.bits[..nb]);
        }
        let n = self.data.len().min(total.saturating_sub(data_off));
        out[data_off..data_off + n].copy_from_slice(&self.data[..n]);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// index_form_tuple / index_form_tuple_context (indextuple.c:43 / :64)
// ---------------------------------------------------------------------------

/// `index_form_tuple(tupleDescriptor, values, isnull)` (indextuple.c:43) — build
/// an index tuple in the current memory context.
pub fn index_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> PgResult<FormedIndexTuple<'mcx>> {
    index_form_tuple_context(mcx, tuple_descriptor, values, isnull)
}

/// `index_form_tuple_context(tupleDescriptor, values, isnull, context)`
/// (indextuple.c:64) — the worker behind [`index_form_tuple`] (the owned model
/// has no separate target memory context distinct from `mcx`).
pub fn index_form_tuple_context<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> PgResult<FormedIndexTuple<'mcx>> {
    let number_of_attributes = tuple_descriptor.natts;

    if number_of_attributes > INDEX_MAX_KEYS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "number of index columns ({number_of_attributes}) exceeds limit ({INDEX_MAX_KEYS})"
            ))
            .into_error());
    }

    let natts = number_of_attributes as usize;

    // TOAST_INDEX_HACK: untoasted_values[INDEX_MAX_KEYS] = {0}; a per-attribute
    // copy of `values` with TOAST-detoasted / compressed substitutions.  In the
    // owned model the substituted values carry their own bytes, so the C
    // `untoasted_free[]` pfree loop is the drop of these owned values.
    let mut untoasted_values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, natts)?;
    for i in 0..natts {
        // untoasted_values[i] = values[i];
        // C: Form_pg_attribute att = TupleDescAttr(tupleDescriptor, i);
        let att = tuple_descriptor.attr(i);

        // Do nothing if value is NULL or not of varlena type (attlen != -1).
        if isnull[i] || att.attlen != -1 {
            untoasted_values.push(clone_tuple_value(mcx, &values[i])?);
            continue;
        }

        // If value is stored EXTERNAL, fetch it so we are not depending on
        // outside storage.
        let mut value = clone_tuple_value(mcx, &values[i])?;
        if varatt_is_external(value.as_ref_bytes()) {
            let detoasted = detoast_seams::detoast_external_attr::call(
                mcx,
                value.as_ref_bytes(),
            )?;
            value = Datum::ByRef(detoasted);
        }

        // If value is above size target and is of a compressible datatype, try
        // to compress it in-line.
        // !VARATT_IS_EXTENDED(p) && VARSIZE(p) > TOAST_INDEX_TARGET &&
        //   (attstorage == EXTENDED || attstorage == MAIN)
        if !varatt_is_extended(value.as_ref_bytes())
            && varsize_4b_len(value.as_ref_bytes()) > TOAST_INDEX_TARGET
            && (att.attstorage == TYPSTORAGE_EXTENDED || att.attstorage == TYPSTORAGE_MAIN)
        {
            if let Some(compressed) =
                toast_internals_seams::toast_compress_datum::call(
                    mcx,
                    value.as_ref_bytes(),
                    att.attcompression,
                )?
            {
                // successful compression
                value = Datum::ByRef(compressed);
            }
        }

        untoasted_values.push(value);
    }

    // Check for nulls.
    let mut hasnull = false;
    for i in 0..natts {
        if isnull[i] {
            hasnull = true;
            break;
        }
    }

    let mut infomask: u16 = 0;
    if hasnull {
        infomask |= INDEX_NULL_MASK;
    }

    let hoff = index_info_find_data_offset(infomask);
    let data_size = heap_compute_data_size(tuple_descriptor, &untoasted_values, isnull)?;
    let mut size = hoff + data_size;
    size = maxalign(size); // be conservative

    // heap_fill_tuple writes the data area, the null bitmap, and the tupmask.
    let filled = heap_fill_tuple(mcx, tuple_descriptor, &untoasted_values, isnull, data_size, hasnull)?;

    // heap_fill_tuple sets a HeapTuple-style "tupmask"; the only relevant info
    // is the "has variable attributes" field.  hasnull was set above.
    if (filled.infomask & HEAP_HASVARWIDTH) != 0 {
        infomask |= INDEX_VAR_MASK;
    }

    // Assert we got rid of external attributes (C: Assert((tupmask &
    // HEAP_HASEXTERNAL) == 0)).  Surfaced as a fail-fast error here.
    if (filled.infomask & HEAP_HASEXTERNAL) != 0 {
        return Err(PgError::error(
            "index tuple still contains external attributes",
        ));
    }

    // Make sure the size will fit in the field reserved for it in t_info.
    if (size & INDEX_SIZE_MASK as usize) != size {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "index row requires {size} bytes, maximum size is {}",
                INDEX_SIZE_MASK as Size
            ))
            .into_error());
    }

    infomask |= size as u16;

    Ok(FormedIndexTuple {
        // tuple->t_info = infomask;  t_tid is left as the zeroed allocation gave
        // it (ItemPointer (0,0,0)); callers (nbtree) overwrite it.
        header: IndexTupleData {
            t_tid: ItemPointerData::default(),
            t_info: infomask,
        },
        bits: filled.bits,
        data: filled.data,
    })
}

// ---------------------------------------------------------------------------
// nocache_index_getattr (indextuple.c:240)
// ---------------------------------------------------------------------------

/// One column produced by [`index_deform_tuple`] / [`nocache_index_getattr`]:
/// a `(value, isnull)` pair (a null column is `(ByVal(Datum::null()), true)`).
pub type IndexColumn<'mcx> = (Datum<'mcx>, bool);

/// `nocache_index_getattr(tup, attnum, tupleDesc)` (indextuple.c:240) — fetch a
/// single attribute of an index tuple, walking only as far as the target column.
///
/// `attnum` is 1-based, as in C. C caches `attcacheoff` on the descriptor; the
/// descriptor is borrowed immutably here so the cache writes are omitted (a pure
/// performance optimization; the computed offsets are identical).
pub fn nocache_index_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedIndexTuple<'_>,
    attnum: i32,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<IndexColumn<'mcx>> {
    nocache_index_getattr_internal(mcx, attnum, tuple_desc, tup.data.as_slice(), tup.null_bitmap())
}

/// `nocache_index_getattr` working directly on the data-area slice `tp` and the
/// optional null bitmap `bp` (the `(tp, bp)` pair `index_deform_tuple_internal`
/// also consumes), so callers that carry the on-disk byte image — rather than a
/// `FormedIndexTuple` — can reuse the exact same single-attribute walk.
pub fn nocache_index_getattr_internal<'mcx>(
    mcx: Mcx<'mcx>,
    attnum: i32,
    tuple_desc: &TupleDescData<'_>,
    tp: &[u8],
    bp: Option<&[bits8]>,
) -> PgResult<IndexColumn<'mcx>> {
    // C: attnum-- (1-based to 0-based); Assert(attnum > 0).
    if attnum < 1 || attnum > tuple_desc.natts {
        return Err(PgError::error(format!("invalid index attnum: {attnum}")));
    }

    let index = (attnum - 1) as usize;

    // C uses IndexTupleHasNulls + att_isnull; a 0 bit means NULL.  When the
    // target itself is null, fetchatt is never called in C (index_getattr's
    // wrapper checks att_isnull first), but nocache_index_getattr's preceding-
    // nulls scan still walks the bitmap; mirror that by returning NULL for the
    // target if it is null.
    if bp.is_some_and(|bits| att_isnull(index, bits)) {
        return Ok((Datum::null(), true));
    }

    // tp = (char *) tup + IndexInfoFindDataOffset(tup->t_info);
    let mut off = 0usize;
    let mut slow = false;

    // The loop walks attributes 0..=index, breaking at the target (C breaks when
    // i == attnum after computing the target offset).
    for cur in 0..=index {
        let thisatt = tuple_desc.compact_attr(cur);

        if bp.is_some_and(|bits| att_isnull(cur, bits)) {
            // An earlier attribute is null: no storage, no alignment padding.
            slow = true;
            continue;
        }

        if !slow && thisatt.attcacheoff >= 0 {
            off = thisatt.attcacheoff as usize;
        } else if thisatt.attlen == -1 {
            if !slow && off == att_nominal_alignby(off, thisatt.attalignby) {
                // C caches thisatt->attcacheoff = off (omitted; offsets identical).
            } else {
                off = att_pointer_alignby(tp, off, thisatt.attalignby, -1);
                slow = true;
            }
        } else {
            off = att_nominal_alignby(off, thisatt.attalignby);
            // if (usecache) thisatt->attcacheoff = off;  (cache write omitted)
        }

        if cur == index {
            return fetchatt(mcx, thisatt, tp, off).map(|v| (v, false));
        }

        off = att_addlength_pointer(off, thisatt.attlen, tp, off);
        if thisatt.attlen <= 0 {
            slow = true;
        }
    }

    // Unreachable: the `cur == index` arm always returns for a non-null target.
    Ok((Datum::null(), true))
}

// ---------------------------------------------------------------------------
// index_deform_tuple / index_deform_tuple_internal (indextuple.c:455 / :478)
// ---------------------------------------------------------------------------

/// `index_deform_tuple(tup, tupleDescriptor, values, isnull)` (indextuple.c:455)
/// — break an index tuple into per-column `(value, isnull)` pairs.
pub fn index_deform_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedIndexTuple<'_>,
    tuple_descriptor: &TupleDescData<'_>,
) -> PgResult<PgVec<'mcx, IndexColumn<'mcx>>> {
    // C: index_deform_tuple_internal(desc, values, isnull,
    //        (char*) tup + IndexInfoFindDataOffset(tup->t_info),
    //        (bits8*) tup + sizeof(IndexTupleData), IndexTupleHasNulls(tup));
    index_deform_tuple_internal(mcx, tuple_descriptor, tup.data.as_slice(), tup.null_bitmap())
}

/// `index_deform_tuple_internal(tupleDescriptor, values, isnull, tp, bp, hasnulls)`
/// (indextuple.c:478) — the offset-walking worker.
///
/// `tp` is the data area; `bp` is the null bitmap (`None` ⇒ `hasnulls == false`).
/// C caches `attcacheoff` on the descriptor; the immutable borrow omits those
/// writes (offsets are identical).
pub fn index_deform_tuple_internal<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    tp: &[u8],
    bp: Option<&[bits8]>,
) -> PgResult<PgVec<'mcx, IndexColumn<'mcx>>> {
    let natts = tuple_descriptor.natts;
    // Assert to protect callers who allocate fixed-size arrays.
    debug_assert!(natts <= INDEX_MAX_KEYS);
    let natts = natts as usize;
    let hasnulls = bp.is_some();

    let mut out: PgVec<'mcx, IndexColumn<'mcx>> = vec_with_capacity_in(mcx, natts)?;
    let mut off = 0usize;
    let mut slow = false;

    for attnum in 0..natts {
        let thisatt = tuple_descriptor.compact_attr(attnum);

        if hasnulls && bp.is_some_and(|bits| att_isnull(attnum, bits)) {
            // values[attnum] = (Datum) 0; isnull[attnum] = true;
            out.push((Datum::null(), true));
            slow = true; // can't use attcacheoff anymore
            continue;
        }

        // isnull[attnum] = false;
        if !slow && thisatt.attcacheoff >= 0 {
            off = thisatt.attcacheoff as usize;
        } else if thisatt.attlen == -1 {
            if !slow && off == att_nominal_alignby(off, thisatt.attalignby) {
                // C caches thisatt->attcacheoff = off (omitted; offsets identical).
            } else {
                off = att_pointer_alignby(tp, off, thisatt.attalignby, -1);
                slow = true;
            }
        } else {
            off = att_nominal_alignby(off, thisatt.attalignby);
            // if (!slow) thisatt->attcacheoff = off;  (cache write omitted)
        }

        // values[attnum] = fetchatt(thisatt, tp + off);
        out.push((fetchatt(mcx, thisatt, tp, off)?, false));

        // off = att_addlength_pointer(off, thisatt->attlen, tp + off);
        off = att_addlength_pointer(off, thisatt.attlen, tp, off);
        if thisatt.attlen <= 0 {
            slow = true;
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// CopyIndexTuple (indextuple.c:546)
// ---------------------------------------------------------------------------

/// `CopyIndexTuple(source)` (indextuple.c:546) — return a copy of an index
/// tuple. C `memcpy`s `IndexTupleSize(source)` bytes into a fresh `palloc`; the
/// owned model deep-clones header, bitmap, and data area into `mcx`.
pub fn CopyIndexTuple<'mcx>(
    mcx: Mcx<'mcx>,
    source: &FormedIndexTuple<'_>,
) -> PgResult<FormedIndexTuple<'mcx>> {
    source.clone_in(mcx)
}

// ---------------------------------------------------------------------------
// index_truncate_tuple (indextuple.c:575)
// ---------------------------------------------------------------------------

/// `index_truncate_tuple(sourceDescriptor, source, leavenatts)`
/// (indextuple.c:575) — reform an index tuple keeping only its first
/// `leavenatts` key attributes (nbtree suffix truncation, building a pivot
/// tuple). The original heap TID (`t_tid`) is preserved.
pub fn index_truncate_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    source_descriptor: &TupleDescData<'_>,
    source: &FormedIndexTuple<'_>,
    leavenatts: i32,
) -> PgResult<FormedIndexTuple<'mcx>> {
    // Assert(leavenatts <= sourceDescriptor->natts).
    debug_assert!(leavenatts <= source_descriptor.natts);

    // Easy case: no truncation actually required.
    if leavenatts == source_descriptor.natts {
        return CopyIndexTuple(mcx, source);
    }

    // Create temporary truncated tuple descriptor.
    let truncdesc =
        tupdesc::CreateTupleDescTruncatedCopy(mcx, source_descriptor, leavenatts)?;

    // Deform, form copy of tuple with fewer attributes.  C uses fixed
    // values[INDEX_MAX_KEYS]/isnull[INDEX_MAX_KEYS]; here index_deform_tuple_internal
    // returns exactly truncdesc.natts columns.
    let columns =
        index_deform_tuple_internal(mcx, &truncdesc, source.data.as_slice(), source.null_bitmap())?;
    let n = columns.len();
    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, n)?;
    let mut isnull: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, n)?;
    for (val, null) in columns {
        values.push(val);
        isnull.push(null);
    }

    let mut truncated = index_form_tuple(mcx, &truncdesc, &values, &isnull)?;

    // truncated->t_tid = source->t_tid;
    truncated.header.t_tid = source.header.t_tid;

    // Assert(IndexTupleSize(truncated) <= IndexTupleSize(source)).
    debug_assert!(truncated.size() <= source.size());

    // (C pfree(truncdesc) is the drop of `truncdesc` here.)
    Ok(truncated)
}

// ===========================================================================
// Seam adapters (installed by init_seams) — the cross-subsystem entry points
// declared in backend-access-common-indextuple-seams.
// ===========================================================================

/// Body for the `index_form_tuple` seam consumed by nbtree (`btinsert`):
/// `index_form_tuple(RelationGetDescr(rel), values, isnull)` with
/// `itup->t_tid = ht_ctid`, returning the formed on-disk bytes.
///
/// The descriptor comes from `rel.rd_att`; the partition-key values now arrive
/// as the canonical [`Datum`] carrier, so they thread straight into
/// [`index_form_tuple`].
pub fn index_form_tuple_seam<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: ItemPointerData,
) -> PgResult<PgVec<'mcx, u8>> {
    let tupdesc = rel.rd_att.as_ref();

    let mut itup = index_form_tuple(mcx, tupdesc, values, isnull)?;
    // itup->t_tid = *ht_ctid;
    itup.header.t_tid = ht_ctid;
    itup.on_disk_image(mcx)
}

/// Body for the `index_form_tuple_desc` seam consumed by GiST
/// (`gistFormTuple`): `index_form_tuple(tupdesc, values, isnull)` against a
/// caller-supplied descriptor (the leaf or truncated non-leaf descriptor),
/// returning the formed on-disk bytes. Unlike [`index_form_tuple_seam`] it does
/// not stamp `t_tid` (the caller — `gistFormTuple` — sets the offset to
/// `0xffff` on its own copy).
pub fn index_form_tuple_desc_seam<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<PgVec<'mcx, u8>> {
    let itup = index_form_tuple(mcx, tupdesc, values, isnull)?;
    itup.on_disk_image(mcx)
}

/// Body for the `index_deform_tuple` seam consumed by nodeIndexonlyscan
/// (`StoreIndexTuple`): deform the on-disk index-tuple byte image `itup`
/// against `itupdesc` into per-attribute `(value, isnull)` pairs.
///
/// `itup` is the contiguous `xs_itup` carrier exactly as `index_form_tuple`
/// lays it out (header / null bitmap / `MAXALIGN`-padded user data). C does
/// ```c
/// tp = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
/// bp = (bits8 *) itup + sizeof(IndexTupleData);
/// index_deform_tuple_internal(itupdesc, values, isnull, tp, bp,
///                             IndexTupleHasNulls(itup));
/// ```
/// here we read `t_info` out of the byte image, slice out the data area and
/// the bitmap, and hand them to [`index_deform_tuple_internal`]. The caller
/// writes the returned columns into the slot's `tts_values`/`tts_isnull`.
pub fn index_deform_tuple_seam<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
    itupdesc: &TupleDescData<'_>,
) -> PgResult<PgVec<'mcx, IndexColumn<'mcx>>> {
    // itup->t_info is the 2-byte field at offset 6 of IndexTupleData.
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    let hasnulls = (t_info & INDEX_NULL_MASK) != 0;
    // tp = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
    let data_off = index_info_find_data_offset(t_info);
    let tp = &itup[data_off..];
    // bp = (bits8 *) itup + sizeof(IndexTupleData); only consulted when hasnulls.
    let bp = if hasnulls {
        Some(&itup[SIZEOF_INDEX_TUPLE_DATA..])
    } else {
        None
    };
    index_deform_tuple_internal(mcx, itupdesc, tp, bp)
}

/// Body for the `nocache_index_getattr` seam consumed by nbtree
/// (`_bt_compare` / scankey value extraction): fetch a single (1-based)
/// attribute out of the on-disk index-tuple byte image `itup` against
/// `itupdesc`.
///
/// `itup` is laid out exactly as `index_deform_tuple_seam` expects (header /
/// null bitmap / `MAXALIGN`-padded data); we read `t_info`, slice the data
/// area and (when present) the bitmap, and hand them to
/// [`nocache_index_getattr_internal`].
pub fn nocache_index_getattr_seam<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
    attnum: i32,
    itupdesc: &TupleDescData<'_>,
) -> PgResult<IndexColumn<'mcx>> {
    // itup->t_info is the 2-byte field at offset 6 of IndexTupleData.
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    let hasnulls = (t_info & INDEX_NULL_MASK) != 0;
    // tp = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
    let data_off = index_info_find_data_offset(t_info);
    let tp = &itup[data_off..];
    // bp = (bits8 *) itup + sizeof(IndexTupleData); only consulted when hasnulls.
    let bp = if hasnulls {
        Some(&itup[SIZEOF_INDEX_TUPLE_DATA..])
    } else {
        None
    };
    nocache_index_getattr_internal(mcx, attnum, itupdesc, tp, bp)
}

/// Body for the `index_truncate_tuple` seam consumed by nbtree (`_bt_truncate`):
/// `index_truncate_tuple(RelationGetDescr(rel), source, leavenatts)` over a
/// byte-sliced index tuple. nbtree carries the source as an on-page byte image,
/// so we parse it into a [`FormedIndexTuple`], call the real
/// [`index_truncate_tuple`] against `rel.rd_att`, and serialize the truncated
/// pivot back to on-disk bytes.
pub fn index_truncate_tuple_seam<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    source: &[u8],
    leavenatts: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let tupdesc = rel.rd_att.as_ref();
    let formed = FormedIndexTuple::from_on_disk_image(mcx, source)?;
    let truncated = index_truncate_tuple(mcx, tupdesc, &formed, leavenatts)?;
    truncated.on_disk_image(mcx)
}

/// Wire this crate's seams (declared in
/// `backend-access-common-indextuple-seams`) to their real bodies.
pub fn init_seams() {
    indextuple_seams::index_form_tuple::set(index_form_tuple_seam);
    indextuple_seams::index_form_tuple_desc::set(index_form_tuple_desc_seam);
    indextuple_seams::index_deform_tuple::set(index_deform_tuple_seam);
    indextuple_seams::nocache_index_getattr::set(nocache_index_getattr_seam);
    indextuple_seams::index_truncate_tuple::set(index_truncate_tuple_seam);
}

// ---------------------------------------------------------------------------
// Bare-word `Datum` <-> canonical `Datum<'mcx>` bridge (the slot-payload frontier)
// ---------------------------------------------------------------------------

/// Clone a [`Datum`] into `mcx` (its `ByRef` bytes are copied).
fn clone_tuple_value<'mcx>(mcx: Mcx<'mcx>, value: &Datum<'_>) -> PgResult<Datum<'mcx>> {
    Ok(match value {
        Datum::ByVal(d) => Datum::ByVal(*d),
        Datum::ByRef(b) => Datum::ByRef(slice_in(mcx, b)?),
        Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
            panic!("clone_tuple_value: non-ByVal/ByRef Datum arm not yet produced — wave 2")
        }
    })
}

// ---------------------------------------------------------------------------
// Alignment + varlena + fetch helpers (tupmacs.h / varatt.h), ported 1:1.
// ---------------------------------------------------------------------------

/// `att_isnull(att, bits)` (tupmacs.h): a 0 bit in the null bitmap means NULL.
#[inline]
fn att_isnull(att: usize, bits: &[bits8]) -> bool {
    (bits[att >> 3] & (1u8 << (att & 0x07))) == 0
}

/// `att_nominal_alignby(cur_offset, attalignby)` (tupmacs.h):
/// `TYPEALIGN(attalignby, cur_offset)`.
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    let align = attalignby as usize;
    if align <= 1 {
        cur_offset
    } else {
        (cur_offset + align - 1) & !(align - 1)
    }
}

/// `att_pointer_alignby(cur_offset, attalignby, attlen, attptr=&tp[off..])`
/// (tupmacs.h): no alignment when a varlena field's first byte is not a pad byte
/// (`VARATT_NOT_PAD_BYTE(ptr)` == `*(ptr) != 0`), else align.
#[inline]
fn att_pointer_alignby(tp: &[u8], cur_offset: usize, attalignby: u8, attlen: i16) -> usize {
    if attlen == -1 && tp.get(cur_offset).copied().unwrap_or(0) != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr=&tp[off..])` (tupmacs.h).
#[inline]
fn att_addlength_pointer(cur_offset: usize, attlen: i16, tp: &[u8], off: usize) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(&tp[off..])
    } else {
        debug_assert_eq!(attlen, -2);
        // strlen + 1
        let bytes = &tp[off..];
        let mut len = 0usize;
        while bytes[len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

/// `fetchatt(att, &tp[off..])` (tupmacs.h): for a by-value att read the scalar;
/// for a by-reference att copy its on-disk field span (C returns a pointer into
/// the tuple — here we copy the exact field bytes into `mcx`).
#[inline]
fn fetchatt<'mcx>(
    mcx: Mcx<'mcx>,
    att: &CompactAttribute,
    tp: &[u8],
    off: usize,
) -> PgResult<Datum<'mcx>> {
    if att.attbyval {
        Ok(fetch_att_byval(tp, off, att.attlen))
    } else {
        let end = att_addlength_pointer(off, att.attlen, tp, off);
        Ok(Datum::ByRef(slice_in(mcx, &tp[off..end])?))
    }
}

/// `fetch_att(T, attbyval=true, attlen)` (tupmacs.h) for a by-value field.
#[inline]
fn fetch_att_byval<'mcx>(tp: &[u8], off: usize, attlen: i16) -> Datum<'mcx> {
    match attlen {
        1 => Datum::from_usize(tp[off] as i8 as i64 as usize),
        2 => Datum::from_usize(i16::from_ne_bytes([tp[off], tp[off + 1]]) as i64 as usize),
        4 => Datum::from_usize(
            i32::from_ne_bytes([tp[off], tp[off + 1], tp[off + 2], tp[off + 3]]) as i64 as usize,
        ),
        8 => Datum::from_usize(usize::from_ne_bytes([
            tp[off],
            tp[off + 1],
            tp[off + 2],
            tp[off + 3],
            tp[off + 4],
            tp[off + 5],
            tp[off + 6],
            tp[off + 7],
        ])),
        _ => panic!("unsupported byval length: {attlen}"),
    }
}

/// `VARATT_IS_1B_E(PTR)` (varatt.h, little-endian): a TOAST pointer
/// (`va_header == 0x01`).
#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    varatt_is_1b_e(b)
}

/// `VARATT_IS_1B(PTR)` (varatt.h, little-endian): a 1-byte ("short") header.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_4B(PTR)` (varatt.h, little-endian): a 4-byte header (low bit 0).
#[inline]
fn varatt_is_4b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x00
}

/// `VARATT_IS_4B_C(PTR)` (varatt.h, little-endian): a compressed 4-byte header
/// (low two bits == 10).
#[inline]
fn varatt_is_4b_c(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

/// `VARATT_IS_COMPRESSED(PTR)` == `VARATT_IS_4B_C(PTR)`.
#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    varatt_is_4b_c(b)
}

/// `VARATT_IS_EXTENDED(PTR)` (varatt.h): `!VARATT_IS_4B_U(PTR)` — i.e. short,
/// compressed, or external (anything that is not a plain uncompressed 4-byte
/// varlena).
#[inline]
fn varatt_is_extended(b: &[u8]) -> bool {
    !(varatt_is_4b(b) && !varatt_is_compressed(b))
}

/// `VARSIZE_4B(PTR)` (varatt.h, little-endian): `(va_header >> 2) & 0x3FFFFFFF`.
#[inline]
fn varsize_4b_len(b: &[u8]) -> usize {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARSIZE_1B(PTR)` (varatt.h, little-endian): `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}

/// `VARTAG_SIZE(tag)` (varatt.h): on-disk size of a TOAST pointer.
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_ONDISK: u8 = 18;
    if tag == VARTAG_INDIRECT {
        // sizeof(varatt_indirect) == sizeof(struct varlena *)
        8
    } else if (tag & !1) == VARTAG_EXPANDED_RO {
        // sizeof(varatt_expanded) == sizeof(ExpandedObjectHeader *)
        8
    } else if tag == VARTAG_ONDISK {
        // sizeof(varatt_external): 4 x 4 bytes.
        16
    } else {
        debug_assert!(false, "invalid varlena TOAST tag");
        0
    }
}

/// `VARSIZE_EXTERNAL(PTR)` (varatt.h): `VARHDRSZ_EXTERNAL (2) + VARTAG_SIZE(tag)`.
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    2 + vartag_size(b[1])
}

/// `VARSIZE_ANY(ptr)` (varatt.h) for an in-line varlena starting at `b[0]`.
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b)
    } else {
        varsize_4b_len(b)
    }
}
