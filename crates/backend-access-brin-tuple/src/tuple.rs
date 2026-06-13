//! The BRIN index-tuple codec: form / deform / placeholder / copy / equal /
//! memtuple lifecycle — a port of `brin_tuple.c`'s body.

use mcx::{slice_in, vec_with_capacity_in, Mcx, PgVec};
use types_brin::{
    BrinDesc, BrinMemTuple, BrinValues, BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_OFFSET_MASK,
    BRIN_PLACEHOLDER_MASK, SIZE_OF_BRIN_TUPLE,
};
use types_core::{BlockNumber, Size};
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::TupleValue;
use types_typcache::{TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN};

use backend_access_common_heaptuple::{heap_compute_data_size, heap_fill_tuple};

use crate::internal::{
    att_isnull, bitmaplen, brin_tuple_data_offset, brin_tuple_get_blkno, brin_tuple_has_nulls,
    brin_tuple_is_empty_range, brin_tuple_is_placeholder, brtuple_disk_tupdesc, maxalign, varsize,
    varsize_any, varatt_is_extended, varatt_is_external, BrinTupleImage, HIGHBIT,
};

/// `TOAST_INDEX_TARGET` (`heaptoast.h`): the size above which `brin_form_tuple`
/// tries to compress a varlena summary datum in-line (`TOAST_TUPLE_TARGET / 4`,
/// `TOAST_TUPLE_TARGET == MaximumBytesPerTuple(4) == 8160`).
pub const TOAST_INDEX_TARGET: usize = 8160 / 4;

/// `InvalidCompressionMethod` (`toast_compression.h`).
pub const INVALID_COMPRESSION_METHOD: i8 = -1;

// ---------------------------------------------------------------------------
// brin_form_tuple (brin_tuple.c:98)
// ---------------------------------------------------------------------------

/// `brin_form_tuple(brdesc, blkno, tuple, &size)` (brin_tuple.c:98): generate a
/// new on-disk tuple for the page range starting at `blkno`. Returns the byte
/// image and its length (the C `*size`). Allocated in `mcx`.
///
/// See [`brin_form_placeholder_tuple`] if you touch this.
pub fn brin_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
    blkno: BlockNumber,
    tuple: &BrinMemTuple<'_>,
) -> PgResult<(BrinTupleImage<'mcx>, Size)> {
    let total = brdesc.bd_totalstored as usize;
    debug_assert!(brdesc.bd_totalstored > 0);

    // values = palloc(Datum * bd_totalstored); nulls = palloc0(bool * total).
    // In the byte model, `values` are TupleValues; default-fill with empty
    // ByVal(0) placeholders for slots never written (the C palloc leaves them
    // uninitialized but only the written slots are read by heap_fill_tuple).
    let mut values: PgVec<'mcx, TupleValue<'mcx>> = vec_with_capacity_in(mcx, total)?;
    let mut nulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, total)?;
    for _ in 0..total {
        values.push(TupleValue::ByVal(types_datum::Datum::null()));
        nulls.push(false);
    }

    let mut anynulls = false;

    // Set up the values/nulls arrays for heap_fill_tuple.
    let mut idxattno: usize = 0;
    for keyno in 0..brdesc.natts() {
        let col = &tuple.bt_columns[keyno];
        let nstored = brdesc.bd_info[keyno].nstored();

        // "allnulls": no data to store; set the null bits and continue.
        if col.bv_allnulls {
            for _ in 0..nstored {
                nulls[idxattno] = true;
                idxattno += 1;
            }
            anynulls = true;
            continue;
        }

        // "hasnulls": still store a real value, but need a null bitmap.
        if col.bv_hasnulls {
            anynulls = true;
        }

        // If needed, serialize the values before forming the on-disk tuple.
        // The opclass serialize callback fills a fresh `col_values` slice from
        // bv_mem_value; otherwise the column's bv_values are used directly.
        let mut serialized: Option<PgVec<'mcx, TupleValue<'mcx>>> = None;
        if col.bv_has_serialize {
            let mut dst: PgVec<'mcx, TupleValue<'mcx>> = vec_with_capacity_in(mcx, nstored)?;
            for _ in 0..nstored {
                dst.push(TupleValue::ByVal(types_datum::Datum::null()));
            }
            let mem = col
                .bv_mem_value
                .as_ref()
                .expect("bv_serialize set but bv_mem_value is NULL");
            backend_access_brin_entry_seams::brin_serialize::call(mcx, keyno, mem, &mut dst)?;
            serialized = Some(dst);
        }

        // Now obtain the values of each stored datum. Some values might be
        // toasted; detoast them and try to compress them (TOAST_INDEX_HACK).
        for datumno in 0..nstored {
            // We must look at the stored type, not the index descriptor.
            let atttype = &brdesc.bd_info[keyno].oi_typcache[datumno];

            let src_value: &TupleValue<'_> = match &serialized {
                Some(s) => &s[datumno],
                None => &col.bv_values[datumno],
            };

            // For non-varlena types we don't need to do anything special.
            if atttype.typlen != -1 {
                values[idxattno] = clone_value(mcx, src_value)?;
                idxattno += 1;
                continue;
            }

            // varlena: bytes of the value (already a by-reference datum).
            let mut value: PgVec<'mcx, u8> = slice_in(mcx, src_value.as_ref_bytes())?;

            // If value is stored EXTERNAL, must fetch it so we are not depending
            // on outside storage.
            if varatt_is_external(&value) {
                value = backend_access_common_detoast_seams::detoast_external_attr::call(
                    mcx, &value,
                )?;
            }

            // If value is above size target and is of a compressible datatype,
            // try to compress it in-line.
            if !varatt_is_extended(&value)
                && varsize(&value) > TOAST_INDEX_TARGET
                && (atttype.typstorage == TYPSTORAGE_EXTENDED
                    || atttype.typstorage == TYPSTORAGE_MAIN)
            {
                // If the BRIN summary and indexed attribute use the same data
                // type with a valid compression method, reuse it; otherwise use
                // the default method.
                let att = brdesc.bd_tupdesc.attr(keyno);
                let compression = if att.atttypid == atttype.type_id {
                    att.attcompression
                } else {
                    INVALID_COMPRESSION_METHOD
                };

                if let Some(cvalue) =
                    backend_access_common_toast_internals_seams::toast_compress_datum::call(
                        mcx,
                        &value,
                        compression,
                    )?
                {
                    // successful compression: the previous `value` bytes are
                    // dropped here (C: pfree of the freed-value).
                    value = cvalue;
                }
            }

            values[idxattno] = TupleValue::ByRef(value);
            idxattno += 1;
        }
    }

    // Assert we did not overrun temp arrays.
    debug_assert!(idxattno <= total);

    // compute total space needed.
    let mut len: Size = SIZE_OF_BRIN_TUPLE;
    if anynulls {
        // Double-length bitmap: first half "allnulls", second half "hasnulls".
        len += bitmaplen(brdesc.natts() * 2);
    }

    len = maxalign(len);
    let hoff = len;

    let disktdesc = brtuple_disk_tupdesc(mcx, brdesc)?;
    let data_len = heap_compute_data_size(&disktdesc, &values, &nulls)?;
    len += data_len;

    len = maxalign(len);

    // rettuple = palloc0(len); rettuple->bt_blkno = blkno; rettuple->bt_info = hoff;
    let mut rettuple = BrinTupleImage::zeroed(mcx, len)?;
    rettuple.set_bt_blkno(blkno);
    rettuple.set_bt_info(hoff as u8);

    // Assert that hoff fits in the offset field.
    debug_assert_eq!((rettuple.bt_info() & BRIN_OFFSET_MASK) as Size, hoff);

    // The infomask and null bitmap heap_fill_tuple produces are useless to us;
    // we copy only its data area into the tuple. C passes a phony infomask + a
    // valid (discarded) null bitmap so heap_fill_tuple skips null attributes.
    let filled = heap_fill_tuple(mcx, &disktdesc, &values, &nulls, data_len, true)?;
    rettuple.bytes[hoff..hoff + data_len].copy_from_slice(&filled.data);

    // (values/nulls/phony bitmap and the untoasted/compressed values are freed
    // by scope drop — the C `pfree` loop over untoasted_values.)

    // Now fill in the real null bitmasks. allnulls first.
    if anynulls {
        rettuple.or_bt_info(BRIN_NULLS_MASK);

        // We reverse the sense of null bits: store a 1 for a null attribute.
        // bitP starts as ((bits8 *)(rettuple + SizeOfBrinTuple)) - 1.
        let bits_base = SIZE_OF_BRIN_TUPLE;
        let mut bit_index: isize = -1;
        let mut bitmask: i32 = HIGHBIT;

        // allnulls bits.
        for keyno in 0..brdesc.natts() {
            if bitmask != HIGHBIT {
                bitmask <<= 1;
            } else {
                bit_index += 1;
                rettuple.bytes[bits_base + bit_index as usize] = 0x0;
                bitmask = 1;
            }

            if !tuple.bt_columns[keyno].bv_allnulls {
                continue;
            }

            rettuple.bytes[bits_base + bit_index as usize] |= bitmask as u8;
        }
        // hasnulls bits follow.
        for keyno in 0..brdesc.natts() {
            if bitmask != HIGHBIT {
                bitmask <<= 1;
            } else {
                bit_index += 1;
                rettuple.bytes[bits_base + bit_index as usize] = 0x0;
                bitmask = 1;
            }

            if !tuple.bt_columns[keyno].bv_hasnulls {
                continue;
            }

            rettuple.bytes[bits_base + bit_index as usize] |= bitmask as u8;
        }
    }

    if tuple.bt_placeholder {
        rettuple.or_bt_info(BRIN_PLACEHOLDER_MASK);
    }

    if tuple.bt_empty_range {
        rettuple.or_bt_info(BRIN_EMPTY_RANGE_MASK);
    }

    Ok((rettuple, len))
}

/// Deep-clone a [`TupleValue`] into `mcx` (the by-value path of `brin_form_tuple`
/// just copies the scalar; this matches that and keeps lifetimes uniform).
fn clone_value<'mcx>(mcx: Mcx<'mcx>, v: &TupleValue<'_>) -> PgResult<TupleValue<'mcx>> {
    v.clone_in(mcx)
}

// ---------------------------------------------------------------------------
// brin_form_placeholder_tuple (brin_tuple.c:387)
// ---------------------------------------------------------------------------

/// `brin_form_placeholder_tuple(brdesc, blkno, &size)` (brin_tuple.c:387):
/// generate a new on-disk tuple with no data values, marked as a placeholder. A
/// cut-down [`brin_form_tuple`].
pub fn brin_form_placeholder_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
    blkno: BlockNumber,
) -> PgResult<(BrinTupleImage<'mcx>, Size)> {
    // compute total space needed: always add nulls.
    let mut len: Size = SIZE_OF_BRIN_TUPLE;
    len += bitmaplen(brdesc.natts() * 2);
    len = maxalign(len);
    let hoff = len;

    let mut rettuple = BrinTupleImage::zeroed(mcx, len)?;
    rettuple.set_bt_blkno(blkno);
    rettuple.set_bt_info(hoff as u8);
    rettuple.or_bt_info(BRIN_NULLS_MASK | BRIN_PLACEHOLDER_MASK | BRIN_EMPTY_RANGE_MASK);

    let bits_base = SIZE_OF_BRIN_TUPLE;
    let mut bit_index: isize = -1;
    let mut bitmask: i32 = HIGHBIT;
    // set allnulls true for all attributes.
    for _keyno in 0..brdesc.natts() {
        if bitmask != HIGHBIT {
            bitmask <<= 1;
        } else {
            bit_index += 1;
            rettuple.bytes[bits_base + bit_index as usize] = 0x0;
            bitmask = 1;
        }

        rettuple.bytes[bits_base + bit_index as usize] |= bitmask as u8;
    }
    // no need to set hasnulls.

    Ok((rettuple, len))
}

// ---------------------------------------------------------------------------
// brin_free_tuple / brin_copy_tuple / brin_tuples_equal (brin_tuple.c:432-472)
// ---------------------------------------------------------------------------

/// `brin_free_tuple(tuple)` (brin_tuple.c:432): free a tuple created by
/// [`brin_form_tuple`]. In Rust the image owns its bytes; dropping it frees them.
pub fn brin_free_tuple(tuple: BrinTupleImage<'_>) {
    drop(tuple);
}

/// `brin_copy_tuple(tuple, len, dest, destsz)` (brin_tuple.c:445): create a copy
/// of `tuple` (length `len`). When `dest` is `Some` and its capacity (`destsz`,
/// the C `*destsz`) is non-zero, the destination buffer is reused (and grown via
/// `repalloc` when `len` exceeds it), avoiding palloc/free cycles in loops;
/// otherwise a fresh image is allocated in `mcx`. Returns the image and its
/// updated `destsz` (the C output `*destsz`).
pub fn brin_copy_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &[u8],
    len: usize,
    dest: Option<BrinTupleImage<'mcx>>,
    destsz: usize,
) -> PgResult<(BrinTupleImage<'mcx>, usize)> {
    // if !destsz || *destsz == 0: dest = palloc(len); (no destsz update)
    // else if len > *destsz: dest = repalloc(dest, len); *destsz = len;
    let (mut img, new_destsz) = match dest {
        Some(d) if destsz != 0 => {
            if len > destsz {
                (d, len)
            } else {
                (d, destsz)
            }
        }
        _ => (BrinTupleImage::zeroed(mcx, 0)?, destsz),
    };

    // memcpy(dest, tuple, len): make the image exactly `len` bytes.
    img.bytes.clear();
    img.bytes
        .try_reserve(len)
        .map_err(|_| mcx.oom(len))?;
    img.bytes.extend_from_slice(&tuple[..len]);

    Ok((img, new_destsz))
}

/// `brin_tuples_equal(a, alen, b, blen)` (brin_tuple.c:464): whether two
/// BrinTuples are bitwise identical.
pub fn brin_tuples_equal(a: &[u8], alen: usize, b: &[u8], blen: usize) -> bool {
    if alen != blen {
        return false;
    }
    a[..alen] == b[..blen]
}

// ---------------------------------------------------------------------------
// brin_new_memtuple / brin_memtuple_initialize (brin_tuple.c:481-538)
// ---------------------------------------------------------------------------

/// `brin_new_memtuple(brdesc)` (brin_tuple.c:481): create a new [`BrinMemTuple`]
/// from scratch, initialized to an empty state. Allocated in `mcx` (C's per-tuple
/// `bt_context` becomes the owned `bt_columns` vector).
pub fn brin_new_memtuple<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
) -> PgResult<BrinMemTuple<'mcx>> {
    let mut dtup = BrinMemTuple {
        bt_placeholder: false,
        bt_empty_range: true,
        bt_blkno: 0,
        bt_columns: vec_with_capacity_in(mcx, brdesc.natts())?,
    };

    brin_memtuple_initialize(mcx, &mut dtup, brdesc)?;

    Ok(dtup)
}

/// `brin_memtuple_initialize(dtuple, brdesc)` (brin_tuple.c:510): reset a
/// [`BrinMemTuple`] to its initial state. `MemoryContextReset(bt_context)` —
/// all per-column expanded values and datum copies are dropped — maps to
/// rebuilding the column vector fresh in `mcx`.
pub fn brin_memtuple_initialize<'mcx>(
    mcx: Mcx<'mcx>,
    dtuple: &mut BrinMemTuple<'mcx>,
    brdesc: &BrinDesc<'_>,
) -> PgResult<()> {
    dtuple.bt_columns.clear();
    dtuple
        .bt_columns
        .try_reserve(brdesc.natts())
        .map_err(|_| mcx.oom(brdesc.natts()))?;
    for i in 0..brdesc.natts() {
        let nstored = brdesc.bd_info[i].nstored();
        // bv_values points into the trailing Datum area, oi_nstored long.
        let mut bv_values: PgVec<'mcx, TupleValue<'mcx>> = vec_with_capacity_in(mcx, nstored)?;
        for _ in 0..nstored {
            bv_values.push(TupleValue::ByVal(types_datum::Datum::null()));
        }
        dtuple.bt_columns.push(BrinValues {
            bv_attno: (i + 1) as i16,
            bv_allnulls: true,
            bv_hasnulls: false,
            bv_values,
            bv_mem_value: None, // PointerGetDatum(NULL)
            bv_has_serialize: false,
        });
    }

    dtuple.bt_empty_range = true;
    Ok(())
}

// ---------------------------------------------------------------------------
// brin_deform_tuple (brin_tuple.c:552)
// ---------------------------------------------------------------------------

/// `brin_deform_tuple(brdesc, tuple, dMemtuple)` (brin_tuple.c:552): convert an
/// on-disk BRIN tuple back to a [`BrinMemTuple`]. The reverse of
/// [`brin_form_tuple`].
///
/// As an optimization, the caller can pass a previously-allocated `d_memtuple`,
/// which is reset and reused; otherwise a fresh one is allocated.
pub fn brin_deform_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
    tuple: &[u8],
    d_memtuple: Option<BrinMemTuple<'mcx>>,
) -> PgResult<BrinMemTuple<'mcx>> {
    let mut dtup = match d_memtuple {
        Some(mut m) => {
            brin_memtuple_initialize(mcx, &mut m, brdesc)?;
            m
        }
        None => brin_new_memtuple(mcx, brdesc)?,
    };

    if brin_tuple_is_placeholder(tuple) {
        dtup.bt_placeholder = true;
    }

    // ranges start as empty, depends on the BrinTuple.
    if !brin_tuple_is_empty_range(tuple) {
        dtup.bt_empty_range = false;
    }

    dtup.bt_blkno = brin_tuple_get_blkno(tuple);

    // values/allnulls/hasnulls scratch arrays (bd_totalstored / natts).
    let total = brdesc.bd_totalstored as usize;
    let mut values: PgVec<'mcx, TupleValue<'mcx>> = vec_with_capacity_in(mcx, total)?;
    for _ in 0..total {
        values.push(TupleValue::ByVal(types_datum::Datum::null()));
    }
    let mut allnulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, brdesc.natts())?;
    let mut hasnulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, brdesc.natts())?;
    for _ in 0..brdesc.natts() {
        allnulls.push(false);
        hasnulls.push(false);
    }

    // tp = (char *) tuple + BrinTupleDataOffset(tuple);
    let data_off = brin_tuple_data_offset(tuple);
    let tp = &tuple[data_off..];

    // nullbits = HasNulls ? (char *) tuple + SizeOfBrinTuple : NULL;
    let nullbits: Option<&[u8]> = if brin_tuple_has_nulls(tuple) {
        Some(&tuple[SIZE_OF_BRIN_TUPLE..])
    } else {
        None
    };

    brin_deconstruct_tuple(
        mcx,
        brdesc,
        tp,
        nullbits,
        brin_tuple_has_nulls(tuple),
        &mut values,
        &mut allnulls,
        &mut hasnulls,
    )?;

    // Assign each value to its column's bv_values, datumCopy'ing in the tuple's
    // context.
    let mut valueno = 0usize;
    for keyno in 0..brdesc.natts() {
        let nstored = brdesc.bd_info[keyno].nstored();

        if allnulls[keyno] {
            valueno += nstored;
            continue;
        }

        for i in 0..nstored {
            let tce = &brdesc.bd_info[keyno].oi_typcache[i];
            let copied = backend_utils_adt_scalar_seams::datum_copy::call(
                mcx,
                &values[valueno],
                tce.typbyval,
                tce.typlen,
            )?;
            dtup.bt_columns[keyno].bv_values[i] = copied;
            valueno += 1;
        }

        dtup.bt_columns[keyno].bv_hasnulls = hasnulls[keyno];
        dtup.bt_columns[keyno].bv_allnulls = false;

        dtup.bt_columns[keyno].bv_mem_value = None; // PointerGetDatum(NULL)
        dtup.bt_columns[keyno].bv_has_serialize = false;
    }

    Ok(dtup)
}

// ---------------------------------------------------------------------------
// brin_deconstruct_tuple (brin_tuple.c:644)
// ---------------------------------------------------------------------------

/// `brin_deconstruct_tuple(brdesc, tp, nullbits, nulls, values, allnulls,
/// hasnulls)` (brin_tuple.c:644): guts of attribute extraction from an on-disk
/// BRIN tuple. Output arrays must be sized by the caller.
#[allow(clippy::too_many_arguments)]
fn brin_deconstruct_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
    tp: &[u8],
    nullbits: Option<&[u8]>,
    nulls: bool,
    values: &mut [TupleValue<'mcx>],
    allnulls: &mut [bool],
    hasnulls: &mut [bool],
) -> PgResult<()> {
    // First iterate to natts to obtain both null flags for each attribute. We
    // reverse the sense of att_isnull, storing 1 for a null value.
    let natts = brdesc.natts();
    for attnum in 0..natts {
        // "all nulls": all values in the page range are null -> no data.
        allnulls[attnum] = nulls && !att_isnull(attnum, nullbits.unwrap_or(&[]));
        // "has nulls": some tuples have nulls -> the tuple contains data. The
        // hasnulls bits follow the allnulls bits in the same bitmask.
        hasnulls[attnum] = nulls && !att_isnull(natts + attnum, nullbits.unwrap_or(&[]));
    }

    // Iterate to obtain each attribute's stored values. We cannot cache offsets
    // since attribute entries may be reused for more than one column.
    let diskdsc = brtuple_disk_tupdesc(mcx, brdesc)?;
    let mut stored = 0usize;
    let mut off = 0usize;
    for attnum in 0..natts {
        let nstored = brdesc.bd_info[attnum].nstored();

        if allnulls[attnum] {
            stored += nstored;
            continue;
        }

        for _datumno in 0..nstored {
            let thisatt = diskdsc.compact_attr(stored);

            if thisatt.attlen == -1 {
                // att_pointer_alignby(off, attalignby, -1, tp + off): a varlena;
                // no alignment if the byte at tp[off] is a short header.
                off = att_pointer_alignby(off, thisatt.attalignby, &tp[off..]);
            } else {
                // not varlena, so safe to use att_nominal_alignby.
                off = att_nominal_alignby(off, thisatt.attalignby);
            }

            // fetchatt(thisatt, tp + off): a by-value scalar or a by-reference
            // datum (pointer into the data area). In the byte model a by-ref
            // value carries the verbatim field bytes.
            values[stored] = fetchatt(mcx, thisatt.attbyval, thisatt.attlen, &tp[off..])?;
            stored += 1;

            // att_addlength_pointer(off, attlen, tp + off).
            off = att_addlength_pointer(off, thisatt.attlen, &tp[off..]);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// tupmacs.h alignment / fetch helpers, ported 1:1 over the byte model.
// ---------------------------------------------------------------------------

/// `TYPEALIGN(ALIGNVAL, LEN)` (`c.h`).
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `att_nominal_alignby(cur_offset, attalignby)` (`tupmacs.h`).
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    type_align(attalignby as usize, cur_offset)
}

/// `att_pointer_alignby(cur_offset, attalignby, attlen, attptr)` (`tupmacs.h`)
/// specialized to `attlen == -1` (the only call here): no alignment when the
/// byte at the pointer is a short 1-byte varlena header, else nominal align.
#[inline]
fn att_pointer_alignby(cur_offset: usize, attalignby: u8, attptr: &[u8]) -> usize {
    // VARATT_NOT_PAD_BYTE(attptr) == (*attptr != 0)
    if attptr[0] != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)` (`tupmacs.h`).
#[inline]
fn att_addlength_pointer(cur_offset: usize, attlen: i16, attptr: &[u8]) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        // VARSIZE_ANY(attptr)
        cur_offset + varsize_any(attptr)
    } else {
        debug_assert_eq!(attlen, -2);
        // strlen(attptr) + 1
        let mut len = 0usize;
        while attptr[len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

/// `fetchatt(att, T)` (`tupmacs.h`): read one attribute from the data area.
/// By-value scalars become a [`TupleValue::ByVal`] machine word; by-reference
/// fields become a [`TupleValue::ByRef`] copy of the field's verbatim bytes
/// (the faithful idiomatic stand-in for C's bare pointer into the tuple).
fn fetchatt<'mcx>(
    mcx: Mcx<'mcx>,
    attbyval: bool,
    attlen: i16,
    src: &[u8],
) -> PgResult<TupleValue<'mcx>> {
    if attbyval {
        // fetch_att: read attlen bytes into a Datum word.
        let word: u64 = match attlen {
            1 => src[0] as u64,
            2 => u16::from_ne_bytes([src[0], src[1]]) as u64,
            4 => u32::from_ne_bytes([src[0], src[1], src[2], src[3]]) as u64,
            8 => u64::from_ne_bytes([
                src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
            ]),
            _ => panic!("unsupported byval length: {attlen}"),
        };
        Ok(TupleValue::ByVal(types_datum::Datum::from_usize(word as usize)))
    } else {
        // by-reference: copy the field's bytes (its on-disk span).
        let span = if attlen > 0 {
            attlen as usize
        } else if attlen == -1 {
            varsize_any(src)
        } else {
            // cstring: strlen + 1
            let mut len = 0usize;
            while src[len] != 0 {
                len += 1;
            }
            len + 1
        };
        Ok(TupleValue::ByRef(slice_in(mcx, &src[..span])?))
    }
}
