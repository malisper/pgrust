//! brin_tuple.c: BrinMemTuple <-> on-disk BrinTuple conversion.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::datum::Datum;
use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::types_brin::*;
use ::types_core::BlockNumber;
use ::types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use ::types_tuple::tupmacs::{
    att_isnull, att_nominal_alignby, att_pointer_alignby, fetchatt,
};
use ::types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b, varsize_any, varsize_external,
    VARHDRSZ, VARHDRSZ_EXTERNAL,
};
use ::types_tuple::{bits8, TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN};

// TOAST_INDEX_TARGET (heaptoast.h): MaxHeapTupleSize / 16.
const TOAST_INDEX_TARGET: usize = ::types_storage::bufpage::MaxHeapTupleSize / 16;

#[inline]
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

#[inline]
const fn bitmaplen(natts: usize) -> usize {
    (natts + 7) / 8
}

pub use ::adt_scalar::datum_ops::datum_copy;

// SAFETY: p is a live non-external varlena.
unsafe fn varlena_image<'a>(p: *const u8) -> &'a [u8] {
    core::slice::from_raw_parts(p, varsize_any(p))
}

/// brin_form_tuple; the returned image's length is the C `*size` (MAXALIGNed).
pub fn brin_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'_>,
    blkno: BlockNumber,
    tuple: &mut BrinMemTuple,
) -> PgResult<PgVec<'mcx, u8>> {
    let natts = bdesc.natts();
    debug_assert!(bdesc.bd_totalstored > 0);

    let mut values: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, bdesc.bd_totalstored)?;
    let mut nulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, bdesc.bd_totalstored)?;
    let mut anynulls = false;

    for keyno in 0..natts {
        let col = &mut tuple.bt_columns[keyno];
        let nstored = bdesc.bd_info[keyno].oi_nstored as usize;

        if col.bv_allnulls {
            for _ in 0..nstored {
                values.push(Datum::null());
                nulls.push(true);
            }
            anynulls = true;
            continue;
        }
        if col.bv_hasnulls {
            anynulls = true;
        }

        if col.bv_mem_value.is_some() {
            debug_assert!(bdesc.bd_info[keyno].kind == types_brin::BrinOpcKind::MinMaxMulti);
            brin_minmax_multi::brin_minmax_multi_serialize(mcx, bdesc, col)?;
        }
        let col = &tuple.bt_columns[keyno];

        for datumno in 0..nstored {
            let mut value = col.bv_values[datumno];
            let idxattno = values.len();

            if bdesc.bd_disktdesc.compact_attr(idxattno).attlen == -1 {
                // SAFETY: non-null varlena datum is live.
                unsafe {
                    let mut p = value.as_usize() as *const u8;
                    if varatt_is_1b_e(p) {
                        let flat = detoast::detoast_external_attr(
                            mcx,
                            external_image(p),
                        )?;
                        value = Datum::from_usize(flat.leak().as_ptr() as usize);
                        p = value.as_usize() as *const u8;
                    }
                    if !varatt_is_1b_e(p)
                        && !varatt_is_1b(p)
                        && !varatt_is_compressed(p)
                        && varsize_any(p) > TOAST_INDEX_TARGET
                    {
                        let storage = bdesc.bd_disktdesc.attr(idxattno).attstorage;
                        if storage == TYPSTORAGE_EXTENDED || storage == TYPSTORAGE_MAIN {
                            let att = bdesc.bd_tupdesc.attr(keyno);
                            let compression =
                                if att.atttypid == bdesc.bd_disktdesc.attr(idxattno).atttypid {
                                    att.attcompression
                                } else {
                                    0
                                };
                            if let Some(cvalue) = heaptoast_seams::toast_compress_datum::call(
                                mcx,
                                varlena_image(p),
                                compression,
                            )? {
                                value = Datum::from_usize(cvalue.leak().as_ptr() as usize);
                            }
                        }
                    }
                }
            }

            values.push(value);
            nulls.push(false);
        }
    }
    debug_assert!(values.len() == bdesc.bd_totalstored);

    let mut len = SizeOfBrinTuple;
    if anynulls {
        len += bitmaplen(natts * 2);
    }
    let hoff = maxalign(len);

    let data_len = heaptuple::heap_compute_data_size(&bdesc.bd_disktdesc, &values, &nulls);
    let total = maxalign(hoff + data_len);

    let mut rettuple: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, total)?;
    rettuple.resize(total, 0);
    brin_tuple_set_blkno(&mut rettuple, blkno);
    debug_assert!(hoff & (BRIN_OFFSET_MASK as usize) == hoff);
    rettuple[4] = hoff as u8;

    let mut phony_infomask: u16 = 0;
    let mut phony_nullbitmap: PgVec<'mcx, bits8> =
        vec_with_capacity_in(mcx, bitmaplen(bdesc.bd_totalstored))?;
    phony_nullbitmap.resize(bitmaplen(bdesc.bd_totalstored), 0);
    // SAFETY: data area = rettuple[hoff..hoff+data_len], zeroed, sized by
    // heap_compute_data_size over the same inputs; bitmap is zeroed.
    unsafe {
        heaptuple::heap_fill_tuple(
            &bdesc.bd_disktdesc,
            &values,
            &nulls,
            rettuple.as_mut_ptr().add(hoff),
            data_len,
            &mut phony_infomask,
            Some(phony_nullbitmap.as_mut_ptr()),
        );
    }

    if anynulls {
        rettuple[4] |= BRIN_NULLS_MASK;
        let mut bit = 0usize;
        for keyno in 0..natts {
            if tuple.bt_columns[keyno].bv_allnulls {
                rettuple[SizeOfBrinTuple + bit / 8] |= 1 << (bit % 8);
            }
            bit += 1;
        }
        for keyno in 0..natts {
            if tuple.bt_columns[keyno].bv_hasnulls {
                rettuple[SizeOfBrinTuple + bit / 8] |= 1 << (bit % 8);
            }
            bit += 1;
        }
    }

    if tuple.bt_placeholder {
        rettuple[4] |= BRIN_PLACEHOLDER_MASK;
    }
    if tuple.bt_empty_range {
        rettuple[4] |= BRIN_EMPTY_RANGE_MASK;
    }

    Ok(rettuple)
}

// SAFETY: p is a live external toast pointer; image length is tag-derived.
unsafe fn external_image<'a>(p: *const u8) -> &'a [u8] {
    core::slice::from_raw_parts(p, varsize_any(p))
}

// VARATT_IS_COMPRESSED on a 4B varlena word.
// SAFETY: p live, at least 1 readable byte.
unsafe fn varatt_is_compressed(p: *const u8) -> bool {
    if cfg!(target_endian = "little") {
        (*p & 0x03) == 0x02
    } else {
        (*p & 0xC0) == 0x40
    }
}

pub fn brin_form_placeholder_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'_>,
    blkno: BlockNumber,
) -> PgResult<PgVec<'mcx, u8>> {
    let natts = bdesc.natts();
    let len = maxalign(SizeOfBrinTuple + bitmaplen(natts * 2));

    let mut rettuple: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    rettuple.resize(len, 0);
    brin_tuple_set_blkno(&mut rettuple, blkno);
    debug_assert!(len & (BRIN_OFFSET_MASK as usize) == len);
    rettuple[4] =
        len as u8 | BRIN_NULLS_MASK | BRIN_PLACEHOLDER_MASK | BRIN_EMPTY_RANGE_MASK;

    for bit in 0..natts {
        rettuple[SizeOfBrinTuple + bit / 8] |= 1 << (bit % 8);
    }

    Ok(rettuple)
}

/// brin_copy_tuple with the retained-buffer optimization folded in: `dest`
/// keeps its capacity across calls.
pub fn brin_copy_tuple(dest: &mut PgVec<'_, u8>, src: &[u8]) -> PgResult<()> {
    dest.clear();
    vec_append_bytes(dest, src)
}

pub fn brin_tuples_equal(a: &[u8], b: &[u8]) -> bool {
    a == b
}

pub fn brin_new_memtuple(bdesc: &BrinDesc<'_>) -> BrinMemTuple {
    let natts = bdesc.natts();
    let mut dtup = BrinMemTuple {
        bt_placeholder: false,
        bt_empty_range: true,
        bt_blkno: 0,
        bt_context: MemoryContext::new_bump("brin dtuple"),
        bt_values: Vec::with_capacity(bdesc.bd_totalstored),
        bt_allnulls: Vec::with_capacity(natts),
        bt_hasnulls: Vec::with_capacity(natts),
        bt_columns: Vec::with_capacity(natts),
    };
    for i in 0..natts {
        dtup.bt_columns.push(BrinValues {
            bv_attno: (i + 1) as u16,
            bv_hasnulls: false,
            bv_allnulls: true,
            bv_values: [Datum::null(); BRIN_MAX_NSTORED],
            bv_mem_value: None,
        });
    }
    dtup
}

/// brin_memtuple_initialize: C resets the value context and per-column state
/// but not bt_placeholder/bt_blkno.
pub fn brin_memtuple_initialize(dtuple: &mut BrinMemTuple, bdesc: &BrinDesc<'_>) {
    dtuple.bt_context.reset();
    let _ = bdesc;
    for (i, col) in dtuple.bt_columns.iter_mut().enumerate() {
        col.bv_attno = (i + 1) as u16;
        col.bv_allnulls = true;
        col.bv_hasnulls = false;
        col.bv_values = [Datum::null(); BRIN_MAX_NSTORED];
        col.bv_mem_value = None;
    }
    dtuple.bt_empty_range = true;
}

/// brin_deform_tuple into a reused memtuple (the C dMemtuple shape; callers
/// needing a fresh one call brin_new_memtuple first).
pub fn brin_deform_tuple(
    bdesc: &BrinDesc<'_>,
    tuple: &[u8],
    dtup: &mut BrinMemTuple,
) -> PgResult<()> {
    brin_memtuple_initialize(dtup, bdesc);

    if BrinTupleIsPlaceholder(tuple) {
        dtup.bt_placeholder = true;
    }
    if !BrinTupleIsEmptyRange(tuple) {
        dtup.bt_empty_range = false;
    }
    dtup.bt_blkno = brin_tuple_blkno(tuple);

    let natts = bdesc.natts();
    let BrinMemTuple {
        bt_context,
        bt_values,
        bt_allnulls,
        bt_hasnulls,
        bt_columns,
        ..
    } = dtup;

    bt_values.clear();
    bt_values.resize(bdesc.bd_totalstored, Datum::null());
    bt_allnulls.clear();
    bt_allnulls.resize(natts, false);
    bt_hasnulls.clear();
    bt_hasnulls.resize(natts, false);

    let nullbits = if BrinTupleHasNulls(tuple) {
        Some(&tuple[SizeOfBrinTuple..])
    } else {
        None
    };
    brin_deconstruct_tuple(
        bdesc,
        &tuple[BrinTupleDataOffset(tuple)..],
        nullbits,
        bt_values,
        bt_allnulls,
        bt_hasnulls,
    )?;

    let mcx = bt_context.mcx();
    let mut valueno = 0usize;
    for keyno in 0..natts {
        let nstored = bdesc.bd_info[keyno].oi_nstored as usize;
        if bt_allnulls[keyno] {
            valueno += nstored;
            continue;
        }
        for i in 0..nstored {
            let att = bdesc.bd_disktdesc.compact_attr(valueno);
            bt_columns[keyno].bv_values[i] =
                datum_copy(mcx, bt_values[valueno], att.attbyval, att.attlen)?;
            valueno += 1;
        }
        bt_columns[keyno].bv_hasnulls = bt_hasnulls[keyno];
        bt_columns[keyno].bv_allnulls = false;
    }

    Ok(())
}

#[cold]
#[inline(never)]
fn corrupt_tuple() -> Box<PgError> {
    Box::new(
        PgError::error(
            "corrupted BRIN summary tuple: attribute extends past tuple data".to_string(),
        )
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

// Validated on-disk byte length of the attribute stored at `tp[off..]`, ensuring
// the whole value (and any varlena/cstring length header it declares) lies
// within the tuple data area. C's brin_deconstruct_tuple trusts these lengths
// (Assert-only) because it reads its own pages; on attacker-craftable on-disk
// bytes we must reject an overrun with ERRCODE_DATA_CORRUPTED rather than walk
// the raw pointer out of bounds.
fn checked_attr_len(tp: &[u8], off: usize, attlen: i32) -> PgResult<usize> {
    let len = if attlen > 0 {
        attlen as usize
    } else if attlen == -1 {
        if off >= tp.len() {
            return Err(corrupt_tuple());
        }
        // SAFETY: off < tp.len(), so the first header byte is readable; any read
        // past it (2B external tag, 4B header) is bounds-checked against tp
        // before the dereference.
        unsafe {
            let p = tp.as_ptr().add(off);
            if varatt_is_1b_e(p) {
                if off + VARHDRSZ_EXTERNAL > tp.len() {
                    return Err(corrupt_tuple());
                }
                varsize_external(p)
            } else if varatt_is_1b(p) {
                varsize_1b(p)
            } else {
                if off + VARHDRSZ > tp.len() {
                    return Err(corrupt_tuple());
                }
                varsize_4b(p)
            }
        }
    } else {
        debug_assert!(attlen == -2);
        // cstring: bounded strlen; the terminating NUL must fall within tp.
        match tp.get(off..).and_then(|s| s.iter().position(|&b| b == 0)) {
            Some(idx) => idx + 1,
            None => return Err(corrupt_tuple()),
        }
    };
    if len == 0 || off + len > tp.len() {
        return Err(corrupt_tuple());
    }
    Ok(len)
}

// brin_deconstruct_tuple: attribute extraction from the on-disk data area.
// `tp` starts at the tuple's data offset; values point INTO tp (no copies).
fn brin_deconstruct_tuple(
    bdesc: &BrinDesc<'_>,
    tp: &[u8],
    nullbits: Option<&[u8]>,
    values: &mut [Datum],
    allnulls: &mut [bool],
    hasnulls: &mut [bool],
) -> PgResult<()> {
    let natts = bdesc.natts();

    // Reversed att_isnull sense: stored 1 means null (see brin_form_tuple).
    for attnum in 0..natts {
        match nullbits {
            Some(bits) => {
                // SAFETY: bitmap covers 2*natts bits; length checked by the
                // tuple's data offset.
                unsafe {
                    allnulls[attnum] = !att_isnull(attnum, bits.as_ptr());
                    hasnulls[attnum] = !att_isnull(natts + attnum, bits.as_ptr());
                }
            }
            None => {
                allnulls[attnum] = false;
                hasnulls[attnum] = false;
            }
        }
    }

    let mut stored = 0usize;
    let mut off = 0usize;
    for attnum in 0..natts {
        let nstored = bdesc.bd_info[attnum].oi_nstored as usize;
        if allnulls[attnum] {
            stored += nstored;
            continue;
        }
        for _ in 0..nstored {
            let thisatt = bdesc.bd_disktdesc.compact_attr(stored);
            let attlen = thisatt.attlen as i32;

            if attlen == -1 {
                // The pad-byte peek in att_pointer_alignby reads tp[off]; bound
                // it before deciding alignment.
                if off >= tp.len() {
                    return Err(corrupt_tuple());
                }
                // SAFETY: off < tp.len().
                off = unsafe {
                    att_pointer_alignby(off, thisatt.attalignby, -1, tp.as_ptr().add(off))
                };
            } else {
                off = att_nominal_alignby(off, thisatt.attalignby);
            }

            // Validate the attribute's declared span before touching it, so both
            // the fetchatt read here and the datum_copy in brin_deform_tuple stay
            // inside tp.
            let this_len = checked_attr_len(tp, off, attlen)?;

            // SAFETY: checked_attr_len guarantees off + this_len <= tp.len(); a
            // by-value fetch reads at most this_len bytes and a by-ref fetch only
            // captures the (in-bounds) pointer.
            values[stored] = unsafe { fetchatt(thisatt, tp.as_ptr().add(off)) };
            off += this_len;
            stored += 1;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::checked_attr_len;

    // A malformed 4B varlena header declaring ~256 MB inside an 8-byte tuple
    // must be rejected, not walked out of bounds (idx-50 OOB read).
    #[test]
    fn oversized_varlena_is_rejected() {
        // 4B varlena word: low 2 bits 00, size encoded as len << 2.
        let word: u32 = 0x1000_0000u32 << 2; // declared VARSIZE = 256 MB
        let mut tp = word.to_ne_bytes().to_vec();
        tp.extend_from_slice(&[0u8; 4]); // 8-byte tuple data area
        assert!(checked_attr_len(&tp, 0, -1).is_err());
    }

    #[test]
    fn fixed_length_overrun_is_rejected() {
        let tp = [0u8; 4];
        // 8-byte fixed attr at offset 0 in a 4-byte area.
        assert!(checked_attr_len(&tp, 0, 8).is_err());
        // ...but a fitting one is accepted.
        assert_eq!(checked_attr_len(&tp, 0, 4).unwrap(), 4);
    }

    #[test]
    fn unterminated_cstring_is_rejected() {
        let tp = [b'a', b'b', b'c'];
        assert!(checked_attr_len(&tp, 0, -2).is_err());
        let tp2 = [b'a', 0u8];
        assert_eq!(checked_attr_len(&tp2, 0, -2).unwrap(), 2);
    }

    #[test]
    fn well_formed_varlena_is_accepted() {
        // VARSIZE = 8 (4B header + 4 payload) inside an 8-byte area.
        let word: u32 = 8u32 << 2;
        let mut tp = word.to_ne_bytes().to_vec();
        tp.extend_from_slice(&[1u8; 4]);
        assert_eq!(checked_attr_len(&tp, 0, -1).unwrap(), 8);
    }
}
