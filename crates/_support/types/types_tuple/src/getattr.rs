use ::datum::Datum;

use crate::htup::{
    HeapTupleData, MaxCommandIdAttributeNumber, MaxTransactionIdAttributeNumber,
    MinCommandIdAttributeNumber, MinTransactionIdAttributeNumber,
    SelfItemPointerAttributeNumber, TableOidAttributeNumber,
};
use crate::tupdesc::TupleDescData;
use crate::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
    fetchatt,
};

pub fn getmissingattr(tupleDesc: &TupleDescData<'_>, attnum: i32, isnull: &mut bool) -> Datum {
    debug_assert!(attnum <= tupleDesc.natts && attnum > 0);
    let att = &tupleDesc.compact_attrs[(attnum - 1) as usize];
    if att.atthasmissing {
        let constr = tupleDesc.constr.as_ref().expect("atthasmissing without constr");
        let attrmiss = &constr.missing[(attnum - 1) as usize];
        if attrmiss.am_present {
            // C's TopMemoryContext missing_cache (lifetime extension) dissolves:
            // am_value's referent is descriptor-owned and borrow-bounded.
            *isnull = false;
            return attrmiss.am_value;
        }
    }
    *isnull = true;
    Datum::null()
}

pub fn heap_attisnull(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: Option<&TupleDescData<'_>>,
) -> bool {
    debug_assert!(tupleDesc.is_none_or(|d| attnum <= d.natts));
    if attnum > tup.t_data().natts() as i32 {
        return match tupleDesc {
            Some(d) => !d.compact_attrs[(attnum - 1) as usize].atthasmissing,
            None => true,
        };
    }

    if attnum > 0 {
        if tup.no_nulls() {
            return false;
        }
        // SAFETY: HASNULL bitmap covers natts bits; attnum <= natts checked above.
        return unsafe { att_isnull((attnum - 1) as usize, tup.bits_ptr()) };
    }

    match attnum {
        TableOidAttributeNumber
        | SelfItemPointerAttributeNumber
        | MinTransactionIdAttributeNumber
        | MinCommandIdAttributeNumber
        | MaxTransactionIdAttributeNumber
        | MaxCommandIdAttributeNumber => false,
        _ => panic!("invalid attnum: {attnum}"),
    }
}

/// # Safety
/// As [`fastgetattr`] (C's nocachegetattr contract: heaptuple.c trusts attnum).
pub unsafe fn nocachegetattr(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
) -> Datum {
    let bp = tup.bits_ptr();
    let hasnulls = tup.has_nulls();
    let mut slow = false;
    let attnum = (attnum - 1) as usize;

    if hasnulls {
        // SAFETY: HASNULL bitmap covers natts bits; attnum < natts (caller contract).
        unsafe {
            let byte = attnum >> 3;
            let finalbit = attnum & 0x07;
            if (!*bp.add(byte)) & ((1 << finalbit) - 1) != 0 {
                slow = true;
            } else {
                for i in 0..byte {
                    if *bp.add(i) != 0xFF {
                        slow = true;
                        break;
                    }
                }
            }
        }
    }

    let tp = tup.getstruct();
    // Full slice: compact_attrs.len() == natts (TupleDesc invariant).
    let atts: &[crate::tupdesc::CompactAttribute] = &tupleDesc.compact_attrs;
    debug_assert!(atts.len() == tupleDesc.natts as usize && attnum < atts.len());
    let mut off: usize;

    if !slow {
        // SAFETY: attnum < natts == atts.len() (caller contract).
        let att = unsafe { atts.get_unchecked(attnum) };
        if att.attcacheoff.get() >= 0 {
            // SAFETY: cached offset points at the live attribute within the image.
            return unsafe { fetchatt(att, tp.add(att.attcacheoff.get() as usize)) };
        }

        if tup.has_var_width() {
            for j in 0..=attnum {
                if atts[j].attlen <= 0 {
                    slow = true;
                    break;
                }
            }
        }
    }

    if !slow {
        let natts = atts.len();
        let mut j = 1;

        atts[0].attcacheoff.set(0);
        while j < natts && atts[j].attcacheoff.get() > 0 {
            j += 1;
        }

        off = atts[j - 1].attcacheoff.get() as usize + atts[j - 1].attlen as usize;

        while j < natts {
            let att = &atts[j];
            if att.attlen <= 0 {
                break;
            }
            off = att_nominal_alignby(off, att.attalignby);
            att.attcacheoff.set(off as i32);
            off += att.attlen as usize;
            j += 1;
        }

        debug_assert!(j > attnum);
        // SAFETY: attnum < atts.len() (caller contract).
        off = unsafe { atts.get_unchecked(attnum) }.attcacheoff.get() as usize;
    } else {
        let mut usecache = true;
        off = 0;
        // Slicing to ..=attnum makes the i <= attnum loop bound the slice bound,
        // so the per-iteration indexing check folds away.
        // SAFETY: attnum < atts.len() (caller contract).
        let watts = unsafe { atts.get_unchecked(..=attnum) };
        let mut i = 0;
        loop {
            let att = &watts[i];
            let attlen = att.attlen;
            // SAFETY: in-bounds for attributes present in the tuple; walk stops at attnum.
            unsafe {
                if hasnulls && att_isnull(i, bp) {
                    usecache = false;
                    i += 1;
                    continue;
                }

                if usecache && att.attcacheoff.get() >= 0 {
                    off = att.attcacheoff.get() as usize;
                } else if attlen == -1 {
                    // Cacheable only when already aligned (valid packed or not).
                    if usecache && off == att_nominal_alignby(off, att.attalignby) {
                        att.attcacheoff.set(off as i32);
                    } else {
                        off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
                        usecache = false;
                    }
                } else {
                    off = att_nominal_alignby(off, att.attalignby);
                    if usecache {
                        att.attcacheoff.set(off as i32);
                    }
                }

                if i == attnum {
                    break;
                }

                off = att_addlength_pointer(off, attlen as i32, tp.add(off));
            }
            if usecache && attlen <= 0 {
                usecache = false;
            }
            i += 1;
        }
    }

    // SAFETY: attnum < atts.len(); off is the attribute's computed in-image offset.
    unsafe { fetchatt(atts.get_unchecked(attnum), tp.add(off)) }
}

pub fn heap_getsysattr(tup: &HeapTupleData<'_>, attnum: i32, isnull: &mut bool) -> Datum {
    *isnull = false;
    match attnum {
        SelfItemPointerAttributeNumber => Datum::from_usize(&tup.t_self as *const _ as usize),
        MinTransactionIdAttributeNumber => Datum::from_u32(tup.t_data().xmin_raw()),
        MaxTransactionIdAttributeNumber => Datum::from_u32(tup.t_data().xmax_raw()),
        MinCommandIdAttributeNumber | MaxCommandIdAttributeNumber => {
            Datum::from_u32(tup.t_data().raw_command_id())
        }
        TableOidAttributeNumber => Datum::from_oid(tup.t_tableOid),
        _ => panic!("invalid attnum: {attnum}"),
    }
}

/// # Safety
/// `1 <= attnum <= tupleDesc.natts`, descriptor matches the tuple image,
/// attribute present in the tuple (C's fastgetattr contract; unchecked).
#[inline]
pub unsafe fn fastgetattr(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    debug_assert!(attnum > 0 && attnum <= tupleDesc.natts);
    *isnull = false;
    if tup.no_nulls() {
        // SAFETY: attnum <= natts == compact_attrs.len() (caller contract).
        let att = unsafe { tupleDesc.compact_attrs.get_unchecked((attnum - 1) as usize) };
        if att.attcacheoff.get() >= 0 {
            // SAFETY: cached offset points at the live attribute within the image.
            unsafe { fetchatt(att, tup.getstruct().add(att.attcacheoff.get() as usize)) }
        } else {
            // SAFETY: caller contract.
            unsafe { nocachegetattr(tup, attnum, tupleDesc) }
        }
    } else {
        // SAFETY: HASNULL bitmap covers attnum-1 (attnum <= natts, caller contract).
        if unsafe { att_isnull((attnum - 1) as usize, tup.bits_ptr()) } {
            *isnull = true;
            Datum::null()
        } else {
            // SAFETY: caller contract.
            unsafe { nocachegetattr(tup, attnum, tupleDesc) }
        }
    }
}

/// C GETSTRUCT-shape read: `attnum` is a fixed-width NOT NULL leading column
/// (no varlena and no null can precede it), so the null-bitmap checks C skips
/// via struct overlay are skipped here too.
///
/// # Safety
/// As [`fastgetattr`], plus the GETSTRUCT invariant above.
#[inline]
pub unsafe fn fastgetattr_fixed(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
) -> Datum {
    debug_assert!(attnum > 0 && attnum <= tupleDesc.natts);
    debug_assert!(tup.no_nulls() || !unsafe { att_isnull((attnum - 1) as usize, tup.bits_ptr()) });
    // SAFETY: attnum <= natts == compact_attrs.len() (caller contract).
    let att = unsafe { tupleDesc.compact_attrs.get_unchecked((attnum - 1) as usize) };
    let off = att.attcacheoff.get();
    if off >= 0 {
        // SAFETY: cached offset points at the live attribute within the image.
        unsafe { fetchatt(att, tup.getstruct().add(off as usize)) }
    } else {
        // SAFETY: caller contract; populates attcacheoff for the fixed prefix.
        unsafe { nocachegetattr(tup, attnum, tupleDesc) }
    }
}

/// # Safety
/// For attnum > 0, as [`fastgetattr`] minus tuple-presence (checked here).
#[inline]
pub unsafe fn heap_getattr(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    if attnum > 0 {
        if attnum > tup.t_data().natts() as i32 {
            getmissingattr(tupleDesc, attnum, isnull)
        } else {
            // SAFETY: attnum <= tuple natts (checked); rest is caller contract.
            unsafe { fastgetattr(tup, attnum, tupleDesc, isnull) }
        }
    } else {
        heap_getsysattr(tup, attnum, isnull)
    }
}

pub fn heap_deform_tuple(
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    values: &mut [Datum],
    isnull: &mut [bool],
) {
    let tup = tuple.t_data();
    let hasnulls = tuple.has_nulls();
    // Vec length == natts (TupleDesc invariant); slicing by it is check-free.
    let atts: &[crate::tupdesc::CompactAttribute] = &tupleDesc.compact_attrs;
    let tdesc_natts = atts.len();
    debug_assert!(tdesc_natts == tupleDesc.natts as usize);
    // Inheritance can hand a tuple wider than the descriptor; clamp to both.
    // The narrow-tuple case (missing-attr tail) leaves at entry so neither
    // length stays live across the walk.
    let natts = tup.natts() as usize;
    if natts < tdesc_natts {
        return deform_narrow(tuple, tupleDesc, values, isnull, natts);
    }
    let natts = tdesc_natts;

    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();

    let atts_n = &atts[..natts];
    let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
    // SAFETY: descriptor matches the image; natts <= tuple natts.
    if let Some((attnum, off)) = unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls) } {
        // SAFETY: same walk contract; resumes at the cstring attribute's
        // length step with its datum already stored.
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off);
        }
    }
}

/// The walk must stay call-free: a non-diverging call inside (or before) the
/// loop pushes the loop state into callee-saved registers — a 6-pair
/// prologue/epilogue paid on every deform. The cstring arm (the strlen call C
/// also pays) exits to a cold continuation via the returned resume point.
///
/// # Safety
/// Slices are same-length; bitmap/image reads walk attributes present in the
/// tuple (caller clamps to the tuple's natts).
#[inline(always)]
unsafe fn deform_walk(
    atts_n: &[crate::tupdesc::CompactAttribute],
    values_n: &mut [Datum],
    isnull_n: &mut [bool],
    tp: *const u8,
    bp: *const crate::htup::bits8,
    hasnulls: bool,
) -> Option<(usize, usize)> {
    let mut off = 0usize;
    let mut slow = false;
    for attnum in 0..atts_n.len() {
        let thisatt = &atts_n[attnum];
        // Locals: the Cell makes the struct non-readonly to LLVM.
        let attlen = thisatt.attlen as i32;
        let attbyval = thisatt.attbyval;
        let attalignby = thisatt.attalignby;
        // SAFETY: caller contract.
        unsafe {
            if hasnulls && att_isnull(attnum, bp) {
                values_n[attnum] = Datum::null();
                isnull_n[attnum] = true;
                slow = true;
                continue;
            }

            isnull_n[attnum] = false;

            if !slow && thisatt.attcacheoff.get() >= 0 {
                off = thisatt.attcacheoff.get() as usize;
            } else if attlen == -1 {
                if !slow && off == att_nominal_alignby(off, attalignby) {
                    thisatt.attcacheoff.set(off as i32);
                } else {
                    off = att_pointer_alignby(off, attalignby, -1, tp.add(off));
                    slow = true;
                }
            } else {
                off = att_nominal_alignby(off, attalignby);
                if !slow {
                    thisatt.attcacheoff.set(off as i32);
                }
            }

            values_n[attnum] = fetch_att(tp.add(off), attbyval, attlen);

            if attlen > 0 {
                off += attlen as usize;
            } else if attlen == -1 {
                off += crate::varatt::varsize_any(tp.add(off));
                slow = true;
            } else {
                debug_assert!(attlen == -2);
                return Some((attnum, off));
            }
        }
    }
    None
}

// Cold: only post-ADD-COLUMN scans see tuples narrower than the descriptor.
#[cold]
#[inline(never)]
fn deform_narrow(
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    values: &mut [Datum],
    isnull: &mut [bool],
    natts: usize,
) {
    let hasnulls = tuple.has_nulls();
    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();
    let cstring_rest = {
        let atts_n = &tupleDesc.compact_attrs[..natts];
        let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
        // SAFETY: as heap_deform_tuple; natts == tuple natts here.
        unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls) }
    };
    if let Some((attnum, off)) = cstring_rest {
        let atts_n = &tupleDesc.compact_attrs[..natts];
        let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
        // SAFETY: as heap_deform_tuple.
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off);
        }
    }
    deform_missing_tail(tupleDesc, values, isnull, natts);
}

// Cold: cstring attributes exist only in catalog-shaped descriptors. From the
// first one on, slow is true for every later attribute (attlen <= 0), so the
// cacheoff branches drop out of the resumed walk.
#[cold]
#[inline(never)]
unsafe fn deform_cstring_rest(
    atts_n: &[crate::tupdesc::CompactAttribute],
    values_n: &mut [Datum],
    isnull_n: &mut [bool],
    tp: *const u8,
    bp: *const crate::htup::bits8,
    hasnulls: bool,
    attnum: usize,
    mut off: usize,
) {
    // SAFETY: caller contract — the walk covers attributes present in the tuple.
    unsafe {
        off = att_addlength_pointer(off, -2, tp.add(off));
        for i in attnum + 1..atts_n.len() {
            let thisatt = &atts_n[i];
            let attlen = thisatt.attlen as i32;
            if hasnulls && att_isnull(i, bp) {
                values_n[i] = Datum::null();
                isnull_n[i] = true;
                continue;
            }
            isnull_n[i] = false;
            if attlen == -1 {
                off = att_pointer_alignby(off, thisatt.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, thisatt.attalignby);
            }
            values_n[i] = fetch_att(tp.add(off), thisatt.attbyval, attlen);
            off = att_addlength_pointer(off, attlen, tp.add(off));
        }
    }
}

// Cold: only post-ADD-COLUMN scans see tuples narrower than the descriptor.
#[cold]
#[inline(never)]
fn deform_missing_tail(
    tupleDesc: &TupleDescData<'_>,
    values: &mut [Datum],
    isnull: &mut [bool],
    natts: usize,
) {
    for attnum in natts..tupleDesc.natts as usize {
        values[attnum] = getmissingattr(tupleDesc, (attnum + 1) as i32, &mut isnull[attnum]);
    }
}
