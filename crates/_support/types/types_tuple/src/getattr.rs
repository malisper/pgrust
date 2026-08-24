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

    let data_len = tuple_data_len(tuple);
    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();

    let atts_n = &atts[..natts];
    let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
    // SAFETY: descriptor matches the image; natts <= tuple natts. The walk is
    // hard-bounded by data_len so a mismatched/crafted image cannot over-read.
    if let Some((attnum, off)) =
        unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls, data_len) }
    {
        // SAFETY: same walk contract; resumes at the cstring attribute's
        // length step with its datum already stored.
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off, data_len);
        }
    }
}

/// [`heap_deform_tuple`] narrowed to the leading `want` attributes — the
/// column-pruned deform for consumers that reference an attno prefix
/// (the sqe heap face: the offset walk is left-to-right, so a prefix is
/// the natural pruning grain; per-column static-offset plans are the
/// finer rung). Attributes past `want` are untouched. A tuple narrower
/// than `want` (added-columns tail) falls back to the full walk.
pub fn heap_deform_tuple_prefix(
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    values: &mut [Datum],
    isnull: &mut [bool],
    want: usize,
) {
    let tup = tuple.t_data();
    let hasnulls = tuple.has_nulls();
    let atts: &[crate::tupdesc::CompactAttribute] = &tupleDesc.compact_attrs;
    let want = want.min(atts.len());
    if (tup.natts() as usize) < want {
        return heap_deform_tuple(tuple, tupleDesc, values, isnull);
    }
    let data_len = tuple_data_len(tuple);
    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();
    let atts_n = &atts[..want];
    let (values_n, isnull_n) = (&mut values[..want], &mut isnull[..want]);
    // SAFETY: descriptor matches the image; want <= tuple natts (checked).
    if let Some((attnum, off)) =
        unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls, data_len) }
    {
        // SAFETY: same walk contract (resume at the cstring attribute).
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off, data_len);
        }
    }
}

/// [`heap_deform_tuple_prefix`] over a bare attribute slice (detached
/// page images crossing threads; a TupleDesc's offset cells can't). A
/// tuple narrower than `want` NULL-fills the tail — callers must gate
/// `atthasmissing` descriptors off this path (defaults live on the desc).
pub fn heap_deform_tuple_prefix_atts(
    tuple: &HeapTupleData<'_>,
    atts: &[crate::tupdesc::CompactAttribute],
    values: &mut [Datum],
    isnull: &mut [bool],
    want: usize,
) {
    let tup = tuple.t_data();
    let hasnulls = tuple.has_nulls();
    let want = want.min(atts.len());
    let present = (tup.natts() as usize).min(want);
    for i in present..want {
        values[i] = Datum::null();
        isnull[i] = true;
    }
    if present == 0 {
        return;
    }
    let data_len = tuple_data_len(tuple);
    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();
    let atts_n = &atts[..present];
    let (values_n, isnull_n) = (&mut values[..present], &mut isnull[..present]);
    // SAFETY: as heap_deform_tuple; present <= tuple natts (clamped).
    if let Some((attnum, off)) =
        unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls, data_len) }
    {
        // SAFETY: same walk contract (resume at the cstring attribute).
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off, data_len);
        }
    }
}

/// Data area available for attributes: `t_len - t_hoff`, the extent of the
/// user-data region `getstruct()` points at. Every offset the deform walk
/// dereferences is bounded against this so a mismatched descriptor or a
/// crafted on-disk image (short body with a full natts, or a lying varlena
/// header) cannot over-read past the tuple. C's heap_deform_tuple trusts the
/// layout and consults no such bound; a well-formed tuple never exceeds it, so
/// this only turns an out-of-bounds read into a deterministic corruption
/// error and never changes valid-tuple behavior.
#[inline]
fn tuple_data_len(tuple: &HeapTupleData<'_>) -> usize {
    let t_hoff = tuple.t_data().t_hoff as usize;
    // t_hoff is an attacker-controlled on-disk field; a header claiming a
    // t_hoff past t_len would make getstruct() form an out-of-bounds pointer.
    match (tuple.t_len as usize).checked_sub(t_hoff) {
        Some(len) => len,
        None => deform_corrupt(),
    }
}

// Cold, divergent: a crafted/mismatched image walked off the tuple. Mirrors
// the crate's panic! precedent for unrecoverable image corruption; the backend
// error boundary turns the unwind into an aborted transaction rather than an
// OOB read.
#[cold]
#[inline(never)]
fn deform_corrupt() -> ! {
    panic!("heap tuple data is corrupt: attribute offset exceeds tuple length");
}

/// Length of the varlena at `p`, bounded so no header byte is read past
/// `avail`. Reads only byte 0 to classify, then the extra header bytes each
/// form guarantees (1 more for external, 3 more for a 4-byte header). Returns
/// `None` when the header or the declared body would run past `avail`.
///
/// # Safety
/// `p` points to a live image readable for at least `avail` bytes.
#[inline]
unsafe fn varsize_bounded(p: *const u8, avail: usize) -> Option<usize> {
    use crate::varatt;
    if avail == 0 {
        return None;
    }
    // SAFETY: avail >= 1, so byte 0 is in range for the tag classification.
    let sz = unsafe {
        if varatt::varatt_is_1b_e(p) {
            if avail < crate::varatt::VARHDRSZ_EXTERNAL {
                return None;
            }
            varatt::varsize_external(p)
        } else if varatt::varatt_is_1b(p) {
            varatt::varsize_1b(p)
        } else {
            if avail < crate::varatt::VARHDRSZ {
                return None;
            }
            varatt::varsize_4b(p)
        }
    };
    (sz <= avail).then_some(sz)
}

/// The walk must stay call-free: a non-diverging call inside (or before) the
/// loop pushes the loop state into callee-saved registers — a 6-pair
/// prologue/epilogue paid on every deform. The cstring arm (the strlen call C
/// also pays) exits to a cold continuation via the returned resume point. The
/// bounds guards feed a cold, divergent `deform_corrupt`, so they add only a
/// well-predicted compare per attribute and no register pressure.
///
/// # Safety
/// Slices are same-length; bitmap/image reads walk attributes present in the
/// tuple (caller clamps to the tuple's natts). Every image dereference is
/// bounded by `data_len` (the tuple's user-data extent).
#[inline(always)]
unsafe fn deform_walk(
    atts_n: &[crate::tupdesc::CompactAttribute],
    values_n: &mut [Datum],
    isnull_n: &mut [bool],
    tp: *const u8,
    bp: *const crate::htup::bits8,
    hasnulls: bool,
    data_len: usize,
) -> Option<(usize, usize)> {
    let mut off = 0usize;
    let mut slow = false;
    for attnum in 0..atts_n.len() {
        let thisatt = &atts_n[attnum];
        // Locals: the Cell makes the struct non-readonly to LLVM.
        let attlen = thisatt.attlen as i32;
        let attbyval = thisatt.attbyval;
        let attalignby = thisatt.attalignby;
        // SAFETY: caller contract; every deref below is bounded by data_len.
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
                    // att_pointer_alignby peeks tp[off] (the pad byte).
                    if off >= data_len {
                        deform_corrupt();
                    }
                    off = att_pointer_alignby(off, attalignby, -1, tp.add(off));
                    slow = true;
                }
            } else {
                off = att_nominal_alignby(off, attalignby);
                if !slow {
                    thisatt.attcacheoff.set(off as i32);
                }
            }

            // Bound the read at the finalized offset. Fixed-width reads span
            // attlen bytes; varlena/cstring dereference the header/first byte
            // (their full length is bounded at the advance step below).
            if attlen > 0 {
                if off + attlen as usize > data_len {
                    deform_corrupt();
                }
            } else if off >= data_len {
                deform_corrupt();
            }

            values_n[attnum] = fetch_att(tp.add(off), attbyval, attlen);

            if attlen > 0 {
                off += attlen as usize;
            } else if attlen == -1 {
                // off < data_len (checked above), so avail >= 1.
                match varsize_bounded(tp.add(off), data_len - off) {
                    Some(vlen) => off += vlen,
                    None => deform_corrupt(),
                }
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
    let data_len = tuple_data_len(tuple);
    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();
    let cstring_rest = {
        let atts_n = &tupleDesc.compact_attrs[..natts];
        let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
        // SAFETY: as heap_deform_tuple; natts == tuple natts here.
        unsafe { deform_walk(atts_n, values_n, isnull_n, tp, bp, hasnulls, data_len) }
    };
    if let Some((attnum, off)) = cstring_rest {
        let atts_n = &tupleDesc.compact_attrs[..natts];
        let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
        // SAFETY: as heap_deform_tuple.
        unsafe {
            deform_cstring_rest(atts_n, values_n, isnull_n, tp, bp, hasnulls, attnum, off, data_len);
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
    data_len: usize,
) {
    // SAFETY: caller contract — the walk covers attributes present in the
    // tuple; every deref below is bounded by data_len.
    unsafe {
        // Finish the cstring attribute already staged in deform_walk: its NUL
        // terminator must lie within the tuple body.
        off = cstring_end_bounded(tp, off, data_len);
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
                if off >= data_len {
                    deform_corrupt();
                }
                off = att_pointer_alignby(off, thisatt.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, thisatt.attalignby);
            }
            if attlen > 0 {
                if off + attlen as usize > data_len {
                    deform_corrupt();
                }
            } else if off >= data_len {
                deform_corrupt();
            }
            values_n[i] = fetch_att(tp.add(off), thisatt.attbyval, attlen);
            if attlen > 0 {
                off += attlen as usize;
            } else if attlen == -1 {
                match varsize_bounded(tp.add(off), data_len - off) {
                    Some(vlen) => off += vlen,
                    None => deform_corrupt(),
                }
            } else {
                debug_assert!(attlen == -2);
                off = cstring_end_bounded(tp, off, data_len);
            }
        }
    }
}

// Advance past a NUL-terminated cstring at `tp[off..]`, bounded by data_len.
// Returns the offset just past the terminator; a string with no NUL inside the
// tuple body is corruption (C's strlen would run off the end).
//
// # Safety
// `tp` points to an image readable for at least `data_len` bytes.
#[inline]
unsafe fn cstring_end_bounded(tp: *const u8, off: usize, data_len: usize) -> usize {
    let mut n = off;
    while n < data_len {
        // SAFETY: n < data_len, in range.
        if unsafe { *tp.add(n) } == 0 {
            return n + 1;
        }
        n += 1;
    }
    deform_corrupt();
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

#[cfg(test)]
mod deform_bounds_tests {
    use super::*;
    use crate::htup::{HeapTupleData, HeapTupleHeaderData, SizeofHeapTupleHeader};
    use crate::itemptr::ItemPointerData;
    use crate::tupdesc::CompactAttribute;
    use crate::varatt::set_varsize_4b_word;
    use ::datum::Datum;
    use alloc::vec;
    use core::cell::Cell;

    // 8-byte aligned scratch image (from_raw_parts wants MAXALIGN).
    #[repr(align(8))]
    struct Image([u8; 64]);

    fn att(attlen: i16, attbyval: bool, attalignby: u8) -> CompactAttribute {
        CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen,
            attbyval,
            attispackable: attlen < 0,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby,
        }
    }

    // Write the single-attribute header (natts=1) into an image. Kept separate
    // from tuple construction so payload writes don't alias the tuple's borrow.
    fn write_header(image: &mut Image, infomask: u16) {
        // SAFETY: Image is 8-aligned and >= SizeofHeapTupleHeader bytes.
        unsafe {
            let hdr = image.0.as_mut_ptr() as *mut HeapTupleHeaderData;
            (*hdr).t_infomask2 = 1; // natts = 1, no flag bits
            (*hdr).t_infomask = infomask;
            (*hdr).t_hoff = SizeofHeapTupleHeader as u8;
        }
    }

    fn make_tuple(image: &Image, data_len: u32) -> HeapTupleData<'_> {
        // SAFETY: header written by write_header; image outlives the tuple.
        unsafe {
            HeapTupleData::from_raw_parts(
                image.0.as_ptr(),
                SizeofHeapTupleHeader as u32 + data_len,
                ItemPointerData::invalid(),
                0,
            )
        }
    }

    // A wider-than-image descriptor (int8) over a 4-byte data area must raise a
    // deterministic corruption error, never read the 8 bytes past the tuple.
    #[test]
    #[should_panic(expected = "corrupt")]
    fn truncated_fixed_attribute_is_bounded() {
        let mut image = Image([0u8; 64]);
        write_header(&mut image, 0);
        let tuple = make_tuple(&image, 4);
        let atts = vec![att(8, true, 8)];
        let mut values = [Datum::null(); 1];
        let mut isnull = [false; 1];
        heap_deform_tuple_prefix_atts(&tuple, &atts, &mut values, &mut isnull, 1);
    }

    // A varlena whose 4-byte header declares a length running past the tuple
    // body must be rejected rather than staging an out-of-bounds datum.
    #[test]
    #[should_panic(expected = "corrupt")]
    fn lying_varlena_header_is_bounded() {
        let mut image = Image([0u8; 64]);
        write_header(&mut image, crate::htup::HEAP_HASVARWIDTH);
        // 8 data bytes, but the header claims a 100-byte varlena.
        // SAFETY: writing within the image's data area (offset t_hoff, 4 bytes).
        unsafe {
            image
                .0
                .as_mut_ptr()
                .add(SizeofHeapTupleHeader)
                .cast::<u32>()
                .write_unaligned(set_varsize_4b_word(100));
        }
        let tuple = make_tuple(&image, 8);
        let atts = vec![att(-1, false, 4)];
        let mut values = [Datum::null(); 1];
        let mut isnull = [false; 1];
        heap_deform_tuple_prefix_atts(&tuple, &atts, &mut values, &mut isnull, 1);
    }

    // A well-formed image deforms without error (guards never fire on valid
    // tuples): one int4 in a 4-byte body.
    #[test]
    fn well_formed_fixed_attribute_deforms() {
        let mut image = Image([0u8; 64]);
        write_header(&mut image, 0);
        // SAFETY: writing the int4 payload within the data area.
        unsafe {
            image
                .0
                .as_mut_ptr()
                .add(SizeofHeapTupleHeader)
                .cast::<i32>()
                .write_unaligned(0x1234_5678);
        }
        let tuple = make_tuple(&image, 4);
        let atts = vec![att(4, true, 4)];
        let mut values = [Datum::null(); 1];
        let mut isnull = [true; 1];
        heap_deform_tuple_prefix_atts(&tuple, &atts, &mut values, &mut isnull, 1);
        assert!(!isnull[0]);
        assert_eq!(values[0].as_i32(), 0x1234_5678);
    }
}
