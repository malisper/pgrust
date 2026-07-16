use ::datum::Datum;
use ::types_tuple::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetchatt,
};
use ::types_tuple::{
    getmissingattr, MinimalTupleData, SizeofMinimalTupleHeader, TupleDescData, HEAP_HASNULL,
    MINIMAL_TUPLE_OFFSET,
};

/// C's `heap_getattr` on a MinimalTuple body (tuplesortvariants.c reaches it
/// through a HeapTupleData wrapper pointing MINIMAL_TUPLE_OFFSET before the
/// allocation; the wrapper dissolves here, as in exectuples deform).
///
/// # Safety
/// `mt` points to a live, complete minimal-tuple image whose attributes match
/// `desc`; `attnum >= 1` (no system columns on this path).
#[inline]
pub(crate) unsafe fn minimal_getattr(
    mt: *const MinimalTupleData,
    attnum: i32,
    desc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    debug_assert!(attnum >= 1 && attnum <= desc.natts);
    let mtref = unsafe { &*mt };
    if attnum > mtref.natts() as i32 {
        return getmissingattr(desc, attnum, isnull);
    }
    let base = mt.cast::<u8>();
    let bp = unsafe { base.add(SizeofMinimalTupleHeader) };
    let hasnulls = (mtref.t_infomask & HEAP_HASNULL) != 0;
    let attidx = (attnum - 1) as usize;
    if hasnulls && unsafe { att_isnull(attidx, bp) } {
        *isnull = true;
        return Datum::null();
    }
    *isnull = false;
    let tp = unsafe { base.add(mtref.t_hoff as usize - MINIMAL_TUPLE_OFFSET) };
    let atts = &desc.compact_attrs[..=attidx];

    // nocachegetattr's usecache walk (heaptuple.c); the target's cached offset
    // short-circuits it, which is the steady state for fixed-offset keys.
    let target = &atts[attidx];
    if !hasnulls && target.attcacheoff.get() >= 0 {
        return unsafe { fetchatt(target, tp.add(target.attcacheoff.get() as usize)) };
    }

    let mut usecache = !hasnulls;
    let mut off = 0usize;
    let mut i = 0usize;
    loop {
        let att = &atts[i];
        if hasnulls && unsafe { att_isnull(i, bp) } {
            usecache = false;
            i += 1;
            continue;
        }
        let attlen = att.attlen as i32;
        // SAFETY: offsets walk attributes present in the tuple (caller contract).
        unsafe {
            if usecache && att.attcacheoff.get() >= 0 {
                off = att.attcacheoff.get() as usize;
            } else if attlen == -1 {
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
            if i == attidx {
                break;
            }
            off = att_addlength_pointer(off, attlen, tp.add(off));
        }
        if usecache && attlen <= 0 {
            usecache = false;
        }
        i += 1;
    }
    // SAFETY: off is the target attribute's computed in-image offset.
    unsafe { fetchatt(target, tp.add(off)) }
}

/// Linear all-attribute deform of a MinimalTuple body (the pgrcolumnar ingest-sort
/// drain; per-attribute `minimal_getattr` would be quadratic on wide rows).
///
/// # Safety
/// As [`minimal_getattr`]; `values`/`isnull` are at least `desc.natts` long.
pub(crate) unsafe fn minimal_deform(
    mt: *const MinimalTupleData,
    desc: &TupleDescData<'_>,
    values: &mut [Datum],
    isnull: &mut [bool],
) {
    let mtref = unsafe { &*mt };
    let base = mt.cast::<u8>();
    let bp = unsafe { base.add(SizeofMinimalTupleHeader) };
    let hasnulls = (mtref.t_infomask & HEAP_HASNULL) != 0;
    let tup_natts = (mtref.natts() as usize).min(desc.natts as usize);
    let tp = unsafe { base.add(mtref.t_hoff as usize - MINIMAL_TUPLE_OFFSET) };
    let atts = &desc.compact_attrs[..desc.natts as usize];
    let mut off = 0usize;
    for i in 0..tup_natts {
        let att = &atts[i];
        if hasnulls && unsafe { att_isnull(i, bp) } {
            values[i] = Datum::null();
            isnull[i] = true;
            continue;
        }
        isnull[i] = false;
        let attlen = att.attlen as i32;
        // SAFETY: offsets walk attributes present in the tuple (fn contract).
        unsafe {
            off = if attlen == -1 {
                att_pointer_alignby(off, att.attalignby, -1, tp.add(off))
            } else {
                att_nominal_alignby(off, att.attalignby)
            };
            values[i] = fetchatt(att, tp.add(off));
            off = att_addlength_pointer(off, attlen, tp.add(off));
        }
    }
    // Missing-attr tail (added columns since the tuple was formed).
    for i in tup_natts..desc.natts as usize {
        values[i] = getmissingattr(desc, i as i32 + 1, &mut isnull[i]);
    }
}
