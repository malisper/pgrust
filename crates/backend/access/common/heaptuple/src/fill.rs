use ::datum::Datum;
use ::types_tuple::tupmacs::{
    att_addlength_datum, att_datum_alignby, att_nominal_alignby, store_att_byval,
};
use ::types_tuple::varatt::{
    set_varsize_short, varatt_can_make_short, varatt_converted_short_size, varatt_is_1b,
    varatt_is_1b_e, varatt_is_external_expanded, varsize_1b, varsize_4b, varsize_external,
    VARHDRSZ,
};
use ::types_tuple::{
    bits8, CompactAttribute, TupleDescData, HEAP_HASEXTERNAL, HEAP_HASNULL, HEAP_HASVARWIDTH,
};

#[cold]
#[inline(never)]
pub(crate) fn expanded_object_unsupported() -> ! {
    panic!("expanded-object flatten: datum::expandeddatum vocabulary landed; heap_compute_data_size/heap_fill_tuple two-pass flatten arms not wired here")
}

/// Data-area size of a tuple to be constructed; by-ref datums must be live.
#[inline]
pub fn heap_compute_data_size(
    tupleDesc: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> usize {
    let natts = tupleDesc.natts as usize;
    let atts = &tupleDesc.compact_attrs[..natts];
    let (values, isnull) = (&values[..natts], &isnull[..natts]);
    let mut data_length = 0usize;

    for i in 0..natts {
        if isnull[i] {
            continue;
        }
        let val = values[i];
        let atti = &atts[i];
        let attlen = atti.attlen as i32;
        let attispackable = atti.attispackable;
        let attalignby = atti.attalignby;

        // SAFETY: by-ref datums are live per the function contract.
        unsafe {
            let p = val.as_usize() as *const u8;
            if attlen == -1 && attispackable && varatt_can_make_short(p) {
                data_length += varatt_converted_short_size(p);
            } else if attlen == -1 && varatt_is_external_expanded(p) {
                expanded_object_unsupported();
            } else {
                data_length = att_datum_alignby(data_length, attalignby, attlen, val);
                data_length = att_addlength_datum(data_length, attlen, val);
            }
        }
    }

    data_length
}

/// # Safety
/// `p` points to a live NUL-terminated cstring.
#[inline]
unsafe fn strlen(p: *const u8) -> usize {
    let mut n = 0usize;
    while *p.add(n) != 0 {
        n += 1;
    }
    n
}

/// # Safety
/// `base` is the MAXALIGN-aligned pre-zeroed data area, writable past `off` for
/// this value; `bits` is the pre-zeroed bitmap + this attribute's bit index;
/// by-ref datums live per their declared shape.
#[inline(always)]
pub(crate) unsafe fn fill_val(
    att: &CompactAttribute,
    bits: Option<(*mut bits8, usize)>,
    base: *mut u8,
    off: &mut usize,
    infomask: &mut u16,
    datum: Datum,
    isnull: bool,
) {
    // Locals: the attcacheoff Cell makes CompactAttribute non-readonly to
    // LLVM; without copies every image store forces field reloads.
    let attlen = att.attlen;
    let attbyval = att.attbyval;
    let attispackable = att.attispackable;
    let attalignby = att.attalignby;

    if let Some((bp, idx)) = bits {
        if isnull {
            *infomask |= HEAP_HASNULL;
            return;
        }
        *bp.add(idx >> 3) |= 1 << (idx & 0x07);
    }

    let data_length;
    if attbyval {
        *off = att_nominal_alignby(*off, attalignby);
        store_att_byval(base.add(*off), datum, attlen as i32);
        data_length = attlen as usize;
    } else if attlen == -1 {
        let val = datum.as_usize() as *const u8;
        *infomask |= HEAP_HASVARWIDTH;
        if varatt_is_1b_e(val) {
            if varatt_is_external_expanded(val) {
                expanded_object_unsupported();
            }
            *infomask |= HEAP_HASEXTERNAL;
            data_length = varsize_external(val);
            core::ptr::copy_nonoverlapping(val, base.add(*off), data_length);
        } else if varatt_is_1b(val) {
            data_length = varsize_1b(val);
            core::ptr::copy_nonoverlapping(val, base.add(*off), data_length);
        } else if attispackable && varatt_can_make_short(val) {
            data_length = varatt_converted_short_size(val);
            set_varsize_short(base.add(*off), data_length);
            core::ptr::copy_nonoverlapping(
                val.add(VARHDRSZ),
                base.add(*off + 1),
                data_length - 1,
            );
        } else {
            *off = att_nominal_alignby(*off, attalignby);
            data_length = varsize_4b(val);
            core::ptr::copy_nonoverlapping(val, base.add(*off), data_length);
        }
    } else if attlen == -2 {
        *infomask |= HEAP_HASVARWIDTH;
        debug_assert!(attalignby == 1);
        let val = datum.as_usize() as *const u8;
        data_length = strlen(val) + 1;
        core::ptr::copy_nonoverlapping(val, base.add(*off), data_length);
    } else {
        *off = att_nominal_alignby(*off, attalignby);
        debug_assert!(attlen > 0);
        data_length = attlen as usize;
        core::ptr::copy_nonoverlapping(datum.as_usize() as *const u8, base.add(*off), data_length);
    }

    *off += data_length;
}

/// # Safety
/// `data..data+data_size` is the writable, PRE-ZEROED, MAXALIGN-aligned data
/// area sized by [`heap_compute_data_size`] over the same inputs; `bits` (if
/// present) covers BITMAPLEN(natts) pre-zeroed bytes; by-ref datums are live.
// always: outlined this cost ~35 instr/call vs C's inlined fill (journal).
#[inline(always)]
pub unsafe fn heap_fill_tuple(
    tupleDesc: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
    data: *mut u8,
    data_size: usize,
    infomask: &mut u16,
    bits: Option<*mut bits8>,
) {
    let natts = tupleDesc.natts as usize;
    let atts = &tupleDesc.compact_attrs[..natts];
    let (values, isnull) = (&values[..natts], &isnull[..natts]);

    *infomask &= !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

    let mut off = 0usize;
    // Split loops hoist C's per-attribute `bit != NULL` test.
    match bits {
        Some(bp) => {
            for i in 0..natts {
                fill_val(&atts[i], Some((bp, i)), data, &mut off, infomask, values[i], isnull[i]);
            }
        }
        None => {
            for i in 0..natts {
                fill_val(&atts[i], None, data, &mut off, infomask, values[i], false);
            }
        }
    }

    debug_assert_eq!(off, data_size);
    let _ = data_size;
}
