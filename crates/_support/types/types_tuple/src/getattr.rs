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

pub fn nocachegetattr(tup: &HeapTupleData<'_>, attnum: i32, tupleDesc: &TupleDescData<'_>) -> Datum {
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
    let atts = &tupleDesc.compact_attrs[..tupleDesc.natts as usize];
    let mut off: usize;

    if !slow {
        let att = &atts[attnum];
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
        off = atts[attnum].attcacheoff.get() as usize;
    } else {
        let mut usecache = true;
        off = 0;
        // Slicing to ..=attnum makes the i <= attnum loop bound the slice bound,
        // so the per-iteration indexing check folds away.
        let watts = &atts[..=attnum];
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

    // SAFETY: off is the attribute's computed in-image offset.
    unsafe { fetchatt(&atts[attnum], tp.add(off)) }
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

#[inline]
pub fn fastgetattr(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    debug_assert!(attnum > 0);
    *isnull = false;
    if tup.no_nulls() {
        let att = &tupleDesc.compact_attrs[(attnum - 1) as usize];
        if att.attcacheoff.get() >= 0 {
            // SAFETY: cached offset points at the live attribute within the image.
            unsafe { fetchatt(att, tup.getstruct().add(att.attcacheoff.get() as usize)) }
        } else {
            nocachegetattr(tup, attnum, tupleDesc)
        }
    } else {
        // SAFETY: HASNULL bitmap covers attnum-1 (attnum <= natts, caller contract).
        if unsafe { att_isnull((attnum - 1) as usize, tup.bits_ptr()) } {
            *isnull = true;
            Datum::null()
        } else {
            nocachegetattr(tup, attnum, tupleDesc)
        }
    }
}

#[inline]
pub fn heap_getattr(
    tup: &HeapTupleData<'_>,
    attnum: i32,
    tupleDesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    if attnum > 0 {
        if attnum > tup.t_data().natts() as i32 {
            getmissingattr(tupleDesc, attnum, isnull)
        } else {
            fastgetattr(tup, attnum, tupleDesc, isnull)
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
    let tdesc_natts = tupleDesc.natts as usize;
    // Inheritance can hand a tuple wider than the descriptor; clamp to both.
    let natts = (tup.natts() as usize).min(tdesc_natts);
    let atts = &tupleDesc.compact_attrs[..tdesc_natts];

    let tp = tuple.getstruct();
    let bp = tuple.bits_ptr();
    let mut off = 0usize;
    let mut slow = false;

    let atts_n = &atts[..natts];
    let (values_n, isnull_n) = (&mut values[..natts], &mut isnull[..natts]);
    for attnum in 0..natts {
        let thisatt = &atts_n[attnum];
        // Locals: the Cell field makes the struct non-readonly to LLVM, which
        // would otherwise reload these after every attcacheoff store.
        let attlen = thisatt.attlen;
        let attbyval = thisatt.attbyval;
        let attalignby = thisatt.attalignby;
        // SAFETY: bitmap/image reads walk attributes present in the tuple.
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

            values_n[attnum] = fetch_att(tp.add(off), attbyval, attlen as i32);

            off = att_addlength_pointer(off, attlen as i32, tp.add(off));
        }

        if attlen <= 0 {
            slow = true;
        }
    }

    for attnum in natts..tdesc_natts {
        values[attnum] = getmissingattr(tupleDesc, (attnum + 1) as i32, &mut isnull[attnum]);
    }
}
