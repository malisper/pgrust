//! access/itup.h + the nbtree.h tuple macros over raw on-page bytes.
//! Unsafe page kernel: every entry takes an `itup` pointer that must point at
//! a live, MAXALIGNed index tuple that stays mapped for the call (pin held).
//! Moves to the common indextuple unit when that lands.

use ::datum::Datum;
use ::types_core::AttrNumber;
use ::types_nbtree::{
    BT_IS_POSTING, BT_OFFSET_MASK, BT_PIVOT_HEAP_TID_ATTR, INDEX_ALT_TID_MASK,
};
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumberNoCheck};
use ::types_tuple::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetchatt,
};
use ::types_tuple::TupleDescData;

pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;

const INDEX_TUPLE_DATA_SIZE: usize = 8;
// MAXALIGN(sizeof(IndexTupleData) + sizeof(IndexAttributeBitMapData))
const INDEX_TUPLE_DATA_WITH_NULLS_SIZE: usize = 16;

pub type ITup = *const u8;

#[inline]
pub unsafe fn t_info(itup: ITup) -> u16 {
    itup.add(6).cast::<u16>().read()
}

#[inline]
pub unsafe fn t_tid(itup: ITup) -> ItemPointerData {
    itup.cast::<ItemPointerData>().read()
}

#[inline]
pub unsafe fn index_tuple_size(itup: ITup) -> usize {
    (t_info(itup) & INDEX_SIZE_MASK) as usize
}

#[inline]
pub unsafe fn index_tuple_has_nulls(itup: ITup) -> bool {
    (t_info(itup) & INDEX_NULL_MASK) != 0
}

#[inline]
pub const fn index_info_find_data_offset(info: u16) -> usize {
    if info & INDEX_NULL_MASK == 0 {
        INDEX_TUPLE_DATA_SIZE
    } else {
        INDEX_TUPLE_DATA_WITH_NULLS_SIZE
    }
}

#[inline]
pub unsafe fn bt_tuple_is_pivot(itup: ITup) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0
        && (t_tid(itup).ip_posid & BT_IS_POSTING) == 0
}

#[inline]
pub unsafe fn bt_tuple_is_posting(itup: ITup) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0
        && (t_tid(itup).ip_posid & BT_IS_POSTING) != 0
}

#[inline]
pub unsafe fn bt_tuple_get_nposting(itup: ITup) -> usize {
    debug_assert!(bt_tuple_is_posting(itup));
    (t_tid(itup).ip_posid & BT_OFFSET_MASK) as usize
}

#[inline]
pub unsafe fn bt_tuple_get_posting_offset(itup: ITup) -> usize {
    debug_assert!(bt_tuple_is_posting(itup));
    ItemPointerGetBlockNumberNoCheck(&t_tid(itup)) as usize
}

#[inline]
pub unsafe fn bt_tuple_get_posting_n(itup: ITup, n: usize) -> ItemPointerData {
    itup.add(bt_tuple_get_posting_offset(itup) + n * core::mem::size_of::<ItemPointerData>())
        .cast::<ItemPointerData>()
        .read_unaligned()
}

#[inline]
pub unsafe fn bt_tuple_get_natts(itup: ITup, indnatts: i32) -> i32 {
    if bt_tuple_is_pivot(itup) {
        (t_tid(itup).ip_posid & BT_OFFSET_MASK) as i32
    } else {
        indnatts
    }
}

#[inline]
pub unsafe fn bt_tuple_get_downlink(pivot: ITup) -> ::types_core::BlockNumber {
    ItemPointerGetBlockNumberNoCheck(&t_tid(pivot))
}

/// BTreeTupleGetHeapTID: lowest heap TID, `None` when a pivot truncated it away.
pub unsafe fn bt_tuple_get_heap_tid(itup: ITup) -> Option<ItemPointerData> {
    if bt_tuple_is_pivot(itup) {
        if (t_tid(itup).ip_posid & BT_PIVOT_HEAP_TID_ATTR) != 0 {
            let off = index_tuple_size(itup) - core::mem::size_of::<ItemPointerData>();
            return Some(itup.add(off).cast::<ItemPointerData>().read_unaligned());
        }
        None
    } else if bt_tuple_is_posting(itup) {
        Some(bt_tuple_get_posting_n(itup, 0))
    } else {
        Some(t_tid(itup))
    }
}

pub unsafe fn bt_tuple_get_max_heap_tid(itup: ITup) -> ItemPointerData {
    debug_assert!(!bt_tuple_is_pivot(itup));
    if bt_tuple_is_posting(itup) {
        bt_tuple_get_posting_n(itup, bt_tuple_get_nposting(itup) - 1)
    } else {
        t_tid(itup)
    }
}

/// index_getattr (itup.h): borrowed deform — by-ref values are pointers into
/// the page image (family-2 rule); attcacheoff is live via CompactAttribute.
///
/// # Safety
/// `itup` per module contract; `attnum` in `1..=natts` for this tuple/desc.
#[inline]
pub unsafe fn index_getattr(
    itup: ITup,
    attnum: AttrNumber,
    tupdesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    debug_assert!(attnum >= 1);
    *isnull = false;
    let a = (attnum - 1) as usize;
    if !index_tuple_has_nulls(itup) {
        let att = tupdesc.compact_attr(a);
        if att.attcacheoff.get() >= 0 {
            return fetchatt(
                att,
                itup.add(INDEX_TUPLE_DATA_SIZE + att.attcacheoff.get() as usize),
            );
        }
        nocache_index_getattr(itup, attnum, tupdesc)
    } else {
        if att_isnull(a, itup.add(INDEX_TUPLE_DATA_SIZE)) {
            *isnull = true;
            return Datum::null();
        }
        nocache_index_getattr(itup, attnum, tupdesc)
    }
}

/// nocache_index_getattr (indextuple.c); mirrors types_tuple::nocachegetattr
/// with the index tuple's header size and null-bitmap placement.
///
/// # Safety
/// As [`index_getattr`].
unsafe fn nocache_index_getattr(
    itup: ITup,
    attnum: AttrNumber,
    tupdesc: &TupleDescData<'_>,
) -> Datum {
    let info = t_info(itup);
    let hasnulls = (info & INDEX_NULL_MASK) != 0;
    let mut slow = false;
    let attnum = (attnum - 1) as usize;
    let bp = itup.add(INDEX_TUPLE_DATA_SIZE);

    if hasnulls {
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

    let tp = itup.add(index_info_find_data_offset(info));
    let atts = &tupdesc.compact_attrs[..];
    debug_assert!(attnum < atts.len());
    let mut off: usize;

    if !slow {
        let att = atts.get_unchecked(attnum);
        if att.attcacheoff.get() >= 0 {
            return fetchatt(att, tp.add(att.attcacheoff.get() as usize));
        }

        if (info & INDEX_VAR_MASK) != 0 {
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
        off = atts.get_unchecked(attnum).attcacheoff.get() as usize;
    } else {
        let mut usecache = true;
        off = 0;
        let watts = atts.get_unchecked(..=attnum);
        let mut i = 0;
        loop {
            let att = &watts[i];
            let attlen = att.attlen;
            if hasnulls && att_isnull(i, bp) {
                usecache = false;
                i += 1;
                continue;
            }

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

            if i == attnum {
                break;
            }

            off = att_addlength_pointer(off, attlen as i32, tp.add(off));
            if usecache && attlen <= 0 {
                usecache = false;
            }
            i += 1;
        }
    }

    fetchatt(atts.get_unchecked(attnum), tp.add(off))
}
