use ::datum::Datum;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b, vartag_size, VARHDRSZ_EXTERNAL,
};
use ::types_tuple::{TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN};

use crate::internals::{toast_compress_datum, toast_delete_datum, toast_save_datum};

pub const TOAST_NEEDS_DELETE_OLD: u8 = 0x01;
pub const TOAST_NEEDS_FREE: u8 = 0x02;
pub const TOAST_HAS_NULLS: u8 = 0x04;
pub const TOAST_NEEDS_CHANGE: u8 = 0x08;

pub const TOASTCOL_NEEDS_DELETE_OLD: u8 = TOAST_NEEDS_DELETE_OLD;
pub const TOASTCOL_NEEDS_FREE: u8 = TOAST_NEEDS_FREE;
pub const TOASTCOL_IGNORE: u8 = 0x10;
pub const TOASTCOL_INCOMPRESSIBLE: u8 = 0x20;

#[derive(Clone, Copy)]
pub struct ToastAttrInfo {
    pub tai_oldexternal: Option<Datum>,
    pub tai_size: i32,
    pub tai_colflags: u8,
    pub tai_compression: i8,
}

pub struct ToastTupleContext<'a, 'rel> {
    pub ttc_rel: &'a RelationData<'rel>,
    pub ttc_values: &'a mut [Datum],
    pub ttc_isnull: &'a [bool],
    pub ttc_oldvalues: Option<&'a [Datum]>,
    pub ttc_oldisnull: Option<&'a [bool]>,
    pub ttc_attr: &'a mut [ToastAttrInfo],
    pub ttc_flags: u8,
    // C NewHeap->rd_toastoid (CLUSTER rewrite), threaded instead of a
    // relcache field; InvalidOid outside a rewrite.
    pub ttc_toastoid: ::types_core::Oid,
}

/// # Safety
/// `d` carries a live varlena pointer readable for its full VARSIZE_ANY.
pub(crate) unsafe fn va_slice<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    unsafe {
        let len = if varatt_is_1b_e(p) {
            VARHDRSZ_EXTERNAL + vartag_size(*p.add(1))
        } else if varatt_is_1b(p) {
            varsize_1b(p)
        } else {
            varsize_4b(p)
        };
        core::slice::from_raw_parts(p, len)
    }
}

#[inline]
pub(crate) fn is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

#[inline]
fn is_external_ondisk(b: &[u8]) -> bool {
    is_external(b) && b[1] == ::types_tuple::varatt::VARTAG_ONDISK
}

#[inline]
pub(crate) fn is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

pub(crate) fn leak_datum(v: ::mcx::PgVec<'_, u8>) -> Datum {
    Datum::from_usize(v.leak().as_ptr() as usize)
}

pub fn toast_tuple_init<'mcx>(mcx: Mcx<'mcx>, ttc: &mut ToastTupleContext<'_, '_>) -> PgResult<()> {
    let tuple_desc = &ttc.ttc_rel.rd_att;
    let num_attrs = tuple_desc.natts as usize;

    ttc.ttc_flags = 0;

    for i in 0..num_attrs {
        let att = &tuple_desc.attrs[i];
        let mut new_value = ttc.ttc_values[i];

        ttc.ttc_attr[i] = ToastAttrInfo {
            tai_colflags: 0,
            tai_oldexternal: None,
            tai_compression: att.attcompression,
            tai_size: 0,
        };

        if let (Some(oldvalues), Some(oldisnull)) = (ttc.ttc_oldvalues, ttc.ttc_oldisnull) {
            let old_value = oldvalues[i];
            // SAFETY: non-null deformed varlena datums point into live images.
            if att.attlen == -1
                && !oldisnull[i]
                && is_external_ondisk(unsafe { va_slice(old_value) })
            {
                let old_img = unsafe { va_slice(old_value) };
                let changed = ttc.ttc_isnull[i]
                    || !is_external_ondisk(unsafe { va_slice(new_value) })
                    || unsafe { va_slice(new_value) } != old_img;
                if changed {
                    ttc.ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_DELETE_OLD;
                    ttc.ttc_flags |= TOAST_NEEDS_DELETE_OLD;
                } else {
                    ttc.ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
                    continue;
                }
            }
        }

        if ttc.ttc_isnull[i] {
            ttc.ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
            ttc.ttc_flags |= TOAST_HAS_NULLS;
            continue;
        }

        if att.attlen == -1 {
            if att.attstorage == TYPSTORAGE_PLAIN {
                ttc.ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
            }

            // SAFETY: non-null deformed varlena datum.
            if is_external(unsafe { va_slice(new_value) }) {
                ttc.ttc_attr[i].tai_oldexternal = Some(new_value);
                let attr = unsafe { va_slice(new_value) };
                let fetched = if att.attstorage == TYPSTORAGE_PLAIN {
                    detoast::detoast_attr(mcx, attr)?
                } else {
                    detoast::detoast_external_attr(mcx, attr)?
                };
                new_value = leak_datum(fetched);
                ttc.ttc_values[i] = new_value;
                ttc.ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_FREE;
                ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
            }

            ttc.ttc_attr[i].tai_size = unsafe { va_slice(new_value) }.len() as i32;
        } else {
            ttc.ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
        }
    }
    Ok(())
}

pub fn toast_tuple_find_biggest_attribute(
    ttc: &ToastTupleContext<'_, '_>,
    for_compression: bool,
    check_main: bool,
) -> i32 {
    let tuple_desc = &ttc.ttc_rel.rd_att;
    let num_attrs = tuple_desc.natts as usize;
    let mut biggest_attno: i32 = -1;
    let mut biggest_size: i32 = ::types_tuple::MAXALIGN(toastdesc::TOAST_POINTER_SIZE) as i32;
    let mut skip_colflags = TOASTCOL_IGNORE;

    if for_compression {
        skip_colflags |= TOASTCOL_INCOMPRESSIBLE;
    }

    for i in 0..num_attrs {
        let att = &tuple_desc.attrs[i];

        if (ttc.ttc_attr[i].tai_colflags & skip_colflags) != 0 {
            continue;
        }
        // SAFETY: columns not flagged IGNORE hold live non-null varlenas.
        let v = unsafe { va_slice(ttc.ttc_values[i]) };
        if is_external(v) {
            continue; // can't happen, toast_action would be PLAIN
        }
        if for_compression && is_compressed(v) {
            continue;
        }
        if check_main && att.attstorage != TYPSTORAGE_MAIN {
            continue;
        }
        if !check_main
            && att.attstorage != TYPSTORAGE_EXTENDED
            && att.attstorage != TYPSTORAGE_EXTERNAL
        {
            continue;
        }

        if ttc.ttc_attr[i].tai_size > biggest_size {
            biggest_attno = i as i32;
            biggest_size = ttc.ttc_attr[i].tai_size;
        }
    }

    biggest_attno
}

pub fn toast_tuple_try_compression<'mcx>(
    mcx: Mcx<'mcx>,
    ttc: &mut ToastTupleContext<'_, '_>,
    attribute: usize,
) -> PgResult<()> {
    let attr = &mut ttc.ttc_attr[attribute];
    // SAFETY: candidate columns hold live non-null varlenas.
    let value = unsafe { va_slice(ttc.ttc_values[attribute]) };
    match toast_compress_datum(mcx, value, attr.tai_compression)? {
        Some(new_value) => {
            attr.tai_size = new_value.len() as i32;
            ttc.ttc_values[attribute] = leak_datum(new_value);
            attr.tai_colflags |= TOASTCOL_NEEDS_FREE;
            ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
        }
        None => {
            attr.tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
        }
    }
    Ok(())
}

pub fn toast_tuple_externalize<'mcx>(
    mcx: Mcx<'mcx>,
    ttc: &mut ToastTupleContext<'_, '_>,
    attribute: usize,
    options: i32,
) -> PgResult<()> {
    let attr = &mut ttc.ttc_attr[attribute];
    // SAFETY: candidate columns hold live non-null varlenas.
    let old_value = unsafe { va_slice(ttc.ttc_values[attribute]) };
    attr.tai_colflags |= TOASTCOL_IGNORE;
    let oldexternal = attr.tai_oldexternal.map(|d| unsafe { va_slice(d) });
    let pointer =
        toast_save_datum(mcx, ttc.ttc_rel, old_value, oldexternal, ttc.ttc_toastoid, options)?;
    let mut img = ::mcx::vec_with_capacity_in(mcx, pointer.len())?;
    ::mcx::vec_append_bytes(&mut img, &pointer)?;
    ttc.ttc_values[attribute] = leak_datum(img);
    attr.tai_colflags |= TOASTCOL_NEEDS_FREE;
    ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
    Ok(())
}

// TOAST_NEEDS_FREE releases are arena resets here (values die with the
// caller's scratch mcx); only the delete-old external work remains.
pub fn toast_tuple_cleanup<'mcx>(
    mcx: Mcx<'mcx>,
    ttc: &mut ToastTupleContext<'_, '_>,
) -> PgResult<()> {
    let num_attrs = ttc.ttc_rel.rd_att.natts as usize;

    if (ttc.ttc_flags & TOAST_NEEDS_DELETE_OLD) != 0 {
        let oldvalues = ttc.ttc_oldvalues.expect("delete-old flagged without old values");
        for i in 0..num_attrs {
            if (ttc.ttc_attr[i].tai_colflags & TOASTCOL_NEEDS_DELETE_OLD) != 0 {
                // SAFETY: flagged columns hold live on-disk external pointers.
                let v = unsafe { va_slice(oldvalues[i]) };
                toast_delete_datum(mcx, ttc.ttc_rel, v, false)?;
            }
        }
    }
    Ok(())
}

pub fn toast_delete_external<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
    values: &[Datum],
    isnull: &[bool],
    is_speculative: bool,
) -> PgResult<()> {
    let tuple_desc = &rel.rd_att;
    let num_attrs = tuple_desc.natts as usize;

    for i in 0..num_attrs {
        if tuple_desc.compact_attrs[i].attlen == -1 {
            if isnull[i] {
                continue;
            }
            // SAFETY: non-null deformed varlena datum.
            let v = unsafe { va_slice(values[i]) };
            if is_external_ondisk(v) {
                toast_delete_datum(mcx, rel, v, is_speculative)?;
            }
        }
    }
    Ok(())
}
