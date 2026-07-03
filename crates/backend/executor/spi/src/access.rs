use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::{set_spi_result, SPI_ERROR_NOATTRIBUTE};

const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

fn bad_fnumber(tupdesc: &TupleDescData<'_>, fnumber: i32) -> bool {
    fnumber > tupdesc.natts || fnumber == 0 || fnumber <= FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER
}

pub fn SPI_fnumber(tupdesc: &TupleDescData<'_>, fname: &str) -> i32 {
    for i in 0..tupdesc.natts as usize {
        let att = &tupdesc.attrs[i];
        if att.attname.name_str() == fname.as_bytes() && !att.attisdropped {
            return i as i32 + 1;
        }
    }
    if let Some(sysatt) = catalog_heap::SystemAttributeByName(fname) {
        return sysatt.attnum as i32;
    }
    SPI_ERROR_NOATTRIBUTE
}

pub fn SPI_gettypeid(tupdesc: &TupleDescData<'_>, fnumber: i32) -> Oid {
    set_spi_result(0);
    if bad_fnumber(tupdesc, fnumber) {
        set_spi_result(SPI_ERROR_NOATTRIBUTE);
        return InvalidOid;
    }
    if fnumber > 0 {
        tupdesc.attrs[fnumber as usize - 1].atttypid
    } else {
        catalog_heap::SystemAttributeDefinition(fnumber as i16).atttypid
    }
}

pub fn SPI_getbinval(
    tuple: &HeapTupleData<'_>,
    tupdesc: &TupleDescData<'_>,
    fnumber: i32,
) -> (Datum, bool) {
    set_spi_result(0);
    if bad_fnumber(tupdesc, fnumber) {
        set_spi_result(SPI_ERROR_NOATTRIBUTE);
        return (Datum::null(), true);
    }
    let mut isnull = false;
    // SAFETY: SPI tuptable tuples are formed against their own tupdesc
    // (spi_dest_startup copies the executor descriptor).
    let d = unsafe { types_tuple::heap_getattr(tuple, fnumber, tupdesc, &mut isnull) };
    (d, isnull)
}

pub fn SPI_getvalue<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tupdesc: &TupleDescData<'_>,
    fnumber: i32,
) -> PgResult<Option<&'mcx [u8]>> {
    set_spi_result(0);
    if bad_fnumber(tupdesc, fnumber) {
        set_spi_result(SPI_ERROR_NOATTRIBUTE);
        return Ok(None);
    }
    let (val, isnull) = SPI_getbinval(tuple, tupdesc, fnumber);
    if isnull {
        return Ok(None);
    }
    let typoid = if fnumber > 0 {
        tupdesc.attrs[fnumber as usize - 1].atttypid
    } else {
        catalog_heap::SystemAttributeDefinition(fnumber as i16).atttypid
    };
    let (foutoid, _typisvarlena) = lsyscache::typ::getTypeOutputInfo(typoid)?;
    let mut finfo = fmgr_core::fmgr_info(foutoid)?;
    let out = types_fmgr::function_call1_coll_in(&mut finfo, InvalidOid, mcx, val)?;
    // SAFETY: type output functions return a NUL-terminated cstring datum
    // allocated in mcx (printtup/copyto precedent).
    let s = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }
        .to_bytes();
    Ok(Some(mcx::slice_borrow_in(mcx, s)?))
}
