//! SPI tuple accessors (`spi.c`): the leaf helpers that read a single column
//! out of a result tuple / its descriptor — [`SPI_fnumber`], [`SPI_getbinval`],
//! [`SPI_gettypeid`].
//!
//! Each is a pure leaf over a [`TupleDescData`] (and, for `SPI_getbinval`, a
//! materialized [`FormedTuple`] read with `heap_getattr`); the only global they
//! touch is the public `SPI_result` (set via [`crate::backbone::set_spi_result`]),
//! exactly as C does. They take the descriptor / tuple as arguments rather than
//! reaching into `SPI_tuptable`, mirroring the C entry points (the SPI consumer
//! passes `SPI_tuptable->tupdesc` / a `SPI_tuptable->vals[i]` it already holds).

use crate::backbone::set_spi_result;
use crate::result_code::SPI_ERROR_NOATTRIBUTE;
use backend_access_common_heaptuple::{heap_getattr, Datum, FormedTuple};
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_tuple::heaptuple::{FirstLowInvalidHeapAttributeNumber, TupleDescData};

/// `true` when a 1-based `fnumber` is out of range for `tupdesc`, mirroring C's
/// shared guard in `SPI_getbinval` / `SPI_gettype` / `SPI_gettypeid` /
/// `SPI_fname`:
///
/// ```c
/// fnumber > tupdesc->natts || fnumber == 0 ||
///     fnumber <= FirstLowInvalidHeapAttributeNumber
/// ```
fn fnumber_out_of_range(tupdesc: &TupleDescData<'_>, fnumber: i32) -> bool {
    fnumber > tupdesc.natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as i32
}

/// `SPI_fnumber(TupleDesc tupdesc, const char *fname)` (`spi.c:1174`) — the
/// 1-based attribute number of the column named `fname`, or
/// `SPI_ERROR_NOATTRIBUTE` when there is no such (non-dropped) column and no
/// system column of that name.
///
/// Faithful to C: scans `tupdesc` comparing `attname` (skipping dropped
/// columns), then falls back to [`SystemAttributeByName`]. Does **not** touch
/// `SPI_result`.
///
/// [`SystemAttributeByName`]: backend_catalog_heap::SystemAttributeByName
pub fn SPI_fnumber(tupdesc: &TupleDescData<'_>, fname: &[u8]) -> i32 {
    // for (res = 0; res < tupdesc->natts; res++) { ... return res + 1; }
    for res in 0..tupdesc.natts {
        let attr = tupdesc.attr(res as usize);
        // namestrcmp(&attr->attname, fname) == 0 && !attr->attisdropped
        if attr.attname.name_str() == fname && !attr.attisdropped {
            return res + 1;
        }
    }

    // sysatt = SystemAttributeByName(fname); if (sysatt != NULL) return sysatt->attnum;
    if let Some(sysatt) = backend_catalog_heap::SystemAttributeByName(fname) {
        return sysatt.attnum as i32;
    }

    // SPI_ERROR_NOATTRIBUTE is different from all sys column numbers.
    SPI_ERROR_NOATTRIBUTE
}

/// `SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull)`
/// (`spi.c:1251`) — the raw `(Datum, isnull)` of column `fnumber` in `tuple`.
///
/// `tuple` is the materialized [`FormedTuple`] the SPI tuptable holds (C's
/// `HeapTuple`). Faithful to C: clears `SPI_result`, range-checks `fnumber`
/// (setting `SPI_ERROR_NOATTRIBUTE` + `isnull = true` and returning a NULL
/// Datum on a bad index), then `heap_getattr`.
pub fn SPI_getbinval<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
    tupdesc: &TupleDescData<'_>,
    fnumber: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
    // SPI_result = 0;
    set_spi_result(0);

    if fnumber_out_of_range(tupdesc, fnumber) {
        // SPI_result = SPI_ERROR_NOATTRIBUTE; *isnull = true; return (Datum) NULL;
        set_spi_result(SPI_ERROR_NOATTRIBUTE);
        return Ok((Datum::null(), true));
    }

    // return heap_getattr(tuple, fnumber, tupdesc, isnull);
    heap_getattr(mcx, tuple, fnumber, tupdesc)
}

/// `SPI_gettypeid(TupleDesc tupdesc, int fnumber)` (`spi.c:1307`) — the data
/// type OID of column `fnumber`, or `InvalidOid` (with `SPI_result` set to
/// `SPI_ERROR_NOATTRIBUTE`) for a bad index.
///
/// Faithful to C: clears `SPI_result`, range-checks, then reads `atttypid` from
/// the descriptor (user column) or [`SystemAttributeDefinition`] (system
/// column).
///
/// [`SystemAttributeDefinition`]: backend_catalog_heap::SystemAttributeDefinition
pub fn SPI_gettypeid(tupdesc: &TupleDescData<'_>, fnumber: i32) -> PgResult<Oid> {
    // SPI_result = 0;
    set_spi_result(0);

    if fnumber_out_of_range(tupdesc, fnumber) {
        // SPI_result = SPI_ERROR_NOATTRIBUTE; return InvalidOid;
        set_spi_result(SPI_ERROR_NOATTRIBUTE);
        return Ok(InvalidOid);
    }

    if fnumber > 0 {
        // return TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
        Ok(tupdesc.attr((fnumber - 1) as usize).atttypid)
    } else {
        // return (SystemAttributeDefinition(fnumber))->atttypid;
        Ok(backend_catalog_heap::SystemAttributeDefinition(fnumber as i16)?.atttypid)
    }
}
