//! typcache+IO: the multirange typcache lookup, `DatumGetMultirangeTypeP`
//! detoast, and the text/binary I/O functions.
//!
//! `multirange_in`/`recv` parse a multirange by delegating each member range to
//! the range type's own I/O proc (reached through `rangetypes-seams`);
//! `multirange_out`/`send` do the reverse. Owns the inward seams
//! `multirange_get_typcache` and `datum_get_multirange_type_p`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rangetypes::MultirangeTypeP;

/// `fn_extra` cache entry for one of the range I/O functions
/// (`MultirangeIOData`, multirangetypes.c:48): the multirange typcache plus the
/// member range type's I/O proc and its I/O parameter OID.
pub struct MultirangeIOData {
    /// `typcache` — the multirange type's typcache entry.
    pub typcache: TypeCacheEntry,
    /// `typioproc` — the range type's I/O proc.
    pub typioproc: FmgrInfo,
    /// `typioparam` — the range type's I/O parameter OID.
    pub typioparam: Oid,
}

/// `IOFuncSelector` (fmgr.h): which I/O direction `get_multirange_io_data`
/// resolves a proc for.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IOFuncSelector {
    /// `IOFunc_input`
    Input,
    /// `IOFunc_output`
    Output,
    /// `IOFunc_receive`
    Receive,
    /// `IOFunc_send`
    Send,
}

/// `multirange_get_typcache(fcinfo, mltrngtypid)` (multirangetypes.c:549): the
/// cached `TypeCacheEntry` for the multirange type. The inward
/// `multirange_get_typcache` seam.
pub fn multirange_get_typcache(mltrngtypid: Oid) -> PgResult<TypeCacheEntry> {
    let _ = mltrngtypid;
    todo!("port multirange_get_typcache (multirangetypes.c:549)")
}

/// `get_multirange_io_data(fcinfo, mltrngtypid, func)` (multirangetypes.c:415):
/// resolve and cache the multirange typcache + member range I/O proc.
pub fn get_multirange_io_data(
    mltrngtypid: Oid,
    func: IOFuncSelector,
) -> PgResult<MultirangeIOData> {
    let _ = (mltrngtypid, func);
    todo!("port get_multirange_io_data (multirangetypes.c:415)")
}

/// `DatumGetMultirangeTypeP(d)` (multirangetypes.h): detoast a `Datum` into a
/// `MultirangeType *`. The inward `datum_get_multirange_type_p` seam.
pub fn datum_get_multirange_type_p<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, d);
    todo!("port DatumGetMultirangeTypeP (multirangetypes.h)")
}

/// `multirange_in(PG_FUNCTION_ARGS)` (multirangetypes.c:117): parse a text
/// multirange literal into a serialized multirange.
pub fn multirange_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    mltrngtypoid: Oid,
    typmod: i32,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, input, mltrngtypoid, typmod);
    todo!("port multirange_in (multirangetypes.c:117)")
}

/// `multirange_out(PG_FUNCTION_ARGS)` (multirangetypes.c:299): render a
/// multirange as its text representation.
pub fn multirange_out(mcx: Mcx<'_>, multirange: Datum) -> PgResult<String> {
    let _ = (mcx, multirange);
    todo!("port multirange_out (multirangetypes.c:299)")
}

/// `multirange_recv(PG_FUNCTION_ARGS)` (multirangetypes.c:337): decode a
/// multirange from its binary wire representation.
pub fn multirange_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut &[u8],
    mltrngtypoid: Oid,
    typmod: i32,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let _ = (mcx, buf, mltrngtypoid, typmod);
    todo!("port multirange_recv (multirangetypes.c:337)")
}

/// `multirange_send(PG_FUNCTION_ARGS)` (multirangetypes.c:377): encode a
/// multirange into its binary wire representation.
pub fn multirange_send(mcx: Mcx<'_>, multirange: Datum) -> PgResult<Vec<u8>> {
    let _ = (mcx, multirange);
    todo!("port multirange_send (multirangetypes.c:377)")
}
