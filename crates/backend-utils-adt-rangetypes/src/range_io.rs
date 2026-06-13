//! Family `range-io`: text and binary I/O.
//!
//! Mirrors `rangetypes.c`: `range_in` / `range_out`, `range_recv` /
//! `range_send`, `get_range_io_data`, and the private `range_parse` /
//! `range_parse_flags` / `range_parse_bound` / `range_deparse` /
//! `range_bound_escape` helpers. Element text/binary I/O routes through the
//! element type's typio support fns (fmgr seam); the wire buffer through the
//! pqformat seam.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_rangetypes::RangeTypeP;

/// `RangeIOData` (rangetypes.c:50): the cached per-range-type I/O support: the
/// element type's typcache entry plus its in/out/recv/send function infos. The
/// fmgr `FmgrInfo`s are inherited-opacity handles owned by fmgr.
#[derive(Clone, Debug, Default)]
pub struct RangeIOData {
    /// `typcache` — the range type's cache entry.
    pub typcache: TypeCacheEntry,
    /// `typiofunc` — the element type's I/O function OID for the requested op.
    pub typiofunc: Oid,
    /// `typioparam` — the element type's I/O parameter OID.
    pub typioparam: Oid,
}

/// `IOFuncSelector` (fmgr.h): which element I/O direction `get_range_io_data`
/// resolves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IOFuncSelector {
    /// `IOFunc_input`.
    Input,
    /// `IOFunc_output`.
    Output,
    /// `IOFunc_receive`.
    Receive,
    /// `IOFunc_send`.
    Send,
}

/// `get_range_io_data(fcinfo, rngtypid, func)` (rangetypes.c:319): resolve and
/// cache the element I/O support for one direction.
pub fn get_range_io_data(_rngtypid: Oid, _func: IOFuncSelector) -> PgResult<RangeIOData> {
    todo!("get_range_io_data")
}

/// `range_in(input, typioparam, typmod)` body (rangetypes.c:90).
pub fn range_in<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache: &RangeIOData,
    _input: &str,
    _typmod: i32,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_in")
}

/// `range_out(range)` body (rangetypes.c:139): the canonical text form.
pub fn range_out(_cache: &RangeIOData, _range: RangeTypeP<'_>) -> PgResult<String> {
    todo!("range_out")
}

/// `range_recv(buf, typioparam, typmod)` body (rangetypes.c:179).
pub fn range_recv<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache: &RangeIOData,
    _buf: &[u8],
    _typmod: i32,
) -> PgResult<RangeTypeP<'mcx>> {
    todo!("range_recv")
}

/// `range_send(range)` body (rangetypes.c:263): the binary wire image.
pub fn range_send(_cache: &RangeIOData, _range: RangeTypeP<'_>) -> PgResult<Vec<u8>> {
    todo!("range_send")
}

/// `range_parse(string, &flags, &lbound, &ubound)` (rangetypes.c:2386): split a
/// text literal into its flags byte and bound substrings (`None` = infinite).
pub fn range_parse(_string: &str) -> PgResult<(u8, Option<String>, Option<String>)> {
    todo!("range_parse")
}

/// `range_parse_flags(flags_str)` (rangetypes.c:2311): the `[)`/`(]`/... flags.
pub fn range_parse_flags(_flags_str: &str) -> PgResult<u8> {
    todo!("range_parse_flags")
}

/// `range_parse_bound(string, ptr, &bound, &infinite)` (rangetypes.c:2502):
/// scan one bound substring, returning `(bound_text, infinite, next_offset)`.
pub fn range_parse_bound(_string: &str, _ptr: usize) -> PgResult<(String, bool, usize)> {
    todo!("range_parse_bound")
}

/// `range_deparse(flags, lbound, ubound)` (rangetypes.c:2571): assemble the
/// text literal from a flags byte and the two escaped bound strings.
pub fn range_deparse(_flags: u8, _lbound: Option<&str>, _ubound: Option<&str>) -> PgResult<String> {
    todo!("range_deparse")
}

/// `range_bound_escape(value)` (rangetypes.c:2601): quote/escape one bound
/// value for the text representation.
pub fn range_bound_escape(_value: &str) -> PgResult<String> {
    todo!("range_bound_escape")
}
