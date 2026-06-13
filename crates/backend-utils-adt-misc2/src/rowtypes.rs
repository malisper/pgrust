//! Family `rowtypes` — `src/backend/utils/adt/rowtypes.c`.
//!
//! Composite-type (RECORD / named row) I/O and operators: `record_in` /
//! `record_out` / `record_recv` / `record_send`, the comparison engine
//! (`record_cmp` + `record_eq` and the bt/lt/le/gt/ge/ne wrappers,
//! `record_larger` / `record_smaller`), the byte-image comparison family
//! (`record_image_cmp` / `record_image_eq` + wrappers, `btrecordimagecmp`),
//! and `hash_record` / `hash_record_extended`.
//!
//! These deconstruct/construct composite Datums and call per-column type I/O
//! and comparison functions, so they take `Mcx` and surface `ereport`s as
//! `PgResult`. Values cross as `Datum`. Independent of the keystone (does not
//! touch expanded records).

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

/// `record_in(string, typioparam, typmod)`.
pub fn record_in<'mcx>(
    _mcx: Mcx<'mcx>,
    _string: Option<&str>,
    _typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum> {
    todo!("record_in")
}

/// `record_out(record)`.
pub fn record_out<'mcx>(_mcx: Mcx<'mcx>, _record: Datum) -> PgResult<Datum> {
    todo!("record_out")
}

/// `record_recv(buf, typioparam, typmod)`.
pub fn record_recv<'mcx>(
    _mcx: Mcx<'mcx>,
    _buf: &[u8],
    _typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum> {
    todo!("record_recv")
}

/// `record_send(record)`.
pub fn record_send<'mcx>(_mcx: Mcx<'mcx>, _record: Datum) -> PgResult<Datum> {
    todo!("record_send")
}

/// `record_cmp(fcinfo)` — the three-way comparison engine shared by the
/// btree/equality wrappers.
pub fn record_cmp<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<i32> {
    todo!("record_cmp")
}

/// `record_eq(record1, record2)`.
pub fn record_eq<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<bool> {
    todo!("record_eq")
}

/// `record_larger(record1, record2)` / `record_smaller(...)`.
pub fn record_larger<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<Datum> {
    todo!("record_larger")
}

/// `record_smaller(record1, record2)`.
pub fn record_smaller<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<Datum> {
    todo!("record_smaller")
}

/// `record_image_cmp(fcinfo)` — byte-image three-way comparison engine.
pub fn record_image_cmp<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<i32> {
    todo!("record_image_cmp")
}

/// `record_image_eq(record1, record2)`.
pub fn record_image_eq<'mcx>(_mcx: Mcx<'mcx>, _left: Datum, _right: Datum) -> PgResult<bool> {
    todo!("record_image_eq")
}

/// `hash_record(record)`.
pub fn hash_record<'mcx>(_mcx: Mcx<'mcx>, _record: Datum) -> PgResult<u32> {
    todo!("hash_record")
}

/// `hash_record_extended(record, seed)`.
pub fn hash_record_extended<'mcx>(
    _mcx: Mcx<'mcx>,
    _record: Datum,
    _seed: u64,
) -> PgResult<u64> {
    todo!("hash_record_extended")
}
