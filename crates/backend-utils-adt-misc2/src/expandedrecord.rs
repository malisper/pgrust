//! Family `expandedrecord` — `src/backend/utils/adt/expandedrecord.c`.
//!
//! The expanded-record type: an `ExpandedRecordHeader` (an
//! [`crate::expandeddatum`] `ExpandedObjectHeader` subclass) holding a record
//! value in a form cheap to read and modify field-by-field. Depends on the
//! keystone EOH ABI and on the `domains` family (`domain_check`, via the
//! check_domain_for_new_* helpers). Builders, field get/set, and flatten.
//!
//! Construction/field-fetch allocate in the expanded object's own context, so
//! they take `Mcx`; ereport sites surface as `PgResult`. The record value
//! crosses as a `Datum`; the `ExpandedRecordHeader` is an owned struct ported
//! field-for-field from `utils/expandedrecord.h` (to be added to types-datum
//! alongside this family).

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

/// `make_expanded_record_from_typeid(type_id, typmod, parentcontext)`.
pub fn make_expanded_record_from_typeid<'mcx>(
    _mcx: Mcx<'mcx>,
    _type_id: u32,
    _typmod: i32,
) -> PgResult<Datum> {
    todo!("make_expanded_record_from_typeid")
}

/// `make_expanded_record_from_tupdesc(tupdesc, parentcontext)`.
pub fn make_expanded_record_from_tupdesc<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("make_expanded_record_from_tupdesc")
}

/// `make_expanded_record_from_exprecord(olderdatum, parentcontext)`.
pub fn make_expanded_record_from_exprecord<'mcx>(
    _mcx: Mcx<'mcx>,
    _older: Datum,
) -> PgResult<Datum> {
    todo!("make_expanded_record_from_exprecord")
}

/// `make_expanded_record_from_datum(recorddatum, parentcontext)`.
pub fn make_expanded_record_from_datum<'mcx>(
    _mcx: Mcx<'mcx>,
    _record: Datum,
) -> PgResult<Datum> {
    todo!("make_expanded_record_from_datum")
}

/// `expanded_record_fetch_tupdesc(erh)`.
pub fn expanded_record_fetch_tupdesc(_erh: Datum) -> PgResult<()> {
    todo!("expanded_record_fetch_tupdesc")
}

/// `expanded_record_get_tuple(erh)`.
pub fn expanded_record_get_tuple(_erh: Datum) -> PgResult<Datum> {
    todo!("expanded_record_get_tuple")
}

/// `deconstruct_expanded_record(erh)`.
pub fn deconstruct_expanded_record(_erh: Datum) -> PgResult<()> {
    todo!("deconstruct_expanded_record")
}

/// `expanded_record_lookup_field(erh, fieldname, finfo)`.
pub fn expanded_record_lookup_field(_erh: Datum, _fieldname: &str) -> PgResult<bool> {
    todo!("expanded_record_lookup_field")
}

/// `expanded_record_fetch_field(erh, fieldno, isnull)`.
pub fn expanded_record_fetch_field(_erh: Datum, _fieldno: i32) -> PgResult<(Datum, bool)> {
    todo!("expanded_record_fetch_field")
}

/// `expanded_record_set_field_internal(erh, fieldno, newValue, isnull, ...)`.
pub fn expanded_record_set_field_internal(
    _erh: Datum,
    _fieldno: i32,
    _new_value: Datum,
    _isnull: bool,
    _check_constraints: bool,
    _expand_external: bool,
) -> PgResult<()> {
    todo!("expanded_record_set_field_internal")
}

/// `expanded_record_set_fields(erh, values, isnulls, expand_external)`.
pub fn expanded_record_set_fields(
    _erh: Datum,
    _values: &[Datum],
    _isnulls: &[bool],
    _expand_external: bool,
) -> PgResult<()> {
    todo!("expanded_record_set_fields")
}

/// `expanded_record_set_tuple(erh, tuple, copy, expand_external)`.
pub fn expanded_record_set_tuple(
    _erh: Datum,
    _tuple: Datum,
    _copy: bool,
    _expand_external: bool,
) -> PgResult<()> {
    todo!("expanded_record_set_tuple")
}
