//! `hstore_skeys` / `hstore_svals` / `hstore_each` (hstore_op.c) — the
//! set-returning functions, run in **materialize mode** via the
//! `::fmgr::mat_srf` sink (the `dispatch_user_setof` path in execSRF), exactly
//! like `pg_stat_statements`'s view function. The C value-per-call SRF protocol
//! is not reachable through the dynamic-library registry, so each row is
//! appended to the sink instead.

use ::fmgr::boundary::RefPayload;
use ::fmgr::mat_srf::{self, MatCell};
use ::fmgr::FunctionCallInfoBaseData;

use crate::{arg_hstore, raise, Datum};

/// Build a header-ful `text` MatCell from payload bytes.
fn text_cell(bytes: &[u8]) -> MatCell {
    let total = bytes.len() + ::datum::varlena::VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(bytes);
    MatCell {
        value: 0,
        ref_payload: Some(RefPayload::Varlena(image)),
        isnull: false,
    }
}

fn null_cell() -> MatCell {
    MatCell {
        value: 0,
        ref_payload: None,
        isnull: true,
    }
}

/// `hstore_skeys(hstore) -> setof text`.
pub fn fc_hstore_skeys(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let keys: Vec<Vec<u8>> = (0..hs.count()).map(|i| hs.key(i).to_vec()).collect();
    mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            sink.materialized = true;
            for k in &keys {
                sink.rows.push(vec![text_cell(k)]);
            }
        } else {
            raise(::types_error::PgError::error(
                "set-valued function called in context that cannot accept a set",
            ));
        }
    });
    Datum::null()
}

/// `hstore_svals(hstore) -> setof text`. A null value yields a NULL row.
pub fn fc_hstore_svals(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let vals: Vec<Option<Vec<u8>>> = (0..hs.count())
        .map(|i| {
            if hs.val_isnull(i) {
                None
            } else {
                Some(hs.val(i).to_vec())
            }
        })
        .collect();
    mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            sink.materialized = true;
            for v in &vals {
                let cell = match v {
                    Some(b) => text_cell(b),
                    None => null_cell(),
                };
                sink.rows.push(vec![cell]);
            }
        } else {
            raise(::types_error::PgError::error(
                "set-valued function called in context that cannot accept a set",
            ));
        }
    });
    Datum::null()
}

/// `hstore_each(IN hs hstore, OUT key text, OUT value text) -> setof record`.
pub fn fc_hstore_each(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..hs.count())
        .map(|i| {
            (
                hs.key(i).to_vec(),
                if hs.val_isnull(i) {
                    None
                } else {
                    Some(hs.val(i).to_vec())
                },
            )
        })
        .collect();
    mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            sink.materialized = true;
            for (k, v) in &rows {
                let key_cell = text_cell(k);
                let val_cell = match v {
                    Some(b) => text_cell(b),
                    None => null_cell(),
                };
                sink.rows.push(vec![key_cell, val_cell]);
            }
        } else {
            raise(::types_error::PgError::error(
                "set-valued function called in context that cannot accept a set",
            ));
        }
    });
    Datum::null()
}
