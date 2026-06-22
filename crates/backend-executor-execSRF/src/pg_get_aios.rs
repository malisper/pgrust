//! Executor-frame registration of the materialize-mode `pg_get_aios()`
//! set-returning function (`storage/aio/aio_funcs.c`).
//!
//! The lock-free per-handle copy/retry render loop lives in
//! `backend-storage-aio-methods` ([`backend_storage_aio_methods::aio_funcs::pg_get_aios_core`]);
//! it emits each rendered [`AioRow`] through the `aio_funcs_seams::tuplestore_putvalues`
//! consumer seam. This module is the executor-frame adapter: it runs
//! `InitMaterializedSRF` to establish `fcinfo->resultinfo`, parks a pointer to
//! the live `ReturnSetInfo` (+ the per-query `Mcx`) in a thread-local for the
//! duration of the core call, and installs the consumer seam to turn each
//! `AioRow` into the 15-column `(values, nulls)` pair and append it with
//! `materialized_srf_putvalues` (the `Int32GetDatum`/`Int64GetDatum`/
//! `BoolGetDatum`/`CStringGetTextDatum` Datum assembly the C does inline).
//!
//! Under `io_method = sync` no AIO handle is ever in flight at query time, so the
//! per-handle loop finds every handle `PGAIO_HS_IDLE` and emits zero rows — which
//! matches real PG's empty `pg_aios` view in a quiescent backend.

extern crate alloc;

use core::cell::Cell;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::ReturnSetInfo;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_storage_aio_funcs_seams::{self as aio_seam, AioRow};
use backend_utils_fmgr_funcapi_seams::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_get_aios()` (OID 6399).
const PG_GET_AIOS: Oid = 6399;

const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;

/// `PG_GET_AIOS_COLS` (aio_funcs.c) — 15 output columns.
const PG_GET_AIOS_COLS: usize = 15;

thread_local! {
    /// Pointers to the live `ReturnSetInfo` + the per-query `Mcx`, parked for the
    /// duration of one `pg_get_aios_core()` call so the installed `AioRow`
    /// consumer seam can append into the live materialized result. Both pointers
    /// are valid only while [`pg_get_aios`] holds them across the synchronous
    /// `pg_get_aios_core()` call (the same single-threaded park-a-borrow idiom the
    /// other row-sink SRFs use); cleared on return.
    static AIO_SINK: Cell<Option<(*mut ReturnSetInfo<'static>, Mcx<'static>)>> =
        const { Cell::new(None) };
}

/// Register the AIO-introspection SRF in the executor-frame SRF table and install
/// the `AioRow` consumer seam that feeds the live materialized result.
pub(crate) fn register_pg_get_aios() {
    aio_seam::tuplestore_putvalues::set(put_aio_row);
    register_srf(PG_GET_AIOS, pg_get_aios);
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// The `aio_funcs_seams::tuplestore_putvalues` consumer: turn one [`AioRow`] into
/// the 15-column `(values, nulls)` pair (the inline Datum assembly aio_funcs.c
/// performs) and append it to the live materialized result.
fn put_aio_row(row: AioRow) -> PgResult<()> {
    AIO_SINK.with(|cell| {
        let (rsinfo_ptr, mcx) = cell
            .get()
            .expect("pg_get_aios: AioRow sink called outside a pg_get_aios dispatch");
        // SAFETY: `pg_get_aios` parks the live `&mut rsinfo` pointer + the
        // per-query `Mcx` across the synchronous core call and clears the cell on
        // return, so the pointer is live and uniquely borrowed here.
        let rsinfo: &mut ReturnSetInfo<'static> = unsafe { &mut *rsinfo_ptr };

        let mut values: [Datum<'static>; PG_GET_AIOS_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_GET_AIOS_COLS];

        // [0] pid — int4; emitted even when 0 (never NULL), per aio_funcs.c.
        match row.pid {
            Some(p) => values[0] = Datum::from_i32(p),
            None => values[0] = Datum::from_i32(0),
        }
        // [1] io_id — int4
        values[1] = Datum::from_i32(row.io_id);
        // [2] io_generation — int8
        values[2] = Datum::from_i64(row.io_generation);
        // [3] state — text
        values[3] = text_datum(mcx, &row.state)?;

        // [4] operation — text; None marks the HANDED_OUT short row (cols 4..=14
        // all NULL).
        match &row.operation {
            Some(op) => values[4] = text_datum(mcx, op)?,
            None => nulls[4] = true,
        }
        // [5] off — int8
        match row.off {
            Some(v) => values[5] = Datum::from_i64(v),
            None => nulls[5] = true,
        }
        // [6] length — int8
        match row.length {
            Some(v) => values[6] = Datum::from_i64(v),
            None => nulls[6] = true,
        }
        // [7] target — text
        match &row.target {
            Some(t) => values[7] = text_datum(mcx, t)?,
            None => nulls[7] = true,
        }
        // [8] handle_data_len — int2
        match row.handle_data_len {
            Some(v) => values[8] = Datum::from_i16(v),
            None => nulls[8] = true,
        }
        // [9] raw_result — int4
        match row.raw_result {
            Some(v) => values[9] = Datum::from_i32(v),
            None => nulls[9] = true,
        }
        // [10] result — text
        match &row.result {
            Some(r) => values[10] = text_datum(mcx, r)?,
            None => nulls[10] = true,
        }
        // [11] target_desc — text
        match &row.target_desc {
            Some(d) => values[11] = text_datum(mcx, d)?,
            None => nulls[11] = true,
        }
        // [12] f_sync — bool
        match row.f_sync {
            Some(b) => values[12] = Datum::from_bool(b),
            None => nulls[12] = true,
        }
        // [13] f_localmem — bool
        match row.f_localmem {
            Some(b) => values[13] = Datum::from_bool(b),
            None => nulls[13] = true,
        }
        // [14] f_buffered — bool
        match row.f_buffered {
            Some(b) => values[14] = Datum::from_bool(b),
            None => nulls[14] = true,
        }

        // Silence the unused-const warnings for the column-type OIDs (kept for
        // documentary parity with the descriptor InitMaterializedSRF builds).
        let _ = (INT2OID, INT4OID, INT8OID);

        materialized_srf_putvalues::call(rsinfo, &values, &nulls)
    })
}

/// `pg_get_aios(PG_FUNCTION_ARGS)` (aio_funcs.c) over the executor frame.
fn pg_get_aios<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_get_aios: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: InitMaterializedSRF(fcinfo, 0);
    InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    // Park the live rsinfo + per-query Mcx for the consumer seam. The pointers
    // outlive nothing past this synchronous call; we clear the cell unconditionally.
    let rsinfo_ptr: *mut ReturnSetInfo<'static> =
        (rsinfo as *mut ReturnSetInfo<'mcx>).cast();
    // SAFETY: re-tag the per-query `Mcx` to `'static` for the thread-local park;
    // it is only read back inside the synchronous core call, where the per-query
    // context is live, and the cell is cleared before this frame returns.
    let mcx_static: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'mcx>, Mcx<'static>>(mcx) };
    AIO_SINK.with(|c| c.set(Some((rsinfo_ptr, mcx_static))));

    let res = backend_storage_aio_methods::aio_funcs::pg_get_aios_core();

    AIO_SINK.with(|c| c.set(None));
    res?;

    // C: return (Datum) 0;
    Ok(Datum::null())
}
