//! `pg_listening_channels()` (OID 3035) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `async.c`'s `pg_listening_channels` is a value-per-call SRF emitting one
//! `text` row per channel the current backend is LISTENing on (the
//! `listenChannels` list). The channel-name collection core (the `listenChannels`
//! walk) is ported in [`commands_async::pg_listening_channels_rows`],
//! which hands back a `Vec<String>` of channel names in order.
//!
//! Here that core is driven over the executor frame in materialize mode: the
//! channel list is collected once and the whole tuplestore filled with one `text`
//! row each. `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the
//! executor's already-resolved one-column `text` descriptor; the names are
//! appended via `materialized_srf_putvalues`. Registered from
//! [`register_pg_listening_channels`] (called by `init_seams`); it bypasses the
//! by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_listening_channels()` (OID 3035).
const PG_LISTENING_CHANNELS: Oid = 3035;

/// Register `pg_listening_channels` in the executor-frame SRF table.
pub(crate) fn register_pg_listening_channels() {
    register_srf(PG_LISTENING_CHANNELS, pg_listening_channels);
}

/// `pg_listening_channels(PG_FUNCTION_ARGS)` (async.c:788) over the executor
/// frame.
fn pg_listening_channels<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_listening_channels: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: the per-call series walks `listenChannels`; the owned core snapshots the
    // channel names in order.
    let channels = commands_async::pg_listening_channels_rows();

    // C: SRF returns one `text` (`CStringGetTextDatum(channel)`) per channel; take
    // the executor's already-resolved one-column `text` descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_listening_channels: InitMaterializedSRF establishes fcinfo->resultinfo");

    for channel in &channels {
        let values = [varlena_seams::cstring_to_text_v::call(mcx, channel)?];
        let nulls = [false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
