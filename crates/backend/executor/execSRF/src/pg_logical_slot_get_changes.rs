//! `pg_logical_slot_{get,peek}[_binary]_changes` (OIDs 3782 get / 3783
//! get_binary / 3784 peek / 3785 peek_binary, per `pg_proc.dat`) registered as
//! executor-frame materialize-mode set-returning functions — the SQL interface
//! to logical decoding (`logicalfuncs.c`).
//!
//! These are the fmgr SRF entry points for the four change-stream functions. The
//! decode core itself
//! ([`logicalfuncs::run_changes_into_collector`]) is
//! the faithful port of `pg_logical_slot_get_changes_guts`' inner `PG_TRY` block
//! (`CreateDecodingContext(SqlSrf)` + the `XLogReadRecord` /
//! `LogicalDecodingProcessRecord` loop bounded by `upto_lsn` / `upto_nchanges` +
//! the optional `LogicalConfirmReceivedLocation`); it lives in the `logicalfuncs`
//! crate because the `OutputWriter::SqlSrf` write callback (`LogicalOutputWrite`)
//! and its backend-local row collector live there.
//!
//! Here the fmgr-frame framing the C `pg_logical_slot_get_changes_guts` wraps
//! around that core is ported: `CheckSlotPermissions` +
//! `CheckLogicalDecodingRequirements`, the `name` / `upto_lsn` / `upto_nchanges`
//! / `text[]` options-array argument reads (`PG_GETARG_NAME` / `PG_GETARG_LSN`
//! `PG_GETARG_INT32` / `PG_GETARG_ARRAYTYPE_P` + `deconstruct_array_builtin` into
//! `(optname, opt)` pairs), `InitMaterializedSRF`, `ReplicationSlotAcquire`,
//! `WaitForStandbyConfirmation`, the call into `run_changes_into_collector`,
//! `ReplicationSlotRelease`, and finally draining the collected
//! `(lsn, xid, data)` rows ([`sql_srf_take_rows`]) into the result tuplestore via
//! `materialized_srf_putvalues`.
//!
//! The four functions differ only in the `(confirm, binary)` pair, matching the
//! C `pg_logical_slot_get_changes` / `peek_changes` / `get_binary_changes` /
//! `peek_binary_changes` thin wrappers. `confirm == true` (the `get_*` family)
//! advances the slot's `confirmed_flush`; `peek_*` leaves it untouched.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use logical_seams as logical_seams;
use replication_slot as slot;

use crate::register_srf;

/// `pg_logical_slot_get_changes(name, pg_lsn, int4, variadic text[])` (OID 3782).
const PG_LOGICAL_SLOT_GET_CHANGES: Oid = 3782;
/// `pg_logical_slot_get_binary_changes(name, pg_lsn, int4, variadic text[])`
/// (OID 3783).
const PG_LOGICAL_SLOT_GET_BINARY_CHANGES: Oid = 3783;
/// `pg_logical_slot_peek_changes(name, pg_lsn, int4, variadic text[])` (OID 3784).
const PG_LOGICAL_SLOT_PEEK_CHANGES: Oid = 3784;
/// `pg_logical_slot_peek_binary_changes(name, pg_lsn, int4, variadic text[])`
/// (OID 3785).
const PG_LOGICAL_SLOT_PEEK_BINARY_CHANGES: Oid = 3785;

/// Register the four logical-slot change-stream SRFs in the executor-frame SRF
/// table. They bypass the by-OID scalar builtin registry whose tag-only
/// `resultinfo` cannot carry the live `ReturnSetInfo` (the same dual-home as the
/// other materialize-mode SRFs).
pub(crate) fn register_pg_logical_slot_get_changes() {
    register_srf(PG_LOGICAL_SLOT_GET_CHANGES, pg_logical_slot_get_changes);
    register_srf(PG_LOGICAL_SLOT_PEEK_CHANGES, pg_logical_slot_peek_changes);
    register_srf(
        PG_LOGICAL_SLOT_GET_BINARY_CHANGES,
        pg_logical_slot_get_binary_changes,
    );
    register_srf(
        PG_LOGICAL_SLOT_PEEK_BINARY_CHANGES,
        pg_logical_slot_peek_binary_changes,
    );
}

/// `pg_logical_slot_get_changes` (logicalfuncs.c:330) — textual, consuming.
fn pg_logical_slot_get_changes<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    pg_logical_slot_get_changes_guts(fcinfo, true, false)
}

/// `pg_logical_slot_peek_changes` (logicalfuncs.c:339) — textual, peeking.
fn pg_logical_slot_peek_changes<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    pg_logical_slot_get_changes_guts(fcinfo, false, false)
}

/// `pg_logical_slot_get_binary_changes` (logicalfuncs.c:348) — binary, consuming.
fn pg_logical_slot_get_binary_changes<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    pg_logical_slot_get_changes_guts(fcinfo, true, true)
}

/// `pg_logical_slot_peek_binary_changes` (logicalfuncs.c:357) — binary, peeking.
fn pg_logical_slot_peek_binary_changes<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    pg_logical_slot_get_changes_guts(fcinfo, false, true)
}

/// `PG_GETARG_NAME(0)` → `NameStr(*name)`: the `name` argument's
/// NUL-trimmed UTF-8 view off the by-reference lane (C passes the whole
/// `NameData` by pointer). Arg 0 is `proisstrict`-protected upstream, but the C
/// also explicitly `PG_ARGISNULL(0)`-checks it, so mirror that here.
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData) -> PgResult<&'a str> {
    if fcinfo.args.first().is_none_or(|a| a.isnull) {
        return Err(PgError::error("slot name must not be null")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }
    let bytes = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("pg_logical_slot_get_changes: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end])
        .map_err(|_| PgError::error("pg_logical_slot_get_changes: name arg not valid UTF-8"))
}

/// The shared `pg_logical_slot_get_changes_guts` (logicalfuncs.c:98) over the
/// executor frame. `confirm` advances the slot's `confirmed_flush`; `binary`
/// selects binary vs textual output.
fn pg_logical_slot_get_changes_guts<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    confirm: bool,
    binary: bool,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_logical_slot_get_changes: fn_mcxt set by ExecMakeTableFunctionResult");

    let my_database_id = init_small::globals::MyDatabaseId();

    // CheckSlotPermissions();
    let user_id = backendstate_pc_seams::get_user_id::call()?;
    slot::CheckSlotPermissions(mcx, user_id)?;

    // CheckLogicalDecodingRequirements();
    let wal_level = types_logical::WalLevel(transam_xlog_seams::wal_level::call() as i32);
    logical_logical::CheckLogicalDecodingRequirements(wal_level, my_database_id)?;

    // name = PG_GETARG_NAME(0);
    let name = arg_name(fcinfo)?.to_string();

    // upto_lsn = PG_ARGISNULL(1) ? InvalidXLogRecPtr : PG_GETARG_LSN(1);
    let upto_lsn = match fcinfo.args.get(1) {
        Some(a) if !a.isnull => a.value.as_u64(),
        _ => 0,
    };

    // upto_nchanges = PG_ARGISNULL(2) ? InvalidXLogRecPtr : PG_GETARG_INT32(2);
    let upto_nchanges = match fcinfo.args.get(2) {
        Some(a) if !a.isnull => a.value.as_i32(),
        _ => 0,
    };

    // if (PG_ARGISNULL(3)) ereport(ERROR, "options array must not be null");
    // arr = PG_GETARG_ARRAYTYPE_P(3);
    if fcinfo.args.get(3).is_none_or(|a| a.isnull) {
        return Err(PgError::error("options array must not be null")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }

    // Deconstruct options array into (optname, opt) -> DefElem pairs. The
    // helper enforces "array must not contain nulls" (a NULL element errors)
    // and flattens whatever dimensionality the array image carries; the C
    // additionally rejects ndim > 1 and an odd element count.
    let options: Vec<(String, Option<String>)> = {
        let image: &[u8] = fcinfo
            .ref_arg(3)
            .and_then(|a| a.as_varlena())
            .expect("pg_logical_slot_get_changes: text[] options arg missing from by-ref lane");
        let strs =
            arrayfuncs::construct::text_array_to_strings_bytes(mcx, image)?;
        // if (nelems % 2 != 0) ereport(ERROR, "array must have even number of elements");
        if strs.len() % 2 != 0 {
            return Err(PgError::error("array must have even number of elements")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        let mut opts = Vec::with_capacity(strs.len() / 2);
        let mut i = 0;
        while i < strs.len() {
            // makeDefElem(optname, makeString(opt), -1).
            opts.push((strs[i].as_str().to_string(), Some(strs[i + 1].as_str().to_string())));
            i += 2;
        }
        opts
    };

    // InitMaterializedSRF(fcinfo, 0); p->tupstore = rsinfo->setResult; ...
    // The executor's already-resolved (lsn pg_lsn, xid xid, data text) row type
    // is taken via MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    // ReplicationSlotAcquire(NameStr(*name), true, true);
    slot::ReplicationSlotAcquire(&name, true, true)?;

    // PG_TRY { ... decode loop ... } PG_CATCH { InvalidateSystemCaches(); RE_THROW }
    // run_changes_into_collector mirrors the inner block. We must also release
    // the slot afterwards (C: ReplicationSlotRelease() in the TRY body) and on
    // error, exactly like the C PG_CATCH/PG_RE_THROW path.
    let decode_result: PgResult<()> = (|| {
        // Wait for sync standbys to confirm receipt (no-op unless this is a
        // failover slot with synchronized_standby_slots set). C uses
        // wait_for_wal_lsn = upto_lsn ? Min(upto_lsn, end_of_wal) : end_of_wal;
        // WaitForStandbyConfirmation handles the no-config fast path itself.
        let end_of_wal = if !transam_xlog_seams::recovery_in_progress::call() {
            transam_xlog_seams::get_flush_rec_ptr::call().0
        } else {
            transam_xlog_seams::get_xlog_replay_rec_ptr::call()
        };
        let wait_for_wal_lsn = if upto_lsn == 0 {
            end_of_wal
        } else {
            core::cmp::min(upto_lsn, end_of_wal)
        };
        slot::WaitForStandbyConfirmation(wait_for_wal_lsn)?;

        logicalfuncs::run_changes_into_collector(
            upto_lsn,
            upto_nchanges,
            options,
            confirm,
            binary,
            my_database_id,
        )
    })();

    // ReplicationSlotRelease(); — released whether the decode succeeded or
    // failed (the C TRY body releases on success; PG_CATCH unwinds the resource
    // owner which releases the acquired slot).
    let release_result = slot::ReplicationSlotRelease();

    if let Err(e) = decode_result {
        // run_changes_into_collector already ran InvalidateSystemCaches +
        // sql_srf_clear_rows in its PG_CATCH; propagate the original error.
        let _ = release_result;
        return Err(e);
    }
    release_result?;

    // Drain the collected (lsn, xid, data) rows into the result tuplestore.
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_logical_slot_get_changes: InitMaterializedSRF establishes resultinfo");
    for (lsn, xid, data) in logical_seams::sql_srf_take_rows() {
        // values[0] = LSNGetDatum(lsn); values[1] = TransactionIdGetDatum(xid);
        // values[2] = cstring_to_text_with_len(...) / the bytea image.
        let data_datum = varlena_seams::bytes_to_varlena_v::call(mcx, &data)?;
        let values = [
            Datum::from_u64(lsn),
            Datum::from_transaction_id(xid),
            data_datum,
        ];
        let nulls = [false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
