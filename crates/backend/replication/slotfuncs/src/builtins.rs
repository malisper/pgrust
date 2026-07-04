use ::datum::Datum;
use ::types_core::InvalidOid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use ::types_tuple::NameData;

use slot::{RS_INVAL_HORIZON, RS_INVAL_NONE, RS_INVAL_WAL_LEVEL, RS_TEMPORARY};

use crate::{get_wal_availability, WALAvailability, PG_GET_REPLICATION_SLOTS_COLS};

fn arg_name(fcinfo: &Fcinfo, i: usize) -> String {
    // SAFETY: catalog arg i of these strict fns is a non-null name.
    let nd = NameData { data: *unsafe { fcinfo.arg_name(i) } };
    String::from_utf8_lossy(nd.name_str()).into_owned()
}

fn text_datum(mcx: mcx::Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn not_row_type() -> Box<PgError> {
    Box::new(PgError::error("return type must be a row type"))
}

pub fn fc_pg_create_physical_replication_slot(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_create_physical_replication_slot: resolved FmgrInfo required");
    let name = arg_name(fcinfo, 0);
    let immediately_reserve = fcinfo.arg_bool(1);
    let temporary = fcinfo.arg_bool(2);

    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(not_row_type());
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");

    slot::CheckSlotPermissions()?;
    slot::CheckSlotRequirements()?;

    crate::create_physical_replication_slot(&name, immediately_reserve, temporary, 0)?;

    let s = slot::MyReplicationSlot().unwrap();
    let d = s.data.get();

    let mut values = [Datum::from_usize(0); 2];
    let mut nulls = [false; 2];
    values[0] = byref_result(mcx, &d.name.data)?;
    if immediately_reserve {
        values[1] = Datum::from_u64(d.restart_lsn);
    } else {
        nulls[1] = true;
    }

    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let result = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)

    slot::ReplicationSlotRelease()?;
    Ok(result)
}

pub fn fc_pg_drop_replication_slot(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = arg_name(fcinfo, 0);

    slot::CheckSlotPermissions()?;
    slot::CheckSlotRequirements()?;

    slot::ReplicationSlotDrop(&name, true)?;
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_get_replication_slots(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_replication_slots: resolved FmgrInfo required");

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, PG_GET_REPLICATION_SLOTS_COLS);

    let currlsn = crate::get_xlog_write_rec_ptr();

    lwlock::LWLockAcquire(
        lwlock::main_lock(types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK),
        lwlock::LW_SHARED,
        init_small::globals::MyProcNumber(),
    )?;
    let scan: PgResult<()> = (|| {
        for s in slot::ReplicationSlotCtl() {
            if !s.in_use.get() {
                continue;
            }

            let (mut data, active_pid, inactive_since) =
                s.with_mutex(|| (s.data.get(), s.active_pid.get(), s.inactive_since.get()));

            let mut values = [Datum::from_usize(0); PG_GET_REPLICATION_SLOTS_COLS];
            let mut nulls = [false; PG_GET_REPLICATION_SLOTS_COLS];
            let mut i = 0;

            values[i] = byref_result(mcx, &data.name.data)?;
            i += 1;

            if data.database == InvalidOid {
                nulls[i] = true;
            } else {
                values[i] = byref_result(mcx, &data.plugin.data)?;
            }
            i += 1;

            values[i] = text_datum(mcx, if data.database == InvalidOid { "physical" } else { "logical" })?;
            i += 1;

            if data.database == InvalidOid {
                nulls[i] = true;
            } else {
                values[i] = Datum::from_oid(data.database);
            }
            i += 1;

            values[i] = Datum::from_bool(data.persistency == RS_TEMPORARY);
            i += 1;
            values[i] = Datum::from_bool(active_pid != 0);
            i += 1;

            if active_pid != 0 {
                values[i] = Datum::from_i32(active_pid);
            } else {
                nulls[i] = true;
            }
            i += 1;

            if data.xmin != 0 {
                values[i] = Datum::from_transaction_id(data.xmin);
            } else {
                nulls[i] = true;
            }
            i += 1;

            if data.catalog_xmin != 0 {
                values[i] = Datum::from_transaction_id(data.catalog_xmin);
            } else {
                nulls[i] = true;
            }
            i += 1;

            if data.restart_lsn != 0 {
                values[i] = Datum::from_u64(data.restart_lsn);
            } else {
                nulls[i] = true;
            }
            i += 1;

            if data.confirmed_flush != 0 {
                values[i] = Datum::from_u64(data.confirmed_flush);
            } else {
                nulls[i] = true;
            }
            i += 1;

            let mut walstate = if data.invalidated != RS_INVAL_NONE {
                WALAvailability::Removed
            } else {
                get_wal_availability(data.restart_lsn)
            };

            match walstate {
                WALAvailability::InvalidLsn => nulls[i] = true,
                WALAvailability::Reserved => values[i] = text_datum(mcx, "reserved")?,
                WALAvailability::Extended => values[i] = text_datum(mcx, "extended")?,
                WALAvailability::Unreserved => values[i] = text_datum(mcx, "unreserved")?,
                WALAvailability::Removed => {
                    let mut lost = true;
                    if data.restart_lsn != 0 {
                        let (pid, restart_lsn) =
                            s.with_mutex(|| (s.active_pid.get(), s.data.get().restart_lsn));
                        data.restart_lsn = restart_lsn;
                        if pid != 0 {
                            values[i] = text_datum(mcx, "unreserved")?;
                            walstate = WALAvailability::Unreserved;
                            lost = false;
                        }
                    }
                    if lost {
                        values[i] = text_datum(mcx, "lost")?;
                    }
                }
            }
            i += 1;

            let max_slot_wal_keep_size_mb = guc_tables::vars::max_slot_wal_keep_size_mb.read();
            if walstate == WALAvailability::Removed || max_slot_wal_keep_size_mb < 0 {
                nulls[i] = true;
            } else {
                let segsize = transam_xlog::wal_segment_size();
                let target_seg = transam_xlog::XLByteToSeg(data.restart_lsn, segsize);
                let slot_keep_segs = crate::convert_to_xsegs(max_slot_wal_keep_size_mb, segsize);
                let keep_segs =
                    crate::convert_to_xsegs(guc_tables::vars::wal_keep_size_mb.read(), segsize);
                let fail_seg = target_seg + slot_keep_segs.max(keep_segs) + 1;
                let fail_lsn = fail_seg * segsize as u64;
                values[i] = Datum::from_i64(fail_lsn.wrapping_sub(currlsn) as i64);
            }
            i += 1;

            values[i] = Datum::from_bool(data.two_phase);
            i += 1;

            if data.two_phase && data.two_phase_at != 0 {
                values[i] = Datum::from_u64(data.two_phase_at);
            } else {
                nulls[i] = true;
            }
            i += 1;

            if inactive_since > 0 {
                values[i] = Datum::from_i64(inactive_since);
            } else {
                nulls[i] = true;
            }
            i += 1;

            let cause = data.invalidated;

            if data.database == InvalidOid {
                nulls[i] = true;
            } else {
                values[i] =
                    Datum::from_bool(cause == RS_INVAL_HORIZON || cause == RS_INVAL_WAL_LEVEL);
            }
            i += 1;

            if cause == RS_INVAL_NONE {
                nulls[i] = true;
            } else {
                values[i] = text_datum(mcx, slot::GetSlotInvalidationCauseName(cause))?;
            }
            i += 1;

            values[i] = Datum::from_bool(data.failover);
            i += 1;

            values[i] = Datum::from_bool(data.synced != 0);
            i += 1;

            debug_assert_eq!(i, PG_GET_REPLICATION_SLOTS_COLS);
            srf.putvalues(&values, &nulls)?;
        }
        Ok(())
    })();
    lwlock::LWLockRelease(lwlock::main_lock(
        types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK,
    ))?;
    scan?;

    Ok(srf.finish(fcinfo))
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SLOTFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(3779, "pg_create_physical_replication_slot", 3, fc_pg_create_physical_replication_slot),
    b(3780, "pg_drop_replication_slot", 1, fc_pg_drop_replication_slot),
    FmgrBuiltin {
        foid: 3781,
        name: "pg_get_replication_slots",
        nargs: 0,
        strict: false,
        retset: true,
        func: fc_pg_get_replication_slots,
    },
];
