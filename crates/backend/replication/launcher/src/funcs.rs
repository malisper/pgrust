use datum::Datum;
use types_core::{InvalidOid, OidIsValid};
use types_error::{PgError, PgResult};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::LogicalRepWorkerType;

pub(crate) fn register_builtins() {
    fmgr_core::register_late_builtins(LAUNCHER_BUILTINS);
}

static LAUNCHER_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 6118,
    name: "pg_stat_get_subscription",
    nargs: 1,
    strict: false,
    retset: true,
    func: fc_pg_stat_get_subscription,
}];

const PG_STAT_GET_SUBSCRIPTION_COLS: usize = 10;

pub fn fc_pg_stat_get_subscription(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_subscription: resolved FmgrInfo required");
    let subid = if fcinfo.argisnull(0) { InvalidOid } else { fcinfo.arg(0).as_oid() };

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for worker in crate::workers_snapshot() {
        if worker.proc_pid == 0 || procarray::BackendPidGetProc(worker.proc_pid).is_none() {
            continue;
        }
        if OidIsValid(subid) && worker.subid != subid {
            continue;
        }
        let worker_pid = worker.proc_pid;

        let mut values = [Datum::from_usize(0); PG_STAT_GET_SUBSCRIPTION_COLS];
        let mut nulls = [false; PG_STAT_GET_SUBSCRIPTION_COLS];

        values[0] = Datum::from_oid(worker.subid);
        if worker.is_tablesync() {
            values[1] = Datum::from_oid(worker.relid);
        } else {
            nulls[1] = true;
        }
        values[2] = Datum::from_i32(worker_pid);
        if worker.is_parallel_apply() {
            values[3] = Datum::from_i32(worker.leader_pid);
        } else {
            nulls[3] = true;
        }
        if worker.last_lsn == 0 {
            nulls[4] = true;
        } else {
            values[4] = Datum::from_u64(worker.last_lsn);
        }
        if worker.last_send_time == 0 {
            nulls[5] = true;
        } else {
            values[5] = Datum::from_i64(worker.last_send_time);
        }
        if worker.last_recv_time == 0 {
            nulls[6] = true;
        } else {
            values[6] = Datum::from_i64(worker.last_recv_time);
        }
        if worker.reply_lsn == 0 {
            nulls[7] = true;
        } else {
            values[7] = Datum::from_u64(worker.reply_lsn);
        }
        if worker.reply_time == 0 {
            nulls[8] = true;
        } else {
            values[8] = Datum::from_i64(worker.reply_time);
        }
        let type_str = match worker.wtype {
            LogicalRepWorkerType::Apply => "apply",
            LogicalRepWorkerType::ParallelApply => "parallel apply",
            LogicalRepWorkerType::TableSync => "table synchronization",
            LogicalRepWorkerType::Unknown => {
                // C: should never happen.
                return Err(Box::new(PgError::error("unknown worker type")));
            }
        };
        values[9] = varlena_result(varlena::cstring_to_text(mcx, type_str.as_bytes())?);

        srf.putvalues(&values, &nulls)?;

        if OidIsValid(subid) {
            break;
        }
    }

    Ok(srf.finish(fcinfo))
}

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::LAUNCHER_BUILTINS);
    }
}
