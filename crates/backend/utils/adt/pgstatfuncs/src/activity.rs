use ::datum::Datum;
use ::types_core::{BackendType, Oid, ProcNumber, INVALID_PROC_NUMBER};
use ::types_error::PgResult;
use ::types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use backend_status_seams::BackendState;

const PG_STAT_GET_ACTIVITY_COLS: usize = 31;
const ROLE_PG_READ_ALL_STATS: Oid = 3375;

fn text_datum(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), s.as_bytes())?))
}

// C: sockaddr_storage ss_family; on both linux and macos targets the libc
// sockaddr_storage layout starts with (len,) family.
fn ss_family(addr: &[u8]) -> i32 {
    let mut ss: libc::sockaddr_storage = unsafe { core::mem::zeroed() };
    let n = core::mem::size_of::<libc::sockaddr_storage>().min(addr.len());
    unsafe {
        core::ptr::copy_nonoverlapping(addr.as_ptr(), (&mut ss as *mut _) as *mut u8, n);
    }
    ss.ss_family as i32
}

fn aux_pid_get_proc(pid: i32) -> Option<&'static types_storage::storage::PGPROC> {
    let procs = &lmgr_proc::ProcGlobal().allProcs;
    procs
        .iter()
        .find(|p| p.pid.load(core::sync::atomic::Ordering::Relaxed) == pid && pid != 0)
}

pub fn fc_pg_stat_get_activity(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_activity: resolved FmgrInfo required");
    let num_backends = backend_status::pgstat_fetch_stat_numbackends();
    let pid_filter = if fcinfo.argisnull(0) { -1 } else { fcinfo.arg(0).as_i32() };

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, PG_STAT_GET_ACTIVITY_COLS);

    let uid = miscinit::GetUserId();

    for curr in 1..=num_backends {
        let Some(local) = backend_status::pgstat_get_local_beentry_by_index(curr) else {
            continue;
        };
        let be = &local;
        if pid_filter != -1 && be.st_procpid != pid_filter {
            continue;
        }

        let mut values = [Datum::from_usize(0); PG_STAT_GET_ACTIVITY_COLS];
        let mut nulls = [false; PG_STAT_GET_ACTIVITY_COLS];

        if be.st_databaseid != 0 {
            values[0] = Datum::from_oid(be.st_databaseid);
        } else {
            nulls[0] = true;
        }
        values[1] = Datum::from_i32(be.st_procpid);
        if be.st_userid != 0 {
            values[2] = Datum::from_oid(be.st_userid);
        } else {
            nulls[2] = true;
        }
        if !be.st_appname.is_empty() {
            values[3] = text_datum(fcinfo, &be.st_appname)?;
        } else {
            nulls[3] = true;
        }
        if local.backend_xid != 0 {
            values[15] = Datum::from_oid(local.backend_xid);
        } else {
            nulls[15] = true;
        }
        if local.backend_xmin != 0 {
            values[16] = Datum::from_oid(local.backend_xmin);
        } else {
            nulls[16] = true;
        }

        let has_perm = acl_seams::has_privs_of_role::call(uid, ROLE_PG_READ_ALL_STATS)?
            || acl_seams::has_privs_of_role::call(uid, be.st_userid)?;
        if has_perm {
            match be.st_state {
                BackendState::STATE_STARTING => values[4] = text_datum(fcinfo, "starting")?,
                BackendState::STATE_IDLE => values[4] = text_datum(fcinfo, "idle")?,
                BackendState::STATE_RUNNING => values[4] = text_datum(fcinfo, "active")?,
                BackendState::STATE_IDLEINTRANSACTION => {
                    values[4] = text_datum(fcinfo, "idle in transaction")?
                }
                BackendState::STATE_FASTPATH => {
                    values[4] = text_datum(fcinfo, "fastpath function call")?
                }
                BackendState::STATE_IDLEINTRANSACTION_ABORTED => {
                    values[4] = text_datum(fcinfo, "idle in transaction (aborted)")?
                }
                BackendState::STATE_DISABLED => values[4] = text_datum(fcinfo, "disabled")?,
                BackendState::STATE_UNDEFINED => nulls[4] = true,
            }

            let clipped = backend_status::pgstat_clip_activity(be.st_activity_raw.as_bytes());
            values[5] = text_datum(fcinfo, &clipped)?;

            nulls[29] = true;

            let mut proc = procarray::BackendPidGetProc(be.st_procpid);
            if proc.is_none() && be.st_backendType != BackendType::Backend {
                proc = aux_pid_get_proc(be.st_procpid);
            }

            let mut wait_event_type = None;
            let mut wait_event = None;
            if let Some(proc) = proc {
                let raw = proc.wait_event_info.load(core::sync::atomic::Ordering::Relaxed);
                wait_event_type = waitevent::pgstat_get_wait_event_type(raw);
                wait_event = waitevent::pgstat_get_wait_event(raw);

                let leader: ProcNumber =
                    proc.lockGroupLeader.load(core::sync::atomic::Ordering::Relaxed);
                if leader != INVALID_PROC_NUMBER {
                    let leader_pid = lmgr_proc::GetPGProcByNumber(leader)
                        .pid
                        .load(core::sync::atomic::Ordering::Relaxed);
                    if leader_pid != be.st_procpid {
                        values[29] = Datum::from_i32(leader_pid);
                        nulls[29] = false;
                    }
                } else if be.st_backendType == BackendType::BgWorker {
                    panic!(
                        "pg_stat_get_activity (pgstatfuncs.c): GetLeaderApplyWorkerPid \
                         unported — logical-replication lane"
                    );
                }
            }

            match wait_event_type {
                Some(t) => values[6] = text_datum(fcinfo, t)?,
                None => nulls[6] = true,
            }
            match wait_event {
                Some(e) => values[7] = text_datum(fcinfo, e)?,
                None => nulls[7] = true,
            }

            // C hides walsender xact time (not kept up to date there).
            if be.st_xact_start_timestamp != 0 && be.st_backendType != BackendType::WalSender {
                values[8] = Datum::from_i64(be.st_xact_start_timestamp);
            } else {
                nulls[8] = true;
            }
            if be.st_activity_start_timestamp != 0 {
                values[9] = Datum::from_i64(be.st_activity_start_timestamp);
            } else {
                nulls[9] = true;
            }
            if be.st_proc_start_timestamp != 0 {
                values[10] = Datum::from_i64(be.st_proc_start_timestamp);
            } else {
                nulls[10] = true;
            }
            if be.st_state_start_timestamp != 0 {
                values[11] = Datum::from_i64(be.st_state_start_timestamp);
            } else {
                nulls[11] = true;
            }

            if be.st_clientaddr.addr.iter().all(|&b| b == 0) {
                nulls[12] = true;
                nulls[13] = true;
                nulls[14] = true;
            } else {
                match ss_family(&be.st_clientaddr.addr) {
                    f if f == libc::AF_INET as i32 || f == libc::AF_INET6 as i32 => panic!(
                        "pg_stat_get_activity (pgstatfuncs.c): inet client_addr \
                         datum unported — adt network lane"
                    ),
                    f if f == libc::AF_UNIX as i32 => {
                        nulls[12] = true;
                        nulls[13] = true;
                        values[14] = Datum::from_i32(-1);
                    }
                    _ => {
                        nulls[12] = true;
                        nulls[13] = true;
                        nulls[14] = true;
                    }
                }
            }

            if be.st_backendType == BackendType::BgWorker {
                panic!(
                    "pg_stat_get_activity (pgstatfuncs.c): GetBackgroundWorkerTypeByPid \
                     unported — bgworker lane"
                );
            }
            values[17] = text_datum(fcinfo, miscinit::GetBackendTypeDesc(be.st_backendType))?;

            if be.st_ssl {
                panic!(
                    "pg_stat_get_activity (pgstatfuncs.c): PgBackendSSLStatus \
                     unported — be-secure-openssl lane"
                );
            }
            values[18] = Datum::from_bool(false);
            nulls[19] = true;
            nulls[20] = true;
            nulls[21] = true;
            nulls[22] = true;
            nulls[23] = true;
            nulls[24] = true;

            if be.st_gss {
                panic!(
                    "pg_stat_get_activity (pgstatfuncs.c): PgBackendGSSStatus \
                     unported — gssapi lane"
                );
            }
            values[25] = Datum::from_bool(false);
            nulls[26] = true;
            values[27] = Datum::from_bool(false);
            values[28] = Datum::from_bool(false);

            if be.st_query_id == 0 {
                nulls[30] = true;
            } else {
                values[30] = Datum::from_i64(be.st_query_id);
            }
        } else {
            values[5] = text_datum(fcinfo, "<insufficient privilege>")?;
            for i in [
                4usize, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                27, 28, 29, 30,
            ] {
                nulls[i] = true;
            }
        }

        srf.putvalues(&values, &nulls)?;

        if pid_filter != -1 {
            break;
        }
    }

    Ok(srf.finish(fcinfo))
}
