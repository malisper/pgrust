//! `pg_stat_get_activity(int4)` (OID 2022) — the `pg_stat_activity` view's
//! backing materialize-mode SRF (pgstatfuncs.c:331).
//!
//! Projects the per-backend `localBackendStatusTable` snapshot into 31 columns,
//! one row per backend (or only the row matching the `pid` argument when it is
//! non-NULL). The role-gated columns (state/query/wait-event/client-addr/ssl/
//! gss/backend-type/query-id/leader-pid) are emitted only when the caller has
//! `HAS_PGSTAT_PERMISSIONS` over the row's `st_userid`; otherwise the activity
//! column shows `<insufficient privilege>` and the gated columns are NULL.
//!
//! The substrate is the `backend-utils-activity-status` local snapshot
//! (`pgstat_fetch_stat_numbackends` / `pgstat_get_local_beentry_by_index` /
//! `pgstat_clip_activity`), the wait-event decoders
//! (`pgstat_get_wait_event{,_type}` over `proc->wait_event_info`), and the
//! `BackendPidGetProc` / `AuxiliaryPidGetProc` proc lookups projecting
//! `wait_event_info` + `lockGroupLeader->pid`.
//!
//! GSS status (`st_gssstatus`) is not snapshotted in the backend-status port and
//! `st_gss` is always reported `false`, so only the no-GSS arm is reachable;
//! that arm is transcribed faithfully here.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::init::BackendType;
use ::types_core::primitive::TimestampTz;
use ::types_core::{Oid, OidIsValid};
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use status as status;
use ::status::{
    BackendState, LocalBackendStatusFields, LocalPgBackendStatus, STATE_DISABLED, STATE_FASTPATH,
    STATE_IDLE, STATE_IDLEINTRANSACTION, STATE_IDLEINTRANSACTION_ABORTED, STATE_RUNNING,
    STATE_STARTING, STATE_UNDEFINED,
};
use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_activity(int4)` (OID 2022).
const PG_STAT_GET_ACTIVITY: Oid = 2022;

/// `#define PG_STAT_GET_ACTIVITY_COLS 31`.
const PG_STAT_GET_ACTIVITY_COLS: usize = 31;

/// `ROLE_PG_READ_ALL_STATS` (pg_authid.dat) — predefined role OID 3375.
const ROLE_PG_READ_ALL_STATS: Oid = 3375;

const INVALID_PID: i32 = 0;

/// Register `pg_stat_get_activity` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_activity() {
    register_srf(PG_STAT_GET_ACTIVITY, pg_stat_get_activity);
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `HAS_PGSTAT_PERMISSIONS(role)` (pgstatfuncs.c:37):
/// `has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) ||
///  has_privs_of_role(GetUserId(), role)`.
fn has_pgstat_permissions(role: Oid) -> PgResult<bool> {
    let user = miscinit::GetUserId();
    if acl_seams::has_privs_of_role::call(user, ROLE_PG_READ_ALL_STATS)? {
        return Ok(true);
    }
    acl_seams::has_privs_of_role::call(user, role)
}

/// `TransactionIdIsValid(xid)`.
#[inline]
fn xid_is_valid(xid: ::types_core::TransactionId) -> bool {
    xid != ::types_core::InvalidTransactionId
}

/// Decode a NUL-terminated owned activity/name buffer (no trailing NUL kept) as
/// UTF-8 up to the first NUL.
fn buf_str(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..end]).unwrap_or("")
}

/// `pg_stat_get_activity(PG_FUNCTION_ARGS)` (pgstatfuncs.c:331) over the executor
/// frame.
fn pg_stat_get_activity<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_activity: fn_mcxt set by ExecMakeTableFunctionResult");

    // int num_backends = pgstat_fetch_stat_numbackends();
    let num_backends = status::pgstat_fetch_stat_numbackends();

    // int pid = PG_ARGISNULL(0) ? -1 : PG_GETARG_INT32(0);
    let pid: i32 = match fcinfo.args.first() {
        Some(nd) if !nd.isnull => nd.value.as_i32(),
        _ => -1,
    };

    // InitMaterializedSRF(fcinfo, 0). Take the executor's resolved 31-col desc.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let mut rows: Vec<(
        [Datum<'mcx>; PG_STAT_GET_ACTIVITY_COLS],
        [bool; PG_STAT_GET_ACTIVITY_COLS],
    )> = Vec::new();

    // for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
    for curr_backend in 1..=num_backends {
        let mut values: [Datum<'mcx>; PG_STAT_GET_ACTIVITY_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_STAT_GET_ACTIVITY_COLS];

        // local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
        // beentry = &local_beentry->backendStatus;
        let local_beentry: LocalPgBackendStatus =
            match status::pgstat_get_local_beentry_by_index(curr_backend) {
                Some(l) => l,
                None => continue,
            };
        let beentry: &LocalBackendStatusFields = &local_beentry.backend_status;

        // if (pid != -1 && beentry->st_procpid != pid) continue;
        if pid != -1 && beentry.st_procpid != pid {
            continue;
        }

        // ---- Values available to all callers ----

        // values[0] = st_databaseid (or NULL).
        if OidIsValid(beentry.st_databaseid) {
            values[0] = Datum::from_oid(beentry.st_databaseid);
        } else {
            nulls[0] = true;
        }

        // values[1] = st_procpid.
        values[1] = Datum::from_i32(beentry.st_procpid);

        // values[2] = st_userid (or NULL).
        if OidIsValid(beentry.st_userid) {
            values[2] = Datum::from_oid(beentry.st_userid);
        } else {
            nulls[2] = true;
        }

        // values[3] = st_appname (or NULL).
        let appname = buf_str(&beentry.st_appname);
        if !beentry.st_appname.is_empty() {
            values[3] = text_datum(mcx, appname)?;
        } else {
            nulls[3] = true;
        }

        // values[15] = backend_xid (or NULL).
        if xid_is_valid(local_beentry.backend_xid) {
            values[15] = Datum::from_u32(local_beentry.backend_xid);
        } else {
            nulls[15] = true;
        }

        // values[16] = backend_xmin (or NULL).
        if xid_is_valid(local_beentry.backend_xmin) {
            values[16] = Datum::from_u32(local_beentry.backend_xmin);
        } else {
            nulls[16] = true;
        }

        // ---- Values only available to role member or pg_read_all_stats ----
        if has_pgstat_permissions(beentry.st_userid)? {
            // values[4] = state string (or NULL for STATE_UNDEFINED).
            match beentry.st_state {
                STATE_STARTING => values[4] = text_datum(mcx, "starting")?,
                STATE_IDLE => values[4] = text_datum(mcx, "idle")?,
                STATE_RUNNING => values[4] = text_datum(mcx, "active")?,
                STATE_IDLEINTRANSACTION => values[4] = text_datum(mcx, "idle in transaction")?,
                STATE_FASTPATH => values[4] = text_datum(mcx, "fastpath function call")?,
                STATE_IDLEINTRANSACTION_ABORTED => {
                    values[4] = text_datum(mcx, "idle in transaction (aborted)")?
                }
                STATE_DISABLED => values[4] = text_datum(mcx, "disabled")?,
                STATE_UNDEFINED => nulls[4] = true,
                #[allow(unreachable_patterns)]
                BackendState::Undefined => nulls[4] = true,
            }

            // clipped_activity = pgstat_clip_activity(beentry->st_activity_raw);
            // values[5] = clipped_activity;
            let clipped = status::pgstat_clip_activity(&beentry.st_activity_raw);
            values[5] = text_datum(mcx, buf_str(&clipped))?;

            // leader_pid; nulls[29] = true;
            nulls[29] = true;

            // proc = BackendPidGetProc(st_procpid); if NULL and !B_BACKEND,
            //   proc = AuxiliaryPidGetProc(st_procpid).
            let mut proc_no: Option<::types_core::ProcNumber> =
                procarray_seams::backend_pid_get_proc_role::call(
                    beentry.st_procpid,
                )
                .map(|(_role, procno)| procno);
            if proc_no.is_none() && beentry.st_backend_type != BackendType::Backend {
                proc_no = lmgr_proc_seams::auxiliary_pid_get_proc::call(
                    beentry.st_procpid,
                );
            }

            let mut wait_event_type: Option<&'static str> = None;
            let mut wait_event: Option<alloc::borrow::Cow<'static, str>> = None;

            if let Some(procno) = proc_no {
                // raw_wait_event = UINT32_ACCESS_ONCE(proc->wait_event_info);
                let raw_wait_event =
                    lmgr_proc_seams::proc_wait_event_info::call(procno);
                wait_event_type =
                    waitevent::pgstat_get_wait_event_type(raw_wait_event);
                wait_event =
                    waitevent::pgstat_get_wait_event(raw_wait_event)?;

                // leader = proc->lockGroupLeader;
                let leader = lmgr_proc_seams::proc_lock_group_leader::call(procno);
                let leader_pid = if leader != ::types_core::INVALID_PROC_NUMBER {
                    lmgr_proc_seams::proc_pid::call(leader)
                } else {
                    INVALID_PID
                };

                // Show the leader only for active parallel workers.
                if leader != ::types_core::INVALID_PROC_NUMBER && leader_pid != beentry.st_procpid {
                    values[29] = Datum::from_i32(leader_pid);
                    nulls[29] = false;
                } else if beentry.st_backend_type == BackendType::BgWorker {
                    let lpid = launcher_seams::GetLeaderApplyWorkerPid::call(
                        beentry.st_procpid,
                    )?;
                    if lpid != INVALID_PID {
                        values[29] = Datum::from_i32(lpid);
                        nulls[29] = false;
                    }
                }
            }

            // values[6] = wait_event_type (or NULL).
            match wait_event_type {
                Some(t) => values[6] = text_datum(mcx, t)?,
                None => nulls[6] = true,
            }

            // values[7] = wait_event (or NULL).
            match &wait_event {
                Some(e) => values[7] = text_datum(mcx, e)?,
                None => nulls[7] = true,
            }

            // values[8] = xact_start (NULL for walsenders / zero).
            if beentry.st_xact_start_timestamp != 0
                && beentry.st_backend_type != BackendType::WalSender
            {
                values[8] = Datum::from_i64(beentry.st_xact_start_timestamp);
            } else {
                nulls[8] = true;
            }

            // values[9] = activity_start (or NULL).
            set_ts(&mut values, &mut nulls, 9, beentry.st_activity_start_timestamp);
            // values[10] = proc_start (or NULL).
            set_ts(&mut values, &mut nulls, 10, beentry.st_proc_start_timestamp);
            // values[11] = state_start (or NULL).
            set_ts(&mut values, &mut nulls, 11, beentry.st_state_start_timestamp);

            // Client address (12=client_addr, 13=client_hostname, 14=client_port).
            fill_client_addr(mcx, &mut values, &mut nulls, beentry)?;

            // values[17] = backend type.
            if beentry.st_backend_type == BackendType::BgWorker {
                match postmaster_bgworker::GetBackgroundWorkerTypeByPid(beentry.st_procpid) {
                    Some(bgw_type) => values[17] = text_datum(mcx, &bgw_type)?,
                    None => nulls[17] = true,
                }
            } else {
                values[17] = text_datum(
                    mcx,
                    miscinit::GetBackendTypeDesc(beentry.st_backend_type),
                )?;
            }

            // SSL information (18..24).
            if beentry.st_ssl {
                let ssl = beentry
                    .st_sslstatus
                    .as_ref()
                    .expect("pg_stat_get_activity: st_ssl set without st_sslstatus");
                values[18] = Datum::from_bool(true);
                values[19] = text_datum(mcx, buf_str(&ssl.ssl_version))?;
                values[20] = text_datum(mcx, buf_str(&ssl.ssl_cipher))?;
                values[21] = Datum::from_i32(ssl.ssl_bits);

                if ssl.ssl_client_dn[0] != 0 {
                    values[22] = text_datum(mcx, buf_str(&ssl.ssl_client_dn))?;
                } else {
                    nulls[22] = true;
                }

                if ssl.ssl_client_serial[0] != 0 {
                    // DirectFunctionCall3(numeric_in, serial, InvalidOid, -1).
                    let serial = buf_str(&ssl.ssl_client_serial);
                    values[23] = numeric_in_datum(mcx, serial)?;
                } else {
                    nulls[23] = true;
                }

                if ssl.ssl_issuer_dn[0] != 0 {
                    values[24] = text_datum(mcx, buf_str(&ssl.ssl_issuer_dn))?;
                } else {
                    nulls[24] = true;
                }
            } else {
                values[18] = Datum::from_bool(false);
                nulls[19] = true;
                nulls[20] = true;
                nulls[21] = true;
                nulls[22] = true;
                nulls[23] = true;
                nulls[24] = true;
            }

            // GSS information (25..28). st_gss is always false in this port
            // (st_gssstatus is not snapshotted), so only the no-GSS arm runs.
            values[25] = Datum::from_bool(false); // gss_auth
            nulls[26] = true; // No GSS principal
            values[27] = Datum::from_bool(false); // GSS Encryption not in use
            values[28] = Datum::from_bool(false); // GSS credentials not delegated

            // values[30] = query_id (or NULL when zero).
            if beentry.st_query_id == 0 {
                nulls[30] = true;
            } else {
                values[30] = Datum::from_i64(beentry.st_query_id);
            }
        } else {
            // No permissions to view data about this session.
            values[5] = text_datum(mcx, "<insufficient privilege>")?;
            for &c in &[
                4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                29, 30,
            ] {
                nulls[c] = true;
            }
        }

        rows.push((values, nulls));

        // If only a single backend was requested, and we found it, break.
        if pid != -1 {
            break;
        }
    }

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_stat_get_activity: InitMaterializedSRF establishes fcinfo->resultinfo");
    for (values, nulls) in &rows {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `if (ts != 0) values[i] = TimestampTzGetDatum(ts); else nulls[i] = true;`.
fn set_ts(
    values: &mut [Datum],
    nulls: &mut [bool],
    i: usize,
    ts: TimestampTz,
) {
    if ts != 0 {
        values[i] = Datum::from_i64(ts);
    } else {
        nulls[i] = true;
    }
}

/// `DirectFunctionCall3(numeric_in, CStringGetDatum(s), InvalidOid, -1)` — parse
/// a decimal string into a `numeric` `Datum` (the SSL serial). Faithful to the
/// numeric_in path: `set_var_from_str` then `make_result`.
fn numeric_in_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let (var, _endptr) = adt_numeric::io::set_var_from_str(mcx, s, 0)?;
    let buf = adt_numeric::convert::make_result(mcx, &var)?;
    Ok(Datum::ByRef(buf))
}

/// `DirectFunctionCall1(inet_in, CStringGetDatum(host))` — parse a numeric host
/// string into an `inet` `Datum`. The `inet_in` core (network.c:121) builds the
/// canonical `inet_struct`; the fmgr boundary's `ret_inet` wraps its 18-byte
/// `to_datum_bytes()` image behind a 4-byte varlena header, landing as a
/// `Datum::ByRef`.
fn inet_in_datum<'mcx>(mcx: Mcx<'mcx>, host: &str) -> PgResult<Datum<'mcx>> {
    use ::datum::varlena::VARHDRSZ;
    // inet_in(host, NULL escontext) — a numeric host always parses, so the soft
    // path (None) is never taken; a parse failure is a hard ereport.
    let addr = adt_network::inet_in(host.as_bytes(), None)?
        .expect("pg_stat_get_activity: inet_in of numeric host returned None");
    let payload = addr.to_datum_bytes();
    let total = payload.len() + VARHDRSZ;
    let mut img: Vec<u8> = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &img)?))
}

/// Fill the client-address columns (12=client_addr inet, 13=client_hostname
/// text, 14=client_port int4) from `beentry->st_clientaddr`
/// (pgstatfuncs.c:515-577).
fn fill_client_addr<'mcx>(
    mcx: Mcx<'mcx>,
    values: &mut [Datum<'mcx>; PG_STAT_GET_ACTIVITY_COLS],
    nulls: &mut [bool; PG_STAT_GET_ACTIVITY_COLS],
    beentry: &LocalBackendStatusFields,
) -> PgResult<()> {
    // A zeroed client addr means we don't know.
    if ip::sockaddr_is_all_zeros(&beentry.st_clientaddr) {
        nulls[12] = true;
        nulls[13] = true;
        nulls[14] = true;
        return Ok(());
    }

    let family = ip::sockaddr_family(&beentry.st_clientaddr);

    if family == libc::AF_INET || family == libc::AF_INET6 {
        // remote_host[0] = '\0'; remote_port[0] = '\0';
        // ret = pg_getnameinfo_all(&st_clientaddr.addr, salen, remote_host, ...,
        //                          remote_port, ..., NI_NUMERICHOST|NI_NUMERICSERV);
        let mut remote_host = String::new();
        let mut remote_port = String::new();
        let ret = ip::pg_getnameinfo_all(
            &beentry.st_clientaddr,
            Some(&mut remote_host),
            Some(&mut remote_port),
            libc::NI_NUMERICHOST | libc::NI_NUMERICSERV,
        );
        if ret == 0 {
            // clean_ipv6_addr(family, remote_host);
            let mut host_bytes = remote_host.into_bytes();
            adt_network::clean_ipv6_addr(family, &mut host_bytes);
            let host = core::str::from_utf8(&host_bytes).unwrap_or("");

            // values[12] = inet_in(remote_host).
            values[12] = inet_in_datum(mcx, host)?;

            // values[13] = st_clienthostname (or NULL).
            if !beentry.st_clienthostname.is_empty() && beentry.st_clienthostname[0] != 0 {
                values[13] = text_datum(mcx, buf_str(&beentry.st_clienthostname))?;
            } else {
                nulls[13] = true;
            }

            // values[14] = atoi(remote_port).
            let port: i32 = remote_port.trim().parse().unwrap_or(0);
            values[14] = Datum::from_i32(port);
        } else {
            nulls[12] = true;
            nulls[13] = true;
            nulls[14] = true;
        }
    } else if family == libc::AF_UNIX {
        // Unix sockets: NULL host/hostname, -1 port.
        nulls[12] = true;
        nulls[13] = true;
        values[14] = Datum::from_i32(-1);
    } else {
        // Unknown address type, should never happen.
        nulls[12] = true;
        nulls[13] = true;
        nulls[14] = true;
    }
    Ok(())
}
