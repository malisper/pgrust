use ::datum::Datum;
use ::types_core::{BackendType, Oid, ProcNumber, INVALID_PROC_NUMBER};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use backend_status_seams::BackendState;

const PG_STAT_GET_ACTIVITY_COLS: usize = 31;
pub(crate) const ROLE_PG_READ_ALL_STATS: Oid = 3375;

pub(crate) fn text_datum(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), s.as_bytes())?))
}

pub(crate) fn has_pgstat_permissions(userid: Oid) -> PgResult<bool> {
    let uid = miscinit::GetUserId();
    Ok(acl_seams::has_privs_of_role::call(uid, ROLE_PG_READ_ALL_STATS)?
        || acl_seams::has_privs_of_role::call(uid, userid)?)
}

// C: sockaddr_storage ss_family; on both linux and macos targets the libc
// sockaddr_storage layout starts with (len,) family.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn ss_family(addr: &[u8]) -> i32 {
    let mut ss: libc::sockaddr_storage = unsafe { core::mem::zeroed() };
    let n = core::mem::size_of::<libc::sockaddr_storage>().min(addr.len());
    unsafe {
        core::ptr::copy_nonoverlapping(addr.as_ptr(), (&mut ss as *mut _) as *mut u8, n);
    }
    ss.ss_family as i32
}

// wasm32: no sockaddr_storage in the wasi libc crate; musl's layout puts
// sa_family_t (u16) at offset 0 (same shape as ip::sockaddr_family's arm).
// Sessions are socketless on WASI, so the stored address is always zeroed.
#[cfg(target_family = "wasm")]
pub(crate) fn ss_family(addr: &[u8]) -> i32 {
    if addr.len() < 2 {
        return 0;
    }
    u16::from_ne_bytes([addr[0], addr[1]]) as i32
}

// wasm32: the wasi libc crate exposes no AF_*; musl values (ip::sys shape).
#[cfg(not(target_family = "wasm"))]
pub(crate) use libc::{AF_INET, AF_INET6, AF_UNIX};
#[cfg(target_family = "wasm")]
pub(crate) const AF_INET: i32 = 2;
#[cfg(target_family = "wasm")]
pub(crate) const AF_INET6: i32 = 10;
#[cfg(target_family = "wasm")]
pub(crate) const AF_UNIX: i32 = 1;

pub(crate) fn aux_pid_get_proc(pid: i32) -> Option<&'static types_storage::storage::PGPROC> {
    let procs = &lmgr_proc::ProcGlobal().allProcs;
    procs
        .iter()
        .find(|p| p.pid.load(core::sync::atomic::Ordering::Relaxed) == pid && pid != 0)
}

// C pgstatfuncs.c:528-541 / 947-956 / 992-997 shared shape: numeric host and
// service strings for the stored client sockaddr via pg_getnameinfo_all
// (NI_NUMERICHOST | NI_NUMERICSERV), the host cleaned of any IPv6 '%zone'
// (network.c clean_ipv6_addr, exported by builtins.h for exactly these
// callers). None mirrors C's ret != 0 arm (columns go NULL).
pub(crate) fn client_addr_port(sa: &::ip::SockAddr) -> Option<(String, String)> {
    let mut remote_host = String::new();
    let mut remote_port = String::new();
    let ret = ::ip::pg_getnameinfo_all(
        sa,
        Some(&mut remote_host),
        Some(&mut remote_port),
        ::ip::sys::NI_NUMERICHOST | ::ip::sys::NI_NUMERICSERV,
    );
    if ret != 0 {
        return None;
    }
    adt_network::builtins::clean_ipv6_addr(ss_family(&sa.addr), &mut remote_host);
    Some((remote_host, remote_port))
}

// C pgstatfuncs.c:542-543 / 958-959: DirectFunctionCall1(inet_in,
// CStringGetDatum(remote_host)) — a numeric-host string always parses.
pub(crate) fn inet_datum(fcinfo: &Fcinfo, host: &str) -> PgResult<Datum> {
    let v = adt_network::network_in(host, false, None)?.expect("hard-error path returns Err");
    let (img, len) = v.image();
    types_fmgr::byref_result(fcinfo.result_mcx(), &img[..len])
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

        let has_perm = has_pgstat_permissions(be.st_userid)?;
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
                    // InvalidPid (-1) unless this is a parallel apply worker.
                    let leader_pid = launcher_seams::get_leader_apply_worker_pid::call(be.st_procpid);
                    if leader_pid != -1 {
                        values[29] = Datum::from_i32(leader_pid);
                        nulls[29] = false;
                    }
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

            // C pgstatfuncs.c:515-577: "A zeroed client addr means we don't
            // know" — no-socket (background) backends leave all three NULL.
            if ::ip::sockaddr_is_all_zeros(&be.st_clientaddr) {
                nulls[12] = true;
                nulls[13] = true;
                nulls[14] = true;
            } else {
                match ss_family(&be.st_clientaddr.addr) {
                    // C:525-556 — inet datum of the numeric remote host,
                    // st_clienthostname only when log_hostname captured one,
                    // client_port = atoi(remote_port).
                    f if f == AF_INET as i32 || f == AF_INET6 as i32 => {
                        match client_addr_port(&be.st_clientaddr) {
                            Some((remote_host, remote_port)) => {
                                values[12] = inet_datum(fcinfo, &remote_host)?;
                                if !be.st_clienthostname.is_empty() {
                                    values[13] = text_datum(fcinfo, &be.st_clienthostname)?;
                                } else {
                                    nulls[13] = true;
                                }
                                // C:549 atoi() — NI_NUMERICSERV is all digits.
                                values[14] =
                                    Datum::from_i32(remote_port.parse().unwrap_or(0));
                            }
                            None => {
                                nulls[12] = true;
                                nulls[13] = true;
                                nulls[14] = true;
                            }
                        }
                    }
                    // C:558-569 — unix sockets report NULL host and -1 port,
                    // distinguishable from no-permission / error rows.
                    f if f == AF_UNIX as i32 => {
                        nulls[12] = true;
                        nulls[13] = true;
                        values[14] = Datum::from_i32(-1);
                    }
                    // C:570-576 — unknown address type, should never happen.
                    _ => {
                        nulls[12] = true;
                        nulls[13] = true;
                        nulls[14] = true;
                    }
                }
            }

            if be.st_backendType == BackendType::BgWorker {
                match bgworker_seams::get_background_worker_type_by_pid::call(be.st_procpid) {
                    Some(bgw_type) => values[17] = text_datum(fcinfo, &bgw_type)?,
                    None => nulls[17] = true,
                }
            } else {
                values[17] =
                    text_datum(fcinfo, miscinit::GetBackendTypeDesc(be.st_backendType))?;
            }

            if be.st_ssl {
                values[18] = Datum::from_bool(true);
                values[19] = text_datum(fcinfo, &be.st_ssl_version)?;
                values[20] = text_datum(fcinfo, &be.st_ssl_cipher)?;
                values[21] = Datum::from_i32(be.st_ssl_bits);
                if !be.st_ssl_client_dn.is_empty() {
                    values[22] = text_datum(fcinfo, &be.st_ssl_client_dn)?;
                } else {
                    nulls[22] = true;
                }
                if !be.st_ssl_client_serial.is_empty() {
                    let img = adt_numeric::numeric_in(&be.st_ssl_client_serial, -1, None)?
                        .expect("hard-error path returns Err");
                    values[23] = types_fmgr::byref_result(fcinfo.result_mcx(), img.as_bytes())?;
                } else {
                    nulls[23] = true;
                }
                if !be.st_ssl_issuer_dn.is_empty() {
                    values[24] = text_datum(fcinfo, &be.st_ssl_issuer_dn)?;
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

            if be.st_gss {
                // unported: PgBackendGSSStatus — gssapi lane. Defensive
                // only: every bestart site sets st_gss = false (no-GSS
                // build), so this arm is unreachable today.
                return Err(Box::new(
                    PgError::error(
                        "pg_stat_get_activity over a GSSAPI-authenticated backend \
                         is not yet implemented",
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
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

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    fn sockaddr_in4(ip: [u8; 4], port: u16) -> ::ip::SockAddr {
        // SAFETY: zeroed sockaddr_in is a valid all-defaults value.
        let mut sin: libc::sockaddr_in = unsafe { core::mem::zeroed() };
        sin.sin_family = libc::AF_INET as libc::sa_family_t;
        sin.sin_port = port.to_be();
        // s_addr is network byte order: the array is already big-endian.
        sin.sin_addr.s_addr = u32::from_ne_bytes(ip);
        let mut sa = ::ip::SockAddr::zeroed();
        let n = core::mem::size_of::<libc::sockaddr_in>();
        // SAFETY: n <= sizeof(sockaddr_storage) == sa.addr.len().
        unsafe {
            core::ptr::copy_nonoverlapping(
                core::ptr::from_ref(&sin).cast::<u8>(),
                sa.addr.as_mut_ptr(),
                n,
            );
        }
        sa.salen = n as u32;
        sa
    }

    fn sockaddr_in6(ip: [u8; 16], port: u16, scope_id: u32) -> ::ip::SockAddr {
        // SAFETY: zeroed sockaddr_in6 is a valid all-defaults value.
        let mut sin6: libc::sockaddr_in6 = unsafe { core::mem::zeroed() };
        sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
        sin6.sin6_port = port.to_be();
        sin6.sin6_addr.s6_addr = ip;
        sin6.sin6_scope_id = scope_id;
        let mut sa = ::ip::SockAddr::zeroed();
        let n = core::mem::size_of::<libc::sockaddr_in6>();
        // SAFETY: n <= sizeof(sockaddr_storage) == sa.addr.len().
        unsafe {
            core::ptr::copy_nonoverlapping(
                core::ptr::from_ref(&sin6).cast::<u8>(),
                sa.addr.as_mut_ptr(),
                n,
            );
        }
        sa.salen = n as u32;
        sa
    }

    #[test]
    fn client_addr_port_renders_ipv4() {
        let (host, port) = client_addr_port(&sockaddr_in4([192, 0, 2, 5], 45678)).unwrap();
        assert_eq!(host, "192.0.2.5");
        assert_eq!(port, "45678");
        assert_eq!(port.parse::<i32>().unwrap(), 45678);
    }

    #[test]
    fn client_addr_port_renders_ipv6_loopback() {
        let mut ip6 = [0u8; 16];
        ip6[15] = 1;
        let (host, port) = client_addr_port(&sockaddr_in6(ip6, 40001, 0)).unwrap();
        assert_eq!(host, "::1");
        assert_eq!(port, "40001");
    }

    #[test]
    fn client_addr_port_strips_the_ipv6_zone() {
        // fe80::1 with a scope id: getnameinfo appends %<zone>; the
        // clean_ipv6_addr pass (C pgstatfuncs.c:541) truncates it.
        let mut ip6 = [0u8; 16];
        ip6[0] = 0xfe;
        ip6[1] = 0x80;
        ip6[15] = 1;
        let (host, port) = client_addr_port(&sockaddr_in6(ip6, 40002, 1)).unwrap();
        assert_eq!(host, "fe80::1");
        assert_eq!(port, "40002");
    }

    #[test]
    fn inet_datum_input_parses_for_numeric_hosts() {
        // The DirectFunctionCall1(inet_in, ...) leg: the numeric-host strings
        // client_addr_port produces are always valid inet input.
        for host in ["192.0.2.5", "::1", "fe80::1", "::ffff:192.0.2.5"] {
            assert!(adt_network::network_in(host, false, None).unwrap().is_some());
        }
    }
}
