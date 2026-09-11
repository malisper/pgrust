//! The live `BackendLogContext` provider: what C's log_line_prefix and the
//! csvlog/jsonlog writers read straight off `MyProcPort`, `MyProc`,
//! `MyBackendType` and friends. pgrust is one process with a thread per
//! session, so every one of those C globals is a thread-local here and the
//! provider is a stateless static that reads them live — installed once per
//! child thread (a retained pool thread keeps it across tasks; each task
//! rewrites the thread-locals it reads) and once for the postmaster.
//!
//! Identity is `MyProcPid` (the value `pg_backend_pid()` returns and
//! `pg_terminate_backend()` accepts), never the OS pid: `%p`, `%c`, `%s` and
//! the pid/session_id columns attribute a line to its session.

use elog::sink::BackendLogContext;
use init_small::globals as g;
use types_core::init::BackendType;
use types_core::INVALID_PROC_NUMBER;

struct LiveBackendLogContext;

static LIVE: LiveBackendLogContext = LiveBackendLogContext;

fn with_port<R>(f: impl FnOnce(&types_startup::Port) -> R) -> Option<R> {
    if !g::HaveMyProcPort() {
        return None;
    }
    Some(g::WithMyProcPort(|p| f(p)))
}

impl BackendLogContext for LiveBackendLogContext {
    fn has_client_port(&self) -> bool {
        g::HaveMyProcPort()
    }

    fn application_name(&self) -> Option<String> {
        // C reads the GUC (`application_name`), not Port->application_name:
        // SET application_name shows up on the next log line.
        if guc_tables::vars::application_name.installed() {
            guc_tables::vars::application_name.read()
        } else {
            with_port(|p| p.application_name.clone()).flatten()
        }
    }

    fn user_name(&self) -> Option<String> {
        with_port(|p| p.user_name.clone()).flatten()
    }

    fn database_name(&self) -> Option<String> {
        with_port(|p| p.database_name.clone()).flatten()
    }

    fn remote_host(&self) -> Option<String> {
        with_port(|p| p.remote_host.clone())
    }

    fn remote_port(&self) -> Option<String> {
        with_port(|p| p.remote_port.clone())
    }

    fn backend_type(&self) -> Option<String> {
        // elog.c get_backend_type_for_log.
        if g::MyProcPid() == g::PostmasterPid() {
            return Some("postmaster".to_owned());
        }
        let bt = miscinit::GetMyBackendType();
        if bt == BackendType::BgWorker {
            if let Some(entry) = bgworker::MyBgworkerEntry() {
                return Some(entry.bgw_type);
            }
        }
        Some(miscinit::GetBackendTypeDesc(bt).to_owned())
    }

    fn process_id(&self) -> u32 {
        elog::sink::current_pid()
    }

    fn lock_group_leader_pid(&self) -> Option<u32> {
        let procno = lmgr_proc::MyProc()?;
        let leader = lmgr_proc::GetPGProcByNumber(procno)
            .lockGroupLeader
            .load(std::sync::atomic::Ordering::Relaxed);
        if leader == INVALID_PROC_NUMBER {
            return None;
        }
        let pid = lmgr_proc::GetPGProcByNumber(leader)
            .pid
            .load(std::sync::atomic::Ordering::Relaxed);
        (pid > 0).then_some(pid as u32)
    }

    fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
        let procno = lmgr_proc::MyProc()?;
        let vxid = &lmgr_proc::GetPGProcByNumber(procno).vxid;
        let proc_number = vxid.procNumber.load(std::sync::atomic::Ordering::Relaxed);
        if proc_number == INVALID_PROC_NUMBER {
            return None;
        }
        Some((
            proc_number,
            vxid.lxid.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }

    fn top_transaction_id(&self) -> u32 {
        xact::GetTopTransactionIdIfAny()
    }

    fn query_id(&self) -> i64 {
        backend_status::pgstat_get_my_query_id()
    }

    fn session_start_time(&self) -> i64 {
        g::MyStartTime()
    }

    fn ps_display(&self) -> Option<String> {
        Some(ps_status::get_ps_display(|b| {
            String::from_utf8_lossy(b).into_owned()
        }))
    }
}

/// Install the live provider on the calling thread (idempotent).
pub fn install() {
    elog::sink::set_backend_log_context(Some(&LIVE));
}
