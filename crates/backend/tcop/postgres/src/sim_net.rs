//! PostgresSimNetMain (pgrust extension, `--cfg pgrust_sim` builds ONLY):
//! the P4 sim-net session harness — ONE deterministic pgwire session served
//! over the in-memory sim-net transport pair (pqcomm_simnet), driven by an
//! in-process scripted client at the provider's deterministic block points.
//!
//! The boot ladder + session half are stdio_wire's inner fn, VERBATIM (it is
//! transport-blind; the provider was installed by
//! seams_init::init_all_with_transport(Transport::SimNet) at process start).
//! What this file adds is sim-harness plumbing, all of it cfg(pgrust_sim):
//!
//! 1. SimVfs namespace seeding: under pgrust_sim the whole binary statically
//!    dispatches vfs to SimVfs (P1 §1.2), whose namespace starts EMPTY — the
//!    known COMPOSE FINDING 1 boot wall. The harness mirrors the host
//!    datadir (argv -D) into the SimVfs universe KEYED RELATIVE to the
//!    datadir root (the ladder chdir()s into -D and the port addresses
//!    datadir files relatively; SimVfs resolves relative paths against "/"),
//!    plus PGRUST_SIMNET_SEED_DIRS (colon-separated host dirs, e.g. the
//!    timezone share) keyed at their ABSOLUTE paths. Raw-fs boot pieces
//!    (conf read, lockfile) hit the REAL datadir — same image, both planes.
//! 2. The scripted wire client (PGRUST_SIMNET_SQL: one simple-query
//!    statement per line): StartupMessage -> per ReadyForQuery send the next
//!    Query -> Terminate -> Finished (client write side closed).
//! 3. Artifact dump at session exit: the full server->client wire byte
//!    stream (PGRUST_SIMNET_TRANSCRIPT) and the op-sequence-numbered SimNet
//!    op log (PGRUST_SIMNET_OPLOG). The determinism gate byte-compares both
//!    across two runs of the same script (pid pinned via
//!    init_small::globals::process_id sim arm; cancel key via the seeded P2
//!    RNG; clock via the frozen SimClock).
//!
//! The std::fs/env reads below are sim-harness domain (cfg'd out of product
//! builds; the determinism lint censuses production code only).

use ::types_error::PgResult;

const PROGNAME: &str = "postgres";

// ---------------------------------------------------------------------------
// SimVfs seeding (host image -> this thread's sim universe).
// ---------------------------------------------------------------------------

fn cpath(p: &str) -> std::ffi::CString {
    std::ffi::CString::new(p).expect("no NUL in seed paths")
}

fn sim_mkdir_p(path: &str) {
    let mut acc = String::new();
    for comp in path.split('/').filter(|c| !c.is_empty() && *c != ".") {
        if !acc.is_empty() || path.starts_with('/') {
            acc.push('/');
        }
        // Relative seeds keep their first component bare; SimVfs resolves
        // both shapes against "/".
        if acc.is_empty() && !path.starts_with('/') {
            acc = comp.to_string();
        } else {
            acc.push_str(comp);
        }
        let _ = vfs::mkdir(&cpath(&acc), 0o700);
    }
}

fn sim_write_file(sim_path: &str, bytes: &[u8]) {
    let c = cpath(sim_path);
    let fd = vfs::open(&c, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY, 0o600);
    assert!(fd >= 0, "simvfs seed open failed for {sim_path} (errno {})", vfs::get_errno());
    let mut off = 0usize;
    while off < bytes.len() {
        let n = vfs::pwrite(fd, &bytes[off..], off as libc::off_t);
        assert!(n > 0, "simvfs seed pwrite failed for {sim_path}");
        off += n as usize;
    }
    vfs::close(fd);
}

/// Mirror `host_dir` (recursively) into the SimVfs universe at `sim_prefix`
/// ("" = keys relative to the mirrored root). Deterministic order (sorted
/// dirents). Symlinks are followed (initdb trees are link-free; tz shares
/// are copied link-resolved by the e2e, matching the wasm-boot cp -RL law).
fn mirror_into_simvfs(host_dir: &std::path::Path, sim_prefix: &str) {
    if !sim_prefix.is_empty() {
        sim_mkdir_p(sim_prefix);
    }
    let mut entries: Vec<_> = std::fs::read_dir(host_dir)
        .unwrap_or_else(|e| panic!("seed read_dir {host_dir:?}: {e}"))
        .map(|r| r.expect("seed dirent"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for ent in entries {
        let name = ent.file_name();
        let name = name.to_str().expect("utf8 seed names");
        // Host-lifecycle files that must not shadow the live session's raw
        // plane (the lockfile is created/owned by THIS boot on the real fs).
        if name == "postmaster.pid" || name == "postmaster.opts" {
            continue;
        }
        let sim_path = if sim_prefix.is_empty() {
            name.to_string()
        } else {
            format!("{sim_prefix}/{name}")
        };
        let ft = ent.file_type().expect("seed file_type");
        let hp = ent.path();
        if ft.is_dir() {
            sim_mkdir_p(&sim_path);
            mirror_into_simvfs(&hp, &sim_path);
        } else {
            let bytes = std::fs::read(&hp).unwrap_or_else(|e| panic!("seed read {hp:?}: {e}"));
            sim_write_file(&sim_path, &bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// The scripted in-process wire client (the pump).
// ---------------------------------------------------------------------------

fn be_i32(v: i32) -> [u8; 4] {
    v.to_be_bytes()
}

fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&be_i32(196608)); // protocol 3.0
    for (k, v) in [("user", user), ("database", database)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = Vec::with_capacity(4 + body.len());
    msg.extend_from_slice(&be_i32(4 + body.len() as i32));
    msg.extend_from_slice(&body);
    msg
}

fn query_message(sql: &str) -> Vec<u8> {
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&be_i32(4 + sql.len() as i32 + 1));
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    msg
}

fn terminate_message() -> Vec<u8> {
    let mut msg = vec![b'X'];
    msg.extend_from_slice(&be_i32(4));
    msg
}

struct SimWireClient {
    stmts: std::collections::VecDeque<String>,
    started: bool,
    terminated: bool,
    /// Everything received, for frame parsing (the provider separately
    /// accumulates the canonical transcript).
    rx: Vec<u8>,
    /// Frame-parse cursor into rx.
    cursor: usize,
    /// Complete ReadyForQuery frames observed.
    zseen: usize,
    /// Queries sent.
    sent: usize,
}

impl SimWireClient {
    fn new(stmts: Vec<String>) -> Self {
        SimWireClient {
            stmts: stmts.into(),
            started: false,
            terminated: false,
            rx: Vec::new(),
            cursor: 0,
            zseen: 0,
            sent: 0,
        }
    }

    fn scan_frames(&mut self) {
        while self.cursor + 5 <= self.rx.len() {
            let ty = self.rx[self.cursor];
            let len = i32::from_be_bytes(
                self.rx[self.cursor + 1..self.cursor + 5].try_into().expect("4 bytes"),
            ) as usize;
            if self.cursor + 1 + len > self.rx.len() {
                break; // incomplete frame
            }
            if ty == b'Z' {
                self.zseen += 1;
            }
            self.cursor += 1 + len;
        }
    }

    /// One pump step at a server block point. Deterministic: state is this
    /// struct + the pair's buffers, nothing ambient.
    fn pump(&mut self) -> pqcomm_simnet::PumpStatus {
        if !self.started {
            self.started = true;
            pqcomm_simnet::client_send(&startup_message("postgres", "postgres"));
            return pqcomm_simnet::PumpStatus::Progress;
        }
        self.rx.extend_from_slice(&pqcomm_simnet::client_recv_all());
        self.scan_frames();
        if self.terminated {
            // Nothing more will ever be sent; the provider maps this to a
            // clean EOF on the server's next read.
            return pqcomm_simnet::PumpStatus::Finished;
        }
        if self.zseen > self.sent {
            match self.stmts.pop_front() {
                Some(sql) => {
                    pqcomm_simnet::client_send(&query_message(&sql));
                    self.sent += 1;
                }
                None => {
                    pqcomm_simnet::client_send(&terminate_message());
                    self.terminated = true;
                }
            }
        }
        // Progress claims byte movement. Since inc-2 the provider's stall
        // fingerprint EXCLUDES op consults (review observation 2): if this
        // step neither received nor sent a byte (nor closed), the pair is
        // protocol-stalled and the provider panics deterministically at the
        // block point — the charter behavior, not an e2e-watchdog timeout.
        // Every healthy block point moves bytes (the server flushes before
        // parking; a new flush always carries a frame we drain here).
        pqcomm_simnet::PumpStatus::Progress
    }
}

// ---------------------------------------------------------------------------
// The mode entry.
// ---------------------------------------------------------------------------

/// Seed THIS thread's SimVfs universe from the host datadir image.
/// Two addressing conventions coexist in the port: post-chdir RELATIVE
/// paths (md.c-style "base/…", "pg_wal/…") and DataDir-joined ABSOLUTE
/// paths (controldata's "<datadir>/global/pg_control"). Seed the image
/// under BOTH keys; each module is internally consistent about which
/// convention it uses, so reads always find the plane its writes land on.
fn seed_universe(datadir: &str) {
    mirror_into_simvfs(std::path::Path::new(datadir), "");
    mirror_into_simvfs(std::path::Path::new(datadir), datadir.trim_end_matches('/'));
    if let Ok(dirs) = std::env::var("PGRUST_SIMNET_SEED_DIRS") {
        for d in dirs.split(':').filter(|d| !d.is_empty()) {
            mirror_into_simvfs(std::path::Path::new(d), d);
        }
    }
}

fn read_script(env_name: &str) -> Vec<String> {
    std::env::var(env_name)
        .ok()
        .map(|p| std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {env_name}: {e}")))
        .unwrap_or_else(|| "SELECT 1".to_string())
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .map(String::from)
        .collect()
}

fn install_pump_for(env_sql: &str) {
    let mut client = SimWireClient::new(read_script(env_sql));
    pqcomm_simnet::install_client_pump(move || client.pump());
}

/// Dump THIS thread's session artifacts (transcript + op log are provider
/// thread-locals — one session per thread, one artifact pair per session).
fn dump_artifacts_env(transcript_env: &str, oplog_env: &str) {
    let (_, received) = pqcomm_simnet::client_transcript();
    if let Ok(path) = std::env::var(transcript_env) {
        let _ = std::fs::write(path, &received);
    }
    if let Ok(path) = std::env::var(oplog_env) {
        let mut out = pqcomm_simnet::op_log().join("\n");
        out.push('\n');
        let _ = std::fs::write(path, out);
    }
}

// Thread-local globals a spawned session thread inherits from the booted
// thread — launch_backend's `Inherited` census (the fork-inheritance list),
// scalar subset; the MAXPGPATH exec-path triple is omitted (nothing in a sim
// wire session consults it; ledgered in notes/permit-s1-demo.md). The spawn
// itself migrates to the launch_backend/pgsync spawn door when WS-CORE's
// registration hook lands — this snapshot is the scaffold stand-in.
macro_rules! sim_inherited {
    ($($field:ident : $ty:ty = $get:ident / $set:ident;)+) => {
        struct SimInherited {
            data_dir: Option<&'static str>,
            $($field: $ty,)+
        }
        impl SimInherited {
            fn capture() -> Self {
                Self {
                    data_dir: init_small::globals::DataDir(),
                    $($field: init_small::globals::$get(),)+
                }
            }
            fn apply(&self) {
                if let Some(dd) = self.data_dir {
                    init_small::globals::SetDataDir(dd);
                }
                $(init_small::globals::$set(self.$field);)+
            }
        }
    };
}

sim_inherited! {
    data_directory_mode: i32 = data_directory_mode / set_data_directory_mode;
    date_style: i32 = DateStyle / SetDateStyle;
    date_order: i32 = DateOrder / SetDateOrder;
    interval_style: i32 = IntervalStyle / SetIntervalStyle;
    enable_fsync: bool = enableFsync / set_enableFsync;
    allow_system_table_mods: bool = allowSystemTableMods / set_allowSystemTableMods;
    work_mem: i32 = work_mem / set_work_mem;
    hash_mem_multiplier: f64 = hash_mem_multiplier / set_hash_mem_multiplier;
    maintenance_work_mem: i32 = maintenance_work_mem / set_maintenance_work_mem;
    max_parallel_maintenance_workers: i32 =
        max_parallel_maintenance_workers / set_max_parallel_maintenance_workers;
    n_buffers: i32 = NBuffers / SetNBuffers;
    max_connections: i32 = MaxConnections / SetMaxConnections;
    max_worker_processes: i32 = max_worker_processes / set_max_worker_processes;
    max_parallel_workers: i32 = max_parallel_workers / set_max_parallel_workers;
    max_backends: i32 = MaxBackends / SetMaxBackends;
    vacuum_buffer_usage_limit: i32 = VacuumBufferUsageLimit / SetVacuumBufferUsageLimit;
    commit_timestamp_buffers: i32 = commit_timestamp_buffers / set_commit_timestamp_buffers;
    multixact_member_buffers: i32 = multixact_member_buffers / set_multixact_member_buffers;
    multixact_offset_buffers: i32 = multixact_offset_buffers / set_multixact_offset_buffers;
    notify_buffers: i32 = notify_buffers / set_notify_buffers;
    serializable_buffers: i32 = serializable_buffers / set_serializable_buffers;
    subtransaction_buffers: i32 = subtransaction_buffers / set_subtransaction_buffers;
    transaction_buffers: i32 = transaction_buffers / set_transaction_buffers;
}

/// Run one wire session on the CURRENT thread (session half only; the boot
/// half must have completed on the booting thread). Returns the session's
/// exit code; dumps this thread's artifacts on a clean ProcExitThread.
fn run_session_on_this_thread(transcript_env: &str, oplog_env: &str) -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
            let err = match crate::stdio_wire::stdio_wire_session_half() {
                Ok(never) => match never {},
                Err(err) => err,
            };
            elog::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
        },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        Some(p) => {
            // Exit callbacks already ran inline during the unwind
            // (!IsUnderPostmaster drains in place); the session is complete.
            dump_artifacts_env(transcript_env, oplog_env);
            p.code
        }
        None => std::panic::resume_unwind(payload),
    }
}

/// Session 2's session half: stdio_wire_session_half's shape for an
/// under-postmaster-style thread — same connection-first order, two
/// deliberate deltas: (1) wedge-ledger W6: the under-postmaster arm runs
/// REAL hba auth, and the sim transport's zeroed raddr ("host \"???\"")
/// matches no hba line — patch the port's peer to an AF_UNIX sockaddr so
/// initdb's `local all all trust` row matches (the transport IS
/// host-supplied, trust-by-construction, same argument as
/// wire_session_initialize's); (2) no single-user signal bridge (native
/// signal dispositions are process-wide; the boot thread owns them).
fn run_second_session_inner() -> ::types_error::PgResult<core::convert::Infallible> {
    // Wedge-ledger W7: the hba table is loaded by the POSTMASTER
    // (PostmasterMain), which this mode does not run — an under-postmaster
    // session thread must load it itself or every entry lookup fails with
    // "no pg_hba.conf entry". Raw-plane read of the initdb-generated file
    // (local/host trust rows), postmaster order: hba then ident.
    assert!(auth_seams::load_hba::call(), "s2: could not load pg_hba.conf");
    let _ = auth_seams::load_ident::call();
    {
        let startup = mcx::MemoryContext::new("WireStartup");
        backend_startup::wire_session_initialize(startup.mcx())?;
    }
    init_small::globals::WithMyProcPort(|p| {
        let mut un: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        un.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let n = std::mem::size_of::<libc::sockaddr_un>().min(p.raddr.addr.len());
        let src = &un as *const libc::sockaddr_un as *const u8;
        unsafe { std::ptr::copy_nonoverlapping(src, p.raddr.addr.as_mut_ptr(), n) };
        p.raddr.salen = n as u32;
    });
    lmgr_proc::InitProcess(miscinit::GetMyBackendType())?;
    let (dbname, username) = init_small::globals::WithMyProcPort(|p| {
        (
            p.database_name.clone().unwrap_or_default(),
            p.user_name.clone().unwrap_or_default(),
        )
    });
    crate::PostgresMain(&dbname, &username)
}

fn run_second_session() -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
            let err = match run_second_session_inner() {
                Ok(never) => match never {},
                Err(err) => err,
            };
            elog::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
        },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        Some(p) => {
            // Under-postmaster-style thread: the exit-callback drain is
            // deferred to the thread top — run_child_task's shape.
            let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ipc::run_deferred_exit_callbacks(p.code)
            }))
            .unwrap_or(101);
            dump_artifacts_env("PGRUST_SIMNET_TRANSCRIPT2", "PGRUST_SIMNET_OPLOG2");
            code
        }
        None => std::panic::resume_unwind(payload),
    }
}

/// The second session thread's prelude: a fresh thread has NONE of the
/// booted thread's thread-local state ("C per-process globals" are
/// thread-locals in the port) and an EMPTY SimVfs universe. Re-derive the
/// cheap config-shaped state the way an EXEC_BACKEND child would, seed the
/// universe, then serve the session. KNOWN LIMIT (wedge-ledger row L1): the
/// universe is PRIVATE — the two sessions do not share a database; the
/// shared plane is process shmem only. Multi-backend sim over ONE shared
/// universe is a substrate follow-on, not a demo-lane fix.
fn second_session_thread(
    argv: Vec<String>,
    datadir: String,
    snap: SimInherited,
) -> pgsync::thread::JoinHandle<i32> {
    // PERMIT-S1 compose wiring: pgsync spawn — under PGRUST_SIM_SCHED=1 the
    // child registers in-model (synthetic vpid, parent-side spawn fence) so
    // the two backends interleave ONLY at scheduler touches and the SCHEDOP
    // stream is a seeded function of PGRUST_SIM_SEED; plain std passthrough
    // when the scheduler is off (the pre-compose behavior).
    pgsync::thread::Builder::new()
        .name("sim-session-2".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            snap.apply();
            // The child-init sequence, the launch_backend way: the second
            // session is an UNDER-POSTMASTER-style backend thread (the boot
            // thread played "postmaster + first backend"). Wedge-ledger W1:
            // a bare thread dies at SwitchToSharedLatch's local-latch debug
            // assert — InitPostmasterChild owns the latch trio. Wedge-ledger
            // W2: a STANDALONE-arm session half re-runs StartupXLOG, which
            // reads the booted thread's PROCESS-GLOBAL ControlFile image but
            // looks for its checkpoint in THIS thread's PRIVATE SimVfs
            // universe → "could not locate a valid checkpoint record" PANIC;
            // IsUnderPostmaster=true is the product path that skips it.
            // Distinct synthetic pid: the sim pid pin gives every thread the
            // same ambient pid; sessions must differ.
            // Wedge-ledger W4: seed THIS thread's universe before any GUC
            // processing — timezone validation walks the tz database through
            // the (thread-local) vfs plane.
            seed_universe(&datadir);
            // Thread-local GUC store: defaults, then argv -c options, then
            // the config files — the EXEC_BACKEND-shaped restore. Wedge-
            // ledger W3: this must run BEFORE InitPostmasterChild flips
            // IsUnderPostmaster (guc_file asserts PGC_POSTMASTER-context
            // reads happen only pre-postmaster).
            guc_seams::initialize_guc_options::call().expect("s2 guc init");
            let mut dbname_arg: Option<String> = None;
            crate::switches::process_postgres_switches_dbname(
                &argv,
                ::types_guc::GucContext::PGC_POSTMASTER as i32 as u8,
                &mut dbname_arg,
            )
            .expect("s2 switches");
            if !guc_seams::select_config_files::call(
                crate::switches::user_d_option().as_deref(),
                PROGNAME,
            )
            .expect("s2 config files")
            {
                return 1;
            }
            miscinit::InitPostmasterChild(init_small::globals::process_id() as i32 + 1)
                .expect("s2 InitPostmasterChild");
            miscinit::SetMyBackendType(types_core::init::BackendType::Backend);
            // Wedge-ledger W5: the postmaster launches backends only after
            // recovery; here the boot thread runs StartupXLOG inside SESSION
            // 1's InitPostgres, so an early second backend reads snapshots
            // "in recovery" (unported KnownAssignedXids panic). Same gate,
            // demo-shaped: wait for the shared recovery state to clear.
            // pgsync sleep = TimedPark under the permit scheduler (a raw
            // std sleep here would hold the permit across the poll and
            // starve session 1 out of ever finishing recovery).
            let mut waited_ms = 0u32;
            while transam_xlog::RecoveryInProgress() {
                pgsync::thread::sleep(std::time::Duration::from_millis(1));
                waited_ms += 1;
                assert!(waited_ms < 60_000, "s2: recovery never completed (W5 gate)");
            }
            install_pump_for("PGRUST_SIMNET_SQL2");
            run_second_session()
        })
        .expect("spawn sim-session-2")
}

#[allow(non_snake_case)]
pub fn PostgresSimNetMain(argv: &[String], username: &str) -> ! {
    // ---- PERMIT-S1 demo corpora that need no server boot at all: the
    // planted race (P2) and the watchdog red fixture (P4) divert here.
    crate::sim_sched_demo::maybe_run_demo_corpus_from_env();

    // PERMIT-S1 compose wiring: register the boot/driver thread at the
    // spawn door (core hand-back: registrations from a REGISTERED thread
    // are program-ordered — the deterministic pattern). No-op unless
    // PGRUST_SIM_SCHED=1, so sim-net-e2e and dst-smoke are byte-unaffected.
    let _sched_door = pgsync::sim::spawn_door::register_self("simnet-main");

    // ---- Sim-harness plumbing BEFORE the transport-blind ladder runs.
    // Datadir: the -D argument (the ladder re-parses it itself later).
    let datadir = argv
        .iter()
        .position(|a| a == "-D")
        .and_then(|i| argv.get(i + 1))
        .cloned()
        .expect("--sim-net requires -D <datadir>");
    seed_universe(&datadir);
    install_pump_for("PGRUST_SIMNET_SQL");

    // Transport fault plan (inc-2): PGRUST_SIMNET_FAULTS carries a
    // parse_fault_spec spec (e.g. "seed=0x5EED Read@12=drop:2"); rules are
    // op-sequence-targeted and every firing is NETFAULT-logged into the same
    // op log the determinism gate byte-compares — fault runs replay too.
    // (Installed on THIS thread: the provider state is thread-local, so the
    // plan governs session 1 — the fault arms run single-session.)
    if let Ok(spec) = std::env::var("PGRUST_SIMNET_FAULTS") {
        if !spec.trim().is_empty() {
            pqcomm_simnet::install_fault_plan_from_spec(&spec);
        }
    }

    let sessions: u32 = std::env::var("PGRUST_SIMNET_SESSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    if sessions <= 1 {
        // ---- The single-session path (stdio_wire's inner, verbatim).
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> core::convert::Infallible {
                let err = match crate::stdio_wire::stdio_wire_main_inner(argv, username) {
                    Ok(never) => match never {},
                    Err(err) => err,
                };
                elog::emit_error_report_for(&err);
                ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
            },
        ));
        let payload = match outcome {
            Ok(never) => match never {},
            Err(payload) => payload,
        };
        match payload.downcast_ref::<ipc::ProcExitThread>() {
            Some(p) => {
                // Exit callbacks (shutdown checkpoint among them) already ran
                // inline during the unwind; the session is complete — dump the
                // determinism artifacts, then take the exit.
                dump_artifacts_env("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
                std::process::exit(p.code)
            }
            None => std::panic::resume_unwind(payload),
        }
    }

    // ---- PERMIT-S1 P1: the two-backend corpus (the first multi-thread sim
    // run). Boot half ONCE on this thread, then two wire sessions: this
    // thread serves session 1 (whose InitPostgres runs StartupXLOG — the
    // "postmaster + first backend" role), a spawned thread serves session 2
    // once recovery clears (W5 gate) — CONCURRENT by default.
    // PGRUST_SIMNET_DUO_SERIAL=1 is the serialized fallback rung for wedge
    // localization: session 1 completes before session 2 spawns.
    assert!(sessions == 2, "PGRUST_SIMNET_SESSIONS supports 1 or 2");
    if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
        elog::emit_error_report_for(&err);
        std::process::exit(1);
    }
    let snap = SimInherited::capture();
    let serial = std::env::var("PGRUST_SIMNET_DUO_SERIAL").is_ok_and(|v| v == "1");
    let (code1, code2) = if serial {
        let code1 = run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        let s2 = second_session_thread(argv.to_vec(), datadir.clone(), snap);
        (code1, s2.join().unwrap_or(101))
    } else {
        let s2 = second_session_thread(argv.to_vec(), datadir.clone(), snap);
        let code1 = run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        (code1, s2.join().unwrap_or(101))
    };
    std::process::exit(code1.max(code2))
}

#[allow(dead_code)]
fn _progname_used() -> &'static str {
    PROGNAME
}
