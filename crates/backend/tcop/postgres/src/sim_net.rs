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

fn dump_artifacts() {
    let (_, received) = pqcomm_simnet::client_transcript();
    if let Ok(path) = std::env::var("PGRUST_SIMNET_TRANSCRIPT") {
        let _ = std::fs::write(path, &received);
    }
    if let Ok(path) = std::env::var("PGRUST_SIMNET_OPLOG") {
        let mut out = pqcomm_simnet::op_log().join("\n");
        out.push('\n');
        let _ = std::fs::write(path, out);
    }
}

#[allow(non_snake_case)]
pub fn PostgresSimNetMain(argv: &[String], username: &str) -> ! {
    // ---- Sim-harness plumbing BEFORE the transport-blind ladder runs.
    // Datadir: the -D argument (the ladder re-parses it itself later).
    let datadir = argv
        .iter()
        .position(|a| a == "-D")
        .and_then(|i| argv.get(i + 1))
        .cloned()
        .expect("--sim-net requires -D <datadir>");
    // Two addressing conventions coexist in the port: post-chdir RELATIVE
    // paths (md.c-style "base/…", "pg_wal/…") and DataDir-joined ABSOLUTE
    // paths (controldata's "<datadir>/global/pg_control"). Seed the image
    // under BOTH keys; each module is internally consistent about which
    // convention it uses, so reads always find the plane its writes land on.
    mirror_into_simvfs(std::path::Path::new(&datadir), "");
    mirror_into_simvfs(std::path::Path::new(&datadir), datadir.trim_end_matches('/'));
    if let Ok(dirs) = std::env::var("PGRUST_SIMNET_SEED_DIRS") {
        for d in dirs.split(':').filter(|d| !d.is_empty()) {
            mirror_into_simvfs(std::path::Path::new(d), d);
        }
    }

    let stmts: Vec<String> = std::env::var("PGRUST_SIMNET_SQL")
        .ok()
        .map(|p| std::fs::read_to_string(p).expect("read PGRUST_SIMNET_SQL"))
        .unwrap_or_else(|| "SELECT 1".to_string())
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .map(String::from)
        .collect();
    let mut client = SimWireClient::new(stmts);
    pqcomm_simnet::install_client_pump(move || client.pump());

    // Transport fault plan (inc-2): PGRUST_SIMNET_FAULTS carries a
    // parse_fault_spec spec (e.g. "seed=0x5EED Read@12=drop:2"); rules are
    // op-sequence-targeted and every firing is NETFAULT-logged into the same
    // op log the determinism gate byte-compares — fault runs replay too.
    if let Ok(spec) = std::env::var("PGRUST_SIMNET_FAULTS") {
        if !spec.trim().is_empty() {
            pqcomm_simnet::install_fault_plan_from_spec(&spec);
        }
    }

    // ---- The transport-blind wire session (stdio_wire's inner, verbatim).
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
            dump_artifacts();
            std::process::exit(p.code)
        }
        None => std::panic::resume_unwind(payload),
    }
}

#[allow(dead_code)]
fn _progname_used() -> &'static str {
    PROGNAME
}
