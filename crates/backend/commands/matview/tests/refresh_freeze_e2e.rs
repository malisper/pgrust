// Freeze-visibility e2e for the transientrel receiver's TABLE_INSERT_FROZEN
// (matview.c transientrel_startup parity):
//
//   1. CREATE MATERIALIZED VIEW ... AS SELECT (the CREATE arm's datafill) and
//      REFRESH MATERIALIZED VIEW (non-concurrent) must produce heaps whose
//      every LP_NORMAL tuple already carries the frozen-xmin hint
//      (HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID) with NO vacuum ever run, and
//      the rows must be visible (count parity) straight after the fill.
//   2. Detection-power control: plain CTAS heaps must NOT be frozen — C's
//      intorel_startup (createas.c) sets only TABLE_INSERT_SKIP_FSM, so a
//      frozen CTAS tuple here would mean the flag leaked where C doesn't
//      put it (and proves the checker can tell the two states apart).
//
// Both receiver write arms are exercised: the W1 multi-insert buffer
// (PGRUST_CTAS_MULTIINSERT default-on) and the per-tuple table_tuple_insert
// path (PGRUST_CTAS_MULTIINSERT=0).
//
// Requires a PostgreSQL 18 install for initdb/psql (same discovery order as
// scripts/*-e2e.sh); the server under test is this workspace's own postgres
// binary, built on demand for the running profile.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const HEAP_XMIN_COMMITTED: u16 = 0x0100;
const HEAP_XMIN_INVALID: u16 = 0x0200;
const HEAP_XMIN_FROZEN: u16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
const BLCKSZ: usize = 8192;

fn find_pgbin() -> Option<PathBuf> {
    let mut cands: Vec<PathBuf> = Vec::new();
    if let Ok(p) = std::env::var("PGINSTALL") {
        if !p.is_empty() {
            cands.push(PathBuf::from(p));
        }
    }
    cands.push(PathBuf::from("/tmp/pgrust_pginstall/bin"));
    cands.push(PathBuf::from("/opt/homebrew/bin"));
    cands.push(PathBuf::from("/opt/homebrew/opt/postgresql@18/bin"));
    cands.into_iter().find(|c| c.join("initdb").is_file())
}

fn tzdir(pgbin: &Path) -> Option<PathBuf> {
    for share in ["../share/postgresql/timezone", "../share/postgresql@18/timezone"] {
        let d = pgbin.join(share);
        if d.is_dir() {
            return Some(d);
        }
    }
    None
}

/// Build (never merely reuse) this workspace's postgres server binary for the
/// profile the test itself runs under. Reusing an existing binary would let a
/// stale server answer the assertions — the build is a no-op when current.
fn server_binary() -> PathBuf {
    // target/<profile>/deps/refresh_freeze_e2e-... -> target/<profile>/postgres
    let exe = std::env::current_exe().expect("current_exe");
    let profile_dir = exe
        .parent()
        .expect("deps dir")
        .parent()
        .expect("profile dir")
        .to_path_buf();
    let bin = profile_dir.join("postgres");
    let release = profile_dir.file_name().is_some_and(|n| n == "release");
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest.ancestors().nth(4).expect("workspace root");
    let mut cmd = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    cmd.current_dir(root).args(["build", "-p", "main_main", "--bin", "postgres"]);
    if release {
        cmd.arg("--release");
    }
    eprintln!("(building {} postgres server binary...)", if release { "release" } else { "debug" });
    let status = cmd.status().expect("spawn cargo build");
    assert!(status.success(), "cargo build of the postgres binary failed");
    assert!(bin.is_file(), "built server binary missing at {}", bin.display());
    bin
}

struct Server {
    child: Child,
    sock: PathBuf,
    port: u16,
    pgbin: PathBuf,
    log: PathBuf,
}

impl Server {
    fn start(work: &Path, pgbin: &Path, bin: &Path, port: u16, multiinsert_on: bool) -> Server {
        let dd = work.join("dd");
        // The Unix socket path has a 103-byte kernel cap, so the socket dir
        // lives at a short path instead of under target/ (the datadir and log
        // stay in `work`).
        let sock = PathBuf::from(format!("/tmp/pgrfz{port}"));
        let log = work.join("server.log");
        let _ = std::fs::remove_dir_all(&sock);
        std::fs::create_dir_all(&sock).unwrap();

        let st = Command::new(pgbin.join("initdb"))
            .args(["-D"])
            .arg(&dd)
            .args(["--no-locale", "--encoding=UTF8", "-U", "postgres", "-A", "trust"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("spawn initdb");
        assert!(st.success(), "initdb failed");

        // ulimit -s for the main thread (session threads get RUST_MIN_STACK).
        let script = format!(
            "ulimit -s 65520 2>/dev/null; exec \"$1\" -D \"$2\" -k \"$3\" -p {port} \
             -c max_stack_depth=60000 -c io_method=sync -c autovacuum=off >>\"$4\" 2>&1"
        );
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(&script)
            .arg("srv")
            .arg(bin)
            .arg(&dd)
            .arg(&sock)
            .arg(&log)
            .env("RUST_MIN_STACK", "67108864")
            .env("RUST_BACKTRACE", "1")
            .env("PGRUST_CTAS_MULTIINSERT", if multiinsert_on { "1" } else { "0" });
        if let Some(tz) = tzdir(pgbin) {
            cmd.env("PGRUST_PGSHAREDIR", tz.parent().unwrap());
            cmd.env("PGRUST_TZDIR", tz);
        }
        let child = cmd.spawn().expect("spawn server");
        let mut srv = Server { child, sock, port, pgbin: pgbin.to_path_buf(), log };
        srv.wait_ready();
        srv
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if let Ok(Some(st)) = self.child.try_wait() {
                panic!("server exited ({st}) during startup; log tail:\n{}", self.log_tail());
            }
            if self.try_sql("SELECT 1").is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("server never became ready; log tail:\n{}", self.log_tail());
    }

    fn log_tail(&self) -> String {
        let s = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    fn try_sql(&self, sql: &str) -> Result<String, String> {
        let mut child = Command::new(self.pgbin.join("psql"))
            .arg("-h")
            .arg(&self.sock)
            .args(["-p", &self.port.to_string(), "-U", "postgres", "-X", "-t", "-A"])
            .args(["-v", "ON_ERROR_STOP=1", "-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn psql");
        child.stdin.take().unwrap().write_all(sql.as_bytes()).unwrap();
        let out = child.wait_with_output().expect("psql wait");
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(format!(
                "psql failed\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    }

    fn sql(&self, sql: &str) -> String {
        match self.try_sql(sql) {
            Ok(o) => o,
            Err(e) => panic!("{e}\nserver log tail:\n{}", self.log_tail()),
        }
    }

    fn scalar(&self, sql: &str) -> String {
        self.sql(sql).trim().to_string()
    }

    fn stop(mut self) {
        let _ = Command::new("kill").args(["-INT", &self.child.id().to_string()]).status();
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                let _ = std::fs::remove_dir_all(&self.sock);
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = self.child.kill();
        panic!("server did not shut down on SIGINT within 60s");
    }
}

/// Scan a heap file: (LP_NORMAL tuple count, tuples whose xmin hint is
/// exactly the frozen pair HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID).
fn scan_heap_frozen(path: &Path) -> (usize, usize) {
    let data = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(data.len() % BLCKSZ == 0, "heap file not page-aligned: {}", path.display());
    let (mut ntup, mut nfrozen) = (0usize, 0usize);
    for page in data.chunks_exact(BLCKSZ) {
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        if pd_lower <= 24 {
            continue; // empty/new page
        }
        for off in (24..pd_lower).step_by(4) {
            let itemid = u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
            let lp_flags = (itemid >> 15) & 0x3;
            let lp_off = (itemid & 0x7FFF) as usize;
            if lp_flags != 1 {
                continue; // LP_NORMAL only
            }
            let infomask = u16::from_le_bytes(page[lp_off + 20..lp_off + 22].try_into().unwrap());
            ntup += 1;
            if infomask & HEAP_XMIN_FROZEN == HEAP_XMIN_FROZEN {
                nfrozen += 1;
            }
        }
    }
    (ntup, nfrozen)
}

fn run_arm(pgbin: &Path, bin: &Path, work: &Path, port: u16, multiinsert_on: bool) {
    let _ = std::fs::remove_dir_all(work);
    std::fs::create_dir_all(work).unwrap();
    let srv = Server::start(work, pgbin, bin, port, multiinsert_on);
    let arm = if multiinsert_on { "multi-insert" } else { "per-tuple" };

    srv.sql(
        "CREATE TABLE src (id int4, t text);\n\
         INSERT INTO src SELECT g, 'val_' || g FROM generate_series(1, 5000) g;\n\
         CREATE MATERIALIZED VIEW mv AS SELECT id, t FROM src;\n\
         CREATE MATERIALIZED VIEW mv_create_only AS SELECT id, t FROM src;\n\
         CREATE TABLE ctas_ctrl AS SELECT id, t FROM src;\n\
         INSERT INTO src SELECT g, 'val_' || g FROM generate_series(5001, 10000) g;\n\
         REFRESH MATERIALIZED VIEW mv;",
    );

    // (a) rows visible right after the fill — no vacuum has ever run
    // (autovacuum=off) and none is run before the on-disk check either.
    assert_eq!(srv.scalar("SELECT count(*) FROM mv"), "10000", "[{arm}] refreshed rows visible");
    assert_eq!(srv.scalar("SELECT count(*) FROM mv_create_only"), "5000", "[{arm}] created rows visible");
    assert_eq!(srv.scalar("SELECT count(*) FROM ctas_ctrl"), "5000", "[{arm}] ctas rows visible");

    let dboid = srv.scalar("SELECT oid FROM pg_database WHERE datname = 'postgres'");
    let rfn = |rel: &str| {
        srv.scalar(&format!("SELECT relfilenode FROM pg_class WHERE relname = '{rel}'"))
    };
    let (mv, mv_create, ctas) = (rfn("mv"), rfn("mv_create_only"), rfn("ctas_ctrl"));
    srv.sql("CHECKPOINT;");
    let dd = work.join("dd");
    srv.stop();

    let base = dd.join("base").join(&dboid);

    // (b) tuples actually frozen on disk, both transientrel-fed heaps.
    for (name, node) in [("mv", &mv), ("mv_create_only", &mv_create), ("ctas_ctrl", &ctas)] {
        let (n, f) = scan_heap_frozen(&base.join(node));
        eprintln!("[{arm}] {name}: {n} tuples, {f} frozen");
    }
    let (n, f) = scan_heap_frozen(&base.join(&mv));
    assert_eq!(n, 10000, "[{arm}] REFRESH heap tuple count");
    assert_eq!(f, n, "[{arm}] every REFRESH-written tuple carries the frozen xmin hint");
    let (n, f) = scan_heap_frozen(&base.join(&mv_create));
    assert_eq!(n, 5000, "[{arm}] CREATE-datafill heap tuple count");
    assert_eq!(f, n, "[{arm}] every CREATE-datafill tuple carries the frozen xmin hint");

    // Control: plain CTAS must NOT freeze (C intorel parity) — and this
    // proves the frozen assertions above cannot pass vacuously.
    let (n, f) = scan_heap_frozen(&base.join(&ctas));
    assert_eq!(n, 5000, "[{arm}] CTAS heap tuple count");
    assert_eq!(f, 0, "[{arm}] plain CTAS tuples must not be frozen");
}

#[test]
fn matview_fill_writes_frozen_tuples() {
    let Some(pgbin) = find_pgbin() else {
        // Same environmental precondition as scripts/*-e2e.sh; without a C
        // PostgreSQL 18 install there is no initdb/psql to drive the server.
        eprintln!("SKIP: no PostgreSQL 18 install found (set PGINSTALL)");
        return;
    };
    let bin = server_binary();
    let tmp = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("refresh_freeze_e2e");

    // Arm 1: W1 multi-insert buffering (the default) — heap_multi_insert lane.
    run_arm(&pgbin, &bin, &tmp.join("multi"), 5573, true);
    // Arm 2: per-tuple table_tuple_insert lane.
    run_arm(&pgbin, &bin, &tmp.join("single"), 5574, false);

    let _ = std::fs::remove_dir_all(&tmp);
}
