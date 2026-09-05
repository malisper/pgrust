//! Close-failure witness for controldata_utils.c:125 / :272 — sim-cfg only.
//! Built by the CI cluster job's root-phase PRE_E2E_CARGO
//! (`--config=build.rustflags=['--cfg','pgrust_sim'] test -p controldata_utils
//! --test sim_close_fault --no-run --target-dir target/sim-controldata`) and
//! executed by scripts/controldata-close-fault-e2e.sh.
//!
//! C's get_controlfile_by_exact_path checks CloseTransientFile(fd) and
//! raises ERROR `could not close file "%s": %m`; update_controlfile checks
//! close(fd) and raises PANIC with the same text. A kernel close(2) on a
//! regular file cannot be made to fail from a test, so the fault is injected
//! through SimVfs's seeded fault plan: the first Close op on a path
//! containing "pg_control" returns -1/EIO.
#![cfg(pgrust_sim)]

use controldata_utils::*;
use std::ffi::CString;
use types_error::{ERRCODE_IO_ERROR, ERROR, PANIC};
use vfs::sim::{FaultDecision, FaultRule, OpKind, OpMatch, SeededFaultPlan, SimVfs};

const DATADIR: &str = "/b162";

fn cpath(p: &str) -> CString {
    CString::new(p).unwrap()
}

// The initdb'd pg_control KAT, compiled in: the e2e phase runs the binary as
// a user that cannot read the checkout.
const FIXTURE: &[u8] = include_bytes!("data/pg_control");

/// Fresh per-thread sim disk holding `<DATADIR>/global/pg_control`.
fn mint_datadir() {
    SimVfs::reset();
    assert_eq!(vfs::mkdir(&cpath(DATADIR), 0o700), 0);
    assert_eq!(vfs::mkdir(&cpath(&format!("{DATADIR}/global")), 0o700), 0);
    let path = format!("{DATADIR}/{XLOG_CONTROL_FILE}");
    let fd = vfs::open(&cpath(&path), libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o600);
    assert!(fd >= 0, "sim open failed: errno {}", vfs::get_errno());
    assert_eq!(vfs::pwrite(fd, FIXTURE, 0), FIXTURE.len() as isize);
    assert_eq!(vfs::close(fd), 0);
}

/// The first close of the control file fails with EIO.
fn arm_close_eio() {
    SeededFaultPlan::install(
        0xB162,
        vec![FaultRule::nth_matching(
            OpMatch {
                kinds: Some(vec![OpKind::Close]),
                class: None,
                path_contains: Some("pg_control".to_string()),
            },
            1,
            FaultDecision::Errno(libc::EIO),
        )],
    );
}

fn expected_close_message() -> String {
    format!(
        "could not close file \"{DATADIR}/{XLOG_CONTROL_FILE}\": {}",
        elog::errno::strerror(libc::EIO)
    )
}

#[test]
fn get_controlfile_close_failure_is_error() {
    mint_datadir();
    arm_close_eio();
    // controldata_utils.c:125: ereport(ERROR, errcode_for_file_access(),
    // "could not close file \"%s\": %m").
    let err = get_controlfile(DATADIR).expect_err("close EIO must raise, not return the image");
    assert_eq!(err.level(), ERROR);
    assert_eq!(err.sqlstate(), ERRCODE_IO_ERROR);
    assert_eq!(err.message(), expected_close_message());
    assert!(
        SimVfs::fault_log().iter().any(|l| l.contains("Close")),
        "fault plan never fired: {:?}",
        SimVfs::fault_log()
    );
}

// PANIC is C's abort(): elog's errfinish unwinds PanicExitThread after
// emitting the report, so the message is observed through the emit-log hook
// (installed per thread) and the level through the unwind payload.
type Report = (types_error::ErrorLevel, types_error::SqlState, String);
static PANIC_REPORTS: std::sync::Mutex<Vec<(std::thread::ThreadId, Report)>> =
    std::sync::Mutex::new(Vec::new());

fn capture_report(err: &types_error::PgError, _output_to_server: &mut bool) {
    PANIC_REPORTS.lock().unwrap().push((
        std::thread::current().id(),
        (err.level(), err.sqlstate(), err.message().to_string()),
    ));
}

#[test]
fn update_controlfile_close_failure_is_panic() {
    mint_datadir();
    let (mut cf, crc_ok) = get_controlfile(DATADIR).unwrap();
    assert!(crc_ok);
    arm_close_eio();
    elog::set_emit_log_hook(Some(capture_report));
    // controldata_utils.c:272: ereport(PANIC, errcode_for_file_access(),
    // "could not close file \"%s\": %m").
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        update_controlfile(DATADIR, &mut cf, true)
    }));
    elog::set_emit_log_hook(None);
    match r {
        Err(payload) => assert!(
            payload.is::<types_error::PanicExitThread>(),
            "close EIO must PANIC (PanicExitThread unwind)"
        ),
        Ok(inner) => panic!("close EIO must PANIC, not return control: {inner:?}"),
    }
    let me = std::thread::current().id();
    let reports = PANIC_REPORTS.lock().unwrap();
    let (level, sqlstate, message) = reports
        .iter()
        .rev()
        .find(|(t, _)| *t == me)
        .map(|(_, r)| r.clone())
        .expect("the PANIC report must reach the emit-log hook");
    assert_eq!(level, PANIC);
    assert_eq!(sqlstate, ERRCODE_IO_ERROR);
    assert_eq!(message, expected_close_message());
}
