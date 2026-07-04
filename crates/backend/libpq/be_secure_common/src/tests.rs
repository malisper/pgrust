use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Once;

static SETUP: Once = Once::new();

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
    });
    fd::InitFileAccess();
}

fn scratch_file(tag: &str, mode: u32) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "pgrust_besec_{}_{tag}_{n}.key",
        std::process::id()
    ));
    std::fs::write(&path, b"KEY").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    path.to_str().unwrap().to_owned()
}

// The GUC backing is process-global; passphrase tests must not interleave.
static PASSPHRASE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn set_passphrase_command(cmd: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = PASSPHRASE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guc_tables::vars::ssl_passphrase_command.write(Some(cmd.to_string()));
    guard
}

#[test]
fn key_file_permissions_0600_ok() {
    setup();
    let path = scratch_file("ok", 0o600);
    assert!(crate::check_ssl_key_file_permissions(&path, false).unwrap());
    assert!(crate::check_ssl_key_file_permissions(&path, true).unwrap());
}

#[test]
fn key_file_permissions_group_access_rejected() {
    setup();
    let path = scratch_file("grp", 0o640);
    assert!(!crate::check_ssl_key_file_permissions(&path, false).unwrap());
    let err = crate::check_ssl_key_file_permissions(&path, true).unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_CONFIG_FILE_ERROR);
    assert!(err.message.contains("has group or world access"));
}

#[test]
fn key_file_permissions_world_access_rejected() {
    setup();
    let path = scratch_file("world", 0o644);
    assert!(!crate::check_ssl_key_file_permissions(&path, false).unwrap());
}

#[test]
fn key_file_missing_rejected() {
    setup();
    assert!(!crate::check_ssl_key_file_permissions("/nonexistent/no.key", false).unwrap());
}

#[test]
fn key_file_not_regular_rejected() {
    setup();
    assert!(!crate::check_ssl_key_file_permissions("/tmp", false).unwrap());
}

#[test]
fn passphrase_command_substitutes_prompt_and_strips_crlf() {
    setup();
    let _g = set_passphrase_command("echo \"got:%p\"");
    let mut buf = [0u8; 256];
    let len = crate::run_ssl_passphrase_command("Enter PEM pass phrase:", true, &mut buf).unwrap();
    assert_eq!(&buf[..len], b"got:Enter PEM pass phrase:");
}

#[test]
fn passphrase_command_percent_escape() {
    setup();
    let _g = set_passphrase_command("echo '100%%'");
    let mut buf = [0u8; 64];
    let len = crate::run_ssl_passphrase_command("p", true, &mut buf).unwrap();
    assert_eq!(&buf[..len], b"100%");
}

#[test]
fn passphrase_command_failure_is_error_at_server_start() {
    setup();
    let _g = set_passphrase_command("exit 3");
    let mut buf = [0u8; 64];
    let err = crate::run_ssl_passphrase_command("p", true, &mut buf).unwrap_err();
    assert!(err.message.contains("failed"), "unexpected error: {err:?}");
}

#[test]
fn passphrase_command_failure_is_soft_on_reload() {
    setup();
    let _g = set_passphrase_command("exit 3");
    let mut buf = [0u8; 64];
    assert_eq!(
        crate::run_ssl_passphrase_command("p", false, &mut buf).unwrap(),
        0
    );
}

#[test]
fn passphrase_bad_placeholder_errors() {
    setup();
    let _g = set_passphrase_command("echo %q");
    let mut buf = [0u8; 64];
    let err = crate::run_ssl_passphrase_command("p", true, &mut buf).unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_INVALID_PARAMETER_VALUE);
}
