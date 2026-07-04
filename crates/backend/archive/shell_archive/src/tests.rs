use std::cell::Cell;

use percentrepl::replace_percent_placeholders;

#[test]
fn percent_placeholders_substitution() {
    let out = replace_percent_placeholders(
        "cp %p /arch/%f",
        "archive_command",
        &[('f', Some("SEG")), ('p', Some("pg_wal/SEG"))],
    )
    .unwrap();
    assert_eq!(out, "cp pg_wal/SEG /arch/SEG");

    assert_eq!(
        replace_percent_placeholders("100%% done", "x", &[]).unwrap(),
        "100% done"
    );
    assert_eq!(replace_percent_placeholders("", "x", &[]).unwrap(), "");
}

#[test]
fn percent_placeholders_errors() {
    let e = replace_percent_placeholders("echo %", "archive_command", &[]).unwrap_err();
    assert_eq!(e.sqlstate, types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(e.message, "invalid value for parameter \"archive_command\": \"echo %\"");
    assert_eq!(
        e.detail.as_deref(),
        Some("String ends unexpectedly after escape character \"%\".")
    );

    let e = replace_percent_placeholders("echo %q", "restore_command", &[('f', Some("x"))])
        .unwrap_err();
    assert_eq!(e.message, "invalid value for parameter \"restore_command\": \"echo %q\"");
    assert_eq!(e.detail.as_deref(), Some("String contains unexpected placeholder \"%q\"."));

    // A present letter with a NULL value reports the same unknown-placeholder error.
    let e = replace_percent_placeholders("echo %p", "restore_command", &[('p', None)])
        .unwrap_err();
    assert_eq!(e.detail.as_deref(), Some("String contains unexpected placeholder \"%p\"."));
}

#[test]
fn wait_result_classification() {
    let rc = wait_error::system("exit 3");
    assert!(wait_error::WIFEXITED(rc));
    assert_eq!(wait_error::WEXITSTATUS(rc), 3);
    assert!(!wait_error::wait_result_is_any_signal(rc, true));
    assert_eq!(wait_error::wait_result_to_str(rc), "child process exited with exit code 3");

    let rc = wait_error::system("true");
    assert_eq!(rc, 0);

    let rc = wait_error::system("command_that_does_not_exist_pgrust");
    assert!(wait_error::WIFEXITED(rc));
    assert_eq!(wait_error::WEXITSTATUS(rc), 127);
    assert_eq!(wait_error::wait_result_to_str(rc), "command not found");
    assert!(wait_error::wait_result_is_any_signal(rc, true));
    assert!(!wait_error::wait_result_is_any_signal(rc, false));

    let rc = wait_error::system("kill -TERM $$");
    assert!(wait_error::WIFSIGNALED(rc));
    assert_eq!(wait_error::WTERMSIG(rc), libc::SIGTERM);
    assert!(wait_error::wait_result_is_signal(rc, libc::SIGTERM));
    assert!(!wait_error::wait_result_is_signal(rc, libc::SIGINT));
    assert!(wait_error::wait_result_is_any_signal(rc, false));
    assert!(wait_error::wait_result_to_str(rc)
        .starts_with(&format!("child process was terminated by signal {}: ", libc::SIGTERM)));

    // Shell-reported child signal death: exit code 128 + signum.
    let rc = wait_error::system("sh -c 'kill -TERM $$'; exit $?");
    assert!(wait_error::wait_result_is_signal(rc, libc::SIGTERM));
}

thread_local! {
    static TEST_ARCHIVE_COMMAND: Cell<&'static str> = const { Cell::new("") };
}

fn install_archive_command_var() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        guc_tables::vars::XLogArchiveCommand.install_if_absent(guc_tables::GucVarAccessors {
            get: || Some(TEST_ARCHIVE_COMMAND.get().to_string()),
            set: |v| TEST_ARCHIVE_COMMAND.set(v.map(|s| &*s.leak()).unwrap_or("")),
        });
    });
}

#[test]
fn shell_archive_exit_status_texts() {
    install_archive_command_var();

    TEST_ARCHIVE_COMMAND.set("");
    assert_eq!(
        crate::shell_archive_configured().as_deref(),
        Some("\"archive_command\" is not set.")
    );

    TEST_ARCHIVE_COMMAND.set("test -f %p && echo %f >/dev/null");
    assert!(crate::shell_archive_configured().is_none());

    // Missing file: plain nonzero exit -> LOG classification, Ok(false).
    let r = crate::shell_archive_file("NOSUCH", Some("pg_wal/NOSUCH")).unwrap();
    assert!(!r);

    TEST_ARCHIVE_COMMAND.set("true");
    let r = crate::shell_archive_file("SEG", Some("pg_wal/SEG")).unwrap();
    assert!(r);

    // FATAL classification (in the live path errfinish proc_exits the
    // archiver thread, so assert on the classifier).
    let rc = wait_error::system("kill -TERM $$");
    let (lev, msg) = crate::classify_archive_failure(rc);
    assert_eq!(lev, types_error::FATAL);
    assert_eq!(
        msg,
        format!(
            "archive command was terminated by signal {}: {}",
            libc::SIGTERM,
            wait_error::pg_strsignal(libc::SIGTERM)
        )
    );

    let rc = wait_error::system("command_that_does_not_exist_pgrust");
    let (lev, msg) = crate::classify_archive_failure(rc);
    assert_eq!(lev, types_error::FATAL);
    assert_eq!(msg, "archive command failed with exit code 127");

    let rc = wait_error::system("exit 1");
    let (lev, msg) = crate::classify_archive_failure(rc);
    assert_eq!(lev, types_error::LOG);
    assert_eq!(msg, "archive command failed with exit code 1");
}
