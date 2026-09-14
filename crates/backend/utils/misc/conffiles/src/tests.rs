use std::path::{Path, PathBuf};
use std::sync::Once;

use types_error::{ERROR, LOG};

use super::*;

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        elog::init_seams();
        init_seams();
    });
}

#[test]
fn absolute_location_forms() {
    setup();
    assert_eq!(
        absolute_config_location("/etc/pg/hba.conf", None),
        PathBuf::from("/etc/pg/hba.conf")
    );
    assert_eq!(
        absolute_config_location("conf.d", Some(Path::new("/etc/pg/postgresql.conf"))),
        PathBuf::from("/etc/pg/conf.d")
    );
    assert_eq!(
        absolute_config_location("../shared/extra.conf", Some(Path::new("/etc/pg/postgresql.conf"))),
        PathBuf::from("/etc/shared/extra.conf")
    );

    // path.c:488: a relative calling file (ALTER SYSTEM's
    // "postgresql.auto.conf") keeps an irreducible leading "..".
    assert_eq!(
        absolute_config_location("../parent.conf", Some(Path::new("postgresql.auto.conf"))),
        PathBuf::from("../parent.conf")
    );
    assert_eq!(
        absolute_config_location("../dir/../p.conf", Some(Path::new("postgresql.auto.conf"))),
        PathBuf::from("../p.conf")
    );
    assert_eq!(
        absolute_config_location("../../x.conf", Some(Path::new("/postgresql.conf"))),
        PathBuf::from("/x.conf")
    );

    init_small::globals::SetDataDir("/var/lib/pgdata");
    assert_eq!(
        absolute_config_location("postgresql.auto.conf", None),
        PathBuf::from("/var/lib/pgdata/postgresql.auto.conf")
    );

    assert_eq!(
        conffiles_seams::absolute_config_location::call(
            "conf.d".to_string(),
            Some(PathBuf::from("/etc/pg/postgresql.conf")),
        ),
        PathBuf::from("/etc/pg/conf.d")
    );
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("conffiles_test_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn conf_files_filtered_and_sorted() {
    setup();
    let dir = tempdir("filter");
    for f in ["b.conf", "a.conf", "notes.txt", ".hidden.conf", "x.conf.bak"] {
        std::fs::write(dir.join(f), "").unwrap();
    }
    // A 5-byte name (bare ".conf" is dot-rejected; "1.cnf" wrong suffix) and a
    // directory named like a conf file, both skipped.
    std::fs::write(dir.join("1.cnf"), "").unwrap();
    std::fs::create_dir(dir.join("sub.conf")).unwrap();

    let out = get_conf_files_in_dir(dir.to_str().unwrap(), None, ERROR).unwrap();
    assert_eq!(out.err_msg, None);
    assert_eq!(
        out.filenames,
        vec![dir.join("a.conf"), dir.join("b.conf")]
    );

    let out = conffiles_seams::get_conf_files_in_dir::call(
        dir.to_str().unwrap().to_string(),
        None,
        ERROR,
    )
    .unwrap();
    assert_eq!(out.filenames.len(), 2);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn relative_includedir_resolves_from_calling_file() {
    setup();
    let dir = tempdir("relative");
    std::fs::create_dir(dir.join("conf.d")).unwrap();
    std::fs::write(dir.join("conf.d/z.conf"), "").unwrap();

    let calling = dir.join("postgresql.conf");
    let out = get_conf_files_in_dir("conf.d", Some(&calling), ERROR).unwrap();
    assert_eq!(out.filenames, vec![dir.join("conf.d/z.conf")]);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn error_surface_matches_c() {
    setup();
    let err = get_conf_files_in_dir("   ", None, ERROR).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message(),
        "empty configuration directory name: \"   \""
    );

    let out = get_conf_files_in_dir("\t\r\n", None, LOG).unwrap();
    assert!(out.filenames.is_empty());
    assert_eq!(
        out.err_msg.as_deref(),
        Some("empty configuration directory name")
    );

    let missing = "/nonexistent_conffiles_test_dir";
    let err = get_conf_files_in_dir(missing, None, ERROR).unwrap_err();
    assert!(err
        .message()
        .starts_with("could not open configuration directory \"/nonexistent_conffiles_test_dir\""));

    let out = get_conf_files_in_dir(missing, None, LOG).unwrap();
    assert_eq!(
        out.err_msg.as_deref(),
        Some("could not open directory \"/nonexistent_conffiles_test_dir\"")
    );
}

#[cfg(unix)]
#[test]
fn subdirectory_is_skipped_by_dirent_type_without_stat() {
    use std::os::unix::fs::PermissionsExt;
    setup();
    let dir = tempdir("noexec");
    std::fs::create_dir(dir.join("sub.conf")).unwrap();
    // No search permission: stat of the entries fails, d_type still answers.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o444)).unwrap();
    let out = get_conf_files_in_dir(dir.to_str().unwrap(), None, LOG);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let out = out.unwrap();
    assert_eq!(out.err_msg, None);
    assert!(out.filenames.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

// Emit-log-hook capture; the hook is per thread and one test installs it.
static EMITTED: std::sync::Mutex<Vec<types_error::PgError>> = std::sync::Mutex::new(Vec::new());

fn capture_emitted(error: &types_error::PgError, _output_to_server: &mut bool) {
    EMITTED.lock().unwrap_or_else(|e| e.into_inner()).push(error.clone());
}

#[test]
fn soft_error_runs_error_context_callbacks() {
    setup();
    let callback = elog::push_emit_context_callback(Box::new(|e| {
        e.add_context_line("line 7 of configuration file \"/x/pg_hba.conf\"");
    }));
    let prev = elog::set_emit_log_hook(Some(capture_emitted));
    let out = get_conf_files_in_dir("/nonexistent_conffiles_ctx_dir", None, LOG).unwrap();
    elog::set_emit_log_hook(prev);
    elog::pop_emit_context_callback(callback);
    assert!(out.err_msg.is_some());
    let emitted = std::mem::take(&mut *EMITTED.lock().unwrap_or_else(|e| e.into_inner()));
    assert_eq!(emitted.len(), 1);
    assert!(emitted[0]
        .message()
        .starts_with("could not open configuration directory \"/nonexistent_conffiles_ctx_dir\""));
    assert_eq!(
        emitted[0].context(),
        Some("line 7 of configuration file \"/x/pg_hba.conf\"")
    );
}
