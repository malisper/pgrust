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
