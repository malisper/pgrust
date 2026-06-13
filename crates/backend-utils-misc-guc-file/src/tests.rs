//! Unit tests for the config-file scanner/parser.
//!
//! `ParseConfigFp` / `DeescapeQuotedString` and the tokenizer need no external
//! state. The include-driven tests exercise `ParseConfigFile` /
//! `ParseConfigDirectory`, which resolve paths through the `conffiles` seams;
//! we install test implementations of those (a once-per-process install,
//! since seams are process-global `OnceLock`s).

use super::*;
use backend_utils_misc_conffiles_seams::ConfFilesInDir;
use std::sync::Once;
use types_error::WARNING;

static INSTALL_CONFFILES: Once = Once::new();

/// Install filesystem-backed `conffiles` seams once for the include tests.
/// `AbsoluteConfigLocation`: an absolute path is returned unchanged; a relative
/// one is resolved against the calling file's directory (the C top-level
/// `DataDir` case never arises in these tests, which always pass an absolute
/// top-level path). `GetConfFilesInDir`: the `*.conf` files in the directory,
/// sorted, mirroring conffiles.c.
fn install_conffiles_seams() {
    INSTALL_CONFFILES.call_once(|| {
        absolute_config_location::set(|location, calling_file| {
            let p = PathBuf::from(&location);
            if p.is_absolute() {
                return p;
            }
            match calling_file.and_then(|f| f.parent().map(Path::to_path_buf)) {
                Some(dir) => dir.join(p),
                None => p,
            }
        });
        get_conf_files_in_dir::set(|includedir, calling_file, _elevel| {
            let dir = {
                let p = PathBuf::from(&includedir);
                if p.is_absolute() {
                    p
                } else {
                    match calling_file.and_then(|f| f.parent().map(Path::to_path_buf)) {
                        Some(d) => d.join(p),
                        None => p,
                    }
                }
            };
            let mut filenames: Vec<PathBuf> = match std::fs::read_dir(&dir) {
                Ok(rd) => rd
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.len() >= 6 && !n.starts_with('.') && n.ends_with(".conf"))
                    })
                    .collect(),
                Err(_) => {
                    return Ok(ConfFilesInDir {
                        filenames: Vec::new(),
                        err_msg: Some(format!("could not open directory \"{}\"", dir.display())),
                    })
                }
            };
            filenames.sort();
            Ok(ConfFilesInDir {
                filenames,
                err_msg: None,
            })
        });
    });
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pgrust-guc-file-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn deescape_quoted_string_matches_postgres_rules() {
    assert_eq!(DeescapeQuotedString("'simple'"), "simple");
    assert_eq!(DeescapeQuotedString("'it''s'"), "it's");
    assert_eq!(DeescapeQuotedString(r"'\n\t\141\\'"), "\n\ta\\");
}

#[test]
fn parse_config_fp_accepts_assignments_and_comments() {
    let mut vars = Vec::new();
    let ok = ParseConfigFp(
        "shared_buffers = 128MB # comment\ncustom.name 'value'\nport 5432\n",
        Path::new("/tmp/postgresql.conf"),
        0,
        ERROR,
        &mut vars,
    )
    .unwrap();
    assert!(ok);
    assert_eq!(vars.len(), 3);
    assert_eq!(vars[0].name.as_deref(), Some("shared_buffers"));
    assert_eq!(vars[0].value.as_deref(), Some("128MB"));
    assert_eq!(vars[1].name.as_deref(), Some("custom.name"));
    assert_eq!(vars[1].value.as_deref(), Some("value"));
    assert_eq!(vars[2].sourceline, 3);
}

#[test]
fn parse_config_file_handles_include_directives() {
    install_conffiles_seams();
    let dir = temp_dir("include");
    std::fs::write(dir.join("child.conf"), "work_mem = '4MB'\n").unwrap();
    std::fs::write(
        dir.join("postgresql.conf"),
        "include = 'child.conf'\nmissing_ok = yes\n",
    )
    .unwrap();

    let mut vars = Vec::new();
    let top = dir.join("postgresql.conf");
    let ok = ParseConfigFile(
        top.to_str().unwrap(),
        true,
        None,
        0,
        CONF_FILE_START_DEPTH,
        ERROR,
        &mut vars,
    )
    .unwrap();
    assert!(ok);
    assert_eq!(vars.len(), 2);
    assert_eq!(vars[0].name.as_deref(), Some("work_mem"));
    assert_eq!(vars[1].name.as_deref(), Some("missing_ok"));
}

#[test]
fn parse_config_file_handles_include_dir_in_sorted_order() {
    install_conffiles_seams();
    let dir = temp_dir("include-dir");
    let confd = dir.join("conf.d");
    std::fs::create_dir(&confd).unwrap();
    std::fs::write(confd.join("b.conf"), "b = 2\n").unwrap();
    std::fs::write(confd.join("a.conf"), "a = 1\n").unwrap();
    std::fs::write(dir.join("postgresql.conf"), "include_dir = 'conf.d'\n").unwrap();

    let mut vars = Vec::new();
    let top = dir.join("postgresql.conf");
    assert!(ParseConfigFile(
        top.to_str().unwrap(),
        true,
        None,
        0,
        CONF_FILE_START_DEPTH,
        ERROR,
        &mut vars,
    )
    .unwrap());
    assert_eq!(vars[0].name.as_deref(), Some("a"));
    assert_eq!(vars[1].name.as_deref(), Some("b"));
}

#[test]
fn parse_errors_are_recorded_below_error() {
    let mut vars = Vec::new();
    let ok = ParseConfigFp(
        "good = 1\nbad = \n",
        Path::new("/tmp/postgresql.conf"),
        0,
        WARNING,
        &mut vars,
    )
    .unwrap();
    assert!(!ok);
    assert_eq!(vars.len(), 2);
    assert_eq!(vars[1].errmsg.as_deref(), Some("syntax error"));
    assert!(vars[1].ignore);
}

#[test]
fn parse_errors_throw_at_error_level() {
    let error = ParseConfigFp(
        "bad = \n",
        Path::new("/tmp/postgresql.conf"),
        0,
        ERROR,
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(error.message().contains("near end of line"));
}

#[test]
fn qualified_id_is_allowed_for_name_but_not_value() {
    let mut vars = Vec::new();
    let ok = ParseConfigFp(
        "custom.name = on\nbad = custom.value\n",
        Path::new("/tmp/postgresql.conf"),
        0,
        WARNING,
        &mut vars,
    )
    .unwrap();
    assert!(!ok);
    assert_eq!(vars[0].name.as_deref(), Some("custom.name"));
    assert_eq!(vars[1].errmsg.as_deref(), Some("syntax error"));
}

#[test]
fn free_config_variables_clears_list() {
    let mut vars = vec![ConfigVariable::setting(
        "x".into(),
        "1".into(),
        PathBuf::from("/tmp/x.conf"),
        1,
    )];
    FreeConfigVariables(&mut vars);
    assert!(vars.is_empty());
}
