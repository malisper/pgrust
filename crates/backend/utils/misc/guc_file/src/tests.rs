use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Once;

use types_error::{ErrorLevel, ERROR, LOG};
use types_guc::PGC_SIGHUP;

use crate::*;

thread_local! {
    static INTERNAL_CALLS: Cell<u32> = const { Cell::new(0) };
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();
        conffiles_seams::absolute_config_location::set(|location, calling_file| {
            let p = Path::new(&location);
            if p.is_absolute() {
                p.to_path_buf()
            } else if let Some(calling) = calling_file {
                calling.parent().unwrap_or(Path::new(".")).join(p)
            } else {
                p.to_path_buf()
            }
        });
        conffiles_seams::get_conf_files_in_dir::set(|includedir, calling_file, _elevel| {
            let dir = if Path::new(&includedir).is_absolute() {
                PathBuf::from(&includedir)
            } else if let Some(calling) = calling_file {
                calling.parent().unwrap_or(Path::new(".")).join(&includedir)
            } else {
                PathBuf::from(&includedir)
            };
            let mut out = conffiles_seams::ConfFilesInDir::default();
            match std::fs::read_dir(&dir) {
                Ok(entries) => {
                    for e in entries.flatten() {
                        let p = e.path();
                        if p.extension().and_then(|x| x.to_str()) == Some("conf") {
                            out.filenames.push(p);
                        }
                    }
                    out.filenames.sort();
                }
                Err(_) => {
                    out.err_msg =
                        Some(format!("could not open directory \"{}\"", dir.display()));
                }
            }
            Ok(out)
        });
        guc_seams::process_config_file_internal::set(|_context, _apply, _elevel| {
            INTERNAL_CALLS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
    });
}

fn parse_str(contents: &str, elevel: ErrorLevel) -> (bool, Vec<ConfigVariable>) {
    setup();
    let mut vars = Vec::new();
    let ok = ParseConfigFp(
        contents.as_bytes(),
        Path::new("/tmp/postgresql.conf"),
        CONF_FILE_START_DEPTH,
        elevel,
        &mut vars,
    )
    .unwrap();
    (ok, vars)
}

fn settings(vars: &[ConfigVariable]) -> Vec<(String, String)> {
    vars.iter()
        .filter(|v| !v.ignore)
        .map(|v| (v.name.clone().unwrap(), v.value.clone().unwrap()))
        .collect()
}

#[test]
fn parses_basic_forms() {
    let (ok, vars) = parse_str(
        "work_mem = 8MB\n\
         shared_buffers 128MB\n\
         # a comment\n\
         \n\
         port = 5432   # trailing comment\n\
         cursor_tuple_fraction = 0.5\n\
         geqo = on\n\
         search_path = 'a, b'\n\
         listen_addresses = localhost\n\
         log_line_prefix = ''\n\
         datadir = var/lib:pg-x_1.d\n",
        LOG,
    );
    assert!(ok);
    assert_eq!(
        settings(&vars),
        vec![
            ("work_mem".into(), "8MB".into()),
            ("shared_buffers".into(), "128MB".into()),
            ("port".into(), "5432".into()),
            ("cursor_tuple_fraction".into(), "0.5".into()),
            ("geqo".into(), "on".into()),
            ("search_path".into(), "a, b".into()),
            ("listen_addresses".into(), "localhost".into()),
            ("log_line_prefix".into(), "".into()),
            ("datadir".into(), "var/lib:pg-x_1.d".into()),
        ]
    );
    assert_eq!(vars[0].sourceline, 1);
    assert_eq!(vars[2].sourceline, 5);
}

#[test]
fn qualified_ids_and_numbers() {
    let (ok, vars) = parse_str(
        "my.custom = 'x'\n\
         a.b = -42\n\
         c.d = +3.5e-2\n\
         e.f = 0xFF\n\
         g.h = 10min\n",
        LOG,
    );
    assert!(ok);
    assert_eq!(
        settings(&vars),
        vec![
            ("my.custom".into(), "x".into()),
            ("a.b".into(), "-42".into()),
            ("c.d".into(), "+3.5e-2".into()),
            ("e.f".into(), "0xFF".into()),
            ("g.h".into(), "10min".into()),
        ]
    );
}

#[test]
fn deescape_quoted_string() {
    assert_eq!(DeescapeQuotedString("'abc'"), "abc");
    assert_eq!(DeescapeQuotedString("'a''b'"), "a'b");
    assert_eq!(DeescapeQuotedString("'a\\'b'"), "a'b");
    assert_eq!(DeescapeQuotedString("'a\\n\\t\\b\\f\\r'"), "a\n\t\u{8}\u{c}\r");
    assert_eq!(DeescapeQuotedString("'\\101'"), "A");
    assert_eq!(DeescapeQuotedString("'\\x'"), "x");
    assert_eq!(DeescapeQuotedString("''"), "");
}

#[test]
fn syntax_errors_recorded_below_error() {
    let (ok, vars) = parse_str("work_mem = = 8MB\n", LOG);
    assert!(!ok);
    assert_eq!(vars.len(), 1);
    assert!(vars[0].ignore);
    assert_eq!(vars[0].errmsg.as_deref(), Some("syntax error"));
    assert_eq!(vars[0].sourceline, 1);

    let (ok, vars) = parse_str("work_mem\n", LOG);
    assert!(!ok);
    assert!(vars[0].ignore);

    // A value token cannot be a qualified id.
    let (ok, _) = parse_str("work_mem = a.b\n", LOG);
    assert!(!ok);

    // Unterminated quote lexes as GUC_ERROR.
    let (ok, _) = parse_str("work_mem = 'oops\n", LOG);
    assert!(!ok);

    // Errors after a good line don't lose it.
    let (ok, vars) = parse_str("work_mem = 4MB\n???\n", LOG);
    assert!(!ok);
    assert_eq!(settings(&vars), vec![("work_mem".into(), "4MB".into())]);
}

#[test]
fn syntax_error_throws_at_error_level() {
    setup();
    let mut vars = Vec::new();
    let e = ParseConfigFp(b"= nonsense\n", Path::new("/tmp/x.conf"), 0, ERROR, &mut vars)
        .unwrap_err();
    assert!(e.message().contains("syntax error in file"));
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("guc_file_test_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn include_directives() {
    setup();
    let dir = scratch_dir("include");
    std::fs::write(dir.join("main.conf"), "work_mem = 1MB\ninclude 'sub.conf'\ninclude_if_exists 'missing.conf'\ninclude_dir 'conf.d'\n").unwrap();
    std::fs::write(dir.join("sub.conf"), "shared_buffers = 128MB\n").unwrap();
    std::fs::create_dir_all(dir.join("conf.d")).unwrap();
    std::fs::write(dir.join("conf.d/b.conf"), "port = 5433\n").unwrap();
    std::fs::write(dir.join("conf.d/a.conf"), "port = 5432\n").unwrap();
    std::fs::write(dir.join("conf.d/ignored.txt"), "port = 9999\n").unwrap();

    let mut vars = Vec::new();
    let ok = ParseConfigFile(
        dir.join("main.conf").to_str().unwrap(),
        true,
        None,
        0,
        CONF_FILE_START_DEPTH,
        LOG,
        &mut vars,
    )
    .unwrap();
    assert!(ok);
    assert_eq!(
        settings(&vars),
        vec![
            ("work_mem".into(), "1MB".into()),
            ("shared_buffers".into(), "128MB".into()),
            ("port".into(), "5432".into()),
            ("port".into(), "5433".into()),
        ]
    );
}

#[test]
fn include_recursion_rejected() {
    setup();
    let dir = scratch_dir("recur");
    let loop_conf = dir.join("loop.conf");
    std::fs::write(&loop_conf, "include 'loop.conf'\n").unwrap();

    let mut vars = Vec::new();
    let ok = ParseConfigFile(loop_conf.to_str().unwrap(), true, None, 0, 0, LOG, &mut vars).unwrap();
    assert!(!ok);
    assert!(vars
        .iter()
        .any(|v| v.errmsg.as_deref() == Some("configuration file recursion")));
}

#[test]
fn missing_strict_include_fails_and_if_exists_skips() {
    setup();
    let dir = scratch_dir("missing");
    let main = dir.join("main.conf");

    std::fs::write(&main, "include 'nope.conf'\n").unwrap();
    let mut vars = Vec::new();
    let ok = ParseConfigFile(main.to_str().unwrap(), true, None, 0, 0, LOG, &mut vars).unwrap();
    assert!(!ok);
    assert!(vars.iter().any(|v| v.ignore));

    std::fs::write(&main, "include_if_exists 'nope.conf'\nwork_mem = 2MB\n").unwrap();
    let mut vars = Vec::new();
    let ok = ParseConfigFile(main.to_str().unwrap(), true, None, 0, 0, LOG, &mut vars).unwrap();
    assert!(ok);
    assert_eq!(settings(&vars), vec![("work_mem".into(), "2MB".into())]);
}

#[test]
fn empty_file_name_rejected() {
    setup();
    let mut vars = Vec::new();
    let ok = ParseConfigFile("  \t", true, None, 0, 0, LOG, &mut vars).unwrap();
    assert!(!ok);
    assert_eq!(vars[0].errmsg.as_deref(), Some("empty configuration file name"));
}

#[test]
fn nesting_depth_limit() {
    setup();
    let mut vars = Vec::new();
    let ok =
        ParseConfigFile("x.conf", true, None, 0, CONF_FILE_MAX_DEPTH + 1, LOG, &mut vars).unwrap();
    assert!(!ok);
    assert_eq!(vars[0].errmsg.as_deref(), Some("nesting depth exceeded"));
}

#[test]
fn non_utf8_bytes_parse() {
    // %option 8bit: \200-\377 are LETTERs.
    let (ok, vars) = parse_str_bytes(b"caf\xe9 = '\xff'\n");
    assert!(ok);
    assert_eq!(vars.len(), 1);
    assert!(!vars[0].ignore);
}

fn parse_str_bytes(contents: &[u8]) -> (bool, Vec<ConfigVariable>) {
    setup();
    let mut vars = Vec::new();
    let ok = ParseConfigFp(contents, Path::new("/tmp/b.conf"), 0, LOG, &mut vars).unwrap();
    (ok, vars)
}

#[test]
fn process_config_file_routes_through_guc_seam() {
    setup();
    let before = INTERNAL_CALLS.with(Cell::get);
    guc_file_seams::process_config_file::call(PGC_SIGHUP).unwrap();
    assert_eq!(INTERNAL_CALLS.with(Cell::get), before + 1);
}
