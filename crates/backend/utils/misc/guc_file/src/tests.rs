//! Unit tests for the config-file scanner/parser.
//!
//! `ParseConfigFp` / `DeescapeQuotedString` and the tokenizer need no external
//! state. The include-driven tests exercise `ParseConfigFile` /
//! `ParseConfigDirectory`, which resolve paths through the `conffiles` seams;
//! we install test implementations of those (a once-per-process install,
//! since seams are process-global `OnceLock`s).

use super::*;
use ::conffiles_seams::ConfFilesInDir;
use std::sync::Once;
use ::types_error::WARNING;

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
        "shared_buffers = 128MB # comment\ncustom.name 'value'\nport 5432\n".as_bytes(),
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
        "good = 1\nbad = \n".as_bytes(),
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
        "bad = \n".as_bytes(),
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
        "custom.name = on\nbad = custom.value\n".as_bytes(),
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
fn parses_non_utf8_bytes() {
    // The flex scanner is `%option 8bit`; high-bit bytes \200-\377 are valid
    // `LETTER`s. A latin-1 value (0xE9 = é) must parse, not be rejected as
    // unreadable.
    let mut vars = Vec::new();
    let contents: &[u8] = b"name = caf\xe9\n";
    let ok = ParseConfigFp(
        contents,
        Path::new("/tmp/postgresql.conf"),
        0,
        WARNING,
        &mut vars,
    )
    .unwrap();
    assert!(ok);
    assert_eq!(vars.len(), 1);
    assert_eq!(vars[0].name.as_deref(), Some("name"));
    // The 0xE9 byte classifies as an unquoted-string LETTER and is stored
    // lossily (the ConfigVariable value is a UTF-8 String).
    assert!(vars[0].value.as_deref().unwrap().starts_with("caf"));
}

/// flex tokenizes by maximal munch over the *regex char classes*, not by
/// splitting on whitespace/`=`/`#`. A value run that contains a char outside
/// the UNQUOTED_STRING class (here `,`) must tokenize as the leading valid
/// value followed by a separate error token, so the `near token` diagnostic
/// names the offending byte the same way flex does (`","`), not the whole run.
#[test]
fn tokenizer_splits_value_run_at_unquoted_class_boundary() {
    let mut vars = Vec::new();
    let err = ParseConfigFp(
        "name = a,b\n".as_bytes(),
        Path::new("/tmp/postgresql.conf"),
        0,
        ERROR,
        &mut vars,
    )
    .unwrap_err();
    // flex: value UNQUOTED_STRING `a`, then `,` -> GUC_ERROR (extra token).
    assert!(
        err.message().contains("near token \",\""),
        "message was: {}",
        err.message()
    );
}

/// flex maximal munch on the bare `'''` (three quotes): the longest STRING that
/// reaches a closing quote is `''` (an empty string), leaving the third `'` to
/// the catch-all `.` as a separate GUC_ERROR. The earlier hand-scanner greedily
/// treated `''` as an embedded doubled quote and ran off the end.
#[test]
fn tokenizer_string_takes_shortest_terminating_match() {
    let mut lexer = Lexer::new(b"'''");
    let first = lexer.next_token().unwrap();
    assert_eq!(first.kind, TokenKind::String);
    assert_eq!(first.text, "''");
    let second = lexer.next_token().unwrap();
    assert_eq!(second.kind, TokenKind::Error);
    assert_eq!(second.text, "'");
    assert!(lexer.next_token().is_none());
}

/// An escaped quote inside a string is body content (`\\.`), and a doubled `''`
/// only stays inside when the string still terminates. `'a''b'` is one STRING
/// whose body is `a''b`.
#[test]
fn tokenizer_string_doubled_quote_when_terminating() {
    let mut lexer = Lexer::new(b"'a''b' rest");
    let tok = lexer.next_token().unwrap();
    assert_eq!(tok.kind, TokenKind::String);
    assert_eq!(tok.text, "'a''b'");
    assert_eq!(DeescapeQuotedString(&tok.text), "a'b");
}

/// `1e5` is not a REAL (flex REAL requires a literal `.`): flex tokenizes it as
/// INTEGER `1e` (digit then UNIT_LETTER) followed by `5`. The mantissa-then-unit
/// boundary must match so the value/diagnostic match flex.
#[test]
fn tokenizer_integer_unit_letters_without_dot() {
    let mut lexer = Lexer::new(b"1e5");
    let first = lexer.next_token().unwrap();
    assert_eq!(first.kind, TokenKind::Integer);
    assert_eq!(first.text, "1e");
    let second = lexer.next_token().unwrap();
    assert_eq!(second.kind, TokenKind::Integer);
    assert_eq!(second.text, "5");
}

/// A bare exponent letter with no exponent digits is not consumed by REAL: `1.5e`
/// is REAL `1.5` then a separate `e` (UNQUOTED_STRING / ID), matching flex's
/// `{EXPONENT}?` only-if-it-fully-matches behavior.
#[test]
fn tokenizer_real_does_not_swallow_bare_exponent() {
    let mut lexer = Lexer::new(b"1.5e");
    let first = lexer.next_token().unwrap();
    assert_eq!(first.kind, TokenKind::Real);
    assert_eq!(first.text, "1.5");
    let second = lexer.next_token().unwrap();
    assert_eq!(second.kind, TokenKind::Id);
    assert_eq!(second.text, "e");
}

/// `0x` with no hex digits is not an INTEGER; flex matches ID `x`? No — `0` is
/// not a LETTER, so `0` falls to the catch-all and `x` is a separate ID. Verify
/// the INTEGER rule rejects the bare `0x` prefix at this position.
#[test]
fn tokenizer_rejects_bare_0x() {
    let mut lexer = Lexer::new(b"0xZ");
    let first = lexer.next_token().unwrap();
    // `0` matches no token rule -> catch-all GUC_ERROR, one byte.
    assert_eq!(first.kind, TokenKind::Error);
    assert_eq!(first.text, "0");
}

/// QUALIFIED_ID matches exactly one dot; a three-part dotted run is longer as an
/// UNQUOTED_STRING (`.` is in its extra class), so flex picks UNQUOTED_STRING and
/// it is rejected as a name. Confirms the longest-match-then-rule-order pick.
#[test]
fn tokenizer_three_part_dotted_is_unquoted_string() {
    let mut lexer = Lexer::new(b"a.b.c");
    let tok = lexer.next_token().unwrap();
    assert_eq!(tok.kind, TokenKind::UnquotedString);
    assert_eq!(tok.text, "a.b.c");
}

/// A hex integer with trailing unit letters keeps the whole run: `0x1fkB` is
/// INTEGER `0x1f` + UNIT_LETTER* `kB`.
#[test]
fn tokenizer_hex_integer_with_units() {
    let mut lexer = Lexer::new(b"0x1fkB");
    let tok = lexer.next_token().unwrap();
    assert_eq!(tok.kind, TokenKind::Integer);
    assert_eq!(tok.text, "0x1fkB");
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
