//! Unit tests for the `repl_scanner.l` lexer.
//!
//! Identifier folding goes through the `backend-parser-scansup-seams`
//! (`downcase_truncate_identifier` / `truncate_identifier`), whose real owner
//! (`backend-parser-small1`) is not linked here, so each test installs a faithful
//! no-clip stub: `truncate_identifier` returns the bytes unchanged (no
//! `NAMEDATALEN` clipping, as in a single-backend short-identifier scenario),
//! and `downcase_truncate_identifier` ASCII-lowercases then returns them — the
//! behavior `repl_scanner.l` relies on for unquoted identifiers.

use super::*;

/// Install the `scansup` folding seams with faithful no-clip stubs. A seam slot
/// may be `set` only once per process, and the tests run in parallel, so the
/// installation is guarded by a `Once`.
fn install_scansup_stubs() {
    extern crate std;
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        truncate_identifier::set(|mcx, ident, _warn| mcx::slice_in(mcx, ident));
        downcase_truncate_identifier::set(|mcx, ident, _warn| {
            let lowered: alloc::vec::Vec<u8> =
                ident.iter().map(|b| b.to_ascii_lowercase()).collect();
            mcx::slice_in(mcx, &lowered)
        });
    });
}

/// Lex `input` to completion and return the token stream WITHOUT the terminal
/// `Token::Eof` (which `lex_all` always appends).
fn lex(input: &str) -> Vec<Token> {
    install_scansup_stubs();
    let mut toks = lex_all(input).expect("no OOM/lex error in this test");
    assert_eq!(toks.last(), Some(&Token::Eof), "stream must end in Eof");
    toks.pop();
    toks
}

#[test]
fn empty_input_is_just_eof() {
    install_scansup_stubs();
    assert_eq!(lex_all("").unwrap(), vec![Token::Eof]);
    assert_eq!(lex_all("   \t\n").unwrap(), vec![Token::Eof]);
}

#[test]
fn keywords_are_case_sensitive_exact() {
    assert_eq!(lex("IDENTIFY_SYSTEM"), vec![Token::IdentifySystem]);
    assert_eq!(lex("BASE_BACKUP"), vec![Token::BaseBackup]);
    assert_eq!(lex("START_REPLICATION"), vec![Token::StartReplication]);
    assert_eq!(lex("TIMELINE_HISTORY"), vec![Token::TimelineHistory]);
    assert_eq!(lex("UPLOAD_MANIFEST"), vec![Token::UploadManifest]);
    // Lowercase is NOT the keyword — it folds to an IDENT.
    assert_eq!(
        lex("identify_system"),
        vec![Token::Ident("identify_system".into())]
    );
}

#[test]
fn unquoted_identifier_is_downcased() {
    assert_eq!(lex("Foo_Bar"), vec![Token::Ident("foo_bar".into())]);
    assert_eq!(lex("node$1"), vec![Token::Ident("node$1".into())]);
}

#[test]
fn quoted_identifier_preserves_case_and_collapses_doubled_quote() {
    // `<xd>` state: case preserved, "" -> ".
    assert_eq!(lex("\"FooBar\""), vec![Token::Ident("FooBar".into())]);
    assert_eq!(lex("\"a\"\"b\""), vec![Token::Ident("a\"b".into())]);
}

#[test]
fn single_quoted_string_is_sconst_with_escape_collapse() {
    assert_eq!(lex("'hello'"), vec![Token::Sconst("hello".into())]);
    assert_eq!(lex("'it''s'"), vec![Token::Sconst("it's".into())]);
    assert_eq!(lex("''"), vec![Token::Sconst(String::new())]);
}

#[test]
fn decimal_run_is_uconst() {
    assert_eq!(lex("123"), vec![Token::Uconst(123)]);
    assert_eq!(lex("0"), vec![Token::Uconst(0)]);
}

#[test]
fn hex_slash_hex_is_recptr() {
    // `%X/%X` -> (hi<<32)|lo.
    assert_eq!(lex("16/B374D848"), vec![Token::Recptr(0x16_B374D848)]);
    assert_eq!(lex("0/0"), vec![Token::Recptr(0)]);
    assert_eq!(lex("FF/1"), vec![Token::Recptr(0xFF_0000_0001)]);
}

#[test]
fn hex_run_without_slash_is_identifier() {
    // A hex run with letters not followed by `/...` is NOT a number; it falls
    // through to `{identifier}` (hex letters are ident_cont) and downcases.
    assert_eq!(lex("ABC"), vec![Token::Ident("abc".into())]);
    // A leading-digit hex run that is not all-decimal: digits are not
    // ident_start, so this does not begin an identifier; the decimal prefix
    // lexes as UCONST and the rest as a separate identifier.
    assert_eq!(
        lex("1A"),
        vec![Token::Uconst(1), Token::Ident("a".into())]
    );
}

#[test]
fn single_characters_returned_as_themselves() {
    assert_eq!(
        lex("( , ) . ;"),
        vec![
            Token::Char(b'('),
            Token::Char(b','),
            Token::Char(b')'),
            Token::Char(b'.'),
            Token::Char(b';'),
        ]
    );
}

#[test]
fn full_start_replication_command() {
    assert_eq!(
        lex("START_REPLICATION SLOT \"my_slot\" LOGICAL 16/B374D848"),
        vec![
            Token::StartReplication,
            Token::Slot,
            Token::Ident("my_slot".into()),
            Token::Logical,
            Token::Recptr(0x16_B374D848),
        ]
    );
}

#[test]
fn unterminated_single_quote_errors() {
    install_scansup_stubs();
    assert!(lex_all("'abc").is_err());
}

#[test]
fn unterminated_double_quote_errors() {
    install_scansup_stubs();
    assert!(lex_all("\"abc").is_err());
}

#[test]
fn invalid_streaming_location_errors() {
    // sscanf overflow: a hex half that does not fit uint32.
    install_scansup_stubs();
    assert!(lex_all("100000000/0").is_err());
}

#[test]
fn is_replication_command_recognizes_introducers() {
    install_scansup_stubs();
    for cmd in [
        "IDENTIFY_SYSTEM",
        "BASE_BACKUP",
        "START_REPLICATION 0/0",
        "CREATE_REPLICATION_SLOT s LOGICAL",
        "DROP_REPLICATION_SLOT s",
        "ALTER_REPLICATION_SLOT s",
        "READ_REPLICATION_SLOT s",
        "TIMELINE_HISTORY 1",
        "UPLOAD_MANIFEST",
        "SHOW x",
    ] {
        assert!(is_replication_command(cmd).unwrap(), "{cmd}");
    }
    // A plain SQL command lexes to an IDENT first token -> not a repl command.
    assert!(!is_replication_command("SELECT 1").unwrap());
    assert!(!is_replication_command("").unwrap());
    // TIMELINE (not TIMELINE_HISTORY) is a keyword but not an introducer.
    assert!(!is_replication_command("TIMELINE 1").unwrap());
}
