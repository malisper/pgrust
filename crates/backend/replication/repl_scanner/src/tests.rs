//! Unit tests for the `repl_scanner.l` lexer.

use super::*;

fn lex(input: &str) -> Vec<Token> {
    let mut toks = replication_lex_all(input).expect("no OOM/lex error in this test");
    assert_eq!(toks.last(), Some(&Token::Eof), "stream must end in Eof");
    toks.pop();
    toks
}

#[test]
fn empty_input_is_just_eof() {
    assert_eq!(replication_lex_all("").unwrap(), vec![Token::Eof]);
    assert_eq!(replication_lex_all("   \t\n").unwrap(), vec![Token::Eof]);
}

#[test]
fn keywords_are_case_sensitive_exact() {
    assert_eq!(lex("IDENTIFY_SYSTEM"), vec![Token::IdentifySystem]);
    assert_eq!(lex("BASE_BACKUP"), vec![Token::BaseBackup]);
    assert_eq!(lex("START_REPLICATION"), vec![Token::StartReplication]);
    assert_eq!(lex("TIMELINE_HISTORY"), vec![Token::TimelineHistory]);
    assert_eq!(lex("UPLOAD_MANIFEST"), vec![Token::UploadManifest]);
    // Lowercase is NOT the keyword -- it folds to an IDENT.
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
    assert_eq!(lex("16/B374D848"), vec![Token::Recptr(0x16_B374D848)]);
    assert_eq!(lex("0/0"), vec![Token::Recptr(0)]);
    assert_eq!(lex("FF/1"), vec![Token::Recptr(0xFF_0000_0001)]);
}

#[test]
fn hex_run_without_slash_is_identifier() {
    // A hex run with letters not followed by `/...` falls through to
    // `{identifier}` (hex letters are ident_cont) and downcases.
    assert_eq!(lex("ABC"), vec![Token::Ident("abc".into())]);
    // A leading-digit run that isn't all-decimal: digits aren't ident_start,
    // so the decimal prefix lexes as UCONST and the rest as its own IDENT.
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
    assert!(replication_lex_all("'abc").is_err());
}

#[test]
fn unterminated_double_quote_errors() {
    assert!(replication_lex_all("\"abc").is_err());
}

#[test]
fn invalid_streaming_location_errors() {
    // sscanf overflow: a hex half that does not fit uint32.
    assert!(replication_lex_all("100000000/0").is_err());
}

#[test]
fn is_replication_command_recognizes_introducers() {
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
