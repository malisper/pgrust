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

// upstream abb5825550a8 (18.5): Clean up quoting of variable strings within replication commands.
#[test]
fn quoted_identifier_preserves_case_and_folds_doubled_dquote() {
    assert_eq!(lex("\"FooBar\""), vec![Token::Ident("FooBar".into())]);
    // <xd>{xddouble} (18.5+): `"a""b"` is ONE identifier a"b.
    assert_eq!(lex("\"a\"\"b\""), vec![Token::Ident("a\"b".into())]);
    assert_eq!(lex("\"\"\"\""), vec![Token::Ident("\"".into())]);
    assert_eq!(lex("\"\""), vec![Token::Ident(String::new())]);
    assert_eq!(
        lex("\"a\" \"b\""),
        vec![Token::Ident("a".into()), Token::Ident("b".into())]
    );
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
fn recptr_hex_overflow_truncates_like_sscanf() {
    // sscanf("%X") never fails on lexer-matched hex runs: strtoul saturates
    // at ULONG_MAX past 2^64 and the uint32 store truncates (LP64). The
    // original port errored here; repl_scanner_diff caught the divergence
    // against the real flex+libc oracle (2026-08-01).
    assert_eq!(
        lex("100000000/0"),
        vec![Token::Recptr(0)] // 0x1_0000_0000 as u32 == 0
    );
    assert_eq!(
        lex("FFFFFFFFF/1"),
        vec![Token::Recptr(0xFFFF_FFFF_0000_0001)]
    );
    assert_eq!(
        lex("FFFFFFFFFFFFFFFFF/0"), // > 2^64: strtoul saturation
        vec![Token::Recptr(0xFFFF_FFFF_0000_0000)]
    );
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

// scansup.c:53 (`pg_database_encoding_max_length() == 1`) and scansup.c:97
// (`pg_mbcliplen`, mbutils.c:1209 -> DatabaseEncoding) read the DATABASE
// encoding: in a SQL_ASCII database (max length 1) C clips a >= NAMEDATALEN
// identifier at exactly NAMEDATALEN-1 bytes, never backing off to a UTF-8
// character boundary. `pg_mbcliplen` keeps 62 'a' + the lone 0xC3 lead byte
// of "é" (63 bytes) -- bytes the UTF-8-only engine cannot carry, so the
// ratified SQL_ASCII enforcement (docs/design/carve-ratifications.md §11:
// scan_fgram utf8_pin spelling) is the 0A000 refusal, never a silently
// shorter identifier (the pre-fix scanner clipped at the UTF-8 boundary,
// 62 bytes, and the slot got CREATED under a name C rejects).
fn with_database_encoding<R>(enc: wchar::pg_enc, body: impl FnOnce() -> R) -> R {
    let saved = mbutils::GetDatabaseEncoding();
    mbutils::SetDatabaseEncoding(enc).unwrap();
    let r = body();
    mbutils::SetDatabaseEncoding(saved).unwrap();
    r
}

fn straddle_input(quoted: bool) -> String {
    // 62 'a' + "é" (0xC3 0xA9) = 64 bytes = NAMEDATALEN.
    let body = format!("{}\u{e9}", "a".repeat(62));
    assert_eq!(body.len(), 64);
    if quoted {
        format!("\"{body}\"")
    } else {
        body
    }
}

#[test]
fn sql_ascii_truncation_clips_at_namedatalen_minus_one_bytes() {
    let expected_msg = "query strings with non-ASCII characters are not supported yet in \
                        databases with encoding \"SQL_ASCII\"";
    with_database_encoding(wchar::PG_SQL_ASCII, || {
        // <xd>{xdstop}: repl_scanner.l:196 truncate_identifier(..., true).
        let err = replication_lex_all(&straddle_input(true))
            .expect_err("C keeps 63 bytes incl. a lone 0xC3: not representable, must refuse");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), expected_msg);
        assert_eq!(err.hint(), Some("Use a database with encoding \"UTF8\"."));

        // {identifier}: repl_scanner.l:211 downcase_truncate_identifier(..., true).
        let err = replication_lex_all(&straddle_input(false))
            .expect_err("unquoted arm clips the same way (scansup.c:60 -> truncate_identifier)");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), expected_msg);
    });
}

#[test]
fn sql_ascii_truncation_on_a_character_boundary_keeps_the_character() {
    // 61 'a' + "é" + 'x' = 65 bytes: byte 63 ends "é" in both encodings, so
    // C and the UTF-8 arm agree on 61 'a' + "é" (live C 18.6 witness).
    let input = format!("\"{}\u{e9}x\"", "a".repeat(61));
    let want = Token::Ident(format!("{}\u{e9}", "a".repeat(61)));
    with_database_encoding(wchar::PG_SQL_ASCII, || {
        assert_eq!(lex(&input), vec![want.clone()]);
    });
    with_database_encoding(wchar::PG_UTF8, || {
        assert_eq!(lex(&input), vec![want.clone()]);
    });
}

#[test]
fn utf8_truncation_backs_off_to_the_character_boundary() {
    // UTF8 database: pg_mbcliplen drops the whole "é" (would end at byte 64).
    with_database_encoding(wchar::PG_UTF8, || {
        assert_eq!(
            lex(&straddle_input(true)),
            vec![Token::Ident("a".repeat(62))]
        );
        assert_eq!(
            lex(&straddle_input(false)),
            vec![Token::Ident("a".repeat(62))]
        );
    });
}
