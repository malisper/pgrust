//! Tests for the idiomatic `backend-tsearch-parse` port.
//!
//! The seams loud-panic by default, so the tests install ASCII / single-byte /
//! SQL_ASCII-correct providers once, single-threaded, before running.  These
//! mirror the fail-safe defaults of the faithful port (the C/POSIX-locale,
//! SQL_ASCII behaviour the C `BackendTsearchParseRuntime` /
//! `BackendTsearchConfigRuntime` defaults provide).
//!
//! The encoding the parser sees and the configuration's dictionary behaviour
//! are switched per-test through thread-locals the seams consult; tests run
//! with `--test-threads=1`, and a global lock serializes the shared seam state.

use super::*;
use crate::seam;
use crate::ts_parse::{DictSubState, LexizeLexeme};
use crate::wparser_def::{
    ASCIIHWORD, ASCIIPARTHWORD, ASCIIWORD, DECIMAL_T, EMAIL, NUMWORD, SIGNEDINT, SPACE, TAG_T,
    UNSIGNEDINT, WORD_T,
};
use backend_utils_error::ereport;
use types_error::{ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERROR};
use std::cell::RefCell;
use std::sync::{Mutex, Once};

static INSTALL: Once = Once::new();
static SEAM_LOCK: Mutex<()> = Mutex::new(());

/// The configuration mode the `config_*` / `dict_lexize` seams emulate.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CfgMode {
    /// Lowercase every word/asciiword/numword token to a single lexeme.
    Simple,
    /// Multiword recognizer for "new york" -> "nyc" (thesaurus-style).
    Thesaurus,
}

thread_local! {
    /// Database encoding the parser sees: `1` = single-byte (ASCII path),
    /// `4` = UTF-8 (wide path).
    static MAX_LEN: RefCell<i32> = const { RefCell::new(1) };
    static DB_ENC: RefCell<i32> = const { RefCell::new(0) };
    /// Whether `char2wchar` should fail (the bad-locale propagation test).
    static CHAR2WCHAR_FAILS: RefCell<bool> = const { RefCell::new(false) };
    /// The active configuration mode for the config/dict seams.
    static CFG_MODE: RefCell<CfgMode> = const { RefCell::new(CfgMode::Simple) };
}

// ---- ASCII / UTF-8 character-class helpers (the C SQL_ASCII / C-locale defaults).

fn ascii_isalpha(c: u32) -> i32 {
    i32::from(matches!(c, 0x41..=0x5a | 0x61..=0x7a))
}
fn ascii_isdigit(c: u32) -> i32 {
    i32::from(matches!(c, 0x30..=0x39))
}
fn ascii_isalnum(c: u32) -> i32 {
    i32::from(ascii_isalpha(c) != 0 || ascii_isdigit(c) != 0)
}
fn ascii_isspace(c: u32) -> i32 {
    i32::from(matches!(c, 0x20 | 0x09 | 0x0a | 0x0b | 0x0c | 0x0d))
}
fn ascii_isxdigit(c: u32) -> i32 {
    i32::from(matches!(c, 0x30..=0x39 | 0x41..=0x46 | 0x61..=0x66))
}

/// UTF-8 lead-byte length (1..=4), defaulting to 1 for invalid lead bytes.
fn utf8_mblen(s: &[u8]) -> i32 {
    match s.first() {
        None => 1,
        Some(&b) if b < 0x80 => 1,
        Some(&b) if b & 0xe0 == 0xc0 => 2,
        Some(&b) if b & 0xf0 == 0xe0 => 3,
        Some(&b) if b & 0xf8 == 0xf0 => 4,
        Some(_) => 1,
    }
}

fn utf8_to_wchar(from: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < from.len() {
        let len = utf8_mblen(&from[i..]) as usize;
        let len = len.min(from.len() - i).max(1);
        let cp = match len {
            1 => from[i] as u32,
            2 => ((from[i] as u32 & 0x1f) << 6) | from.get(i + 1).map_or(0, |&b| b as u32 & 0x3f),
            3 => {
                ((from[i] as u32 & 0x0f) << 12)
                    | (from.get(i + 1).map_or(0, |&b| b as u32 & 0x3f) << 6)
                    | from.get(i + 2).map_or(0, |&b| b as u32 & 0x3f)
            }
            _ => {
                ((from[i] as u32 & 0x07) << 18)
                    | (from.get(i + 1).map_or(0, |&b| b as u32 & 0x3f) << 12)
                    | (from.get(i + 2).map_or(0, |&b| b as u32 & 0x3f) << 6)
                    | from.get(i + 3).map_or(0, |&b| b as u32 & 0x3f)
            }
        };
        out.push(cp);
        i += len;
    }
    out
}

fn report_invalid_encoding(mbstr: &[u8], mblen: i32, len: i32) -> backend_utils_error::PgError {
    let limit = (mblen.max(0) as usize).min(len.max(0) as usize).min(8);
    let bytes = mbstr
        .iter()
        .take(limit)
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    ereport(ERROR)
        .errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
        .errmsg(format!("invalid byte sequence for encoding \"UTF8\": {bytes}"))
        .into_error()
}

// ---- config / dictionary seam emulation.

fn simple_dict_ids(token_type: i32) -> Vec<u32> {
    if token_type == ASCIIWORD || token_type == WORD_T || token_type == NUMWORD {
        vec![101]
    } else {
        vec![]
    }
}

fn simple_lexize(lemm: &[u8], dstate: &mut DictSubState) -> Option<Vec<LexizeLexeme>> {
    // simple-style dictionary: lowercase, one lexeme, no flags.
    dstate.getnext = false;
    Some(vec![LexizeLexeme {
        nvariant: 0,
        flags: 0,
        lexeme: lemm.to_ascii_lowercase(),
    }])
}

fn thesaurus_dict_ids(token_type: i32) -> Vec<u32> {
    if token_type == ASCIIWORD {
        vec![7]
    } else {
        vec![]
    }
}

fn thesaurus_lexize(lemm: &[u8], dstate: &mut DictSubState) -> Option<Vec<LexizeLexeme>> {
    // Multiword recognizer for "new york" -> "nyc".
    if dstate.private_state == 0 {
        if lemm == b"new" {
            dstate.private_state = 1; // stash "saw 'new'"
            dstate.getnext = true;
            return None;
        }
        dstate.getnext = false;
        return None;
    }
    assert_eq!(dstate.private_state, 1, "private_state must thread across calls");
    dstate.getnext = false;
    if lemm == b"york" {
        Some(vec![LexizeLexeme {
            nvariant: 0,
            flags: 0,
            lexeme: b"nyc".to_vec(),
        }])
    } else {
        None
    }
}

fn install_env() {
    INSTALL.call_once(|| {
        // Encoding / locale parameters.
        seam::pg_database_encoding_max_length::set(|| {
            MAX_LEN.with(|m| *m.borrow())
        });
        seam::get_database_encoding::set(|| DB_ENC.with(|e| *e.borrow()));
        seam::database_ctype_is_c::set(|| false);

        // pg_mblen_range: UTF-8 leading-char length with the C bounds check.
        seam::pg_mblen_range::set(|s| {
            let length = utf8_mblen(s);
            if length as usize > s.len() {
                return Err(report_invalid_encoding(s, length, s.len() as i32));
            }
            Ok(length)
        });

        // pg_dsplen: printable ASCII -> 1, control -> -1, multibyte -> 1.
        seam::pg_dsplen::set(|s| match s.first() {
            None => -1,
            Some(&b) if b < 0x20 || b == 0x7f => -1,
            Some(&b) if b < 0x7f => 1,
            Some(_) => 1,
        });

        // char2wchar: pass-through UTF-8 decode unless the test asks it to fail.
        seam::char2wchar::set(|from| {
            if CHAR2WCHAR_FAILS.with(|f| *f.borrow()) {
                return Err(backend_utils_error::PgError::error(
                    "simulated char2wchar failure",
                ));
            }
            Ok(utf8_to_wchar(&from))
        });
        seam::pg_mb2wchar_with_len::set(|from| Ok(utf8_to_wchar(&from)));

        // Character-class predicates (ASCII byte + UTF-8-aware wide).
        seam::isalnum::set(ascii_isalnum);
        seam::isalpha::set(ascii_isalpha);
        seam::isdigit::set(ascii_isdigit);
        seam::isspace::set(ascii_isspace);
        seam::isxdigit::set(ascii_isxdigit);
        seam::iswalnum::set(ascii_isalnum);
        seam::iswalpha::set(ascii_isalpha);
        seam::iswdigit::set(ascii_isdigit);
        seam::iswspace::set(ascii_isspace);
        seam::iswxdigit::set(ascii_isxdigit);

        // Configuration / dictionary cache + lexize dispatch.
        seam::config_lenmap::set(|_cfg_id| Ok(24));
        seam::config_dict_ids::set(|_cfg_id, token_type| {
            Ok(CFG_MODE.with(|m| match *m.borrow() {
                CfgMode::Simple => simple_dict_ids(token_type),
                CfgMode::Thesaurus => thesaurus_dict_ids(token_type),
            }))
        });
        seam::dict_lexize::set(|_dict_id, lemm, mut dstate| {
            let res = CFG_MODE.with(|m| match *m.borrow() {
                CfgMode::Simple => simple_lexize(&lemm, &mut dstate),
                CfgMode::Thesaurus => thesaurus_lexize(&lemm, &mut dstate),
            });
            Ok((dstate, res))
        });

        // Generic tsquery execution engine (TS_execute / TS_execute_locations
        // with checkcondition_HL).  The test mock supports the AND-of-operands
        // top-level queries the headline tests use; checkcondition_HL is
        // emulated by `hl_resolve` over the (item, pos) match-table.
        seam::ts_execute_hl::set(|items, match_table, _flags| {
            Ok(test_ts_execute(&items, &match_table))
        });
        seam::ts_execute_locations_hl::set(|items, match_table, _flags| {
            Ok(test_ts_execute_locations(&items, &match_table))
        });
    });
}

/// `checkcondition_HL` (wparser_def.c:1981) emulation over the match-table:
/// the operand at query-item index `opidx` matches the words whose
/// `item == Some(opidx)`, reporting their ordered, deduplicated positions.
fn hl_resolve(opidx: usize, match_table: &[(Option<usize>, u16)]) -> ExecPhraseData {
    let mut pos: Vec<i32> = Vec::new();
    for (item, p) in match_table {
        if *item == Some(opidx) {
            let pv = *p as i32;
            if pos.last().map(|&last| last < pv).unwrap_or(true) {
                pos.push(pv);
            }
        }
    }
    ExecPhraseData {
        npos: pos.len() as i32,
        pos,
        width: 0,
    }
}

/// Minimal TS_execute for the test: a query that is a list of `QI_VAL`
/// operands (implicitly AND'ed at top level, as the headline tests build)
/// matches iff every operand has at least one position in the range.
fn test_ts_execute(items: &[QueryItem], match_table: &[(Option<usize>, u16)]) -> bool {
    let mut any = false;
    for (k, it) in items.iter().enumerate() {
        if matches!(it, QueryItem::Operand(_)) {
            any = true;
            if hl_resolve(k, match_table).npos == 0 {
                return false;
            }
        }
    }
    any
}

/// Minimal TS_execute_locations for the test: one ExecPhraseData per operand,
/// in query-item order, as `hlCover` consumes (each is a top-level AND'ed
/// term).  Returns empty (no covers) if any operand has no positions.
fn test_ts_execute_locations(
    items: &[QueryItem],
    match_table: &[(Option<usize>, u16)],
) -> Vec<ExecPhraseData> {
    let mut out = Vec::new();
    for (k, it) in items.iter().enumerate() {
        if matches!(it, QueryItem::Operand(_)) {
            let pd = hl_resolve(k, match_table);
            if pd.npos == 0 {
                return Vec::new();
            }
            out.push(pd);
        }
    }
    out
}

/// Configure the encoding the parser sees (max length + db encoding).
fn set_encoding(max_len: i32, db_enc: i32) {
    MAX_LEN.with(|m| *m.borrow_mut() = max_len);
    DB_ENC.with(|e| *e.borrow_mut() = db_enc);
}

fn set_cfg_mode(mode: CfgMode) {
    CFG_MODE.with(|m| *m.borrow_mut() = mode);
}

/// Run a closure with the seams installed, the shared seam state locked, and
/// the encoding reset to the ASCII (single-byte) default.
fn with_env<R>(f: impl FnOnce() -> R) -> R {
    install_env();
    let _guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    set_encoding(1, 0);
    set_cfg_mode(CfgMode::Simple);
    CHAR2WCHAR_FAILS.with(|c| *c.borrow_mut() = false);
    f()
}

// ---------------------------------------------------------------------------
// Tokenizer.
// ---------------------------------------------------------------------------

fn tokens(input: &str) -> Vec<(i32, String)> {
    let bytes = input.as_bytes().to_vec();
    let len = bytes.len();
    let mut prs = prsd_start(bytes, len).unwrap();
    let mut out = Vec::new();
    loop {
        let (ty, tok) = prsd_nexttoken(&mut prs).unwrap();
        if ty == 0 {
            break;
        }
        out.push((ty, String::from_utf8_lossy(tok).into_owned()));
    }
    prsd_end(prs);
    out
}

#[test]
fn lextype_descriptors() {
    let d = prsd_lextype();
    assert_eq!(d.len(), (crate::wparser_def::LASTNUM + 1) as usize);
    assert_eq!(d[0], (1, "asciiword".into(), "Word, all ASCII".into()));
    assert_eq!(
        d[crate::wparser_def::LASTNUM as usize],
        (0, String::new(), String::new())
    );
}

#[test]
fn token_type_list_matches_default_parser() {
    let list = tt_storage_list();
    assert_eq!(list.len(), 23);
    assert_eq!(
        list[0],
        TokenTypeRow {
            lexid: 1,
            alias: "asciiword".into(),
            descr: "Word, all ASCII".into(),
        }
    );
    assert_eq!(list[22].alias, "entity");
}

#[test]
fn ascii_word_and_space() {
    with_env(|| {
        let t = tokens("hello world");
        assert_eq!(
            t,
            vec![
                (ASCIIWORD, "hello".into()),
                (SPACE, " ".into()),
                (ASCIIWORD, "world".into()),
            ]
        );
    });
}

#[test]
fn integers_and_decimals() {
    with_env(|| {
        assert_eq!(tokens("123"), vec![(UNSIGNEDINT, "123".into())]);
        assert_eq!(tokens("3.14"), vec![(DECIMAL_T, "3.14".into())]);
        assert_eq!(tokens("-42"), vec![(SIGNEDINT, "-42".into())]);
    });
}

#[test]
fn email_and_url() {
    with_env(|| {
        let t = tokens("noreply@pgrust.comom");
        assert_eq!(t, vec![(EMAIL, "noreply@pgrust.comom".into())]);
    });
}

#[test]
fn hyphenated_word() {
    with_env(|| {
        let t = tokens("foo-bar");
        assert_eq!(t[0], (ASCIIHWORD, "foo-bar".into()));
        assert!(t.iter().any(|(ty, w)| *ty == ASCIIPARTHWORD && w == "foo"));
        assert!(t.iter().any(|(ty, w)| *ty == ASCIIPARTHWORD && w == "bar"));
    });
}

#[test]
fn prs_tokenize_default_parser() {
    with_env(|| {
        let toks = prs_tokenize(b"foo bar").unwrap();
        let simple: Vec<(i32, String)> = toks
            .iter()
            .map(|e| (e.type_, String::from_utf8_lossy(&e.lexeme).into_owned()))
            .collect();
        assert_eq!(
            simple,
            vec![
                (ASCIIWORD, "foo".into()),
                (SPACE, " ".into()),
                (ASCIIWORD, "bar".into()),
            ]
        );
    });
}

/// A multibyte database encoding whose `pg_mblen_range` raises on a truncated
/// multibyte sequence at end of buffer must surface a soft `PgError` from
/// `prsd_nexttoken` — not advance `posbyte` past `lenstr`.
#[test]
fn truncated_multibyte_raises_soft_error() {
    with_env(|| {
        set_encoding(4, 6); // UTF-8
        // 0xE4 is a 3-byte lead byte with only one byte present.
        let bytes = vec![0xE4u8];
        let len = bytes.len();
        let mut prs = prsd_start(bytes, len).unwrap();
        let err = prsd_nexttoken(&mut prs).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
        assert!(
            err.message().contains("invalid byte sequence for encoding"),
            "unexpected message: {}",
            err.message()
        );
        prsd_end(prs);
    });
}

/// A `char2wchar` that raises must propagate out of `prsd_start` as a soft
/// `PgError`, instead of being swallowed into an empty wide buffer.
#[test]
fn char2wchar_error_propagates_from_prsd_start() {
    with_env(|| {
        set_encoding(4, 6); // UTF-8 -> wide path
        CHAR2WCHAR_FAILS.with(|c| *c.borrow_mut() = true);
        let bytes = b"hello".to_vec();
        let len = bytes.len();
        match prsd_start(bytes, len) {
            Ok(_) => panic!("expected char2wchar error to propagate"),
            Err(err) => assert!(err.message().contains("simulated char2wchar failure")),
        }
    });
}

// ---------------------------------------------------------------------------
// parsetext driver.
// ---------------------------------------------------------------------------

#[test]
fn parsetext_lowercases_words_and_sets_positions() {
    with_env(|| {
        let mut prs = ParsedText::with_lenwords(4);
        parsetext(1, &mut prs, b"Hello World").unwrap();

        let words: Vec<(String, u16)> = prs
            .words
            .iter()
            .map(|w| (String::from_utf8_lossy(&w.word).into_owned(), w.pos))
            .collect();
        assert_eq!(
            words,
            vec![("hello".to_string(), 1), ("world".to_string(), 2)]
        );
        assert_eq!(prs.curwords, 2);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0, "ParsedText leak after free");
    });
}

#[test]
fn parsetext_skips_space_tokens() {
    with_env(|| {
        let mut prs = ParsedText::with_lenwords(4);
        parsetext(1, &mut prs, b"a   b").unwrap();
        assert_eq!(prs.curwords, 2);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// A multiword (thesaurus-style) dictionary must thread parsing state in
/// `DictSubState.private_state` across consecutive `getnext` calls.
#[test]
fn parsetext_threads_private_state_across_multiword_calls() {
    with_env(|| {
        set_cfg_mode(CfgMode::Thesaurus);
        let mut prs = ParsedText::with_lenwords(4);
        parsetext(1, &mut prs, b"new york").unwrap();
        let words: Vec<String> = prs
            .words
            .iter()
            .map(|w| String::from_utf8_lossy(&w.word).into_owned())
            .collect();
        assert_eq!(words, vec!["nyc".to_string()]);
        assert_eq!(prs.curwords, 1);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

// ---------------------------------------------------------------------------
// Headline path.
// ---------------------------------------------------------------------------

/// Build a single-operand tsquery whose operand string is `operand`.
fn one_term_query(operand: &[u8]) -> TSQuery {
    TSQuery {
        size: 1,
        items: vec![QueryItem::Operand(QueryOperand {
            type_: 1, // QI_VAL
            weight: 0,
            prefix: false,
            valcrc: 0,
            length: operand.len() as u32,
            distance: 0,
        })],
        operands: operand.to_vec(),
    }
}

#[test]
fn hlparsetext_fills_words_and_tags_query_match() {
    with_env(|| {
        let query = one_term_query(b"world");
        let mut prs = HeadlineParsedText::with_lenwords(8);
        hlparsetext(1, &mut prs, &query, b"Hello World").unwrap();

        let typed: Vec<(u8, String)> = prs
            .words
            .iter()
            .map(|w| (w.type_, String::from_utf8_lossy(&w.word).into_owned()))
            .collect();
        assert!(typed.iter().any(|(t, w)| *t == ASCIIWORD as u8 && w == "Hello"));
        assert!(typed.iter().any(|(t, w)| *t == ASCIIWORD as u8 && w == "World"));
        let world = prs
            .words
            .iter()
            .find(|w| &*w.word == b"World")
            .expect("World token present");
        assert!(world.item.is_some());
        prs.free();
        assert_eq!(prs.charged_bytes(), 0, "HeadlineParsedText leak after free");
    });
}

/// `generateHeadline` wraps the assembled bytes in a 4-byte varlena header and
/// emits startsel/stopsel around selected words and fragment delimiters between
/// fragments.
#[test]
fn generate_headline_assembles_selected_and_fragments() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        prs.startsel = b"<b>".to_vec();
        prs.stopsel = b"</b>".to_vec();
        prs.fragdelim = b" ... ".to_vec();
        prs.startsellen = 3;
        prs.stopsellen = 4;
        prs.fragdelimlen = 5;
        // word 0: in, selected -> "<b>foo</b>"
        push_word(&mut prs, b"foo", true, true, false, false);
        // word 1: not in -> closes fragment, contributes nothing
        push_word(&mut prs, b" ", false, false, false, false);
        // word 2: in (new fragment) -> fragdelim then "bar"
        push_word(&mut prs, b"bar", true, false, false, false);
        prs.curwords = 3;

        let out = generateHeadline(&prs);
        let total_len = out.len() as u32;
        assert_eq!(&out[..4], &(total_len << 2).to_ne_bytes());
        let payload = &out[4..];
        assert_eq!(payload, b"<b>foo</b> ... bar");
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// Helper: push a word into a `HeadlineParsedText` with the given flags,
/// charging its bytes to the context (mirroring `hladdword`).
fn push_word(
    prs: &mut HeadlineParsedText,
    word: &[u8],
    in_: bool,
    selected: bool,
    replace: bool,
    skip: bool,
) {
    crate::ts_parse::test_push_word(prs, word, in_, selected, replace, skip);
}

/// `mark_fragment` marks the selected/in/replace flags over a range.
#[test]
fn mark_fragment_marks_range() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        // word 0: has item, asciiword -> selected, in
        crate::ts_parse::test_push_word_typed(&mut prs, ASCIIWORD as u8, Some(0));
        // word 1: tag -> replace (HLIDREPLACE), in
        crate::ts_parse::test_push_word_typed(&mut prs, TAG_T as u8, None);
        prs.curwords = 2;
        mark_fragment(&mut prs, false, 0, 1);
        assert!(prs.words[0].selected);
        assert!(prs.words[0].in_);
        assert!(prs.words[1].replace);
        assert!(prs.words[1].in_);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

// ---------------------------------------------------------------------------
// prsd_headline / hlCover / mark_hl_words / mark_hl_fragments.
// ---------------------------------------------------------------------------

/// Build a single-operand tsquery (`GETQUERY(query)` = one `QI_VAL`).
fn single_operand_query() -> TSQuery {
    TSQuery {
        size: 1,
        items: vec![QueryItem::Operand(QueryOperand {
            type_: 1, // QI_VAL
            weight: 0,
            prefix: false,
            valcrc: 0,
            length: 0,
            distance: 0,
        })],
        operands: Vec::new(),
    }
}

/// `prsd_headline` (MaxFragments == 0): selects a headline around the cover and
/// fills the default markup strings / lengths.
#[test]
fn prsd_headline_marks_cover_and_defaults() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        // 0: "the" (non-interesting word), 1: "cat" (query match, item 0),
        // 2: "sat" (word).  Positions track lexeme order.
        crate::ts_parse::test_push_hl_word(&mut prs, b"the", ASCIIWORD as u8, None, false, 1);
        crate::ts_parse::test_push_hl_word(&mut prs, b"cat", ASCIIWORD as u8, Some(0), false, 2);
        crate::ts_parse::test_push_hl_word(&mut prs, b"sat", ASCIIWORD as u8, None, false, 3);

        let query = single_operand_query();
        // min_words(1) < max_words(35); shortword 0 so no word is a bad endpoint.
        let opts = vec![
            ("MinWords".to_string(), "1".to_string()),
            ("MaxWords".to_string(), "35".to_string()),
            ("ShortWord".to_string(), "0".to_string()),
        ];
        prsd_headline(&mut prs, &opts, &query).unwrap();

        // The query word must be selected and in the headline.
        assert!(prs.words[1].selected, "query word selected");
        assert!(prs.words[1].in_, "query word in headline");
        // Default markup strings filled.
        assert_eq!(prs.startsel, b"<b>");
        assert_eq!(prs.stopsel, b"</b>");
        assert_eq!(prs.fragdelim, b" ... ");
        assert_eq!(prs.startsellen, 3);
        assert_eq!(prs.stopsellen, 4);
        assert_eq!(prs.fragdelimlen, 5);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// `prsd_headline` (MaxFragments > 0): the fragment selector marks the cover
/// fragment and honours custom StartSel/StopSel/FragmentDelimiter.
#[test]
fn prsd_headline_fragments_mode_and_custom_markup() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        crate::ts_parse::test_push_hl_word(&mut prs, b"a", ASCIIWORD as u8, None, false, 1);
        crate::ts_parse::test_push_hl_word(&mut prs, b"dog", ASCIIWORD as u8, Some(0), false, 2);
        crate::ts_parse::test_push_hl_word(&mut prs, b"b", ASCIIWORD as u8, None, false, 3);

        let query = single_operand_query();
        let opts = vec![
            ("MaxFragments".to_string(), "1".to_string()),
            ("MinWords".to_string(), "1".to_string()),
            ("MaxWords".to_string(), "10".to_string()),
            ("ShortWord".to_string(), "0".to_string()),
            ("StartSel".to_string(), "[".to_string()),
            ("StopSel".to_string(), "]".to_string()),
            ("FragmentDelimiter".to_string(), "|".to_string()),
        ];
        prsd_headline(&mut prs, &opts, &query).unwrap();

        assert!(prs.words[1].selected, "query word selected in fragment mode");
        assert_eq!(prs.startsel, b"[");
        assert_eq!(prs.stopsel, b"]");
        assert_eq!(prs.fragdelim, b"|");
        assert_eq!(prs.startsellen, 1);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// `prsd_headline` HighlightAll: the whole document is the headline; the
/// MinWords/MaxWords sanity checks are skipped.
#[test]
fn prsd_headline_highlightall_marks_everything() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        crate::ts_parse::test_push_hl_word(&mut prs, b"x", ASCIIWORD as u8, Some(0), false, 1);
        crate::ts_parse::test_push_hl_word(&mut prs, b"y", ASCIIWORD as u8, None, false, 2);

        let query = single_operand_query();
        let opts = vec![("HighlightAll".to_string(), "true".to_string())];
        prsd_headline(&mut prs, &opts, &query).unwrap();

        assert!(prs.words[0].in_);
        assert!(prs.words[1].in_);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// `prsd_headline` empty query (size 0): nothing matches, so the fallback marks
/// the first min_words words.
#[test]
fn prsd_headline_empty_query_marks_first_words() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        crate::ts_parse::test_push_hl_word(&mut prs, b"one", ASCIIWORD as u8, None, false, 1);
        crate::ts_parse::test_push_hl_word(&mut prs, b"two", ASCIIWORD as u8, None, false, 2);

        let query = TSQuery::default(); // size 0
        let opts = vec![
            ("MinWords".to_string(), "1".to_string()),
            ("MaxWords".to_string(), "35".to_string()),
        ];
        prsd_headline(&mut prs, &opts, &query).unwrap();

        // Fallback selected at least the first word into the headline.
        assert!(prs.words[0].in_);
        prs.free();
        assert_eq!(prs.charged_bytes(), 0);
    });
}

/// `prsd_headline` rejects an unrecognized option (ERRCODE 22023).
#[test]
fn prsd_headline_unrecognized_option_errors() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        let query = single_operand_query();
        let opts = vec![("Bogus".to_string(), "1".to_string())];
        let err = prsd_headline(&mut prs, &opts, &query).unwrap_err();
        assert!(format!("{err:?}").contains("unrecognized headline parameter"));
        prs.free();
    });
}

/// `prsd_headline` enforces MinWords < MaxWords (when not HighlightAll).
#[test]
fn prsd_headline_minwords_ge_maxwords_errors() {
    with_env(|| {
        let mut prs = HeadlineParsedText::default();
        let query = single_operand_query();
        let opts = vec![
            ("MinWords".to_string(), "35".to_string()),
            ("MaxWords".to_string(), "10".to_string()),
        ];
        let err = prsd_headline(&mut prs, &opts, &query).unwrap_err();
        assert!(format!("{err:?}").contains("MinWords must be less than MaxWords"));
        prs.free();
    });
}
