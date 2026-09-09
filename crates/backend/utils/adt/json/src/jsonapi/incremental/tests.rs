//! C: src/test/modules/test_json_parser (test_json_parser_incremental.c)
//! in spirit — the incremental parser must produce the same semantic-action
//! stream and the same errors as the recursive-descent parser for every
//! chunking of the input, and JSON_INCOMPLETE until the last chunk.

use mbutils::SetDatabaseEncoding;
use mcx::MemoryContext;
use types_error::PgResult;
use wchar::PG_UTF8;

use super::*;
use crate::jsonapi::{parse_sem, JsonLex, JsonLexDe, JsonSem, JsonSemToken};

fn setup() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
}

/// A sink that records every semantic action as owned text, so it can serve
/// both parsers (`for<'a> JsonSem<'a>`).
#[derive(Default)]
struct Events(Vec<String>);

impl<'m> JsonSem<'m> for Events {
    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        self.0.push(format!("ostart@{}", lex.lex_level));
        Ok(true)
    }
    fn object_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        self.0.push(format!("oend@{}", lex.lex_level));
        Ok(true)
    }
    fn array_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        self.0.push(format!("astart@{}", lex.lex_level));
        Ok(true)
    }
    fn array_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        self.0.push(format!("aend@{}", lex.lex_level));
        Ok(true)
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'m [u8],
        isnull: bool,
    ) -> PgResult<bool> {
        self.0.push(format!("ofstart {:?} null={isnull}", String::from_utf8_lossy(fname)));
        Ok(true)
    }
    fn object_field_end(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'m [u8],
        isnull: bool,
    ) -> PgResult<bool> {
        self.0.push(format!("ofend {:?} null={isnull}", String::from_utf8_lossy(fname)));
        Ok(true)
    }
    fn array_element_start(&mut self, _lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        self.0.push(format!("aestart null={isnull}"));
        Ok(true)
    }
    fn array_element_end(&mut self, _lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        self.0.push(format!("aeend null={isnull}"));
        Ok(true)
    }
    fn scalar(&mut self, _lex: &JsonLex<'_>, token: JsonSemToken<'m>) -> PgResult<bool> {
        let s = match token {
            JsonSemToken::String(s) => format!("str {:?}", String::from_utf8_lossy(s)),
            JsonSemToken::Number(n) => format!("num {}", String::from_utf8_lossy(n)),
            JsonSemToken::True => "true".to_string(),
            JsonSemToken::False => "false".to_string(),
            JsonSemToken::Null => "null".to_string(),
        };
        self.0.push(s);
        Ok(true)
    }
}

/// Whole-buffer parse: (result, errdetail, events).
fn whole(doc: &[u8]) -> (JsonError, String, Vec<String>) {
    setup();
    let cx = MemoryContext::new("t");
    let mut lex = JsonLexDe::new(cx.mcx(), doc, PG_UTF8);
    let mut ev = Events::default();
    let r = parse_sem(&mut lex, &mut ev).unwrap();
    let detail = if r == JsonError::Success { String::new() } else { lex.lex.errdetail(r) };
    (r, detail, ev.0)
}

/// Incremental parse over the given split points: (result, errdetail,
/// events). Every non-last chunk must yield Incomplete; the result is that of
/// the first non-Incomplete chunk (an error stops the feed, as C's callers
/// stop).
fn chunked(doc: &[u8], splits: &[usize]) -> (JsonError, String, Vec<String>) {
    setup();
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    let mut ev = Events::default();
    let mut bounds: Vec<usize> = vec![0];
    bounds.extend_from_slice(splits);
    bounds.push(doc.len());
    let n = bounds.len() - 1;
    for i in 0..n {
        let piece = &doc[bounds[i]..bounds[i + 1]];
        let is_last = i == n - 1;
        // A fresh context per chunk: the per-token scratch is bulk-freed.
        let cx = MemoryContext::new("chunk");
        let mut ch = st.chunk(cx.mcx(), piece, is_last);
        let r = ch.parse(&mut ev).unwrap();
        if is_last {
            let detail = if r == JsonError::Success { String::new() } else { ch.errdetail(r) };
            return (r, detail, ev.0);
        }
        if r != JsonError::Incomplete {
            let detail = ch.errdetail(r);
            return (r, detail, ev.0);
        }
    }
    unreachable!()
}

/// Every single split point, plus a handful of multi-way splits.
fn assert_all_splits_match(doc: &[u8]) {
    let w = whole(doc);
    for i in 1..doc.len() {
        let c = chunked(doc, &[i]);
        assert_eq!(c, w, "split at {i} of {:?}", String::from_utf8_lossy(doc));
    }
    // byte-at-a-time
    let all: Vec<usize> = (1..doc.len()).collect();
    assert_eq!(chunked(doc, &all), w, "byte-at-a-time {:?}", String::from_utf8_lossy(doc));
    // three-way
    if doc.len() > 5 {
        let a = doc.len() / 3;
        let b = 2 * doc.len() / 3;
        assert_eq!(chunked(doc, &[a, b]), w, "3-way {:?}", String::from_utf8_lossy(doc));
    }
}

#[test]
fn incremental_matches_recursive_descent_on_valid_documents() {
    let docs: &[&[u8]] = &[
        b"{}",
        b"[]",
        b"  {  }  ",
        b"\"scalar\"",
        b"12345",
        b"-0.5e+10",
        b"true",
        b"false",
        b"null",
        b"[1, 2.5, -3e2, \"x\", true, false, null, [], {}]",
        b"{\"a\": 1, \"b\": [1, {\"c\": null}], \"d\": \"e\\\"f\\\\g\\n\", \"h\": {\"i\": {\"j\": []}}}",
        b"{\"esc\": \"\\u0041\\u00e9\\ud83d\\ude00 tail\"}",
        b"[\"a\\/b\", \"\\b\\f\\n\\r\\t\"]",
        b"{\"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [\n{ \"Path\": \"PG_VERSION\", \"Size\": 3 }\n],\n\"Manifest-Checksum\": \"abc\"}\n",
        b"\n\n  [ \n 1 \n ] \n",
        b"{\"k\": [1,[2,[3,[4]]]]}",
        b"[1e5,2E-5,3.25,0,-0]",
    ];
    for doc in docs {
        let (r, _, _) = whole(doc);
        assert_eq!(r, JsonError::Success, "{:?}", String::from_utf8_lossy(doc));
        assert_all_splits_match(doc);
    }
}

#[test]
fn incremental_matches_recursive_descent_on_invalid_documents() {
    let docs: &[&[u8]] = &[
        b"",
        b"   ",
        b"{",
        b"[1,",
        b"{\"a\"",
        b"{\"a\":",
        b"{\"a\":1",
        b"{\"a\" 1}",
        b"{1: 2}",
        b"[1 2]",
        b"[1,]",
        b"{\"a\":1,}",
        b"{} x",
        b"{} 1",
        b"nul",
        b"nullx",
        b"tru",
        b"truex",
        b"falsey",
        b"01",
        b"1.",
        b"1.x",
        b"1e",
        b"1e+",
        b"-",
        b"--1",
        b"1.2.3",
        b"\"abc",
        b"\"abc\\",
        b"\"abc\\q\"",
        b"\"a\\u12\"",
        b"\"a\\u12G4\"",
        b"\"\\ud83d\"",
        b"\"\\ude00\"",
        b"\"\\ud83d\\ud83d\"",
        b"\"\\u0000\"",
        b"\"ctl\x01char\"",
        b"[\"a\", \"b\"",
        b"{\"a\": [1, 2}",
        b"[1, 2]]",
        b"{\"a\": 1}}",
        b"@",
        b"[1, @]",
        b"{\"a\": tru}",
    ];
    for doc in docs {
        let (r, _, _) = whole(doc);
        assert_ne!(r, JsonError::Success, "{:?}", String::from_utf8_lossy(doc));
        assert_all_splits_match(doc);
    }
}

#[test]
fn partial_tokens_complete_across_chunks() {
    // String with an escaped quote straddling the boundary.
    let (r, _, ev) = chunked(b"\"ab\\\"cd\"", &[4]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(ev, vec!["str \"ab\\\"cd\""]);

    // Backslash exactly at the boundary, then the quote.
    let (r, _, ev) = chunked(b"\"ab\\\"cd\"", &[3]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(ev, vec!["str \"ab\\\"cd\""]);

    // \u escape split in the middle of the hex digits.
    let (r, _, ev) = chunked(b"\"\\u0041\"", &[4]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(ev, vec!["str \"A\""]);

    // Number continued by a fraction and an exponent sign.
    let (r, _, ev) = chunked(b"[12.5e-3]", &[3, 6, 7]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(ev, vec!["astart@0", "aestart null=false", "num 12.5e-3", "aeend null=false", "aend@0"]);

    // Literal split anywhere.
    let (r, _, ev) = chunked(b"[null]", &[2, 4]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(ev, vec!["astart@0", "aestart null=true", "null", "aeend null=true", "aend@0"]);

    // A field name split across chunks reaches the field hooks intact.
    let (r, _, ev) = chunked(b"{\"long-field-name\": null}", &[7]);
    assert_eq!(r, JsonError::Success);
    assert_eq!(
        ev,
        vec![
            "ostart@0",
            "ofstart \"long-field-name\" null=true",
            "null",
            "ofend \"long-field-name\" null=true",
            "oend@0"
        ]
    );
}

#[test]
fn partial_token_errors_report_the_accumulated_token() {
    // The partial token is what json_errdetail prints, not the last chunk.
    let (r, detail, _) = chunked(b"[nullx]", &[3]);
    assert_eq!(r, JsonError::InvalidToken);
    assert_eq!(detail, "Token \"nullx\" is invalid.");

    // Unterminated string in the last chunk: the whole accumulated token,
    // opening quote included (C: token_start = ptok->data).
    let (r, detail, _) = chunked(b"\"abcdef", &[3]);
    assert_eq!(r, JsonError::InvalidToken);
    assert_eq!(detail, "Token \"\"abcdef\" is invalid.");
    assert_eq!(whole(b"\"abcdef").1, detail);

    // Invalid escape completed from the partial buffer: token_start moves to
    // the escape (json_lex_string's handling of invalid escapes).
    let (r, detail, _) = chunked(b"\"ab\\qcd\"", &[3]);
    assert_eq!(r, JsonError::EscapingInvalid);
    assert_eq!(detail, "Escape sequence \"\\q\" is invalid.");

    // Control character inside a string assembled from two chunks.
    let (r, detail, _) = chunked(b"\"ab\x01cd\"", &[2]);
    assert_eq!(r, JsonError::EscapingRequired);
    assert_eq!(detail, "Character with value 0x01 must be escaped.");

    // A number that is still open at the last chunk boundary is invalid.
    let (r, detail, _) = chunked(b"[1.]", &[2]);
    assert_eq!(r, JsonError::InvalidToken);
    assert_eq!(detail, "Token \"1.\" is invalid.");

    // Trailing garbage after the document, delivered in its own chunk.
    let (r, detail, _) = chunked(b"{}x", &[2]);
    assert_eq!(r, JsonError::InvalidToken);
    assert_eq!(detail, "Token \"x\" is invalid.");
}

#[test]
fn non_last_chunks_are_incomplete_and_last_chunk_completes() {
    setup();
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    let mut ev = Events::default();
    let cx = MemoryContext::new("t");
    assert!(!st.started());
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b"{\"a\":", false).unwrap(), JsonError::Incomplete);
    assert!(st.started());
    assert_eq!(st.lex_level(), 1);
    // An empty chunk in the middle is fine.
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b"", false).unwrap(), JsonError::Incomplete);
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b" [1", false).unwrap(), JsonError::Incomplete);
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b"]}", true).unwrap(), JsonError::Success);
    assert_eq!(st.lex_level(), 0);
    assert_eq!(
        ev.0,
        vec![
            "ostart@0",
            "ofstart \"a\" null=false",
            "astart@1",
            "aestart null=false",
            "num 1",
            "aeend null=false",
            "aend@1",
            "ofend \"a\" null=false",
            "oend@0"
        ]
    );
}

#[test]
fn nesting_limit_is_json_td_max_stack() {
    setup();
    let cx = MemoryContext::new("t");
    let depth = JSON_TD_MAX_STACK as usize;

    let mut ok = vec![b'['; depth];
    ok.extend(std::iter::repeat(b']').take(depth));
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    let mut ev = Events::default();
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, &ok, true).unwrap(), JsonError::Success);

    let mut too_deep = vec![b'['; depth + 1];
    too_deep.extend(std::iter::repeat(b']').take(depth + 1));
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    let mut ev = Events::default();
    let mut ch = st.chunk(cx.mcx(), &too_deep, true);
    let r = ch.parse(&mut ev).unwrap();
    assert_eq!(r, JsonError::NestingTooDeep);
    assert_eq!(ch.errdetail(r), "JSON nested too deep, maximum permitted depth is 6400.");
}

#[test]
fn line_numbers_carry_across_chunks() {
    setup();
    let cx = MemoryContext::new("t");
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    let mut ev = Events::default();
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b"[\n1,\n", false).unwrap(), JsonError::Incomplete);
    let mut ch = st.chunk(cx.mcx(), b"\n2 3]", true);
    let r = ch.parse(&mut ev).unwrap();
    assert_eq!(r, JsonError::ExpectedArrayNext);
    assert_eq!(ch.errdetail(r), "Expected \",\" or \"]\", but found \"3\".");
    assert_eq!(ch.lex().lex.line_number, 4);
}

#[test]
fn sem_action_failure_propagates() {
    struct Refuse;
    impl<'m> JsonSem<'m> for Refuse {
        fn scalar(&mut self, _lex: &JsonLex<'_>, _token: JsonSemToken<'m>) -> PgResult<bool> {
            Ok(false)
        }
    }
    setup();
    let cx = MemoryContext::new("t");
    let mut st = JsonLexIncremental::new(PG_UTF8, true);
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut Refuse, b"[1", false).unwrap(), JsonError::Incomplete);
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut Refuse, b"]", true).unwrap(), JsonError::SemActionFailed);
}

#[test]
fn need_escapes_false_hands_empty_strings() {
    setup();
    let cx = MemoryContext::new("t");
    let mut st = JsonLexIncremental::new(PG_UTF8, false);
    let mut ev = Events::default();
    assert_eq!(pg_parse_json_incremental(&mut st, cx.mcx(), &mut ev, b"{\"k\": \"v\", \"n\": 7}", true).unwrap(), JsonError::Success);
    assert_eq!(
        ev.0,
        vec!["ostart@0", "ofstart \"\" null=false", "str \"\"", "ofend \"\" null=false", "ofstart \"\" null=false", "num 7", "ofend \"\" null=false", "oend@0"]
    );
}
