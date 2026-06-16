//! Unit tests for the in-crate (de)serialization state machine and the
//! `buildDefItem` value classification. These exercise pure logic only (no
//! catalog/syscache seams); the command drivers are covered by the cross-crate
//! tests once their owners land.

use super::*;
use mcx::MemoryContext;

fn de<'mcx>(mcx: Mcx<'mcx>, txt: &str) -> Vec<DefElem<'mcx>> {
    deserialize_deflist(mcx, txt).unwrap()
}

fn ser<'mcx>(mcx: Mcx<'mcx>, list: &[DefElem<'mcx>]) -> String {
    let refs: Vec<&DefElem> = list.iter().collect();
    serialize_deflist(mcx, &refs).unwrap()
}

#[test]
fn build_def_item_classifies_unquoted_values() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    let i = buildDefItem(mcx, "k", "42", false).unwrap();
    assert!(matches!(i.arg.as_deref(), Some(Node::Integer(n)) if n.ival == 42));

    let f = buildDefItem(mcx, "k", "1.5", false).unwrap();
    assert!(matches!(f.arg.as_deref(), Some(Node::Float(_))));

    let b = buildDefItem(mcx, "k", "true", false).unwrap();
    assert!(matches!(b.arg.as_deref(), Some(Node::Boolean(n)) if n.boolval));

    let s = buildDefItem(mcx, "k", "hello", false).unwrap();
    assert!(matches!(s.arg.as_deref(), Some(Node::String(_))));

    // A quoted value is always a string, even when it looks numeric.
    let q = buildDefItem(mcx, "k", "42", true).unwrap();
    assert!(matches!(q.arg.as_deref(), Some(Node::String(_))));
}

#[test]
fn serialize_quotes_strings_not_numbers() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    let list = alloc::vec![
        buildDefItem(mcx, "n", "5", false).unwrap(),
        buildDefItem(mcx, "s", "abc", true).unwrap(),
    ];
    let out = ser(mcx, &list);
    assert_eq!(out, "n = 5, s = 'abc'");
}

#[test]
fn serialize_escapes_quote_and_backslash() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    // a single quote is doubled
    let q = alloc::vec![buildDefItem(mcx, "k", "a'b", true).unwrap()];
    assert_eq!(ser(mcx, &q), "k = 'a''b'");

    // a backslash forces E-string syntax and is doubled
    let bs = alloc::vec![buildDefItem(mcx, "k", "a\\b", true).unwrap()];
    assert_eq!(ser(mcx, &bs), "k = E'a\\\\b'");
}

#[test]
fn deserialize_round_trips_serialize() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    let orig = "stopwords = 'english', accept = false, maxlen = 10";
    let parsed = de(mcx, orig);
    assert_eq!(parsed.len(), 3);
    assert_eq!(def_name(&parsed[0]), "stopwords");
    assert_eq!(def_name(&parsed[1]), "accept");
    assert_eq!(def_name(&parsed[2]), "maxlen");

    // re-serialize: quoting differs (round-trip is value-preserving, the
    // serialized form re-quotes per buildDefItem classification)
    let back = ser(mcx, &parsed);
    let reparsed = de(mcx, &back);
    assert_eq!(reparsed.len(), 3);
    assert_eq!(def_get_string(mcx, &reparsed[0]).unwrap(), "english");
    assert_eq!(def_get_string(mcx, &reparsed[2]).unwrap(), "10");
}

#[test]
fn deserialize_handles_quoted_keys_and_doubled_quotes() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    let parsed = de(mcx, "\"my key\" = 'va''lue'");
    assert_eq!(parsed.len(), 1);
    assert_eq!(def_name(&parsed[0]), "my key");
    assert_eq!(def_get_string(mcx, &parsed[0]).unwrap(), "va'lue");
}

#[test]
fn deserialize_rejects_malformed_input() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    // an unterminated single-quoted value is invalid
    assert!(deserialize_deflist(mcx, "k = 'unterminated").is_err());
}
