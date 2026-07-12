use ::mcx::{Mcx, MemoryContext, PgVec};

use crate::{parse_rule_line, unaccent_lexize, UnaccentTrie};

fn leaked_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("unaccent-test"))).mcx()
}

fn trie_from(mcx: Mcx<'static>, content: &str) -> UnaccentTrie {
    let mut trie = UnaccentTrie {
        nodes: PgVec::new_in(mcx),
        replacements: PgVec::new_in(mcx),
    };
    for line in content.as_bytes().split_inclusive(|&b| b == b'\n') {
        if let Ok(Some((src, trg))) = parse_rule_line(line) {
            trie.place(mcx, src, &trg).unwrap();
        }
    }
    trie
}

fn lexize(mcx: Mcx<'static>, trie: &UnaccentTrie, token: &str) -> Option<String> {
    unaccent_lexize(mcx, trie, token.as_bytes())
        .unwrap()
        .map(|r| String::from_utf8_lossy(&r.0[0].lexeme).into_owned())
}

#[test]
fn parse_line_forms() {
    let (src, trg) = parse_rule_line("\u{00c0} A\n".as_bytes()).unwrap().unwrap();
    assert_eq!(src, "\u{00c0}".as_bytes());
    assert_eq!(trg, b"A");
    let (src, trg) = parse_rule_line("\u{0301}\n".as_bytes()).unwrap().unwrap();
    assert_eq!(src, "\u{0301}".as_bytes());
    assert_eq!(trg, b"");
    let (_, trg) = parse_rule_line(b"x \"a b\"\n").unwrap().unwrap();
    assert_eq!(trg, b"a b");
    let (_, trg) = parse_rule_line(b"y \"a\"\"b\"\n").unwrap().unwrap();
    assert_eq!(trg, b"a\"b");
    assert!(parse_rule_line(b"   \n").unwrap().is_none());
    assert_eq!(parse_rule_line(b"a b c\n").unwrap_err(), -1);
    assert_eq!(parse_rule_line(b"a \"bc\n").unwrap_err(), -2);
}

#[test]
fn lexize_replaces_and_filters() {
    let mcx = leaked_mcx();
    let trie = trie_from(mcx, "\u{00e9} e\n\u{0153} \"oe\"\n\u{2103} \"\u{00b0}C\"\n");
    assert_eq!(lexize(mcx, &trie, "foobar"), None);
    assert_eq!(lexize(mcx, &trie, "caf\u{00e9}").as_deref(), Some("cafe"));
    assert_eq!(lexize(mcx, &trie, "\u{0153}uf").as_deref(), Some("oeuf"));
    assert_eq!(lexize(mcx, &trie, "25\u{2103}").as_deref(), Some("25\u{00b0}C"));
}

#[test]
fn lexize_longest_match_and_empty_replacement() {
    let mcx = leaked_mcx();
    let trie = trie_from(mcx, "ab X\nabc Y\n\u{0300}\n");
    assert_eq!(lexize(mcx, &trie, "abcd").as_deref(), Some("Yd"));
    assert_eq!(lexize(mcx, &trie, "abd").as_deref(), Some("Xd"));
    assert_eq!(lexize(mcx, &trie, "A\u{0300}").as_deref(), Some("A"));
}

#[test]
fn duplicate_source_keeps_first() {
    let mcx = leaked_mcx();
    let trie = trie_from(mcx, "a X\na Y\n");
    assert_eq!(lexize(mcx, &trie, "a").as_deref(), Some("X"));
}
