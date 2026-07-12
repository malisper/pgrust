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
    // "src trg"
    let (src, trg) = parse_rule_line("\u{00c0} A\n".as_bytes()).unwrap().unwrap();
    assert_eq!(src, "\u{00c0}".as_bytes());
    assert_eq!(trg, b"A");
    // trg omitted -> empty replacement
    let (src, trg) = parse_rule_line("\u{0301}\n".as_bytes()).unwrap().unwrap();
    assert_eq!(src, "\u{0301}".as_bytes());
    assert_eq!(trg, b"");
    // quoted trg keeps whitespace, doubled quote unescapes
    let (_, trg) = parse_rule_line(b"x \"a b\"\n").unwrap().unwrap();
    assert_eq!(trg, b"a b");
    let (_, trg) = parse_rule_line(b"y \"a\"\"b\"\n").unwrap().unwrap();
    assert_eq!(trg, b"a\"b");
    // empty line
    assert!(parse_rule_line(b"   \n").unwrap().is_none());
    // more than two strings
    assert_eq!(parse_rule_line(b"a b c\n").unwrap_err(), -1);
    // unfinished quoted string
    assert_eq!(parse_rule_line(b"a \"bc\n").unwrap_err(), -2);
}

#[test]
fn lexize_replaces_and_filters() {
    let mcx = leaked_mcx();
    let trie = trie_from(mcx, "\u{00e9} e\n\u{0153} \"oe\"\n\u{2103} \"\u{00b0}C\"\n");
    // no substitution -> None (dictionary passes the token through)
    assert_eq!(lexize(mcx, &trie, "foobar"), None);
    // single-char replacement, mixed with untouched bytes
    assert_eq!(lexize(mcx, &trie, "caf\u{00e9}").as_deref(), Some("cafe"));
    // one-to-many replacement
    assert_eq!(lexize(mcx, &trie, "\u{0153}uf").as_deref(), Some("oeuf"));
    // multibyte replacement target
    assert_eq!(lexize(mcx, &trie, "25\u{2103}").as_deref(), Some("25\u{00b0}C"));
}

#[test]
fn lexize_longest_match_and_empty_replacement() {
    let mcx = leaked_mcx();
    // multi-byte source sequence ("1/2" ligature style) plus deletion rule
    let trie = trie_from(mcx, "ab X\nabc Y\n\u{0300}\n");
    assert_eq!(lexize(mcx, &trie, "abcd").as_deref(), Some("Yd"));
    assert_eq!(lexize(mcx, &trie, "abd").as_deref(), Some("Xd"));
    // combining accent removed entirely
    assert_eq!(lexize(mcx, &trie, "A\u{0300}").as_deref(), Some("A"));
}

#[test]
fn duplicate_source_keeps_first() {
    let mcx = leaked_mcx();
    let trie = trie_from(mcx, "a X\na Y\n");
    assert_eq!(lexize(mcx, &trie, "a").as_deref(), Some("X"));
}
