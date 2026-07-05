use ::adt_tsvector_core::layout::TsVec;
use ::adt_tsvector_core::query::TsQueryRef;
use ::gin_vocab::{GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GIN_TRUE};
use ::mcx::{Mcx, MemoryContext};

fn tsq<'m>(mcx: Mcx<'m>, s: &str) -> ::mcx::PgVec<'m, u8> {
    ::adt_tsquery_core::io::tsquery_in_core(mcx, s.as_bytes(), None)
        .expect("tsquery parse")
        .expect("no soft error")
}

fn text_payload<'a>(d: ::datum::Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    unsafe {
        let len = ::types_tuple::varatt::varsize_4b(p);
        core::slice::from_raw_parts(p.add(4), len - 4)
    }
}

#[test]
fn cmp_fns() {
    assert_eq!(crate::gin_cmp_tslexeme(b"abc", b"abc"), 0);
    assert!(crate::gin_cmp_tslexeme(b"abc", b"abd") < 0);
    assert!(crate::gin_cmp_tslexeme(b"abcd", b"abc") > 0);
    assert_eq!(crate::gin_cmp_prefix(b"ab", b"abc"), 0);
    assert_eq!(crate::gin_cmp_prefix(b"ab", b"ab"), 0);
    // smaller key: "prevent continue scan" fix-up
    assert_eq!(crate::gin_cmp_prefix(b"b", b"a"), 1);
    assert!(crate::gin_cmp_prefix(b"ab", b"ac") > 0);
}

#[test]
fn extract_tsvector() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = ::adt_tsvector_core::io::tsvector_in_core(mcx, b"'b':2 'a':1 'c':3", None)
        .unwrap()
        .unwrap();
    let entries = crate::gin_extract_tsvector(mcx, TsVec { payload: &img[4..] }).unwrap();
    let mut lexemes: Vec<&[u8]> = entries.iter().map(|&d| text_payload(d)).collect();
    lexemes.sort();
    assert_eq!(lexemes, vec![b"a".as_slice(), b"b", b"c"]);
}

#[test]
fn extract_tsquery_shapes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let q = tsq(mcx, "foo & bar:*");
    let out = crate::gin_extract_tsquery(mcx, TsQueryRef { payload: &q[4..] }).unwrap();
    assert_eq!(out.entries.len(), 2);
    assert_eq!(out.search_mode, GIN_SEARCH_MODE_DEFAULT);
    let texts: Vec<&[u8]> = out.entries.iter().map(|&d| text_payload(d)).collect();
    let flags: Vec<bool> = out.partial_match.iter().copied().collect();
    let pos_foo = texts.iter().position(|t| *t == b"foo").unwrap();
    let pos_bar = texts.iter().position(|t| *t == b"bar").unwrap();
    assert!(!flags[pos_foo]);
    assert!(flags[pos_bar]);

    let q = tsq(mcx, "!foo");
    let out = crate::gin_extract_tsquery(mcx, TsQueryRef { payload: &q[4..] }).unwrap();
    assert_eq!(out.search_mode, GIN_SEARCH_MODE_ALL);
    assert_eq!(out.entries.len(), 1);
}

// present: lexemes marked GIN_TRUE; entry positions come from extraction
// order (tsquery items are polish-ordered, not query-text-ordered).
fn consistent(qs: &str, present: &[&[u8]]) -> (bool, bool) {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let q = tsq(mcx, qs);
    let qr = TsQueryRef { payload: &q[4..] };
    let out = crate::gin_extract_tsquery(mcx, qr).unwrap();
    let check: Vec<i8> = out
        .entries
        .iter()
        .map(|&d| {
            if present.contains(&text_payload(d)) {
                GIN_TRUE
            } else {
                GIN_FALSE
            }
        })
        .collect();
    crate::gin_tsquery_consistent(mcx, &check, qr, out.map_item_operand.as_slice()).unwrap()
}

#[test]
fn consistent_matrix() {
    assert_eq!(consistent("a & b", &[b"a", b"b"]), (true, false));
    assert_eq!(consistent("a & b", &[b"a"]), (false, false));
    assert_eq!(consistent("a | b", &[b"b"]), (true, false));
    assert_eq!(consistent("a & !b", &[b"a"]), (true, false));
    assert_eq!(consistent("a & !b", &[b"a", b"b"]), (false, false));
    // weights force recheck
    assert_eq!(consistent("a:A", &[b"a"]), (true, true));
    // phrase forces recheck through the MAYBE lane
    assert_eq!(consistent("a <-> b", &[b"a", b"b"]), (true, true));
    assert_eq!(consistent("a <-> b", &[b"a"]), (false, false));
}

#[test]
fn triconsistent_matrix() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let q = tsq(mcx, "a & b");
    let qr = TsQueryRef { payload: &q[4..] };
    let out = crate::gin_extract_tsquery(mcx, qr).unwrap();
    let map = out.map_item_operand.as_slice();
    assert_eq!(
        crate::gin_tsquery_triconsistent(mcx, &[GIN_TRUE, GIN_TRUE], qr, map).unwrap(),
        GIN_TRUE
    );
    assert_eq!(
        crate::gin_tsquery_triconsistent(mcx, &[GIN_TRUE, GIN_MAYBE], qr, map).unwrap(),
        GIN_MAYBE
    );
    assert_eq!(
        crate::gin_tsquery_triconsistent(mcx, &[GIN_FALSE, GIN_MAYBE], qr, map).unwrap(),
        GIN_FALSE
    );
}
