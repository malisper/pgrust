use ::mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::ts_locale::TSL_PREFIX;

fn leaked_mcx() -> Mcx<'static> {
    ::pg_locale::set_default_locale_c_for_tests();
    Box::leak(Box::new(MemoryContext::new("dict-test"))).mcx()
}

fn to_lines(mcx: Mcx<'static>, content: &str) -> PgVec<'static, PgVec<'static, u8>> {
    let mut lines = PgVec::new_in(mcx);
    for chunk in content.as_bytes().split_inclusive(|&b| b == b'\n') {
        let mut v = vec_with_capacity_in(mcx, chunk.len()).unwrap();
        v.extend_from_slice(chunk);
        lines.push(v);
    }
    lines
}

#[test]
fn synonym_sample_parse() {
    let mcx = leaked_mcx();
    let lines = to_lines(mcx, include_str!("../fixtures/synonym_sample.syn"));
    let syn = crate::synonym::load_synonyms(mcx, &lines, false).unwrap();
    let find = |k: &[u8]| syn.iter().find(|s| s.input.as_slice() == k);
    assert_eq!(find(b"postgres").unwrap().output.as_slice(), b"pgsql");
    assert_eq!(find(b"gogle").unwrap().output.as_slice(), b"googl");
    let idx = find(b"indices").unwrap();
    assert_eq!(idx.output.as_slice(), b"index");
    assert_eq!(idx.flags, TSL_PREFIX);
    assert!(syn.windows(2).all(|w| w[0].input.as_slice() <= w[1].input.as_slice()));
    assert_eq!(syn.len(), 5);
}

#[test]
fn synonym_case_and_junk_lines() {
    let mcx = leaked_mcx();
    let lines = to_lines(mcx, "OneWord\n\nA B extra\nUPPER lower\n");
    let syn = crate::synonym::load_synonyms(mcx, &lines, false).unwrap();
    assert_eq!(syn.len(), 2);
    assert_eq!(syn[0].input.as_slice(), b"a");
    assert_eq!(syn[0].output.as_slice(), b"b");
    assert_eq!(syn[1].input.as_slice(), b"upper");
    assert_eq!(syn[1].output.as_slice(), b"lower");

    let cs = crate::synonym::load_synonyms(mcx, &lines, true).unwrap();
    assert_eq!(cs[1].input.as_slice(), b"UPPER");
}
