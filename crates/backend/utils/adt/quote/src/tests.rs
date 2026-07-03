use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};

use mcx::{Mcx, MemoryContext};

use crate::*;

static QAI: AtomicBool = AtomicBool::new(false);
// quote_identifier reads the GUC; serialize tests that touch it.
static GUC_LOCK: Mutex<()> = Mutex::new(());

fn init() -> std::sync::MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::vars::quote_all_identifiers.install(guc_tables::GucVarAccessors {
            get: || QAI.load(Ordering::Relaxed),
            set: |v| QAI.store(v, Ordering::Relaxed),
        });
    });
    GUC_LOCK.lock().unwrap()
}

fn qi(mcx: Mcx<'_>, s: &[u8]) -> Vec<u8> {
    quote_ident(mcx, s).unwrap().data().to_vec()
}

fn ql(mcx: Mcx<'_>, s: &[u8]) -> Vec<u8> {
    quote_literal(mcx, s).unwrap().data().to_vec()
}

#[test]
fn ident_safe_forms() {
    let _g = init();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for s in [&b"abc"[..], b"_abc", b"a1", b"a_b_c", b"_", b"x0123456789"] {
        assert_eq!(qi(mcx, s), s, "{}", String::from_utf8_lossy(s));
        assert!(matches!(
            quote_identifier(mcx, s).unwrap(),
            QuotedIdent::Plain(_)
        ));
    }
    // Unreserved keywords stay bare.
    for s in [&b"abort"[..], b"zone", b"day", b"insert"] {
        assert_eq!(qi(mcx, s), s);
    }
}

#[test]
fn ident_quoted_forms() {
    let _g = init();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cases: &[(&[u8], &[u8])] = &[
        (b"select", b"\"select\""),
        (b"table", b"\"table\""),
        (b"all", b"\"all\""),
        (b"between", b"\"between\""),
        (b"left", b"\"left\""),
        (b"integer", b"\"integer\""),
        (b"Abc", b"\"Abc\""),
        (b"ABC", b"\"ABC\""),
        (b"1abc", b"\"1abc\""),
        (b"a b", b"\"a b\""),
        (b"a-b", b"\"a-b\""),
        (b"", b"\"\""),
        (b"a\"b", b"\"a\"\"b\""),
        (b"\"", b"\"\"\"\""),
        ("é".as_bytes(), "\"é\"".as_bytes()),
        ("日本語".as_bytes(), "\"日本語\"".as_bytes()),
    ];
    for (input, expect) in cases {
        assert_eq!(qi(mcx, input), *expect, "{}", String::from_utf8_lossy(input));
    }
}

#[test]
fn ident_quote_all_guc() {
    let _g = init();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    QAI.store(true, Ordering::Relaxed);
    let quoted = qi(mcx, b"abc");
    QAI.store(false, Ordering::Relaxed);
    assert_eq!(quoted, b"\"abc\"");
}

#[test]
fn literal_forms() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cases: &[(&[u8], &[u8])] = &[
        (b"abc", b"'abc'"),
        (b"", b"''"),
        (b"it's", b"'it''s'"),
        (b"'", b"''''"),
        (b"\\", b"E'\\\\'"),
        (b"a\\b", b"E'a\\\\b'"),
        (b"a'b\\c", b"E'a''b\\\\c'"),
        (b"line1\nline2", b"'line1\nline2'"),
        ("é日本語".as_bytes(), "'é日本語'".as_bytes()),
        (b"NULL", b"'NULL'"),
    ];
    for (input, expect) in cases {
        assert_eq!(ql(mcx, input), *expect, "{}", String::from_utf8_lossy(input));
    }
}
