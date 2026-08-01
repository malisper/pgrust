use ::adt_tsvector_core::io::tsvector_in_core;
use ::adt_tsvector_core::layout::TsVec;
use ::adt_tsvector_core::op::ts_match_vq_core;
use ::adt_tsvector_core::query::TsQueryRef;
use ::mcx::{MemoryContext, Mcx};

use crate::io::{tsq_mcontains_core, tsquery_in_core, tsquery_out_core, tsquerytree_core};

fn roundtrip(input: &str) -> String {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = tsquery_in_core(mcx, input.as_bytes(), None)
        .expect("parse ok")
        .expect("no soft error");
    let out = tsquery_out_core(mcx, TsQueryRef { payload: &img[4..] }).expect("out ok");
    String::from_utf8(out[..out.len() - 1].to_vec()).expect("utf8")
}

fn parse_err(input: &str) -> String {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let msg = match tsquery_in_core(mcx, input.as_bytes(), None) {
        Err(e) => e.message().to_string(),
        Ok(_) => panic!("expected error for {input:?}"),
    };
    msg
}

#[test]
fn tsquery_io_matrix() {
    assert_eq!(roundtrip("1"), "'1'");
    assert_eq!(roundtrip("1 "), "'1'");
    assert_eq!(roundtrip(" 1"), "'1'");
    assert_eq!(roundtrip("'1 2'"), "'1 2'");
    assert_eq!(roundtrip("!1"), "!'1'");
    assert_eq!(roundtrip("1|2"), "'1' | '2'");
    assert_eq!(roundtrip("1|!2"), "'1' | !'2'");
    assert_eq!(roundtrip("!1|2"), "!'1' | '2'");
    assert_eq!(roundtrip("!(!1|!2)"), "!( !'1' | !'2' )");
    assert_eq!(roundtrip("!(1|2)"), "!( '1' | '2' )");
    assert_eq!(roundtrip("1&2"), "'1' & '2'");
    assert_eq!(roundtrip("!1&2"), "!'1' & '2'");
    assert_eq!(roundtrip("(1&2)"), "'1' & '2'");
    assert_eq!(roundtrip("1&(2)"), "'1' & '2'");
    assert_eq!(roundtrip("!(1&2)"), "!( '1' & '2' )");
    assert_eq!(roundtrip("1|2&3"), "'1' | '2' & '3'");
    assert_eq!(roundtrip("(1|2)&3"), "( '1' | '2' ) & '3'");
    assert_eq!(roundtrip("1|2&!3"), "'1' | '2' & !'3'");
    assert_eq!(roundtrip("!1|2&3"), "!'1' | '2' & '3'");
    assert_eq!(roundtrip("1|(2|(4|(5|6)))"), "'1' | '2' | '4' | '5' | '6'");
    assert_eq!(roundtrip("1|2|4|5|6"), "'1' | '2' | '4' | '5' | '6'");
    assert_eq!(roundtrip("1&(2&(4&(5&6)))"), "'1' & '2' & '4' & '5' & '6'");
    assert_eq!(roundtrip("1&(2&(4&(5|6)))"), "'1' & '2' & '4' & ( '5' | '6' )");
    assert_eq!(roundtrip("1&(2&(4&(5|!6)))"), "'1' & '2' & '4' & ( '5' | !'6' )");
    assert_eq!(roundtrip("1<->2"), "'1' <-> '2'");
    assert_eq!(roundtrip("1 <2> 2"), "'1' <2> '2'");
    assert_eq!(roundtrip("(1&2)<->3"), "( '1' & '2' ) <-> '3'");
    assert_eq!(roundtrip("1<->(2&3)"), "'1' <-> ( '2' & '3' )");
    assert_eq!(roundtrip("(1<->2)<->3"), "'1' <-> '2' <-> '3'");
    assert_eq!(roundtrip("1<->(2<->3)"), "'1' <-> ( '2' <-> '3' )");
    assert_eq!(roundtrip("a:* & nbb:*ac | doo:a* | goo"), "'a':* & 'nbb':*AC | 'doo':*A | 'goo'");
    assert_eq!(parse_err("1|"), "no operand in tsquery: \"1|\"");
    assert_eq!(parse_err("|2"), "syntax error in tsquery: \"|2\"");
}

#[test]
fn tsquery_soft_error() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut esc = ::types_error::SoftErrorContext::new(true);
    let res = tsquery_in_core(mcx, b"foo!bar", Some(&mut esc)).expect("soft path");
    assert!(res.is_none());
    assert!(esc.error_occurred());
}

fn q<'a>(mcx: Mcx<'a>, s: &str) -> TsQueryRef<'a> {
    let img = tsquery_in_core(mcx, s.as_bytes(), None).unwrap().unwrap();
    TsQueryRef { payload: &img.leak()[4..] }
}

fn v<'a>(mcx: Mcx<'a>, s: &str) -> TsVec<'a> {
    let img = tsvector_in_core(mcx, s.as_bytes(), None).unwrap().unwrap();
    TsVec { payload: &img.leak()[4..] }
}

#[test]
fn match_matrix() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let doc = v(mcx, "a b:89  ca:23A,64b d:34c");
    for (query, want) in [
        ("d:AC & ca", true),
        ("d:AC & ca:B", true),
        ("d:AC & ca:A", true),
        ("d:AC & ca:C", false),
        ("d:AC & ca:CB", true),
        ("d:AC & c:*C", false),
        ("d:AC & c:*CB", true),
    ] {
        assert_eq!(ts_match_vq_core(mcx, doc, q(mcx, query)).unwrap(), want, "{query}");
    }

    let doc2 = v(mcx, "wa:1D wb:2A");
    assert!(ts_match_vq_core(mcx, doc2, q(mcx, "w:*D & w:*A")).unwrap());
    assert!(ts_match_vq_core(mcx, doc2, q(mcx, "w:*D <-> w:*A")).unwrap());
    let doc3 = v(mcx, "wa:1A wb:2D");
    assert!(!ts_match_vq_core(mcx, doc3, q(mcx, "w:*D <-> w:*A")).unwrap());
    let doc4 = v(mcx, "wa:1A");
    assert!(ts_match_vq_core(mcx, doc4, q(mcx, "w:*A")).unwrap());
    assert!(!ts_match_vq_core(mcx, doc4, q(mcx, "w:*D")).unwrap());
    assert!(!ts_match_vq_core(mcx, doc4, q(mcx, "!w:*A")).unwrap());
    assert!(ts_match_vq_core(mcx, doc4, q(mcx, "!w:*D")).unwrap());

    let phrase_doc = v(mcx, "1:1 2:2 3:3 4:4");
    assert!(ts_match_vq_core(mcx, phrase_doc, q(mcx, "1 <-> 2 <-> 3")).unwrap());
    assert!(ts_match_vq_core(mcx, phrase_doc, q(mcx, "(1 <-> 2) <-> 3")).unwrap());
    assert!(ts_match_vq_core(mcx, phrase_doc, q(mcx, "1 <-> (2 <-> 3)")).unwrap());
    assert!(!ts_match_vq_core(mcx, phrase_doc, q(mcx, "1 <2> (2 <-> 3)")).unwrap());

    let ab = v(mcx, "a:1 b:2");
    assert!(ts_match_vq_core(mcx, ab, q(mcx, "a <-> b")).unwrap());
    assert!(!ts_match_vq_core(mcx, ab, q(mcx, "a <0> b")).unwrap());
    assert!(ts_match_vq_core(mcx, ab, q(mcx, "a <1> b")).unwrap());
    assert!(!ts_match_vq_core(mcx, ab, q(mcx, "a <2> b")).unwrap());
    let ab3 = v(mcx, "a:1 b:3");
    assert!(!ts_match_vq_core(mcx, ab3, q(mcx, "a <-> b")).unwrap());
    assert!(ts_match_vq_core(mcx, ab3, q(mcx, "a <2> b")).unwrap());
    assert!(ts_match_vq_core(mcx, ab3, q(mcx, "a <0> a:*")).unwrap());
}

#[test]
fn mcontains() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert!(tsq_mcontains_core(mcx, q(mcx, "1&(2&(4&(5&6)))"), q(mcx, "2&4")).unwrap());
    assert!(!tsq_mcontains_core(mcx, q(mcx, "1&(2&(4&(5&6)))"), q(mcx, "3&4")).unwrap());
}

#[test]
fn querytree() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t = tsquerytree_core(mcx, q(mcx, "!1&2")).unwrap();
    assert_eq!(&t[..], b"'2'");
    let t = tsquerytree_core(mcx, q(mcx, "1&(2&(4&(5&6)))")).unwrap();
    assert_eq!(&t[..], b"'1' & '2' & '4' & '5' & '6'");
}

/// p1-deadguard REGRESSION (release blocker, task #62): the whole tsquery /
/// tsvector recursion family shipped with NO stack guard at all — not even a
/// dead frame-count cap. C guards every one of these walks with
/// check_stack_depth() (24 call sites across tsquery.c, tsquery_util.c,
/// tsquery_cleanup.c, tsquery_rewrite.c and tsvector_op.c), which measures
/// actual stack BYTES against max_stack_depth and raises
/// ERRCODE_STATEMENT_TOO_COMPLEX (54001).
///
/// Without it, deep input overflows the thread stack and the Rust runtime
/// aborts the PROCESS. pgrust is thread-per-backend, so that kills every
/// session, not just the offending one.
///
/// Measured pre-fix (local --release, aarch64 macOS, 8 MiB worker stack):
///   'a&a&…&a'          survives 7000, ABORTS at 8000  (~1120 bytes/frame)
///   '((((…a…))))'      survives 6000, ABORTS at 7000  (~1290 bytes/frame)
/// Both are plain unprivileged casts of a `repeat()` literal. C 18.3 on the
/// same inputs raises 54001 and never dies.
///
/// Runs each probe in a subprocess because a stack overflow aborts the process.
#[test]
fn tsquery_deep_recursion_raises_54001_and_does_not_abort() {
    // (shape, nesting depth) — every one of these ABORTED before the fix.
    // ('!' repetition is NOT in this set: NOT pushes onto makepol's 32-deep
    // operator stack, so C and pgrust both reject it long before any recursion.)
    const CASES: [(&str, usize); 7] = [
        ("and", 8000),
        ("and", 20000),
        ("and", 100_000),
        ("paren", 7000),
        ("paren", 20000),
        ("paren", 100_000),
        ("phrase", 20000),
    ];
    if let (Ok(d), Ok(kind)) =
        (std::env::var("TSQ_STACK_PROBE_DEPTH"), std::env::var("TSQ_STACK_PROBE_KIND"))
    {
        let depth: usize = d.parse().unwrap();
        let s: String = match kind.as_str() {
            // findoprnd_recurse / infix / qt2qtn: one frame per tree level.
            "and" => format!("{}a", "a&".repeat(depth)),
            // makepol: one frame per '('.
            "paren" => format!("{}a{}", "(".repeat(depth), ")".repeat(depth)),
            "phrase" => format!("{}a", "a<->".repeat(depth)),
            other => panic!("bad probe kind {other}"),
        };
        let h = std::thread::Builder::new()
            // Production HEADROOM: an 8 MiB worker stack paired with
            // max_stack_depth = 2048 kB. Pairing a 2 MiB stack with 2048 kB
            // leaves NO headroom and reddens the CI cluster's dev profile.
            .stack_size(8 << 20)
            .spawn(move || {
                // A backend thread records its stack base at spawn (C: main()).
                // Without this, stack_is_too_deep() short-circuits on base == 0
                // and every guard below is INERT — the test would be vacuous.
                ::stack_depth::set_stack_base();
                ::stack_depth::assign_max_stack_depth(2048);
                let ctx = MemoryContext::new("t");
                let mcx = ctx.mcx();
                let img = tsquery_in_core(mcx, s.as_bytes(), None)?.expect("no soft error");
                let out = tsquery_out_core(mcx, TsQueryRef { payload: &img[4..] })?;
                Ok::<usize, Box<::types_error::PgError>>(out.len())
            })
            .unwrap();
        match h.join().expect("parser thread must not panic") {
            Ok(n) => eprintln!("PROBE OK {n}"),
            Err(e) => eprintln!("PROBE ERR {}", e.sqlstate().0),
        }
        return;
    }
    // ERRCODE_STATEMENT_TOO_COMPLEX == MAKE_SQLSTATE("54001").
    const STATEMENT_TOO_COMPLEX: u32 = 5 + (4 << 6) + (1 << 24);
    let exe = std::env::current_exe().unwrap();
    for (kind, depth) in CASES {
        let out = std::process::Command::new(&exe)
            .args([
                "--exact",
                "--nocapture",
                "tests::tsquery_deep_recursion_raises_54001_and_does_not_abort",
            ])
            .env("TSQ_STACK_PROBE_KIND", kind)
            .env("TSQ_STACK_PROBE_DEPTH", depth.to_string())
            .output()
            .unwrap();
        let se = String::from_utf8_lossy(&out.stderr);
        let line = se.lines().find(|l| l.starts_with("PROBE")).unwrap_or_else(|| {
            panic!("{kind}/{depth}: process died without a verdict (stack overflow): {se}")
        });
        // The depth at which the guard trips is a function of frame size and is
        // NOT a comparison surface against C. That the process SURVIVES and
        // reports 54001 rather than aborting is.
        assert_eq!(
            line,
            format!("PROBE ERR {STATEMENT_TOO_COMPLEX}"),
            "{kind}/{depth}: expected a clean 54001, got {line:?}"
        );
    }
}
