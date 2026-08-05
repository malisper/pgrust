use ::adt_tsvector_core::io::tsvector_in_core;
use ::adt_tsvector_core::layout::TsVec;
use ::adt_tsvector_core::op::ts_match_vq_core;
use ::adt_tsvector_core::query::TsQueryRef;
// tsvector_op.c's TS_execute walks call CHECK_FOR_INTERRUPTS(); the seam has no
// default, so unit tests must install the no-op leg.
fn cfi_installed() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| ::postgres_seams::check_for_interrupts::set(|| Ok(())));
}

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
    cfi_installed();
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
            // leaves NO headroom and reddens the fleet's dev profile.
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

/// MaxAllocSize ceiling (task #85 sibling sweep, tsquery arm): C's tsqueryout
/// grows its INFIX buffer via RESIZEBUF — buflen doubles (top-level initial
/// 32, sub-buffer 16) and repallocs, so once the needed output crosses 2^29
/// bytes the doubled request is 2^30 = 1073741824 > MaxAllocSize (0x3FFF_FFFF)
/// and repalloc raises the CATCHABLE "invalid memory alloc request size
/// 1073741824". Pre-fix, the port's infallible PgVec growth rode past that
/// point and ABORTED the process at the allocator ceiling instead of raising.
///
/// A balanced AND tree keeps recursion at depth 18 (a left-deep chain of the
/// same size would trip the stack guard's 54001 first, masking this path):
/// 2^18 leaves of a shared 2046-byte operand deparse to
/// 2^18*2048 + (2^18-1)*3 = 537,657,341 bytes > 2^29 = 536,870,912.
#[test]
fn tsquery_out_over_ceiling_raises_c_repalloc_error() {
    use ::adt_tsvector_core::query::{Item, Operand, Operator, OP_AND};

    let ctx = MemoryContext::new("t85");
    let mcx = ctx.mcx();
    let oplen = 2046usize;
    let mut pool = std::vec![b'x'; oplen];
    pool.push(0);
    // Every leaf shares the pool's single operand (distance 0); infix never
    // reads valcrc or Operator.left, so those stay 0.
    let leaf =
        Item::Val(Operand { weight: 0, prefix: false, valcrc: 0, length: oplen, distance: 0 });
    fn gen(items: &mut std::vec::Vec<Item>, depth: u32, leaf: Item) {
        if depth == 0 {
            items.push(leaf);
        } else {
            items.push(Item::Opr(Operator { oper: OP_AND, distance: 0, left: 0 }));
            gen(items, depth - 1, leaf);
            gen(items, depth - 1, leaf);
        }
    }
    let mut items: std::vec::Vec<Item> = std::vec::Vec::with_capacity((1 << 19) - 1);
    gen(&mut items, 18, leaf);
    let img = crate::parse::build_query_image(mcx, &items, &pool).expect("image builds");
    let err = tsquery_out_core(mcx, TsQueryRef { payload: &img[4..] })
        .expect_err("over-ceiling tsquery output must raise C's repalloc refusal");
    assert_eq!(err.message(), "invalid memory alloc request size 1073741824");
}

// ---------------------------------------------------------------------------
// qtn_sort tie re-witness (laneaf closeout, task #135).
//
// C QTNSort (tsquery_util.c:163) sorts the QTNode* child array with qsort ==
// pg_qsort (port.h), and its equal-key output order is USER-VISIBLE:
// QTNodeCompare ignores operand weight/prefix, so same-lexeme different-
// payload children tie while being image-distinct, and ts_rewrite emits them
// in pg_qsort's tie order (fuzz/divergences/tsqrw_diff/FINDINGS-qsort-tie.md,
// docker-18.3 adjudicated). qtn_sort therefore runs the canonical
// pg_qsort_arg over an index proxy (util.rs). Witness structure per
// GL-PARMERGE-1: within-tie ORDER is the ratified non-surface for equal
// elements — the multiset gate is the always-true witness — and where the
// order IS observable (>= 7 tie-carrying children through ts_rewrite's sort)
// the gate is byte-exact against adjudicated PostgreSQL 18.3 output.
// ---------------------------------------------------------------------------

use ::adt_tsvector_core::query::Item as QItem;

use crate::util::{qt2qtn, qtn2qt, qtn_binary, qtn_sort, qtn_ternary, qtnode_compare, QtNode};

/// The QTNSort-visible slice of the ts_rewrite pipeline (fc_tsquery_rewrite
/// with a never-matching pattern): parse -> QT2QTN -> QTNTernary -> QTNSort ->
/// QTNBinary -> QTN2QT -> out. findsubquery is identity when nothing matches,
/// so this reproduces exactly what PostgreSQL prints for
/// ts_rewrite(q, 'q'::tsquery, 'r'::tsquery) with 'q' absent from the input.
fn sortview(input: &str) -> String {
    let ctx = MemoryContext::new("t135");
    let mcx = ctx.mcx();
    let img = tsquery_in_core(mcx, input.as_bytes(), None)
        .expect("parse ok")
        .expect("no soft error");
    let mut tree = qt2qtn(mcx, TsQueryRef { payload: &img[4..] }, 0).expect("qt2qtn");
    qtn_ternary(&mut tree).expect("ternary");
    qtn_sort(&mut tree).expect("sort");
    qtn_binary(mcx, &mut tree).expect("binary");
    let out_img = qtn2qt(mcx, &tree).expect("qtn2qt");
    let out = tsquery_out_core(mcx, TsQueryRef { payload: &out_img[4..] }).expect("out ok");
    String::from_utf8(out[..out.len() - 1].to_vec()).expect("utf8")
}

/// Top-level OR term multiset of a printed tsquery (flat OR inputs only).
fn term_multiset(printed: &str) -> std::vec::Vec<String> {
    let mut v: std::vec::Vec<String> = printed.split(" | ").map(|s| s.to_string()).collect();
    v.sort();
    v
}

/// OBSERVABLE-ORDER GATE (byte-exact, adjudicated): the two FINDINGS-qsort-tie
/// witnesses, docker postgres:18.3-adjudicated on 2026-07-31. Both carry a
/// same-lexeme different-weight tie pair in a 7-child OR node — exactly the
/// regime (nchild >= 7, not presorted) where pg_qsort's tie order departs
/// from a stable sort. Swapping the pair in the input swaps the output pair;
/// a stable sort emits 'a':A first in the first case (the shipped pre-fix
/// divergence) and fails this test.
#[test]
fn qtn_sort_tie_order_matches_adjudicated_pg() {
    assert_eq!(
        sortview("b | c | d | a:A | e | f | a:B"),
        "'a':B | 'e' | 'c' | 'b' | 'f' | 'a':A | 'd'"
    );
    assert_eq!(
        sortview("b | c | d | a:B | e | f | a:A"),
        "'a':A | 'e' | 'c' | 'b' | 'f' | 'a':B | 'd'"
    );
}

/// Snapshot of the payload bits qtnode_compare IGNORES but the image keeps:
/// (word bytes, weight, prefix). Equal-compare children differing here are
/// the tie class whose placement the index proxy must get C-exact.
fn val_descriptor(n: &QtNode<'_>) -> (std::vec::Vec<u8>, u8, bool) {
    match n.item {
        QItem::Val(op) => (n.word.to_vec(), op.weight, op.prefix),
        _ => panic!("fixture children must be operands"),
    }
}

/// FULL-TIE + MIXED-TIE FIXTURES across the sort_template regimes (n < 7
/// insertion sort, 7 <= n <= 40 med-of-3, n > 40 med-of-9) — two gates:
///
///   (a) sorted-multiset equality (the ratified non-surface witness): the
///       sorted output is a permutation of the input terms;
///   (b) permutation-application exactness: qtn_sort's placed order equals
///       the canonical pg_qsort_arg permutation of the pre-sort children
///       under qtnode_compare — i.e. the index proxy's invert-and-scatter
///       step is the identity transform on what pg_qsort decided. (C-order
///       exactness itself is anchored by the adjudicated witnesses above and
///       by the tsqrw_diff C oracle, which vendors PG's own sort_template
///       instantiation.)
#[test]
fn qtn_sort_tie_fixtures_multiset_and_proxy_exact() {
    let tie_pool = ["a:A", "a:B", "a:C", "a:D", "a", "a:*", "a:AB", "a:CD", "a:ABCD"];
    let word_pool = ["b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"];
    let mut fixtures: std::vec::Vec<String> = std::vec::Vec::new();
    // All-equal keys: every child ties with every other.
    for n in [2usize, 5, 6, 7, 8, 13, 40, 41, 45] {
        let terms: std::vec::Vec<&str> = (0..n).map(|i| tie_pool[i % tie_pool.len()]).collect();
        fixtures.push(terms.join(" | "));
    }
    // Mixed-tie groups: distinct lexemes interleaved with two tie families.
    for n in [6usize, 7, 9, 13, 41] {
        let mut terms: std::vec::Vec<String> = std::vec::Vec::new();
        for i in 0..n {
            terms.push(match i % 3 {
                0 => tie_pool[(i / 3) % tie_pool.len()].to_string(),
                1 => word_pool[i % word_pool.len()].to_string(),
                _ => format!("{}:{}", word_pool[i % word_pool.len()], ["A", "B"][(i / 3) % 2]),
            });
        }
        fixtures.push(terms.join(" | "));
    }

    for input in &fixtures {
        // Gate (a): multiset equality, order-insensitive (non-surface).
        assert_eq!(
            term_multiset(&sortview(input)),
            term_multiset(&roundtrip(input)),
            "multiset gate: {input}"
        );

        // Gate (b): proxy-applied order == canonical pg_qsort_arg order.
        let ctx = MemoryContext::new("t135b");
        let mcx = ctx.mcx();
        let img = tsquery_in_core(mcx, input.as_bytes(), None).unwrap().unwrap();
        let mut tree = qt2qtn(mcx, TsQueryRef { payload: &img[4..] }, 0).unwrap();
        qtn_ternary(&mut tree).unwrap();
        let pre: std::vec::Vec<_> = tree.children.iter().map(val_descriptor).collect();
        let mut idx: std::vec::Vec<u32> = (0..tree.children.len() as u32).collect();
        {
            let children = &tree.children;
            ::pg_qsort::pg_qsort_arg(&mut idx, |&a, &b| {
                qtnode_compare(&children[a as usize], &children[b as usize])
            })
            .unwrap();
        }
        qtn_sort(&mut tree).unwrap();
        let got: std::vec::Vec<_> = tree.children.iter().map(val_descriptor).collect();
        let expect: std::vec::Vec<_> =
            idx.iter().map(|&k| pre[k as usize].clone()).collect();
        assert_eq!(got, expect, "proxy-exactness gate: {input}");
    }
}

/// qtn_sort's PgResult contract: a stack-depth error raised below it (its own
/// per-level entry guard, matching C QTNSort's check_stack_depth) surfaces as
/// Err(54001), never a panic or an abort, and the tree stays a valid
/// permutation. Frame-pad recursion makes the trip deterministic under a
/// floor-level max_stack_depth regardless of profile frame sizes.
#[test]
fn qtn_sort_stack_error_propagates_as_54001() {
    // ERRCODE_STATEMENT_TOO_COMPLEX == MAKE_SQLSTATE("54001").
    const STATEMENT_TOO_COMPLEX: i32 = 5 + (4 << 6) + (1 << 24);
    #[inline(never)]
    fn descend(depth: usize, tree: &mut QtNode<'_>) -> ::types_error::PgResult<()> {
        let pad = [0u8; 512];
        std::hint::black_box(&pad);
        if depth == 0 { qtn_sort(tree) } else { descend(depth - 1, tree) }
    }
    let h = std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(|| {
            let ctx = MemoryContext::new("t135c");
            let mcx = ctx.mcx();
            let img = tsquery_in_core(mcx, b"b | c | a:A | a:B | d | e | f", None)
                .unwrap()
                .unwrap();
            let mut tree = qt2qtn(mcx, TsQueryRef { payload: &img[4..] }, 0).unwrap();
            qtn_ternary(&mut tree).unwrap();
            // Base at THIS frame; 1kB limit; 32 padded frames (> 16kB) below.
            ::stack_depth::set_stack_base();
            ::stack_depth::assign_max_stack_depth(1);
            let err = descend(32, &mut tree).expect_err("guard must trip");
            ::stack_depth::assign_max_stack_depth(2048);
            assert_eq!(err.sqlstate().0, STATEMENT_TOO_COMPLEX);
            // Untouched-on-error: still the 7 original children, all operands.
            assert_eq!(tree.children.len(), 7);
        })
        .unwrap();
    h.join().expect("qtn_sort must return Err, not unwind");
}
