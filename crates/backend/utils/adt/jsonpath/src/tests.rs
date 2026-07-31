//! Round-trip tests against vectors extracted from C 18.3 regress
//! expected/jsonpath.out (in/out canonical forms + error texts).

use std::sync::Once;

use mcx::MemoryContext;
use types_error::SoftErrorContext;

use crate::path::{jsonpath_in, jsonpath_out, JSONPATH_LAX, JSONPATH_VERSION};

use crate::vectors::{ERR_VECTORS, OK_VECTORS};

fn setup() {
    let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
    });
}

fn out_text(image: &[u8]) -> String {
    let cx = MemoryContext::new("jsonpath test out");
    let v = jsonpath_out(cx.mcx(), image).expect("jsonpath_out");
    assert_eq!(v.last(), Some(&0));
    String::from_utf8(v[..v.len() - 1].to_vec()).expect("utf8 output")
}

#[test]
fn regress_ok_vectors_round_trip() {
    setup();
    for (input, expected) in OK_VECTORS {
        let cx = MemoryContext::new("jsonpath test");
        let image = jsonpath_in(cx.mcx(), input.as_bytes(), None)
            .unwrap_or_else(|e| panic!("jsonpath_in({input:?}): {}", e.message()))
            .expect("hard path returns Some");
        let out = out_text(&image);
        assert_eq!(&out, expected, "canonical form of {input:?}");

        // The canonical form re-parses to itself (regress does the same via
        // the text cast round trip).
        let cx2 = MemoryContext::new("jsonpath test 2");
        let image2 = jsonpath_in(cx2.mcx(), out.as_bytes(), None)
            .unwrap_or_else(|e| panic!("re-parse {out:?}: {}", e.message()))
            .expect("hard path returns Some");
        assert_eq!(out_text(&image2), out, "idempotent canonical form of {input:?}");
    }
}

#[test]
fn regress_err_vectors() {
    setup();
    for (input, msg, detail) in ERR_VECTORS {
        let cx = MemoryContext::new("jsonpath test err");
        let err = match jsonpath_in(cx.mcx(), input.as_bytes(), None) {
            Err(e) => e,
            Ok(v) => panic!(
                "expected error {msg:?} for {input:?}, got {:?}",
                v.map(|img| out_text(&img))
            ),
        };
        assert_eq!(err.message(), *msg, "message for {input:?}");
        if let Some(detail) = detail {
            assert_eq!(err.detail(), Some(*detail), "detail for {input:?}");
        }
    }
}

#[test]
fn soft_errors_are_recorded_not_raised() {
    setup();
    for (input, msg, _detail) in ERR_VECTORS {
        let cx = MemoryContext::new("jsonpath test soft");
        let mut esc = SoftErrorContext::new(true);
        let res = jsonpath_in(cx.mcx(), input.as_bytes(), Some(&mut esc))
            .unwrap_or_else(|e| panic!("soft parse of {input:?} raised: {}", e.message()));
        assert!(res.is_none(), "soft error for {input:?}");
        assert!(esc.error_occurred(), "escontext set for {input:?}");
        assert_eq!(esc.error().expect("saved error").message(), *msg, "{input:?}");
    }
}

// Bison expr/predicate class + method-keyword disambiguation + scanner
// edge cases from the audit (C 18.3 behavior derived from the grammar/flex
// rules; not present in regress).
static EXTRA_OK: &[(&str, &str)] = &[
    ("$.type", "$.\"type\""),
    ("$.size", "$.\"size\""),
    ("$.datetime", "$.\"datetime\""),
    ("$.decimal", "$.\"decimal\""),
    ("$.timestamp_tz", "$.\"timestamp_tz\""),
    ("(1).type()", "(1).type()"),
];

static EXTRA_ERR: &[(&str, &str)] = &[
    ("1 && 2", "syntax error at or near \"&&\" of jsonpath input"),
    ("$ ? (@)", "syntax error at or near \")\" of jsonpath input"),
    ("$?(1)", "syntax error at or near \")\" of jsonpath input"),
    ("exists(1 == 2)", "syntax error at or near \"==\" of jsonpath input"),
    ("!(1)", "syntax error at or near \")\" of jsonpath input"),
    // yytext for a keyword token emitted by the xnq {blank}+ rule is the
    // blank run (flex), hence " " not "is".
    ("(1) is unknown", "syntax error at or near \" \" of jsonpath input"),
    ("$[1 == 1]", "syntax error at or near \"==\" of jsonpath input"),
    ("(1 == 1) + 2", "syntax error at or near \"+\" of jsonpath input"),
    ("exists($.a) + 1", "syntax error at or near \"+\" of jsonpath input"),
    ("-(1 == 1)", "syntax error at end of jsonpath input"),
    ("1 == !(true)", "syntax error at or near \"!\" of jsonpath input"),
    ("\"abc\\", "unexpected end after backslash at or near \"\\\" of jsonpath input"),
    ("\"a\\\nb\"", "unexpected end after backslash at or near \"\\\" of jsonpath input"),
    ("1 2 \"x", "syntax error at or near \"2\" of jsonpath input"),
];

#[test]
fn audit_extra_vectors() {
    setup();
    for (input, expected) in EXTRA_OK {
        let cx = MemoryContext::new("jsonpath extra ok");
        let image = jsonpath_in(cx.mcx(), input.as_bytes(), None)
            .unwrap_or_else(|e| panic!("jsonpath_in({input:?}): {}", e.message()))
            .expect("hard path returns Some");
        assert_eq!(&out_text(&image), expected, "canonical form of {input:?}");
    }
    for (input, msg) in EXTRA_ERR {
        let cx = MemoryContext::new("jsonpath extra err");
        let res = jsonpath_in(cx.mcx(), input.as_bytes(), None);
        match res {
            Err(e) => assert_eq!(e.message(), *msg, "message for {input:?}"),
            Ok(v) => panic!(
                "expected error {msg:?} for {input:?}, got {:?}",
                v.map(|img| out_text(&img))
            ),
        }
    }
}

#[test]
fn header_flags() {
    setup();
    let cx = MemoryContext::new("jsonpath header");
    let lax = jsonpath_in(cx.mcx(), b"$.a", None).unwrap().unwrap();
    let hdr = u32::from_ne_bytes([lax[4], lax[5], lax[6], lax[7]]);
    assert_eq!(hdr, JSONPATH_VERSION | JSONPATH_LAX);
    let strict = jsonpath_in(cx.mcx(), b"strict $.a", None).unwrap().unwrap();
    let hdr = u32::from_ne_bytes([strict[4], strict[5], strict[6], strict[7]]);
    assert_eq!(hdr, JSONPATH_VERSION);
    // Varlena length header covers the whole image.
    let word = u32::from_ne_bytes([lax[0], lax[1], lax[2], lax[3]]);
    assert_eq!((word >> 2) as usize, lax.len());
}

#[test]
fn send_recv_round_trip() {
    setup();
    let cx = MemoryContext::new("jsonpath sendrecv");
    let mcx = cx.mcx();
    for (input, expected) in OK_VECTORS.iter().take(40) {
        let image = jsonpath_in(mcx, input.as_bytes(), None).unwrap().unwrap();
        let sent = crate::path::jsonpath_send(mcx, &image).unwrap();
        let bytes = sent.data();
        assert_eq!(bytes[0], 1, "version byte for {input:?}");
        assert_eq!(
            core::str::from_utf8(&bytes[1..]).unwrap(),
            *expected,
            "send payload for {input:?}"
        );
        let mut msg = stringinfo::StringInfo::from_vec(mcx::slice_in(mcx, bytes).unwrap()).unwrap();
        let recv = crate::path::jsonpath_recv(mcx, &mut msg).unwrap();
        assert_eq!(out_text(&recv), *expected, "recv round trip for {input:?}");
    }
}

// ---------------------------------------------------------------------------
// Parser recursion guard (gram.rs check_depth) — regression for the
// p1-laneaa process-abort bug: the recursive-descent parser used to have NO
// depth guard, so a deep-enough nesting overflowed the native backend stack
// and SIGABRT'd the whole thread-per-backend process. With the guard, deep
// nesting is a clean ERRCODE_STATEMENT_TOO_COMPLEX (54001), soft-errorable
// like the parser's other errors. (C 18.3 errors 42601 "memory exhausted" at
// bison's YYMAXDEPTH instead — measured paren flip at N=9996 on docker
// postgres:18.3; the errcode/threshold residual is documented on
// gram.rs::check_depth and in fuzz/README-TODO-jsonpath_diff.md.)
// ---------------------------------------------------------------------------

/// Run `f` on a spawned thread with an explicit SMALL stack (1 MiB) and the
/// stack-depth guard armed exactly as a real backend thread arms it
/// (set_stack_base at thread top; max_stack_depth keeps its thread-local
/// 100kB boot default). Deterministic and cheap: the guard must fire at
/// ~100kB of native stack, far below the 1 MiB the thread actually has —
/// while the pre-fix code overflowed any stack on these inputs.
fn on_guarded_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(move || {
            let _ = stack_depth::set_stack_base();
            setup();
            f()
        })
        .expect("spawn guarded thread")
        .join()
        .expect("guarded parse must return, not abort or panic")
}

fn deep_input(prefix: &str, open: &str, core: &str, close: &str, n: usize) -> Vec<u8> {
    let mut s = Vec::with_capacity(prefix.len() + n * (open.len() + close.len()) + core.len());
    s.extend_from_slice(prefix.as_bytes());
    for _ in 0..n {
        s.extend_from_slice(open.as_bytes());
    }
    s.extend_from_slice(core.as_bytes());
    for _ in 0..n {
        s.extend_from_slice(close.as_bytes());
    }
    s
}

/// (i)+(iv) Every recursion cycle of the parser is bounded: each shape drives
/// a different cycle (parens; !(...) chains, which bypass parse_unary; unary
/// +/- chains, which bypass parse_delimited_predicate; nested array
/// subscripts; nested filters via exists). Pre-fix each of these aborted the
/// process on a small stack; now each returns a clean hard 54001.
#[test]
fn parser_depth_guard_bounds_every_recursion_cycle() {
    const N: usize = 50_000;
    let shapes: Vec<(&str, Vec<u8>)> = vec![
        ("paren", deep_input("", "(", "1", ")", N)),
        ("not-chain", deep_input("$ ? (", "!(", "@ == 1", ")", N).into_iter().chain(*b")").collect()),
        ("unary-chain", deep_input("", "-", "1", "", N)),
        ("subscript", deep_input("", "$[", "0", "]", N)),
        ("filter-exists", deep_input("$", "?(exists(@", "", "))", N)),
    ];
    for (name, input) in shapes {
        let err = on_guarded_thread(move || {
            let cx = MemoryContext::new("depth guard hard");
            let e = match jsonpath_in(cx.mcx(), &input, None) {
                Err(e) => e,
                Ok(v) => panic!("{:?}: expected depth error, got {:?}", &input[..20], v.is_some()),
            };
            e
        });
        assert_eq!(
            err.sqlstate(),
            types_error::ERRCODE_STATEMENT_TOO_COMPLEX,
            "sqlstate for deep {name} shape"
        );
        assert_eq!(err.message(), "stack depth limit exceeded", "message for {name}");
    }
}

/// (ii) SOFT-error mode: the depth error is recorded through the armed
/// escontext (like every other parser error) instead of raising, and is not
/// overwritten by a generic syntax error.
#[test]
fn parser_depth_guard_is_soft_errorable() {
    let input = deep_input("", "(", "$.a", ")", 50_000);
    let (res_is_none, occurred, sqlstate) = on_guarded_thread(move || {
        let cx = MemoryContext::new("depth guard soft");
        let mut esc = SoftErrorContext::new(true);
        let res = jsonpath_in(cx.mcx(), &input, Some(&mut esc))
            .unwrap_or_else(|e| panic!("soft depth error raised hard: {}", e.message()));
        (
            res.is_none(),
            esc.error_occurred(),
            esc.error().map(|e| e.sqlstate()),
        )
    });
    assert!(res_is_none, "soft depth error yields Ok(None)");
    assert!(occurred, "escontext records the depth error");
    assert_eq!(
        sqlstate,
        Some(types_error::ERRCODE_STATEMENT_TOO_COMPLEX),
        "recorded sqlstate"
    );
}

/// (iii) Just below the guard, nesting still parses and canonicalizes exactly
/// as real PG does on the SAME guarded thread — the guard must not perturb
/// the accepted region. (docker postgres:18.3:
/// `select ('((((1))))')::jsonpath` -> `1`;
/// `select ('(($.a))')::jsonpath` -> `$."a"`.)
#[test]
fn parser_depth_guard_below_threshold_round_trips() {
    for (input, expected) in [
        (deep_input("", "(", "1", ")", 8), "1"),
        (deep_input("", "(", "$.a", ")", 8), "$.\"a\""),
        (deep_input("$ ? (", "!(", "@ == 1", ")", 4).into_iter().chain(*b")").collect::<Vec<u8>>(),
         "$?(!(!(!(!(@ == 1)))))"),
    ] {
        let out = on_guarded_thread(move || {
            let cx = MemoryContext::new("depth guard shallow");
            let image = jsonpath_in(cx.mcx(), &input, None)
                .unwrap_or_else(|e| {
                    panic!("below-guard input must parse, got: {}", e.message())
                })
                .expect("hard path returns Some");
            out_text(&image)
        });
        assert_eq!(out, expected);
    }
}

