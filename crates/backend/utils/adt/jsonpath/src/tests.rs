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
