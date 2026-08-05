// tsvector_op.c's TS_execute walks call CHECK_FOR_INTERRUPTS(); the seam has no
// default, so unit tests must install the no-op leg.
fn cfi_installed() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| ::postgres_seams::check_for_interrupts::set(|| Ok(())));
}

use ::mcx::MemoryContext;

use crate::io::{tsvector_in_core, tsvector_out_core};
use crate::layout::TsVec;
use crate::op::*;
use crate::query::TsQueryRef;

fn roundtrip(input: &str) -> String {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = tsvector_in_core(mcx, input.as_bytes(), None)
        .expect("parse ok")
        .expect("no soft error");
    let out = tsvector_out_core(mcx, TsVec { payload: &img[4..] }).expect("out ok");
    String::from_utf8(out[..out.len() - 1].to_vec()).expect("utf8")
}

fn parse_err(input: &str) -> String {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let msg = match tsvector_in_core(mcx, input.as_bytes(), None) {
        Err(e) => e.message().to_string(),
        Ok(_) => panic!("expected error for {input:?}"),
    };
    msg
}

#[test]
fn tsvector_io_matrix() {
    assert_eq!(roundtrip("1"), "'1'");
    assert_eq!(roundtrip("1 "), "'1'");
    assert_eq!(roundtrip(" 1"), "'1'");
    assert_eq!(roundtrip(" 1 "), "'1'");
    assert_eq!(roundtrip("1 2"), "'1' '2'");
    assert_eq!(roundtrip("'1 2'"), "'1 2'");
    assert_eq!(roundtrip("'1 \\'2'"), "'1 ''2'");
    assert_eq!(roundtrip("'1 \\'2'3"), "'1 ''2' '3'");
    assert_eq!(roundtrip("'1 \\'2' 3"), "'1 ''2' '3'");
    assert_eq!(roundtrip("'1 \\'2' ' 3' 4 "), "' 3' '1 ''2' '4'");
    assert_eq!(
        roundtrip(r"'\\as' ab\c ab\\c AB\\\c ab\\\\c"),
        r"'AB\\c' '\\as' 'ab\\\\c' 'ab\\c' 'abc'"
    );
    assert_eq!(roundtrip("'w':4A,3B,2C,1D,5 a:8"), "'a':8 'w':1,2C,3B,4A,5");
    assert_eq!(
        roundtrip("base:7 hidden:6 rebel:1 spaceship:2,33A,34B,35C,36D strike:3"),
        "'base':7 'hidden':6 'rebel':1 'spaceship':2,33A,34B,35C,36 'strike':3"
    );
    assert_eq!(parse_err("'' '1' '2'"), "syntax error in tsvector: \"'' '1' '2'\"");
    assert_eq!(roundtrip("foo"), "'foo'");
}

#[test]
fn tsvector_soft_error() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut esc = ::types_error::SoftErrorContext::new(true);
    let res = tsvector_in_core(mcx, b"''", Some(&mut esc)).expect("soft path");
    assert!(res.is_none());
    assert!(esc.error_occurred());
    assert_eq!(
        esc.error().expect("saved").message(),
        "syntax error in tsvector: \"''\""
    );
}

fn tsv<'a>(mcx: ::mcx::Mcx<'a>, s: &str) -> TsVec<'a> {
    let img = tsvector_in_core(mcx, s.as_bytes(), None).unwrap().unwrap();
    TsVec { payload: &img.leak()[4..] }
}

#[test]
fn tsvector_ops() {
    cfi_installed();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let a = tsv(mcx, "a:3A b:2a");
    let b = tsv(mcx, "ba:1234 a:1B");
    let out = tsvector_concat_core(mcx, a, b).unwrap();
    let s = tsvector_out_core(mcx, TsVec { payload: &out[4..] }).unwrap();
    assert_eq!(&s[..s.len() - 1], b"'a':3A,4B 'b':2A 'ba':1237");

    let v = tsv(mcx, "w:12B w:13* w:12,5,6 a:1,3* a:3 w asd:1dc asd");
    let stripped = tsvector_strip_core(mcx, v).unwrap();
    let s = tsvector_out_core(mcx, TsVec { payload: &stripped[4..] }).unwrap();
    assert_eq!(&s[..s.len() - 1], b"'a' 'asd' 'w'");

    let v = tsv(mcx, "a:1,3A asd:1C w:5,6,12B,13A zxc:81,222A,567");
    let out = tsvector_setweight_core(mcx, v, 1).unwrap();
    let s = tsvector_out_core(mcx, TsVec { payload: &out[4..] }).unwrap();
    assert_eq!(&s[..s.len() - 1], b"'a':1C,3C 'asd':1C 'w':5C,6C,12C,13C 'zxc':81C,222C,567C");

    assert_eq!(silly_cmp_tsvector(a, a), 0);
    assert_ne!(silly_cmp_tsvector(a, b), 0);
}

fn tsq<'a>(mcx: ::mcx::Mcx<'a>, s: &str) -> TsQueryRef<'a> {
    // Test-only: parse via the tsquery crate is unavailable here (dependency
    // direction), so lay out a minimal single-operand query by hand.
    let mut items: Vec<u8> = Vec::new();
    items.extend_from_slice(&1i32.to_ne_bytes());
    let mut raw = [0u8; 12];
    raw[0] = 1;
    let packed = (s.len() as u32 & 0xfff) | (0u32 << 12);
    raw[8..12].copy_from_slice(&packed.to_ne_bytes());
    items.extend_from_slice(&raw);
    items.extend_from_slice(s.as_bytes());
    items.push(0);
    let mut v = ::mcx::vec_with_capacity_in(mcx, items.len()).unwrap();
    v.extend_from_slice(&items);
    TsQueryRef { payload: v.leak() }
}

#[test]
fn match_single_operand() {
    cfi_installed();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = tsv(mcx, "a b:89 ca:23A,64b d:34c");
    assert!(ts_match_vq_core(mcx, v, tsq(mcx, "ca")).unwrap());
    assert!(!ts_match_vq_core(mcx, v, tsq(mcx, "cb")).unwrap());
    let empty_q = {
        let mut v2 = ::mcx::vec_with_capacity_in(mcx, 4).unwrap();
        v2.extend_from_slice(&0i32.to_ne_bytes());
        TsQueryRef { payload: v2.leak() }
    };
    assert!(!ts_match_vq_core(mcx, v, empty_q).unwrap());
}

// Regression: C atoi wrap semantics on tsvector positions (DIVERGENCE-2,
// p1-laneae; ground-truthed postgres:18.3 2026-07-31).
#[test]
fn tsvector_position_atoi_wrap() {
    // (int)20069458489 wraps negative; & 0x3fff = 8761 (real PG: 'b':8761).
    assert_eq!(roundtrip("b:20069458489"), "'b':8761");
    assert_eq!(roundtrip("a b:89,00020069458489"), "'a' 'b':89,8761");
    // (int)4294967296 == 0 -> "wrong position info" error, exactly as C.
    assert!(parse_err("b:4294967296").starts_with("wrong position info"));
    // strtol saturation band (>= 2^63): LONG_MAX -> (int)-1 -> & 0x3fff = 16383.
    assert_eq!(roundtrip("b:99999999999999999999"), "'b':16383");
    // Plain clamp band stays: 16384..2^31-1 -> 16383.
    assert_eq!(roundtrip("b:16384"), "'b':16383");
    assert_eq!(roundtrip("b:2147483647"), "'b':16383");
}

// Regression: recv needSort keeps STORAGE in wire order, sorts entries only
// (KNOWN-DIVERGENCE-1; ground-truthed via binary COPY + pageinspect).
#[test]
fn tsvector_recv_needsort_storage_wire_order() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // wire: nentries=2, "bb" (npos 0), "aa" (npos 0) — out of order.
    let wire: &[u8] = &[0, 0, 0, 2, b'b', b'b', 0, 0, 0, b'a', b'a', 0, 0, 0];
    let mut vec = ::mcx::vec_with_capacity_in::<u8>(mcx, wire.len()).expect("cap");
    ::mcx::vec_append_bytes(&mut vec, wire).expect("append");
    let mut buf = ::stringinfo::StringInfo::from_vec(vec).expect("si");
    let img = crate::io::tsvector_recv_core(mcx, &mut buf).expect("recv ok");
    let v = TsVec { payload: &img[4..] };
    assert_eq!(v.size(), 2);
    // entries sorted: aa first...
    assert_eq!(v.lexeme(v.entry(0)), b"aa");
    assert_eq!(v.lexeme(v.entry(1)), b"bb");
    // ...but storage keeps wire order "bbaa" (C parity: entry(aa).pos = 2).
    assert_eq!(v.entry(0).pos(), 2);
    assert_eq!(v.entry(1).pos(), 0);
    assert_eq!(v.strdata(), b"bbaa");
}

// Regression: uniquePos kept-weight at the 16383 break is decided by
// pg_qsort's equal-position tie order (C qsort, tsvector.c:60); ground-truthed
// on postgres:18.3 2026-07-31. A stable sort produced 'w':...,16383 where real
// PG keeps 'w':...,16383A on the second input.
#[test]
fn tsvector_uniquepos_tie_weight_pg_qsort_parity() {
    assert_eq!(
        roundtrip("w:1,2,3,4,5,6,7,16384,20000A"),
        "'w':1,2,3,4,5,6,7,16383"
    );
    assert_eq!(
        roundtrip("w:1,2,3,4,5,6,7,20000A,16384"),
        "'w':1,2,3,4,5,6,7,16383A"
    );
    assert_eq!(
        roundtrip("w:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,16384,20000A,17000B"),
        "'w':1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,16383"
    );
}

/// Task #85 sibling (ceiling sweep): C tsvectorout pallocs its exact
/// worst-case lenbuf up front, so a tsvector whose worst-case text form
/// exceeds MaxAllocSize raises palloc's CATCHABLE "invalid memory alloc
/// request size {lenbuf}" before emitting anything. The port used an
/// unceilinged heuristic reserve and then grew infallibly (post-allocator-
/// ceiling: an uncatchable abort; pre-ceiling: multi-GB RSS). Entries may
/// share lexeme storage (WordEntry.pos is a 20-bit offset), so a ~2.4MB
/// payload legally declares a >1GB worst case. Pre-fix this test FAILS
/// (the out call returns Ok).
#[test]
fn tsvector_out_over_ceiling_lenbuf_raises_palloc_error() {
    use crate::layout::WordEntry;
    // storage: lexeme "ab" (len 2, shortalign 2) + npos=256 + 256 positions
    let lex_len = 2usize;
    let npos = 256usize;
    let enc_max = ::mbutils::pg_database_encoding_max_length() as usize;
    let per_entry = lex_len * 2 * enc_max + 1 + 7 * npos;
    // number of entries needed for lenbuf > MAX_ALLOC_SIZE
    let n = (::mcx::MAX_ALLOC_SIZE - 2) / (per_entry + 3) + 2;
    let mut payload: Vec<u8> = Vec::with_capacity(4 + n * 4 + 2 + 2 + npos * 2);
    payload.extend_from_slice(&(n as i32).to_ne_bytes());
    let e = WordEntry::new(true, lex_len, 0);
    for _ in 0..n {
        payload.extend_from_slice(&e.0.to_ne_bytes());
    }
    payload.extend_from_slice(b"ab");
    payload.extend_from_slice(&(npos as u16).to_ne_bytes());
    for _ in 0..npos {
        payload.extend_from_slice(&0u16.to_ne_bytes());
    }
    // the exact lenbuf C computes for this value
    let lenbuf: usize = n * 2 + (n - 1) + 2 + n * per_entry;
    assert!(lenbuf > ::mcx::MAX_ALLOC_SIZE);
    let ctx = MemoryContext::new("t85");
    let err = tsvector_out_core(ctx.mcx(), TsVec { payload: &payload })
        .expect_err("worst-case text form above MaxAllocSize must raise palloc's error");
    assert_eq!(err.message(), format!("invalid memory alloc request size {lenbuf}"));
}
