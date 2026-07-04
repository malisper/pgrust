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
