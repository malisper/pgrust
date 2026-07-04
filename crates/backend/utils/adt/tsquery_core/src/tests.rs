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
