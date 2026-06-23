//! Unit tests for the `tsquery` "small" ADT port.
//!
//! These build small serialized `tsquery` images by hand (the same flat polish
//! layout `parse_tsquery` produces), run the ported toolkit, and check the
//! result. The recursion / interrupt guards are seams; the tests install
//! no-op implementations up front.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;

use ::mcx::MemoryContext;
use ::tsearch::tsearch::{
    QueryItem, QueryOperand, QueryOperator, HDRSIZETQ, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_OPR,
    QI_VAL, QI_VALSTOP,
};

use postgres_seams as tcop;

use crate::cleanup::{clean_NOT, cleanup_tsquery_stopwords};
use crate::rewrite::tsquery_rewrite;
use crate::util::{qt2qtn, qtn2qt, QTNEq, QTNSort, QTNTernary};

const QI_SIZE: usize = 12;

/// Install the recursion / interrupt guard seams as no-ops, once per process.
///
/// These seams are process-global (`OnceLock`-backed). A bare
/// `if !is_installed() { set(..) }` is check-then-act: under parallel
/// `cargo test` two tests can both observe "not installed" and both call
/// `set`, and the second `set` panics ("seam installed twice"). `Once`
/// makes the install atomic and blocks every caller until it completes, so
/// install->use is race-free across all tests in this binary.
fn install_seams() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        tcop::check_stack_depth::set(|| Ok(()));
        tcop::check_for_interrupts::set(|| Ok(()));
    });
}

/// A simple spec for building test tsqueries.
enum Spec {
    Val(&'static [u8], i32), // word, valcrc
    And(Box<Spec>, Box<Spec>),
    Or(Box<Spec>, Box<Spec>),
    Not(Box<Spec>),
    Phrase(i16, Box<Spec>, Box<Spec>),
}

/// Count nodes and total operand length (incl. '\0') of a spec.
fn spec_size(s: &Spec) -> (i32, i32) {
    match s {
        Spec::Val(w, _) => (1, w.len() as i32 + 1),
        Spec::Not(c) => {
            let (n, l) = spec_size(c);
            (n + 1, l)
        }
        Spec::And(a, b) | Spec::Or(a, b) | Spec::Phrase(_, a, b) => {
            let (na, la) = spec_size(a);
            let (nb, lb) = spec_size(b);
            (na + nb + 1, la + lb)
        }
    }
}

/// Emit a spec into items + operand buffers in polish order. PostgreSQL lays a
/// binary node out as `[op, right-subtree, left-subtree]`: child[0] (`in+1`) is
/// the right operand, child[1] (`in+left`) the left. `And(a, b)` reads as
/// "a OP b", so we emit `b` (right) as child[0] and `a` (left) as child[1].
fn emit(s: &Spec, items: &mut Vec<QueryItem>, operand: &mut Vec<u8>) {
    match s {
        Spec::Val(w, crc) => {
            let dist = operand.len() as u32;
            operand.extend_from_slice(w);
            operand.push(0);
            let mut qop = QueryOperand {
                type_: QI_VAL,
                weight: 0,
                prefix: false,
                valcrc: *crc,
                len_dist: 0,
            };
            qop.set_length(w.len() as u32);
            qop.set_distance(dist);
            items.push(QueryItem::Qoperand(qop));
        }
        Spec::Not(c) => {
            let cur = items.len();
            items.push(QueryItem::Qoperator(QueryOperator {
                type_: QI_OPR,
                oper: OP_NOT,
                distance: 0,
                left: 0,
            }));
            // child[0] = in + 1 (the only child)
            emit(c, items, operand);
            let _ = cur;
        }
        Spec::And(a, b) | Spec::Or(a, b) | Spec::Phrase(_, a, b) => {
            let (oper, distance) = match s {
                Spec::And(..) => (OP_AND, 0i16),
                Spec::Or(..) => (OP_OR, 0i16),
                Spec::Phrase(d, ..) => (OP_PHRASE, *d),
                _ => unreachable!(),
            };
            let cur = items.len();
            items.push(QueryItem::Qoperator(QueryOperator {
                type_: QI_OPR,
                oper,
                distance,
                left: 0,
            }));
            // child[0] = right (b), child[1] = left (a) at offset `left`.
            emit(b, items, operand);
            let left = (items.len() - cur) as u32;
            if let QueryItem::Qoperator(o) = &mut items[cur] {
                o.left = left;
            }
            emit(a, items, operand);
        }
    }
}

/// Serialize a spec into the flat `tsquery` varlena image.
fn build(s: &Spec) -> Vec<u8> {
    let (nnode, sumlen) = spec_size(s);
    let mut items: Vec<QueryItem> = Vec::new();
    let mut operand: Vec<u8> = Vec::new();
    emit(s, &mut items, &mut operand);
    assert_eq!(items.len() as i32, nnode);
    assert_eq!(operand.len() as i32, sumlen);

    let len = HDRSIZETQ + (nnode as usize) * QI_SIZE + (sumlen as usize);
    let mut out = vec![0u8; len];
    out[0..4].copy_from_slice(&((len as u32) << 2).to_ne_bytes());
    out[4..8].copy_from_slice(&nnode.to_ne_bytes());
    for (i, it) in items.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        crate::util::encode_record(it, &mut out[base..base + QI_SIZE]);
    }
    let opbase = HDRSIZETQ + (nnode as usize) * QI_SIZE;
    out[opbase..opbase + sumlen as usize].copy_from_slice(&operand);
    out
}

fn tsq_size(q: &[u8]) -> i32 {
    i32::from_ne_bytes([q[4], q[5], q[6], q[7]])
}

#[test]
fn qt2qtn_qtn2qt_roundtrip_single_operand() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let q = build(&Spec::Val(b"cat", 0x1234));
    let tree = qt2qtn(mcx, &q).unwrap();
    assert_eq!(tree.valnode_type(), QI_VAL);
    assert_eq!(&tree.word[..], b"cat");

    let out = qtn2qt(mcx, &tree).unwrap();
    // Round-trip reproduces the same logical query (size 1).
    assert_eq!(tsq_size(&out), 1);
}

#[test]
fn qt2qtn_qtn2qt_roundtrip_and() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let q = build(&Spec::And(
        Box::new(Spec::Val(b"cat", 1)),
        Box::new(Spec::Val(b"dog", 2)),
    ));
    let tree = qt2qtn(mcx, &q).unwrap();
    assert!(tree.is_opr());
    assert_eq!(tree.oper(), OP_AND);
    assert_eq!(tree.nchild(), 2);

    let out = qtn2qt(mcx, &tree).unwrap();
    assert_eq!(tsq_size(&out), 3);
}

#[test]
fn qtnsort_and_qtneq() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // (cat & dog) vs (dog & cat) — after sort they must compare equal.
    let q1 = build(&Spec::And(
        Box::new(Spec::Val(b"cat", 10)),
        Box::new(Spec::Val(b"dog", 20)),
    ));
    let q2 = build(&Spec::And(
        Box::new(Spec::Val(b"dog", 20)),
        Box::new(Spec::Val(b"cat", 10)),
    ));
    let mut t1 = qt2qtn(mcx, &q1).unwrap();
    let mut t2 = qt2qtn(mcx, &q2).unwrap();
    QTNTernary(mcx, &mut t1).unwrap();
    QTNSort(&mut t1).unwrap();
    QTNTernary(mcx, &mut t2).unwrap();
    QTNSort(&mut t2).unwrap();
    assert!(QTNEq(&t1, &t2).unwrap());
}

#[test]
fn qtnsort_orders_by_valcrc_descending() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // OR of three operands with distinct (signed) valcrc. C cmpQTN
    // (tsquery_util.c:135) returns -1 when a.valcrc > b.valcrc, so qsort orders
    // the child array by *descending* valcrc — child[0] holds the highest.
    // Since child[0] (fillQT's in+1) is the right-printed operand, the highest
    // valcrc prints rightmost, matching PostgreSQL 18.3 ts_rewrite exactly.
    let q = build(&Spec::Or(
        Box::new(Spec::Or(
            Box::new(Spec::Val(b"x1", 10)),
            Box::new(Spec::Val(b"x2", 30)),
        )),
        Box::new(Spec::Val(b"x3", 20)),
    ));
    let mut tree = qt2qtn(mcx, &q).unwrap();
    QTNTernary(mcx, &mut tree).unwrap();
    QTNSort(&mut tree).unwrap();
    assert_eq!(tree.nchild(), 3);
    let crcs: alloc::vec::Vec<i32> = (0..3).map(|i| tree.child[i].valcrc()).collect();
    assert_eq!(crcs, alloc::vec![30, 20, 10], "expected descending valcrc order");
}

#[test]
fn qtnternary_flattens_associative() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // (a | b) | c flattens to a 3-child OR.
    let q = build(&Spec::Or(
        Box::new(Spec::Or(
            Box::new(Spec::Val(b"a", 1)),
            Box::new(Spec::Val(b"b", 2)),
        )),
        Box::new(Spec::Val(b"c", 3)),
    ));
    let mut tree = qt2qtn(mcx, &q).unwrap();
    QTNTernary(mcx, &mut tree).unwrap();
    assert_eq!(tree.oper(), OP_OR);
    assert_eq!(tree.nchild(), 3);
}

#[test]
fn clean_not_drops_not_subtree() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // cat & !dog  ->  cat  (the !dog operand cancels, collapsing the AND).
    let q = build(&Spec::And(
        Box::new(Spec::Val(b"cat", 1)),
        Box::new(Spec::Not(Box::new(Spec::Val(b"dog", 2)))),
    ));
    let items = crate::util::get_query(&q).unwrap();
    let (cleaned, len) = clean_NOT(mcx, &items).unwrap();
    assert_eq!(len, 1);
    assert_eq!(cleaned[0].item_type(), QI_VAL);
}

#[test]
fn clean_not_whole_query_cancels() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // !cat alone cancels to NULL (empty result).
    let q = build(&Spec::Not(Box::new(Spec::Val(b"cat", 1))));
    let items = crate::util::get_query(&q).unwrap();
    let (cleaned, len) = clean_NOT(mcx, &items).unwrap();
    assert_eq!(len, 0);
    assert!(cleaned.is_empty());
}

#[test]
fn cleanup_stopwords_removes_valstop() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // cat & <stop>  ->  cat.  Build with a QI_VALSTOP node for the right child.
    let mut items: Vec<QueryItem> = Vec::new();
    let mut operand: Vec<u8> = Vec::new();
    // AND node
    items.push(QueryItem::Qoperator(QueryOperator {
        type_: QI_OPR,
        oper: OP_AND,
        distance: 0,
        left: 0,
    }));
    // child[0] (right) = VALSTOP
    items.push(QueryItem::Type_(QI_VALSTOP));
    // left offset = 2
    if let QueryItem::Qoperator(o) = &mut items[0] {
        o.left = 2;
    }
    // child[1] (left) = cat
    let dist = operand.len() as u32;
    operand.extend_from_slice(b"cat");
    operand.push(0);
    let mut qop = QueryOperand {
        type_: QI_VAL,
        weight: 0,
        prefix: false,
        valcrc: 7,
        len_dist: 0,
    };
    qop.set_length(3);
    qop.set_distance(dist);
    items.push(QueryItem::Qoperand(qop));

    let nnode = items.len() as i32;
    let sumlen = operand.len() as i32;
    let len = HDRSIZETQ + (nnode as usize) * QI_SIZE + sumlen as usize;
    let mut q = vec![0u8; len];
    q[0..4].copy_from_slice(&((len as u32) << 2).to_ne_bytes());
    q[4..8].copy_from_slice(&nnode.to_ne_bytes());
    for (i, it) in items.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        crate::util::encode_record(it, &mut q[base..base + QI_SIZE]);
    }
    let opbase = HDRSIZETQ + (nnode as usize) * QI_SIZE;
    q[opbase..opbase + sumlen as usize].copy_from_slice(&operand);

    let out = cleanup_tsquery_stopwords(mcx, &q, false).unwrap();
    // Degenerates to the single surviving operand "cat".
    assert_eq!(tsq_size(&out), 1);
    let opbase = HDRSIZETQ + QI_SIZE;
    assert_eq!(&out[opbase..opbase + 3], b"cat");
}

#[test]
fn qt2qtn_qtn2qt_roundtrip_phrase() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // cat <2> dog — an OP_PHRASE node with distance 2.
    let q = build(&Spec::Phrase(
        2,
        Box::new(Spec::Val(b"cat", 1)),
        Box::new(Spec::Val(b"dog", 2)),
    ));
    let tree = qt2qtn(mcx, &q).unwrap();
    assert!(tree.is_opr());
    assert_eq!(tree.oper(), OP_PHRASE);
    assert_eq!(tree.nchild(), 2);

    let out = qtn2qt(mcx, &tree).unwrap();
    assert_eq!(tsq_size(&out), 3);
    // The phrase distance survives the round-trip.
    let opr = crate::util::decode_record(&out[HDRSIZETQ..HDRSIZETQ + QI_SIZE]);
    if let QueryItem::Qoperator(o) = opr {
        assert_eq!(o.oper, OP_PHRASE);
        assert_eq!(o.distance, 2);
    } else {
        panic!("root is not an operator");
    }
}

#[test]
fn rewrite_substitutes_matching_subquery() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // query: cat & dog ; target: dog ; subst: fish  =>  cat & fish
    let query = build(&Spec::And(
        Box::new(Spec::Val(b"cat", 1)),
        Box::new(Spec::Val(b"dog", 2)),
    ));
    let target = build(&Spec::Val(b"dog", 2));
    let subst = build(&Spec::Val(b"fish", 3));

    let out = tsquery_rewrite(mcx, &query, &target, &subst).unwrap();
    assert_eq!(tsq_size(&out), 3);
    // The operand storage now contains "cat" and "fish", not "dog".
    let opbase = HDRSIZETQ + 3 * QI_SIZE;
    let ops = &out[opbase..];
    let joined: Vec<u8> = ops.to_vec();
    assert!(joined.windows(4).any(|w| w == b"fish"));
    assert!(!joined.windows(3).any(|w| w == b"dog"));
}

#[test]
fn rewrite_empty_target_returns_query_copy() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let query = build(&Spec::Val(b"cat", 1));
    // empty target tsquery (size 0)
    let mut empty = vec![0u8; HDRSIZETQ];
    empty[0..4].copy_from_slice(&((HDRSIZETQ as u32) << 2).to_ne_bytes());
    let subst = build(&Spec::Val(b"dog", 2));

    let out = tsquery_rewrite(mcx, &query, &empty, &subst).unwrap();
    assert_eq!(out, query);
}
