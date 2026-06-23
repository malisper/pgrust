//! Unit tests for the `tsquery` core ADT port. These exercise the pure-logic
//! paths that do not require the unported `tsvector_parser.c` engine: the
//! `QTNode`-toolkit-backed `tsquery_op` constructors/comparisons and the GiST
//! signature math. Building a flat `tsquery` image by hand mirrors the polish
//! layout `parse_tsquery` produces.

use ::mcx::MemoryContext;

use ::tsearch::tsearch::{
    QueryItem, QueryOperand, QueryOperator, HDRSIZETQ, OP_AND, QI_OPR, QI_VAL, TSQS_SIGLEN,
};

use postgres_seams as tcop;
use ::ts_small::util::{encode_record, QI_SIZE};

use crate::gist::{gtsquery_consistent, gtsquery_union, RT_CONTAINS_STRATEGY_NUMBER};
use crate::op::{makeTSQuerySign, tsquery_and, tsquery_cmp, tsquery_numnode};

fn install_seams() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        tcop::check_stack_depth::set(|| Ok(()));
    });
}

/// Build a flat `tsquery` image from a list of `QueryItem`s and an operand
/// store, mirroring the on-disk layout (`HDRSIZETQ + items + operands`).
fn make_tsquery(items: &[QueryItem], operand: &[u8]) -> Vec<u8> {
    let n = items.len();
    let total = HDRSIZETQ + n * QI_SIZE + operand.len();
    let mut q = vec![0u8; total];
    q[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
    q[4..8].copy_from_slice(&(n as i32).to_ne_bytes());
    for (i, it) in items.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        encode_record(it, &mut q[base..base + QI_SIZE]);
    }
    let opbase = HDRSIZETQ + n * QI_SIZE;
    q[opbase..opbase + operand.len()].copy_from_slice(operand);
    q
}

/// One operand "cat" with `distance`/`length`/`valcrc`.
fn operand(distance: u32, valcrc: i32, len: u32) -> QueryItem {
    let mut o = QueryOperand {
        type_: QI_VAL,
        weight: 0,
        prefix: false,
        valcrc,
        len_dist: 0,
    };
    o.set_length(len);
    o.set_distance(distance);
    QueryItem::Qoperand(o)
}

#[test]
fn numnode_counts_items() {
    // "cat" — a single QI_VAL operand.
    let q = make_tsquery(&[operand(0, 0x1234, 3)], b"cat\0");
    assert_eq!(tsquery_numnode(&q), 1);
}

#[test]
fn make_sign_sets_crc_bits() {
    let crc_a: i32 = 5;
    let crc_b: i32 = 70; // 70 % 64 = 6
    let q = make_tsquery(
        &[
            QueryItem::Qoperator(QueryOperator { type_: QI_OPR, oper: OP_AND, distance: 0, left: 2 }),
            operand(0, crc_a, 1),
            operand(2, crc_b, 1),
        ],
        b"a\0b\0",
    );
    let sign = makeTSQuerySign(&q).unwrap();
    let expected = (1u64 << (crc_a as u32 % TSQS_SIGLEN)) | (1u64 << (crc_b as u32 % TSQS_SIGLEN));
    assert_eq!(sign, expected);
}

#[test]
fn union_ors_signatures() {
    assert_eq!(gtsquery_union(&[0b0011, 0b0101]), 0b0111);
    assert_eq!(gtsquery_union(&[]), 0);
}

#[test]
fn consistent_contains_leaf_and_inner() {
    // key has bits {0,1}, query "a" has crc bit 0.
    let q = make_tsquery(&[operand(0, 0, 1)], b"a\0");
    // leaf: (key & sq) == sq  -> bit0 present -> true
    let (m, recheck) = gtsquery_consistent(0b11, &q, RT_CONTAINS_STRATEGY_NUMBER, true).unwrap();
    assert!(m && recheck);
    // leaf, key missing bit0:
    let (m, _) = gtsquery_consistent(0b10, &q, RT_CONTAINS_STRATEGY_NUMBER, true).unwrap();
    assert!(!m);
    // inner: (key & sq) != 0
    let (m, _) = gtsquery_consistent(0b01, &q, RT_CONTAINS_STRATEGY_NUMBER, false).unwrap();
    assert!(m);
}

#[test]
fn and_with_empty_returns_other() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // empty tsquery: size 0, just the header.
    let empty = make_tsquery(&[], b"");
    let cat = make_tsquery(&[operand(0, 0x1234, 3)], b"cat\0");

    // a & b with a empty -> b
    let out = tsquery_and(mcx, &empty, &cat).unwrap();
    assert_eq!(out, cat);
    // a & b with b empty -> a
    let out = tsquery_and(mcx, &cat, &empty).unwrap();
    assert_eq!(out, cat);
}

#[test]
fn cmp_by_size_then_varsize() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let one = make_tsquery(&[operand(0, 1, 1)], b"a\0");
    let two = make_tsquery(
        &[
            QueryItem::Qoperator(QueryOperator { type_: QI_OPR, oper: OP_AND, distance: 0, left: 2 }),
            operand(0, 1, 1),
            operand(2, 2, 1),
        ],
        b"a\0b\0",
    );
    // one has fewer nodes -> a->size < b->size -> -1
    assert_eq!(tsquery_cmp(mcx, &one, &two).unwrap(), -1);
    assert_eq!(tsquery_cmp(mcx, &two, &one).unwrap(), 1);
    assert_eq!(tsquery_cmp(mcx, &one, &one).unwrap(), 0);
}
