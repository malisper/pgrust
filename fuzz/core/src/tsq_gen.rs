//! tsq_gen: structurally-valid tsquery VARLENA-PAYLOAD generator for the
//! text-search differential targets (tsvector_core_diff match arm,
//! tsrank_diff). Both sides receive IDENTICAL image bytes, so the C tsquery
//! PARSER is never needed (no ts_cache / text-search-configuration GUC —
//! the lane-F tsqrw ruling, reused).
//!
//! Payload layout (ts_type.h `TSQueryData` after `vl_len_`, i.e. exactly
//! `adt_tsvector_core::query::TsQueryRef.payload`):
//!   [i32 size][QueryItem x size, 12 B each][operand pool: NUL-terminated]
//!
//! Prefix-order RPN: an operator's RIGHT subtree starts at `idx + 1`, its
//! LEFT subtree at `idx + left` (QueryOperator.left). Leaves are QI_VAL with
//! weight bits 0..15, optional prefix flag, and `valcrc = 0` — valcrc is a
//! parse-time cache consumed by tsquery comparison/rewrite, never by
//! TS_execute/checkcondition_str or the tsrank kernels this generator
//! feeds (documented non-surface for these targets).
//!
//! Lexemes are drawn from a small alphabet overlapping the tsvector arms'
//! seed vocabulary (so matches actually happen), plus fuzz-derived bytes
//! for the mismatch space.

use adt_tsvector_core::query::{Item, Operand, Operator, OP_AND, OP_NOT, OP_OR, OP_PHRASE};

// 96 (was 32 through the first fleet floor): >40 collected operands are
// needed to reach pg_qsort's med3-of-9 pivot band in tsrank's
// SortAndUniqItems — SQL-reachable (a 41-operand query), so the generator
// envelope must include it.
pub const MAX_ITEMS: usize = 96;

/// Byte-stream cursor over the fuzz payload.
struct Cur<'a> {
    data: &'a [u8],
    off: usize,
}

impl<'a> Cur<'a> {
    fn u8(&mut self) -> u8 {
        let b = self.data.get(self.off).copied().unwrap_or(0);
        self.off += 1;
        b
    }
}

/// Lexeme alphabet: overlaps the tsvector seed vocabulary on purpose.
const LEXEMES: &[&[u8]] = &[
    b"a", b"b", b"c", b"d", b"ab", b"abc", b"cat", b"cats", b"catalog", b"dog", b"w1", b"w2",
    "é".as_bytes(),
    "日本".as_bytes(),
    b"qq", b"z",
];

enum Node {
    Val { lex: Vec<u8>, weight: u8, prefix: bool },
    Op { oper: i8, distance: i16, left: Box<Node>, right: Box<Node> },
    Not { child: Box<Node> },
}

fn node_size(n: &Node) -> usize {
    match n {
        Node::Val { .. } => 1,
        Node::Not { child } => 1 + node_size(child),
        Node::Op { left, right, .. } => 1 + node_size(left) + node_size(right),
    }
}

fn gen_node(cur: &mut Cur<'_>, budget: &mut usize, depth: usize) -> Node {
    let b = cur.u8();
    // leaf if out of budget, too deep, or the byte says so
    if *budget < 3 || depth > 6 || b & 0x80 == 0 {
        *budget = budget.saturating_sub(1);
        let sel = cur.u8();
        let lex: Vec<u8> = if sel & 0x40 != 0 {
            // fuzz-derived lexeme: 1..=4 raw bytes, NUL/quote-free ASCII-fold
            let n = (sel as usize & 3) + 1;
            (0..n)
                .map(|_| {
                    let c = cur.u8();
                    if c == 0 { b'x' } else { c }
                })
                .collect()
        } else {
            LEXEMES[sel as usize % LEXEMES.len()].to_vec()
        };
        let w = cur.u8();
        return Node::Val { lex, weight: w & 0x0f, prefix: w & 0x10 != 0 };
    }
    match b % 4 {
        0 => {
            *budget = budget.saturating_sub(1);
            Node::Not { child: Box::new(gen_node(cur, budget, depth + 1)) }
        }
        oper => {
            *budget = budget.saturating_sub(1);
            let distance = if oper == 3 {
                // OP_PHRASE distance: bias small, allow up to MAXENTRYPOS
                let d = ((cur.u8() as u16) << 8 | cur.u8() as u16) % 16385;
                d as i16
            } else {
                0
            };
            let left = Box::new(gen_node(cur, budget, depth + 1));
            let right = Box::new(gen_node(cur, budget, depth + 1));
            let op = match oper {
                1 => OP_AND,
                2 => OP_OR,
                _ => OP_PHRASE,
            };
            Node::Op { oper: op, distance, left, right }
        }
    }
}

fn emit(n: &Node, items: &mut Vec<Item>, pool: &mut Vec<u8>) {
    match n {
        Node::Val { lex, weight, prefix } => {
            // NO pool dedup: real PG (tsqueryin / tsqueryrecv / QTN2QT)
            // appends every operand's string separately, so QueryOperand
            // distances are UNIQUE per operand. A deduped pool makes two
            // operands share a distance — legal bytes but SQL-unreachable,
            // and the Rust rank kernel's distance->item map is ambiguous on
            // them (fuzz/DIVERGENCES-tsrank.md robustness note).
            let distance = push_operand(pool, lex);
            items.push(Item::Val(Operand {
                weight: *weight,
                prefix: *prefix,
                valcrc: 0,
                length: lex.len(),
                distance,
            }));
        }
        Node::Not { child } => {
            items.push(Item::Opr(Operator { oper: OP_NOT, distance: 0, left: 1 }));
            emit(child, items, pool);
        }
        Node::Op { oper, distance, left, right } => {
            let left_off = 1 + node_size(right) as u32;
            items.push(Item::Opr(Operator { oper: *oper, distance: *distance, left: left_off }));
            emit(right, items, pool);
            emit(left, items, pool);
        }
    }
}

fn push_operand(pool: &mut Vec<u8>, lex: &[u8]) -> usize {
    let off = pool.len();
    pool.extend_from_slice(lex);
    pool.push(0);
    off
}

/// Generate a tsquery payload from fuzz bytes. `None` when the bytes ask
/// for an empty query (size = 0 — a legal stored tsquery both engines
/// treat as no-match) roughly 1 time in 32.
pub fn gen_tsquery_payload(data: &[u8]) -> Vec<u8> {
    let mut cur = Cur { data, off: 0 };
    let first = cur.u8();
    if first % 32 == 31 {
        // empty tsquery: size 0, no items, no pool
        return 0i32.to_ne_bytes().to_vec();
    }
    let mut budget = MAX_ITEMS;
    let root = gen_node(&mut cur, &mut budget, 0);
    let mut items = Vec::new();
    let mut pool = Vec::new();
    emit(&root, &mut items, &mut pool);
    let mut out = Vec::with_capacity(4 + items.len() * 12 + pool.len());
    out.extend_from_slice(&(items.len() as i32).to_ne_bytes());
    for it in &items {
        out.extend_from_slice(&it.encode());
    }
    out.extend_from_slice(&pool);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use adt_tsvector_core::query::TsQueryRef;

    #[test]
    fn generated_queries_decode() {
        for seed in 0u32..500 {
            let bytes: Vec<u8> = (0..64u32)
                .map(|i| (seed.wrapping_mul(2654435761).wrapping_add(i * 40503) >> 8) as u8)
                .collect();
            let payload = gen_tsquery_payload(&bytes);
            let q = TsQueryRef { payload: &payload };
            // walk every item; Item::decode panics on junk
            for i in 0..q.size() {
                match q.item(i) {
                    Item::Val(op) => {
                        let s = q.operand_str(&op);
                        assert!(!s.is_empty());
                    }
                    Item::Opr(op) => {
                        assert!((1..=4).contains(&op.oper));
                        if op.oper != OP_NOT {
                            assert!(op.left as usize + i < q.size());
                        }
                    }
                    Item::ValStop => panic!("generator emitted QI_VALSTOP"),
                }
            }
        }
    }
}
