//! Crate-local helpers shared by the indxpath modules: catalog OID constants,
//! the `IndexCollMatchesExprColl` macro, the `Relids` bit iterator, and the
//! inline node classifiers (`is_notclause`/`get_notclausearg`/
//! `restriction_is_or_clause`/`is_andclause`).

use types_core::primitive::Oid;
use types_nodes::primnodes::{BoolExprType, Expr};
use types_pathnodes::{PlannerInfo, Relids, RinfoId};

/// `InvalidOid`.
pub const INVALID_OID: Oid = 0;

/// `BOOLOID` (pg_type.dat) — the boolean type OID.
pub const BOOLOID: Oid = 16;

/// `RECORDOID` (pg_type.dat) — the pseudo-type for an unspecified row type.
pub const RECORDOID: Oid = 2249;

/// `BooleanEqualOperator` (pg_operator.dat: `=` for `bool`, OID 91).
pub const BOOLEAN_EQUAL_OPERATOR: Oid = 91;

/// `BTREE_AM_OID` (pg_am.dat) — the btree access method OID.
pub const BTREE_AM_OID: Oid = 403;

/// `BOOL_BTREE_FAM_OID` (pg_opfamily.dat) — bool btree opfamily.
pub const BOOL_BTREE_FAM_OID: Oid = 424;
/// `BOOL_HASH_FAM_OID` (pg_opfamily.dat) — bool hash opfamily.
pub const BOOL_HASH_FAM_OID: Oid = 2222;

/// `FirstNormalObjectId` (transam.h) — OIDs below this are built-in.
pub const FIRST_NORMAL_OBJECT_ID: Oid = 16384;

/// `FirstLowInvalidHeapAttributeNumber` (sysattr.h) = -7 — system-attr offset
/// base (TableOidAttributeNumber is -6, FirstLowInvalid is one past it). This
/// must match var.c's `pull_varattnos` offset (also -7); using -8 here shifted
/// the index-canreturn bitmap by one column relative to attrs_used, so
/// `check_index_only`'s subset test always failed and no index-only scan was
/// ever generated.
pub const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

/// btree strategy numbers (stratnum.h).
pub const BT_LESS_STRATEGY_NUMBER: i32 = 1;
pub const BT_LESS_EQUAL_STRATEGY_NUMBER: i32 = 2;
pub const BT_GREATER_EQUAL_STRATEGY_NUMBER: i32 = 4;
pub const BT_GREATER_STRATEGY_NUMBER: i32 = 5;

/// `RELOPT_OTHER_MEMBER_REL` (pathnodes.h RelOptKind) — inheritance/partition
/// child member rel.
pub const RELOPT_OTHER_MEMBER_REL: u32 = 1;

/// `IndexCollMatchesExprColl(idxcollation, exprcollation)` (indexing.h macro) —
/// the index collation must match the clause's collation unless the index is
/// collation-less.
#[inline]
pub fn index_coll_matches_expr_coll(idxcollation: Oid, exprcoll: Oid) -> bool {
    idxcollation == INVALID_OID || idxcollation == exprcoll
}

/// `IsBuiltinBooleanOpfamily(opfamily)` (pg_opfamily.h macro): a built-in
/// boolean opfamily is the bool btree or bool hash opfamily.
#[inline]
pub fn is_builtin_boolean_opfamily(opfamily: Oid) -> bool {
    opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID
}

const BITS_PER_BITMAPWORD: i32 = 64;

/// `bms_next_member(a, prevbit)` over the planner `Relids`
/// (`Option<Box<Bitmapset{words: Vec<u64>}>>`): the smallest set member strictly
/// greater than `prevbit`, or `-2` when none (the C sentinel; callers test
/// `< 0`). Start the scan with `prevbit = -1`.
pub fn relids_next_member(a: &Relids, prevbit: i32) -> i32 {
    let bms = match a {
        None => return -2,
        Some(b) => b,
    };
    let nwords = bms.words.len();
    let start = prevbit + 1;
    let mut wnum = (start / BITS_PER_BITMAPWORD) as usize;
    if wnum >= nwords {
        return -2;
    }
    let mut mask: u64 = (!0u64) << (start % BITS_PER_BITMAPWORD);
    while wnum < nwords {
        let w = bms.words[wnum] & mask;
        if w != 0 {
            return wnum as i32 * BITS_PER_BITMAPWORD + w.trailing_zeros() as i32;
        }
        mask = !0u64;
        wnum += 1;
    }
    -2
}

/// `bms_is_member(x, a)` over the planner `Relids`
/// (`Option<Box<Bitmapset{words: Vec<u64>}>>`): is bit `x` set?
pub fn relids_is_member(x: i32, a: &Relids) -> bool {
    if x < 0 {
        return false;
    }
    let bms = match a {
        None => return false,
        Some(b) => b,
    };
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum >= bms.words.len() {
        return false;
    }
    (bms.words[wnum] >> (x % BITS_PER_BITMAPWORD)) & 1 != 0
}

use alloc::boxed::Box;
use alloc::vec::Vec;

/// `bms_make_singleton(x)` -> `Relids` with a single bit `x` set, then
/// `bms_add_member(a, x)` semantics: return a fresh set = `a` with bit `x` added.
pub fn relids_add_member(a: Relids, x: i32) -> Relids {
    debug_assert!(x >= 0);
    let mut words: Vec<u64> = match a {
        None => Vec::new(),
        Some(b) => b.words,
    };
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum >= words.len() {
        words.resize(wnum + 1, 0);
    }
    words[wnum] |= 1u64 << (x % BITS_PER_BITMAPWORD);
    Some(Box::new(types_pathnodes::Bitmapset { words }))
}

/// `bms_copy(a)` over the planner `Relids`.
pub fn relids_copy(a: &Relids) -> Relids {
    a.as_ref().map(|b| Box::new(types_pathnodes::Bitmapset { words: b.words.clone() }))
}

/// `bms_equal(a, b)` over the planner `Relids`.
pub fn relids_equal(a: &Relids, b: &Relids) -> bool {
    let aw: &[u64] = a.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    let bw: &[u64] = b.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    let n = aw.len().max(bw.len());
    for i in 0..n {
        if aw.get(i).copied().unwrap_or(0) != bw.get(i).copied().unwrap_or(0) {
            return false;
        }
    }
    true
}

/// `bms_overlap(a, b)` over the planner `Relids`.
pub fn relids_overlap(a: &Relids, b: &Relids) -> bool {
    let aw: &[u64] = a.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    let bw: &[u64] = b.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    let n = aw.len().min(bw.len());
    for i in 0..n {
        if aw[i] & bw[i] != 0 {
            return true;
        }
    }
    false
}

/// `bms_add_members(a, b)` over the planner `Relids` (a |= b).
pub fn relids_add_members(a: Relids, b: &Relids) -> Relids {
    let bw: &[u64] = b.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    if bw.is_empty() {
        return a;
    }
    let mut words: Vec<u64> = match a {
        None => Vec::new(),
        Some(x) => x.words,
    };
    if words.len() < bw.len() {
        words.resize(bw.len(), 0);
    }
    for (i, &w) in bw.iter().enumerate() {
        words[i] |= w;
    }
    Some(Box::new(types_pathnodes::Bitmapset { words }))
}

/// `bms_is_subset(a, b)` over the planner `Relids`.
pub fn relids_is_subset(a: &Relids, b: &Relids) -> bool {
    let aw: &[u64] = a.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    let bw: &[u64] = b.as_ref().map(|x| x.words.as_slice()).unwrap_or(&[]);
    for i in 0..aw.len() {
        let bword = bw.get(i).copied().unwrap_or(0);
        if aw[i] & !bword != 0 {
            return false;
        }
    }
    true
}

/// `bms_union(a, b)` over the planner `Relids`.
pub fn relids_union(a: &Relids, b: &Relids) -> Relids {
    relids_add_members(relids_copy(a), b)
}

/// `restriction_is_or_clause(rinfo)` (restrictinfo.c:406) — true iff the
/// RestrictInfo carries an OR clause (`rinfo->orclause != NULL`).
#[inline]
pub fn restriction_is_or_clause(root: &PlannerInfo, rinfo: RinfoId) -> bool {
    root.rinfo(rinfo).orclause.is_some()
}

/// `is_notclause(clause)` (nodeFuncs.h) — `IsA(clause, BoolExpr) && boolop ==
/// NOT_EXPR`.
#[inline]
pub fn is_notclause(clause: &Expr) -> bool {
    match clause.as_boolexpr() {
        Some(b) => b.boolop == BoolExprType::NOT_EXPR,
        None => false,
    }
}

/// `is_andclause(clause)` (nodeFuncs.h) — `IsA(clause, BoolExpr) && boolop ==
/// AND_EXPR`.
#[inline]
pub fn is_andclause(clause: &Expr) -> bool {
    match clause.as_boolexpr() {
        Some(b) => b.boolop == BoolExprType::AND_EXPR,
        None => false,
    }
}

/// `get_notclausearg(notclause)` (nodeFuncs.h) — the single argument of a `NOT`
/// `BoolExpr`.
#[inline]
pub fn get_notclausearg<'a, 'mcx>(notclause: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    notclause
        .as_boolexpr()
        .expect("get_notclausearg: not a BoolExpr")
        .args
        .first()
        .expect("NOT clause must have one argument")
}
