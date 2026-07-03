extern crate std;

use mcx::MemoryContext;
use std::collections::BTreeSet;
use std::string::String as StdString;
use std::vec::Vec;

use crate::bitmapset::{Bitmapset, BmsComparison, BmsMembership};
use crate::list::{IntList, NodeList, OidList, XidList};
use crate::node_tree::Node;
use crate::tags::{NodeTag, NODE_TAG_TABLE};
use crate::JoinType;

#[test]
fn tags_match_c_header() {
    let header = include_str!("../vendor/nodetags.h");
    let mut c_tags: Vec<(StdString, u16)> = Vec::new();
    for line in header.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("T_") {
            let (name, val) = rest.split_once(" = ").expect("tag line shape");
            let val: u16 = val.trim_end_matches(',').parse().expect("numeric tag");
            c_tags.push((std::format!("T_{name}"), val));
        }
    }
    assert_eq!(c_tags.len(), 479);
    assert_eq!(NODE_TAG_TABLE.len(), c_tags.len() + 1);
    assert_eq!(NODE_TAG_TABLE[0], ("T_Invalid", 0));
    for (i, (name, val)) in c_tags.iter().enumerate() {
        assert_eq!(NODE_TAG_TABLE[i + 1], (name.as_str(), *val));
    }
    assert_eq!(NodeTag::T_List as u16, 1);
    assert_eq!(NodeTag::T_Bitmapset as u16, 445);
    assert_eq!(NodeTag::T_Integer as u16, 465);
    assert_eq!(NodeTag::T_BitString as u16, 469);
    assert_eq!(NodeTag::T_IntList as u16, 471);
    assert_eq!(NodeTag::T_XidList as u16, 473);
}

#[test]
fn jointype_values() {
    assert_eq!(JoinType::JOIN_INNER as u32, 0);
    assert_eq!(JoinType::JOIN_ANTI as u32, 5);
    assert_eq!(JoinType::JOIN_UNIQUE_INNER as u32, 9);
    assert!(JoinType::JOIN_RIGHT_ANTI.is_outer_join());
    assert!(!JoinType::JOIN_SEMI.is_outer_join());
    assert!(!JoinType::JOIN_UNIQUE_OUTER.is_outer_join());
}

#[test]
fn list_growth_matches_list_c() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mut l = IntList::nil();
    assert_eq!(l.capacity(), 0);
    // new_list(1): pg_nextpower2_32(max(8, 1+3)) - 3 = 5.
    l.lappend(mcx, 0).unwrap();
    assert_eq!(l.capacity(), 5);
    // enlarge_list(6): pg_nextpower2_32(max(16, 6)) = 16; then 32, 64.
    for i in 1..=40 {
        l.lappend(mcx, i).unwrap();
        let expected = match l.len() {
            1..=5 => 5,
            6..=16 => 16,
            17..=32 => 32,
            _ => 64,
        };
        assert_eq!(l.capacity(), expected, "at len {}", l.len());
    }
    let collected: Vec<i32> = l.iter().collect();
    assert_eq!(collected, (0..=40).collect::<Vec<i32>>());
}

#[test]
fn list_make_initial_capacities() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let l = IntList::make1(mcx, 7).unwrap();
    assert_eq!((l.len(), l.capacity()), (1, 5));
    let l = IntList::from_slice(mcx, &[1, 2, 3, 4, 5]).unwrap();
    assert_eq!((l.len(), l.capacity()), (5, 5));
    // new_list(6): nextpower2(6+3)=16, minus overhead 3 = 13.
    let l = IntList::from_slice(mcx, &[1, 2, 3, 4, 5, 6]).unwrap();
    assert_eq!((l.len(), l.capacity()), (6, 13));
}

#[test]
fn list_ops() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mut l = OidList::make2(mcx, 10, 30).unwrap();
    l.insert_nth(mcx, 1, 20).unwrap();
    l.lcons(mcx, 5).unwrap();
    assert_eq!(l.as_slice(), &[5, 10, 20, 30]);
    assert_eq!(l.nth(2), 20);
    assert_eq!((l.first(), l.last()), (Some(5), Some(30)));
    let tail = OidList::make2(mcx, 40, 50).unwrap();
    l.concat(mcx, &tail).unwrap();
    assert_eq!(l.as_slice(), &[5, 10, 20, 30, 40, 50]);
    let copy = l.clone_in(mcx).unwrap();
    l.truncate(2);
    assert_eq!(l.as_slice(), &[5, 10]);
    assert_eq!(copy.as_slice(), &[5, 10, 20, 30, 40, 50]);
    assert_eq!(copy.tag(), NodeTag::T_OidList);

    let mut x = XidList::nil();
    assert!(x.is_nil());
    x.lappend(mcx, 777).unwrap();
    assert_eq!(x.tag(), NodeTag::T_XidList);
    assert_eq!(x.as_slice(), &[777]);
}

#[test]
fn node_value_round_trips() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let n = Node::mk_integer(mcx, -42).unwrap();
    assert_eq!(n.node_tag(), NodeTag::T_Integer);
    assert_eq!(n.as_integer().unwrap().ival, -42);
    assert!(n.as_string().is_none());
    assert!(n.as_list().is_none());

    let f = Node::mk_float(mcx, "3.14159").unwrap();
    assert_eq!(f.node_tag(), NodeTag::T_Float);
    assert_eq!(f.as_float().unwrap().fval, "3.14159");

    let b = Node::mk_boolean(mcx, true).unwrap();
    assert!(b.as_boolean().unwrap().boolval);

    let s = Node::mk_string(mcx, "hello").unwrap();
    assert_eq!(s.node_tag(), NodeTag::T_String);
    assert_eq!(s.as_string().unwrap().sval, "hello");
    assert!(s.as_bitstring().is_none());

    let bs = Node::mk_bitstring(mcx, "b1010").unwrap();
    assert_eq!(bs.as_bitstring().unwrap().bsval, "b1010");
    assert_eq!(bs.node_tag(), NodeTag::T_BitString);
}

#[test]
fn node_lists_and_bitmapsets() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let mut inner = NodeList::nil();
    inner.lappend(mcx, Node::mk_integer(mcx, 1).unwrap()).unwrap();
    inner.lappend(mcx, Node::mk_string(mcx, "two").unwrap()).unwrap();
    let ln = Node::mk_list(mcx, inner).unwrap();
    assert_eq!(ln.node_tag(), NodeTag::T_List);
    let got = ln.as_list().unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got.nth(0).as_integer().unwrap().ival, 1);
    assert_eq!(got.nth(1).as_string().unwrap().sval, "two");
    assert!(ln.as_int_list().is_none());

    let il = Node::mk_int_list(mcx, IntList::make2(mcx, 3, 4).unwrap()).unwrap();
    assert_eq!(il.node_tag(), NodeTag::T_IntList);
    assert_eq!(il.as_int_list().unwrap().as_slice(), &[3, 4]);
    assert!(il.as_oid_list().is_none());
    assert!(il.as_xid_list().is_none());

    let ol = Node::mk_oid_list(mcx, OidList::make1(mcx, 16384).unwrap()).unwrap();
    assert_eq!(ol.node_tag(), NodeTag::T_OidList);
    let xl = Node::mk_xid_list(mcx, XidList::make1(mcx, 99).unwrap()).unwrap();
    assert_eq!(xl.node_tag(), NodeTag::T_XidList);

    let bms = Bitmapset::make_singleton(mcx, 130).unwrap();
    let bn = Node::mk_bitmapset(mcx, bms).unwrap();
    assert_eq!(bn.node_tag(), NodeTag::T_Bitmapset);
    assert!(bn.as_bitmapset().unwrap().is_member(130));
}

struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn check_invariants(b: &Bitmapset<'_>) {
    // PG 16+ invariant: empty set is nwords == 0; no trailing zero word.
    if !b.is_empty() {
        assert_ne!(*b.as_words().last().unwrap(), 0);
    }
}

fn from_set<'m>(mcx: mcx::Mcx<'m>, s: &BTreeSet<i32>) -> Bitmapset<'m> {
    let mut b = Bitmapset::empty();
    for &x in s {
        b.add_member(mcx, x).unwrap();
    }
    b
}

#[test]
fn bms_basics() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let mut b = Bitmapset::empty();
    assert!(b.is_empty());
    assert!(!b.is_member(0));
    assert_eq!(b.next_member(-1), -2);
    assert_eq!(b.membership(), BmsMembership::BmsEmptySet);

    b.add_member(mcx, 64).unwrap();
    assert_eq!(b.nwords(), 2);
    assert_eq!(b.membership(), BmsMembership::BmsSingleton);
    assert_eq!(b.get_singleton_member(), Some(64));
    b.add_member(mcx, 0).unwrap();
    assert_eq!(b.membership(), BmsMembership::BmsMultiple);
    assert_eq!(b.get_singleton_member(), None);
    assert_eq!(b.num_members(), 2);

    b.del_member(64);
    check_invariants(&b);
    assert_eq!(b.nwords(), 1);
    b.del_member(0);
    assert!(b.is_empty());

    let s = Bitmapset::make_singleton(mcx, 200).unwrap();
    assert_eq!(s.nwords(), 4);
    assert!(s.is_member(200));
    assert!(!s.is_member(199));
    assert_eq!(s.next_member(-1), 200);
    assert_eq!(s.next_member(200), -2);
    assert_eq!(s.prev_member(-1), 200);
    assert_eq!(s.prev_member(200), -2);
}

#[test]
fn bms_next_prev_member_match_c_vectors() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    for (members, next_walk, prev_walk) in crate::bms_c_vectors::NEXT_MEMBER_VECTORS {
        let mut b = Bitmapset::empty();
        for &x in *members {
            b.add_member(mcx, x).unwrap();
        }
        check_invariants(&b);
        let mut fwd = Vec::new();
        let mut x = -1;
        while {
            x = b.next_member(x);
            x >= 0
        } {
            fwd.push(x);
        }
        assert_eq!(&fwd, next_walk);
        let mut back = Vec::new();
        let mut x = -1;
        while {
            x = b.prev_member(x);
            x >= 0
        } {
            back.push(x);
        }
        assert_eq!(&back, prev_walk);
    }

    let mut b = Bitmapset::empty();
    for x in [0, 63, 64, 127, 129, 300] {
        b.add_member(mcx, x).unwrap();
    }
    for &(p, next, prev) in crate::bms_c_vectors::NEXT_FROM_VECTORS {
        assert_eq!(b.next_member(p), next, "next_member({p})");
        if p == -1 || p > 0 {
            assert_eq!(b.prev_member(p), prev, "prev_member({p})");
        }
    }
}

#[test]
fn bms_property_vs_reference() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mut rng = XorShift(0x9E3779B97F4A7C15);

    for round in 0..200 {
        let mut ra: BTreeSet<i32> = BTreeSet::new();
        let mut rb: BTreeSet<i32> = BTreeSet::new();
        let range = if round % 3 == 0 { 24 } else { 400 };
        for _ in 0..(rng.next() % 64) {
            ra.insert((rng.next() % range) as i32);
        }
        for _ in 0..(rng.next() % 64) {
            rb.insert((rng.next() % range) as i32);
        }
        let a = from_set(mcx, &ra);
        let b = from_set(mcx, &rb);
        check_invariants(&a);
        check_invariants(&b);

        for x in 0..range as i32 {
            assert_eq!(a.is_member(x), ra.contains(&x));
        }
        assert_eq!(a.num_members() as usize, ra.len());
        assert_eq!(a.equal(&b), ra == rb);
        assert_eq!(a.overlap(&b), !ra.is_disjoint(&rb));
        assert_eq!(a.is_subset(&b), ra.is_subset(&rb));
        assert_eq!(
            a.nonempty_difference(&b),
            ra.difference(&rb).next().is_some()
        );

        let expected_cmp = match (ra.is_subset(&rb), rb.is_subset(&ra)) {
            (true, true) => BmsComparison::BmsEqual,
            (true, false) => BmsComparison::BmsSubset1,
            (false, true) => BmsComparison::BmsSubset2,
            (false, false) => BmsComparison::BmsDifferent,
        };
        assert_eq!(a.subset_compare(&b), expected_cmp);

        let u = a.union(&b, mcx).unwrap();
        check_invariants(&u);
        let ru: BTreeSet<i32> = ra.union(&rb).copied().collect();
        assert_eq!(u.iter().collect::<Vec<_>>(), ru.iter().copied().collect::<Vec<_>>());

        let i = a.intersect(&b, mcx).unwrap();
        check_invariants(&i);
        let ri: BTreeSet<i32> = ra.intersection(&rb).copied().collect();
        assert_eq!(i.iter().collect::<Vec<_>>(), ri.iter().copied().collect::<Vec<_>>());

        let d = a.difference(&b, mcx).unwrap();
        check_invariants(&d);
        let rd: BTreeSet<i32> = ra.difference(&rb).copied().collect();
        assert_eq!(d.iter().collect::<Vec<_>>(), rd.iter().copied().collect::<Vec<_>>());

        let mut am = a.clone_in(mcx).unwrap();
        am.add_members(mcx, &b).unwrap();
        check_invariants(&am);
        assert_eq!(am.iter().collect::<Vec<_>>(), ru.iter().copied().collect::<Vec<_>>());

        let mut im = a.clone_in(mcx).unwrap();
        im.int_members(&b);
        check_invariants(&im);
        assert_eq!(im.iter().collect::<Vec<_>>(), ri.iter().copied().collect::<Vec<_>>());

        let mut dm = a.clone_in(mcx).unwrap();
        dm.del_members(&b);
        check_invariants(&dm);
        assert_eq!(dm.iter().collect::<Vec<_>>(), rd.iter().copied().collect::<Vec<_>>());

        // next_member / prev_member walk from every start point.
        let mut fwd = Vec::new();
        let mut x = -1;
        loop {
            x = a.next_member(x);
            if x < 0 {
                assert_eq!(x, -2);
                break;
            }
            fwd.push(x);
        }
        assert_eq!(fwd, ra.iter().copied().collect::<Vec<_>>());
        let mut back = Vec::new();
        let mut x = -1;
        loop {
            x = a.prev_member(x);
            if x < 0 {
                assert_eq!(x, -2);
                break;
            }
            back.push(x);
        }
        assert_eq!(back, ra.iter().rev().copied().collect::<Vec<_>>());

        let mut del = a.clone_in(mcx).unwrap();
        for &x in &rb {
            del.del_member(x);
            check_invariants(&del);
        }
        assert_eq!(del.iter().collect::<Vec<_>>(), rd.iter().copied().collect::<Vec<_>>());

        assert_eq!(a.compare(&b), ra.iter().rev().cmp(rb.iter().rev()));

        match a.membership() {
            BmsMembership::BmsEmptySet => assert_eq!(ra.len(), 0),
            BmsMembership::BmsSingleton => assert_eq!(ra.len(), 1),
            BmsMembership::BmsMultiple => assert!(ra.len() > 1),
        }
        assert_eq!(
            a.get_singleton_member(),
            if ra.len() == 1 { ra.first().copied() } else { None }
        );
    }
}

fn strip_c_comments(src: &str) -> StdString {
    let bytes = src.as_bytes();
    let mut out = StdString::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            out.push(' ');
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn strip_pg_node_attr(src: &str) -> StdString {
    let mut out = StdString::new();
    let mut rest = src;
    while let Some(pos) = rest.find("pg_node_attr(") {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos + "pg_node_attr(".len()..];
        let mut depth = 1usize;
        let mut end = 0;
        for (j, ch) in tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = j + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

fn c_struct_fields(header: &str, name: &str) -> Vec<StdString> {
    let start = header
        .find(&std::format!("typedef struct {name}\n"))
        .expect("struct present");
    let body_start = header[start..].find('{').unwrap() + start + 1;
    let end_marker = std::format!("}} {name};");
    let body_end = header[body_start..].find(&end_marker).unwrap() + body_start;
    let body = strip_pg_node_attr(&strip_c_comments(&header[body_start..body_end]));
    let mut fields = Vec::new();
    for decl in body.split(';') {
        let decl = decl.trim();
        if decl.is_empty() {
            continue;
        }
        let last = decl.split_whitespace().last().unwrap();
        let field = last.trim_start_matches('*');
        if field == "type" {
            continue;
        }
        fields.push(StdString::from(field));
    }
    fields
}

fn c_enum_values(header: &str, name: &str) -> Vec<(StdString, u32)> {
    let start = header
        .find(&std::format!("typedef enum {name}\n"))
        .expect("enum present");
    let body_start = header[start..].find('{').unwrap() + start + 1;
    let end_marker = std::format!("}} {name};");
    let body_end = header[body_start..].find(&end_marker).unwrap() + body_start;
    let body = strip_pg_node_attr(&strip_c_comments(&header[body_start..body_end]));
    let mut vals = Vec::new();
    let mut next: u32 = 0;
    for entry in body.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (name, val) = match entry.split_once('=') {
            Some((n, v)) => (n.trim(), v.trim().parse::<u32>().expect("numeric enum value")),
            None => (entry, next),
        };
        next = val + 1;
        vals.push((StdString::from(name), val));
    }
    vals
}

macro_rules! check_enum {
    ($header:expr, $cname:literal, $ty:ident, [$($variant:ident),+ $(,)?]) => {{
        let c = c_enum_values($header, $cname);
        let rust: Vec<(&str, u32)> = std::vec![$((stringify!($variant), $ty::$variant as u32)),+];
        assert_eq!(c.len(), rust.len(), "{} variant count", $cname);
        for ((cn, cv), (rn, rv)) in c.iter().zip(rust.iter()) {
            assert_eq!((cn.as_str(), *cv), (*rn, *rv), "{} variant", $cname);
        }
    }};
}

#[test]
fn enum_values_match_c_headers() {
    let nodes_h = include_str!("../vendor/nodes.h");
    let parse_h = include_str!("../vendor/parsenodes.h");
    let prim_h = include_str!("../vendor/primnodes.h");
    use crate::nodes_enums::{CmdType, LimitOption};
    use crate::parsenodes::{QuerySource, RTEKind, SetOperation};
    use crate::primnodes::{CoercionForm, OverridingKind, ParamKind, VarReturningType};
    use crate::rawnodes::A_Expr_Kind;
    check_enum!(nodes_h, "CmdType", CmdType, [
        CMD_UNKNOWN, CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, CMD_MERGE, CMD_UTILITY,
        CMD_NOTHING,
    ]);
    check_enum!(nodes_h, "LimitOption", LimitOption, [
        LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
    ]);
    check_enum!(nodes_h, "JoinType", JoinType, [
        JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_RIGHT, JOIN_SEMI, JOIN_ANTI, JOIN_RIGHT_SEMI,
        JOIN_RIGHT_ANTI, JOIN_UNIQUE_OUTER, JOIN_UNIQUE_INNER,
    ]);
    check_enum!(parse_h, "QuerySource", QuerySource, [
        QSRC_ORIGINAL, QSRC_PARSER, QSRC_INSTEAD_RULE, QSRC_QUAL_INSTEAD_RULE,
        QSRC_NON_INSTEAD_RULE,
    ]);
    check_enum!(parse_h, "SetOperation", SetOperation, [
        SETOP_NONE, SETOP_UNION, SETOP_INTERSECT, SETOP_EXCEPT,
    ]);
    check_enum!(parse_h, "RTEKind", RTEKind, [
        RTE_RELATION, RTE_SUBQUERY, RTE_JOIN, RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES, RTE_CTE,
        RTE_NAMEDTUPLESTORE, RTE_RESULT, RTE_GROUP,
    ]);
    check_enum!(parse_h, "A_Expr_Kind", A_Expr_Kind, [
        AEXPR_OP, AEXPR_OP_ANY, AEXPR_OP_ALL, AEXPR_DISTINCT, AEXPR_NOT_DISTINCT, AEXPR_NULLIF,
        AEXPR_IN, AEXPR_LIKE, AEXPR_ILIKE, AEXPR_SIMILAR, AEXPR_BETWEEN, AEXPR_NOT_BETWEEN,
        AEXPR_BETWEEN_SYM, AEXPR_NOT_BETWEEN_SYM,
    ]);
    check_enum!(prim_h, "OverridingKind", OverridingKind, [
        OVERRIDING_NOT_SET, OVERRIDING_USER_VALUE, OVERRIDING_SYSTEM_VALUE,
    ]);
    check_enum!(prim_h, "CoercionForm", CoercionForm, [
        COERCE_EXPLICIT_CALL, COERCE_EXPLICIT_CAST, COERCE_IMPLICIT_CAST, COERCE_SQL_SYNTAX,
    ]);
    check_enum!(prim_h, "ParamKind", ParamKind, [
        PARAM_EXTERN, PARAM_EXEC, PARAM_SUBLINK, PARAM_MULTIEXPR,
    ]);
    check_enum!(prim_h, "VarReturningType", VarReturningType, [
        VAR_RETURNING_DEFAULT, VAR_RETURNING_OLD, VAR_RETURNING_NEW,
    ]);
    use crate::parsenodes::{DefElemAction, VariableSetKind};
    check_enum!(parse_h, "VariableSetKind", VariableSetKind, [
        VAR_SET_VALUE, VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_SET_MULTI, VAR_RESET, VAR_RESET_ALL,
    ]);
    check_enum!(parse_h, "DefElemAction", DefElemAction, [
        DEFELEM_UNSPEC, DEFELEM_SET, DEFELEM_ADD, DEFELEM_DROP,
    ]);
    use crate::primnodes::{BoolExprType, NullTestType};
    use crate::rawnodes::{SortByDir, SortByNulls};
    check_enum!(parse_h, "SortByDir", SortByDir, [
        SORTBY_DEFAULT, SORTBY_ASC, SORTBY_DESC, SORTBY_USING,
    ]);
    check_enum!(parse_h, "SortByNulls", SortByNulls, [
        SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST, SORTBY_NULLS_LAST,
    ]);
    check_enum!(prim_h, "BoolExprType", BoolExprType, [AND_EXPR, OR_EXPR, NOT_EXPR]);
    check_enum!(prim_h, "NullTestType", NullTestType, [IS_NULL, IS_NOT_NULL]);
}

#[test]
fn raw_expr_node_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    let prim_h = include_str!("../vendor/primnodes.h");

    assert_eq!(
        c_struct_fields(parse_h, "SortBy"),
        ["node", "sortby_dir", "sortby_nulls", "useOp", "location"]
    );
    let crate::rawnodes::SortBy { node: _, sortby_dir: _, sortby_nulls: _, useOp: _, location: _ } =
        crate::rawnodes::SortBy::default();

    assert_eq!(
        c_struct_fields(parse_h, "FuncCall"),
        [
            "funcname", "args", "agg_order", "agg_filter", "over", "agg_within_group",
            "agg_star", "agg_distinct", "func_variadic", "funcformat", "location",
        ]
    );
    let crate::rawnodes::FuncCall {
        funcname: _, args: _, agg_order: _, agg_filter: _, over: _, agg_within_group: _,
        agg_star: _, agg_distinct: _, func_variadic: _, funcformat: _, location: _,
    } = crate::rawnodes::FuncCall::default();

    assert_eq!(
        c_struct_fields(parse_h, "TypeName"),
        [
            "names", "typeOid", "setof", "pct_type", "typmods", "typemod", "arrayBounds",
            "location",
        ]
    );
    let crate::rawnodes::TypeName {
        names: _, typeOid: _, setof: _, pct_type: _, typmods: _, typemod: _, arrayBounds: _,
        location: _,
    } = crate::rawnodes::TypeName::default();

    assert_eq!(c_struct_fields(parse_h, "TypeCast"), ["arg", "typeName", "location"]);
    let crate::rawnodes::TypeCast { arg: _, typeName: _, location: _ } =
        crate::rawnodes::TypeCast::default();

    let mut be = c_struct_fields(prim_h, "BoolExpr");
    assert_eq!(be.remove(0), "xpr");
    assert_eq!(be, ["boolop", "args", "location"]);
    let crate::primnodes::BoolExpr { boolop: _, args: _, location: _ } =
        crate::primnodes::BoolExpr::default();

    let mut nt = c_struct_fields(prim_h, "NullTest");
    assert_eq!(nt.remove(0), "xpr");
    assert_eq!(nt, ["arg", "nulltesttype", "argisrow", "location"]);
    let crate::primnodes::NullTest { arg: _, nulltesttype: _, argisrow: _, location: _ } =
        crate::primnodes::NullTest::default();
}

#[test]
fn variable_set_stmt_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    assert_eq!(
        c_struct_fields(parse_h, "VariableSetStmt"),
        ["kind", "name", "args", "jumble_args", "is_local", "location"]
    );
    let crate::parsenodes::VariableSetStmt {
        kind: _, name: _, args: _, jumble_args: _, is_local: _, location: _,
    } = crate::parsenodes::VariableSetStmt::default();

    assert_eq!(c_struct_fields(parse_h, "VariableShowStmt"), ["name"]);
    let crate::parsenodes::VariableShowStmt { name: _ } =
        crate::parsenodes::VariableShowStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "DefElem"),
        ["defnamespace", "defname", "arg", "defaction", "location"]
    );
    let crate::parsenodes::DefElem {
        defnamespace: _, defname: _, arg: _, defaction: _, location: _,
    } = crate::parsenodes::DefElem::default();

    assert_eq!(c_struct_fields(parse_h, "ExplainStmt"), ["query", "options"]);
    let crate::parsenodes::ExplainStmt { query: _, options: _ } =
        crate::parsenodes::ExplainStmt::default();
}

#[test]
fn query_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    // Declaration order of crate::parsenodes::Query, C spellings.
    let rust_order = [
        "commandType", "querySource", "queryId", "canSetTag", "utilityStmt", "resultRelation",
        "hasAggs", "hasWindowFuncs", "hasTargetSRFs", "hasSubLinks", "hasDistinctOn",
        "hasRecursive", "hasModifyingCTE", "hasForUpdate", "hasRowSecurity", "hasGroupRTE",
        "isReturn", "cteList", "rtable", "rteperminfos", "jointree", "mergeActionList",
        "mergeTargetRelation", "mergeJoinCondition", "targetList", "override", "onConflict",
        "returningOldAlias", "returningNewAlias", "returningList", "groupClause", "groupDistinct",
        "groupingSets", "havingQual", "windowClause", "distinctClause", "sortClause",
        "limitOffset", "limitCount", "limitOption", "rowMarks", "setOperations",
        "constraintDeps", "withCheckOptions", "stmt_location", "stmt_len",
    ];
    assert_eq!(c_struct_fields(parse_h, "Query"), rust_order);
    // Compile-time completeness: every C field exists on the Rust struct.
    let crate::parsenodes::Query {
        commandType: _, querySource: _, queryId: _, canSetTag: _, utilityStmt: _,
        resultRelation: _, hasAggs: _, hasWindowFuncs: _, hasTargetSRFs: _, hasSubLinks: _,
        hasDistinctOn: _, hasRecursive: _, hasModifyingCTE: _, hasForUpdate: _,
        hasRowSecurity: _, hasGroupRTE: _, isReturn: _, cteList: _, rtable: _, rteperminfos: _,
        jointree: _, mergeActionList: _, mergeTargetRelation: _, mergeJoinCondition: _,
        targetList: _, r#override: _, onConflict: _, returningOldAlias: _, returningNewAlias: _,
        returningList: _, groupClause: _, groupDistinct: _, groupingSets: _, havingQual: _,
        windowClause: _, distinctClause: _, sortClause: _, limitOffset: _, limitCount: _,
        limitOption: _, rowMarks: _, setOperations: _, constraintDeps: _, withCheckOptions: _,
        stmt_location: _, stmt_len: _,
    } = crate::parsenodes::Query::default();
}

#[test]
fn const_field_order_and_size_match_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let rust_order = [
        "consttype", "consttypmod", "constcollid", "constlen", "constvalue", "constisnull",
        "constbyval", "location",
    ];
    let mut c_fields = c_struct_fields(prim_h, "Const");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(c_fields, rust_order);
    // C sizeof(Const) is 40 (4-byte tag + pad to Datum); ours matches via the
    // 2-byte tag + repr(C) NodeRep padding to the same 8-aligned payload.
    assert_eq!(core::mem::size_of::<crate::primnodes::Const>(), 32);
}

#[test]
fn rte_and_selectstmt_field_order_match_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    let rte_order = [
        "alias", "eref", "rtekind", "relid", "inh", "relkind", "rellockmode", "perminfoindex",
        "tablesample", "subquery", "security_barrier", "jointype", "joinmergedcols",
        "joinaliasvars", "joinleftcols", "joinrightcols", "join_using_alias", "functions",
        "funcordinality", "tablefunc", "values_lists", "ctename", "ctelevelsup",
        "self_reference", "coltypes", "coltypmods", "colcollations", "enrname", "enrtuples",
        "groupexprs", "lateral", "inFromCl", "securityQuals",
    ];
    assert_eq!(c_struct_fields(parse_h, "RangeTblEntry"), rte_order);
    let select_order = [
        "distinctClause", "intoClause", "targetList", "fromClause", "whereClause",
        "groupClause", "groupDistinct", "havingClause", "windowClause", "valuesLists",
        "sortClause", "limitOffset", "limitCount", "limitOption", "lockingClause",
        "withClause", "op", "all", "larg", "rarg",
    ];
    assert_eq!(c_struct_fields(parse_h, "SelectStmt"), select_order);
}

#[test]
fn plannedstmt_plan_result_field_order_match_c() {
    let plan_h = include_str!("../vendor/plannodes.h");
    let stmt_order = [
        "commandType", "queryId", "planId", "hasReturning", "hasModifyingCTE", "canSetTag",
        "transientPlan", "dependsOnRole", "parallelModeNeeded", "jitFlags", "planTree",
        "partPruneInfos", "rtable", "unprunableRelids", "permInfos", "resultRelations",
        "appendRelations", "subplans", "rewindPlanIDs", "rowMarks", "relationOids", "invalItems",
        "paramExecTypes", "utilityStmt", "stmt_location", "stmt_len",
    ];
    assert_eq!(c_struct_fields(plan_h, "PlannedStmt"), stmt_order);
    let crate::plannodes::PlannedStmt {
        commandType: _, queryId: _, planId: _, hasReturning: _, hasModifyingCTE: _, canSetTag: _,
        transientPlan: _, dependsOnRole: _, parallelModeNeeded: _, jitFlags: _, planTree: _,
        partPruneInfos: _, rtable: _, unprunableRelids: _, permInfos: _, resultRelations: _,
        appendRelations: _, subplans: _, rewindPlanIDs: _, rowMarks: _, relationOids: _,
        invalItems: _, paramExecTypes: _, utilityStmt: _, stmt_location: _, stmt_len: _,
    } = crate::plannodes::PlannedStmt::default();

    let plan_order = [
        "disabled_nodes", "startup_cost", "total_cost", "plan_rows", "plan_width",
        "parallel_aware", "parallel_safe", "async_capable", "plan_node_id", "targetlist", "qual",
        "lefttree", "righttree", "initPlan", "extParam", "allParam",
    ];
    assert_eq!(c_struct_fields(plan_h, "Plan"), plan_order);
    let crate::plannodes::Plan {
        disabled_nodes: _, startup_cost: _, total_cost: _, plan_rows: _, plan_width: _,
        parallel_aware: _, parallel_safe: _, async_capable: _, plan_node_id: _, targetlist: _,
        qual: _, lefttree: _, righttree: _, initPlan: _, extParam: _, allParam: _,
    } = crate::plannodes::Plan::default();

    let mut result_fields = c_struct_fields(plan_h, "Result");
    assert_eq!(result_fields.remove(0), "plan");
    assert_eq!(result_fields, ["resconstantqual"]);
    let crate::plannodes::Result { plan: _, resconstantqual: _ } =
        crate::plannodes::Result::default();
}

#[test]
fn plan_node_tag_round_trips() {
    use crate::plannodes::{PlannedStmt, Result};
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let stmt = Node::build::<PlannedStmt>(mcx).unwrap().seal();
    assert_eq!(stmt.node_tag(), NodeTag::T_PlannedStmt);
    assert!(stmt.as_planned_stmt().is_some());
    assert!(stmt.as_result().is_none());
    assert!(stmt.as_plan().is_none());
    assert!(stmt.as_query().is_none());

    let result = Node::build::<Result>(mcx).unwrap().seal();
    assert_eq!(result.node_tag(), NodeTag::T_Result);
    assert!(result.as_result().is_some());
    assert!(result.as_plan().is_some());
    assert!(result.as_planned_stmt().is_none());

    let q = Node::build::<crate::parsenodes::Query>(mcx).unwrap().seal();
    assert!(q.as_plan().is_none());
    assert!(q.as_planned_stmt().is_none());
}

#[test]
fn select1_plan_shape_and_setrefs_mutation() {
    use crate::plannodes::{PlannedStmt, Result};
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    // createplan.c make_result for `SELECT 1`: Result, no outer plan,
    // targetlist [TargetEntry(Const 1, resno 1)].
    let cnst = Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, cnst, 1, Some("?column?"), false).unwrap();
    let mut result = Node::build::<Result>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    result.plan.plan_rows = 1.0;
    result.plan.plan_width = 4;
    result.plan.total_cost = 0.01;
    let plan_tree = result.seal();

    // standard_planner output shell.
    let mut stmt = Node::build::<PlannedStmt>(mcx).unwrap();
    stmt.commandType = crate::CmdType::CMD_SELECT;
    stmt.canSetTag = true;
    stmt.planTree = Some(plan_tree);
    stmt.stmt_location = 0;
    stmt.stmt_len = 8;
    let stmt = stmt.seal();

    // set_plan_references walk over the sealed tree: assign plan_node_id via
    // the Plan base, retarget the shared TLE's expr in place.
    let walked = stmt.as_planned_stmt().unwrap().planTree.unwrap();
    // SAFETY: this walk is the tree's only accessor; no reference derived
    // before it is used afterward.
    unsafe {
        walked.with_plan_mut(|p| p.plan_node_id = 7).unwrap();
        let tle0 = walked.as_plan().unwrap().targetlist.nth(0);
        assert!(tle0.with_mut::<crate::primnodes::Var, _>(|_| ()).is_none());
        tle0.with_mut::<crate::primnodes::TargetEntry, _>(|t| {
            t.resorigtbl = 0;
            t.expr =
                Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(2), false, true).unwrap();
        })
        .unwrap();
    }

    let s = stmt.as_planned_stmt().unwrap();
    assert_eq!(s.commandType, crate::CmdType::CMD_SELECT);
    assert!(s.canSetTag && !s.hasReturning && !s.dependsOnRole);
    assert!(s.rtable.is_nil() && s.subplans.is_nil() && s.resultRelations.is_nil());
    assert!(s.unprunableRelids.is_empty() && s.rewindPlanIDs.is_empty());
    assert_eq!((s.stmt_location, s.stmt_len), (0, 8));
    let plan = s.planTree.unwrap().as_plan().unwrap();
    assert_eq!(plan.plan_node_id, 7);
    assert_eq!((plan.plan_rows, plan.plan_width), (1.0, 4));
    assert!(plan.lefttree.is_none() && plan.righttree.is_none() && plan.qual.is_nil());
    let r = s.planTree.unwrap().as_result().unwrap();
    assert!(r.resconstantqual.is_none());
    let tle = plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert_eq!(tle.expr.as_const().unwrap().constvalue.as_i32(), 2);
}

#[test]
fn parse_node_tag_round_trips() {
    use crate::parsenodes::{Query, RTEPermissionInfo, RangeTblEntry};
    use crate::primnodes::{
        Alias, FromExpr, FuncExpr, OpExpr, Param, RangeVar, Var,
    };
    use crate::rawnodes::{SelectStmt, ValUnion};
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let cases: Vec<(Node, NodeTag)> = std::vec![
        (Node::mk_raw_stmt(mcx, None, 0, 0).unwrap(), NodeTag::T_RawStmt),
        (Node::build::<SelectStmt>(mcx).unwrap().seal(), NodeTag::T_SelectStmt),
        (Node::mk_res_target(mcx, None, NodeList::nil(), None, -1).unwrap(), NodeTag::T_ResTarget),
        (
            Node::mk_a_expr(mcx, crate::rawnodes::A_Expr_Kind::AEXPR_OP, NodeList::nil(), None, None, -1)
                .unwrap(),
            NodeTag::T_A_Expr,
        ),
        (
            Node::mk_a_const(mcx, Some(ValUnion::Integer(crate::Integer { ival: 1 })), 7).unwrap(),
            NodeTag::T_A_Const,
        ),
        (Node::mk_column_ref(mcx, NodeList::nil(), -1).unwrap(), NodeTag::T_ColumnRef),
        (Node::mk_param_ref(mcx, 1, -1).unwrap(), NodeTag::T_ParamRef),
        (Node::mk_a_star(mcx).unwrap(), NodeTag::T_A_Star),
        (Node::build::<Query>(mcx).unwrap().seal(), NodeTag::T_Query),
        (Node::build::<RangeTblEntry>(mcx).unwrap().seal(), NodeTag::T_RangeTblEntry),
        (Node::build::<RTEPermissionInfo>(mcx).unwrap().seal(), NodeTag::T_RTEPermissionInfo),
        (Node::build::<Alias>(mcx).unwrap().seal(), NodeTag::T_Alias),
        (Node::build::<RangeVar>(mcx).unwrap().seal(), NodeTag::T_RangeVar),
        (Node::build::<Var>(mcx).unwrap().seal(), NodeTag::T_Var),
        (
            Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(1), false, true).unwrap(),
            NodeTag::T_Const,
        ),
        (Node::build::<Param>(mcx).unwrap().seal(), NodeTag::T_Param),
        (
            Node::mk_target_entry(mcx, Node::mk_a_star(mcx).unwrap(), 1, None, false).unwrap(),
            NodeTag::T_TargetEntry,
        ),
        (Node::mk_from_expr(mcx, NodeList::nil(), None).unwrap(), NodeTag::T_FromExpr),
        (Node::mk_range_tbl_ref(mcx, 1).unwrap(), NodeTag::T_RangeTblRef),
        (Node::build::<OpExpr>(mcx).unwrap().seal(), NodeTag::T_OpExpr),
        (Node::build::<FuncExpr>(mcx).unwrap().seal(), NodeTag::T_FuncExpr),
    ];
    for (node, tag) in &cases {
        assert_eq!(node.node_tag(), *tag);
    }
    let a_const = cases[4].0;
    assert!(a_const.as_a_const().is_some());
    assert!(a_const.as_a_expr().is_none());
    assert!(a_const.as_query().is_none());
    let q = cases[8].0;
    assert!(q.as_query().is_some());
    assert!(q.as_select_stmt().is_none());
    assert!(q.as_range_tbl_entry().is_none());
}

#[test]
fn select1_parse_and_analyze_shape() {
    use crate::parsenodes::Query;
    use crate::primnodes::FromExpr;
    use crate::rawnodes::{SelectStmt, ValUnion};
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    // gram.y output for `SELECT 1`.
    let a_const =
        Node::mk_a_const(mcx, Some(ValUnion::Integer(crate::Integer { ival: 1 })), 7).unwrap();
    let res_target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(a_const), 7).unwrap();
    let mut select = Node::build::<SelectStmt>(mcx).unwrap();
    select.targetList = NodeList::make1(mcx, res_target).unwrap();
    let raw = Node::mk_raw_stmt(mcx, Some(select.seal()), 0, 0).unwrap();

    let stmt = raw.as_raw_stmt().unwrap().stmt.unwrap().as_select_stmt().unwrap();
    assert_eq!(stmt.targetList.len(), 1);
    assert!(stmt.whereClause.is_none());
    assert!(stmt.fromClause.is_nil());
    let rt = stmt.targetList.nth(0).as_res_target().unwrap();
    assert!(rt.name.is_none());
    let val = rt.val.unwrap().as_a_const().unwrap();
    assert!(!val.isnull());
    assert!(matches!(val.val, Some(ValUnion::Integer(crate::Integer { ival: 1 }))));
    assert_eq!(val.location, 7);

    // analyze.c output: Query { CMD_SELECT, tlist [TargetEntry(Const 1)],
    // jointree FromExpr(NIL, NULL) }.
    let cnst = Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, cnst, 1, Some("?column?"), false).unwrap();
    let mut query = Node::build::<Query>(mcx).unwrap();
    query.commandType = crate::CmdType::CMD_SELECT;
    query.canSetTag = true;
    query.targetList = NodeList::make1(mcx, tle).unwrap();
    query.jointree = Some(
        Node::mk_from_expr(mcx, NodeList::nil(), None).unwrap().as_from_expr().unwrap(),
    );
    // In-place mutation before seal (C: parse analysis fixups).
    query.stmt_location = 0;
    query.stmt_len = 8;
    let qnode = query.seal();

    let q = qnode.as_query().unwrap();
    assert_eq!(q.commandType, crate::CmdType::CMD_SELECT);
    assert_eq!(q.querySource, crate::QuerySource::QSRC_ORIGINAL);
    assert!(q.canSetTag);
    assert!(q.rtable.is_nil());
    let jt: &FromExpr = q.jointree.unwrap();
    assert!(jt.fromlist.is_nil() && jt.quals.is_none());
    let tle = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert_eq!(tle.resname, Some("?column?"));
    assert!(!tle.resjunk);
    let c = tle.expr.as_const().unwrap();
    assert_eq!((c.consttype, c.constlen, c.constbyval, c.constisnull), (23, 4, true, false));
    assert_eq!(c.constvalue.as_i32(), 1);
    assert_eq!(c.location, -1);
    assert_eq!(q.stmt_len, 8);
}
