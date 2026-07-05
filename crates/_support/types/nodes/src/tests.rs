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
    use crate::rawnodes::{A_Expr_Kind, JsonTableColumnType};
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
    use crate::primnodes::CoercionContext;
    check_enum!(prim_h, "CoercionContext", CoercionContext, [
        COERCION_IMPLICIT, COERCION_ASSIGNMENT, COERCION_PLPGSQL, COERCION_EXPLICIT,
    ]);
    check_enum!(prim_h, "ParamKind", ParamKind, [
        PARAM_EXTERN, PARAM_EXEC, PARAM_SUBLINK, PARAM_MULTIEXPR,
    ]);
    check_enum!(prim_h, "VarReturningType", VarReturningType, [
        VAR_RETURNING_DEFAULT, VAR_RETURNING_OLD, VAR_RETURNING_NEW,
    ]);
    use crate::primnodes::SubLinkType;
    check_enum!(prim_h, "SubLinkType", SubLinkType, [
        EXISTS_SUBLINK, ALL_SUBLINK, ANY_SUBLINK, ROWCOMPARE_SUBLINK, EXPR_SUBLINK,
        MULTIEXPR_SUBLINK, ARRAY_SUBLINK, CTE_SUBLINK,
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
    use crate::parsenodes::{DropBehavior, ObjectType};
    check_enum!(parse_h, "ObjectType", ObjectType, [
        OBJECT_ACCESS_METHOD, OBJECT_AGGREGATE, OBJECT_AMOP, OBJECT_AMPROC, OBJECT_ATTRIBUTE,
        OBJECT_CAST, OBJECT_COLUMN, OBJECT_COLLATION, OBJECT_CONVERSION, OBJECT_DATABASE,
        OBJECT_DEFAULT, OBJECT_DEFACL, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT, OBJECT_EVENT_TRIGGER,
        OBJECT_EXTENSION, OBJECT_FDW, OBJECT_FOREIGN_SERVER, OBJECT_FOREIGN_TABLE,
        OBJECT_FUNCTION, OBJECT_INDEX, OBJECT_LANGUAGE, OBJECT_LARGEOBJECT, OBJECT_MATVIEW,
        OBJECT_OPCLASS, OBJECT_OPERATOR, OBJECT_OPFAMILY, OBJECT_PARAMETER_ACL, OBJECT_POLICY,
        OBJECT_PROCEDURE, OBJECT_PUBLICATION, OBJECT_PUBLICATION_NAMESPACE,
        OBJECT_PUBLICATION_REL, OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE, OBJECT_SCHEMA,
        OBJECT_SEQUENCE, OBJECT_SUBSCRIPTION, OBJECT_STATISTIC_EXT, OBJECT_TABCONSTRAINT,
        OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRANSFORM, OBJECT_TRIGGER,
        OBJECT_TSCONFIGURATION, OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE,
        OBJECT_TYPE, OBJECT_USER_MAPPING, OBJECT_VIEW,
    ]);
    check_enum!(parse_h, "DropBehavior", DropBehavior, [DROP_RESTRICT, DROP_CASCADE]);
    let lockopt_h = include_str!("../vendor/lockoptions.h");
    use crate::nodes_enums::{LockClauseStrength, LockWaitPolicy};
    check_enum!(lockopt_h, "LockClauseStrength", LockClauseStrength, [
        LCS_NONE, LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE,
    ]);
    check_enum!(lockopt_h, "LockWaitPolicy", LockWaitPolicy, [
        LockWaitBlock, LockWaitSkip, LockWaitError,
    ]);
    let plan_h = include_str!("../vendor/plannodes.h");
    use crate::plannodes::RowMarkType;
    check_enum!(plan_h, "RowMarkType", RowMarkType, [
        ROW_MARK_EXCLUSIVE, ROW_MARK_NOKEYEXCLUSIVE, ROW_MARK_SHARE, ROW_MARK_KEYSHARE,
        ROW_MARK_REFERENCE, ROW_MARK_COPY,
    ]);
    use crate::primnodes::{TableFuncType, XmlExprOp, XmlOptionType};
    check_enum!(prim_h, "XmlExprOp", XmlExprOp, [
        IS_XMLCONCAT, IS_XMLELEMENT, IS_XMLFOREST, IS_XMLPARSE, IS_XMLPI, IS_XMLROOT,
        IS_XMLSERIALIZE, IS_DOCUMENT,
    ]);
    check_enum!(prim_h, "XmlOptionType", XmlOptionType, [
        XMLOPTION_DOCUMENT, XMLOPTION_CONTENT,
    ]);
    check_enum!(prim_h, "TableFuncType", TableFuncType, [TFT_XMLTABLE, TFT_JSON_TABLE]);
    check_enum!(parse_h, "JsonTableColumnType", JsonTableColumnType, [
        JTC_FOR_ORDINALITY, JTC_REGULAR, JTC_EXISTS, JTC_FORMATTED, JTC_NESTED,
    ]);
}

#[test]
fn xml_node_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    let prim_h = include_str!("../vendor/primnodes.h");

    assert_eq!(
        c_struct_fields(parse_h, "RangeTableFunc"),
        ["lateral", "docexpr", "rowexpr", "namespaces", "columns", "alias", "location"]
    );
    let crate::rawnodes::RangeTableFunc {
        lateral: _, docexpr: _, rowexpr: _, namespaces: _, columns: _, alias: _, location: _,
    } = crate::rawnodes::RangeTableFunc::default();

    assert_eq!(
        c_struct_fields(parse_h, "RangeTableFuncCol"),
        ["colname", "typeName", "for_ordinality", "is_not_null", "colexpr", "coldefexpr",
         "location"]
    );
    let crate::rawnodes::RangeTableFuncCol {
        colname: _, typeName: _, for_ordinality: _, is_not_null: _, colexpr: _, coldefexpr: _,
        location: _,
    } = crate::rawnodes::RangeTableFuncCol::default();

    assert_eq!(
        c_struct_fields(parse_h, "XmlSerialize"),
        ["xmloption", "expr", "typeName", "indent", "location"]
    );
    let crate::rawnodes::XmlSerialize {
        xmloption: _, expr: _, typeName: _, indent: _, location: _,
    } = crate::rawnodes::XmlSerialize::default();

    // The harness drops C fields literally named "type" (the NodeTag skip), so
    // XmlExpr's Oid `type` (Rust r#type) is absent from the expected list.
    let mut xe = c_struct_fields(prim_h, "XmlExpr");
    assert_eq!(xe.remove(0), "xpr");
    assert_eq!(
        xe,
        ["op", "name", "named_args", "arg_names", "args", "xmloption", "indent", "typmod",
         "location"]
    );
    let crate::primnodes::XmlExpr {
        op: _, name: _, named_args: _, arg_names: _, args: _, xmloption: _, indent: _,
        r#type: _, typmod: _, location: _,
    } = crate::primnodes::XmlExpr::default();

    assert_eq!(
        c_struct_fields(prim_h, "TableFunc"),
        ["functype", "ns_uris", "ns_names", "docexpr", "rowexpr", "colnames", "coltypes",
         "coltypmods", "colcollations", "colexprs", "coldefexprs", "colvalexprs",
         "passingvalexprs", "notnulls", "plan", "ordinalitycol", "location"]
    );
    let crate::primnodes::TableFunc {
        functype: _, ns_uris: _, ns_names: _, docexpr: _, rowexpr: _, colnames: _, coltypes: _,
        coltypmods: _, colcollations: _, colexprs: _, coldefexprs: _, colvalexprs: _,
        passingvalexprs: _, notnulls: _, plan: _, ordinalitycol: _, location: _,
    } = crate::primnodes::TableFunc::default();
}

#[test]
fn rowmark_node_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    let plan_h = include_str!("../vendor/plannodes.h");

    assert_eq!(
        c_struct_fields(parse_h, "LockingClause"),
        ["lockedRels", "strength", "waitPolicy"]
    );
    let crate::rawnodes::LockingClause { lockedRels: _, strength: _, waitPolicy: _ } =
        crate::rawnodes::LockingClause::default();

    assert_eq!(
        c_struct_fields(parse_h, "RowMarkClause"),
        ["rti", "strength", "waitPolicy", "pushedDown"]
    );
    let crate::parsenodes::RowMarkClause { rti: _, strength: _, waitPolicy: _, pushedDown: _ } =
        crate::parsenodes::RowMarkClause::default();

    assert_eq!(
        c_struct_fields(plan_h, "PlanRowMark"),
        [
            "rti", "prti", "rowmarkId", "markType", "allMarkTypes", "strength", "waitPolicy",
            "isParent",
        ]
    );
    let crate::plannodes::PlanRowMark {
        rti: _, prti: _, rowmarkId: _, markType: _, allMarkTypes: _, strength: _, waitPolicy: _,
        isParent: _,
    } = crate::plannodes::PlanRowMark::default();

    assert_eq!(c_struct_fields(plan_h, "LockRows"), ["plan", "rowMarks", "epqParam"]);
    let crate::plannodes::LockRows { plan: _, rowMarks: _, epqParam: _ } =
        crate::plannodes::LockRows::default();
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

    assert_eq!(
        c_struct_fields(parse_h, "DeleteStmt"),
        ["relation", "usingClause", "whereClause", "returningClause", "withClause"]
    );
    let crate::rawnodes::DeleteStmt {
        relation: _, usingClause: _, whereClause: _, returningClause: _, withClause: _,
    } = crate::rawnodes::DeleteStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "UpdateStmt"),
        ["relation", "targetList", "whereClause", "fromClause", "returningClause", "withClause"]
    );
    let crate::rawnodes::UpdateStmt {
        relation: _, targetList: _, whereClause: _, fromClause: _, returningClause: _,
        withClause: _,
    } = crate::rawnodes::UpdateStmt::default();

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

    let mut ce = c_struct_fields(prim_h, "CaseExpr");
    assert_eq!(ce.remove(0), "xpr");
    assert_eq!(ce, ["casetype", "casecollid", "arg", "args", "defresult", "location"]);
    let crate::primnodes::CaseExpr {
        casetype: _,
        casecollid: _,
        arg: _,
        args: _,
        defresult: _,
        location: _,
    } = crate::primnodes::CaseExpr::default();

    let mut ct = c_struct_fields(prim_h, "CaseTestExpr");
    assert_eq!(ct.remove(0), "xpr");
    assert_eq!(ct, ["typeId", "typeMod", "collation"]);
    let crate::primnodes::CaseTestExpr { typeId: _, typeMod: _, collation: _ } =
        crate::primnodes::CaseTestExpr::default();

    let mut cw = c_struct_fields(prim_h, "CaseWhen");
    assert_eq!(cw.remove(0), "xpr");
    assert_eq!(cw, ["expr", "result", "location"]);
    let crate::primnodes::CaseWhen { expr: _, result: _, location: _ } =
        crate::primnodes::CaseWhen::default();

    let mut co = c_struct_fields(prim_h, "CoalesceExpr");
    assert_eq!(co.remove(0), "xpr");
    assert_eq!(co, ["coalescetype", "coalescecollid", "args", "location"]);
    let crate::primnodes::CoalesceExpr {
        coalescetype: _,
        coalescecollid: _,
        args: _,
        location: _,
    } = crate::primnodes::CoalesceExpr::default();

    let mut mm = c_struct_fields(prim_h, "MinMaxExpr");
    assert_eq!(mm.remove(0), "xpr");
    assert_eq!(mm, ["minmaxtype", "minmaxcollid", "inputcollid", "op", "args", "location"]);
    let crate::primnodes::MinMaxExpr {
        minmaxtype: _,
        minmaxcollid: _,
        inputcollid: _,
        op: _,
        args: _,
        location: _,
    } = crate::primnodes::MinMaxExpr::default();
    use crate::primnodes::MinMaxOp;
    check_enum!(prim_h, "MinMaxOp", MinMaxOp, [IS_GREATEST, IS_LEAST]);

    let mut na = c_struct_fields(prim_h, "NamedArgExpr");
    assert_eq!(na.remove(0), "xpr");
    assert_eq!(na, ["arg", "name", "argnumber", "location"]);

    assert_eq!(c_struct_fields(parse_h, "RangeSubselect"), ["lateral", "subquery", "alias"]);
    let crate::rawnodes::RangeSubselect { lateral: _, subquery: _, alias: _ } =
        crate::rawnodes::RangeSubselect::default();
}

#[test]
fn call_stmt_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    assert_eq!(c_struct_fields(parse_h, "CallStmt"), ["funccall", "funcexpr", "outargs"]);
    let crate::rawnodes::CallStmt { funccall: _, funcexpr: _, outargs: _ } =
        crate::rawnodes::CallStmt::default();

    assert_eq!(c_struct_fields(parse_h, "CallContext"), ["atomic"]);
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

    assert_eq!(c_struct_fields(parse_h, "DoStmt"), ["args"]);
    let crate::parsenodes::DoStmt { args: _ } = crate::parsenodes::DoStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "DefElem"),
        ["defnamespace", "defname", "arg", "defaction", "location"]
    );
    let crate::parsenodes::DefElem {
        defnamespace: _, defname: _, arg: _, defaction: _, location: _,
    } = crate::parsenodes::DefElem::default();

    assert_eq!(
        c_struct_fields(parse_h, "CopyStmt"),
        [
            "relation", "query", "attlist", "is_from", "is_program", "filename", "options",
            "whereClause",
        ]
    );
    let crate::parsenodes::CopyStmt {
        relation: _, query: _, attlist: _, is_from: _, is_program: _, filename: _, options: _,
        whereClause: _,
    } = crate::parsenodes::CopyStmt::default();

    assert_eq!(c_struct_fields(parse_h, "ExplainStmt"), ["query", "options"]);
    let crate::parsenodes::ExplainStmt { query: _, options: _ } =
        crate::parsenodes::ExplainStmt::default();

    assert_eq!(c_struct_fields(parse_h, "VacuumStmt"), ["options", "rels", "is_vacuumcmd"]);
    let crate::parsenodes::VacuumStmt { options: _, rels: _, is_vacuumcmd: _ } =
        crate::parsenodes::VacuumStmt::default();

    assert_eq!(c_struct_fields(parse_h, "VacuumRelation"), ["relation", "oid", "va_cols"]);
    let crate::parsenodes::VacuumRelation { relation: _, oid: _, va_cols: _ } =
        crate::parsenodes::VacuumRelation::default();

    assert_eq!(
        c_struct_fields(parse_h, "FetchStmt"),
        ["direction", "howMany", "portalname", "ismove"]
    );
    let crate::parsenodes::FetchStmt { direction, howMany, portalname: _, ismove: _ } =
        crate::parsenodes::FetchStmt::default();
    assert_eq!(direction, crate::parsenodes::FetchDirection::FETCH_FORWARD);
    assert_eq!(howMany, 0);
    assert_eq!(crate::parsenodes::FETCH_ALL, i64::MAX);

    assert_eq!(
        c_struct_fields(parse_h, "DropStmt"),
        ["objects", "removeType", "behavior", "missing_ok", "concurrent"]
    );
    let crate::parsenodes::DropStmt {
        objects: _, removeType: _, behavior: _, missing_ok: _, concurrent: _,
    } = crate::parsenodes::DropStmt::default();
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

#[test]
fn join_expr_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    assert_eq!(
        c_struct_fields(prim_h, "JoinExpr"),
        [
            "jointype", "isNatural", "larg", "rarg", "usingClause", "join_using_alias",
            "quals", "alias", "rtindex",
        ]
    );
}

#[test]
fn scalar_array_op_expr_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let mut c_fields = c_struct_fields(prim_h, "ScalarArrayOpExpr");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(
        c_fields,
        ["opno", "opfuncid", "hashfuncid", "negfuncid", "useOr", "inputcollid", "args",
         "location"]
    );
}

#[test]
fn array_expr_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let mut c_fields = c_struct_fields(prim_h, "ArrayExpr");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(
        c_fields,
        ["array_typeid", "array_collid", "element_typeid", "elements", "multidims",
         "list_start", "list_end", "location"]
    );
}

#[test]
fn sublink_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let rust_order =
        ["subLinkType", "subLinkId", "testexpr", "operName", "subselect", "location"];
    let mut c_fields = c_struct_fields(prim_h, "SubLink");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(c_fields, rust_order);
}

#[test]
fn subplan_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let rust_order = [
        "subLinkType", "testexpr", "paramIds", "plan_id", "plan_name", "firstColType",
        "firstColTypmod", "firstColCollation", "useHashTable", "unknownEqFalse",
        "parallel_safe", "setParam", "parParam", "args", "startup_cost", "per_call_cost",
    ];
    let mut c_fields = c_struct_fields(prim_h, "SubPlan");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(c_fields, rust_order);
    let crate::primnodes::SubPlan {
        subLinkType: _, testexpr: _, paramIds: _, plan_id: _, plan_name: _, firstColType: _,
        firstColTypmod: _, firstColCollation: _, useHashTable: _, unknownEqFalse: _,
        parallel_safe: _, setParam: _, parParam: _, args: _, startup_cost: _, per_call_cost: _,
    } = crate::primnodes::SubPlan::default();
}

#[test]
fn aggref_field_order_matches_c() {
    let prim_h = include_str!("../vendor/primnodes.h");
    let rust_order = [
        "aggfnoid", "aggtype", "aggcollid", "inputcollid", "aggtranstype", "aggargtypes",
        "aggdirectargs", "args", "aggorder", "aggdistinct", "aggfilter", "aggstar",
        "aggvariadic", "aggkind", "aggpresorted", "agglevelsup", "aggsplit", "aggno",
        "aggtransno", "location",
    ];
    let mut c_fields = c_struct_fields(prim_h, "Aggref");
    assert_eq!(c_fields.remove(0), "xpr");
    assert_eq!(c_fields, rust_order);
    let crate::primnodes::Aggref {
        aggfnoid: _, aggtype: _, aggcollid: _, inputcollid: _, aggtranstype: _, aggargtypes: _,
        aggdirectargs: _, args: _, aggorder: _, aggdistinct: _, aggfilter: _, aggstar: _,
        aggvariadic: _, aggkind: _, aggpresorted: _, agglevelsup: _, aggsplit: _, aggno: _,
        aggtransno: _, location: _,
    } = crate::primnodes::Aggref::default();
}

#[test]
fn agg_plan_field_order_matches_c() {
    let plan_h = include_str!("../vendor/plannodes.h");
    let rust_order = [
        "aggstrategy", "aggsplit", "numCols", "grpColIdx", "grpOperators", "grpCollations",
        "numGroups", "transitionSpace", "aggParams", "groupingSets", "chain",
    ];
    let mut c_fields = c_struct_fields(plan_h, "Agg");
    assert_eq!(c_fields.remove(0), "plan");
    assert_eq!(c_fields, rust_order);
    let crate::plannodes::Agg {
        plan: _, aggstrategy: _, aggsplit: _, numCols: _, grpColIdx: _, grpOperators: _,
        grpCollations: _, numGroups: _, transitionSpace: _, aggParams: _, groupingSets: _,
        chain: _,
    } = crate::plannodes::Agg::default();
}

#[test]
fn sort_group_clause_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    assert_eq!(
        c_struct_fields(parse_h, "SortGroupClause"),
        ["tleSortGroupRef", "eqop", "sortop", "reverse_sort", "nulls_first", "hashable"]
    );
    let crate::parsenodes::SortGroupClause {
        tleSortGroupRef: _, eqop: _, sortop: _, reverse_sort: _, nulls_first: _, hashable: _,
    } = crate::parsenodes::SortGroupClause::default();
}

fn mk_var_at(mcx: mcx::Mcx<'_>, varno: i32, attno: i16, location: i32) -> Node<'_> {
    let mut v = crate::primnodes::Var {
        varno,
        varattno: attno,
        vartype: 23,
        vartypmod: -1,
        varcollid: 0,
        ..Default::default()
    };
    v.location = location;
    v.varnosyn = varno as u32 + 7;
    v.varattnosyn = attno + 7;
    Node::mk(mcx, v).unwrap()
}

#[test]
fn equal_ignores_location_and_jumble_fields() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // Var: location + varnosyn/varattnosyn are equal_ignore in C.
    assert!(crate::equal(mk_var_at(mcx, 1, 2, 10), mk_var_at(mcx, 1, 2, 99)));
    assert!(!crate::equal(mk_var_at(mcx, 1, 2, 10), mk_var_at(mcx, 1, 3, 10)));
    let p1 = Node::mk_param_ref(mcx, 4, 5).unwrap();
    let p2 = Node::mk_param_ref(mcx, 4, 50).unwrap();
    assert!(crate::equal(p1, p2));
    let c1 = Node::mk_a_const(mcx, Some(crate::ValUnion::Integer(crate::Integer { ival: 3 })), 1).unwrap();
    let c2 = Node::mk_a_const(mcx, Some(crate::ValUnion::Integer(crate::Integer { ival: 3 })), 2).unwrap();
    assert!(crate::equal(c1, c2));
    let f = Node::mk_a_const(
        mcx,
        Some(crate::ValUnion::Float(crate::Float { fval: "3" })),
        1,
    )
    .unwrap();
    assert!(!crate::equal(c1, f), "A_Const value-union tag mismatch");
    assert!(!crate::equal(c1, Node::mk_a_const(mcx, None, 1).unwrap()));
}

#[test]
fn equal_const_datum_semantics() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let int4 = |v: i32| {
        Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(v), false, true).unwrap()
    };
    assert!(crate::equal(int4(7), int4(7)));
    assert!(!crate::equal(int4(7), int4(8)));
    // All NULLs of the same type are equal regardless of the value word.
    let null_a = Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(1), true, true).unwrap();
    let null_b = Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(2), true, true).unwrap();
    assert!(crate::equal(null_a, null_b));
    assert!(!crate::equal(null_a, int4(1)));
    // By-ref: byte-image compare through the pointer word (typlen -1 varlena
    // with a 1-byte header, and -2 cstring).
    let v1: &[u8] = &[7, b'h', b'i'];
    let v2: &[u8] = &[7, b'h', b'i'];
    let v3: &[u8] = &[7, b'h', b'o'];
    let vla = |img: &[u8]| {
        let d = datum::Datum::from_usize(img.as_ptr() as usize);
        Node::mk_const(mcx, 25, -1, 100, -1, d, false, false).unwrap()
    };
    assert!(crate::equal(vla(v1), vla(v2)));
    assert!(!crate::equal(vla(v1), vla(v3)));
    let s1: &[u8] = b"abc\0";
    let s2: &[u8] = b"abc\0";
    let s3: &[u8] = b"abd\0";
    let cstr = |img: &[u8]| {
        let d = datum::Datum::from_usize(img.as_ptr() as usize);
        Node::mk_const(mcx, 2275, -1, 0, -2, d, false, false).unwrap()
    };
    assert!(crate::equal(cstr(s1), cstr(s2)));
    assert!(!crate::equal(cstr(s1), cstr(s3)));
}

#[test]
fn equal_opexpr_zero_opfuncid_matches_any() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let op = |opfuncid: u32, location: i32| {
        let args = NodeList::make2(mcx, mk_var_at(mcx, 1, 1, 0), mk_var_at(mcx, 1, 2, 0)).unwrap();
        Node::mk(
            mcx,
            crate::OpExpr { opno: 96, opfuncid, opresulttype: 16, args, location, ..Default::default() },
        )
        .unwrap()
    };
    assert!(crate::equal(op(65, 1), op(65, 2)));
    assert!(crate::equal(op(0, 1), op(65, 1)));
    assert!(crate::equal(op(65, 1), op(0, 1)));
    assert!(!crate::equal(op(65, 1), op(66, 1)));
}

#[test]
fn equal_recurses_lists_and_ignores_coercionform() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let fx = |fmt: crate::CoercionForm, arg_attno: i16| {
        let args = NodeList::make1(mcx, mk_var_at(mcx, 1, arg_attno, 0)).unwrap();
        Node::mk(
            mcx,
            crate::FuncExpr { funcid: 481, funcresulttype: 20, funcformat: fmt, args, ..Default::default() },
        )
        .unwrap()
    };
    assert!(crate::equal(
        fx(crate::CoercionForm::COERCE_EXPLICIT_CALL, 1),
        fx(crate::CoercionForm::COERCE_IMPLICIT_CAST, 1),
    ));
    assert!(!crate::equal(
        fx(crate::CoercionForm::COERCE_EXPLICIT_CALL, 1),
        fx(crate::CoercionForm::COERCE_EXPLICIT_CALL, 2),
    ));
    let tle = |attno: i16, resno: i16| {
        Node::mk_target_entry(mcx, mk_var_at(mcx, 1, attno, 0), resno, Some("x"), false).unwrap()
    };
    let l1 = Node::mk_list(mcx, NodeList::make2(mcx, tle(1, 1), tle(2, 2)).unwrap()).unwrap();
    let l2 = Node::mk_list(mcx, NodeList::make2(mcx, tle(1, 1), tle(2, 2)).unwrap()).unwrap();
    let l3 = Node::mk_list(mcx, NodeList::make2(mcx, tle(1, 1), tle(3, 2)).unwrap()).unwrap();
    let l4 = Node::mk_list(mcx, NodeList::make1(mcx, tle(1, 1)).unwrap()).unwrap();
    assert!(crate::equal(l1, l2));
    assert!(!crate::equal(l1, l3));
    assert!(!crate::equal(l1, l4));
    assert!(!crate::equal(l1, mk_var_at(mcx, 1, 1, 0)), "tag mismatch is unequal");
    let il1 = Node::mk_int_list(mcx, IntList::make2(mcx, 1, 2).unwrap()).unwrap();
    let il2 = Node::mk_int_list(mcx, IntList::make2(mcx, 1, 2).unwrap()).unwrap();
    let il3 = Node::mk_int_list(mcx, IntList::make2(mcx, 1, 3).unwrap()).unwrap();
    assert!(crate::equal(il1, il2));
    assert!(!crate::equal(il1, il3));
}

#[test]
fn equal_aggref_ignores_transtype_and_presorted() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let agg = |aggtranstype: u32, aggpresorted: bool, aggno: i32| {
        let args = NodeList::make1(mcx, mk_var_at(mcx, 1, 1, 0)).unwrap();
        Node::mk(
            mcx,
            crate::primnodes::Aggref {
                aggfnoid: 2108,
                aggtype: 20,
                aggargtypes: OidList::make1(mcx, 23).unwrap(),
                args,
                aggtranstype,
                aggpresorted,
                aggno,
                ..Default::default()
            },
        )
        .unwrap()
    };
    assert!(crate::equal(agg(20, false, -1), agg(2281, true, -1)));
    // aggno/aggtransno ARE compared (not equal_ignore in C).
    assert!(!crate::equal(agg(20, false, 0), agg(20, false, 1)));
}

#[test]
fn bms_add_range() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let mut b = Bitmapset::empty();
    b.add_range(mcx, 5, 2).unwrap();
    assert!(b.is_empty());
    b.add_range(mcx, -5, -10).unwrap();
    assert!(b.is_empty());

    b.add_range(mcx, 3, 3).unwrap();
    assert_eq!(b.iter().collect::<Vec<_>>(), [3]);

    let mut b = Bitmapset::empty();
    b.add_range(mcx, 0, 63).unwrap();
    check_invariants(&b);
    assert_eq!(b.nwords(), 1);
    assert_eq!(b.num_members(), 64);

    let mut b = Bitmapset::empty();
    b.add_range(mcx, 63, 65).unwrap();
    check_invariants(&b);
    assert_eq!(b.iter().collect::<Vec<_>>(), [63, 64, 65]);

    let mut b = Bitmapset::empty();
    b.add_range(mcx, 10, 200).unwrap();
    check_invariants(&b);
    assert_eq!(b.num_members(), 191);
    assert!(!b.is_member(9));
    assert!(b.is_member(10));
    assert!(b.is_member(200));
    assert!(!b.is_member(201));

    let mut b = Bitmapset::make_singleton(mcx, 300).unwrap();
    b.add_range(mcx, 64, 127).unwrap();
    check_invariants(&b);
    assert!(b.is_member(300));
    assert_eq!(b.num_members(), 65);

    let mut rng = XorShift(0xC0FFEE1234567891);
    for _ in 0..100 {
        let mut rset: BTreeSet<i32> = BTreeSet::new();
        for _ in 0..(rng.next() % 16) {
            rset.insert((rng.next() % 300) as i32);
        }
        let mut b = from_set(mcx, &rset);
        let lower = (rng.next() % 300) as i32;
        let upper = lower + (rng.next() % 150) as i32 - 20;
        b.add_range(mcx, lower, upper).unwrap();
        for x in lower..=upper {
            rset.insert(x);
        }
        check_invariants(&b);
        assert_eq!(b.iter().collect::<Vec<_>>(), rset.iter().copied().collect::<Vec<_>>());
    }
}

#[test]
#[should_panic(expected = "negative bitmapset member")]
fn bms_add_range_negative_lower_is_loud() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mut b = Bitmapset::empty();
    let _ = b.add_range(mcx, -1, 5);
}

#[test]
fn bms_join() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let a = from_set(mcx, &BTreeSet::from([1, 70]));
    let b = Bitmapset::empty();
    let j = Bitmapset::join(a, b);
    assert_eq!(j.iter().collect::<Vec<_>>(), [1, 70]);

    let a = Bitmapset::empty();
    let b = from_set(mcx, &BTreeSet::from([2]));
    let j = Bitmapset::join(a, b);
    assert_eq!(j.iter().collect::<Vec<_>>(), [2]);

    for swap in [false, true] {
        let shorter = from_set(mcx, &BTreeSet::from([0, 5]));
        let longer = from_set(mcx, &BTreeSet::from([3, 130]));
        let j = if swap {
            Bitmapset::join(longer, shorter)
        } else {
            Bitmapset::join(shorter, longer)
        };
        check_invariants(&j);
        assert_eq!(j.iter().collect::<Vec<_>>(), [0, 3, 5, 130]);
    }
}

#[test]
fn bms_replace_members() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let mut a = from_set(mcx, &BTreeSet::from([1, 200]));
    a.replace_members(mcx, &Bitmapset::empty()).unwrap();
    assert!(a.is_empty());

    let b = from_set(mcx, &BTreeSet::from([7, 90]));
    a.replace_members(mcx, &b).unwrap();
    check_invariants(&a);
    assert_eq!(a.iter().collect::<Vec<_>>(), [7, 90]);

    let shrunk = from_set(mcx, &BTreeSet::from([4]));
    a.replace_members(mcx, &shrunk).unwrap();
    check_invariants(&a);
    assert_eq!(a.iter().collect::<Vec<_>>(), [4]);

    let grown = from_set(mcx, &BTreeSet::from([9, 400]));
    a.replace_members(mcx, &grown).unwrap();
    check_invariants(&a);
    assert_eq!(a.iter().collect::<Vec<_>>(), [9, 400]);
}

#[test]
fn bms_member_index() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let b = from_set(mcx, &BTreeSet::from([0, 5, 63, 64, 128, 300]));
    for (i, x) in b.iter().enumerate() {
        assert_eq!(b.member_index(x), i as i32, "member_index({x})");
    }
    assert_eq!(b.member_index(1), -1);
    assert_eq!(b.member_index(299), -1);
    assert_eq!(b.member_index(1000), -1);
    assert_eq!(Bitmapset::empty().member_index(0), -1);
}

#[test]
fn bms_overlap_list() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();

    let b = from_set(mcx, &BTreeSet::from([3, 130]));
    assert!(b.overlap_list(&[99, 130]));
    assert!(b.overlap_list(&[3]));
    assert!(!b.overlap_list(&[4, 129, 131, 500]));
    assert!(!b.overlap_list(&[]));
    assert!(!Bitmapset::empty().overlap_list(&[-1, 2]));
}

#[test]
#[should_panic(expected = "negative bitmapset member")]
fn bms_overlap_list_negative_is_loud() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let b = from_set(mcx, &BTreeSet::from([1]));
    b.overlap_list(&[-3]);
}

#[test]
fn tsearch_lane_stmt_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    assert_eq!(
        c_struct_fields(parse_h, "DefineStmt"),
        ["kind", "oldstyle", "defnames", "args", "definition", "if_not_exists", "replace"]
    );
    let crate::parsenodes::DefineStmt {
        kind: _, oldstyle: _, defnames: _, args: _, definition: _, if_not_exists: _, replace: _,
    } = crate::parsenodes::DefineStmt::default();

    assert_eq!(c_struct_fields(parse_h, "CompositeTypeStmt"), ["typevar", "coldeflist"]);
    let crate::rawnodes::CompositeTypeStmt { typevar: _, coldeflist: _ } =
        crate::rawnodes::CompositeTypeStmt::default();

    assert_eq!(c_struct_fields(parse_h, "AlterTSDictionaryStmt"), ["dictname", "options"]);
    let crate::rawnodes::AlterTSDictionaryStmt { dictname: _, options: _ } =
        crate::rawnodes::AlterTSDictionaryStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "AlterTSConfigurationStmt"),
        ["kind", "cfgname", "tokentype", "dicts", "override", "replace", "missing_ok"]
    );
    let crate::rawnodes::AlterTSConfigurationStmt {
        kind: _, cfgname: _, tokentype: _, dicts: _, r#override: _, replace: _, missing_ok: _,
    } = crate::rawnodes::AlterTSConfigurationStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "A_ArrayExpr"),
        ["elements", "list_start", "list_end", "location"]
    );
    let crate::rawnodes::A_ArrayExpr { elements: _, list_start: _, list_end: _, location: _ } =
        crate::rawnodes::A_ArrayExpr::default();

    assert_eq!(crate::rawnodes::AlterTSConfigType::ALTER_TSCONFIG_DROP_MAPPING as u32, 4);
}

#[test]
fn definestmt_tail_field_order_matches_c() {
    let parse_h = include_str!("../vendor/parsenodes.h");
    assert_eq!(c_struct_fields(parse_h, "CreateAmStmt"), ["amname", "handler_name", "amtype"]);
    let crate::parsenodes::CreateAmStmt { amname: _, handler_name: _, amtype: _ } =
        crate::parsenodes::CreateAmStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "CreateCastStmt"),
        ["sourcetype", "targettype", "func", "context", "inout"]
    );
    let crate::parsenodes::CreateCastStmt {
        sourcetype: _, targettype: _, func: _, context: _, inout: _,
    } = crate::parsenodes::CreateCastStmt::default();

    assert_eq!(
        c_struct_fields(parse_h, "CreateTransformStmt"),
        ["replace", "type_name", "lang", "fromsql", "tosql"]
    );
    let crate::parsenodes::CreateTransformStmt {
        replace: _, type_name: _, lang: _, fromsql: _, tosql: _,
    } = crate::parsenodes::CreateTransformStmt::default();
}

#[test]
fn equal_coerce_to_domain_matches_c_field_rules() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mk = |typmod: i32, form: crate::primnodes::CoercionForm, location: i32| {
        Node::mk(
            mcx,
            crate::primnodes::CoerceToDomain {
                arg: mk_var_at(mcx, 1, 2, 0),
                resulttype: 23,
                resulttypmod: typmod,
                resultcollid: 0,
                coercionformat: form,
                location,
            },
        )
        .unwrap()
    };
    use crate::primnodes::CoercionForm::{COERCE_EXPLICIT_CAST, COERCE_IMPLICIT_CAST};
    // CoercionForm and location are never compared (C COMPARE_COERCIONFORM_FIELD
    // / COMPARE_LOCATION_FIELD).
    assert!(crate::equal(
        mk(-1, COERCE_EXPLICIT_CAST, 5),
        mk(-1, COERCE_IMPLICIT_CAST, 99)
    ));
    assert!(!crate::equal(
        mk(-1, COERCE_EXPLICIT_CAST, 5),
        mk(7, COERCE_EXPLICIT_CAST, 5)
    ));

    let mkv = |collation: u32, location: i32| {
        Node::mk(
            mcx,
            crate::primnodes::CoerceToDomainValue {
                typeId: 23,
                typeMod: -1,
                collation,
                location,
            },
        )
        .unwrap()
    };
    assert!(crate::equal(mkv(0, 1), mkv(0, 2)));
    assert!(!crate::equal(mkv(0, 1), mkv(100, 1)));
}
