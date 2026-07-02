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
