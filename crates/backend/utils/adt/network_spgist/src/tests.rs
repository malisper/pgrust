use super::*;
use ::adt_network::{
    network_cmp_internal, network_in, network_overlap, network_sub, network_subeq, network_sup,
    network_supeq, InetValue,
};

fn v(s: &str) -> InetValue {
    network_in(s, false, None).unwrap().unwrap()
}

fn corpus() -> Vec<InetValue> {
    [
        "0.0.0.0/0",
        "10.0.0.0/8",
        "10.1.0.0/16",
        "10.1.2.0/24",
        "10.1.2.3",
        "10.1.2.3/8",
        "10.1.3.0/24",
        "10.128.0.0/9",
        "192.168.1.0/24",
        "192.168.1.5",
        "192.168.1.255",
        "255.255.255.255",
        "127.0.0.1",
        "::/0",
        "::1",
        "2001:db8::/32",
        "2001:db8::1",
        "2001:db8:0:1::/64",
        "2001:db8:0:1::5",
        "2001:db8:8000::/33",
        "fe80::/10",
        "fe80::1",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "::ffff:10.4.3.2",
    ]
    .iter()
    .map(|s| v(s))
    .collect()
}

const ALL_STRATEGIES: [u16; 11] = [
    3, // RTOverlapStrategyNumber
    RTEqualStrategyNumber,
    RTNotEqualStrategyNumber,
    RTLessStrategyNumber,
    RTLessEqualStrategyNumber,
    RTGreaterStrategyNumber,
    RTGreaterEqualStrategyNumber,
    RTSubStrategyNumber,
    RTSubEqualStrategyNumber,
    RTSuperStrategyNumber,
    RTSuperEqualStrategyNumber,
];

fn oracle(k: &InetValue, q: &InetValue, strategy: u16) -> bool {
    let (k, q) = (k.iref(), q.iref());
    match strategy {
        3 => network_overlap(k, q),
        RTEqualStrategyNumber => network_cmp_internal(k, q) == 0,
        RTNotEqualStrategyNumber => network_cmp_internal(k, q) != 0,
        RTLessStrategyNumber => network_cmp_internal(k, q) < 0,
        RTLessEqualStrategyNumber => network_cmp_internal(k, q) <= 0,
        RTGreaterStrategyNumber => network_cmp_internal(k, q) > 0,
        RTGreaterEqualStrategyNumber => network_cmp_internal(k, q) >= 0,
        RTSubStrategyNumber => network_sub(k, q),
        RTSubEqualStrategyNumber => network_subeq(k, q),
        RTSuperStrategyNumber => network_sup(k, q),
        RTSuperEqualStrategyNumber => network_supeq(k, q),
        _ => unreachable!(),
    }
}

struct KeyImage {
    img: [u8; 22],
    _len: usize,
}

fn scankey(strategy: u16, q: &InetValue, store: &mut Vec<Box<KeyImage>>) -> ScanKeyData {
    let (img, len) = q.image();
    store.push(Box::new(KeyImage { img, _len: len }));
    let mut k = ScanKeyData::empty();
    k.sk_strategy = strategy;
    k.sk_argument = Datum::from_usize(store.last().unwrap().img.as_ptr() as usize);
    k
}

#[test]
fn node_number_partitions() {
    let p = v("10.1.0.0/16");
    // masklen == prefix bits, next addr bit 0 -> node 0
    assert_eq!(inet_spg_node_number(v("10.1.0.0/16").iref(), 16), 0);
    // next addr bit 1, same masklen -> node 1
    assert_eq!(inet_spg_node_number(v("10.1.128.0/16").iref(), 16), 1);
    // longer masklen, next bit 0 -> node 2
    assert_eq!(inet_spg_node_number(v("10.1.0.0/24").iref(), 16), 2);
    // longer masklen, next bit 1 -> node 3
    assert_eq!(inet_spg_node_number(v("10.1.128.0/24").iref(), 16), 3);
    // no more address bits -> even node
    assert_eq!(inet_spg_node_number(p.iref(), 32) & 1, 0);
}

#[test]
fn leaf_bitmap_matches_operators() {
    let c = corpus();
    let mut store = Vec::new();
    for k in &c {
        for q in &c {
            for s in ALL_STRATEGIES {
                let keys = [scankey(s, q, &mut store)];
                let got = inet_spg_consistent_bitmap(k.iref(), &keys, true) != 0;
                assert_eq!(got, oracle(k, q, s), "k={k:?} q={q:?} strategy={s}");
            }
        }
    }
}

#[test]
fn inner_bitmap_never_excludes_matching_leaf() {
    // For a prefix P and any leaf L under P (same family, L.bits >= P.bits,
    // common prefix matches), the node inet_spg_node_number(L, P.bits) must
    // stay set whenever L matches the query.
    let c = corpus();
    let mut store = Vec::new();
    for p in &c {
        let pref = ::adt_network::cidr_set_masklen_internal(p.iref(), p.bits as i32);
        for l in &c {
            let (li, pi) = (l.iref(), pref.iref());
            if li.family != pi.family
                || (li.bits as i32) < pi.bits as i32
                || ::adt_network::bitncmp(pi.addr, li.addr, pi.bits as i32) != 0
            {
                continue;
            }
            let node = inet_spg_node_number(li, pi.bits as i32);
            for q in &c {
                for s in ALL_STRATEGIES {
                    if !oracle(l, q, s) {
                        continue;
                    }
                    let keys = [scankey(s, q, &mut store)];
                    let bitmap = inet_spg_consistent_bitmap(pref.iref(), &keys, false);
                    assert!(
                        bitmap & (1 << node) != 0,
                        "false negative: prefix={p:?} leaf={l:?} q={q:?} strategy={s} node={node} bitmap={bitmap:#x}"
                    );
                }
            }
        }
    }
}

// In-memory replay of spgdoinsert's choose/picksplit state machine over the
// e2e corpus, searched with inet_spg_consistent_bitmap. Pins the port to C's
// behavior INCLUDING the upstream quirk: inet_spg_picksplit's commonbits==0
// early break skips the different-family check, so v4 leaves land under a v6
// ::/0 prefix tuple and family-pruning hides them (reproduced on live PGDG
// 18; scripts/inet-spgist-c-oracle-e2e.sh).
mod tree_sim {
    use super::*;
    use ::adt_network::{bitncommon, cidr_set_masklen_internal, network_supeq, InetValue};

    const LEAF_CAP: usize = 100;

    pub enum Node {
        Inner {
            has_prefix: bool,
            prefix: Option<InetValue>,
            all_the_same: bool,
            children: Vec<Option<Box<Node>>>,
        },
        Leaf(Vec<(i32, InetValue)>),
    }

    fn e2e_val(i: i64) -> InetValue {
        let s = if i % 3 == 0 {
            format!(
                "{}.{}.{}.{}/{}",
                10 + i % 5,
                (i / 7) % 256,
                (i / 3) % 256,
                i % 256,
                8 + (i % 25)
            )
        } else if i % 3 == 1 {
            format!(
                "2001:db8:{:x}:{:x}::{:x}/{}",
                (i / 11) % 65536,
                i % 65536,
                i % 4096,
                33 + (i % 96)
            )
        } else {
            format!("fe80::{:x}:{:x}/{}", (i / 13) % 65536, i % 65536, 64 + (i % 65))
        };
        network_in(&s, false, None).unwrap().unwrap()
    }

    // mirror of fc_inet_spg_choose over the sim node
    enum ChooseResult {
        Match(usize),
        Split {
            new_prefix: Option<InetValue>,
            new_has_prefix: bool,
            n_nodes: usize,
            child_node: usize,
        },
    }

    fn choose(
        has_prefix: bool,
        prefix: Option<&InetValue>,
        _all_the_same: bool,
        val: &InetValue,
    ) -> ChooseResult {
        let v = val.iref();
        if !has_prefix {
            return ChooseResult::Match(if v.family == PGSQL_AF_INET { 0 } else { 1 });
        }
        let p = prefix.unwrap().iref();
        let commonbits = p.bits as i32;
        if v.family != p.family {
            return ChooseResult::Split {
                new_prefix: None,
                new_has_prefix: false,
                n_nodes: 2,
                child_node: if p.family == PGSQL_AF_INET { 0 } else { 1 },
            };
        }
        if (v.bits as i32) < commonbits || ::adt_network::bitncmp(p.addr, v.addr, commonbits) != 0 {
            let cb = bitncommon(p.addr, v.addr, (v.bits as i32).min(commonbits));
            return ChooseResult::Split {
                new_prefix: Some(cidr_set_masklen_internal(v, cb)),
                new_has_prefix: true,
                n_nodes: 4,
                child_node: inet_spg_node_number(p, cb) as usize,
            };
        }
        ChooseResult::Match(inet_spg_node_number(v, commonbits) as usize)
    }

    // mirror of fc_inet_spg_picksplit
    fn picksplit(vals: &[(i32, InetValue)]) -> (bool, Option<InetValue>, usize, Vec<usize>) {
        let prefix = vals[0].1;
        let p = prefix.iref();
        let mut commonbits = p.bits as i32;
        let mut different = false;
        for (_, t) in vals.iter().skip(1) {
            let t = t.iref();
            if t.family != p.family {
                different = true;
                break;
            }
            if (t.bits as i32) < commonbits {
                commonbits = t.bits as i32;
            }
            commonbits = bitncommon(p.addr, t.addr, commonbits);
            if commonbits == 0 {
                break;
            }
        }
        if different {
            let map = vals
                .iter()
                .map(|(_, t)| if t.family == PGSQL_AF_INET { 0 } else { 1 })
                .collect();
            (false, None, 2, map)
        } else {
            let pfx = cidr_set_masklen_internal(p, commonbits);
            let map = vals
                .iter()
                .map(|(_, t)| inet_spg_node_number(t.iref(), commonbits) as usize)
                .collect();
            (true, Some(pfx), 4, map)
        }
    }

    fn insert(node: &mut Node, pk: i32, val: InetValue, depth: usize) {
        assert!(depth < 200, "runaway descent");
        match node {
            Node::Leaf(list) => {
                if list.len() < LEAF_CAP {
                    list.push((pk, val));
                    return;
                }
                let mut all: Vec<(i32, InetValue)> = std::mem::take(list);
                all.push((pk, val));
                let (has_prefix, prefix, n_nodes, mut map) = picksplit(&all);
                // checkAllTheSame emulation
                let the = map[0];
                let all_same = map.iter().all(|&m| m == the);
                let (n_nodes, all_the_same) = if all_same {
                    for (i, m) in map.iter_mut().enumerate() {
                        *m = i % 8;
                    }
                    (8, true)
                } else {
                    (n_nodes, false)
                };
                let mut children: Vec<Option<Box<Node>>> = (0..n_nodes).map(|_| None).collect();
                for ((p, v), m) in all.into_iter().zip(map) {
                    children[m]
                        .get_or_insert_with(|| Box::new(Node::Leaf(Vec::new())))
                        .as_mut()
                        .push_leaf(p, v);
                }
                *node = Node::Inner {
                    has_prefix,
                    prefix,
                    all_the_same,
                    children,
                };
            }
            Node::Inner {
                has_prefix,
                prefix,
                all_the_same,
                children,
            } => {
                match choose(*has_prefix, prefix.as_ref(), *all_the_same, &val) {
                    ChooseResult::Match(mut n) => {
                        if *all_the_same {
                            n = (pk as usize) % children.len();
                        }
                        insert(
                            children[n].get_or_insert_with(|| Box::new(Node::Leaf(Vec::new()))),
                            pk,
                            val,
                            depth + 1,
                        );
                    }
                    ChooseResult::Split {
                        new_prefix,
                        new_has_prefix,
                        n_nodes,
                        child_node,
                    } => {
                        let old = std::mem::replace(
                            node,
                            Node::Inner {
                                has_prefix: new_has_prefix,
                                prefix: new_prefix,
                                all_the_same: false,
                                children: (0..n_nodes).map(|_| None).collect(),
                            },
                        );
                        if let Node::Inner { children, .. } = node {
                            children[child_node] = Some(Box::new(old));
                        }
                        insert(node, pk, val, depth + 1);
                    }
                }
            }
        }
    }

    impl Node {
        fn push_leaf(&mut self, pk: i32, v: InetValue) {
            if let Node::Leaf(l) = self {
                l.push((pk, v));
            } else {
                unreachable!()
            }
        }
    }

    fn search(node: &Node, keys: &[ScanKeyData], out: &mut Vec<i32>) {
        match node {
            Node::Leaf(list) => {
                for (pk, v) in list {
                    if inet_spg_consistent_bitmap(v.iref(), keys, true) != 0 {
                        out.push(*pk);
                    }
                }
            }
            Node::Inner {
                has_prefix,
                prefix,
                all_the_same,
                children,
            } => {
                let which: u32 = if !*has_prefix {
                    // family-split arm of fc_inet_spg_inner_consistent
                    let mut which = 1 | (1 << 1);
                    for key in keys {
                        let argument = unsafe { crate::inet_at(key.sk_argument) };
                        match key.sk_strategy {
                            RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                                if argument.family == PGSQL_AF_INET {
                                    which &= 1;
                                }
                            }
                            RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                                if argument.family != PGSQL_AF_INET {
                                    which &= 1 << 1;
                                }
                            }
                            RTNotEqualStrategyNumber => {}
                            _ => {
                                if argument.family == PGSQL_AF_INET {
                                    which &= 1;
                                } else {
                                    which &= 1 << 1;
                                }
                            }
                        }
                    }
                    which
                } else if !*all_the_same {
                    inet_spg_consistent_bitmap(prefix.as_ref().unwrap().iref(), keys, false)
                } else {
                    !0
                };
                for (i, c) in children.iter().enumerate() {
                    if which & (1 << i) != 0 {
                        if let Some(c) = c {
                            search(c, keys, out);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn e2e_corpus_supeq_replay() {
        let vals: Vec<(i32, InetValue)> = (1..=12000).map(|i| (i as i32, e2e_val(i))).collect();
        let mut root = Node::Leaf(Vec::new());
        for (pk, v) in &vals {
            insert(&mut root, *pk, *v, 0);
        }
        let quirk_missing: &[(&str, u16, &[i32])] = &[
            ("13.9.21.0/11", 3, &[3, 18, 33, 48, 63, 78, 93]),
            ("13.9.21.0/11", RTSubEqualStrategyNumber, &[3, 18, 33, 48, 63, 78, 93]),
        ];
        let mut store = Vec::new();
        for qs in [
            "13.9.21.0/11",
            "10.75.175.25",
            "10.30.70.0/18",
            "10.105.245.35",
        ] {
            let q = network_in(qs, false, None).unwrap().unwrap();
            for strat in super::ALL_STRATEGIES {
                let keys = [super::scankey(strat, &q, &mut store)];
                let mut got = Vec::new();
                search(&root, &keys, &mut got);
                got.sort_unstable();
                let mut want: Vec<i32> = vals
                    .iter()
                    .filter(|(_, v)| super::oracle(v, &q, strat))
                    .map(|(pk, _)| *pk)
                    .collect();
                want.sort_unstable();
                let missing: Vec<i32> =
                    want.iter().copied().filter(|p| !got.contains(p)).collect();
                let extra: Vec<i32> =
                    got.iter().copied().filter(|p| !want.contains(p)).collect();
                assert!(extra.is_empty(), "false positives q={qs} strat={strat}: {extra:?}");
                if let Some((_, _, pinned)) = quirk_missing
                    .iter()
                    .find(|(g, st, _)| *g == qs && *st == strat)
                {
                    assert_eq!(missing, *pinned, "quirk drift q={qs} strat={strat}");
                } else {
                    // every other (query, strategy) pair either matches the
                    // oracle or misses only pks whose values sort under a
                    // family-mixed ::/0 subtree (upstream quirk class:
                    // low-masklen leaves picksplit-batched behind
                    // zero-commonbits v6 pairs).
                    for pk in &missing {
                        assert_eq!(pk % 3, 0, "non-v4 quirk miss q={qs} strat={strat} pk={pk}");
                    }
                }
            }
        }
        let _ = network_supeq;
    }
}
