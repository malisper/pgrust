use super::*;
use core::cell::RefCell;
use mcx::MemoryContext;
use types_fmgr::FmgrInfo;
use types_nodes::Node;

fn static_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("partbounds test"))).mcx()
}

fn test_key(strategy: u8) -> PartitionKeyData {
    let mcx = static_mcx();
    let mut partattrs = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    partattrs.push(1i16);
    let one_oid = |v: u32| {
        let mut x = mcx::vec_with_capacity_in(mcx, 1).unwrap();
        x.push(v);
        x
    };
    let mut parttypmod = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    parttypmod.push(-1i32);
    let mut parttyplen = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    parttyplen.push(4i16);
    let mut parttypbyval = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    parttypbyval.push(true);
    let mut parttypalign = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    parttypalign.push(b'i' as i8);
    PartitionKeyData {
        strategy: strategy as i8,
        partnatts: 1,
        partattrs,
        partexprs: types_nodes::NodeList::nil(),
        partopfamily: one_oid(0),
        partopcintype: one_oid(23),
        partsupfunc: vec![RefCell::new(FmgrInfo::unresolved())],
        partcollation: one_oid(0),
        parttypid: one_oid(23),
        parttypmod,
        parttyplen,
        parttypbyval,
        parttypalign,
        parttypcoll: one_oid(0),
    }
}

fn hash_spec<'m>(mcx: Mcx<'m>, modulus: i32, remainder: i32) -> &'m PartitionBoundSpec<'m> {
    let mut b = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    b.strategy = PARTITION_STRATEGY_HASH;
    b.modulus = modulus;
    b.remainder = remainder;
    b.seal_ref()
}

fn int_const<'m>(mcx: Mcx<'m>, v: Option<i32>) -> Node<'m> {
    Node::mk(
        mcx,
        Const {
            consttype: 23,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: v.map_or(Datum::null(), Datum::from_i32),
            constisnull: v.is_none(),
            constbyval: true,
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn hbound_cmp_orders_by_modulus_then_remainder() {
    assert_eq!(partition_hbound_cmp(2, 1, 4, 0), -1);
    assert_eq!(partition_hbound_cmp(4, 0, 2, 1), 1);
    assert_eq!(partition_hbound_cmp(4, 1, 4, 3), -1);
    assert_eq!(partition_hbound_cmp(4, 3, 4, 1), 1);
    assert_eq!(partition_hbound_cmp(4, 2, 4, 2), 0);
}

#[test]
fn hash_combine64_matches_c() {
    // a ^ (b + 0x49a0f4dd15e5a8e3 + (a<<54) + (a>>7)) with wrapping arithmetic.
    assert_eq!(hash_combine64(0, 0), 0x49a0f4dd15e5a8e3);
    let a = 0x123456789abcdef0u64;
    let b = 0x0fedcba987654321u64;
    let expected = a ^ (b
        .wrapping_add(0x49a0f4dd15e5a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7));
    assert_eq!(hash_combine64(a, b), expected);
}

#[test]
fn create_hash_bounds_sorts_and_maps() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_HASH);
    let specs = [
        hash_spec(mcx, 8, 3),
        hash_spec(mcx, 2, 0),
        hash_spec(mcx, 8, 7),
        hash_spec(mcx, 4, 1),
    ];
    let (info, mapping) = partition_bounds_create(mcx, &specs, &key).unwrap();
    assert_eq!(info.ndatums, 4);
    assert_eq!(info.width, 2);
    let pairs: Vec<(i32, i32)> =
        (0..4).map(|i| (info.datum(i, 0).as_i32(), info.datum(i, 1).as_i32())).collect();
    assert_eq!(pairs, vec![(2, 0), (4, 1), (8, 3), (8, 7)]);
    assert_eq!(&info.indexes[..], &[0, 1, 0, 2, 0, 1, 0, 3]);
    assert_eq!(mapping, vec![2, 0, 3, 1]);
    assert_eq!(get_hash_partition_greatest_modulus(&info), 8);
    assert_eq!(info.default_index, -1);
    assert_eq!(info.null_index, -1);
}

#[test]
fn hash_bsearch_finds_greatest_le_pair() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_HASH);
    let specs = [hash_spec(mcx, 4, 0), hash_spec(mcx, 8, 2)];
    let (info, _) = partition_bounds_create(mcx, &specs, &key).unwrap();
    assert_eq!(partition_hash_bsearch(&info, 2, 0), -1);
    assert_eq!(partition_hash_bsearch(&info, 4, 0), 0);
    assert_eq!(partition_hash_bsearch(&info, 8, 1), 0);
    assert_eq!(partition_hash_bsearch(&info, 8, 2), 1);
    assert_eq!(partition_hash_bsearch(&info, 16, 0), 1);
}

#[test]
fn check_new_hash_partition_no_conflict() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_HASH);
    let specs = [hash_spec(mcx, 4, 0), hash_spec(mcx, 8, 2)];
    let (info, _) = partition_bounds_create(mcx, &specs, &key).unwrap();
    let new_spec = hash_spec(mcx, 8, 1);
    check_new_partition_bound(mcx, "p_new", &key, Some(&info), &[100, 101], new_spec, None).unwrap();
}

#[test]
fn check_default_against_empty_parent_ok() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_LIST);
    let mut b = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    b.strategy = PARTITION_STRATEGY_LIST;
    b.is_default = true;
    let spec = b.seal_ref();
    check_new_partition_bound(mcx, "p_def", &key, None, &[], spec, None).unwrap();
}

#[test]
fn create_list_bounds_assigns_default_last() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_LIST);

    let mut def = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    def.strategy = PARTITION_STRATEGY_LIST;
    def.is_default = true;
    let def = def.seal_ref();

    let mut plain = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    plain.strategy = PARTITION_STRATEGY_LIST;
    plain.listdatums.lappend(mcx, int_const(mcx, Some(42))).unwrap();
    plain.listdatums.lappend(mcx, int_const(mcx, None)).unwrap();
    let plain = plain.seal_ref();

    let (info, mapping) = partition_bounds_create(mcx, &[def, plain], &key).unwrap();
    assert_eq!(info.ndatums, 1);
    assert_eq!(&info.indexes[..], &[0]);
    assert_eq!(info.null_index, 0);
    assert_eq!(info.default_index, 1);
    assert_eq!(mapping, vec![1, 0]);
}

#[test]
fn create_range_bounds_assigns_default_last() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_RANGE);

    let mut plain = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    plain.strategy = PARTITION_STRATEGY_RANGE;
    let mut lo = Node::build::<PartitionRangeDatum>(mcx).unwrap();
    lo.kind = PartitionRangeDatumKind::Minvalue;
    plain.lowerdatums.lappend(mcx, lo.seal()).unwrap();
    let mut hi = Node::build::<PartitionRangeDatum>(mcx).unwrap();
    hi.kind = PartitionRangeDatumKind::Maxvalue;
    plain.upperdatums.lappend(mcx, hi.seal()).unwrap();
    let plain = plain.seal_ref();

    let mut def = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    def.strategy = PARTITION_STRATEGY_RANGE;
    def.is_default = true;
    let def = def.seal_ref();

    let (info, mapping) = partition_bounds_create(mcx, &[plain, def], &key).unwrap();
    assert_eq!(info.ndatums, 2);
    assert_eq!(&info.indexes[..], &[-1, 0, -1]);
    assert_eq!(info.default_index, 1);
    assert_eq!(mapping, vec![0, 1]);
}
