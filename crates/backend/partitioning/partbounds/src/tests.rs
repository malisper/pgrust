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

// partbounds.c:4315-4317 probes RELOID for each partition of the default
// partition's parent and reports a miss with
// elog(ERROR, "cache lookup failed for relation %u", inhrelid) -- catchable,
// SQLSTATE XX000, transaction-scoped.  pgrust's read_boundspec_opt panicked
// instead, taking the backend down.
#[test]
fn boundspec_cache_lookup_failure_is_a_catchable_xx000() {
    let e = crate::qual::cache_lookup_failed(16384);
    assert_eq!(e.message(), "cache lookup failed for relation 16384");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.level(), types_error::ERROR);
}

// ---- audit-18.6 remediation b142 witnesses ------------------------------

fn list_spec<'m>(mcx: Mcx<'m>, vals: &[Option<i32>]) -> &'m PartitionBoundSpec<'m> {
    let mut b = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    b.strategy = PARTITION_STRATEGY_LIST;
    for v in vals {
        b.listdatums.lappend(mcx, int_const(mcx, *v)).unwrap();
    }
    b.seal_ref()
}

fn range_datum<'m>(mcx: Mcx<'m>, v: Option<i32>) -> Node<'m> {
    let mut d = Node::build::<PartitionRangeDatum>(mcx).unwrap();
    d.kind = PartitionRangeDatumKind::Value;
    d.value = Some(int_const(mcx, v));
    d.seal()
}

fn range_spec<'m>(mcx: Mcx<'m>, lo: Option<i32>, hi: Option<i32>) -> &'m PartitionBoundSpec<'m> {
    let mut b = Node::build::<PartitionBoundSpec>(mcx).unwrap();
    b.strategy = PARTITION_STRATEGY_RANGE;
    b.lowerdatums.lappend(mcx, range_datum(mcx, lo)).unwrap();
    b.upperdatums.lappend(mcx, range_datum(mcx, hi)).unwrap();
    b.seal_ref()
}

fn assert_internal_error(e: &PgError, message: &str) {
    assert_eq!(e.message(), message);
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.level(), types_error::ERROR);
}

// partbounds.c:372 (create_hash_bounds), :493 (create_list_bounds) and :713
// (create_range_bounds): a bound spec whose strategy disagrees with the
// partition key's is elog(ERROR, "invalid strategy in partition bound spec")
// -- a catchable XX000 (reachable through pg_class.relpartbound with
// allow_system_table_mods), not an assertion panic.
#[test]
fn bound_spec_with_wrong_strategy_is_a_catchable_xx000() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    // HASH key, LIST spec.
    let key = test_key(PARTITION_STRATEGY_HASH);
    let e = partition_bounds_create(mcx, &[list_spec(mcx, &[Some(1)])], &key)
        .err()
        .expect("hash key with a list spec must error");
    assert_internal_error(&e, "invalid strategy in partition bound spec");
    // LIST key, HASH spec.
    let key = test_key(PARTITION_STRATEGY_LIST);
    let e = partition_bounds_create(mcx, &[hash_spec(mcx, 2, 0)], &key)
        .err()
        .expect("list key with a hash spec must error");
    assert_internal_error(&e, "invalid strategy in partition bound spec");
    // RANGE key, HASH spec.
    let key = test_key(PARTITION_STRATEGY_RANGE);
    let e = partition_bounds_create(mcx, &[hash_spec(mcx, 2, 0)], &key)
        .err()
        .expect("range key with a hash spec must error");
    assert_internal_error(&e, "invalid strategy in partition bound spec");
}

// partbounds.c:523: two list partitions both claiming NULL is
// elog(ERROR, "found null more than once") -- catchable XX000.
#[test]
fn duplicate_null_list_bound_is_a_catchable_xx000() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_LIST);
    let specs = [list_spec(mcx, &[None, Some(1)]), list_spec(mcx, &[None])];
    let e = partition_bounds_create(mcx, &specs, &key)
        .err()
        .expect("two NULL-accepting list partitions must error");
    assert_internal_error(&e, "found null more than once");
}

// partbounds.c:3456 (make_one_partition_rbound): a NULL Const in a range
// bound is elog(ERROR, "invalid range bound datum") -- catchable XX000.
#[test]
fn null_range_bound_datum_is_a_catchable_xx000() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = test_key(PARTITION_STRATEGY_RANGE);
    let e = partition_bounds_create(mcx, &[range_spec(mcx, None, Some(10))], &key)
        .err()
        .expect("a NULL range bound datum must error");
    assert_internal_error(&e, "invalid range bound datum");
}

// A btree support function that raises, as a user-defined opclass's
// comparison function can (ereport(ERROR) inside FunctionCall2Coll).
fn raising_cmp(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Err(Box::new(
        PgError::error("cmp failed").with_sqlstate(types_error::ERRCODE_RAISE_EXCEPTION),
    ))
}

fn raising_key(strategy: u8) -> PartitionKeyData {
    let mut key = test_key(strategy);
    key.partsupfunc = vec![RefCell::new(FmgrInfo::new(raising_cmp, 0, 2, true, false))];
    key
}

fn assert_cmp_failed(e: &PgError) {
    assert_eq!(e.message(), "cmp failed");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_RAISE_EXCEPTION);
    assert_eq!(e.level(), types_error::ERROR);
}

// partbounds.c:533 (create_list_bounds qsort_arg) and :771 (create_range_bounds
// qsort): the support function's ereport(ERROR) longjmps out of the sort and
// the statement aborts with the function's own error, not a panic.
#[test]
fn support_function_error_surfaces_from_bound_creation() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = raising_key(PARTITION_STRATEGY_LIST);
    let specs = [list_spec(mcx, &[Some(1)]), list_spec(mcx, &[Some(2)])];
    let e = partition_bounds_create(mcx, &specs, &key)
        .err()
        .expect("list bound sort must surface the support function error");
    assert_cmp_failed(&e);

    let key = raising_key(PARTITION_STRATEGY_RANGE);
    let specs = [range_spec(mcx, Some(1), Some(10)), range_spec(mcx, Some(10), Some(20))];
    let e = partition_bounds_create(mcx, &specs, &key)
        .err()
        .expect("range bound sort must surface the support function error");
    assert_cmp_failed(&e);
}

// partbounds.c:3525 (partition_rbound_cmp via check_new_partition_bound's
// lower-vs-upper compare): same error, same catchability.
#[test]
fn support_function_error_surfaces_from_check_new_partition_bound() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let key = raising_key(PARTITION_STRATEGY_RANGE);
    let spec = range_spec(mcx, Some(1), Some(10));
    let e = check_new_partition_bound(mcx, "p_new", &key, None, &[], spec, None)
        .err()
        .expect("range bound check must surface the support function error");
    assert_cmp_failed(&e);

    let key = raising_key(PARTITION_STRATEGY_LIST);
    let (info, _) = partition_bounds_create(
        mcx,
        &[list_spec(mcx, &[Some(1)])],
        &test_key(PARTITION_STRATEGY_LIST),
    )
    .unwrap();
    let e = check_new_partition_bound(
        mcx,
        "p_new",
        &key,
        Some(&info),
        &[100],
        list_spec(mcx, &[Some(2)]),
        None,
    )
    .err()
    .expect("list bound bsearch must surface the support function error");
    assert_cmp_failed(&e);
}
