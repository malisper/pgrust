//! Unit tests for the `brin_minmax.c` port.
//!
//! The seam slots are process-global `OnceLock`s, so every test shares one
//! install ([`install_test_seams`]). The fmgr `function_call2_coll` stub
//! implements the int4 B-tree comparison operators keyed by the function OID
//! installed by the `get_opcode` stub, so the strategy dispatch / equality
//! short-circuit / cache invalidation can be exercised end-to-end.

extern crate std;

use super::*;
use mcx::MemoryContext;
use std::sync::Once;

use types_core::NAMEDATALEN;
use types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
use types_typcache::{TypeCacheEntry, TYPSTORAGE_PLAIN};

// Synthetic function OIDs the get_opcode stub maps each B-tree operator to.
const FN_INT4_LT: Oid = 9001;
const FN_INT4_LE: Oid = 9002;
const FN_INT4_GT: Oid = 9005;
const FN_INT4_GE: Oid = 9004;

// Synthetic operator OIDs returned by get_opfamily_member per strategy.
const OP_INT4_LT: Oid = 8001;
const OP_INT4_LE: Oid = 8002;
const OP_INT4_GT: Oid = 8005;
const OP_INT4_GE: Oid = 8004;

const INT4_OPFAMILY: Oid = 1976;
const INT4OID: Oid = 23;

static INSTALL: Once = Once::new();

fn install_test_seams() {
    INSTALL.call_once(|| {
        // brin_minmax_opcinfo's lookup_type_cache(typoid, 0).
        typcache::lookup_type_cache::set(|type_id, _flags| Ok(int4_typcache(type_id)));

        // get_opfamily_member(opfamily, lefttype, righttype, strategy) -> opr.
        lsyscache::get_opfamily_member::set(|_opfamily, _lt, _rt, strategy| {
            Ok(match strategy as u16 {
                BT_LESS_STRATEGY_NUMBER => OP_INT4_LT,
                BT_LESS_EQUAL_STRATEGY_NUMBER => OP_INT4_LE,
                BT_GREATER_EQUAL_STRATEGY_NUMBER => OP_INT4_GE,
                BT_GREATER_STRATEGY_NUMBER => OP_INT4_GT,
                _ => INVALID_OID,
            })
        });

        // get_opcode(opr) -> comparison function OID.
        lsyscache::get_opcode::set(|opno| {
            Ok(match opno {
                OP_INT4_LT => FN_INT4_LT,
                OP_INT4_LE => FN_INT4_LE,
                OP_INT4_GE => FN_INT4_GE,
                OP_INT4_GT => FN_INT4_GT,
                _ => INVALID_OID,
            })
        });

        // function_call2_coll(fn, coll, a, b) -> bool word for int4 comparisons.
        fmgr::function_call2_coll::set(|function_id, _coll, a, b| {
            let x = a.as_usize() as i32;
            let y = b.as_usize() as i32;
            let res = match function_id {
                FN_INT4_LT => x < y,
                FN_INT4_LE => x <= y,
                FN_INT4_GT => x > y,
                FN_INT4_GE => x >= y,
                other => panic!("unexpected fn oid {other}"),
            };
            Ok(types_datum::Datum::from_usize(res as usize))
        });

        // datum_copy of a by-value int4 is a fresh by-value word (no borrow of
        // the input, so it carries the caller's 'mcx).
        scalar::datum_copy::set(|_mcx, value, _byval, _len| {
            Ok(Datum::from_usize(value.as_usize()))
        });
    });
}

fn int4_typcache(type_id: Oid) -> TypeCacheEntry {
    TypeCacheEntry {
        type_id,
        typlen: 4,
        typbyval: true,
        typalign: b'i' as i8,
        typstorage: TYPSTORAGE_PLAIN,
        typtype: b'b' as i8,
    }
}

fn index_tupdesc<'mcx>(mcx: Mcx<'mcx>, natts: usize) -> mcx::PgBox<'mcx, TupleDescData<'mcx>> {
    let mut compact_attrs = mcx::vec_with_capacity_in(mcx, natts).unwrap();
    let mut attrs = mcx::vec_with_capacity_in(mcx, natts).unwrap();
    for i in 0..natts {
        compact_attrs.push(CompactAttribute {
            attcacheoff: -1,
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });
        attrs.push(FormData_pg_attribute {
            attrelid: 0,
            attname: NameData {
                data: [0u8; NAMEDATALEN as usize],
            },
            atttypid: INT4OID,
            attlen: 4,
            attnum: (i + 1) as i16,
            atttypmod: -1,
            attndims: 0,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: TYPSTORAGE_PLAIN,
            attcompression: 0,
            attnotnull: false,
            atthasdef: false,
            atthasmissing: false,
            attidentity: 0,
            attgenerated: 0,
            attisdropped: false,
            attislocal: true,
            attinhcount: 0,
            attcollation: 0,
        });
    }
    mcx::alloc_in(
        mcx,
        TupleDescData {
            natts: natts as i32,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs,
            attrs,
        },
    )
    .unwrap()
}

fn make_index_rel<'mcx>(mcx: Mcx<'mcx>, natts: usize) -> Relation<'mcx> {
    use types_rel::{FormData_pg_class, RelationData};
    use types_storage::RelFileLocator;
    let mut rd_opfamily = mcx::PgVec::new_in(mcx);
    for _ in 0..natts {
        rd_opfamily.push(INT4_OPFAMILY);
    }
    let rd = RelationData {
        rd_id: 1,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_rel: FormData_pg_class {
            relname: mcx::PgString::from_str_in("brinidx", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhasindex: false,
            relhassubclass: false,
            relpersistence: b'p',
            relkind: b'i',
            relam: 0,
            relispopulated: true,
            relreplident: b'n',
            relispartition: false,
            relfrozenxid: 0,
        },
        rd_att: index_tupdesc(mcx, natts),
        rd_options: None,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily,
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_trigdesc: None,
    };
    Relation::open(rd, None)
}

fn brin_desc<'mcx>(mcx: Mcx<'mcx>, natts: usize) -> BrinDesc<'mcx> {
    let mut bd_info = mcx::vec_with_capacity_in(mcx, natts).unwrap();
    for _ in 0..natts {
        let mut typcache_vec = mcx::vec_with_capacity_in(mcx, 2).unwrap();
        typcache_vec.push(int4_typcache(INT4OID));
        typcache_vec.push(int4_typcache(INT4OID));
        bd_info.push(
            mcx::alloc_in(
                mcx,
                BrinOpcInfo {
                    oi_nstored: 2,
                    oi_regular_nulls: true,
                    oi_opaque: Some(OpaqueOpcInfo::Minmax(MinmaxOpaque::default())),
                    oi_typcache: typcache_vec,
                },
            )
            .unwrap(),
        );
    }
    BrinDesc {
        bd_index: make_index_rel(mcx, natts),
        bd_tupdesc: index_tupdesc(mcx, natts),
        bd_totalstored: (natts * 2) as i32,
        bd_info,
    }
}

fn d(v: i32) -> Datum<'static> {
    Datum::from_usize(v as usize)
}

fn brin_values<'mcx>(mcx: Mcx<'mcx>, min: i32, max: i32) -> BrinValues<'mcx> {
    let mut vals = mcx::vec_with_capacity_in(mcx, 2).unwrap();
    vals.push(d(min));
    vals.push(d(max));
    BrinValues {
        bv_attno: 1,
        bv_hasnulls: false,
        bv_allnulls: false,
        bv_values: vals,
        bv_mem_value: None,
        bv_has_serialize: false,
    }
}

#[test]
fn opcinfo_two_stored_regular_nulls() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let info = brin_minmax_opcinfo(root.mcx(), INT4OID).unwrap();
    assert_eq!(info.oi_nstored, 2);
    assert!(info.oi_regular_nulls);
    assert_eq!(info.oi_typcache.len(), 2);
    assert_eq!(info.oi_typcache[0].type_id, INT4OID);
    assert!(matches!(info.oi_opaque, Some(OpaqueOpcInfo::Minmax(_))));
}

#[test]
fn add_value_into_allnulls_sets_min_and_max() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, 0, 0);
    col.bv_allnulls = true;
    let updated = brin_minmax_add_value(mcx, &bdesc, &mut col, &d(42), false, 0).unwrap();
    assert!(updated);
    assert!(!col.bv_allnulls);
    assert_eq!(col.bv_values[0].as_usize() as i32, 42);
    assert_eq!(col.bv_values[1].as_usize() as i32, 42);
}

#[test]
fn add_value_extends_range_both_ends() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, 10, 20);

    // Below the min: extends the minimum, max unchanged.
    assert!(brin_minmax_add_value(mcx, &bdesc, &mut col, &d(5), false, 0).unwrap());
    assert_eq!(col.bv_values[0].as_usize() as i32, 5);
    assert_eq!(col.bv_values[1].as_usize() as i32, 20);

    // Inside the range: no change.
    assert!(!brin_minmax_add_value(mcx, &bdesc, &mut col, &d(12), false, 0).unwrap());

    // Above the max: extends the maximum.
    assert!(brin_minmax_add_value(mcx, &bdesc, &mut col, &d(99), false, 0).unwrap());
    assert_eq!(col.bv_values[1].as_usize() as i32, 99);
}

#[test]
fn consistent_equal_two_call_short_circuit() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let col = brin_values(mcx, 10, 20);

    let key = |strat: u16, arg: i32| ScanKeyData {
        sk_flags: 0,
        sk_attno: 1,
        sk_strategy: strat,
        sk_subtype: INT4OID,
        sk_collation: 0,
        sk_func: types_core::fmgr::FmgrInfo::empty(),
        sk_argument: d(arg),
        sk_subkeys: None,
    };

    // = 15: 10 <= 15 (true) AND 20 >= 15 (true) => match.
    assert!(brin_minmax_consistent(&bdesc, &col, &key(BT_EQUAL_STRATEGY_NUMBER, 15), 0).unwrap());
    // = 5: 10 <= 5 is false => short-circuit, no match.
    assert!(!brin_minmax_consistent(&bdesc, &col, &key(BT_EQUAL_STRATEGY_NUMBER, 5), 0).unwrap());
    // = 25: 10 <= 25 (true) but 20 >= 25 is false => no match.
    assert!(!brin_minmax_consistent(&bdesc, &col, &key(BT_EQUAL_STRATEGY_NUMBER, 25), 0).unwrap());

    // < 25: min(10) < 25 => match. < 5: min(10) < 5 false.
    assert!(brin_minmax_consistent(&bdesc, &col, &key(BT_LESS_STRATEGY_NUMBER, 25), 0).unwrap());
    assert!(!brin_minmax_consistent(&bdesc, &col, &key(BT_LESS_STRATEGY_NUMBER, 5), 0).unwrap());

    // > 15: max(20) > 15 => match.
    assert!(brin_minmax_consistent(&bdesc, &col, &key(BT_GREATER_STRATEGY_NUMBER, 15), 0).unwrap());
}

#[test]
fn consistent_invalid_strategy_errors() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let col = brin_values(mcx, 10, 20);
    let key = ScanKeyData {
        sk_flags: 0,
        sk_attno: 1,
        sk_strategy: 99,
        sk_subtype: INT4OID,
        sk_collation: 0,
        sk_func: types_core::fmgr::FmgrInfo::empty(),
        sk_argument: d(1),
        sk_subkeys: None,
    };
    assert!(brin_minmax_consistent(&bdesc, &col, &key, 0).is_err());
}

#[test]
fn strategy_cache_invalidated_on_subtype_change() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let opaque = match bdesc.bd_info[0].oi_opaque.as_ref().unwrap() {
        OpaqueOpcInfo::Minmax(o) => o,
    };

    // First lookup at subtype 23 caches the LESS slot.
    let f = minmax_get_strategy_procinfo(&bdesc, opaque, 1, INT4OID, BT_LESS_STRATEGY_NUMBER).unwrap();
    assert_eq!(f, FN_INT4_LT);
    assert_eq!(opaque.cached_subtype.get(), INT4OID);
    assert_eq!(opaque.strategy_procinfos[0].get(), FN_INT4_LT);

    // Same subtype: GREATER slot fills, LESS stays.
    let g =
        minmax_get_strategy_procinfo(&bdesc, opaque, 1, INT4OID, BT_GREATER_STRATEGY_NUMBER).unwrap();
    assert_eq!(g, FN_INT4_GT);
    assert_eq!(opaque.strategy_procinfos[0].get(), FN_INT4_LT);

    // Subtype change invalidates the whole cache, re-caches LESS.
    let other_subtype: Oid = 20;
    let f2 =
        minmax_get_strategy_procinfo(&bdesc, opaque, 1, other_subtype, BT_LESS_STRATEGY_NUMBER)
            .unwrap();
    assert_eq!(f2, FN_INT4_LT);
    assert_eq!(opaque.cached_subtype.get(), other_subtype);
    // GREATER slot was invalidated by the subtype change.
    assert_eq!(opaque.strategy_procinfos[BT_GREATER_STRATEGY_NUMBER as usize - 1].get(), INVALID_OID);
}

#[test]
fn union_adjusts_both_bounds() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col_a = brin_values(mcx, 10, 20);
    let col_b = brin_values(mcx, 5, 25);
    brin_minmax_union(mcx, &bdesc, &mut col_a, &col_b, 0).unwrap();
    assert_eq!(col_a.bv_values[0].as_usize() as i32, 5);
    assert_eq!(col_a.bv_values[1].as_usize() as i32, 25);
}

#[test]
fn missing_operator_errors() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let opaque = match bdesc.bd_info[0].oi_opaque.as_ref().unwrap() {
        OpaqueOpcInfo::Minmax(o) => o,
    };
    // strategy 3 (Equal) is not mapped by the get_opfamily_member stub => missing.
    let r = minmax_get_strategy_procinfo(&bdesc, opaque, 1, INT4OID, BT_EQUAL_STRATEGY_NUMBER);
    assert!(r.is_err());
}
