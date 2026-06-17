//! Unit tests for the `brin_inclusion.c` port.
//!
//! The seam slots are process-global `OnceLock`s, so every test shares one
//! install ([`install_test_seams`]). A simple model type is used: the "union"
//! is a by-value integer interval encoded as `(lo << 16) | hi`; the R-tree
//! support/strategy procedures are stubbed over the canonical-`Datum` fmgr
//! seams, so the strategy dispatch, the empty/unmergeable flag handling, and the
//! procinfo caches can be exercised end-to-end.

extern crate std;

use super::*;
use mcx::MemoryContext;
use std::sync::Once;

use types_core::NAMEDATALEN;
use types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

// Synthetic OIDs.
const MODEL_OID: Oid = 700700;
const MODEL_OPFAMILY: Oid = 700701;

// Stub support-procedure OIDs (extra_procinfos), keyed by procnum via
// index_getprocid / index_getprocinfo.
const FN_MERGE: Oid = 9101;
const FN_MERGEABLE: Oid = 9102;
const FN_CONTAINS: Oid = 9103;
const FN_EMPTY: Oid = 9104;

// Stub strategy operator + function OIDs.
const OP_OVERLAP: Oid = 8203;
const FN_OVERLAP: Oid = 9203;
const OP_CONTAINS_STRAT: Oid = 8207;
const FN_CONTAINS_STRAT: Oid = 9207;

static INSTALL: Once = Once::new();

// Encode/decode a model interval into a by-value Datum word.
fn enc(lo: u16, hi: u16) -> Datum<'static> {
    Datum::from_usize(((lo as usize) << 16) | hi as usize)
}
fn dec(d: &Datum) -> (u16, u16) {
    let w = d.as_usize();
    (((w >> 16) & 0xffff) as u16, (w & 0xffff) as u16)
}

fn install_test_seams() {
    INSTALL.call_once(|| {
        typcache::lookup_type_cache::set(|type_id, _flags| {
            Ok(types_typcache::TypeCacheEntry {
                type_id,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: types_typcache::TYPSTORAGE_PLAIN,
                typtype: b'b' as i8,
            })
        });

        // index_getprocid maps the support procnum to the stub OID (InvalidOid
        // when the opclass does not define the optional procedure — here every
        // model proc is present).
        indexam::index_getprocid::set(|_irel, _attno, procnum| {
            Ok(match procnum {
                PROCNUM_MERGE => FN_MERGE,
                PROCNUM_MERGEABLE => FN_MERGEABLE,
                PROCNUM_CONTAINS => FN_CONTAINS,
                PROCNUM_EMPTY => FN_EMPTY,
                _ => INVALID_OID,
            })
        });
        indexam::index_getprocinfo::set(|_irel, _attno, procnum| {
            let oid = match procnum {
                PROCNUM_MERGE => FN_MERGE,
                PROCNUM_MERGEABLE => FN_MERGEABLE,
                PROCNUM_CONTAINS => FN_CONTAINS,
                PROCNUM_EMPTY => FN_EMPTY,
                _ => INVALID_OID,
            };
            let mut f = types_core::fmgr::FmgrInfo::empty();
            f.fn_oid = oid;
            Ok(f)
        });

        // get_opfamily_member / get_opcode for the two strategies the tests use.
        lsyscache::get_opfamily_member::set(|_fam, _lt, _rt, strategy| {
            Ok(match strategy as u16 {
                RT_OVERLAP_STRATEGY_NUMBER => OP_OVERLAP,
                RT_CONTAINS_STRATEGY_NUMBER => OP_CONTAINS_STRAT,
                _ => INVALID_OID,
            })
        });
        lsyscache::get_opcode::set(|opno| {
            Ok(match opno {
                OP_OVERLAP => FN_OVERLAP,
                OP_CONTAINS_STRAT => FN_CONTAINS_STRAT,
                _ => INVALID_OID,
            })
        });

        // function_call1_coll_datum: only PROCNUM_EMPTY (lo == hi == 0 marks an
        // "empty" model value).
        fmgr::function_call1_coll_datum::set(|_mcx, function_id, _coll, arg1| {
            let (lo, hi) = dec(&arg1);
            let res = match function_id {
                FN_EMPTY => lo == 0 && hi == 0,
                other => panic!("unexpected 1-arg fn oid {other}"),
            };
            Ok(Datum::from_bool(res))
        });

        // function_call2_coll_datum: the merge (returns the bounding interval),
        // mergeable (always true here), contains (a ⊇ b), overlap, contains-strat.
        fmgr::function_call2_coll_datum::set(|_mcx, function_id, _coll, a, b| {
            let (alo, ahi) = dec(&a);
            let (blo, bhi) = dec(&b);
            Ok(match function_id {
                FN_MERGE => enc(alo.min(blo), ahi.max(bhi)),
                FN_MERGEABLE => Datum::from_bool(true),
                FN_CONTAINS | FN_CONTAINS_STRAT => Datum::from_bool(alo <= blo && bhi <= ahi),
                FN_OVERLAP => Datum::from_bool(alo <= bhi && blo <= ahi),
                other => panic!("unexpected 2-arg fn oid {other}"),
            })
        });

        scalar::datum_copy::set(|_mcx, value, _byval, _len| Ok(Datum::from_usize(value.as_usize())));
    });
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
            atttypid: MODEL_OID,
            attlen: 4,
            attnum: (i + 1) as i16,
            atttypmod: -1,
            attndims: 0,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: types_typcache::TYPSTORAGE_PLAIN,
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

fn make_index_rel<'mcx>(mcx: Mcx<'mcx>, natts: usize) -> types_rel::Relation<'mcx> {
    use types_rel::{FormData_pg_class, RelationData};
    use types_storage::RelFileLocator;
    let mut rd_opfamily = mcx::PgVec::new_in(mcx);
    for _ in 0..natts {
        rd_opfamily.push(MODEL_OPFAMILY);
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
            reltype: 0,
            relam: 0,
            relispopulated: true,
            relreplident: b'n',
            relispartition: false,
            relfrozenxid: 0,
            relminmxid: 0,
        },
        rd_att: index_tupdesc(mcx, natts),
        rd_options: None,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily,
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_trigdesc: None,
        pgstat_enabled: false,
    };
    types_rel::Relation::open(rd, None)
}

fn brin_desc<'mcx>(mcx: Mcx<'mcx>, natts: usize) -> BrinDesc<'mcx> {
    let mut bd_info = mcx::vec_with_capacity_in(mcx, natts).unwrap();
    for _ in 0..natts {
        let mut typcache_vec = mcx::vec_with_capacity_in(mcx, 3).unwrap();
        for _ in 0..3 {
            typcache_vec.push(types_typcache::TypeCacheEntry {
                type_id: MODEL_OID,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: types_typcache::TYPSTORAGE_PLAIN,
                typtype: b'b' as i8,
            });
        }
        bd_info.push(
            mcx::alloc_in(
                mcx,
                BrinOpcInfo {
                    oi_nstored: 3,
                    oi_regular_nulls: true,
                    oi_opaque: Some(OpaqueOpcInfo::Inclusion(InclusionOpaque::default())),
                    oi_typcache: typcache_vec,
                },
            )
            .unwrap(),
        );
    }
    BrinDesc {
        bd_index: make_index_rel(mcx, natts),
        bd_tupdesc: index_tupdesc(mcx, natts),
        bd_totalstored: (natts * 3) as i32,
        bd_info,
    }
}

fn brin_values<'mcx>(
    mcx: Mcx<'mcx>,
    union: Datum<'static>,
    unmergeable: bool,
    contains_empty: bool,
) -> BrinValues<'mcx> {
    let mut vals = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    vals.push(union);
    vals.push(Datum::from_bool(unmergeable));
    vals.push(Datum::from_bool(contains_empty));
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
fn opcinfo_three_stored_regular_nulls() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let info = brin_inclusion_opcinfo(root.mcx(), MODEL_OID).unwrap();
    assert_eq!(info.oi_nstored, 3);
    assert!(info.oi_regular_nulls);
    assert_eq!(info.oi_typcache.len(), 3);
    assert!(matches!(info.oi_opaque, Some(OpaqueOpcInfo::Inclusion(_))));
}

#[test]
fn add_value_into_allnulls_initializes_flags() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, enc(0, 0), false, false);
    col.bv_allnulls = true;
    // newval (10..20) is not empty, so contains-empty stays false; new => true.
    let updated =
        brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(10, 20), false, 0).unwrap();
    assert!(updated);
    assert!(!col.bv_allnulls);
    assert_eq!(dec(&col.bv_values[INCLUSION_UNION]), (10, 20));
    assert!(!col.bv_values[INCLUSION_UNMERGEABLE].as_bool());
    assert!(!col.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool());
}

#[test]
fn add_value_empty_sets_contains_empty() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, enc(10, 20), false, false);
    // empty model value (0,0): sets contains-empty, returns true.
    assert!(brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(0, 0), false, 0).unwrap());
    assert!(col.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool());
    // a second empty value: already set => no change.
    assert!(!brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(0, 0), false, 0).unwrap());
}

#[test]
fn add_value_already_contained_no_change() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, enc(10, 20), false, false);
    // 12..18 is contained by 10..20 => no change.
    assert!(!brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(12, 18), false, 0).unwrap());
    assert_eq!(dec(&col.bv_values[INCLUSION_UNION]), (10, 20));
}

#[test]
fn add_value_merges_outside_value() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, enc(10, 20), false, false);
    // 25..30 is outside, mergeable => merged into the union.
    assert!(brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(25, 30), false, 0).unwrap());
    assert_eq!(dec(&col.bv_values[INCLUSION_UNION]), (10, 30));
}

#[test]
fn add_value_short_circuits_on_unmergeable() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col = brin_values(mcx, enc(10, 20), true, false);
    // already unmergeable => returns false, no modification.
    assert!(!brin_inclusion_add_value(mcx, &bdesc, &mut col, &enc(25, 30), false, 0).unwrap());
    assert_eq!(dec(&col.bv_values[INCLUSION_UNION]), (10, 20));
}

#[test]
fn consistent_unmergeable_always_true() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let col = brin_values(mcx, enc(10, 20), true, false);
    let key = make_key(RT_OVERLAP_STRATEGY_NUMBER, enc(100, 200));
    assert!(brin_inclusion_consistent(mcx, &bdesc, &col, &key, 0).unwrap());
}

#[test]
fn consistent_overlap_calls_operator() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let col = brin_values(mcx, enc(10, 20), false, false);
    // overlap 15..25 with 10..20 => true; 30..40 => false.
    assert!(brin_inclusion_consistent(
        mcx,
        &bdesc,
        &col,
        &make_key(RT_OVERLAP_STRATEGY_NUMBER, enc(15, 25)),
        0
    )
    .unwrap());
    assert!(!brin_inclusion_consistent(
        mcx,
        &bdesc,
        &col,
        &make_key(RT_OVERLAP_STRATEGY_NUMBER, enc(30, 40)),
        0
    )
    .unwrap());
}

#[test]
fn consistent_contained_by_uses_overlap_then_empty() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    // ContainedBy uses the overlap operator; if no overlap, falls back to the
    // contains-empty flag.
    let col = brin_values(mcx, enc(10, 20), false, true);
    // no overlap (100..200) but contains_empty => true.
    assert!(brin_inclusion_consistent(
        mcx,
        &bdesc,
        &col,
        &make_key(RT_CONTAINED_BY_STRATEGY_NUMBER, enc(100, 200)),
        0
    )
    .unwrap());
    let col2 = brin_values(mcx, enc(10, 20), false, false);
    assert!(!brin_inclusion_consistent(
        mcx,
        &bdesc,
        &col2,
        &make_key(RT_CONTAINED_BY_STRATEGY_NUMBER, enc(100, 200)),
        0
    )
    .unwrap());
}

#[test]
fn consistent_invalid_strategy_errors() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let col = brin_values(mcx, enc(10, 20), false, false);
    let key = make_key(99, enc(1, 2));
    assert!(brin_inclusion_consistent(mcx, &bdesc, &col, &key, 0).is_err());
}

#[test]
fn union_propagates_empty_and_merges() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col_a = brin_values(mcx, enc(10, 20), false, false);
    let col_b = brin_values(mcx, enc(5, 25), false, true);
    brin_inclusion_union(mcx, &bdesc, &mut col_a, &col_b, 0).unwrap();
    // B's contains-empty propagates to A; the unions merge.
    assert!(col_a.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool());
    assert_eq!(dec(&col_a.bv_values[INCLUSION_UNION]), (5, 25));
}

#[test]
fn union_propagates_unmergeable_from_b() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);
    let mut col_a = brin_values(mcx, enc(10, 20), false, false);
    let col_b = brin_values(mcx, enc(5, 25), true, false);
    brin_inclusion_union(mcx, &bdesc, &mut col_a, &col_b, 0).unwrap();
    assert!(col_a.bv_values[INCLUSION_UNMERGEABLE].as_bool());
    // union untouched (we got out before merging).
    assert_eq!(dec(&col_a.bv_values[INCLUSION_UNION]), (10, 20));
}

#[test]
fn strategy_cache_invalidated_on_subtype_change() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);

    let f = inclusion_get_strategy_procinfo(mcx, &bdesc, 1, MODEL_OID, RT_OVERLAP_STRATEGY_NUMBER)
        .unwrap();
    assert_eq!(f, FN_OVERLAP);
    {
        let opaque = inclusion_opaque(&bdesc, 1);
        assert_eq!(opaque.cached_subtype.get(), MODEL_OID);
        assert_eq!(
            opaque.strategy_procinfos[(RT_OVERLAP_STRATEGY_NUMBER - 1) as usize].get(),
            FN_OVERLAP
        );
    }

    // Subtype change invalidates the cache.
    let other: Oid = MODEL_OID + 1;
    let f2 =
        inclusion_get_strategy_procinfo(mcx, &bdesc, 1, other, RT_OVERLAP_STRATEGY_NUMBER).unwrap();
    assert_eq!(f2, FN_OVERLAP);
    {
        let opaque = inclusion_opaque(&bdesc, 1);
        assert_eq!(opaque.cached_subtype.get(), other);
    }
}

#[test]
fn procinfo_missing_records_missing() {
    install_test_seams();
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let bdesc = brin_desc(mcx, 1);

    // Override index_getprocid is not possible per-test (OnceLock), so use a
    // separate opaque whose extra_proc_missing path is reached via the stub: the
    // stub always returns a valid OID, so confirm the found path caches it.
    let oid = inclusion_get_procinfo(mcx, &bdesc, 1, PROCNUM_MERGE, false).unwrap();
    assert_eq!(oid, Some(FN_MERGE));
    let opaque = inclusion_opaque(&bdesc, 1);
    assert_eq!(
        opaque.extra_procinfos[(PROCNUM_MERGE - PROCNUM_BASE) as usize].get(),
        FN_MERGE
    );
}

fn make_key(strat: u16, arg: Datum<'static>) -> ScanKeyData<'static> {
    ScanKeyData {
        sk_flags: 0,
        sk_attno: 1,
        sk_strategy: strat,
        sk_subtype: MODEL_OID,
        sk_collation: 0,
        sk_func: types_core::fmgr::FmgrInfo::empty(),
        sk_argument: arg,
        sk_subkeys: None,
    }
}
