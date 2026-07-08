use super::*;
use datum::Datum;
use std::sync::Once;
use syscache_seams::PgTypeTypcacheShape;
use types_core::INT4OID;
use types_tuple::NameData;

const INT4_ARRAY: Oid = 1007;
const COMPOSITE_OID: Oid = 90004;
const COMPOSITE_REL: Oid = 5001;
const DOMAIN_OID: Oid = 90001;
const RANGE_OID: Oid = 90005;
const MULTI_OID: Oid = 90007;
const F_INT4RANGE_CANONICAL: Oid = 3914;
const SHELL_OID: Oid = 90003;
const NOHASH_OID: Oid = 90006;
const ENUM_OID: Oid = 90008;

const INT4_BTREE_OPCLASS: Oid = 1978;
const INT4_HASH_OPCLASS: Oid = 1979;
const INT_BTREE_FAM: Oid = 1976;
const INT_HASH_FAM: Oid = 1977;
const ARRAY_BTREE_OPCLASS: Oid = 397;
const ARRAY_HASH_OPCLASS: Oid = 627;
const ARRAY_BTREE_FAM: Oid = 398;
const ARRAY_HASH_FAM: Oid = 628;
const ANYARRAYOID: Oid = 2277;

const INT4_EQ: Oid = 96;
const INT4_LT: Oid = 97;
const INT4_GT: Oid = 521;
const F_INT4EQ: Oid = 65;
const F_BTINT4CMP: Oid = 351;
const F_HASHINT4: Oid = 450;
const F_HASHINT4EXTENDED: Oid = 425;
const F_ARRAY_EQ: Oid = 744;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

fn name(s: &str) -> NameData {
    let mut n = NameData::default();
    n.namestrcpy(s);
    n
}

fn typrow(
    nm: &str,
    typtype: i8,
    typisdefined: bool,
    typrelid: Oid,
    typelem: Oid,
    typsubscript: Oid,
) -> PgTypeTypcacheShape {
    PgTypeTypcacheShape {
        typname: name(nm),
        typlen: 4,
        typbyval: true,
        typalign: b'i' as i8,
        typstorage: b'p' as i8,
        typtype,
        typisdefined,
        typrelid,
        typsubscript,
        typelem,
        typarray: InvalidOid,
        typcollation: InvalidOid,
    }
}

static SEAMS: Once = Once::new();

thread_local! {
    static ENUM_MEMBERS: core::cell::RefCell<Vec<(Oid, f32)>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

fn install() {
    SEAMS.call_once(|| {
        use syscache_seams as s;
        fmgr_core::init_seams();
        clauses::init_seams();
        pg_enum_seams::scan_enum_members::set(|mcx, typid| {
            assert_eq!(typid, ENUM_OID);
            let mut out = mcx::PgVec::new_in(mcx);
            ENUM_MEMBERS.with(|m| out.extend_from_slice(&m.borrow()));
            Ok(out)
        });
        s::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(typrow("int4", b'b' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                INT4_ARRAY => Some(typrow(
                    "_int4",
                    b'b' as i8,
                    true,
                    InvalidOid,
                    INT4OID,
                    F_ARRAY_SUBSCRIPT_HANDLER,
                )),
                COMPOSITE_OID => Some(typrow("comp", b'c' as i8, true, COMPOSITE_REL, InvalidOid, InvalidOid)),
                DOMAIN_OID => Some(typrow("dom", b'd' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                RANGE_OID => Some(typrow("rng", b'r' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                MULTI_OID => Some(typrow("mrng", b'm' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                SHELL_OID => Some(typrow("shell", b'b' as i8, false, InvalidOid, InvalidOid, InvalidOid)),
                NOHASH_OID => Some(typrow("nohash", b'b' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                ENUM_OID => Some(typrow("mood", b'e' as i8, true, InvalidOid, InvalidOid, InvalidOid)),
                _ => None,
            })
        });
        s::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(0x9e3779b1)));
        s::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                INT4_BTREE_OPCLASS => Some(s::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: INT_BTREE_FAM,
                    opcintype: INT4OID,
                    opckeytype: 0,
                }),
                INT4_HASH_OPCLASS => Some(s::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: INT_HASH_FAM,
                    opcintype: INT4OID,
                    opckeytype: 0,
                }),
                ARRAY_BTREE_OPCLASS => Some(s::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: ARRAY_BTREE_FAM,
                    opcintype: ANYARRAYOID,
                    opckeytype: 0,
                }),
                ARRAY_HASH_OPCLASS => Some(s::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: ARRAY_HASH_FAM,
                    opcintype: ANYARRAYOID,
                    opckeytype: 0,
                }),
                _ => None,
            })
        });
        s::lookup_pg_amop_by_strategy::set(|opfamily, lefttype, righttype, strategy| {
            assert_eq!(lefttype, righttype);
            Ok(match (opfamily, strategy) {
                (INT_BTREE_FAM, 1) => INT4_LT,
                (INT_BTREE_FAM, 3) => INT4_EQ,
                (INT_BTREE_FAM, 5) => INT4_GT,
                (INT_HASH_FAM, 1) => INT4_EQ,
                (ARRAY_BTREE_FAM, 1) => ARRAY_LT_OP,
                (ARRAY_BTREE_FAM, 3) => ARRAY_EQ_OP,
                (ARRAY_BTREE_FAM, 5) => ARRAY_GT_OP,
                (ARRAY_HASH_FAM, 1) => ARRAY_EQ_OP,
                _ => InvalidOid,
            })
        });
        s::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            assert_eq!(lefttype, righttype);
            Ok(match (opfamily, procnum) {
                (INT_BTREE_FAM, 1) => F_BTINT4CMP,
                (INT_HASH_FAM, 1) => F_HASHINT4,
                (INT_HASH_FAM, 2) => F_HASHINT4EXTENDED,
                (ARRAY_BTREE_FAM, 1) => F_BTARRAYCMP,
                (ARRAY_HASH_FAM, 1) => F_HASH_ARRAY,
                (ARRAY_HASH_FAM, 2) => F_HASH_ARRAY_EXTENDED,
                _ => InvalidOid,
            })
        });
        s::lookup_pg_operator_shape::set(|opno| {
            let oprcode = match opno {
                INT4_EQ => F_INT4EQ,
                ARRAY_EQ_OP => F_ARRAY_EQ,
                _ => InvalidOid,
            };
            Ok((oprcode != InvalidOid).then_some(s::PgOperatorShape {
                oprnamespace: 11,
                oprleft: InvalidOid,
                oprright: InvalidOid,
                oprresult: InvalidOid,
                oprcom: InvalidOid,
                oprnegate: InvalidOid,
                oprcode,
                oprrest: InvalidOid,
                oprjoin: InvalidOid,
                oprcanmerge: false,
                oprcanhash: false,
            }))
        });
        s::pg_type_base_shape::set(|typid| {
            Ok(match typid {
                INT4_ARRAY => Some(s::PgTypeBaseShape {
                    typtype: b'b' as i8,
                    typbasetype: InvalidOid,
                    typtypmod: -1,
                    typelem: INT4OID,
                    typsubscript: F_ARRAY_SUBSCRIPT_HANDLER,
                }),
                INT4OID | NOHASH_OID => Some(s::PgTypeBaseShape {
                    typtype: b'b' as i8,
                    typbasetype: InvalidOid,
                    typtypmod: -1,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                DOMAIN_OID => Some(s::PgTypeBaseShape {
                    typtype: b'd' as i8,
                    typbasetype: INT4OID,
                    typtypmod: -1,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                _ => None,
            })
        });
        s::pg_type_typrelid::set(|_| Ok(Some(InvalidOid)));
        s::pg_type_domain_shape::set(|typid| {
            Ok(match typid {
                DOMAIN_OID => Some(s::PgTypeDomainShape {
                    typname: name("dom"),
                    typnamespace: 2200,
                    typtype: b'd' as i8,
                    typnotnull: true,
                    typbasetype: INT4OID,
                }),
                INT4OID => Some(s::PgTypeDomainShape {
                    typname: name("int4"),
                    typnamespace: 11,
                    typtype: b'b' as i8,
                    typnotnull: false,
                    typbasetype: InvalidOid,
                }),
                _ => None,
            })
        });
        s::lookup_pg_range_shape::set(|range_oid| {
            Ok((range_oid == RANGE_OID).then_some(s::PgRangeShape {
                rngsubtype: INT4OID,
                rngmultitypid: MULTI_OID,
                rngcollation: InvalidOid,
                rngsubopc: INT4_BTREE_OPCLASS,
                rngcanonical: F_INT4RANGE_CANONICAL,
                rngsubdiff: InvalidOid,
            }))
        });
        s::lookup_pg_range_by_multirange::set(|mr| {
            Ok((mr == MULTI_OID).then_some(RANGE_OID))
        });
        s::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == 147).then_some(s::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: 16,
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
            }))
        });
        typcache_seams::scan_domain_check_constraints::set(|mcx, contypid| {
            let mut rows = mcx::vec_with_capacity_in(mcx, 2)?;
            if contypid == DOMAIN_OID {
                for nm in ["dom_check_b", "dom_check_a"] {
                    rows.push(typcache_seams::DomainCheckRow {
                        conname: name(nm),
                        conbin: CONBIN_VALUE_GT_0,
                    });
                }
            }
            Ok(rows)
        });
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4OID, types_core::BTREE_AM_OID) => INT4_BTREE_OPCLASS,
                (INT4OID, _) => INT4_HASH_OPCLASS,
                (INT4_ARRAY, types_core::BTREE_AM_OID) => ARRAY_BTREE_OPCLASS,
                (INT4_ARRAY, _) => ARRAY_HASH_OPCLASS,
                _ => InvalidOid,
            })
        });
    });
}

#[test]
fn scalar_lazy_fill_and_warm_hit() {
    install();
    let e = lookup_type_cache(INT4OID, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR).unwrap();
    assert_eq!(e.typlen(), 4);
    assert!(e.typbyval());
    assert_eq!(e.eq_opr(), INT4_EQ);
    assert_eq!(e.lt_opr(), INT4_LT);
    assert_eq!(e.gt_opr(), InvalidOid); // not requested yet
    assert_eq!(e.btree_opf(), INT_BTREE_FAM);

    let e2 = lookup_type_cache(INT4OID, TYPECACHE_EQ_OPR).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));

    let e3 = lookup_type_cache(INT4OID, TYPECACHE_GT_OPR | TYPECACHE_CMP_PROC).unwrap();
    assert!(Rc::ptr_eq(&e, &e3));
    assert_eq!(e.gt_opr(), INT4_GT);
    assert_eq!(e.cmp_proc(), F_BTINT4CMP);
}

#[test]
fn hash_and_finfo_resolution() {
    install();
    let e = lookup_type_cache(
        INT4OID,
        TYPECACHE_EQ_OPR_FINFO
            | TYPECACHE_CMP_PROC_FINFO
            | TYPECACHE_HASH_PROC_FINFO
            | TYPECACHE_HASH_EXTENDED_PROC_FINFO,
    )
    .unwrap();
    assert_eq!(e.eq_opr(), INT4_EQ);
    assert_eq!(e.hash_proc(), F_HASHINT4);
    assert_eq!(e.hash_extended_proc(), F_HASHINT4EXTENDED);
    assert_eq!(e.eq_opr_finfo().fn_oid, F_INT4EQ);
    assert_eq!(e.cmp_proc_finfo().fn_oid, F_BTINT4CMP);
    assert_eq!(e.hash_proc_finfo().fn_oid, F_HASHINT4);
    assert_eq!(e.hash_extended_proc_finfo().fn_oid, F_HASHINT4EXTENDED);
    assert_eq!(e.hash_proc_finfo().fn_nargs, 1);
}

#[test]
fn array_element_properties_gate_array_ops() {
    install();
    let e = lookup_type_cache(
        INT4_ARRAY,
        TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC,
    )
    .unwrap();
    // int4 supports equality/compare/hashing, so array_eq et al. survive.
    assert_eq!(e.eq_opr(), ARRAY_EQ_OP);
    assert_eq!(e.lt_opr(), ARRAY_LT_OP);
    assert_eq!(e.cmp_proc(), F_BTARRAYCMP);
    assert_eq!(e.hash_proc(), F_HASH_ARRAY);
}

#[test]
fn no_opclasses_yields_invalid_oids() {
    install();
    let e = lookup_type_cache(NOHASH_OID, TYPECACHE_EQ_OPR | TYPECACHE_HASH_PROC).unwrap();
    assert_eq!(e.eq_opr(), InvalidOid);
    assert_eq!(e.hash_proc(), InvalidOid);
    // Negative results are cached: warm hit afterwards.
    let e2 = lookup_type_cache(NOHASH_OID, TYPECACHE_EQ_OPR | TYPECACHE_HASH_PROC).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
}

#[test]
fn missing_and_shell_types_ereport() {
    install();
    let err = lookup_type_cache(424242, 0).unwrap_err();
    assert!(err.message().contains("type with OID 424242 does not exist"));
    let err = lookup_type_cache(SHELL_OID, 0).unwrap_err();
    assert!(err.message().contains("type \"shell\" is only a shell"));
    // The failed lookups left in_progress slots; eoxact cleanup drains them.
    AtEOXact_TypeCache();
    with_state(|st| assert!(st.in_progress.is_empty()));
}

#[test]
fn typ_invalidation_forces_reload() {
    install();
    let e = lookup_type_cache(INT4OID, TYPECACHE_EQ_OPR).unwrap();
    let hash = e.type_id_hash;
    invalidate::TypeCacheTypCallback(Datum::from_oid(InvalidOid), 82, hash);
    // pg_type data dropped: even a flags==0 lookup goes slow and reloads.
    let e2 = lookup_type_cache(INT4OID, 0).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
    assert_ne!(e.flags_raw() & TCFLAGS_HAVE_PG_TYPE_DATA, 0);
    // Operator info survives a pg_type-only inval, as in C.
    assert_eq!(e.eq_opr(), INT4_EQ);
}

#[test]
fn opc_invalidation_clears_operator_flags() {
    install();
    let e = lookup_type_cache(INT4OID, TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO).unwrap();
    assert_eq!(e.eq_opr_finfo().fn_oid, F_INT4EQ);
    invalidate::TypeCacheOpcCallback(Datum::from_oid(InvalidOid), 14, 0);
    // Values remain readable while flags are cleared (C leaves them in place).
    assert_eq!(e.eq_opr(), INT4_EQ);
    let e2 = lookup_type_cache(INT4OID, TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
    assert_eq!(e2.eq_opr(), INT4_EQ);
    // Same OID re-resolved: finfo kept (C only resets it when the OID changes).
    assert_eq!(e2.eq_opr_finfo().fn_oid, F_INT4EQ);
}

#[test]
fn domain_entry_threads_into_chain() {
    install();
    let e = lookup_type_cache(DOMAIN_OID, 0).unwrap();
    assert_eq!(e.typtype(), TYPTYPE_DOMAIN);
    with_state(|st| {
        let mut t = st.first_domain_type_entry;
        let mut found = false;
        while t != InvalidOid {
            if t == DOMAIN_OID {
                found = true;
            }
            t = st.type_cache[&t].next_domain_get();
        }
        assert!(found);
    });
    // Constr callback walks the chain without touching scalar readiness.
    invalidate::TypeCacheConstrCallback(Datum::from_oid(InvalidOid), 19, 0);
    let e2 = lookup_type_cache(DOMAIN_OID, 0).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
}

#[test]
fn composite_entry_maintains_rel_map() {
    install();
    let _e = lookup_type_cache(COMPOSITE_OID, 0).unwrap();
    with_state(|st| assert_eq!(st.rel_id_to_type_id.get(&COMPOSITE_REL), Some(&COMPOSITE_OID)));
    invalidate::TypeCacheTypCallback(
        Datum::from_oid(InvalidOid),
        82,
        COMPOSITE_OID.wrapping_mul(0x9e3779b1),
    );
    with_state(|st| assert_eq!(st.rel_id_to_type_id.get(&COMPOSITE_REL), None));
}

#[test]
#[should_panic(expected = "composite TUPDESC lane not ported")]
fn composite_tupdesc_lane_is_loud() {
    install();
    let _ = lookup_type_cache(COMPOSITE_OID, TYPECACHE_TUPDESC);
}

#[test]
fn range_info_fills_and_links_elem() {
    install();
    let e = lookup_type_cache(RANGE_OID, TYPECACHE_RANGE_INFO).unwrap();
    assert_eq!(e.rng_collation(), InvalidOid);
    assert_eq!(e.rng_opfamily(), INT_BTREE_FAM);
    assert_eq!(e.rng_cmp_proc_finfo().fn_oid, F_BTINT4CMP);
    assert_eq!(e.rng_canonical_finfo().fn_oid, F_INT4RANGE_CANONICAL);
    let elem = e.rngelemtype().expect("rngelemtype linked");
    assert_eq!(elem.type_id, INT4OID);
    // Re-request re-verifies the elem entry, per C, and stays the same pin.
    let e2 = lookup_type_cache(RANGE_OID, TYPECACHE_RANGE_INFO).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
}

#[test]
fn multirange_info_links_range_entry() {
    install();
    let e = lookup_type_cache(MULTI_OID, TYPECACHE_MULTIRANGE_INFO).unwrap();
    let rt = e.rngtype().expect("rngtype linked");
    assert_eq!(rt.type_id, RANGE_OID);
    assert!(rt.rngelemtype().is_some());
    let e2 = lookup_type_cache(MULTI_OID, TYPECACHE_MULTIRANGE_INFO).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
}

const CONBIN_VALUE_GT_0: &str = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 \
    :opretset false :opcollid 0 :inputcollid 0 :args ({COERCETODOMAINVALUE \
    :typeId 23 :typeMod -1 :collation 0 :location 47} {CONST :consttype 23 \
    :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull \
    false :location 55 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) :location 53}";

#[test]
fn domain_base_info_lane() {
    install();
    let e = lookup_type_cache(DOMAIN_OID, TYPECACHE_DOMAIN_BASE_INFO).unwrap();
    assert_eq!(e.domain_base_type(), INT4OID);
    assert_eq!(e.domain_base_typmod(), -1);
}

#[test]
fn domain_constraints_order_and_update() {
    install();
    assert!(DomainHasConstraints(DOMAIN_OID).unwrap());
    let mut r = crate::domain::DomainConstraintRef::init(DOMAIN_OID).unwrap();
    let names: Vec<&str> = r.constraints().iter().map(|c| c.name).collect();
    assert_eq!(names, ["NOT NULL", "dom_check_a", "dom_check_b"]);
    assert_eq!(r.constraints()[0].constrainttype, DomConstraintType::NotNull);
    assert!(r.constraints()[1].check_expr.is_some());
    assert!(!r.update().unwrap());
    invalidate::TypeCacheConstrCallback(Datum::from_oid(InvalidOid), 19, 0);
    assert!(r.update().unwrap());
    assert_eq!(r.constraints().len(), 3);
}

#[test]
fn deferred_lane_flags_are_noops_for_other_typtypes() {
    install();
    // C's arms no-op when the typtype doesn't match; these must be warm-safe.
    let e = lookup_type_cache(
        INT4OID,
        TYPECACHE_TUPDESC
            | TYPECACHE_RANGE_INFO
            | TYPECACHE_MULTIRANGE_INFO
            | TYPECACHE_DOMAIN_BASE_INFO
            | TYPECACHE_DOMAIN_CONSTR_INFO,
    )
    .unwrap();
    assert_eq!(e.type_id, INT4OID);
}

#[test]
fn enum_compare_fast_path_and_reload() {
    install();
    ENUM_MEMBERS.with(|m| {
        *m.borrow_mut() = vec![(90100, 1.0), (90102, 2.0), (90104, 3.0)];
    });
    let e = lookup_type_cache(ENUM_OID, 0).unwrap();
    // Even in-order OIDs land in the known-sorted bitmap: bare OID compare.
    assert_eq!(compare_values_of_enum(&e, 90100, 90104).unwrap(), -1);
    assert_eq!(compare_values_of_enum(&e, 90104, 90100).unwrap(), 1);
    assert_eq!(compare_values_of_enum(&e, 90102, 90102).unwrap(), 0);

    // New odd-OID midpoint member appears after the cache loaded: the miss
    // forces a reload, then sort_order decides.
    ENUM_MEMBERS.with(|m| m.borrow_mut().push((90101, 2.5)));
    assert_eq!(compare_values_of_enum(&e, 90101, 90102).unwrap(), 1);
    assert_eq!(compare_values_of_enum(&e, 90101, 90104).unwrap(), -1);
    assert_eq!(compare_values_of_enum(&e, 90100, 90101).unwrap(), -1);

    // Out-of-order even OID (sorts before everything): binary-search path.
    ENUM_MEMBERS.with(|m| m.borrow_mut().push((90106, 0.5)));
    let e2 = lookup_type_cache(ENUM_OID, 0).unwrap();
    *e2.enum_data.borrow_mut() = None;
    assert_eq!(compare_values_of_enum(&e2, 90106, 90100).unwrap(), -1);
    assert_eq!(compare_values_of_enum(&e2, 90104, 90106).unwrap(), 1);
}

// SQLancer TLP campaign panic (lib.rs compute_ready): ANALYZE holds the
// entry's cmp_proc_finfo RefMut across the comparator call; a concurrent
// session's DDL inval (or a reentrant same-type lookup, e.g. range_cmp's
// fn_extra fill) recomputes readiness while the finfo is borrowed.
#[test]
fn compute_ready_tolerates_borrowed_finfo() {
    install();
    let e = lookup_type_cache(INT4OID, TYPECACHE_CMP_PROC_FINFO).unwrap();
    let guard = e.cmp_proc_finfo();
    assert_eq!(guard.fn_oid, F_BTINT4CMP);
    // TypCallback keeps TCFLAGS_CHECKED_CMP_PROC set, so the refilling lookup
    // recomputes readiness through the borrowed-finfo branch.
    invalidate::TypeCacheTypCallback(
        Datum::from_oid(InvalidOid),
        82,
        INT4OID.wrapping_mul(0x9e3779b1),
    );
    let e2 = lookup_type_cache(INT4OID, TYPECACHE_LT_OPR).unwrap();
    assert!(Rc::ptr_eq(&e, &e2));
    assert_eq!(e2.lt_opr(), INT4_LT);
    drop(guard);
}
