use super::*;
use mcx::{Mcx, MemoryContext, PgVec};
use std::sync::Once;
use syscache_seams::{
    PgAmopMemberShape, PgAmopShape, PgAttributeLsShape, PgClassLsShape, PgCollationShape,
    PgConstraintShape, PgIndexLsShape, PgOpclassShape, PgOperatorShape, PgOpfamilyShape,
    PgProcShape, PgRangeShape, PgStatisticSlotShape, PgTransformShape, PgTypeBaseShape,
    PgTypeElementShape, PgTypeIoShape,
};
use types_core::{InvalidAttrNumber, InvalidOid, Oid, BOOLOID, INT4OID, TEXTOID};
use types_tuple::{NameData, PgTypeShape, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN};

const INT4_ARRAY: Oid = 1007;
const DOMAIN_OID: Oid = 90001;
const DOMAIN2_OID: Oid = 90002;
const SHELL_OID: Oid = 90003;
const COMPOSITE_OID: Oid = 90004;
const REL_OID: Oid = 5001;
const IDX_OID: Oid = 5002;
const INT4_EQ: Oid = 96;
const INT4_LT: Oid = 97;
const F_INT4EQ: Oid = 65;
const INT_BTREE_FAM: Oid = 1976;
const INT_HASH_FAM: Oid = 1977;
const INT4_OPCLASS: Oid = 10013;

fn name(s: &str) -> NameData {
    let mut n = NameData::default();
    n.namestrcpy(s);
    n
}

static SEAMS: Once = Once::new();

fn install() {
    SEAMS.call_once(|| {
        use syscache_seams as s;
        s::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: InvalidOid,
                }),
                TEXTOID | DOMAIN_OID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_EXTENDED,
                    typcollation: 100,
                }),
                _ => None,
            })
        });
        // get_cast_oid's undefined-cast message renders via format_type_be.
        s::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                INT4OID | TEXTOID => Some(syscache_seams::PgTypeTypcacheShape {
                    typname: name(if typid == INT4OID { "int4" } else { "text" }),
                    typlen: if typid == INT4OID { 4 } else { -1 },
                    typbyval: typid == INT4OID,
                    typalign: TYPALIGN_INT,
                    typstorage: if typid == INT4OID {
                        TYPSTORAGE_PLAIN
                    } else {
                        TYPSTORAGE_EXTENDED
                    },
                    typtype: b'b' as i8,
                    typisdefined: true,
                    typrelid: InvalidOid,
                    typsubscript: InvalidOid,
                    typelem: InvalidOid,
                    typarray: InvalidOid,
                    typcollation: InvalidOid,
                }),
                _ => None,
            })
        });
        namespace_seams::type_is_visible::set(|_typid| Ok(true));
        s::pg_type_isdefined::set(|typid| Ok((typid != 1).then_some(typid != SHELL_OID)));
        s::pg_type_typtype::set(|typid| {
            Ok(match typid {
                DOMAIN_OID | DOMAIN2_OID => Some(typ::TYPTYPE_DOMAIN),
                COMPOSITE_OID => Some(typ::TYPTYPE_COMPOSITE),
                INT4OID | TEXTOID => Some(typ::TYPTYPE_BASE),
                _ => None,
            })
        });
        s::pg_type_category::set(|typid| Ok((typid == INT4OID).then_some((b'N' as i8, true))));
        s::pg_type_typrelid::set(|typid| Ok((typid == COMPOSITE_OID).then_some(4242)));
        s::pg_type_element_shape::set(|typid| {
            Ok(match typid {
                INT4_ARRAY => Some(PgTypeElementShape {
                    typelem: INT4OID,
                    typsubscript: typ::F_ARRAY_SUBSCRIPT_HANDLER,
                }),
                INT4OID | TEXTOID => Some(PgTypeElementShape {
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                _ => None,
            })
        });
        s::pg_type_typarray::set(|typid| {
            Ok(match typid {
                INT4OID => Some(INT4_ARRAY),
                INT4_ARRAY | DOMAIN2_OID => Some(InvalidOid),
                _ => None,
            })
        });
        s::pg_type_base_shape::set(|typid| {
            Ok(match typid {
                DOMAIN_OID => Some(PgTypeBaseShape {
                    typtype: typ::TYPTYPE_DOMAIN,
                    typbasetype: TEXTOID,
                    typtypmod: 7,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                DOMAIN2_OID => Some(PgTypeBaseShape {
                    typtype: typ::TYPTYPE_DOMAIN,
                    typbasetype: INT4_ARRAY,
                    typtypmod: -1,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                INT4_ARRAY => Some(PgTypeBaseShape {
                    typtype: typ::TYPTYPE_BASE,
                    typbasetype: InvalidOid,
                    typtypmod: -1,
                    typelem: INT4OID,
                    typsubscript: typ::F_ARRAY_SUBSCRIPT_HANDLER,
                }),
                TEXTOID | INT4OID | COMPOSITE_OID => Some(PgTypeBaseShape {
                    typtype: if typid == COMPOSITE_OID {
                        typ::TYPTYPE_COMPOSITE
                    } else {
                        typ::TYPTYPE_BASE
                    },
                    typbasetype: InvalidOid,
                    typtypmod: -1,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }),
                _ => None,
            })
        });
        s::pg_type_io_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeIoShape {
                    oid: INT4OID,
                    typinput: 42,
                    typoutput: 43,
                    typreceive: 2406,
                    typsend: 2407,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: InvalidOid,
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                INT4_ARRAY => Some(PgTypeIoShape {
                    oid: INT4_ARRAY,
                    typinput: 750,
                    typoutput: 751,
                    typreceive: 2400,
                    typsend: 2401,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: INT4OID,
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                SHELL_OID => Some(PgTypeIoShape {
                    oid: SHELL_OID,
                    typinput: InvalidOid,
                    typoutput: InvalidOid,
                    typreceive: InvalidOid,
                    typsend: InvalidOid,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: InvalidOid,
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typdelim: b',' as i8,
                    typisdefined: false,
                }),
                _ => None,
            })
        });
        s::pg_type_default_strings::set(|_mcx, typid| {
            Ok((typid == INT4OID).then(|| syscache_seams::PgTypeDefaultShape {
                typdefaultbin: None,
                typdefault: None,
            }))
        });
        s::lookup_pg_operator_shape::set(|opno| {
            Ok(match opno {
                INT4_EQ => Some(PgOperatorShape {
                oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: BOOLOID,
                    oprcom: INT4_EQ,
                    oprnegate: 518,
                    oprcode: F_INT4EQ,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                INT4_LT => Some(PgOperatorShape {
                oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: BOOLOID,
                    oprcom: 521,
                    oprnegate: 525,
                    oprcode: 66,
                    oprrest: 103,
                    oprjoin: 106,
                    oprcanmerge: false,
                    oprcanhash: false,
                }),
                _ => None,
            })
        });
        s::pg_operator_oprname::set(|opno| {
            Ok(match opno {
                INT4_EQ => Some(name("=")),
                INT4_LT => Some(name("<")),
                _ => None,
            })
        });
        s::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
            if purpose != amop::AMOP_SEARCH || opfamily != INT_BTREE_FAM {
                return Ok(None);
            }
            let strategy = match opno {
                INT4_EQ => amop::BTEqualStrategyNumber,
                INT4_LT => amop::BTLessStrategyNumber,
                _ => return Ok(None),
            };
            Ok(Some(PgAmopShape {
                amopstrategy: strategy,
                amopsortfamily: InvalidOid,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }))
        });
        s::lookup_pg_amop_by_strategy::set(|opfamily, lefttype, righttype, strategy| {
            Ok(match (opfamily, lefttype, righttype, strategy) {
                (INT_BTREE_FAM, INT4OID, INT4OID, 3) => INT4_EQ,
                (INT_BTREE_FAM, INT4OID, INT4OID, 1) => INT4_LT,
                (INT_HASH_FAM, INT4OID, INT4OID, 1) => INT4_EQ,
                _ => InvalidOid,
            })
        });
        s::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            let mut v = PgVec::new_in(mcx);
            match opno {
                INT4_EQ => {
                    v.push(PgAmopMemberShape {
                        amopfamily: INT_BTREE_FAM,
                        amoplefttype: INT4OID,
                        amoprighttype: INT4OID,
                        amopstrategy: 3,
                        amopmethod: types_core::BTREE_AM_OID,
                    });
                    v.push(PgAmopMemberShape {
                        amopfamily: INT_HASH_FAM,
                        amoplefttype: INT4OID,
                        amoprighttype: INT4OID,
                        amopstrategy: 1,
                        amopmethod: amop::HASH_AM_OID,
                    });
                }
                INT4_LT => {
                    v.push(PgAmopMemberShape {
                        amopfamily: INT_BTREE_FAM,
                        amoplefttype: INT4OID,
                        amoprighttype: INT4OID,
                        amopstrategy: 1,
                        amopmethod: types_core::BTREE_AM_OID,
                    });
                }
                _ => {}
            }
            Ok(v)
        });
        s::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(
                if (opfamily, lefttype, righttype, procnum) == (INT_HASH_FAM, INT4OID, INT4OID, 1)
                {
                    450
                } else {
                    InvalidOid
                },
            )
        });
        s::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == F_INT4EQ).then_some(PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: BOOLOID,
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: true,
            }))
        });
        s::pg_proc_proname::set(|funcid| Ok((funcid == F_INT4EQ).then(|| name("int4eq"))));
        s::lookup_pg_proc_signature::set(|mcx, funcid| {
            if funcid != F_INT4EQ {
                return Ok(None);
            }
            let mut args = PgVec::new_in(mcx);
            args.push(INT4OID);
            args.push(INT4OID);
            Ok(Some((BOOLOID, args)))
        });
        s::lookup_pg_attribute_shape::set(|relid, attnum| {
            Ok(((relid, attnum) == (REL_OID, 1)).then_some(PgAttributeLsShape {
                attname: name("id"),
                atttypid: INT4OID,
                atttypmod: -1,
                attcollation: InvalidOid,
                attgenerated: 0,
            }))
        });
        s::lookup_pg_attribute_attnum_by_name::set(|relid, attname| {
            Ok(if relid == REL_OID && attname == "id" { 1 } else { InvalidAttrNumber })
        });
        s::pg_attribute_attoptions::set(|_mcx, relid, attnum| {
            Ok(((relid, attnum) == (REL_OID, 1)).then_some(None))
        });
        s::lookup_pg_class_ls_shape::set(|relid| {
            Ok((relid == REL_OID).then_some(PgClassLsShape {
                relnamespace: 2200,
                reltype: COMPOSITE_OID,
                relam: 2,
                reltablespace: InvalidOid,
                relnatts: 3,
                relkind: b'r' as i8,
                relpersistence: b'p' as i8,
                relispartition: false,
                relhassubclass: false,
            }))
        });
        s::pg_class_relname::set(|relid| Ok((relid == REL_OID).then(|| name("t1"))));
        s::lookup_pg_class_relid_by_name::set(|relname, nsp| {
            Ok(if relname == "t1" && nsp == 2200 { REL_OID } else { InvalidOid })
        });
        s::lookup_pg_index_ls_shape::set(|index_oid| {
            Ok((index_oid == IDX_OID).then_some(PgIndexLsShape {
                indnatts: 2,
                indnkeyatts: 1,
                indisreplident: false,
                indisvalid: true,
                indisclustered: false,
            }))
        });
        s::pg_index_indclass_element::set(|index_oid, idx| {
            Ok(((index_oid, idx) == (IDX_OID, 0)).then_some(INT4_OPCLASS))
        });
        s::lookup_pg_opclass_shape::set(|opclass| {
            Ok((opclass == INT4_OPCLASS).then_some(PgOpclassShape {
                opcmethod: types_core::BTREE_AM_OID,
                opcfamily: INT_BTREE_FAM,
                opcintype: INT4OID,
                opckeytype: 0,
            }))
        });
        s::lookup_pg_opfamily_shape::set(|opfid| {
            Ok(match opfid {
                INT_BTREE_FAM => Some(PgOpfamilyShape {
                    opfmethod: types_core::BTREE_AM_OID,
                    opfname: name("integer_ops"),
                }),
                INT_HASH_FAM => Some(PgOpfamilyShape {
                    opfmethod: amop::HASH_AM_OID,
                    opfname: name("integer_ops"),
                }),
                _ => None,
            })
        });
        s::lookup_pg_cast_oid::set(|src, tgt| {
            Ok(if (src, tgt) == (INT4OID, TEXTOID) { 7777 } else { InvalidOid })
        });
        s::lookup_pg_collation_shape::set(|colloid| {
            Ok((colloid == 100).then_some(PgCollationShape {
                collname: name("default"),
                collnamespace: 11,
                collisdeterministic: true,
            }))
        });
        s::lookup_pg_constraint_shape::set(|conoid| {
            Ok(match conoid {
                8001 => Some(PgConstraintShape {
                    conname: name("t1_pkey"),
                    contype: misc::CONSTRAINT_PRIMARY,
                    conindid: IDX_OID,
                }),
                8002 => Some(PgConstraintShape {
                    conname: name("t1_fkey"),
                    contype: misc::CONSTRAINT_FOREIGN,
                    conindid: IDX_OID,
                }),
                _ => None,
            })
        });
        s::lookup_pg_language_name::set(|langoid| Ok((langoid == 13).then(|| name("plpgsql"))));
        s::lookup_pg_transform_shape::set(|typid, langid| {
            Ok(((typid, langid) == (INT4OID, 13)).then_some(PgTransformShape {
                trffromsql: 9001,
                trftosql: 9002,
            }))
        });
        s::pg_namespace_nspname::set(|nspid| Ok((nspid == 2200).then(|| name("public"))));
        s::lookup_pg_range_shape::set(|range_oid| {
            Ok((range_oid == 3904).then_some(PgRangeShape {
                rngsubtype: INT4OID,
                rngmultitypid: 4451,
                rngcollation: InvalidOid,
                rngsubopc: 1978,
                rngcanonical: 3914,
                rngsubdiff: 3922,
            }))
        });
        s::lookup_pg_range_by_multirange::set(|mr| Ok((mr == 4451).then_some(3904)));
        s::lookup_pg_publication_oid::set(|pubname| {
            Ok(if pubname == "pub1" { 6001 } else { InvalidOid })
        });
        s::pg_publication_pubname::set(|pubid| Ok((pubid == 6001).then(|| name("pub1"))));
        s::lookup_pg_subscription_oid::set(|dbid, subname| {
            Ok(if dbid == 1 && subname == "sub1" { 6002 } else { InvalidOid })
        });
        s::pg_subscription_subname::set(|subid| Ok((subid == 6002).then(|| name("sub1"))));
        s::pg_statistic_stawidth::set(|relid, attnum, inh| {
            Ok(((relid, attnum, inh) == (REL_OID, 1, false)).then_some(4))
        });
        s::pg_statistic_slot_shape::set(|_tuple| PgStatisticSlotShape {
            stakind: [1, 2, 0, 0, 0],
            staop: [INT4_EQ, INT4_LT, 0, 0, 0],
            stacoll: [InvalidOid, InvalidOid, 0, 0, 0],
        });
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    });
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install();
    let ctx = MemoryContext::new("lsyscache test");
    f(ctx.mcx())
}

#[test]
fn type_getters() {
    with_mcx(|_m| {
        assert_eq!(get_typlen(INT4OID).unwrap(), 4);
        assert_eq!(get_typlen(TEXTOID).unwrap(), -1);
        assert_eq!(get_typlen(1).unwrap(), 0);
        assert!(get_typbyval(INT4OID).unwrap());
        assert!(!get_typbyval(1).unwrap());
        assert_eq!(get_typlenbyval(INT4OID).unwrap(), (4, true));
        assert_eq!(get_typlenbyvalalign(TEXTOID).unwrap(), (-1, false, TYPALIGN_INT));
        assert!(get_typlenbyval(1).is_err());
        assert_eq!(get_typstorage(TEXTOID).unwrap(), TYPSTORAGE_EXTENDED);
        assert_eq!(get_typstorage(1).unwrap(), TYPSTORAGE_PLAIN);
        assert_eq!(get_typalign(1).unwrap(), TYPALIGN_INT);
        assert_eq!(get_typcollation(TEXTOID).unwrap(), 100);
        assert!(type_is_collatable(TEXTOID).unwrap());
        assert!(!type_is_collatable(INT4OID).unwrap());
        assert!(get_typisdefined(INT4OID).unwrap());
        assert!(!get_typisdefined(SHELL_OID).unwrap());
        assert_eq!(get_typtype(DOMAIN_OID).unwrap(), typ::TYPTYPE_DOMAIN);
        assert!(type_is_rowtype(types_core::RECORDOID).unwrap());
        assert!(type_is_rowtype(COMPOSITE_OID).unwrap());
        assert!(!type_is_rowtype(INT4OID).unwrap());
        assert!(!type_is_enum(INT4OID).unwrap());
        assert!(!type_is_range(INT4OID).unwrap());
        assert!(!type_is_multirange(INT4OID).unwrap());
        assert_eq!(get_type_category_preferred(INT4OID).unwrap(), (b'N' as i8, true));
        assert_eq!(get_typ_typrelid(COMPOSITE_OID).unwrap(), 4242);
        assert_eq!(get_typ_typrelid(INT4OID).unwrap(), InvalidOid);
    });
}

#[test]
fn type_domain_and_array() {
    with_mcx(|_m| {
        let mut typmod = -1;
        assert_eq!(getBaseTypeAndTypmod(DOMAIN_OID, &mut typmod).unwrap(), TEXTOID);
        assert_eq!(typmod, 7);
        assert_eq!(getBaseType(TEXTOID).unwrap(), TEXTOID);
        assert_eq!(get_element_type(INT4_ARRAY).unwrap(), INT4OID);
        assert_eq!(get_element_type(INT4OID).unwrap(), InvalidOid);
        assert_eq!(get_array_type(INT4OID).unwrap(), INT4_ARRAY);
        assert_eq!(get_promoted_array_type(INT4OID).unwrap(), INT4_ARRAY);
        assert_eq!(get_promoted_array_type(INT4_ARRAY).unwrap(), INT4_ARRAY);
        assert_eq!(get_base_element_type(DOMAIN2_OID).unwrap(), INT4OID);
        assert_eq!(get_base_element_type(INT4OID).unwrap(), InvalidOid);
        assert_eq!(get_base_element_type(1).unwrap(), InvalidOid);
    });
}

#[test]
fn type_io() {
    with_mcx(|m| {
        assert_eq!(getTypeInputInfo(INT4OID).unwrap(), (42, INT4OID));
        assert_eq!(getTypeInputInfo(INT4_ARRAY).unwrap(), (750, INT4OID));
        assert_eq!(getTypeOutputInfo(INT4OID).unwrap(), (43, false));
        assert_eq!(getTypeBinaryInputInfo(INT4OID).unwrap(), (2406, INT4OID));
        assert_eq!(getTypeBinaryOutputInfo(INT4_ARRAY).unwrap(), (2401, true));
        let err = getTypeInputInfo(SHELL_OID).unwrap_err();
        assert!(err.message().contains("is only a shell"));
        let io = get_type_io_data(INT4_ARRAY, IOFuncSelector::IOFunc_output).unwrap();
        assert_eq!((io.typlen, io.typbyval, io.typioparam, io.func), (-1, false, INT4OID, 751));
        assert_eq!(get_typmodin(INT4OID).unwrap(), InvalidOid);
        assert_eq!(get_typmodout(1).unwrap(), InvalidOid);
        assert_eq!(get_typsubscript(INT4_ARRAY).unwrap(), (typ::F_ARRAY_SUBSCRIPT_HANDLER, INT4OID));
        assert!(getSubscriptingRoutines(INT4OID).unwrap().is_none());
        assert!(get_typdefault(m, INT4OID).unwrap().is_none());
        assert_eq!(get_typavgwidth(INT4OID, -1).unwrap(), 4);
        assert_eq!(get_typavgwidth(TEXTOID, -1).unwrap(), 32);
    });
}

#[test]
fn operator_getters() {
    with_mcx(|m| {
        assert_eq!(get_opcode(INT4_EQ).unwrap(), F_INT4EQ);
        assert_eq!(get_opcode(1).unwrap(), InvalidOid);
        assert_eq!(get_opname(m, INT4_EQ).unwrap().unwrap().as_str(), "=");
        assert!(get_opname(m, 1).unwrap().is_none());
        assert_eq!(get_op_rettype(INT4_EQ).unwrap(), BOOLOID);
        assert_eq!(op_input_types(INT4_EQ).unwrap(), (INT4OID, INT4OID));
        assert!(op_input_types(1).is_err());
        assert!(op_mergejoinable(INT4_EQ, INT4OID).unwrap());
        assert!(!op_mergejoinable(INT4_LT, INT4OID).unwrap());
        assert!(op_hashjoinable(INT4_EQ, INT4OID).unwrap());
        assert!(op_strict(INT4_EQ).unwrap());
        assert_eq!(op_volatile(INT4_EQ).unwrap(), b'i' as i8);
        assert!(op_strict(1).is_err());
        assert_eq!(get_commutator(INT4_EQ).unwrap(), INT4_EQ);
        assert_eq!(get_negator(INT4_EQ).unwrap(), 518);
        assert_eq!(get_oprrest(INT4_EQ).unwrap(), 101);
        assert_eq!(get_oprjoin(INT4_EQ).unwrap(), 105);
    });
}

#[test]
fn amop_getters() {
    with_mcx(|m| {
        assert!(op_in_opfamily(INT4_EQ, INT_BTREE_FAM).unwrap());
        assert!(!op_in_opfamily(999, INT_BTREE_FAM).unwrap());
        assert_eq!(get_op_opfamily_strategy(INT4_EQ, INT_BTREE_FAM).unwrap(), 3);
        assert_eq!(get_op_opfamily_strategy(999, INT_BTREE_FAM).unwrap(), 0);
        assert_eq!(get_op_opfamily_sortfamily(INT4_EQ, INT_BTREE_FAM).unwrap(), InvalidOid);
        assert_eq!(
            get_op_opfamily_properties(INT4_LT, INT_BTREE_FAM, false).unwrap(),
            (1, INT4OID, INT4OID)
        );
        assert!(get_op_opfamily_properties(999, INT_BTREE_FAM, false).is_err());
        assert_eq!(get_opfamily_member(INT_BTREE_FAM, INT4OID, INT4OID, 1).unwrap(), INT4_LT);
        assert_eq!(
            get_opfamily_member_for_cmptype(INT_BTREE_FAM, INT4OID, INT4OID, COMPARE_EQ).unwrap(),
            INT4_EQ
        );
        assert_eq!(
            get_ordering_op_properties(INT4_LT).unwrap(),
            Some((INT_BTREE_FAM, INT4OID, COMPARE_LT))
        );
        assert_eq!(get_ordering_op_properties(INT4_EQ).unwrap(), None);
        assert_eq!(
            get_equality_op_for_ordering_op(INT4_LT).unwrap(),
            Some((INT4_EQ, false))
        );
        assert_eq!(get_ordering_op_for_equality_op(INT4_EQ, true).unwrap(), INT4_LT);
        let fams = get_mergejoin_opfamilies(m, INT4_EQ).unwrap();
        assert_eq!(fams.as_slice(), &[INT_BTREE_FAM]);
        assert_eq!(get_compatible_hash_operators(INT4_EQ).unwrap(), Some((INT4_EQ, INT4_EQ)));
        assert_eq!(get_op_hash_functions(INT4_EQ).unwrap(), Some((450, 450)));
        let interp = get_op_index_interpretation(m, INT4_EQ).unwrap();
        assert_eq!(interp.len(), 1);
        assert_eq!(interp[0].opfamily_id, INT_BTREE_FAM);
        assert_eq!(interp[0].cmptype, COMPARE_EQ);
        assert!(equality_ops_are_compatible(INT4_EQ, INT4_EQ).unwrap());
        assert!(comparison_ops_are_compatible(INT4_EQ, INT4_LT).unwrap());
        assert_eq!(get_opfamily_proc(INT_HASH_FAM, INT4OID, INT4OID, 1).unwrap(), 450);
    });
}

#[test]
fn opclass_opfamily_getters() {
    with_mcx(|m| {
        assert_eq!(get_opclass_family(INT4_OPCLASS).unwrap(), INT_BTREE_FAM);
        assert_eq!(get_opclass_input_type(INT4_OPCLASS).unwrap(), INT4OID);
        assert_eq!(
            get_opclass_opfamily_and_input_type(INT4_OPCLASS).unwrap(),
            Some((INT_BTREE_FAM, INT4OID))
        );
        assert!(get_opclass_opfamily_and_input_type(1).unwrap().is_none());
        assert_eq!(get_opclass_method(INT4_OPCLASS).unwrap(), types_core::BTREE_AM_OID);
        assert!(get_opclass_family(1).is_err());
        assert_eq!(get_opfamily_method(INT_BTREE_FAM).unwrap(), types_core::BTREE_AM_OID);
        assert_eq!(
            get_opfamily_name(m, INT_BTREE_FAM, false).unwrap().unwrap().as_str(),
            "integer_ops"
        );
        assert!(get_opfamily_name(m, 1, true).unwrap().is_none());
    });
}

#[test]
fn function_getters() {
    with_mcx(|m| {
        assert_eq!(get_func_name(m, F_INT4EQ).unwrap().unwrap().as_str(), "int4eq");
        assert!(get_func_name(m, 1).unwrap().is_none());
        assert_eq!(get_func_namespace(F_INT4EQ).unwrap(), 11);
        assert_eq!(get_func_rettype(F_INT4EQ).unwrap(), BOOLOID);
        assert!(get_func_rettype(1).is_err());
        assert_eq!(get_func_nargs(F_INT4EQ).unwrap(), 2);
        let (ret, args) = get_func_signature(m, F_INT4EQ).unwrap();
        assert_eq!(ret, BOOLOID);
        assert_eq!(args.as_slice(), &[INT4OID, INT4OID]);
        assert_eq!(get_func_variadictype(F_INT4EQ).unwrap(), InvalidOid);
        assert!(!get_func_retset(F_INT4EQ).unwrap());
        assert!(func_strict(F_INT4EQ).unwrap());
        assert_eq!(func_volatile(F_INT4EQ).unwrap(), b'i' as i8);
        assert_eq!(func_parallel(F_INT4EQ).unwrap(), b's' as i8);
        assert_eq!(get_func_prokind(F_INT4EQ).unwrap(), b'f' as i8);
        assert!(get_func_leakproof(F_INT4EQ).unwrap());
        assert_eq!(get_func_support(F_INT4EQ).unwrap(), InvalidOid);
        assert_eq!(get_func_support(1).unwrap(), InvalidOid);
    });
}

#[test]
fn attribute_getters() {
    with_mcx(|m| {
        assert_eq!(get_attname(m, REL_OID, 1, false).unwrap().unwrap().as_str(), "id");
        assert!(get_attname(m, REL_OID, 9, true).unwrap().is_none());
        assert!(get_attname(m, REL_OID, 9, false).is_err());
        assert_eq!(get_attnum(REL_OID, "id").unwrap(), 1);
        assert_eq!(get_attnum(REL_OID, "nope").unwrap(), InvalidAttrNumber);
        assert_eq!(get_attgenerated(REL_OID, 1).unwrap(), 0);
        assert_eq!(get_atttype(REL_OID, 1).unwrap(), INT4OID);
        assert_eq!(get_atttype(REL_OID, 9).unwrap(), InvalidOid);
        assert_eq!(
            get_atttypetypmodcoll(REL_OID, 1).unwrap(),
            (INT4OID, -1, InvalidOid)
        );
        assert_eq!(get_attoptions(m, REL_OID, 1).unwrap(), datum::Datum::null());
        assert!(get_attoptions(m, REL_OID, 9).is_err());
    });
}

#[test]
fn relation_getters() {
    with_mcx(|m| {
        assert_eq!(get_relname_relid("t1", 2200).unwrap(), REL_OID);
        assert_eq!(get_relnatts(REL_OID).unwrap(), 3);
        assert_eq!(get_rel_name(m, REL_OID).unwrap().unwrap().as_str(), "t1");
        assert!(get_rel_name(m, 1).unwrap().is_none());
        assert_eq!(get_rel_namespace(REL_OID).unwrap(), 2200);
        assert_eq!(get_rel_type_id(REL_OID).unwrap(), COMPOSITE_OID);
        assert_eq!(get_rel_relkind(REL_OID).unwrap(), b'r' as i8);
        assert_eq!(get_rel_relkind(1).unwrap(), 0);
        assert!(!get_rel_relispartition(REL_OID).unwrap());
        assert_eq!(get_rel_tablespace(REL_OID).unwrap(), InvalidOid);
        assert_eq!(get_rel_persistence(REL_OID).unwrap(), b'p' as i8);
        assert!(get_rel_persistence(1).is_err());
        assert_eq!(get_rel_relam(REL_OID).unwrap(), 2);
        assert_eq!(get_index_column_opclass(IDX_OID, 1).unwrap(), INT4_OPCLASS);
        assert_eq!(get_index_column_opclass(IDX_OID, 2).unwrap(), InvalidOid);
        assert_eq!(get_index_column_opclass(1, 1).unwrap(), InvalidOid);
        assert!(!get_index_isreplident(IDX_OID).unwrap());
        assert!(get_index_isvalid(IDX_OID).unwrap());
        assert!(get_index_isvalid(1).is_err());
        assert!(!get_index_isclustered(IDX_OID).unwrap());
    });
}

#[test]
fn misc_getters() {
    with_mcx(|m| {
        assert_eq!(get_cast_oid(INT4OID, TEXTOID, false).unwrap(), 7777);
        let err = get_cast_oid(TEXTOID, INT4OID, false).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_OBJECT);
        assert_eq!(err.message(), "cast from type text to type integer does not exist");
        assert_eq!(get_cast_oid(TEXTOID, INT4OID, true).unwrap(), InvalidOid);
        assert_eq!(get_collation_name(m, 100).unwrap().unwrap().as_str(), "default");
        assert!(get_collation_isdeterministic(100).unwrap());
        assert!(get_collation_isdeterministic(1).is_err());
        assert_eq!(get_constraint_name(m, 8001).unwrap().unwrap().as_str(), "t1_pkey");
        assert_eq!(get_constraint_index(8001).unwrap(), IDX_OID);
        assert_eq!(get_constraint_index(8002).unwrap(), InvalidOid);
        assert_eq!(get_constraint_type(8002).unwrap(), misc::CONSTRAINT_FOREIGN);
        assert_eq!(get_language_name(m, 13, false).unwrap().unwrap().as_str(), "plpgsql");
        assert!(get_language_name(m, 1, true).unwrap().is_none());
        assert!(get_language_name(m, 1, false).is_err());
        assert_eq!(get_transform_fromsql(INT4OID, 13, &[INT4OID]).unwrap(), 9001);
        assert_eq!(get_transform_fromsql(INT4OID, 13, &[TEXTOID]).unwrap(), InvalidOid);
        assert_eq!(get_transform_tosql(INT4OID, 13, &[INT4OID]).unwrap(), 9002);
        assert_eq!(get_namespace_name(m, 2200).unwrap().unwrap().as_str(), "public");
        assert_eq!(get_range_subtype(3904).unwrap(), INT4OID);
        assert_eq!(get_range_subtype(1).unwrap(), InvalidOid);
        assert_eq!(get_range_collation(3904).unwrap(), InvalidOid);
        assert_eq!(get_range_multirange(3904).unwrap(), 4451);
        assert_eq!(get_multirange_range(4451).unwrap(), 3904);
        assert_eq!(get_publication_oid("pub1", false).unwrap(), 6001);
        assert!(get_publication_oid("nope", false).is_err());
        assert_eq!(get_publication_name(m, 6001, false).unwrap().unwrap().as_str(), "pub1");
        init_small::globals::SetMyDatabaseId(1);
        assert_eq!(get_subscription_oid("sub1", false).unwrap(), 6002);
        assert_eq!(get_subscription_name(m, 6002, false).unwrap().unwrap().as_str(), "sub1");
    });
}

#[test]
fn statistics_getters() {
    with_mcx(|m| {
        assert_eq!(get_attavgwidth(REL_OID, 1).unwrap(), 4);
        assert_eq!(get_attavgwidth(1, 1).unwrap(), 0);
        let prev = set_get_attavgwidth_hook(Some(|_relid, _attnum| 99));
        assert_eq!(get_attavgwidth(REL_OID, 1).unwrap(), 99);
        set_get_attavgwidth_hook(prev);
        assert_eq!(get_attavgwidth(REL_OID, 1).unwrap(), 4);

        let image = [0u64; 8];
        // SAFETY: dummy aligned image, larger than the fixed header; the
        // mocked pg_statistic_slot_shape seam never dereferences it.
        let tuple = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                image.as_ptr().cast(),
                core::mem::size_of_val(&image) as u32,
                Default::default(),
                InvalidOid,
            )
        };
        let slot = get_attstatsslot(m, &tuple, 1, InvalidOid, 0).unwrap().unwrap();
        assert_eq!(slot.staop, INT4_EQ);
        assert!(slot.values.is_empty());
        let slot2 = get_attstatsslot(m, &tuple, 2, INT4_LT, 0).unwrap().unwrap();
        assert_eq!(slot2.staop, INT4_LT);
        assert!(get_attstatsslot(m, &tuple, 7, InvalidOid, 0).unwrap().is_none());
        free_attstatsslot(slot);
    });
}

#[test]
fn init_seams_installs() {
    install();
    static INIT: Once = Once::new();
    INIT.call_once(super::init_seams);
    assert_eq!(lsyscache_seams::get_type_output_info::call(INT4OID).unwrap(), (43, false));
    assert_eq!(
        lsyscache_seams::get_type_binary_output_info::call(INT4_ARRAY).unwrap(),
        (2401, true)
    );
    assert_eq!(
        lsyscache_seams::get_base_type_and_typmod::call(DOMAIN_OID, -1).unwrap(),
        (TEXTOID, 7)
    );
}
