use std::sync::Once;

use datum::Datum;
use mcx::{alloc_leak_in, Mcx, MemoryContext};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeTag};
use types_portal::{ParamListHandle, CURSOR_OPT_PARALLEL_OK};
use types_tuple::PgTypeShape;

use crate::planner;

// Serializes tests that flip or observe planner strategy GUCs.
pub(crate) static GUC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn install_fixtures() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::pg_class_aclmask::set(|_, _, mask, _| Ok(mask));
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                23 => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }),
                20 => Some(PgTypeShape {
                    typlen: 8,
                    typbyval: true,
                    typalign: b'd' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }),
                25 => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typstorage: b'x' as i8,
                    typcollation: 100,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_type_typtype::set(|typid| {
            Ok(matches!(typid, 16 | 20 | 23 | 25).then_some(b'b' as i8))
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            // (typinput, typoutput) = int4in/out, int8in/out.
            let (typlen, typbyval, typalign, io) = match typid {
                23 => (4i16, true, b'i' as i8, (42, 43)),
                20 => (8i16, true, b'd' as i8, (460, 461)),
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgTypeIoShape {
                oid: typid,
                typinput: io.0,
                typoutput: io.1,
                typreceive: 0,
                typsend: 0,
                typmodin: 0,
                typmodout: 0,
                typelem: 0,
                typlen,
                typbyval,
                typalign,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        install_scan_fixtures();
    });
}

const TBL: u32 = 16384;
const IDX: u32 = 16385;
const JT1: u32 = 16400;
const JT2: u32 = 16401;
const JT3: u32 = 16402;
const JT4: u32 = 16403;
const STT: u32 = 16410;
const PTT: u32 = 16411;
// Torn-stats fixtures (GL-STATSLOT-1): slot kinds recorded at bundle-probe
// time paired with arrays from a later row generation — the MCV slot carries
// values but an EMPTY numbers array, the shape a concurrent ANALYZE rewrite
// produces through the lazy per-slot image re-probe (RWREG-2 rung b2/post-7
// panic evidence).
const TORN: u32 = 16412;
const TORN2: u32 = 16413;
const INT4EQ_OP: u32 = 96;
const INT4_LT_OP: u32 = 97;
const INT4_GT_OP: u32 = 521;
const INT8GT_OP: u32 = 413;
const INT4EQ_PROC: u32 = 65;
const INT4_BTREE_FAM: u32 = 1976;
const TEXTEQ_OP: u32 = 98;
const TEXT_LT_OP: u32 = 664;
const TEXT_GE_OP: u32 = 667;
const TEXT_REGEXEQ_OP: u32 = 641;
const TEXT_BTREE_FAM: u32 = 1994;
// Not a real catalog OID: CREATE AGGREGATE first_val(anyelement) (sfunc=...,
// stype=anyelement) for the polymorphic aggtranstype resolution test.
const FIRST_VAL_ANYELEMENT: u32 = 9999;

fn text_datum(mcx: Mcx<'_>, s: &str) -> Datum {
    let image = varlena::cstring_to_text(mcx, s.as_bytes()).unwrap().into_image();
    Datum::from_usize(image.leak().as_ptr() as usize)
}

fn install_scan_fixtures() {
    adt_regexp::init_seams();
    syscache_seams::pg_type_typnamespace::set(|_| Ok(Some(11)));
    syscache_seams::pg_type_element_shape::set(|typid| {
        Ok(matches!(typid, 1007 | 1009).then(|| syscache_seams::PgTypeElementShape {
            typelem: if typid == 1007 { 23 } else { 25 },
            typsubscript: lsyscache::F_ARRAY_SUBSCRIPT_HANDLER,
        }))
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        let shape = |rettype, nargs, kind: u8, strict| syscache_seams::PgProcShape {
            prolang: 12,
            prosecdef: false,
            proconfig_isnull: true,
            pronamespace: 11,
            prorettype: rettype,
            provariadic: 0,
            prosupport: 0,
            pronargs: nargs,
            prokind: kind as i8,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: strict,
            proleakproof: false,
        };
        Ok(match funcid {
            177 => Some(shape(23, 2, b'f', true)),
            65 => Some(shape(16, 2, b'f', true)),
            66 => Some(shape(16, 2, b'f', true)),
            470 => Some(shape(16, 2, b'f', true)),
            // texteq / text_lt / text_ge / textregexeq.
            67 | 740 | 743 | 1254 => Some(shape(16, 2, b'f', true)),
            // pg_proc.dat rows for the plain-agg lane (agg::* tests).
            2803 => Some(shape(20, 0, b'a', false)),
            3100 | 3101 => Some(shape(20, 0, b'w', false)),
            2108 => Some(shape(20, 1, b'a', false)),
            1219 => Some(shape(20, 1, b'f', true)),
            1841 => Some(shape(20, 2, b'f', false)),
            // max(int4)/min(int4) + int4larger/int4smaller (minmax lane).
            2116 | 2132 => Some(shape(23, 1, b'a', false)),
            768 | 769 => Some(shape(23, 2, b'f', true)),
            // generate_series(int4,int4).
            1067 => Some(syscache_seams::PgProcShape { proretset: true, ..shape(23, 2, b'f', true) }),
            // first_val_anyelement(anyelement) shell fn + its sfunc (agg::* lane).
            FIRST_VAL_ANYELEMENT => Some(shape(2283, 1, b'a', false)),
            9998 => Some(shape(2283, 2, b'f', false)),
            _ => None,
        })
    });
    clauses_seams::evaluate_expr::set(|_, _, _, _, _| panic!("evaluate_expr not exercised"));
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        Ok(match opno {
            INT4EQ_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: INT4EQ_OP,
                oprnegate: 518,
                oprcode: INT4EQ_PROC,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }),
            INT4_LT_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: 521,
                oprnegate: 525,
                oprcode: 66,
                oprrest: 103,
                oprjoin: 107,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            INT4_GT_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: INT4_LT_OP,
                oprnegate: 523,
                oprcode: 147,
                oprrest: 104,
                oprjoin: 108,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            TEXTEQ_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 25,
                oprright: 25,
                oprresult: 16,
                oprcom: TEXTEQ_OP,
                oprnegate: 531,
                oprcode: 67,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }),
            TEXT_LT_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 25,
                oprright: 25,
                oprresult: 16,
                oprcom: 666,
                oprnegate: 667,
                oprcode: 740,
                oprrest: 103,
                oprjoin: 107,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            TEXT_GE_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 25,
                oprright: 25,
                oprresult: 16,
                oprcom: 665,
                oprnegate: 664,
                oprcode: 743,
                oprrest: 337,
                oprjoin: 398,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            TEXT_REGEXEQ_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 25,
                oprright: 25,
                oprresult: 16,
                oprcom: 0,
                oprnegate: 642,
                oprcode: 1254,
                oprrest: 1818,
                oprjoin: 1824,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            // int8 > int8 (HAVING-lane tests).
            INT8GT_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 20,
                oprright: 20,
                oprresult: 16,
                oprcom: 412,
                oprnegate: 414,
                oprcode: 470,
                oprrest: 104,
                oprjoin: 108,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
        Ok(if purpose == b's' && opfamily == INT4_BTREE_FAM {
            match opno {
                INT4EQ_OP => Some(syscache_seams::PgAmopShape {
                    amopstrategy: 3,
                    amopsortfamily: 0,
                    amoplefttype: 23,
                    amoprighttype: 23,
                }),
                INT4_LT_OP => Some(syscache_seams::PgAmopShape {
                    amopstrategy: 1,
                    amopsortfamily: 0,
                    amoplefttype: 23,
                    amoprighttype: 23,
                }),
                INT4_GT_OP => Some(syscache_seams::PgAmopShape {
                    amopstrategy: 5,
                    amopsortfamily: 0,
                    amoplefttype: 23,
                    amoprighttype: 23,
                }),
                _ => None,
            }
        } else if matches!(opno, TEXTEQ_OP | TEXT_LT_OP | TEXT_GE_OP)
            && purpose == b's'
            && opfamily == TEXT_BTREE_FAM
        {
            Some(syscache_seams::PgAmopShape {
                amopstrategy: match opno {
                    TEXTEQ_OP => 3,
                    TEXT_LT_OP => 1,
                    _ => 4,
                },
                amopsortfamily: 0,
                amoplefttype: 25,
                amoprighttype: 25,
            })
        } else {
            None
        })
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
        let mut v = mcx::PgVec::new_in(mcx);
        if opno == INT4EQ_OP || opno == INT4_LT_OP || opno == INT4_GT_OP {
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: INT4_BTREE_FAM,
                amoplefttype: 23,
                amoprighttype: 23,
                amopstrategy: match opno {
                    INT4EQ_OP => 3,
                    INT4_LT_OP => 1,
                    _ => 5,
                },
                amopmethod: 403,
            });
        }
        if matches!(opno, TEXTEQ_OP | TEXT_LT_OP | TEXT_GE_OP) {
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: TEXT_BTREE_FAM,
                amoplefttype: 25,
                amoprighttype: 25,
                amopstrategy: match opno {
                    TEXTEQ_OP => 3,
                    TEXT_LT_OP => 1,
                    _ => 4,
                },
                amopmethod: 403,
            });
        }
        Ok(v)
    });
    syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
        Ok((opfid == INT4_BTREE_FAM).then(|| syscache_seams::PgOpfamilyShape {
            opfmethod: 403,
            opfname: types_tuple::NameData::default(),
        }))
    });
    syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
        Ok(match (opfamily, left, right, strategy) {
            (INT4_BTREE_FAM, 23, 23, 3) => INT4EQ_OP,
            (INT4_BTREE_FAM, 23, 23, 1) => INT4_LT_OP,
            _ => 0,
        })
    });
    syscache_seams::pg_proc_cost_shape::set(|funcid| {
        Ok(match funcid {
            INT4EQ_PROC | 66 | 1219 | 1841 | 470 | 2108 | 768 | 769 | 147 | 67 | 740 | 742
            | 743 | 1254 | 177 | 9998 | 2803 => {
                Some(syscache_seams::PgProcCostShape { procost: 1.0, prorows: 0.0, prosupport: 0 })
            }
            // generate_series(int4,int4): prosupport row estimation is not
            // exercised (Const-args support fn lives in adt).
            1067 => Some(syscache_seams::PgProcCostShape {
                procost: 1.0,
                prorows: 1000.0,
                prosupport: 0,
            }),
            // row_number/rank/dense_rank carry live prosupport rows; the
            // support fns return NULL for SupportRequestCost (adt_windowfuncs).
            3100 => Some(syscache_seams::PgProcCostShape {
                procost: 1.0,
                prorows: 0.0,
                prosupport: 6233,
            }),
            3101 => Some(syscache_seams::PgProcCostShape {
                procost: 1.0,
                prorows: 0.0,
                prosupport: 6234,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
        // pg_aggregate.dat rows for count() / sum(int4).
        let shape = |transfn| syscache_seams::PgAggregateShape {
            aggkind: b'n' as i8,
            aggnumdirectargs: 0,
            aggtransfn: transfn,
            aggfinalfn: 0,
            aggcombinefn: 463,
            aggserialfn: 0,
            aggdeserialfn: 0,
            aggmtransfn: 0,
            aggminvtransfn: 0,
            aggmfinalfn: 0,
            aggfinalextra: false,
            aggmfinalextra: false,
            aggfinalmodify: b'r' as i8,
            aggmfinalmodify: b'r' as i8,
            aggsortop: 0,
            aggtranstype: 20,
            aggtransspace: 0,
            aggmtranstype: 0,
        };
        Ok(match aggfnoid {
            2803 => Some(shape(1219)),
            2108 => Some(shape(1841)),
            2116 => Some(syscache_seams::PgAggregateShape {
                aggtransfn: 768,
                aggsortop: INT4_GT_OP,
                aggtranstype: 23,
                aggcombinefn: 768,
                ..shape(768)
            }),
            2132 => Some(syscache_seams::PgAggregateShape {
                aggtransfn: 769,
                aggsortop: INT4_LT_OP,
                aggtranstype: 23,
                aggcombinefn: 769,
                ..shape(769)
            }),
            // first_val_anyelement(anyelement): STYPE=anyelement, aggtranstype
            // resolves against the call's actual arg type (resolve_aggregate_transtype).
            FIRST_VAL_ANYELEMENT => Some(syscache_seams::PgAggregateShape {
                aggtranstype: types_core::catalog::ANYELEMENTOID,
                ..shape(9998)
            }),
            _ => None,
        })
    });
    syscache_seams::pg_aggregate_agginitval::set(|mcx, aggfnoid| {
        Ok(match aggfnoid {
            2803 => Some(Some(mcx::PgString::from_str_in("0", mcx)?)),
            2108 | 2116 | 2132 | FIRST_VAL_ANYELEMENT => Some(None),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_proc_signature::set(|mcx, funcid| {
        Ok(match funcid {
            FIRST_VAL_ANYELEMENT => {
                let mut declared = mcx::PgVec::new_in(mcx);
                declared.push(types_core::catalog::ANYELEMENTOID);
                Some((types_core::catalog::ANYELEMENTOID, declared))
            }
            _ => None,
        })
    });
    syscache_seams::lookup_pg_statistic_shape::set(|_, _, _| Ok(None));
    // Typcache fixtures for scalararraysel + check_memoizable: int4/text
    // pg_type rows, no default opclass (eq_opr resolution stays invalid,
    // containment skipped, hasheqoperator stays unset).
    syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
        Ok(match typid {
            23 => Some(syscache_seams::PgTypeTypcacheShape {
                typname: types_tuple::NameData::default(),
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: 0,
                typsubscript: 0,
                typelem: 0,
                typarray: 1007,
                typcollation: 0,
            }),
            25 => Some(syscache_seams::PgTypeTypcacheShape {
                typname: types_tuple::NameData::default(),
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: 0,
                typsubscript: 0,
                typelem: 0,
                typarray: 1009,
                typcollation: 100,
            }),
            _ => None,
        })
    });
    syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid));
    indexcmds_seams::get_default_opclass::set(|_, _| Ok(0));
    syscache_seams::pg_type_base_shape::set(|typid| {
        Ok(match typid {
            // True array types carry the array subscript handler; without it
            // get_base_element_type (scalararraysel's C element-type probe)
            // sees no array.
            1007 | 1009 => Some(syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: 0,
                typtypmod: -1,
                typelem: if typid == 1007 { 23 } else { 25 },
                typsubscript: lsyscache::F_ARRAY_SUBSCRIPT_HANDLER,
            }),
            _ => Some(syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: 0,
                typtypmod: -1,
                typelem: 0,
                typsubscript: 0,
            }),
        })
    });
    // STT carries a pinned stats fixture (MCV [1->0.30, 2->0.20], histogram
    // [0,10,20,30,40], stadistinct 10); PTT the text twin (MCV ["bar"->0.20,
    // "foo"->0.10], 5-entry histogram); everything else has no stats row.
    syscache_seams::lookup_pg_statistic_bundle::set(|mcx, relid, attnum, inh| {
        if relid == PTT && attnum == 1 && !inh {
            let mut slots = mcx::PgVec::new_in(mcx);
            let mut mcv_values = mcx::PgVec::new_in(mcx);
            mcv_values.extend([text_datum(mcx, "bar"), text_datum(mcx, "foo")]);
            let mut mcv_numbers = mcx::PgVec::new_in(mcx);
            mcv_numbers.extend([0.20f32, 0.10f32]);
            slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
                1,
                TEXTEQ_OP,
                950,
                25,
                mcv_values,
                mcv_numbers,
                mcx::PgVec::new_in(mcx),
            ));
            let mut hist_values = mcx::PgVec::new_in(mcx);
            hist_values.extend(
                ["apple", "dog", "foo", "milk", "zebra"].map(|s| text_datum(mcx, s)),
            );
            slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
                2,
                TEXT_LT_OP,
                950,
                25,
                hist_values,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
            ));
            return Ok(Some(syscache_seams::PgStatisticBundle {
                stanullfrac: 0.0,
                stawidth: 8,
                stadistinct: 10.0,
                slots,
            }));
        }
        // TBL.pk carries a degenerate CORRELATION slot with an EMPTY numbers
        // array — the exact shape the RWREG-2 rung b2 backend panics hit at
        // selfuncs.rs:2708 (stock-latent, fixed at t47 by 3e606afd9). Every
        // btcostestimate over TBL now traverses the guarded read; C-parity
        // (btcostestimate reads numbers[0] only under nnumbers > 0) leaves
        // index_correlation at its 0.0 default, so all existing plan/cost
        // expectations are unchanged.
        if relid == TBL && attnum == 1 && !inh {
            let mut slots = mcx::PgVec::new_in(mcx);
            slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
                3,
                INT4_LT_OP,
                0,
                23,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
            ));
            return Ok(Some(syscache_seams::PgStatisticBundle {
                stanullfrac: 0.0,
                stawidth: 4,
                stadistinct: 0.0,
                slots,
            }));
        }
        // TORN/TORN2: MCV slot with values but an EMPTY numbers array (torn),
        // plus a well-formed histogram — drives the unguarded numbers[i]
        // reads in var_eq_const / mcv_selectivity / eqjoinsel.
        if (relid == TORN || relid == TORN2) && attnum == 1 && !inh {
            let mut slots = mcx::PgVec::new_in(mcx);
            let mut mcv_values = mcx::PgVec::new_in(mcx);
            mcv_values.extend([datum::Datum::from_i32(1), datum::Datum::from_i32(2)]);
            slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
                1,
                INT4EQ_OP,
                0,
                23,
                mcv_values,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
            ));
            let mut hist_values = mcx::PgVec::new_in(mcx);
            hist_values.extend([0i32, 10, 20, 30, 40].map(datum::Datum::from_i32));
            slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
                2,
                INT4_LT_OP,
                0,
                23,
                hist_values,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
            ));
            return Ok(Some(syscache_seams::PgStatisticBundle {
                stanullfrac: 0.0,
                stawidth: 4,
                stadistinct: 10.0,
                slots,
            }));
        }
        if relid != STT || attnum != 1 || inh {
            return Ok(None);
        }
        let mut slots = mcx::PgVec::new_in(mcx);
        let mut mcv_values = mcx::PgVec::new_in(mcx);
        mcv_values.extend([datum::Datum::from_i32(1), datum::Datum::from_i32(2)]);
        let mut mcv_numbers = mcx::PgVec::new_in(mcx);
        mcv_numbers.extend([0.30f32, 0.20f32]);
        slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
            1,
            INT4EQ_OP,
            0,
            23,
            mcv_values,
            mcv_numbers,
            mcx::PgVec::new_in(mcx),
        ));
        let mut hist_values = mcx::PgVec::new_in(mcx);
        hist_values.extend([0i32, 10, 20, 30, 40].map(datum::Datum::from_i32));
        slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
            2,
            INT4_LT_OP,
            0,
            23,
            hist_values,
            mcx::PgVec::new_in(mcx),
            mcx::PgVec::new_in(mcx),
        ));
        Ok(Some(syscache_seams::PgStatisticBundle {
            stanullfrac: 0.0,
            stawidth: 4,
            stadistinct: 10.0,
            slots,
        }))
    });
    syscache_seams::pg_statistic_stawidth::set(|_, _, _| Ok(None));
    relation_seams::relation_open::set(|mcx, relid, _lockmode| {
        Ok(match relid {
            TBL => make_heap_rel(mcx),
            IDX => make_index_rel(mcx),
            JT1 => make_join_rel_fixture(mcx, JT1, "jt1", 1, 1.0),
            JT2 => make_join_rel_fixture(mcx, JT2, "jt2", 1, 2.0),
            JT3 => make_join_rel_fixture(mcx, JT3, "jt3", 100, 10000.0),
            JT4 => make_join_rel_fixture(mcx, JT4, "jt4", 100, 10000.0),
            STT => make_join_rel_fixture(mcx, STT, "stt", 10, 1000.0),
            TORN => make_join_rel_fixture(mcx, TORN, "torn", 10, 1000.0),
            TORN2 => make_join_rel_fixture(mcx, TORN2, "torn2", 10, 1000.0),
            PTT => make_text_rel_fixture(mcx, PTT, "ptt", 10, 1000.0),
            other => panic!("fixture relation_open: unknown oid {other}"),
        })
    });
    // Real relcache path: the index list comes off relcache's rd_indexlist
    // cache, fed by pg_class/pg_index fixtures underneath its build seams.
    relcache_build_seams::scan_pg_relation::set(|relid, _, _| {
        Ok((relid == TBL).then(|| relcache_build_seams::ScannedPgClass {
            relchecks: 0, relhastriggers: false, relhasrules: false,
            form: make_pg_class(TBL, "t", b'r', 2, true),
            options: None,
        }))
    });
    relcache_build_seams::relation_build_tuple_desc::set(|mcx, _, _, _| {
        Ok(std::rc::Rc::new(types_tuple::TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: mcx::PgVec::new_in(mcx),
            attrs: mcx::PgVec::new_in(mcx),
        }))
    });
    relcache_build_seams::scan_pg_index_shapes::set(|mcx, indrelid| {
        let mut v = mcx::PgVec::new_in(mcx);
        if indrelid == TBL {
            v.push(relcache_build_seams::PgIndexListShape {
                indexrelid: IDX,
                indislive: true,
                indisunique: true,
                indisprimary: true,
                indimmediate: true,
                indisvalid: true,
                indisreplident: false,
                has_indpred: false,
            });
        }
        Ok(v)
    });
    relcache_seams::relation_get_index_list::set(relcache::RelationGetIndexList);
    relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(mcx::PgVec::new_in(mcx)));
    relcache_seams::relation_get_fkey_list::set(|_relid| Ok(std::rc::Rc::from(Vec::new())));
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
        Ok(match rel.rd_id {
            TBL => 100,
            IDX => 30,
            JT1 | JT2 => 1,
            JT3 | JT4 => 100,
            STT | PTT | TORN | TORN2 => 10,
            other => panic!("fixture nblocks: unknown oid {other}"),
        })
    });
}

fn make_pg_class(
    oid: u32,
    name: &str,
    relkind: u8,
    relam: u32,
    relhasindex: bool,
) -> types_rel::FormData_pg_class {
    let mut relname = types_tuple::NameData::default();
    relname.namestrcpy(name);
    types_rel::FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 100,
        reltuples: 10000.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex,
        relisshared: false,
        relpersistence: b'p',
        relkind,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: types_rel::REPLICA_IDENTITY_DEFAULT,
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    }
}

fn make_rel_data<'mcx>(
    mcx: Mcx<'mcx>,
    oid: u32,
    rd_rel: types_rel::FormData_pg_class,
    rd_att: std::rc::Rc<types_tuple::TupleDescData<'mcx>>,
) -> types_rel::RelationData<'mcx> {
    use std::cell::Cell;
    types_rel::RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: types_rel::LockInfoData {
            lockRelId: types_rel::LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel,
        rd_att,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

fn int4_attr(attnum: i16, name: &str, notnull: bool) -> types_tuple::FormData_pg_attribute {
    let mut attname = types_tuple::NameData::default();
    attname.namestrcpy(name);
    types_tuple::FormData_pg_attribute {
        attrelid: TBL,
        attname,
        atttypid: 23,
        attlen: 4,
        attnum,
        atttypmod: -1,
        attndims: 0,
        attbyval: true,
        attalign: b'i' as i8,
        attstorage: b'p' as i8,
        attcompression: 0,
        attnotnull: notnull,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: 0,
        attisdropped: false,
        attislocal: true,
        attinhcount: 0,
        attcollation: 0,
    }
}

// Index-less two-int-column relation for the nestloop lane; pages/tuples
// pinned so costs match a live-PG fixture (1 page, reltuples rows, VACUUMed,
// never ANALYZEd).
fn make_join_rel_fixture<'mcx>(
    mcx: Mcx<'mcx>,
    oid: u32,
    name: &str,
    pages: i32,
    tuples: f32,
) -> types_rel::Relation<'mcx> {
    use types_tuple::tupdesc::ATTNULLABLE_UNRESTRICTED;
    let mut attrs = mcx::PgVec::new_in(mcx);
    attrs.push(int4_attr(1, "a", false));
    attrs.push(int4_attr(2, "pad", false));
    let mut compact_attrs = mcx::PgVec::new_in(mcx);
    for a in attrs.iter() {
        let mut c = types_tuple::CompactAttribute::populate_from(a);
        c.attnullability = ATTNULLABLE_UNRESTRICTED;
        compact_attrs.push(c);
    }
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs,
        attrs,
    });
    let mut form = make_pg_class(oid, name, b'r', 2, false);
    form.relpages = pages;
    form.reltuples = tuples;
    types_rel::Relation::open(make_rel_data(mcx, oid, form, rd_att), None)
}

fn make_text_rel_fixture<'mcx>(
    mcx: Mcx<'mcx>,
    oid: u32,
    name: &str,
    pages: i32,
    tuples: f32,
) -> types_rel::Relation<'mcx> {
    use types_tuple::tupdesc::ATTNULLABLE_UNRESTRICTED;
    let mut attr = int4_attr(1, "t", false);
    attr.atttypid = 25;
    attr.attlen = -1;
    attr.attbyval = false;
    attr.attalign = b'i' as i8;
    attr.attstorage = b'x' as i8;
    attr.attcollation = 950;
    let mut attrs = mcx::PgVec::new_in(mcx);
    attrs.push(attr);
    let mut compact_attrs = mcx::PgVec::new_in(mcx);
    for a in attrs.iter() {
        let mut c = types_tuple::CompactAttribute::populate_from(a);
        c.attnullability = ATTNULLABLE_UNRESTRICTED;
        compact_attrs.push(c);
    }
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs,
        attrs,
    });
    let mut form = make_pg_class(oid, name, b'r', 2, false);
    form.relpages = pages;
    form.reltuples = tuples;
    types_rel::Relation::open(make_rel_data(mcx, oid, form, rd_att), None)
}

fn make_heap_rel<'mcx>(mcx: Mcx<'mcx>) -> types_rel::Relation<'mcx> {
    use types_tuple::tupdesc::{ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID};
    let mut attrs = mcx::PgVec::new_in(mcx);
    attrs.push(int4_attr(1, "pk", true));
    attrs.push(int4_attr(2, "val", false));
    let mut compact_attrs = mcx::PgVec::new_in(mcx);
    for a in attrs.iter() {
        let mut c = types_tuple::CompactAttribute::populate_from(a);
        c.attnullability =
            if a.attnotnull { ATTNULLABLE_VALID } else { ATTNULLABLE_UNRESTRICTED };
        compact_attrs.push(c);
    }
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs,
        attrs,
    });
    types_rel::Relation::open(
        make_rel_data(mcx, TBL, make_pg_class(TBL, "t", b'r', 2, true), rd_att),
        None,
    )
}

fn make_index_rel<'mcx>(mcx: Mcx<'mcx>) -> types_rel::Relation<'mcx> {
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    });
    let mut data = make_rel_data(mcx, IDX, make_pg_class(IDX, "t_pkey", b'i', 403, false), rd_att);
    let mut indkey = mcx::PgVec::new_in(mcx);
    indkey.push(1i16);
    data.rd_index = Some(types_rel::FormData_pg_index {
        indexrelid: IDX,
        indrelid: TBL,
        indnatts: 1,
        indnkeyatts: 1,
        indisunique: true,
        indnullsnotdistinct: false,
        indisprimary: true,
        indisexclusion: false,
        indimmediate: true,
        indisvalid: true,
        indisready: true,
        indkey,
        has_indpred: false,
        indexprs_src: None,
        indpred_src: None,
    });
    data.rd_opfamily.push(INT4_BTREE_FAM);
    data.rd_opcintype.push(23);
    data.rd_indoption.push(0);
    data.rd_indcollation.push(0);
    data.rd_amcache.set(Some(types_nbtree::page::BTMetaPageData {
        btm_magic: types_nbtree::page::BTREE_MAGIC,
        btm_version: types_nbtree::page::BTREE_VERSION,
        btm_root: 3,
        btm_level: 1,
        btm_fastroot: 3,
        btm_fastlevel: 1,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: true,
    }));
    types_rel::Relation::open(data, None)
}

// The analyzer's output for `SELECT * FROM t [WHERE <quals>]`.
fn table_query<'mcx>(mcx: Mcx<'mcx>, quals: Option<Node<'mcx>>) -> Query<'mcx> {
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = TBL;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    rte.inh = false;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals },
    )
    .unwrap();
    let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
    let tle1 = Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, val, 2, Some("val"), false).unwrap();
    let mut target_list = NodeList::make1(mcx, tle1).unwrap();
    target_list.lappend(mcx, tle2).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: target_list,
        stmt_location: 0,
        stmt_len: 30,
        ..Query::default()
    }
}

fn eq_qual<'mcx>(mcx: Mcx<'mcx>, attno: i16, value: i32) -> Node<'mcx> {
    let var = Node::mk_var(mcx, 1, attno, 23, -1, 0, 0).unwrap();
    let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(value), false, true).unwrap();
    Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: INT4EQ_OP,
            opfuncid: INT4EQ_PROC,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, konst).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

// tests: planner entry now takes the Query by arena reference.
fn leak_q<'mcx>(mcx: Mcx<'mcx>, q: types_nodes::parsenodes::Query<'mcx>) -> &'mcx mut types_nodes::parsenodes::Query<'mcx> {
    mcx::leak_in(mcx::alloc_in(mcx, q).unwrap())
}

#[test]
fn point_select_plans_to_index_scan() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 1, 42)));
    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "SELECT * FROM t WHERE pk = 42",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.rtable.len(), 1);
    assert_eq!(stmt.relationOids.len(), 1);

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_IndexScan);
    let iscan = plan.as_index_scan().unwrap();
    assert_eq!(iscan.scan.scanrelid, 1);
    assert_eq!(iscan.indexid, IDX);
    assert_eq!(iscan.indexorderdir, 1);
    assert_eq!(iscan.scan.plan.plan_rows, 1.0);
    assert_eq!(iscan.scan.plan.plan_width, 8);
    assert!(iscan.scan.plan.qual.is_nil());
    assert_eq!(iscan.scan.plan.plan_node_id, 0);

    // EXPLAIN: Index Scan using t_pkey on t (cost=0.29..8.30 rows=1 width=8)
    // over 100 heap pages / 10000 tuples / 30 index pages / tree height 1.
    assert!((iscan.scan.plan.startup_cost - 0.285).abs() < 1e-9);
    assert!((iscan.scan.plan.total_cost - 8.3025).abs() < 1e-9);

    // indexqual carries the INDEX_VAR-rewritten copy; indexqualorig the
    // original table Var.
    assert_eq!(iscan.indexqual.len(), 1);
    let fixed = iscan.indexqual.nth(0).as_op_expr().unwrap();
    let fixed_var = fixed.args.nth(0).as_var().unwrap();
    assert_eq!(fixed_var.varno, -3);
    assert_eq!(fixed_var.varattno, 1);
    assert_eq!(iscan.indexqualorig.len(), 1);
    let orig = iscan.indexqualorig.nth(0).as_op_expr().unwrap();
    assert_eq!(orig.args.nth(0).as_var().unwrap().varno, 1);
    assert_eq!(orig.args.nth(1).as_const().unwrap().constvalue.as_i32(), 42);

    assert_eq!(iscan.scan.plan.targetlist.len(), 2);
    let tle = iscan.scan.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resname, Some("pk"));
    assert_eq!(tle.expr.as_var().unwrap().varattno, 1);
}

#[test]
fn bitmap_heap_path_plans_to_bitmap_scan_nodes() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 1, 42)));
    let mut run = crate::run::PlannerRun::new(mcx);
    crate::subquery::subquery_planner(&mut run, leak_q(mcx, parse), false, 0.0, None).unwrap();
    let final_rel = crate::planmain::fetch_final_rel(&mut run);
    // The bitmap heap path was generated but is dominated by the plain index
    // scan (as C); rebuild one over the surviving index path to plan it.
    let ipath = run.root.rel(final_rel).cheapest_total_path.unwrap();
    assert!(matches!(run.root.path(ipath), types_pathnodes::PathNode::IndexPath(_)));
    let (index_total, index_scan_total) = {
        let types_pathnodes::PathNode::IndexPath(ip) = run.root.path(ipath) else {
            unreachable!()
        };
        (ip.indextotalcost, ip.path.total_cost)
    };
    let baserel = run.root.path(ipath).base().parent;
    let bpath =
        crate::pathnode::create_bitmap_heap_path(&mut run, baserel, ipath, &crate::relnode::RELIDS_UNSET, 1.0, 0).unwrap();

    // Exact C arithmetic over the fixture (100 pages / 10000 tuples, one
    // matching row): tree cost = indextotalcost + 0.1*cpu_operator_cost*1;
    // one heap page at random_page_cost; cpu = cpu_tuple_cost + 0.0025.
    let tree_cost = index_total + 0.1 * 0.0025;
    let b = run.root.path(bpath).base();
    assert!((b.startup_cost - tree_cost).abs() < 1e-9, "startup {}", b.startup_cost);
    assert!(
        (b.total_cost - (tree_cost + 4.0 + 0.01 + 0.0025)).abs() < 1e-9,
        "total {}",
        b.total_cost
    );
    assert_eq!(b.rows, 1.0);
    // The plain index scan beats it (why C picks the index scan by default).
    assert!(b.total_cost > index_scan_total);

    let plan = crate::createplan::create_plan(&mut run, bpath).unwrap();
    let plan = crate::setrefs::set_plan_references(&mut run, plan).unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_BitmapHeapScan);
    let bhs = plan.as_bitmap_heap_scan().unwrap();
    assert_eq!(bhs.scan.scanrelid, 1);
    assert_eq!(bhs.scan.plan.plan_rows, 1.0);
    assert_eq!(bhs.scan.plan.plan_width, 8);
    assert!(bhs.scan.plan.qual.is_nil());
    assert_eq!(bhs.scan.plan.plan_node_id, 0);
    assert_eq!(bhs.bitmapqualorig.len(), 1);
    let orig = bhs.bitmapqualorig.nth(0).as_op_expr().unwrap();
    assert_eq!(orig.args.nth(0).as_var().unwrap().varno, 1);
    assert_eq!(orig.args.nth(1).as_const().unwrap().constvalue.as_i32(), 42);

    let child = bhs.scan.plan.lefttree.unwrap();
    assert_eq!(child.node_tag(), NodeTag::T_BitmapIndexScan);
    let biss = child.as_bitmap_index_scan().unwrap();
    assert_eq!(biss.indexid, IDX);
    assert!(!biss.isshared);
    assert_eq!(biss.scan.scanrelid, 1);
    assert!(biss.scan.plan.targetlist.is_nil() && biss.scan.plan.qual.is_nil());
    assert_eq!(biss.scan.plan.startup_cost, 0.0);
    assert!((biss.scan.plan.total_cost - index_total).abs() < 1e-9);
    assert_eq!(biss.scan.plan.plan_rows, 1.0);
    assert_eq!(biss.scan.plan.plan_node_id, 1);
    assert_eq!(biss.indexqual.len(), 1);
    let fixed = biss.indexqual.nth(0).as_op_expr().unwrap();
    assert_eq!(fixed.args.nth(0).as_var().unwrap().varno, -3);
    assert_eq!(biss.indexqualorig.len(), 1);
}

#[test]
fn select_star_plans_to_seqscan() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, None);
    let stmt = planner(mcx, leak_q(mcx, parse), "SELECT * FROM t", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
        .unwrap();

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_SeqScan);
    let sscan = plan.as_seq_scan().unwrap();
    assert_eq!(sscan.scan.scanrelid, 1);
    assert!(sscan.scan.plan.qual.is_nil());
    // EXPLAIN: Seq Scan on t (cost=0.00..200.00 rows=10000 width=8).
    assert_eq!(sscan.scan.plan.startup_cost, 0.0);
    assert!((sscan.scan.plan.total_cost - 200.0).abs() < 1e-9);
    assert_eq!(sscan.scan.plan.plan_rows, 10000.0);
    assert_eq!(sscan.scan.plan.plan_width, 8);
    assert_eq!(sscan.scan.plan.targetlist.len(), 2);
}

#[test]
fn competing_paths_pick_cheapest_total_and_startup() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 1, 42)));
    // tuple_fraction > 0 sets consider_startup: the seqscan (startup 0) and
    // the index scan (cheaper total) both survive add_path's fuzzy compare.
    let mut run = crate::run::PlannerRun::new(mcx);
    crate::subquery::subquery_planner(&mut run, leak_q(mcx, parse), false, 0.1, None).unwrap();
    let final_rel = crate::planmain::fetch_final_rel(&mut run);
    let rel = run.root.rel(final_rel);
    assert_eq!(rel.pathlist.len(), 2);
    let total = rel.cheapest_total_path.unwrap();
    let startup = rel.cheapest_startup_path.unwrap();
    assert!(matches!(run.root.path(total), types_pathnodes::PathNode::IndexPath(_)));
    assert_eq!(
        run.root.path(startup).base().pathtype,
        crate::pathnode::tag16(NodeTag::T_SeqScan)
    );
    assert!(run.root.path(startup).base().startup_cost == 0.0);
    assert!(
        run.root.path(total).base().total_cost < run.root.path(startup).base().total_cost
    );
}

#[test]
fn non_index_qual_plans_to_seqscan_with_qual() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 2, 7)));
    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "SELECT * FROM t WHERE val = 7",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_SeqScan);
    let sscan = plan.as_seq_scan().unwrap();
    assert_eq!(sscan.scan.plan.qual.len(), 1);
    // No stats: selectivity 1/DEFAULT_NUM_DISTINCT -> rows 50; the eq
    // operator adds cpu_operator_cost per tuple.
    assert_eq!(sscan.scan.plan.plan_rows, 50.0);
    assert!((sscan.scan.plan.total_cost - 225.0).abs() < 1e-9);
}

fn cx() -> MemoryContext {
    install_fixtures();
    MemoryContext::new_bump("planner-test")
}

// The analyzer's output for `SELECT 1`.
fn select_1_query(mcx: Mcx<'_>) -> Query<'_> {
    let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 8,
        ..Query::default()
    }
}

#[test]
fn select_1_plans_to_a_result_node() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        leak_q(mcx, select_1_query(mcx)),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.commandType, CmdType::CMD_SELECT);
    assert!(stmt.canSetTag);
    assert!(!stmt.hasReturning);
    assert_eq!(stmt.jitFlags, 0);
    assert!(stmt.subplans.is_nil());
    assert!(stmt.relationOids.is_nil());
    assert!(stmt.unprunableRelids.is_empty());
    assert_eq!(stmt.stmt_len, 8);

    // replace_empty_jointree's dummy RTE survives into the flat rtable.
    assert_eq!(stmt.rtable.len(), 1);
    let rte = stmt.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_RESULT);
    assert_eq!(rte.eref.unwrap().aliasname, Some("*RESULT*"));

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_Result);
    let result = plan.as_result().unwrap();
    assert!(result.plan.lefttree.is_none());
    assert!(result.resconstantqual.is_none());
    assert_eq!(result.plan.plan_node_id, 0);
    // EXPLAIN SELECT 1: cost=0.00..0.01 rows=1 width=4.
    assert_eq!(result.plan.startup_cost, 0.0);
    assert_eq!(result.plan.total_cost, 0.01);
    assert_eq!(result.plan.plan_rows, 1.0);
    assert_eq!(result.plan.plan_width, 4);

    assert_eq!(result.plan.targetlist.len(), 1);
    let tle = result.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert_eq!(tle.resname, Some("?column?"));
    assert!(!tle.resjunk);
    let c = tle.expr.as_const().unwrap();
    assert_eq!(c.consttype, 23);
    assert_eq!(c.constvalue.as_i32(), 1);
}

#[test]
fn seam_routes_to_standard_planner() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner_seams::planner::call(
        mcx,
        leak_q(mcx, select_1_query(mcx)),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();
    assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_Result);
}

#[test]
fn select_arithmetic_folds_before_planning() {
    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = select_1_query(mcx);
    let one = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let null = Node::mk_const(mcx, 23, -1, 0, 4, Datum::null(), true, true).unwrap();
    let op = Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: 551,
            opfuncid: 177,
            opresulttype: 23,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, one, null).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, op, 1, Some("?column?"), false).unwrap();
    parse.targetList = NodeList::make1(mcx, tle).unwrap();

    // int4pl is strict with a NULL arg: folds to a NULL Const, no executor.
    let stmt = planner(mcx, leak_q(mcx, parse), "SELECT 1 + NULL", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
        .unwrap();
    let plan = stmt.planTree.unwrap();
    let tle = plan.as_result().unwrap().plan.targetlist.nth(0).as_target_entry().unwrap();
    assert!(tle.expr.as_const().unwrap().constisnull);
}

#[test]
fn guc_boot_values_match_the_settings_tables() {
    use guc_tables::{GucDefaultValue, GucSetting};
    let expect: &[(&str, GucDefaultValue)] = &[
        ("cpu_tuple_cost", GucDefaultValue::Real(crate::gucs::cpu_tuple_cost())),
        ("cursor_tuple_fraction", GucDefaultValue::Real(crate::gucs::cursor_tuple_fraction())),
        ("jit_above_cost", GucDefaultValue::Real(crate::gucs::jit_above_cost())),
        (
            "jit_optimize_above_cost",
            GucDefaultValue::Real(crate::gucs::jit_optimize_above_cost()),
        ),
        ("jit_inline_above_cost", GucDefaultValue::Real(crate::gucs::jit_inline_above_cost())),
        ("jit", GucDefaultValue::Bool(crate::gucs::jit_enabled())),
        ("jit_expressions", GucDefaultValue::Bool(crate::gucs::jit_expressions())),
        ("jit_tuple_deforming", GucDefaultValue::Bool(crate::gucs::jit_tuple_deforming())),
        (
            "max_parallel_workers_per_gather",
            GucDefaultValue::Int(crate::gucs::max_parallel_workers_per_gather()),
        ),
        (
            "debug_parallel_query",
            GucDefaultValue::Enum(crate::gucs::debug_parallel_query()),
        ),
    ];
    for (name, have) in expect {
        let boot = guc_tables::all_settings()
            .find_map(|s| match s {
                GucSetting::Bool(b) if b.name == *name => Some(b.boot_val),
                GucSetting::Int(i) if i.name == *name => Some(i.boot_val),
                GucSetting::Real(r) if r.name == *name => Some(r.boot_val),
                GucSetting::Enum(e) if e.name == *name => Some(e.boot_val),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name} not in guc tables"));
        assert_eq!(boot, *have, "{name}");
    }
}

// The analyzer's output for `WITH x AS (SELECT pk, val FROM t) SELECT pk, val FROM x`.
fn with_cte_query(mcx: Mcx<'_>, cterefcount: i32) -> Query<'_> {
    let cte = types_nodes::parsenodes::CommonTableExpr {
        ctename: Some("x"),
        ctequery: Some(Node::mk(mcx, table_query(mcx, None)).unwrap()),
        cterefcount,
        ..Default::default()
    };
    let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "pk").unwrap()).unwrap();
    colnames.lappend(mcx, Node::mk_string(mcx, "val").unwrap()).unwrap();
    let eref = alloc_leak_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("x"), colnames },
    )
    .unwrap();
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_CTE;
    rte.ctename = Some("x");
    rte.ctelevelsup = 0;
    rte.eref = Some(eref);
    rte.inFromCl = true;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
    )
    .unwrap();
    let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
    let tle1 = Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, val, 2, Some("val"), false).unwrap();
    let mut target_list = NodeList::make1(mcx, tle1).unwrap();
    target_list.lappend(mcx, tle2).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        cteList: NodeList::make1(mcx, Node::mk(mcx, cte).unwrap()).unwrap(),
        jointree: Some(jointree),
        rtable,
        targetList: target_list,
        stmt_location: 0,
        stmt_len: 50,
        ..Query::default()
    }
}

#[test]
fn single_ref_cte_inlines_to_plain_scan() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        leak_q(mcx, with_cte_query(mcx, 1)),
        "WITH x AS (SELECT pk, val FROM t) SELECT pk, val FROM x",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.subplans.len(), 0);
    assert_eq!(stmt.paramExecTypes.len(), 0);
    assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_SeqScan);
}

// Default-policy CTE referenced more than once stays materialized (C
// SS_process_ctes refcount > 1 arm).
#[test]
fn with_cte_plans_to_ctescan_over_an_initplan_subplan() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        leak_q(mcx, with_cte_query(mcx, 2)),
        "WITH x AS (SELECT pk, val FROM t) SELECT pk, val FROM x",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.subplans.len(), 1);
    assert_eq!(stmt.subplans.nth(0).unwrap().node_tag(), NodeTag::T_SeqScan);
    assert_eq!(stmt.paramExecTypes.len(), 1);

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_CteScan);
    let cscan = plan.as_cte_scan().unwrap();
    assert_eq!(cscan.ctePlanId, 1);
    assert_eq!(cscan.cteParam, 0);
    // Top plan flattens first (as C); the subplan's SeqScan gets the offset.
    assert_eq!(stmt.rtable.len(), 2);
    assert_eq!(cscan.scan.scanrelid, 1);
    assert_eq!(stmt.subplans.nth(0).unwrap().as_seq_scan().unwrap().scan.scanrelid, 2);
    assert_eq!(cscan.scan.plan.plan_rows, 10000.0);

    assert_eq!(cscan.scan.plan.initPlan.len(), 1);
    let sp = cscan.scan.plan.initPlan.nth(0).as_sub_plan().unwrap();
    assert_eq!(sp.plan_id, 1);
    assert_eq!(sp.plan_name, Some("CTE x"));
    assert_eq!(sp.setParam.nth(0), 0);
}

#[test]
fn unreferenced_select_cte_is_skipped() {
    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = with_cte_query(mcx, 0);
    parse.rtable = NodeList::nil();
    parse.targetList = select_1_query(mcx).targetList;
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    parse.jointree = Some(jointree);
    let stmt =
        planner(mcx, leak_q(mcx, parse), "WITH x AS (...) SELECT 1", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
            .unwrap();
    assert!(stmt.subplans.is_nil());
    assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_Result);
}

// Plain-aggregation lane. Fixtures superset the shared ones so Once ordering
// across test binaries is irrelevant; pg_aggregate/pg_proc rows are
// pg_aggregate.dat/pg_proc.dat-exact.
mod agg {
    use super::*;
    use types_nodes::primnodes::{Aggref, OUTER_VAR};

    const COUNT_STAR: u32 = 2803;
    const SUM_INT4: u32 = 2108;
    const INT8OID: u32 = 20;

    fn agg_cx() -> MemoryContext {
        cx()
    }

    fn count_star_aggref(mcx: Mcx<'_>) -> Node<'_> {
        Node::mk(
            mcx,
            Aggref {
                aggfnoid: COUNT_STAR,
                aggtype: INT8OID,
                aggstar: true,
                ..Aggref::default()
            },
        )
        .unwrap()
    }

    fn sum_val_aggref(mcx: Mcx<'_>) -> Node<'_> {
        let var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let arg = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let mut aggargtypes = types_nodes::list::OidList::nil();
        aggargtypes.lappend(mcx, 23).unwrap();
        Node::mk(
            mcx,
            Aggref {
                aggfnoid: SUM_INT4,
                aggtype: INT8OID,
                aggargtypes,
                args: NodeList::make1(mcx, arg).unwrap(),
                ..Aggref::default()
            },
        )
        .unwrap()
    }

    fn first_val_aggref(mcx: Mcx<'_>) -> Node<'_> {
        let var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let arg = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let mut aggargtypes = types_nodes::list::OidList::nil();
        aggargtypes.lappend(mcx, 23).unwrap();
        Node::mk(
            mcx,
            Aggref {
                aggfnoid: FIRST_VAL_ANYELEMENT,
                aggtype: 23,
                aggargtypes,
                args: NodeList::make1(mcx, arg).unwrap(),
                ..Aggref::default()
            },
        )
        .unwrap()
    }

    fn agg_query<'mcx>(mcx: Mcx<'mcx>, aggs: &[(Node<'mcx>, &'mcx str)]) -> Query<'mcx> {
        let mut parse = table_query(mcx, None);
        let mut tlist = NodeList::nil();
        for (i, (agg, name)) in aggs.iter().enumerate() {
            let tle =
                Node::mk_target_entry(mcx, *agg, (i + 1) as i16, Some(name), false).unwrap();
            tlist.lappend(mcx, tle).unwrap();
        }
        parse.targetList = tlist;
        parse.hasAggs = true;
        parse
    }

    #[test]
    fn count_star_plans_to_plain_agg_over_seqscan() {
        let cx = agg_cx();
        let mcx = cx.mcx();
        let parse = agg_query(mcx, &[(count_star_aggref(mcx), "count")]);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT count(*) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_PLAIN);
        assert_eq!(agg.aggsplit, types_pathnodes::AGGSPLIT_SIMPLE);
        assert_eq!(agg.numCols, 0);
        assert_eq!(agg.numGroups, 1);
        assert_eq!(agg.transitionSpace, 0);
        assert!(agg.plan.qual.is_nil());

        // EXPLAIN: Aggregate (cost=225.00..225.01 rows=1 width=8) over
        // Seq Scan (cost=0.00..200.00 rows=10000 width=0).
        assert!((agg.plan.startup_cost - 225.0).abs() < 1e-9);
        assert!((agg.plan.total_cost - 225.01).abs() < 1e-9);
        assert_eq!(agg.plan.plan_rows, 1.0);
        assert_eq!(agg.plan.plan_width, 8);

        assert_eq!(agg.plan.targetlist.len(), 1);
        let tle = agg.plan.targetlist.nth(0).as_target_entry().unwrap();
        assert_eq!(tle.resname, Some("count"));
        let aggref = tle.expr.as_aggref().unwrap();
        assert_eq!(aggref.aggno, 0);
        assert_eq!(aggref.aggtransno, 0);
        assert_eq!(aggref.aggtranstype, INT8OID);
        assert!(aggref.args.is_nil());

        let child = agg.plan.lefttree.unwrap();
        assert_eq!(child.node_tag(), NodeTag::T_SeqScan);
        let sscan = child.as_seq_scan().unwrap();
        assert_eq!(sscan.scan.plan.plan_rows, 10000.0);
        assert_eq!(sscan.scan.plan.plan_width, 0);
        assert!((sscan.scan.plan.total_cost - 200.0).abs() < 1e-9);
        // use_physical_tlist: the child emits the physical tuple.
        assert_eq!(sscan.scan.plan.targetlist.len(), 2);
        assert_eq!(sscan.scan.plan.plan_node_id, 1);
        assert_eq!(agg.plan.plan_node_id, 0);
    }

    #[test]
    fn sum_arg_var_retargets_to_outer_subplan_column() {
        let cx = agg_cx();
        let mcx = cx.mcx();
        let parse = agg_query(mcx, &[(sum_val_aggref(mcx), "sum")]);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT sum(val) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let agg = plan.as_agg().unwrap();
        assert!((agg.plan.startup_cost - 225.0).abs() < 1e-9);
        assert!((agg.plan.total_cost - 225.01).abs() < 1e-9);
        assert_eq!(agg.plan.plan_width, 8);

        let aggref =
            agg.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_aggref().unwrap();
        assert_eq!((aggref.aggno, aggref.aggtransno, aggref.aggtranstype), (0, 0, INT8OID));
        assert_eq!(aggref.args.len(), 1);
        let arg_var =
            aggref.args.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!(arg_var.varno, OUTER_VAR);
        assert_eq!(arg_var.varattno, 2);
        assert_eq!(arg_var.vartype, 23);

        let child = agg.plan.lefttree.unwrap().as_seq_scan().unwrap();
        assert_eq!(child.scan.plan.targetlist.len(), 2);
        assert_eq!(child.scan.plan.plan_width, 4);
    }

    #[test]
    fn two_aggs_get_distinct_agg_and_trans_numbers() {
        let cx = agg_cx();
        let mcx = cx.mcx();
        let parse = agg_query(
            mcx,
            &[(count_star_aggref(mcx), "count"), (sum_val_aggref(mcx), "sum")],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT count(*), sum(val) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let agg = stmt.planTree.unwrap().as_agg().unwrap();
        // Two transfns at procost 1: startup 200 + 2 * 0.0025 * 10000.
        assert!((agg.plan.startup_cost - 250.0).abs() < 1e-9);
        assert!((agg.plan.total_cost - 250.01).abs() < 1e-9);
        assert_eq!(agg.plan.targetlist.len(), 2);
        let a0 = agg.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_aggref().unwrap();
        let a1 = agg.plan.targetlist.nth(1).as_target_entry().unwrap().expr.as_aggref().unwrap();
        assert_eq!((a0.aggno, a0.aggtransno), (0, 0));
        assert_eq!((a1.aggno, a1.aggtransno), (1, 1));
    }

    #[test]
    fn identical_aggrefs_share_one_aggno() {
        let cx = agg_cx();
        let mcx = cx.mcx();
        let parse = agg_query(
            mcx,
            &[(count_star_aggref(mcx), "count"), (count_star_aggref(mcx), "count")],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT count(*), count(*) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let agg = stmt.planTree.unwrap().as_agg().unwrap();
        // One shared transition state: costs match the single-count plan.
        assert!((agg.plan.startup_cost - 225.0).abs() < 1e-9);
        let a0 = agg.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_aggref().unwrap();
        let a1 = agg.plan.targetlist.nth(1).as_target_entry().unwrap().expr.as_aggref().unwrap();
        assert_eq!((a0.aggno, a0.aggtransno), (0, 0));
        assert_eq!((a1.aggno, a1.aggtransno), (0, 0));
    }

    // resolve_aggregate_transtype (parse_agg.c): a polymorphic STYPE=anyelement
    // aggregate resolves aggtranstype against the actual int4 argument, rather
    // than panicking (crates/backend/optimizer/plan/planner/src/prepagg.rs).
    #[test]
    fn polymorphic_transtype_resolves_against_actual_arg_type() {
        let cx = agg_cx();
        let mcx = cx.mcx();
        let parse = agg_query(mcx, &[(first_val_aggref(mcx), "first_val")]);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT first_val(val) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let agg = stmt.planTree.unwrap().as_agg().unwrap();
        let aggref =
            agg.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_aggref().unwrap();
        assert_eq!(aggref.aggtranstype, 23);
    }
}

// The analyzer's output for `INSERT INTO t (pk) VALUES (7)` over t(pk, val).
fn insert_query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = TBL;
    rte.relkind = b'r';
    rte.rellockmode = 3;
    rte.perminfoindex = 1;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let perminfo = Node::mk(
        mcx,
        types_nodes::parsenodes::RTEPermissionInfo {
            relid: TBL,
            requiredPerms: types_nodes::parsenodes::ACL_INSERT,
            ..Default::default()
        },
    )
    .unwrap();
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    let c = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(7), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, c, 1, Some("pk"), false).unwrap();
    Query {
        commandType: CmdType::CMD_INSERT,
        canSetTag: true,
        resultRelation: 1,
        jointree: Some(jointree),
        rtable,
        rteperminfos: NodeList::make1(mcx, perminfo).unwrap(),
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 29,
        ..Query::default()
    }
}

#[test]
fn insert_values_plans_to_modifytable_over_result() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = insert_query(mcx);
    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "INSERT INTO t (pk) VALUES (7)",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.commandType, CmdType::CMD_INSERT);
    assert!(!stmt.hasReturning);
    let mut rr = stmt.resultRelations.iter();
    assert_eq!((rr.next(), rr.next()), (Some(1), None));
    assert_eq!(stmt.rtable.len(), 2);
    assert_eq!(stmt.permInfos.len(), 1);
    let flat_rte = stmt.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(flat_rte.perminfoindex, 1);

    let mt_node = stmt.planTree.unwrap();
    assert_eq!(mt_node.node_tag(), NodeTag::T_ModifyTable);
    let mt = mt_node.as_modify_table().unwrap();
    assert_eq!(mt.operation, CmdType::CMD_INSERT);
    assert!(mt.canSetTag);
    assert_eq!(mt.nominalRelation, 1);
    assert_eq!(mt.rootRelation, 0);
    assert!(mt.plan.targetlist.is_nil());
    assert_eq!(mt.plan.plan_rows, 0.0);

    let sub = mt.plan.lefttree.unwrap();
    assert_eq!(sub.node_tag(), NodeTag::T_Result);
    let result = sub.as_result().unwrap();
    // Subplan tlist = processed tlist: (pk = 7, val = NULL) in attno order.
    assert_eq!(result.plan.targetlist.len(), 2);
    let t0 = result.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!((t0.resno, t0.resname), (1, Some("pk")));
    assert_eq!(t0.expr.as_const().unwrap().constvalue.as_i32(), 7);
    let t1 = result.plan.targetlist.nth(1).as_target_entry().unwrap();
    assert_eq!((t1.resno, t1.resname), (2, Some("val")));
    let c1 = t1.expr.as_const().unwrap();
    assert!(c1.constisnull);
    assert_eq!(c1.consttype, 23);
}

// INSERT ... ON CONFLICT lane over t(pk unique via IDX, val).
mod on_conflict {
    use super::*;
    use types_nodes::primnodes::{InferenceElem, OnConflictAction, OnConflictExpr, INNER_VAR};

    fn pk_arbiter_elems(mcx: Mcx<'_>) -> NodeList<'_> {
        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let elem =
            Node::mk(mcx, InferenceElem { expr: Some(var), infercollid: 0, inferopclass: 0 })
                .unwrap();
        NodeList::make1(mcx, elem).unwrap()
    }

    // The analyzer's output for the excluded pseudo-rel (RTI 2) targetlist.
    fn excluded_tlist(mcx: Mcx<'_>) -> NodeList<'_> {
        let pk = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let val = Node::mk_var(mcx, 2, 2, 23, -1, 0, 0).unwrap();
        let mut tl =
            NodeList::make1(mcx, Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap())
                .unwrap();
        tl.lappend(mcx, Node::mk_target_entry(mcx, val, 2, Some("val"), false).unwrap())
            .unwrap();
        tl
    }

    fn upsert_query<'mcx>(mcx: Mcx<'mcx>, oc: OnConflictExpr<'mcx>) -> Query<'mcx> {
        let mut parse = insert_query(mcx);
        if oc.exclRelIndex != 0 {
            let mut excl = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            excl.rtekind = RTEKind::RTE_RELATION;
            excl.relid = TBL;
            excl.relkind = b'c';
            excl.rellockmode = 3;
            excl.perminfoindex = 2;
            parse.rtable.lappend(mcx, excl.seal()).unwrap();
            let perminfo = Node::mk(
                mcx,
                types_nodes::parsenodes::RTEPermissionInfo { relid: TBL, ..Default::default() },
            )
            .unwrap();
            parse.rteperminfos.lappend(mcx, perminfo).unwrap();
        }
        parse.onConflict = Some(Node::mk(mcx, oc).unwrap());
        parse
    }

    fn plan<'mcx>(
        mcx: Mcx<'mcx>,
        parse: Query<'mcx>,
    ) -> &'mcx types_nodes::plannodes::ModifyTable<'mcx> {
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "INSERT INTO t (pk) VALUES (7) ON CONFLICT ...",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        stmt.planTree.unwrap().as_modify_table().unwrap()
    }

    #[test]
    fn do_nothing_without_infer_has_no_arbiters() {
        let cx = cx();
        let mcx = cx.mcx();
        let parse = upsert_query(
            mcx,
            OnConflictExpr {
                action: OnConflictAction::ONCONFLICT_NOTHING,
                ..Default::default()
            },
        );
        let mt = plan(mcx, parse);
        assert_eq!(mt.onConflictAction, OnConflictAction::ONCONFLICT_NOTHING as u32);
        assert!(mt.arbiterIndexes.is_nil());
        assert!(mt.onConflictSet.is_nil() && mt.onConflictWhere.is_none());
        assert_eq!(mt.exclRelRTI, 0);
    }

    #[test]
    fn do_nothing_infers_unique_index_arbiter() {
        let cx = cx();
        let mcx = cx.mcx();
        let parse = upsert_query(
            mcx,
            OnConflictExpr {
                action: OnConflictAction::ONCONFLICT_NOTHING,
                arbiterElems: pk_arbiter_elems(mcx),
                ..Default::default()
            },
        );
        let mt = plan(mcx, parse);
        let mut arbiters = mt.arbiterIndexes.iter();
        assert_eq!((arbiters.next(), arbiters.next()), (Some(IDX), None));
    }

    #[test]
    fn no_matching_index_is_42p10() {
        let cx = cx();
        let mcx = cx.mcx();
        let val_var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let elem = Node::mk(
            mcx,
            InferenceElem { expr: Some(val_var), infercollid: 0, inferopclass: 0 },
        )
        .unwrap();
        let parse = upsert_query(
            mcx,
            OnConflictExpr {
                action: OnConflictAction::ONCONFLICT_NOTHING,
                arbiterElems: NodeList::make1(mcx, elem).unwrap(),
                ..Default::default()
            },
        );
        let err = planner(
            mcx,
            leak_q(mcx, parse),
            "INSERT INTO t (pk) VALUES (7) ON CONFLICT (val) DO NOTHING",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        let err = match err {
            Err(e) => e,
            Ok(_) => panic!("expected 42P10, planner succeeded"),
        };
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_COLUMN_REFERENCE);
    }

    #[test]
    fn do_update_retags_excluded_vars_to_inner() {
        let cx = cx();
        let mcx = cx.mcx();
        // SET val = excluded.val WHERE t.val < 0; parser resnos are attnos.
        let excl_val = Node::mk_var(mcx, 2, 2, 23, -1, 0, 0).unwrap();
        let set_tle = Node::mk_target_entry(mcx, excl_val, 2, Some("val"), false).unwrap();
        let t_val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let zero = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(0), false, true).unwrap();
        let where_clause = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4_LT_OP,
                opfuncid: 66,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, t_val, zero).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let parse = upsert_query(
            mcx,
            OnConflictExpr {
                action: OnConflictAction::ONCONFLICT_UPDATE,
                arbiterElems: pk_arbiter_elems(mcx),
                onConflictSet: NodeList::make1(mcx, set_tle).unwrap(),
                onConflictWhere: Some(where_clause),
                exclRelIndex: 2,
                exclRelTlist: excluded_tlist(mcx),
                ..Default::default()
            },
        );
        let mt = plan(mcx, parse);

        assert_eq!(mt.onConflictAction, OnConflictAction::ONCONFLICT_UPDATE as u32);
        assert_eq!(mt.exclRelRTI, 2);
        let mut arbiters = mt.arbiterIndexes.iter();
        assert_eq!((arbiters.next(), arbiters.next()), (Some(IDX), None));

        // extract_update_targetlist_colnos: resno renumbered, attno saved.
        let mut cols = mt.onConflictCols.iter();
        assert_eq!((cols.next(), cols.next()), (Some(2), None));
        let tle = mt.onConflictSet.nth(0).as_target_entry().unwrap();
        assert_eq!(tle.resno, 1);
        let set_var = tle.expr.as_var().unwrap();
        assert_eq!((set_var.varno, set_var.varattno), (INNER_VAR, 2));

        // WHERE's result-rel Var passes through as a scan Var.
        let where_list = mt.onConflictWhere.unwrap().as_list().unwrap();
        assert_eq!(where_list.len(), 1);
        let op = where_list.nth(0).as_op_expr().unwrap();
        let where_var = op.args.nth(0).as_var().unwrap();
        assert_eq!((where_var.varno, where_var.varattno), (1, 2));
    }
}

// GROUP BY hashed lane: SELECT pk, count(*) FROM t GROUP BY pk.
// planagg lane: SELECT max(pk)/min(pk) FROM t rewrites to a Param-fed Result
// over an InitPlan Limit -> ordered Index Only Scan.
mod minmax_agg {
    use super::*;
    use types_nodes::primnodes::Aggref;

    const MAX_INT4: u32 = 2116;
    const MIN_INT4: u32 = 2132;

    fn minmax_aggref<'mcx>(mcx: Mcx<'mcx>, fnoid: u32, attno: i16) -> Node<'mcx> {
        let var = Node::mk_var(mcx, 1, attno, 23, -1, 0, 0).unwrap();
        let arg = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let mut aggargtypes = types_nodes::list::OidList::nil();
        aggargtypes.lappend(mcx, 23).unwrap();
        Node::mk(
            mcx,
            Aggref {
                aggfnoid: fnoid,
                aggtype: 23,
                aggargtypes,
                args: NodeList::make1(mcx, arg).unwrap(),
                ..Aggref::default()
            },
        )
        .unwrap()
    }

    fn minmax_query<'mcx>(mcx: Mcx<'mcx>, fnoid: u32, attno: i16) -> Query<'mcx> {
        let mut parse = table_query(mcx, None);
        let name = if fnoid == MAX_INT4 { "max" } else { "min" };
        let tle =
            Node::mk_target_entry(mcx, minmax_aggref(mcx, fnoid, attno), 1, Some(name), false)
                .unwrap();
        parse.targetList = NodeList::make1(mcx, tle).unwrap();
        parse.hasAggs = true;
        parse
    }

    fn plan_minmax<'mcx>(
        mcx: Mcx<'mcx>,
        fnoid: u32,
        attno: i16,
    ) -> types_nodes::plannodes::PlannedStmt<'mcx> {
        install_fixtures();
        planner(
            mcx,
            leak_q(mcx, minmax_query(mcx, fnoid, attno)),
            "SELECT max(pk) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap()
    }

    #[test]
    fn max_pk_plans_to_result_with_ios_backward_initplan() {
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = plan_minmax(mcx, MAX_INT4, 1);

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Result);
        let result = plan.as_result().unwrap();
        assert!(result.plan.lefttree.is_none());
        assert!(result.resconstantqual.is_none());

        // tlist: the Aggref was swapped for the InitPlan output Param $0.
        assert_eq!(result.plan.targetlist.len(), 1);
        let tle = result.plan.targetlist.nth(0).as_target_entry().unwrap();
        assert_eq!(tle.resname, Some("max"));
        let prm = tle.expr.as_param().unwrap();
        assert_eq!(prm.paramid, 0);
        assert_eq!(prm.paramtype, 23);

        // The InitPlan SubPlan hangs off the Result.
        assert_eq!(result.plan.initPlan.len(), 1);
        let sp = result.plan.initPlan.nth(0).as_sub_plan().unwrap();
        assert_eq!(sp.plan_id, 1);
        assert_eq!(sp.plan_name, Some("InitPlan 1"));
        assert_eq!(sp.firstColType, 23);
        let mut set_param = sp.setParam.iter();
        assert_eq!((set_param.next(), set_param.next()), (Some(0), None));

        // subplans[0]: Limit 1 -> Index Only Scan Backward on t_pkey with the
        // IS NOT NULL index qual.
        assert_eq!(stmt.subplans.len(), 1);
        let limit_node = stmt.subplans.nth(0).unwrap();
        assert_eq!(limit_node.node_tag(), NodeTag::T_Limit);
        let limit = limit_node.as_limit().unwrap();
        assert_eq!(limit.plan.plan_rows, 1.0);
        assert!(limit.limitOffset.is_none());
        assert_eq!(limit.limitCount.unwrap().as_const().unwrap().constvalue.as_i64(), 1);

        let ios_node = limit.plan.lefttree.unwrap();
        assert_eq!(ios_node.node_tag(), NodeTag::T_IndexOnlyScan);
        let ios = ios_node.as_index_only_scan().unwrap();
        assert_eq!(ios.indexid, IDX);
        assert_eq!(ios.indexorderdir, -1);
        // pk is NOT NULL: restriction_is_always_true (initsplan.c) drops the
        // generated IS NOT NULL qual, so no Index Cond survives (C 18 same).
        assert!(ios.indexqual.is_nil());
        assert!(ios.scan.plan.qual.is_nil());
        assert_eq!(ios.scan.plan.plan_rows, 10000.0);
        // The subplan's flattened RTE joins the top one.
        assert_eq!(ios.scan.scanrelid, 2);
        assert_eq!(stmt.rtable.len(), 2);

        assert_eq!(stmt.paramExecTypes.len(), 1);
    }

    #[test]
    fn min_pk_initplan_scans_forward() {
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = plan_minmax(mcx, MIN_INT4, 1);
        assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_Result);
        assert_eq!(stmt.subplans.len(), 1);
        let limit = stmt.subplans.nth(0).unwrap().as_limit().unwrap();
        let ios = limit.plan.lefttree.unwrap().as_index_only_scan().unwrap();
        assert_eq!(ios.indexorderdir, 1);
    }

    #[test]
    fn max_on_unindexed_column_keeps_plain_agg() {
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = plan_minmax(mcx, MAX_INT4, 2);
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        assert!(stmt.subplans.is_nil());
        assert!(stmt.paramExecTypes.is_nil() || stmt.paramExecTypes.len() == 1);
    }
}

mod group_by_hashed {
    use super::*;
    use types_nodes::parsenodes::SortGroupClause;
    use types_nodes::primnodes::{Aggref, OUTER_VAR};

    const COUNT_STAR: u32 = 2803;
    const INT4_LT_OP: u32 = 97;

    fn grouped_count_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        // val, not pk: the pk index's leading column would trip the loud
        // build_index_pathkeys guard now that group_pathkeys are built.
        let group_var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle1 = Node::mk_target_entry(mcx, group_var, 1, Some("val"), false).unwrap();
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle1.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        let aggref = Node::mk(
            mcx,
            Aggref { aggfnoid: COUNT_STAR, aggtype: 20, aggstar: true, ..Aggref::default() },
        )
        .unwrap();
        let tle2 = Node::mk_target_entry(mcx, aggref, 2, Some("count"), false).unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, tle2).unwrap();
        parse.targetList = tlist;
        parse.hasAggs = true;
        parse.groupClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        parse
    }

    #[test]
    fn group_by_plans_to_hashed_agg_over_seqscan() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        // cost_agg's hash_mem estimate reads the work_mem-backed globals.
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let parse = grouped_count_query(mcx);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT pk, count(*) FROM t GROUP BY pk",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_HASHED);
        assert_eq!(agg.aggsplit, types_pathnodes::AGGSPLIT_SIMPLE);
        assert_eq!(agg.numCols, 1);
        // Physical child tlist (use_physical_tlist under CP_LABEL_TLIST):
        // the grouping column sits at its attnum, as C.
        assert_eq!(agg.grpColIdx, &[2i16]);
        assert_eq!(agg.grpOperators, &[INT4EQ_OP]);
        assert_eq!(agg.grpCollations, &[0u32]);
        assert!(agg.numGroups > 0);
        assert!(agg.plan.qual.is_nil());

        // Projection: pk keeps its sortgroupref and retargets at OUTER.1;
        // count(*) carries planner aggno/aggtransno.
        assert_eq!(agg.plan.targetlist.len(), 2);
        let t0 = agg.plan.targetlist.nth(0).as_target_entry().unwrap();
        assert_eq!(t0.resname, Some("val"));
        let v0 = t0.expr.as_var().unwrap();
        assert_eq!((v0.varno, v0.varattno), (OUTER_VAR, 2));
        let t1 = agg.plan.targetlist.nth(1).as_target_entry().unwrap();
        let aggref = t1.expr.as_aggref().unwrap();
        assert_eq!((aggref.aggno, aggref.aggtransno), (0, 0));
        assert_eq!(aggref.aggtranstype, 20);

        // Child scan carries the grouping column with its sortgroupref.
        let child = agg.plan.lefttree.unwrap();
        assert_eq!(child.node_tag(), NodeTag::T_SeqScan);
        let ctl = &child.as_seq_scan().unwrap().scan.plan.targetlist;
        assert_eq!(ctl.len(), 2);
        let c0 = ctl.nth(0).as_target_entry().unwrap();
        assert_eq!(c0.ressortgroupref, 0);
        assert_eq!(c0.expr.as_var().unwrap().varattno, 1);
        let c1 = ctl.nth(1).as_target_entry().unwrap();
        assert_eq!(c1.ressortgroupref, 1);
        assert_eq!(c1.expr.as_var().unwrap().varattno, 2);
    }
}

// find_compatible_agg (prepagg.c) via equal(): identical sum(pk) calls whose
// argument Vars differ only in parse location share one agg/trans state.
mod shared_aggrefs {
    use super::*;
    use types_nodes::primnodes::Aggref;

    const SUM_INT4: u32 = 2108;

    #[test]
    fn duplicate_aggrefs_share_aggno_and_transno() {
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);

        let sum_at = |loc: i32, resno: i16| {
            let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
            // SAFETY: freshly built node; no other reference is live.
            unsafe {
                var.with_mut::<types_nodes::primnodes::Var, _>(|v| v.location = loc).unwrap()
            };
            let arg = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
            let aggref = Node::mk(
                mcx,
                Aggref {
                    aggfnoid: SUM_INT4,
                    aggtype: 20,
                    aggargtypes: types_nodes::OidList::make1(mcx, 23).unwrap(),
                    args: NodeList::make1(mcx, arg).unwrap(),
                    location: loc,
                    ..Aggref::default()
                },
            )
            .unwrap();
            Node::mk_target_entry(mcx, aggref, resno, Some("sum"), false).unwrap()
        };
        parse.targetList = NodeList::make2(mcx, sum_at(7, 1), sum_at(29, 2)).unwrap();
        parse.hasAggs = true;

        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT sum(pk), sum(pk) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        let tl = &agg.plan.targetlist;
        assert_eq!(tl.len(), 2);
        let a0 = tl.nth(0).as_target_entry().unwrap().expr.as_aggref().unwrap();
        let a1 = tl.nth(1).as_target_entry().unwrap().expr.as_aggref().unwrap();
        assert_eq!((a0.aggno, a0.aggtransno), (0, 0));
        assert_eq!(
            (a1.aggno, a1.aggtransno),
            (0, 0),
            "location-only differences must not defeat agg sharing"
        );
    }
}

mod sort_limit {
    use super::*;
    use types_nodes::parsenodes::SortGroupClause;

    // SELECT 1 ORDER BY 1: the const sort expr forms an ec_has_const EC, the
    // pathkey is EC_MUST_BE_REDUNDANT, and C plans a bare Result (no Sort).
    #[test]
    fn const_order_by_pathkey_is_redundant() {
        install_fixtures();
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = select_1_query(mcx);
        let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
        let tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: konst,
                resno: 1,
                resname: Some("?column?"),
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap();
        parse.targetList = NodeList::make1(mcx, tle).unwrap();
        parse.sortClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT 1 ORDER BY 1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Result);
        let result = plan.as_result().unwrap();
        assert!(result.plan.lefttree.is_none());
        assert_eq!(result.plan.total_cost, 0.01);
    }

    // The analyzer's output for `SELECT pk FROM t ORDER BY val LIMIT 2`:
    // val is a resjunk tlist entry carrying the sortgroupref.
    fn order_by_limit_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        let mut tl = NodeList::nil();
        let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        tl.lappend(mcx, Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap()).unwrap();
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let junk = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: val,
                resno: 2,
                resname: Some("val"),
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: true,
            },
        )
        .unwrap();
        tl.lappend(mcx, junk).unwrap();
        parse.targetList = tl;
        parse.sortClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        parse.limitCount =
            Some(Node::mk_const(mcx, 20, -1, 0, 8, Datum::from_i64(2), false, true).unwrap());
        parse.limitOption = types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT;
        parse
    }

    // EXPLAIN SELECT pk FROM t ORDER BY val LIMIT 2 (C formulas over the
    // 100-page/10000-tuple fixture):
    //   Limit    (cost=300.00..300.01 rows=2 width=8)
    //   -> Sort  (cost=300.00..325.00 rows=10000 width=8) [bounded heap]
    //      -> Seq Scan on t (cost=0.00..200.00 rows=10000 width=8)
    #[test]
    fn order_by_limit_plans_to_limit_sort_seqscan() {
        install_fixtures();
        let cx = cx();
        let mcx = cx.mcx();
        let parse = order_by_limit_query(mcx);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT pk FROM t ORDER BY val LIMIT 2",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Limit);
        let limit = plan.as_limit().unwrap();
        assert!((limit.plan.startup_cost - 300.0).abs() < 1e-9);
        assert!((limit.plan.total_cost - 300.005).abs() < 1e-9);
        assert_eq!(limit.plan.plan_rows, 2.0);
        assert_eq!(limit.plan.plan_width, 8);
        assert!(limit.limitOffset.is_none());
        let c = limit.limitCount.unwrap().as_const().unwrap();
        assert_eq!(c.constvalue.as_i64(), 2);

        let sort_node = limit.plan.lefttree.unwrap();
        assert_eq!(sort_node.node_tag(), NodeTag::T_Sort);
        let sort = sort_node.as_sort().unwrap();
        assert!((sort.plan.startup_cost - 300.0).abs() < 1e-9);
        assert!((sort.plan.total_cost - 325.0).abs() < 1e-9);
        assert_eq!(sort.plan.plan_rows, 10000.0);
        assert_eq!(sort.numCols, 1);
        assert_eq!(sort.sortColIdx, &[2i16]);
        assert_eq!(sort.sortOperators, &[INT4_LT_OP]);
        assert_eq!(sort.collations, &[0u32]);
        assert_eq!(sort.nullsFirst, &[false]);

        let scan_node = sort.plan.lefttree.unwrap();
        assert_eq!(scan_node.node_tag(), NodeTag::T_SeqScan);
        let scan = scan_node.as_seq_scan().unwrap();
        assert_eq!(scan.scan.plan.startup_cost, 0.0);
        assert!((scan.scan.plan.total_cost - 200.0).abs() < 1e-9);
        assert_eq!(scan.scan.plan.plan_width, 8);

        // The junk sort column survives every tlist; the top tlist is labeled.
        for node in [plan, sort_node, scan_node] {
            let tl = &node.as_plan().unwrap().targetlist;
            assert_eq!(tl.len(), 2);
            assert!(!tl.nth(0).as_target_entry().unwrap().resjunk);
            let junk = tl.nth(1).as_target_entry().unwrap();
            assert!(junk.resjunk);
            assert_eq!(junk.ressortgroupref, 1);
        }
        // Sort/Limit tlists were retargeted at OUTER_VAR by setrefs.
        let top_tle = plan.as_plan().unwrap().targetlist.nth(0).as_target_entry().unwrap();
        assert_eq!(top_tle.expr.as_var().unwrap().varno, types_nodes::primnodes::OUTER_VAR);
        assert_eq!(top_tle.resname, Some("pk"));
    }

    // ORDER BY covered by the already-chosen output ordering can't arise yet
    // (no index-provided ordering); the sort is always explicit, so the
    // pathkeys_contained_in skip is exercised by the Limit-over-Sort path
    // keeping the Sort's pathkeys (no second Sort above the Limit input).
    #[test]
    fn order_by_without_limit_costs_full_sort() {
        install_fixtures();
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = order_by_limit_query(mcx);
        parse.limitCount = None;
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT pk FROM t ORDER BY val",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Sort);
        let sort = plan.as_sort().unwrap();
        // Full quicksort: 0.005 * 10000 * log2(10000) + 200 input.
        let expected = 0.005 * 10000.0 * (10000.0f64.ln() / 0.693147180559945) + 200.0;
        assert!((sort.plan.startup_cost - expected).abs() < 1e-6, "{}", sort.plan.startup_cost);
        assert!((sort.plan.total_cost - (expected + 25.0)).abs() < 1e-6);
    }
}

// Nestloop join lane: SELECT * FROM jt1, jt2 WHERE jt1.a = jt2.a over the
// index-less fixtures (jt1: 1 page/1 row, jt2: 1 page/2 rows).
mod join {
    use super::*;
    use types_nodes::primnodes::{INNER_VAR, OUTER_VAR};

    fn join_query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
        join_query_rels(mcx, JT1, JT2)
    }

    fn join_query_rels<'mcx>(mcx: Mcx<'mcx>, r1: u32, r2: u32) -> Query<'mcx> {
        let mk_rte = |relid: u32| {
            let mut rte =
                Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            rte.rtekind = RTEKind::RTE_RELATION;
            rte.relid = relid;
            rte.relkind = b'r';
            rte.rellockmode = 1;
            rte.inh = false;
            rte.seal()
        };
        let mut rtable = NodeList::make1(mcx, mk_rte(r1)).unwrap();
        rtable.lappend(mcx, mk_rte(r2)).unwrap();

        let qual = {
            let l = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
            let r = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: INT4EQ_OP,
                    opfuncid: INT4EQ_PROC,
                    opresulttype: 16,
                    opretset: false,
                    opcollid: 0,
                    inputcollid: 0,
                    args: NodeList::make2(mcx, l, r).unwrap(),
                    location: -1,
                },
            )
            .unwrap()
        };
        let rtr1 = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let rtr2 = Node::mk_range_tbl_ref(mcx, 2).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr {
                fromlist: NodeList::make2(mcx, rtr1, rtr2).unwrap(),
                quals: Some(qual),
            },
        )
        .unwrap();

        let mut target_list = NodeList::nil();
        for (varno, attno, name) in
            [(1, 1, "a"), (1, 2, "pad"), (2, 1, "a"), (2, 2, "pad")]
        {
            let v = Node::mk_var(mcx, varno, attno, 23, -1, 0, 0).unwrap();
            let tle = Node::mk_target_entry(
                mcx,
                v,
                target_list.len() as i16 + 1,
                Some(name),
                false,
            )
            .unwrap();
            target_list.lappend(mcx, tle).unwrap();
        }
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: target_list,
            stmt_location: 0,
            stmt_len: 42,
            ..Query::default()
        }
    }

    fn assert_outer_inner_var(node: Node<'_>, varno: i32, attno: i16) {
        let v = node.as_var().expect("Var");
        assert_eq!((v.varno, v.varattno), (varno, attno));
    }

    #[test]
    fn comma_join_plans_to_inner_nestloop_with_join_filter() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, join_query(mcx)),
            "SELECT * FROM jt1, jt2 WHERE jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        assert_eq!(stmt.rtable.len(), 2);
        assert_eq!(stmt.relationOids.len(), 2);
        let nl = stmt.planTree.unwrap().as_nest_loop().expect("NestLoop root");

        // Live PG 18.3, same stats (1-page tables, reltuples 1 and 2, no
        // pg_statistic rows):
        //   Nested Loop  (cost=0.00..2.06 rows=1 width=16)
        //     Join Filter: (jt1.a = jt2.a)
        //     ->  Seq Scan on jt1  (cost=0.00..1.01 rows=1 width=8)
        //     ->  Seq Scan on jt2  (cost=0.00..1.02 rows=2 width=8)
        assert_eq!(nl.join.plan.startup_cost, 0.0);
        assert!((nl.join.plan.total_cost - 2.055).abs() < 1e-9, "{}", nl.join.plan.total_cost);
        assert_eq!(nl.join.plan.plan_rows, 1.0);
        assert_eq!(nl.join.plan.plan_width, 16);
        assert_eq!(nl.join.jointype, types_nodes::JoinType::JOIN_INNER);
        assert!(!nl.join.inner_unique);
        assert!(nl.nestParams.is_nil());

        let outer = nl.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan");
        assert_eq!(outer.scan.scanrelid, 1);
        assert!((outer.scan.plan.total_cost - 1.01).abs() < 1e-9);
        assert_eq!(outer.scan.plan.plan_rows, 1.0);
        assert_eq!(outer.scan.plan.plan_width, 8);
        let inner = nl.join.plan.righttree.unwrap().as_seq_scan().expect("inner SeqScan");
        assert_eq!(inner.scan.scanrelid, 2);
        assert!((inner.scan.plan.total_cost - 1.02).abs() < 1e-9);
        assert_eq!(inner.scan.plan.plan_rows, 2.0);

        // Join filter fixed up to OUTER_VAR/INNER_VAR over the child tlists.
        assert_eq!(nl.join.joinqual.len(), 1);
        let op = nl.join.joinqual.nth(0).as_op_expr().expect("join filter OpExpr");
        assert_eq!(op.opno, INT4EQ_OP);
        assert_outer_inner_var(op.args.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(op.args.nth(1), INNER_VAR, 1);
        assert!(nl.join.plan.qual.is_nil());

        // Join tlist: outer cols then inner cols, all retargeted.
        let tles: Vec<_> = nl.join.plan.targetlist.iter().collect();
        assert_eq!(tles.len(), 4);
        for (tle, (varno, attno)) in tles
            .iter()
            .zip([(OUTER_VAR, 1i16), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)])
        {
            assert_outer_inner_var(tle.as_target_entry().unwrap().expr, varno, attno);
        }

        // Children carry physical tlists (NestLoop projects).
        assert_eq!(outer.scan.plan.targetlist.len(), 2);
        assert_eq!(inner.scan.plan.targetlist.len(), 2);
    }

    // Cost crossover: over two 100-page/10000-row tables both nestloop and
    // hash paths are candidates, and hash wins once the merge path (which C
    // prefers here) is disabled. Live PG 18.3, no pg_statistic, mergejoin
    // off: Hash Join (cost=325.00..18050.00 rows=500000 width=16).
    #[test]
    fn large_comma_join_plans_to_hash_join() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        // final_cost_hashjoin's get_hash_memory_limit reads work_mem/hash_mem_multiplier.
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        crate::gucs::set_enable_mergejoin(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, join_query_rels(mcx, JT3, JT4)),
            "SELECT * FROM jt3, jt4 WHERE jt3.a = jt4.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_mergejoin(true);
        let stmt = stmt.unwrap();

        let hj = stmt
            .planTree
            .unwrap()
            .as_hash_join()
            .expect("HashJoin root (hash beats nestloop on large inputs)");
        assert_eq!(hj.join.jointype, types_nodes::JoinType::JOIN_INNER);
        // The equijoin clause is the hashclause; joinqual is emptied.
        assert_eq!(hj.hashclauses.len(), 1);
        assert!(hj.join.joinqual.is_nil());
        assert_eq!(hj.hashkeys.len(), 1);
        // Inner side is a Hash over a SeqScan; its hashkeys reference the child.
        let hash = hj.join.plan.righttree.unwrap().as_hash().expect("Hash inner");
        assert_eq!(hash.hashkeys.len(), 1);
        hash.plan.lefttree.unwrap().as_seq_scan().expect("Hash over SeqScan");
        hj.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan");
        // hashclause + hashkeys resolved to OUTER_VAR/INNER_VAR.
        let hc = hj.hashclauses.nth(0).as_op_expr().expect("hashclause OpExpr");
        assert_outer_inner_var(hc.args.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(hc.args.nth(1), INNER_VAR, 1);
        assert_outer_inner_var(hj.hashkeys.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(hash.hashkeys.nth(0), OUTER_VAR, 1);

        assert!((hj.join.plan.startup_cost - 325.0).abs() < 1e-9, "{}", hj.join.plan.startup_cost);
        assert!((hj.join.plan.total_cost - 18050.0).abs() < 1e-9, "{}", hj.join.plan.total_cost);
    }

    // Mergejoin lane: with nestloop and hashjoin disabled the explicit-sort
    // merge path (sort_inner_and_outer) wins. Costs are C's formulas over the
    // fixture stats (1-page tables, reltuples 1 and 2, no pg_statistic):
    //   Merge Join  (cost=2.05..2.08 rows=1 width=16)
    //     Merge Cond: (jt1.a = jt2.a)
    //     ->  Sort (cost=1.02..1.02 rows=1)  ->  Seq Scan on jt1 (..1.01)
    //     ->  Sort (cost=1.03..1.03 rows=2)  ->  Seq Scan on jt2 (..1.02)
    #[test]
    fn merge_join_wins_with_nestloop_and_hash_disabled() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        crate::gucs::set_enable_nestloop(false);
        crate::gucs::set_enable_hashjoin(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, join_query(mcx)),
            "SELECT * FROM jt1, jt2 WHERE jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_nestloop(true);
        crate::gucs::set_enable_hashjoin(true);
        let stmt = stmt.unwrap();

        let mj = stmt.planTree.unwrap().as_merge_join().expect("MergeJoin root");
        assert_eq!(mj.join.jointype, types_nodes::JoinType::JOIN_INNER);
        assert!(!mj.join.inner_unique);
        assert!(!mj.skip_mark_restore);
        assert!((mj.join.plan.startup_cost - 2.05).abs() < 1e-9, "{}", mj.join.plan.startup_cost);
        assert!((mj.join.plan.total_cost - 2.0775).abs() < 1e-9, "{}", mj.join.plan.total_cost);
        assert_eq!(mj.join.plan.plan_rows, 1.0);
        assert_eq!(mj.join.plan.plan_width, 16);

        // The equijoin clause is the (unswitched) merge clause; joinqual is
        // emptied; per-clause executor arrays carry the btree family shape.
        assert_eq!(mj.mergeclauses.len(), 1);
        assert!(mj.join.joinqual.is_nil());
        assert!(mj.join.plan.qual.is_nil());
        assert_eq!(mj.mergeFamilies, [INT4_BTREE_FAM]);
        assert_eq!(mj.mergeCollations, [0]);
        assert_eq!(mj.mergeReversals, [false]);
        assert_eq!(mj.mergeNullsFirst, [false]);
        let mc = mj.mergeclauses.nth(0).as_op_expr().expect("mergeclause OpExpr");
        assert_eq!(mc.opno, INT4EQ_OP);
        assert_outer_inner_var(mc.args.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(mc.args.nth(1), INNER_VAR, 1);

        // Explicit sorts on both inputs, costed by label_sort_with_costsize.
        let osort = mj.join.plan.lefttree.unwrap().as_sort().expect("outer Sort");
        let isort = mj.join.plan.righttree.unwrap().as_sort().expect("inner Sort");
        assert_eq!(osort.numCols, 1);
        assert_eq!(isort.numCols, 1);
        assert!((osort.plan.startup_cost - 1.02).abs() < 1e-9, "{}", osort.plan.startup_cost);
        assert!((osort.plan.total_cost - 1.025).abs() < 1e-9, "{}", osort.plan.total_cost);
        assert_eq!(osort.plan.plan_rows, 1.0);
        assert!((isort.plan.startup_cost - 1.03).abs() < 1e-9, "{}", isort.plan.startup_cost);
        assert!((isort.plan.total_cost - 1.035).abs() < 1e-9, "{}", isort.plan.total_cost);
        assert_eq!(isort.plan.plan_rows, 2.0);
        let oscan = osort.plan.lefttree.unwrap().as_seq_scan().expect("Sort over SeqScan");
        let iscan = isort.plan.lefttree.unwrap().as_seq_scan().expect("Sort over SeqScan");
        assert_eq!(oscan.scan.scanrelid, 1);
        assert_eq!(iscan.scan.scanrelid, 2);

        // scansel_cache was written back on the join clause (lesson 10).
        // (Verified indirectly: a second plan of the same query hits the
        // cached MergeScanSelCache; the cache lives per-planner-run here, so
        // just re-plan to prove the whole lane is reentrant.)
        crate::gucs::set_enable_nestloop(false);
        crate::gucs::set_enable_hashjoin(false);
        let again = planner(
            mcx,
            leak_q(mcx, join_query(mcx)),
            "SELECT * FROM jt1, jt2 WHERE jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_nestloop(true);
        crate::gucs::set_enable_hashjoin(true);
        let again = again.unwrap();
        let mj2 = again.planTree.unwrap().as_merge_join().expect("MergeJoin root");
        assert_eq!(mj2.join.plan.total_cost, mj.join.plan.total_cost);
    }

    // Parser output for `jt1 JOIN jt2 ON jt1.a = jt2.a`: an RTE_JOIN entry and
    // a JoinExpr jointree carrying the ON qual.
    fn join_on_query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
        let mut q = join_query(mcx);
        let f = q.jointree.unwrap();
        let on_qual = f.quals.expect("equijoin qual");

        let mut joinaliasvars = NodeList::nil();
        let mut colnames = NodeList::nil();
        for (varno, attno, name) in [(1, 1, "a"), (1, 2, "pad"), (2, 1, "a"), (2, 2, "pad")] {
            joinaliasvars
                .lappend(mcx, Node::mk_var(mcx, varno, attno, 23, -1, 0, 0).unwrap())
                .unwrap();
            colnames.lappend(mcx, Node::mk_string(mcx, name).unwrap()).unwrap();
        }
        let mut leftcols = types_nodes::list::IntList::nil();
        let mut rightcols = types_nodes::list::IntList::nil();
        for c in [1, 2] {
            leftcols.lappend(mcx, c).unwrap();
            rightcols.lappend(mcx, c).unwrap();
        }
        let eref = Node::mk_mut(
            mcx,
            types_nodes::Alias { aliasname: Some("unnamed_join"), colnames },
        )
        .unwrap()
        .seal_ref();
        let mut jrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        jrte.rtekind = RTEKind::RTE_JOIN;
        jrte.jointype = types_nodes::JoinType::JOIN_INNER;
        jrte.joinaliasvars = joinaliasvars;
        jrte.joinleftcols = leftcols;
        jrte.joinrightcols = rightcols;
        jrte.eref = Some(eref);
        jrte.inFromCl = true;
        q.rtable.lappend(mcx, jrte.seal()).unwrap();

        let join = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_INNER,
                isNatural: false,
                larg: f.fromlist.nth(0),
                rarg: f.fromlist.nth(1),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(on_qual),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        q.jointree = Some(
            alloc_leak_in(
                mcx,
                FromExpr { fromlist: NodeList::make1(mcx, join).unwrap(), quals: None },
            )
            .unwrap(),
        );
        q
    }

    // JOIN ... ON must deconstruct to the same jointree shape as the comma
    // join (C deconstruct_jointree) — identical plan, identical costs, with
    // the RTE_JOIN entry riding along in the flat rtable.
    #[test]
    fn explicit_inner_join_on_matches_comma_join_plan() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, join_on_query(mcx)),
            "SELECT * FROM jt1 JOIN jt2 ON jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        assert_eq!(stmt.rtable.len(), 3);
        assert_eq!(stmt.relationOids.len(), 2);
        let jrte = stmt.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.rtekind, RTEKind::RTE_JOIN);
        // add_rte_to_flat_rtable zaps the join alias lists.
        assert!(jrte.joinaliasvars.is_nil());

        let nl = stmt.planTree.unwrap().as_nest_loop().expect("NestLoop root");
        assert_eq!(nl.join.plan.startup_cost, 0.0);
        assert!((nl.join.plan.total_cost - 2.055).abs() < 1e-9, "{}", nl.join.plan.total_cost);
        assert_eq!(nl.join.plan.plan_rows, 1.0);
        assert_eq!(nl.join.plan.plan_width, 16);
        assert_eq!(nl.join.joinqual.len(), 1);
        let op = nl.join.joinqual.nth(0).as_op_expr().expect("join filter OpExpr");
        assert_eq!(op.opno, INT4EQ_OP);
        assert_outer_inner_var(op.args.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(op.args.nth(1), INNER_VAR, 1);
        let outer = nl.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan");
        assert_eq!(outer.scan.scanrelid, 1);
        let inner = nl.join.plan.righttree.unwrap().as_seq_scan().expect("inner SeqScan");
        assert_eq!(inner.scan.scanrelid, 2);
    }

    // Parser output for `jt1 <jointype> JOIN jt2 ON jt1.a = jt2.a`: the
    // nullable side's Vars carry the join RTE's index in varnullingrels
    // (markRelsAsNulledBy), including in the SELECT-list.
    fn outer_join_query<'mcx>(
        mcx: Mcx<'mcx>,
        jointype: types_nodes::JoinType,
        quals: Option<Node<'mcx>>,
    ) -> Query<'mcx> {
        let mut q = join_on_query(mcx);
        let f = q.jointree.unwrap();
        let join = f.fromlist.nth(0).as_join_expr().unwrap();
        let is_nulled = |varno: i32| match jointype {
            types_nodes::JoinType::JOIN_LEFT => varno == 2,
            types_nodes::JoinType::JOIN_FULL => true,
            _ => varno == 1,
        };
        let nulled_var = |varno: i32, attno: i16| {
            let mut nulling = types_nodes::Bitmapset::empty();
            nulling.add_member(mcx, 3).unwrap();
            Node::mk(
                mcx,
                types_nodes::primnodes::Var {
                    varno,
                    varattno: attno,
                    vartype: 23,
                    vartypmod: -1,
                    varnullingrels: nulling,
                    ..Default::default()
                },
            )
            .unwrap()
        };
        let mut tlist = NodeList::nil();
        for (i, te) in q.targetList.iter().enumerate() {
            let te = te.as_target_entry().unwrap();
            let v = te.expr.as_var().unwrap();
            let expr = if is_nulled(v.varno) { nulled_var(v.varno, v.varattno) } else { te.expr };
            tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, expr, i as i16 + 1, te.resname, false).unwrap(),
                )
                .unwrap();
        }
        q.targetList = tlist;
        let new_join = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype,
                isNatural: false,
                larg: join.larg,
                rarg: join.rarg,
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: join.quals,
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        q.jointree = Some(
            alloc_leak_in(
                mcx,
                FromExpr { fromlist: NodeList::make1(mcx, new_join).unwrap(), quals },
            )
            .unwrap(),
        );
        let jrte = q.rtable.nth(2);
        // SAFETY: freshly built query fixture, no other handles.
        unsafe {
            jrte.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| r.jointype = jointype)
        };
        q
    }

    // Live PG 18.3, same fixture stats:
    //   Hash Full Join  (cost=1.02..2.06 rows=2 width=16)
    //     Hash Cond: (jt2.a = jt1.a)
    //     ->  Seq Scan on jt2  (cost=0.00..1.02 rows=2 width=8)
    //     ->  Hash  (cost=1.01..1.01 rows=1 width=8)
    //           ->  Seq Scan on jt1  (cost=0.00..1.01 rows=1 width=8)
    #[test]
    fn full_join_plans_hash_full_join() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        // final_cost_hashjoin's get_hash_memory_limit reads work_mem/hash_mem_multiplier.
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let stmt = planner(
            mcx,
            leak_q(mcx, outer_join_query(mcx, types_nodes::JoinType::JOIN_FULL, None)),
            "SELECT * FROM jt1 FULL JOIN jt2 ON jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let hj = stmt.planTree.unwrap().as_hash_join().expect("HashJoin root");
        assert_eq!(hj.join.jointype, types_nodes::JoinType::JOIN_FULL);
        assert!((hj.join.plan.startup_cost - 1.0225).abs() < 1e-3, "{}", hj.join.plan.startup_cost);
        assert!((hj.join.plan.total_cost - 2.06).abs() < 5e-3, "{}", hj.join.plan.total_cost);
        assert_eq!(hj.join.plan.plan_rows, 2.0);
        assert_eq!(hj.join.plan.plan_width, 16);
        // C picks jt2 (the bigger rel) as outer: probe jt2, hash jt1.
        let outer = hj.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan");
        assert_eq!(outer.scan.scanrelid, 2);
        let hash = hj.join.plan.righttree.unwrap().as_hash().expect("inner Hash");
        let inner = hash.plan.lefttree.unwrap().as_seq_scan().expect("hashed SeqScan");
        assert_eq!(inner.scan.scanrelid, 1);
    }

    // Live PG 18.3, same fixture stats:
    //   Nested Loop Left Join  (cost=0.00..2.06 rows=1 width=16)
    //     Join Filter: (jt1.a = jt2.a)
    //     ->  Seq Scan on jt1  (cost=0.00..1.01 rows=1 width=8)
    //     ->  Seq Scan on jt2  (cost=0.00..1.02 rows=2 width=8)
    #[test]
    fn left_join_plans_nestloop_left_join() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, outer_join_query(mcx, types_nodes::JoinType::JOIN_LEFT, None)),
            "SELECT * FROM jt1 LEFT JOIN jt2 ON jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let nl = stmt.planTree.unwrap().as_nest_loop().expect("NestLoop root");
        assert_eq!(nl.join.jointype, types_nodes::JoinType::JOIN_LEFT);
        assert_eq!(nl.join.plan.startup_cost, 0.0);
        assert!((nl.join.plan.total_cost - 2.055).abs() < 1e-9, "{}", nl.join.plan.total_cost);
        assert_eq!(nl.join.plan.plan_rows, 1.0);
        assert_eq!(nl.join.plan.plan_width, 16);
        assert_eq!(nl.join.joinqual.len(), 1);
        assert!(nl.join.plan.qual.is_nil());
        let op = nl.join.joinqual.nth(0).as_op_expr().expect("join filter OpExpr");
        assert_outer_inner_var(op.args.nth(0), OUTER_VAR, 1);
        assert_outer_inner_var(op.args.nth(1), INNER_VAR, 1);
        let outer = nl.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan");
        assert_eq!(outer.scan.scanrelid, 1);
        let inner = nl.join.plan.righttree.unwrap().as_seq_scan().expect("inner SeqScan");
        assert_eq!(inner.scan.scanrelid, 2);
    }

    // RIGHT flips to LEFT in reduce_outer_joins. Live PG 18.3:
    //   Nested Loop Left Join  (cost=0.00..2.06 rows=2 width=16)
    //     Join Filter: (jt1.a = jt2.a)
    //     ->  Seq Scan on jt2  (cost=0.00..1.02 rows=2 width=8)
    //     ->  Materialize  (cost=0.00..1.01 rows=1 width=8)
    //           ->  Seq Scan on jt1  (cost=0.00..1.01 rows=1 width=8)
    #[test]
    fn right_join_flips_to_nestloop_left_join() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, outer_join_query(mcx, types_nodes::JoinType::JOIN_RIGHT, None)),
            "SELECT * FROM jt1 RIGHT JOIN jt2 ON jt1.a = jt2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let nl = stmt.planTree.unwrap().as_nest_loop().expect("NestLoop root");
        assert_eq!(nl.join.jointype, types_nodes::JoinType::JOIN_LEFT);
        assert_eq!(nl.join.plan.plan_rows, 2.0);
        assert!((nl.join.plan.total_cost - 2.0625).abs() < 1e-9, "{}", nl.join.plan.total_cost);
        let outer = nl.join.plan.lefttree.unwrap().as_seq_scan().expect("outer SeqScan jt2");
        assert_eq!(outer.scan.scanrelid, 2);
        let mat = nl.join.plan.righttree.unwrap().as_material().expect("Materialize inner");
        let inner = mat.plan.lefttree.unwrap().as_seq_scan().expect("SeqScan jt1");
        assert_eq!(inner.scan.scanrelid, 1);
        // The flipped join's RTE was updated in place.
        let jrte = stmt.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.jointype, types_nodes::JoinType::JOIN_LEFT);
    }

    // A strict WHERE on the nullable side reduces LEFT to INNER
    // (reduce_outer_joins). Live PG 18.3:
    //   Nested Loop  (cost=0.00..2.05 rows=1 width=16)
    //     Join Filter: (jt1.a = jt2.a)
    //     ->  Seq Scan on jt1  (cost=0.00..1.01 rows=1 width=8)
    //     ->  Seq Scan on jt2  (cost=0.00..1.02 rows=1 width=8)
    //           Filter: (pad = 5)
    #[test]
    fn left_join_with_strict_where_reduces_to_inner() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let mut nulling = types_nodes::Bitmapset::empty();
        nulling.add_member(mcx, 3).unwrap();
        let where_qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4EQ_OP,
                opfuncid: INT4EQ_PROC,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::Var {
                            varno: 2,
                            varattno: 2,
                            vartype: 23,
                            vartypmod: -1,
                            varnullingrels: nulling,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                    Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(5), false, true).unwrap(),
                )
                .unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, outer_join_query(mcx, types_nodes::JoinType::JOIN_LEFT, Some(where_qual))),
            "SELECT * FROM jt1 LEFT JOIN jt2 ON jt1.a = jt2.a WHERE jt2.pad = 5",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let nl = stmt.planTree.unwrap().as_nest_loop().expect("NestLoop root");
        assert_eq!(nl.join.jointype, types_nodes::JoinType::JOIN_INNER);
        assert_eq!(nl.join.plan.plan_rows, 1.0);
        let inner = nl.join.plan.righttree.unwrap().as_seq_scan().expect("inner SeqScan");
        assert_eq!(inner.scan.scanrelid, 2);
        assert_eq!(inner.scan.plan.qual.len(), 1);
        assert_eq!(inner.scan.plan.plan_rows, 1.0);
        // Reduction stripped the join's nulling bit everywhere.
        let jrte = stmt.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.jointype, types_nodes::JoinType::JOIN_INNER);
    }

    // No-stats large LEFT join picks the merge path. Live PG 18.3 (100-page,
    // 10000-row tables, no pg_statistic):
    //   Merge Left Join  (cost=1728.77..9278.77 rows=500000 width=16)
    //     Merge Cond: (jt3.a = jt4.a)
    //     ->  Sort (cost=864.39..889.39 rows=10000) -> Seq Scan on jt3
    //     ->  Sort (cost=864.39..889.39 rows=10000) -> Seq Scan on jt4
    #[test]
    fn large_left_join_plans_merge_left_join() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mut q = join_query_rels(mcx, JT3, JT4);
        // Rebuild as jt3 LEFT JOIN jt4 with parser-marked nullable Vars.
        let base = outer_join_query(mcx, types_nodes::JoinType::JOIN_LEFT, None);
        let f = base.jointree.unwrap();
        q.targetList = base.targetList;
        q.jointree = Some(f);
        q.rtable = base.rtable;
        // Point the copied rtable at the large fixtures.
        for (i, relid) in [(0usize, JT3), (1usize, JT4)] {
            // SAFETY: freshly built query fixture, no other handles.
            unsafe {
                q.rtable
                    .nth(i)
                    .with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| r.relid = relid)
            };
        }
        let stmt = planner(
            mcx,
            leak_q(mcx, q),
            "SELECT * FROM jt3 LEFT JOIN jt4 ON jt3.a = jt4.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let mj = stmt.planTree.unwrap().as_merge_join().expect("MergeJoin root");
        assert_eq!(mj.join.jointype, types_nodes::JoinType::JOIN_LEFT);
        assert_eq!(mj.join.plan.plan_rows, 500000.0);
        assert!((mj.join.plan.startup_cost - 1728.77).abs() < 5e-3, "{}", mj.join.plan.startup_cost);
        assert!((mj.join.plan.total_cost - 9278.77).abs() < 5e-3, "{}", mj.join.plan.total_cost);
        let osort = mj.join.plan.lefttree.unwrap().as_sort().expect("outer Sort");
        assert!((osort.plan.startup_cost - 864.39).abs() < 5e-3, "{}", osort.plan.startup_cost);
        let oscan = osort.plan.lefttree.unwrap().as_seq_scan().expect("Sort over SeqScan");
        assert_eq!(oscan.scan.scanrelid, 1);
    }

    // from_collapse_limit / join_collapse_limit joinlist shaping
    // (initsplan.c deconstruct_recurse).
    fn mk_plain_rte<'mcx>(mcx: Mcx<'mcx>, relid: u32) -> Node<'mcx> {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = relid;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        rte.seal()
    }

    fn mk_int4eq_vars<'mcx>(mcx: Mcx<'mcx>, lvarno: i32, rvarno: i32) -> Node<'mcx> {
        let l = Node::mk_var(mcx, lvarno, 1, 23, -1, 0, 0).unwrap();
        let r = Node::mk_var(mcx, rvarno, 1, 23, -1, 0, 0).unwrap();
        Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4EQ_OP,
                opfuncid: INT4EQ_PROC,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, l, r).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    // n copies of jt1, comma-joined with a chain of equality quals.
    fn many_comma_join_query<'mcx>(mcx: Mcx<'mcx>, n: usize) -> Query<'mcx> {
        let mut rtable = NodeList::nil();
        let mut fromlist = NodeList::nil();
        for i in 0..n {
            rtable.lappend(mcx, mk_plain_rte(mcx, JT1)).unwrap();
            fromlist
                .lappend(mcx, Node::mk_range_tbl_ref(mcx, i as i32 + 1).unwrap())
                .unwrap();
        }
        let mut args = NodeList::nil();
        for i in 1..n as i32 {
            args.lappend(mcx, mk_int4eq_vars(mcx, i, i + 1)).unwrap();
        }
        let quals = Node::mk(
            mcx,
            types_nodes::primnodes::BoolExpr {
                boolop: types_nodes::primnodes::BoolExprType::AND_EXPR,
                args,
                location: -1,
            },
        )
        .unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist, quals: Some(quals) }).unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 42,
            ..Query::default()
        }
    }

    fn mk_join_rte<'mcx>(mcx: Mcx<'mcx>, base_varnos: &[i32]) -> Node<'mcx> {
        let mut joinaliasvars = NodeList::nil();
        let mut colnames = NodeList::nil();
        let mut leftcols = types_nodes::list::IntList::nil();
        let mut rightcols = types_nodes::list::IntList::nil();
        for (i, &varno) in base_varnos.iter().enumerate() {
            for (attno, name) in [(1i16, "a"), (2, "pad")] {
                joinaliasvars
                    .lappend(mcx, Node::mk_var(mcx, varno, attno, 23, -1, 0, 0).unwrap())
                    .unwrap();
                colnames.lappend(mcx, Node::mk_string(mcx, name).unwrap()).unwrap();
            }
            if i + 1 < base_varnos.len() {
                leftcols.lappend(mcx, 2 * i as i32 + 1).unwrap();
                leftcols.lappend(mcx, 2 * i as i32 + 2).unwrap();
            } else {
                rightcols.lappend(mcx, 1).unwrap();
                rightcols.lappend(mcx, 2).unwrap();
            }
        }
        let eref = Node::mk_mut(
            mcx,
            types_nodes::Alias { aliasname: Some("unnamed_join"), colnames },
        )
        .unwrap()
        .seal_ref();
        let mut jrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        jrte.rtekind = RTEKind::RTE_JOIN;
        jrte.jointype = types_nodes::JoinType::JOIN_INNER;
        jrte.joinaliasvars = joinaliasvars;
        jrte.joinleftcols = leftcols;
        jrte.joinrightcols = rightcols;
        jrte.eref = Some(eref);
        jrte.inFromCl = true;
        jrte.seal()
    }

    fn mk_inner_join<'mcx>(
        mcx: Mcx<'mcx>,
        larg: Node<'mcx>,
        rarg: Node<'mcx>,
        quals: Node<'mcx>,
        rtindex: i32,
    ) -> Node<'mcx> {
        Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_INNER,
                isNatural: false,
                larg,
                rarg,
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(quals),
                alias: None,
                rtindex,
            },
        )
        .unwrap()
    }

    // rt1=jt3 (100 rows), rt2=jt4 (100 rows), rt3=jt1 (1 row):
    // (jt3 JOIN jt4 ON jt3.a = jt4.a) JOIN jt1 ON jt4.a = jt1.a.
    fn join_chain_query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
        let mut rtable = NodeList::nil();
        for relid in [JT3, JT4, JT1] {
            rtable.lappend(mcx, mk_plain_rte(mcx, relid)).unwrap();
        }
        rtable.lappend(mcx, mk_join_rte(mcx, &[1, 2])).unwrap();
        rtable.lappend(mcx, mk_join_rte(mcx, &[1, 2, 3])).unwrap();
        let rtr = |i: i32| Node::mk_range_tbl_ref(mcx, i).unwrap();
        let lower = mk_inner_join(mcx, rtr(1), rtr(2), mk_int4eq_vars(mcx, 1, 2), 4);
        let upper = mk_inner_join(mcx, lower, rtr(3), mk_int4eq_vars(mcx, 2, 3), 5);
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, upper).unwrap(), quals: None },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 3, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 42,
            ..Query::default()
        }
    }

    // Skip past unary Hash/Material/Sort nodes.
    fn descend(mut node: Node<'_>) -> Node<'_> {
        loop {
            if let Some(h) = node.as_hash() {
                node = h.plan.lefttree.unwrap();
            } else if let Some(m) = node.as_material() {
                node = m.plan.lefttree.unwrap();
            } else if let Some(s) = node.as_sort() {
                node = s.plan.lefttree.unwrap();
            } else {
                return node;
            }
        }
    }

    fn join_children<'mcx>(node: Node<'mcx>) -> Option<(Node<'mcx>, Node<'mcx>)> {
        let plan = if let Some(j) = node.as_nest_loop() {
            &j.join.plan
        } else if let Some(j) = node.as_hash_join() {
            &j.join.plan
        } else if let Some(j) = node.as_merge_join() {
            &j.join.plan
        } else {
            return None;
        };
        Some((descend(plan.lefttree.unwrap()), descend(plan.righttree.unwrap())))
    }

    fn scan_relid(node: Node<'_>) -> Option<u32> {
        node.as_seq_scan().map(|s| s.scan.scanrelid)
    }

    // Panicked before the collapse-limit port: 9 rels > join_collapse_limit.
    #[test]
    fn nine_way_comma_join_plans() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let stmt = planner(
            mcx,
            leak_q(mcx, many_comma_join_query(mcx, 9)),
            "SELECT a1.a FROM jt1 a1, ..., jt1 a9 WHERE chained equijoins",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        assert_eq!(stmt.rtable.len(), 9);
        assert_eq!(stmt.relationOids.len(), 9);
        assert!(join_children(stmt.planTree.unwrap()).is_some());
    }

    // join_collapse_limit=1 forces the syntactic order: (jt3 JOIN jt4)
    // planned as its own subproblem, jt1 joined on top. Live PG 18.3 with
    // the default limit instead joins the 1-row jt1 below the top join.
    #[test]
    fn join_collapse_limit_one_forces_join_order() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }

        let default_stmt = planner(
            mcx,
            leak_q(mcx, join_chain_query(mcx)),
            "SELECT jt1.a FROM jt3 JOIN jt4 ON jt3.a = jt4.a JOIN jt1 ON jt4.a = jt1.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let (dl, dr) = join_children(default_stmt.planTree.unwrap()).expect("join root");
        assert!(
            scan_relid(dl) != Some(3) && scan_relid(dr) != Some(3),
            "default limit joins the 1-row jt1 below the top join"
        );

        crate::gucs::set_join_collapse_limit(1);
        let stmt = planner(
            mcx,
            leak_q(mcx, join_chain_query(mcx)),
            "SET join_collapse_limit = 1; same query",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_join_collapse_limit(8);
        let stmt = stmt.unwrap();
        let (l, r) = join_children(stmt.planTree.unwrap()).expect("join root");
        let (sub, scan3) = if scan_relid(r) == Some(3) { (l, r) } else { (r, l) };
        assert_eq!(scan_relid(scan3), Some(3));
        let (sl, sr) = join_children(sub).expect("forced (jt3 JOIN jt4) subproblem");
        let mut rels = [scan_relid(sl).unwrap(), scan_relid(sr).unwrap()];
        rels.sort_unstable();
        assert_eq!(rels, [1, 2]);
    }

    // from_collapse_limit=2 keeps the JOIN subproblem as a nested joinlist
    // item next to the third FROM entry; the search still yields a plan with
    // the join pair grouped.
    #[test]
    fn from_collapse_limit_nests_join_subproblem() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        // FROM (jt3 JOIN jt4 ON jt3.a = jt4.a), jt1 WHERE jt4.a = jt1.a
        fn query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
            let mut rtable = NodeList::nil();
            for relid in [JT3, JT4, JT1] {
                rtable.lappend(mcx, mk_plain_rte(mcx, relid)).unwrap();
            }
            rtable.lappend(mcx, mk_join_rte(mcx, &[1, 2])).unwrap();
            let rtr = |i: i32| Node::mk_range_tbl_ref(mcx, i).unwrap();
            let join = mk_inner_join(mcx, rtr(1), rtr(2), mk_int4eq_vars(mcx, 1, 2), 4);
            let jointree = alloc_leak_in(
                mcx,
                FromExpr {
                    fromlist: NodeList::make2(mcx, join, rtr(3)).unwrap(),
                    quals: Some(mk_int4eq_vars(mcx, 2, 3)),
                },
            )
            .unwrap();
            let v = Node::mk_var(mcx, 3, 1, 23, -1, 0, 0).unwrap();
            let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
            Query {
                commandType: CmdType::CMD_SELECT,
                canSetTag: true,
                jointree: Some(jointree),
                rtable,
                targetList: NodeList::make1(mcx, tle).unwrap(),
                stmt_location: 0,
                stmt_len: 42,
                ..Query::default()
            }
        }
        crate::gucs::set_from_collapse_limit(2);
        let stmt = planner(
            mcx,
            leak_q(mcx, query(mcx)),
            "SET from_collapse_limit = 2; SELECT ... FROM (jt3 JOIN jt4 ON ...), jt1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_from_collapse_limit(8);
        let stmt = stmt.unwrap();
        let (l, r) = join_children(stmt.planTree.unwrap()).expect("join root");
        let (sub, scan3) = if scan_relid(r) == Some(3) { (l, r) } else { (r, l) };
        assert_eq!(scan_relid(scan3), Some(3));
        let (sl, sr) = join_children(sub).expect("nested (jt3 JOIN jt4) subproblem");
        let mut rels = [scan_relid(sl).unwrap(), scan_relid(sr).unwrap()];
        rels.sort_unstable();
        assert_eq!(rels, [1, 2]);
    }
}

mod stats_arms {
    use super::*;

    fn stt_query<'mcx>(mcx: Mcx<'mcx>, opno: u32, opfuncid: u32, constval: i32) -> Query<'mcx> {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = STT;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();

        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let konst =
            Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(constval), false, true).unwrap();
        let qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno,
                opfuncid,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, var, konst).unwrap(),
                location: -1,
            },
        )
        .unwrap();

        let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: Some(qual) },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 30,
            ..Query::default()
        }
    }

    fn plan_rows(mcx: Mcx<'_>, opno: u32, opfuncid: u32, constval: i32, sql: &'static str) -> f64 {
        let stmt =
            planner(mcx, leak_q(mcx, stt_query(mcx, opno, opfuncid, constval)), sql, CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
                .unwrap();
        let scan = stmt.planTree.unwrap().as_seq_scan().expect("SeqScan");
        scan.scan.plan.plan_rows
    }

    #[test]
    fn eqsel_mcv_match_uses_exact_frequency() {
        let cx = cx();
        let mcx = cx.mcx();
        // MCV entry 1 -> 0.30; 0.30 * 1000 tuples.
        assert_eq!(plan_rows(mcx, INT4EQ_OP, INT4EQ_PROC, 1, "SELECT a FROM stt WHERE a = 1"), 300.0);
    }

    #[test]
    fn eqsel_non_mcv_spreads_remainder_over_other_distinct() {
        let cx = cx();
        let mcx = cx.mcx();
        // (1 - 0.50 sumcommon)/(10 - 2) = 0.0625 -> rint(62.5) = 62.
        assert_eq!(plan_rows(mcx, INT4EQ_OP, INT4EQ_PROC, 7, "SELECT a FROM stt WHERE a = 7"), 62.0);
    }

    #[test]
    fn scalarltsel_interpolates_histogram() {
        let cx = cx();
        let mcx = cx.mcx();
        // histfrac = (1 + (15-10)/(20-10))/4 - eq_selec 1/8 = 0.25;
        // selec = (1 - 0.50)*0.25 + mcv(1,2 both < 15 -> 0.50) = 0.625.
        assert_eq!(plan_rows(mcx, INT4_LT_OP, 66, 15, "SELECT a FROM stt WHERE a < 15"), 625.0);
    }
}

// GL-STATSLOT-1: torn statistics slots must degrade like C (soft fallback),
// never panic. C reads pg_statistic from a pinned tuple copy, so a slot's
// kind and its arrays can never disagree; our lazy per-slot image re-probe
// can pair a bundle-time kind with arrays from a rewritten row (ANALYZE
// racing per-bind replanning), witnessed as an MCV/CORRELATION slot whose
// numbers array is empty. Born RED at t49 (index-out-of-bounds backend
// panic), except the correlation site which t47 3e606afd9 already guards.
mod torn_stats_arms {
    use super::*;

    fn torn_query<'mcx>(mcx: Mcx<'mcx>, opno: u32, opfuncid: u32, constval: i32) -> Query<'mcx> {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = TORN;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();

        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let konst =
            Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(constval), false, true).unwrap();
        let qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno,
                opfuncid,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, var, konst).unwrap(),
                location: -1,
            },
        )
        .unwrap();

        let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: Some(qual) },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 30,
            ..Query::default()
        }
    }

    fn plan_rows(mcx: Mcx<'_>, opno: u32, opfuncid: u32, constval: i32, sql: &'static str) -> f64 {
        let stmt = planner(
            mcx,
            leak_q(mcx, torn_query(mcx, opno, opfuncid, constval)),
            sql,
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let scan = stmt.planTree.unwrap().as_seq_scan().expect("SeqScan");
        scan.scan.plan.plan_rows
    }

    #[test]
    fn eqsel_torn_mcv_slot_falls_back_like_absent_mcv() {
        let cx = cx();
        let mcx = cx.mcx();
        // var_eq_const: no (value, freq) pair exists, so the const matches no
        // MCV entry; C's no-match arm with sumcommon = 0 over zero numbers:
        // selec = (1 - 0 - 0) / (10 - 0) = 0.1 -> 100 of 1000 tuples.
        assert_eq!(
            plan_rows(mcx, INT4EQ_OP, INT4EQ_PROC, 1, "SELECT a FROM torn WHERE a = 1"),
            100.0
        );
    }

    #[test]
    fn scalarltsel_torn_mcv_slot_uses_histogram_only() {
        let cx = cx();
        let mcx = cx.mcx();
        // mcv_selectivity over zero pairs yields (0, 0); histogram [0,10,20,
        // 30,40] for a < 15: histfrac = (1 + 0.5)/4 - eq_selec 1/(10-0) =
        // 0.275; selec = (1 - 0)*0.275 + 0 -> 275 of 1000 tuples.
        assert_eq!(
            plan_rows(mcx, INT4_LT_OP, 66, 15, "SELECT a FROM torn WHERE a < 15"),
            275.0
        );
    }

    fn torn_join_query<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
        let mk_rte = |relid: u32| {
            let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            rte.rtekind = RTEKind::RTE_RELATION;
            rte.relid = relid;
            rte.relkind = b'r';
            rte.rellockmode = 1;
            rte.inh = false;
            rte.seal()
        };
        let mut rtable = NodeList::make1(mcx, mk_rte(TORN)).unwrap();
        rtable.lappend(mcx, mk_rte(TORN2)).unwrap();

        let qual = {
            let l = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
            let r = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: INT4EQ_OP,
                    opfuncid: INT4EQ_PROC,
                    opresulttype: 16,
                    opretset: false,
                    opcollid: 0,
                    inputcollid: 0,
                    args: NodeList::make2(mcx, l, r).unwrap(),
                    location: -1,
                },
            )
            .unwrap()
        };
        let rtr1 = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let rtr2 = Node::mk_range_tbl_ref(mcx, 2).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make2(mcx, rtr1, rtr2).unwrap(), quals: Some(qual) },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 42,
            ..Query::default()
        }
    }

    #[test]
    fn eqjoinsel_torn_mcv_slots_fall_back_to_distinct_ratio() {
        let cx = cx();
        let mcx = cx.mcx();
        // eqjoinsel_inner over zero (value, freq) pairs: matchprodfreq = 0,
        // otherfreq1 = otherfreq2 = 1; totalsel = 1/(nd - 0) = 0.1 ->
        // 1000 * 1000 * 0.1 rows at the join.
        let stmt = planner(
            mcx,
            leak_q(mcx, torn_join_query(mcx)),
            "SELECT torn.a FROM torn, torn2 WHERE torn.a = torn2.a",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let root = stmt.planTree.unwrap();
        let rows = if let Some(hj) = root.as_hash_join() {
            hj.join.plan.plan_rows
        } else if let Some(mj) = root.as_merge_join() {
            mj.join.plan.plan_rows
        } else if let Some(nl) = root.as_nest_loop() {
            nl.join.plan.plan_rows
        } else {
            panic!("join root expected, got {:?}", root.node_tag())
        };
        assert_eq!(rows, 100000.0);
    }

    #[test]
    fn btcost_tolerates_empty_correlation_slot() {
        let cx = cx();
        let mcx = cx.mcx();
        // TBL.pk carries a CORRELATION slot whose numbers array is empty (the
        // RWREG-2 witnessed shape). btcostestimate must leave the correlation
        // at its 0.0 default, exactly as when the slot is absent (C reads
        // numbers[0] only under nnumbers > 0); the unique-index eq estimate
        // is 1 row and the IndexScan choice is unchanged.
        let stmt = planner(
            mcx,
            leak_q(mcx, table_query(mcx, Some(eq_qual(mcx, 1, 1)))),
            "SELECT * FROM t WHERE pk = 1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let iscan = stmt.planTree.unwrap().as_index_scan().expect("IndexScan");
        assert_eq!(iscan.scan.plan.plan_rows, 1.0);
    }
}

mod pattern_saop_arms {
    use super::*;
    use types_nodes::primnodes::{OpExpr, ScalarArrayOpExpr};

    fn setup() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            regex_core::init_seams();
        });
        mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    }

    fn one_rel_query<'mcx>(
        mcx: Mcx<'mcx>,
        relid: u32,
        vartype: u32,
        varcollid: u32,
        qual: Node<'mcx>,
    ) -> Query<'mcx> {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = relid;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
        let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: Some(qual) },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 1, 1, vartype, -1, varcollid, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("t"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 30,
            ..Query::default()
        }
    }

    fn plan_rows<'a>(mcx: Mcx<'a>, q: Query<'a>, sql: &'static str) -> f64 {
        let stmt =
            planner(mcx, leak_q(mcx, q), sql, CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL).unwrap();
        let scan = stmt.planTree.unwrap().as_seq_scan().expect("SeqScan");
        scan.scan.plan.plan_rows
    }

    fn text_op_qual<'mcx>(mcx: Mcx<'mcx>, opno: u32, opfuncid: u32, rhs: &str) -> Node<'mcx> {
        let var = Node::mk_var(mcx, 1, 1, 25, -1, 950, 0).unwrap();
        let konst =
            Node::mk_const(mcx, 25, -1, 100, -1, text_datum(mcx, rhs), false, false).unwrap();
        Node::mk(
            mcx,
            OpExpr {
                opno,
                opfuncid,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 950,
                args: NodeList::make2(mcx, var, konst).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    #[test]
    fn regex_exact_prefix_matches_eqsel() {
        setup();
        let cx = cx();
        let mcx = cx.mcx();
        // '^(foo)$' -> Pattern_Prefix_Exact -> var_eq_const, MCV "foo" 0.10.
        let q = one_rel_query(mcx, PTT, 25, 950, text_op_qual(mcx, TEXT_REGEXEQ_OP, 1254, "^(foo)$"));
        let regex_rows = plan_rows(mcx, q, "SELECT t FROM ptt WHERE t ~ '^(foo)$'");
        let q = one_rel_query(mcx, PTT, 25, 950, text_op_qual(mcx, TEXTEQ_OP, 67, "foo"));
        let eq_rows = plan_rows(mcx, q, "SELECT t FROM ptt WHERE t = 'foo'");
        assert_eq!(regex_rows, 100.0);
        assert_eq!(regex_rows, eq_rows);
    }


    fn int4_array_const<'a>(mcx: Mcx<'a>, elems: &[i32]) -> Node<'a> {
        let total = 24 + 4 * elems.len();
        let mut image: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total).unwrap();
        image.extend_from_slice(&datum::varlena::set_varsize_4b(total));
        image.extend_from_slice(&1i32.to_ne_bytes());
        image.extend_from_slice(&0i32.to_ne_bytes());
        image.extend_from_slice(&23i32.to_ne_bytes());
        image.extend_from_slice(&(elems.len() as i32).to_ne_bytes());
        image.extend_from_slice(&1i32.to_ne_bytes());
        for e in elems {
            image.extend_from_slice(&e.to_ne_bytes());
        }
        let value = Datum::from_usize(image.leak().as_ptr() as usize);
        Node::mk_const(mcx, 1007, -1, 0, -1, value, false, false).unwrap()
    }

    #[test]
    fn in_list_sums_disjoint_probabilities() {
        setup();
        let cx = cx();
        let mcx = cx.mcx();
        // a IN (1, 7): 0.30 (MCV) + (1-0.50)/(10-2) = 0.3625.
        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let qual = Node::mk(
            mcx,
            ScalarArrayOpExpr {
                opno: INT4EQ_OP,
                opfuncid: INT4EQ_PROC,
                hashfuncid: 0,
                negfuncid: 0,
                useOr: true,
                inputcollid: 0,
                args: NodeList::make2(mcx, var, int4_array_const(mcx, &[1, 7])).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let q = one_rel_query(mcx, STT, 23, 0, qual);
        // f32 catalog fractions: 0.30+0.20 sums above 0.5, so 362.50001
        // rounds up (C sums the same float4 slots into a double).
        assert_eq!(plan_rows(mcx, q, "SELECT a FROM stt WHERE a IN (1, 7)"), 363.0);
    }

    #[test]
    fn function_selectivity_defaults_to_one_third() {
        install_fixtures();
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        assert_eq!(
            crate::plancat::function_selectivity(
                &mut run,
                65,
                &[],
                0,
                false,
                0,
                types_pathnodes::JOIN_INNER,
                None,
            )
            .unwrap(),
            0.3333333
        );
    }
}

// HAVING / DISTINCT / sorted-grouping plan lanes.
mod having_distinct_sorted {
    use super::*;
    use types_nodes::parsenodes::SortGroupClause;
    use types_nodes::primnodes::Aggref;

    const COUNT_STAR: u32 = 2803;

    fn ensure_work_mem() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if !guc_tables::vars::work_mem.installed() {
                init_small::init_seams();
            }
        });
    }

    fn grouped_count_query(mcx: Mcx<'_>, with_having: bool) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        // val, not pk: the pk index's leading column trips the loud
        // build_index_pathkeys guard (index-order lane unported).
        let group_var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle1 = Node::mk_target_entry(mcx, group_var, 1, Some("val"), false).unwrap();
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle1.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        fn mk_count<'m>(mcx: Mcx<'m>) -> Node<'m> {
            Node::mk(
                mcx,
                Aggref { aggfnoid: COUNT_STAR, aggtype: 20, aggstar: true, ..Aggref::default() },
            )
            .unwrap()
        }
        let tle2 = Node::mk_target_entry(mcx, mk_count(mcx), 2, Some("count"), false).unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, tle2).unwrap();
        parse.targetList = tlist;
        parse.hasAggs = true;
        parse.groupClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        if with_having {
            let one =
                Node::mk_const(mcx, 20, -1, 0, 8, Datum::from_i64(1), false, true).unwrap();
            let mut args = NodeList::make1(mcx, mk_count(mcx)).unwrap();
            args.lappend(mcx, one).unwrap();
            parse.havingQual = Some(
                Node::mk(
                    mcx,
                    types_nodes::primnodes::OpExpr {
                        opno: INT8GT_OP,
                        opfuncid: 470,
                        opresulttype: 16,
                        opretset: false,
                        opcollid: 0,
                        inputcollid: 0,
                        args,
                        location: -1,
                    },
                )
                .unwrap(),
            );
        }
        parse
    }

    // GROUP BY pk HAVING count(*) > 1: the qual lands on the Agg plan node
    // (retargeted through set_upper_references), rows scaled by
    // DEFAULT_INEQ_SEL over the no-stats 200-group estimate.
    #[test]
    fn having_qual_lands_on_agg_plan() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, grouped_count_query(mcx, true)),
            "SELECT pk, count(*) FROM t GROUP BY pk HAVING count(*) > 1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_HASHED);
        assert_eq!(agg.plan.qual.len(), 1);
        let q = agg.plan.qual.nth(0).as_op_expr().unwrap();
        assert_eq!(q.opno, INT8GT_OP);
        let qagg = q.args.nth(0).as_aggref().unwrap();
        assert_eq!((qagg.aggno, qagg.aggtransno), (0, 0));
        // 200 default groups * DEFAULT_INEQ_SEL, clamped.
        assert_eq!(agg.plan.plan_rows, 67.0);
    }

    // With hashing disabled the sorted lane carries the same query:
    // Sort(pk) under Agg(AGG_SORTED), HAVING qual intact.
    #[test]
    fn group_by_without_hashagg_plans_agg_sorted_over_sort() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        crate::gucs::set_enable_hashagg(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, grouped_count_query(mcx, true)),
            "SELECT pk, count(*) FROM t GROUP BY pk HAVING count(*) > 1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_hashagg(true);
        let stmt = stmt.unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_SORTED);
        assert_eq!(agg.numCols, 1);
        assert_eq!(agg.grpColIdx, &[1i16]);
        assert_eq!(agg.plan.qual.len(), 1);
        let sort = agg.plan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        let s = sort.as_sort().unwrap();
        assert_eq!(s.numCols, 1);
        assert_eq!(s.sortColIdx, &[1i16]);
        assert_eq!(s.sortOperators, &[INT4_LT_OP]);
        assert_eq!(
            sort.as_plan().unwrap().lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
    }

    fn distinct_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        let var = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, Some("val"), false).unwrap();
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        parse.targetList = NodeList::make1(mcx, tle).unwrap();
        parse.distinctClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        parse
    }

    // SELECT DISTINCT pk FROM t: hashed DISTINCT (Agg with no Aggrefs) wins
    // by cost over the Sort+Unique candidate on the 10000-row fixture.
    #[test]
    fn distinct_plans_hashed_agg_by_cost() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, distinct_query(mcx)),
            "SELECT DISTINCT pk FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_HASHED);
        assert_eq!(agg.numCols, 1);
        assert_eq!(agg.grpColIdx, &[2i16]);
        assert_eq!(agg.grpOperators, &[INT4EQ_OP]);
        // No aggregates: the tlist is just the grouping Var.
        assert_eq!(agg.plan.targetlist.len(), 1);
        assert!(agg.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_var().is_some());
        assert_eq!(
            agg.plan.lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
        // C EXPLAIN: HashAggregate (cost=225.00..227.00 rows=200) over the
        // 100-page/10000-row fixture.
        assert!((agg.plan.startup_cost - 225.0).abs() < 1e-9);
        assert!((agg.plan.total_cost - 227.0).abs() < 1e-9);
        assert_eq!(agg.plan.plan_rows, 200.0);
    }

    // The sorted strategy: Unique over an explicit Sort when hashing is off.
    #[test]
    fn distinct_without_hashagg_plans_unique_over_sort() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        crate::gucs::set_enable_hashagg(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, distinct_query(mcx)),
            "SELECT DISTINCT pk FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_hashagg(true);
        let stmt = stmt.unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Unique);
        let uq = plan.as_unique().unwrap();
        assert_eq!(uq.numCols, 1);
        assert_eq!(uq.uniqColIdx, &[1i16]);
        assert_eq!(uq.uniqOperators, &[INT4EQ_OP]);
        assert_eq!(uq.uniqCollations, &[0u32]);
        assert_eq!(uq.plan.plan_rows, 200.0);
        let sort = uq.plan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        assert_eq!(
            sort.as_plan().unwrap().lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
    }
}

// --- uncorrelated sublink lane (subselect.rs) ---

fn scalar_subquery_node<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
    // The analyzer's output for `(SELECT pk FROM t)`.
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = TBL;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
    )
    .unwrap();
    let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap();
    let sub = Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: NodeList::make1(mcx, tle).unwrap(),
        ..Query::default()
    };
    Node::mk(mcx, sub).unwrap()
}

#[test]
fn expr_sublink_plans_to_initplan_param() {
    let cx = cx();
    let mcx = cx.mcx();

    let sublink = Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: scalar_subquery_node(mcx),
            location: -1,
        },
    )
    .unwrap();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let qual = Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: INT4EQ_OP,
            opfuncid: INT4EQ_PROC,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, sublink).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let mut parse = table_query(mcx, Some(qual));
    parse.hasSubLinks = true;

    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "SELECT * FROM t WHERE pk = (SELECT pk FROM t)",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    // One initplan: glob-level lists carry it, param 0 is int4.
    assert_eq!(stmt.subplans.len(), 1);
    assert_eq!(stmt.paramExecTypes.len(), 1);
    assert_eq!(stmt.paramExecTypes.nth(0), 23);
    // The flat rtable holds the outer scan's RTE plus the subplan's.
    assert_eq!(stmt.rtable.len(), 2);
    assert_eq!(stmt.relationOids.len(), 2);

    let top = stmt.planTree.unwrap();
    let base = top.as_plan().unwrap();
    assert_eq!(base.initPlan.len(), 1);
    let sp = base.initPlan.nth(0).as_sub_plan().unwrap();
    assert_eq!(sp.plan_id, 1);
    assert_eq!(sp.plan_name, Some("InitPlan 1"));
    assert_eq!(sp.firstColType, 23);
    assert_eq!(sp.setParam.as_slice(), &[0]);
    assert!(sp.parParam.is_nil() && !sp.useHashTable);
    // extParam/allParam (SS_finalize_plan): the initplan's setParam is in
    // allParam but not extParam of the node it hangs on.
    assert!(base.allParam.is_member(0));
    assert!(!base.extParam.is_member(0));

    // The initplan's plan tree scans t and returns pk.
    let subplan = stmt.subplans.nth(0).unwrap();
    let sub_base = subplan.as_plan().unwrap();
    assert_eq!(sub_base.targetlist.len(), 1);
    // Subplan scan relid was offset into the flat rtable.
    let sub_scan = match subplan.node_tag() {
        NodeTag::T_SeqScan => subplan.as_seq_scan().unwrap().scan.scanrelid,
        NodeTag::T_IndexScan => subplan.as_index_scan().unwrap().scan.scanrelid,
        other => panic!("unexpected subplan shape {other:?}"),
    };
    assert_eq!(sub_scan, 2);

    // The outer qual compares pk against the PARAM_EXEC Param.
    fn find_param_qual(plan: Node<'_>) -> bool {
        let quals = match plan.node_tag() {
            NodeTag::T_SeqScan => &plan.as_seq_scan().unwrap().scan.plan.qual,
            NodeTag::T_IndexScan => &plan.as_index_scan().unwrap().indexqual,
            _ => return false,
        };
        quals.iter().any(|q| {
            q.as_op_expr().is_some_and(|o| {
                o.args.iter().any(|a| {
                    a.as_param().is_some_and(|p| {
                        p.paramkind == types_nodes::ParamKind::PARAM_EXEC && p.paramid == 0
                    })
                })
            })
        })
    }
    let scan = if top.node_tag() == NodeTag::T_Result {
        top.as_result().unwrap().plan.lefttree.unwrap()
    } else {
        top
    };
    assert!(find_param_qual(scan), "outer scan qual references $0");
}

#[test]
fn uncorrelated_exists_plans_to_gating_result_over_initplan() {
    let cx = cx();
    let mcx = cx.mcx();

    let one = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let sub_tle = Node::mk_target_entry(mcx, one, 1, None, false).unwrap();
    let sub_node = {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = TBL;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
        let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
        )
        .unwrap();
        let sub = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, sub_tle).unwrap(),
            ..Query::default()
        };
        Node::mk(mcx, sub).unwrap()
    };
    let sublink = Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXISTS_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: sub_node,
            location: -1,
        },
    )
    .unwrap();
    let mut parse = table_query(mcx, Some(sublink));
    parse.hasSubLinks = true;

    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t)",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.subplans.len(), 1);
    assert_eq!(stmt.paramExecTypes.len(), 1);
    assert_eq!(stmt.paramExecTypes.nth(0), 16);

    // EXISTS at qual top level is pseudoconstant: a gating Result evaluates
    // $0 as a one-time filter above the scan.
    let top = stmt.planTree.unwrap();
    assert_eq!(top.node_tag(), NodeTag::T_Result);
    let result = top.as_result().unwrap();
    let rcq = result.resconstantqual.expect("one-time filter").as_list().unwrap();
    assert_eq!(rcq.len(), 1);
    let p = rcq.nth(0).as_param().unwrap();
    assert_eq!((p.paramkind, p.paramid, p.paramtype), (types_nodes::ParamKind::PARAM_EXEC, 0, 16));
    assert!(result.plan.lefttree.is_some());
    assert_eq!(result.plan.initPlan.len(), 1);
    let sp = result.plan.initPlan.nth(0).as_sub_plan().unwrap();
    assert_eq!(sp.subLinkType, types_nodes::SubLinkType::EXISTS_SUBLINK);
    assert_eq!(sp.firstColType, 2278);

    // simplify_EXISTS_query stripped the sub-tlist.
    let subplan = stmt.subplans.nth(0).unwrap();
    assert_eq!(subplan.node_tag(), NodeTag::T_SeqScan);
    assert!(subplan.as_plan().unwrap().targetlist.is_nil());
    assert_eq!(subplan.as_seq_scan().unwrap().scan.scanrelid, 2);
}

// The analyzer's output for `VALUES (3), (1), (2)` (bare multi-row VALUES).
fn values_query(mcx: Mcx<'_>) -> Query<'_> {
    let mut values_lists = NodeList::nil();
    for v in [3, 1, 2] {
        let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
        let row = Node::mk_list(mcx, NodeList::make1(mcx, konst).unwrap()).unwrap();
        values_lists.lappend(mcx, row).unwrap();
    }
    let colname = Node::mk_string(mcx, "column1").unwrap();
    let eref = mcx::alloc_leak_in(
        mcx,
        types_nodes::primnodes::Alias {
            aliasname: Some("*VALUES*"),
            colnames: NodeList::make1(mcx, colname).unwrap(),
        },
    )
    .unwrap();
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_VALUES;
    rte.values_lists = values_lists;
    rte.eref = Some(eref);
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None })
            .unwrap();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("column1"), false).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 22,
        ..Query::default()
    }
}

#[test]
fn multi_row_values_plans_to_values_scan() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        leak_q(mcx, values_query(mcx)),
        "VALUES (3), (1), (2)",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_ValuesScan);
    let vscan = plan.as_values_scan().unwrap();
    assert_eq!(vscan.scan.scanrelid, 1);
    assert_eq!(vscan.values_lists.len(), 3);
    assert_eq!(vscan.scan.plan.plan_rows, 3.0);
    assert_eq!(vscan.scan.plan.plan_width, 4);
    // C EXPLAIN: Values Scan on "*VALUES*" (cost=0.00..0.04 rows=3 width=4):
    // 3 * (cpu_operator_cost 0.0025 + cpu_tuple_cost 0.01).
    assert_eq!(vscan.scan.plan.startup_cost, 0.0);
    assert!((vscan.scan.plan.total_cost - 0.0375).abs() < 1e-9);
    let tle = vscan.scan.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resname, Some("column1"));
    assert_eq!(tle.expr.as_var().unwrap().varattno, 1);
}

// `SELECT * FROM (VALUES (2, 6), (1, 7)) v(a, b)` after parse analysis:
// outer query over an RTE_SUBQUERY whose subquery is the VALUES Query.
#[test]
fn from_values_subquery_pulls_up_to_values_scan() {
    let cx = cx();
    let mcx = cx.mcx();

    let mut values_lists = NodeList::nil();
    for (a, b) in [(2, 6), (1, 7)] {
        let mut row = NodeList::nil();
        for v in [a, b] {
            row.lappend(
                mcx,
                Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap(),
            )
            .unwrap();
        }
        values_lists.lappend(mcx, Node::mk_list(mcx, row).unwrap()).unwrap();
    }
    let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "column1").unwrap()).unwrap();
    colnames.lappend(mcx, Node::mk_string(mcx, "column2").unwrap()).unwrap();
    let eref = mcx::alloc_leak_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("*VALUES*"), colnames },
    )
    .unwrap();
    let mut vrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    vrte.rtekind = RTEKind::RTE_VALUES;
    vrte.values_lists = values_lists;
    vrte.eref = Some(eref);
    let inner_rtable = NodeList::make1(mcx, vrte.seal()).unwrap();
    let inner_jt = alloc_leak_in(
        mcx,
        FromExpr {
            fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
            quals: None,
        },
    )
    .unwrap();
    let mut inner_tl = NodeList::nil();
    for j in 1..=2i16 {
        let var = Node::mk_var(mcx, 1, j, 23, -1, 0, 0).unwrap();
        inner_tl
            .lappend(mcx, Node::mk_target_entry(mcx, var, j, Some("column"), false).unwrap())
            .unwrap();
    }
    let inner = mcx::alloc_leak_in(
        mcx,
        Query {
            commandType: CmdType::CMD_SELECT,
            jointree: Some(inner_jt),
            rtable: inner_rtable,
            targetList: inner_tl,
            ..Query::default()
        },
    )
    .unwrap();

    let mut srte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    srte.rtekind = RTEKind::RTE_SUBQUERY;
    srte.subquery = Some(inner);
    let vcols = {
        let mut l = NodeList::make1(mcx, Node::mk_string(mcx, "a").unwrap()).unwrap();
        l.lappend(mcx, Node::mk_string(mcx, "b").unwrap()).unwrap();
        l
    };
    srte.eref = Some(
        mcx::alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias { aliasname: Some("v"), colnames: vcols },
        )
        .unwrap(),
    );
    let rtable = NodeList::make1(mcx, srte.seal()).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr {
            fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
            quals: None,
        },
    )
    .unwrap();
    let mut target_list = NodeList::nil();
    for (j, name) in [(1i16, "a"), (2, "b")] {
        let var = Node::mk_var(mcx, 1, j, 23, -1, 0, 0).unwrap();
        target_list
            .lappend(mcx, Node::mk_target_entry(mcx, var, j, Some(name), false).unwrap())
            .unwrap();
    }
    let parse = Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: target_list,
        stmt_location: 0,
        stmt_len: 48,
        ..Query::default()
    };

    let stmt = planner(
        mcx,
        leak_q(mcx, parse),
        "SELECT * FROM (VALUES (2, 6), (1, 7)) v(a, b)",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.rtable.len(), 2);
    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_ValuesScan);
    let vscan = plan.as_values_scan().unwrap();
    assert_eq!(vscan.scan.scanrelid, 2);
    assert_eq!(vscan.values_lists.len(), 2);
    assert_eq!(vscan.scan.plan.plan_rows, 2.0);
    let tle = vscan.scan.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resname, Some("a"));
    let v = tle.expr.as_var().unwrap();
    assert_eq!((v.varno, v.varattno), (2, 1));
}

mod window {
    use super::*;
    use types_nodes::parsenodes::{SortGroupClause, WindowClause};
    use types_nodes::primnodes::{WindowFunc, OUTER_VAR};
    use types_nodes::rawnodes::FRAMEOPTION_DEFAULTS;

    const ROW_NUMBER: u32 = 3100;
    const RANK: u32 = 3101;
    const SUM_INT4: u32 = 2108;
    const INT8OID: u32 = 20;

    fn sgc(sortgroupref: u32) -> SortGroupClause {
        SortGroupClause {
            tleSortGroupRef: sortgroupref,
            eqop: INT4EQ_OP,
            sortop: INT4_LT_OP,
            reverse_sort: false,
            nulls_first: false,
            hashable: true,
        }
    }

    // The analyzer's output for
    //   SELECT pk, row_number() OVER (PARTITION BY pk ORDER BY val) FROM t.
    fn window_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        let wfunc = Node::mk(
            mcx,
            WindowFunc {
                winfnoid: ROW_NUMBER,
                wintype: INT8OID,
                winref: 1,
                ..WindowFunc::default()
            },
        )
        .unwrap();
        let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle1 = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: pk,
                resno: 1,
                resname: Some("pk"),
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap();
        let tle2 = Node::mk_target_entry(mcx, wfunc, 2, Some("row_number"), false).unwrap();
        let tle3 = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: val,
                resno: 3,
                resname: None,
                ressortgroupref: 2,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: true,
            },
        )
        .unwrap();
        let mut tlist = NodeList::make2(mcx, tle1, tle2).unwrap();
        tlist.lappend(mcx, tle3).unwrap();
        parse.targetList = tlist;
        let wc = Node::mk(
            mcx,
            WindowClause {
                partitionClause: NodeList::make1(mcx, Node::mk(mcx, sgc(2)).unwrap()).unwrap(),
                orderClause: NodeList::make1(mcx, Node::mk(mcx, sgc(2)).unwrap()).unwrap(),
                frameOptions: FRAMEOPTION_DEFAULTS,
                winref: 1,
                ..WindowClause::default()
            },
        )
        .unwrap();
        parse.windowClause = NodeList::make1(mcx, wc).unwrap();
        parse.hasWindowFuncs = true;
        parse
    }

    #[test]
    fn window_plans_to_windowagg_over_sort() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, window_query(mcx)),
            "SELECT pk, row_number() OVER (PARTITION BY val ORDER BY val) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let wagg = plan.as_window_agg().expect("WindowAgg root");
        assert_eq!(wagg.winname, Some("w1"));
        assert_eq!(wagg.winref, 1);
        assert_eq!(wagg.partNumCols, 1);
        assert_eq!(wagg.partOperators, &[INT4EQ_OP]);
        assert_eq!(wagg.partCollations, &[0]);
        assert_eq!(wagg.ordNumCols, 1);
        assert_eq!(wagg.ordOperators, &[INT4EQ_OP]);
        assert_eq!(wagg.frameOptions, FRAMEOPTION_DEFAULTS);
        assert!(wagg.startOffset.is_none() && wagg.endOffset.is_none());
        assert!(wagg.runCondition.is_nil());
        assert_eq!(wagg.plan.plan_rows, 10000.0);

        // tlist: pk (OUTER var), row_number (WindowFunc), junk val. The
        // window input target lists sgref columns first (val), then the
        // flattened pk — so pk reads child column 2.
        assert_eq!(wagg.plan.targetlist.len(), 3);
        let tle1 = wagg.plan.targetlist.nth(0).as_target_entry().unwrap();
        let v = tle1.expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (OUTER_VAR, 2));
        let tle2 = wagg.plan.targetlist.nth(1).as_target_entry().unwrap();
        let wf = tle2.expr.as_window_func().expect("WindowFunc survives setrefs");
        assert_eq!(wf.winfnoid, ROW_NUMBER);
        assert_eq!(wf.winref, 1);

        let sort = wagg.plan.lefttree.unwrap().as_sort().expect("Sort below WindowAgg");
        // PARTITION BY val ORDER BY val: the order pathkey is redundant with
        // the partition pathkey, so one sort key; both plan arrays keep val.
        assert_eq!(sort.numCols, 1);
        // Sort's own tlist is dummy OUTER refs after setrefs; the real key
        // expr lives in the scan tlist below (projection scribbles onto it).
        let scan_tl = &plan_of_node(sort.plan.lefttree.unwrap()).targetlist;
        let key_att = |resno: i16| get_sort_tl_var(scan_tl, resno);
        assert_eq!(key_att(sort.sortColIdx[0]), 2);
        assert_eq!(sort.sortOperators, &[INT4_LT_OP]);
        assert_eq!(sort.nullsFirst, &[false]);
        assert_eq!(wagg.partColIdx[0], sort.sortColIdx[0]);
        assert_eq!(wagg.ordColIdx[0], sort.sortColIdx[0]);

        let sscan = sort.plan.lefttree.unwrap().as_seq_scan().expect("SeqScan below Sort");
        assert_eq!(sscan.scan.plan.plan_rows, 10000.0);

        // WindowAgg preserves input ordering and never lowers cost.
        assert!(wagg.plan.total_cost > sort.plan.total_cost);
        assert!(wagg.plan.startup_cost >= sort.plan.startup_cost);
    }

    // SELECT val, sum(val) OVER (PARTITION BY pk), rank() OVER (ORDER BY val):
    // two windows stack two WindowAggs with a Sort under each (rank's
    // (val) ordering sorts after sum's (pk) — select_active_windows order).
    #[test]
    fn two_windows_stack_two_windowaggs() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let sum_arg = val;
        let sum_wf = Node::mk(
            mcx,
            WindowFunc {
                winfnoid: SUM_INT4,
                wintype: INT8OID,
                args: NodeList::make1(mcx, sum_arg).unwrap(),
                winref: 1,
                winagg: true,
                ..WindowFunc::default()
            },
        )
        .unwrap();
        let rank_wf = Node::mk(
            mcx,
            WindowFunc {
                winfnoid: RANK,
                wintype: INT8OID,
                winref: 2,
                ..WindowFunc::default()
            },
        )
        .unwrap();
        let tle1 = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: val,
                resno: 1,
                resname: Some("val"),
                ressortgroupref: 2,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap();
        let tle2 = Node::mk_target_entry(mcx, sum_wf, 2, Some("sum"), false).unwrap();
        let tle3 = Node::mk_target_entry(mcx, rank_wf, 3, Some("rank"), false).unwrap();
        let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle4 = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: pk,
                resno: 4,
                resname: None,
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: true,
            },
        )
        .unwrap();
        let mut tlist = NodeList::make2(mcx, tle1, tle2).unwrap();
        tlist.lappend(mcx, tle3).unwrap();
        tlist.lappend(mcx, tle4).unwrap();
        parse.targetList = tlist;
        let wc1 = Node::mk(
            mcx,
            WindowClause {
                partitionClause: NodeList::make1(mcx, Node::mk(mcx, sgc(1)).unwrap()).unwrap(),
                frameOptions: FRAMEOPTION_DEFAULTS,
                winref: 1,
                ..WindowClause::default()
            },
        )
        .unwrap();
        let wc2 = Node::mk(
            mcx,
            WindowClause {
                orderClause: NodeList::make1(mcx, Node::mk(mcx, sgc(2)).unwrap()).unwrap(),
                frameOptions: FRAMEOPTION_DEFAULTS,
                winref: 2,
                ..WindowClause::default()
            },
        )
        .unwrap();
        let mut wcl = NodeList::make1(mcx, wc1).unwrap();
        wcl.lappend(mcx, wc2).unwrap();
        parse.windowClause = wcl;
        parse.hasWindowFuncs = true;

        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val, sum(val) OVER (PARTITION BY pk), rank() OVER (ORDER BY val) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let top = stmt.planTree.unwrap().as_window_agg().expect("top WindowAgg");
        let top_wf = top
            .plan
            .targetlist
            .nth(1)
            .as_target_entry()
            .unwrap()
            .expr
            .as_window_func()
            .expect("top window func");
        assert_eq!(top_wf.winfnoid, SUM_INT4);
        assert!(top_wf.args.nth(0).as_var().is_some());
        assert_eq!(top.partNumCols, 1);
        assert_eq!(top.ordNumCols, 0);
        assert!(top.topWindow);

        let sort2 = top.plan.lefttree.unwrap().as_sort().expect("Sort between WindowAggs");
        assert_eq!(sort2.numCols, 1);
        let lower = sort2.plan.lefttree.unwrap().as_window_agg().expect("lower WindowAgg");
        assert_eq!(lower.partNumCols, 0);
        assert_eq!(lower.ordNumCols, 1);
        assert!(!lower.topWindow);
        let lower_wf_tle = lower
            .plan
            .targetlist
            .iter()
            .filter_map(|n| n.as_target_entry().unwrap().expr.as_window_func())
            .next()
            .expect("rank in lower tlist");
        assert_eq!(lower_wf_tle.winfnoid, RANK);
        assert!(lower_wf_tle.args.is_nil());
        let sort1 = lower.plan.lefttree.unwrap().as_sort().expect("Sort below lower WindowAgg");
        assert_eq!(sort1.numCols, 1);
        assert!(sort1.plan.lefttree.unwrap().as_seq_scan().is_some());

        assert_eq!(lower.winname, Some("w1"));
        assert_eq!(top.winname, Some("w2"));
    }

    fn plan_of_node<'a, 'mcx>(n: Node<'mcx>) -> &'a types_nodes::plannodes::Plan<'mcx>
    where
        'mcx: 'a,
    {
        n.as_plan().expect("plan node")
    }

    fn get_sort_tl_var(tlist: &NodeList<'_>, resno: i16) -> i16 {
        tlist
            .iter()
            .map(|n| n.as_target_entry().unwrap())
            .find(|t| t.resno == resno)
            .expect("sort key tle")
            .expr
            .as_var()
            .expect("sort key is a Var")
            .varattno
    }
}

mod dummy_rel {
    use super::*;

    fn assert_dummy_result(plan: Node<'_>) {
        assert_eq!(plan.node_tag(), NodeTag::T_Result);
        let result = plan.as_result().unwrap();
        assert!(result.plan.lefttree.is_none());
        // Live PG 18.3: Result (cost=0.00..0.00 rows=0 width=0),
        // One-Time Filter: false.
        assert_eq!(result.plan.startup_cost, 0.0);
        assert_eq!(result.plan.total_cost, 0.0);
        assert_eq!(result.plan.plan_rows, 0.0);
        assert_eq!(result.plan.plan_width, 0);
        let rcq = result.resconstantqual.expect("one-time filter").as_list().unwrap();
        assert_eq!(rcq.len(), 1);
        let c = rcq.nth(0).as_const().unwrap();
        assert_eq!(c.consttype, 16);
        assert!(!c.constisnull);
        assert!(!c.constvalue.as_bool());
    }

    #[test]
    fn is_null_on_not_null_column_plans_dummy_result() {
        let cx = cx();
        let mcx = cx.mcx();
        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let qual = Node::mk(
            mcx,
            types_nodes::primnodes::NullTest {
                arg: Some(var),
                nulltesttype: types_nodes::primnodes::NullTestType::IS_NULL,
                argisrow: false,
                location: -1,
            },
        )
        .unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, table_query(mcx, Some(qual))),
            "SELECT * FROM t WHERE pk IS NULL",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        assert_dummy_result(stmt.planTree.unwrap());
    }

    #[test]
    fn constant_false_qual_plans_dummy_result() {
        let cx = cx();
        let mcx = cx.mcx();
        let qual = clauses::make_bool_const(mcx, false, false).unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, table_query(mcx, Some(qual))),
            "SELECT * FROM t WHERE 1 = 2",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        assert_dummy_result(stmt.planTree.unwrap());
    }

    #[test]
    fn select_1_where_false_plans_gated_result() {
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = select_1_query(mcx);
        let qual = clauses::make_bool_const(mcx, false, false).unwrap();
        parse.jointree = Some(
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: Some(qual) })
                .unwrap(),
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT 1 WHERE 1 = 2",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Result);
        let result = plan.as_result().unwrap();
        assert!(result.plan.lefttree.is_none());
        // Live PG 18.3: Result (cost=0.00..0.01 rows=1 width=4).
        assert_eq!(result.plan.startup_cost, 0.0);
        assert_eq!(result.plan.total_cost, 0.01);
        assert_eq!(result.plan.plan_rows, 1.0);
        assert_eq!(result.plan.plan_width, 4);
        let rcq = result.resconstantqual.expect("one-time filter").as_list().unwrap();
        assert_eq!(rcq.len(), 1);
        assert!(!rcq.nth(0).as_const().unwrap().constvalue.as_bool());
    }
}

mod setops {
    use super::*;
    use types_nodes::list::{IntList, OidList};
    use types_nodes::parsenodes::{SetOperation, SetOperationStmt, SortGroupClause};

    fn subquery_rte<'mcx>(
        mcx: Mcx<'mcx>,
        subquery: Query<'mcx>,
        name: &'mcx str,
        colnames: &[&'mcx str],
    ) -> Node<'mcx> {
        let mut cols = NodeList::nil();
        for c in colnames {
            cols.lappend(mcx, Node::mk_string(mcx, c).unwrap()).unwrap();
        }
        let eref = alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias { aliasname: Some(name), colnames: cols },
        )
        .unwrap();
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_SUBQUERY;
        rte.subquery = Some(alloc_leak_in(mcx, subquery).unwrap());
        rte.eref = Some(eref);
        rte.alias = Some(eref);
        rte.inFromCl = false;
        rte.seal()
    }

    fn int4_group_clause(mcx: Mcx<'_>) -> Node<'_> {
        Node::mk(
            mcx,
            SortGroupClause {
                tleSortGroupRef: 0,
                eqop: INT4EQ_OP,
                sortop: INT4_LT_OP,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap()
    }

    fn setop_query<'mcx>(
        mcx: Mcx<'mcx>,
        op: SetOperation,
        all: bool,
        left: Query<'mcx>,
        right: Query<'mcx>,
        ncols: usize,
        colnames: &[&'mcx str],
    ) -> Query<'mcx> {
        let mut rtable = NodeList::make1(mcx, subquery_rte(mcx, left, "*SELECT* 1", colnames))
            .unwrap();
        rtable
            .lappend(mcx, subquery_rte(mcx, right, "*SELECT* 2", colnames))
            .unwrap();
        let mut col_types = OidList::nil();
        let mut col_typmods = IntList::nil();
        let mut col_collations = OidList::nil();
        let mut group_clauses = NodeList::nil();
        let mut tlist = NodeList::nil();
        for i in 0..ncols {
            col_types.lappend(mcx, 23).unwrap();
            col_typmods.lappend(mcx, -1).unwrap();
            col_collations.lappend(mcx, 0).unwrap();
            if !all {
                group_clauses.lappend(mcx, int4_group_clause(mcx)).unwrap();
            }
            let v = Node::mk_var(mcx, 1, (i + 1) as i16, 23, -1, 0, 0).unwrap();
            tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, v, (i + 1) as i16, Some(colnames[i]), false)
                        .unwrap(),
                )
                .unwrap();
        }
        let stmt = Node::mk(
            mcx,
            SetOperationStmt {
                op,
                all,
                larg: Some(Node::mk_range_tbl_ref(mcx, 1).unwrap()),
                rarg: Some(Node::mk_range_tbl_ref(mcx, 2).unwrap()),
                colTypes: col_types,
                colTypmods: col_typmods,
                colCollations: col_collations,
                groupClauses: group_clauses,
            },
        )
        .unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: tlist,
            setOperations: Some(stmt),
            stmt_location: 0,
            stmt_len: 40,
            ..Query::default()
        }
    }

    fn select_const_query(mcx: Mcx<'_>, v: i32) -> Query<'_> {
        let konst =
            Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
        let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 8,
            ..Query::default()
        }
    }

    fn val_only_table_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, val, 1, Some("val"), false).unwrap();
        parse.targetList = NodeList::make1(mcx, tle).unwrap();
        parse
    }

    // The analyzer's output for `SELECT a.pk AS x FROM t a, t b WHERE a.pk = b.pk`.
    fn two_rel_join_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut rtable = NodeList::nil();
        for _ in 0..2 {
            let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            rte.rtekind = RTEKind::RTE_RELATION;
            rte.relid = TBL;
            rte.relkind = b'r';
            rte.rellockmode = 1;
            rte.inh = false;
            rtable.lappend(mcx, rte.seal()).unwrap();
        }
        let mut fromlist = NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 2).unwrap()).unwrap();
        let a_pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let b_pk = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let quals = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4EQ_OP,
                opfuncid: INT4EQ_PROC,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, a_pk, b_pk).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist, quals: Some(quals) }).unwrap();
        let x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, x, 1, Some("x"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 50,
            ..Query::default()
        }
    }

    // The analyzer's output for `SELECT x FROM (<two_rel_join_query>) s`.
    fn wrapped_join_subquery_query(mcx: Mcx<'_>) -> Query<'_> {
        let rtable = NodeList::make1(
            mcx,
            subquery_rte(mcx, two_rel_join_query(mcx), "s", &["x"]),
        )
        .unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr {
                fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
                quals: None,
            },
        )
        .unwrap();
        let x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, x, 1, Some("x"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 60,
            ..Query::default()
        }
    }

    // Wrap a query one level: `SELECT x FROM (<inner>) <name>`.
    fn wrap_subquery<'mcx>(mcx: Mcx<'mcx>, inner: Query<'mcx>, name: &'mcx str) -> Query<'mcx> {
        let rtable = NodeList::make1(mcx, subquery_rte(mcx, inner, name, &["x"])).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr {
                fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
                quals: None,
            },
        )
        .unwrap();
        let x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, x, 1, Some("x"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 80,
            ..Query::default()
        }
    }

    // info-schema lane r6 panic shape (data_type_privileges/element_types):
    // the UNION ALL sits INSIDE a pulled-up FROM subquery (the
    // pull_up_simple_union_all path, not top-level flatten), and every member
    // wraps a join subquery, so each member's pullup is declined by the
    // post-recursion recheck and the members plan as retained subquery
    // leaves via set_subquery_pathlist.
    #[test]
    fn nested_union_all_of_join_subquery_members_plans() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let setop = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            true,
            wrapped_join_subquery_query(mcx),
            wrapped_join_subquery_query(mcx),
            1,
            &["x"],
        );
        let parse = wrap_subquery(mcx, wrap_subquery(mcx, setop, "u"), "q");
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT x FROM (SELECT x FROM ((join-sub) UNION ALL (join-sub)) u) q",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let mut plan = stmt.planTree.unwrap();
        while plan.node_tag() != NodeTag::T_Append {
            plan = plan.as_plan().unwrap().lefttree.expect("Append below unary nodes");
        }
        assert_eq!(plan.as_append().unwrap().appendplans.len(), 2);
    }

    // info-schema lane r4/r5 e2e panic shape: after the recursive pull-up the
    // UNION ALL member's jointree holds two rels + quals, so the
    // post-recursion is_safe_append_member recheck must decline the pullup
    // and the member plans as an ordinary subquery leaf.
    #[test]
    fn union_all_member_with_join_subquery_plans() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let parse = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            true,
            wrapped_join_subquery_query(mcx),
            select_const_query(mcx, 99),
            1,
            &["x"],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT x FROM (SELECT a.pk AS x FROM t a, t b WHERE a.pk = b.pk) s \
             UNION ALL SELECT 99",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Append);
        assert_eq!(plan.as_append().unwrap().appendplans.len(), 2);
    }

    #[test]
    fn union_all_of_consts_plans_to_append_of_results() {
        let cx = cx();
        let mcx = cx.mcx();
        let parse = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            true,
            select_const_query(mcx, 1),
            select_const_query(mcx, 2),
            1,
            &["?column?"],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT 1 UNION ALL SELECT 2",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Append);
        let a = plan.as_append().unwrap();
        assert_eq!(a.appendplans.len(), 2);
        // Trivial SubqueryScans are elided; the leaf Results surface directly.
        assert_eq!(a.appendplans.nth(0).node_tag(), NodeTag::T_Result);
        assert_eq!(a.appendplans.nth(1).node_tag(), NodeTag::T_Result);
        assert_eq!(a.plan.plan_rows, 2.0);
        // flatten_simple_union_all: 2 leaf RTEs + the leftmost-leaf copy +
        // each pulled-up leaf's RTE_RESULT (C 18.3 lays out the same 5).
        assert_eq!(stmt.rtable.len(), 5);
        let c0 = a.appendplans.nth(0).as_result().unwrap();
        assert!(c0.plan.targetlist.nth(0).as_target_entry().unwrap().expr.as_const().is_some());
    }

    #[test]
    fn union_of_consts_plans_to_hashagg_over_append() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let parse = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            false,
            select_const_query(mcx, 1),
            select_const_query(mcx, 2),
            1,
            &["?column?"],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT 1 UNION SELECT 2",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        // Two const rows: C picks Sort+Unique over HashAggregate (verified
        // on live PG 18.3: Unique 0.04..0.05, Sort 0.04..0.05, Append
        // 0.00..0.03, Result 0.00..0.01).
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Unique);
        let uplan = plan.as_plan().unwrap();
        let sort = uplan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        let append = sort.as_plan().unwrap().lefttree.unwrap();
        assert_eq!(append.node_tag(), NodeTag::T_Append);
        assert!((uplan.total_cost - 0.05).abs() < 0.005, "{}", uplan.total_cost);
        assert!((append.as_plan().unwrap().total_cost - 0.03).abs() < 0.005);
    }

    #[test]
    fn union_all_order_by_limit_plans_to_limit_sort_append() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let mut parse = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            true,
            val_only_table_query(mcx),
            val_only_table_query(mcx),
            1,
            &["val"],
        );
        let tle = parse.targetList.nth(0);
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        parse.sortClause = NodeList::make1(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef: 1,
                    eqop: INT4EQ_OP,
                    sortop: INT4_LT_OP,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
        parse.limitCount =
            Some(Node::mk_const(mcx, 20, -1, 0, 8, Datum::from_i64(5), false, true).unwrap());
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val FROM t UNION ALL SELECT val FROM t ORDER BY 1 LIMIT 5",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Limit);
        let sort = plan.as_plan().unwrap().lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        let append = sort.as_plan().unwrap().lefttree.unwrap();
        assert_eq!(append.node_tag(), NodeTag::T_Append);
        assert_eq!(append.as_append().unwrap().appendplans.len(), 2);
    }

    #[test]
    fn union_of_table_scans_plans_to_hashagg_over_append() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let parse = setop_query(
            mcx,
            SetOperation::SETOP_UNION,
            false,
            val_only_table_query(mcx),
            val_only_table_query(mcx),
            1,
            &["val"],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val FROM t UNION SELECT val FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_HASHED);
        assert_eq!(agg.numCols, 1);
        let child = agg.plan.lefttree.unwrap();
        assert_eq!(child.node_tag(), NodeTag::T_Append);
        let a = child.as_append().unwrap();
        assert_eq!(a.appendplans.len(), 2);
        assert_eq!(a.appendplans.nth(0).node_tag(), NodeTag::T_SeqScan);
    }

    #[test]
    fn intersect_of_table_scans_plans_to_hashsetop() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let parse = setop_query(
            mcx,
            SetOperation::SETOP_INTERSECT,
            false,
            val_only_table_query(mcx),
            val_only_table_query(mcx),
            1,
            &["val"],
        );
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val FROM t INTERSECT SELECT val FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_SetOp);
        let so = plan.as_set_op().unwrap();
        assert_eq!(so.cmd, types_pathnodes::SETOPCMD_INTERSECT);
        assert_eq!(so.strategy, types_pathnodes::SETOP_HASHED);
        assert_eq!(so.numCols, 1);
        assert_eq!(so.cmpOperators, &[INT4EQ_OP]);
        assert_eq!(so.plan.lefttree.unwrap().node_tag(), NodeTag::T_SeqScan);
        assert_eq!(so.plan.righttree.unwrap().node_tag(), NodeTag::T_SeqScan);
        // Each SeqScan renumbers into its own flattened-rtable slot.
        let lscan = so.plan.lefttree.unwrap().as_seq_scan().unwrap();
        let rscan = so.plan.righttree.unwrap().as_seq_scan().unwrap();
        assert_eq!(lscan.scan.scanrelid, 3);
        assert_eq!(rscan.scan.scanrelid, 4);
    }
}

mod grouping_sets {
    use super::*;
    use types_nodes::list::IntList;
    use types_nodes::parsenodes::{GroupingSet, GroupingSetKind, SortGroupClause};
    use types_nodes::primnodes::{Aggref, GroupingFunc};

    const COUNT_STAR: u32 = 2803;

    fn ensure_work_mem() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if !guc_tables::vars::work_mem.installed() {
                init_small::init_seams();
            }
        });
    }

    fn set_of<'m>(mcx: Mcx<'m>, refs: &[i32]) -> mcx::PgVec<'m, i32> {
        let mut v = mcx::PgVec::new_in(mcx);
        v.extend_from_slice(refs);
        v
    }

    fn sets_of<'m>(mcx: Mcx<'m>, sets: &[&[i32]]) -> mcx::PgVec<'m, mcx::PgVec<'m, i32>> {
        let mut v = mcx::PgVec::new_in(mcx);
        for s in sets {
            v.push(set_of(mcx, s));
        }
        v
    }

    fn chain_sets(chain: &[types_pathnodes::GroupingSetData<'_>]) -> Vec<Vec<u32>> {
        chain.iter().map(|gs| gs.set.to_vec()).collect()
    }

    #[test]
    fn extract_rollup_sets_single_chain() {
        let cx = cx();
        let mcx = cx.mcx();
        // (a),(a,b),(a,b,c) nest into one rollup chain.
        let sets = sets_of(mcx, &[&[1], &[1, 2], &[1, 2, 3]]);
        let chains = crate::groupingsets::extract_rollup_sets(mcx, sets);
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].len(), 3);
    }

    #[test]
    fn extract_rollup_sets_disjoint_and_overlapping_need_two_chains() {
        let cx = cx();
        let mcx = cx.mcx();
        let chains =
            crate::groupingsets::extract_rollup_sets(mcx, sets_of(mcx, &[&[1], &[2]]));
        assert_eq!(chains.len(), 2);
        // (a,b),(b,c): neither is a subset of the other.
        let chains = crate::groupingsets::extract_rollup_sets(
            mcx,
            sets_of(mcx, &[&[1, 2], &[2, 3]]),
        );
        assert_eq!(chains.len(), 2);
    }

    #[test]
    fn extract_rollup_sets_puts_empty_sets_on_first_chain() {
        let cx = cx();
        let mcx = cx.mcx();
        let chains = crate::groupingsets::extract_rollup_sets(
            mcx,
            sets_of(mcx, &[&[], &[1], &[2]]),
        );
        assert_eq!(chains.len(), 2);
        assert_eq!(chains[0].iter().map(|s| s.to_vec()).collect::<Vec<_>>(), vec![
            Vec::<i32>::new(),
            vec![1]
        ]);
        assert_eq!(chains[1][0].to_vec(), vec![2]);
    }

    #[test]
    fn reorder_grouping_sets_prefix_orders_largest_first() {
        let cx = cx();
        let mcx = cx.mcx();
        let chain = sets_of(mcx, &[&[], &[2], &[2, 1]]);
        let data =
            crate::groupingsets::reorder_grouping_sets(mcx, chain, &NodeList::nil());
        assert_eq!(chain_sets(&data), vec![vec![2, 1], vec![2], vec![]]);
    }

    fn mk_sgc<'m>(mcx: Mcx<'m>, sgref: u32) -> Node<'m> {
        Node::mk(
            mcx,
            SortGroupClause {
                tleSortGroupRef: sgref,
                eqop: INT4EQ_OP,
                sortop: INT4_LT_OP,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap()
    }

    fn int_list_node<'m>(mcx: Mcx<'m>, refs: &[i32]) -> Node<'m> {
        let mut il = IntList::nil();
        for &r in refs {
            il.lappend(mcx, r).unwrap();
        }
        Node::mk_int_list(mcx, il).unwrap()
    }

    // preprocess_grouping_sets over an already-expanded ROLLUP(a,b):
    // [[],[1],[1,2]] -> one rollup, gsets [[0,1],[0],[]].
    #[test]
    fn preprocess_grouping_sets_rollup_shape() {
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let mut gc = NodeList::make1(mcx, mk_sgc(mcx, 1)).unwrap();
        gc.lappend(mcx, mk_sgc(mcx, 2)).unwrap();
        parse.groupClause = gc;
        let mut gsets = NodeList::make1(mcx, int_list_node(mcx, &[])).unwrap();
        gsets.lappend(mcx, int_list_node(mcx, &[1])).unwrap();
        gsets.lappend(mcx, int_list_node(mcx, &[1, 2])).unwrap();
        parse.groupingSets = gsets;
        let mut run = crate::run::PlannerRun::new(mcx);
        let sealed: &Query<'_> = alloc_leak_in(mcx, parse).unwrap();
        run.root.parse = run.intern_query(sealed);

        let gd = crate::groupingsets::preprocess_grouping_sets(&mut run).unwrap();
        assert_eq!(gd.rollups.len(), 1);
        let rollup = &gd.rollups[0];
        assert_eq!(rollup.groupClause.len(), 2);
        let gsets: Vec<Vec<i32>> = rollup.gsets.iter().map(|s| s.to_vec()).collect();
        assert_eq!(gsets, vec![vec![0, 1], vec![0], vec![]]);
        assert_eq!(chain_sets(&rollup.gsets_data), vec![vec![1, 2], vec![1], vec![]]);
        assert!(rollup.hashable);
        assert!(gd.any_hashable);
        assert!(gd.unsortable_sets.is_empty());
        assert_eq!(run.root.processed_groupClause.len(), 2);
    }

    fn mk_count<'m>(mcx: Mcx<'m>) -> Node<'m> {
        Node::mk(
            mcx,
            Aggref { aggfnoid: COUNT_STAR, aggtype: 20, aggstar: true, ..Aggref::default() },
        )
        .unwrap()
    }

    // SELECT val, grouping(val), count(*) FROM t GROUP BY ROLLUP(val).
    fn rollup_val_query(mcx: Mcx<'_>) -> Query<'_> {
        let mut parse = table_query(mcx, None);
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let tle1 = Node::mk_target_entry(mcx, val, 1, Some("val"), false).unwrap();
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle1.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        let gf = Node::mk(
            mcx,
            GroupingFunc {
                args: NodeList::make1(mcx, val).unwrap(),
                refs: IntList::make1(mcx, 1).unwrap(),
                cols: IntList::nil(),
                agglevelsup: 0,
                location: -1,
            },
        )
        .unwrap();
        let tle2 = Node::mk_target_entry(mcx, gf, 2, Some("grouping"), false).unwrap();
        let tle3 = Node::mk_target_entry(mcx, mk_count(mcx), 3, Some("count"), false).unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, tle2).unwrap();
        tlist.lappend(mcx, tle3).unwrap();
        parse.targetList = tlist;
        parse.hasAggs = true;
        parse.groupClause = NodeList::make1(mcx, mk_sgc(mcx, 1)).unwrap();
        let simple = Node::mk(
            mcx,
            GroupingSet {
                kind: GroupingSetKind::GROUPING_SET_SIMPLE,
                content: NodeList::make1(mcx, Node::mk_integer(mcx, 1).unwrap()).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let rollup = Node::mk(
            mcx,
            GroupingSet {
                kind: GroupingSetKind::GROUPING_SET_ROLLUP,
                content: NodeList::make1(mcx, simple).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        parse.groupingSets = NodeList::make1(mcx, rollup).unwrap();
        parse
    }

    // GROUP BY ROLLUP(val) under enable_hashagg=off: one rollup, so a single
    // AGG_SORTED phase (empty chain) atop Sort(val); groupingSets [[0],[]];
    // GROUPING(val) resolved to cols [1] by setrefs.
    #[test]
    fn rollup_plans_sorted_agg_chain() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        crate::gucs::set_enable_hashagg(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, rollup_val_query(mcx)),
            "SELECT val, grouping(val), count(*) FROM t GROUP BY ROLLUP(val)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_hashagg(true);
        let stmt = stmt.unwrap();
        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Agg);
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_SORTED);
        assert_eq!(agg.numCols, 1);
        assert_eq!(agg.grpColIdx, &[1i16]);
        assert_eq!(agg.grpOperators, &[INT4EQ_OP]);
        let gsets: Vec<Vec<i32>> =
            agg.groupingSets.iter().map(|n| n.as_int_list().unwrap().iter().collect()).collect();
        assert_eq!(gsets, vec![vec![0], vec![]]);
        assert!(agg.chain.is_nil());
        // 200 default groups for (val) + 1 for the empty set.
        assert_eq!(agg.numGroups, 201);
        assert_eq!(agg.plan.plan_rows, 201.0);

        // GROUPING(val): refs [1] remapped through grouping_map to cols [1].
        let gf_tle = agg.plan.targetlist.nth(1).as_target_entry().unwrap();
        let gf = gf_tle.expr.as_grouping_func().unwrap();
        assert_eq!(gf.refs.iter().collect::<Vec<i32>>(), vec![1]);
        assert_eq!(gf.cols.iter().collect::<Vec<i32>>(), vec![1]);
        assert!(gf.args.nth(0).as_var().is_some());

        let sort = agg.plan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        let s = sort.as_sort().unwrap();
        assert_eq!(s.sortColIdx, &[1i16]);
        assert_eq!(
            sort.as_plan().unwrap().lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
    }

    // Default settings: the !is_sorted consider_groupingsets_paths arm wins
    // for ROLLUP(val) over an unsorted seqscan — MixedAggregate with the
    // hashed (val) rollup on top and the empty-set AGG_PLAIN chain entry
    // (no vestigial Sort: it consumes the shared input).
    #[test]
    fn rollup_with_hashagg_builds_mixed() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        let stmt = planner(
            mcx,
            leak_q(mcx, rollup_val_query(mcx)),
            "SELECT val, grouping(val), count(*) FROM t GROUP BY ROLLUP(val)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let agg = stmt.planTree.unwrap().as_agg().expect("top plan is Agg");
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_MIXED);
        assert_eq!(agg.numCols, 1);
        // The unsorted subplan keeps the seqscan physical tlist: val is
        // column 2 of t.
        assert_eq!(agg.grpColIdx, &[2i16]);
        assert_eq!(agg.numGroups, 200);
        let gsets: Vec<Vec<i32>> =
            agg.groupingSets.iter().map(|n| n.as_int_list().unwrap().iter().collect()).collect();
        assert_eq!(gsets, vec![vec![0]]);
        assert_eq!(agg.chain.len(), 1);
        let chain0 = agg.chain.nth(0).as_agg().unwrap();
        assert_eq!(chain0.aggstrategy, types_pathnodes::AGG_PLAIN);
        assert_eq!(chain0.numCols, 0);
        assert!(chain0.plan.lefttree.is_none());
        assert_eq!(agg.plan.lefttree.unwrap().node_tag(), NodeTag::T_SeqScan);
    }

    // SELECT val, pk, count(*) FROM t GROUP BY GROUPING SETS ((val),(pk)):
    // two rollups -> top Agg for (val) plus a one-element chain for (pk)
    // with a vestigial stripped Sort.
    #[test]
    fn grouping_sets_two_rollups_build_chain() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle1 = Node::mk_target_entry(mcx, val, 1, Some("val"), false).unwrap();
        let tle2 = Node::mk_target_entry(mcx, pk, 2, Some("pk"), false).unwrap();
        // SAFETY: freshly built tlist; no other reference is live.
        unsafe {
            tle1.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
        }
        .unwrap();
        // SAFETY: as above.
        unsafe {
            tle2.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 2)
        }
        .unwrap();
        let tle3 = Node::mk_target_entry(mcx, mk_count(mcx), 3, Some("count"), false).unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, tle2).unwrap();
        tlist.lappend(mcx, tle3).unwrap();
        parse.targetList = tlist;
        parse.hasAggs = true;
        let mut gc = NodeList::make1(mcx, mk_sgc(mcx, 1)).unwrap();
        gc.lappend(mcx, mk_sgc(mcx, 2)).unwrap();
        parse.groupClause = gc;
        let simple = |r: i32| {
            Node::mk(
                mcx,
                GroupingSet {
                    kind: GroupingSetKind::GROUPING_SET_SIMPLE,
                    content: NodeList::make1(mcx, Node::mk_integer(mcx, r).unwrap()).unwrap(),
                    location: -1,
                },
            )
            .unwrap()
        };
        let mut content = NodeList::make1(mcx, simple(1)).unwrap();
        content.lappend(mcx, simple(2)).unwrap();
        let sets = Node::mk(
            mcx,
            GroupingSet {
                kind: GroupingSetKind::GROUPING_SET_SETS,
                content,
                location: -1,
            },
        )
        .unwrap();
        parse.groupingSets = NodeList::make1(mcx, sets).unwrap();

        crate::gucs::set_enable_hashagg(false);
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val, pk, count(*) FROM t GROUP BY GROUPING SETS ((val),(pk))",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        );
        crate::gucs::set_enable_hashagg(true);
        let stmt = stmt.unwrap();
        let plan = stmt.planTree.unwrap();
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_SORTED);
        assert_eq!(agg.numCols, 1);
        assert_eq!(agg.grpColIdx, &[1i16]);
        let gsets: Vec<Vec<i32>> =
            agg.groupingSets.iter().map(|n| n.as_int_list().unwrap().iter().collect()).collect();
        assert_eq!(gsets, vec![vec![0]]);
        // First phase covers (val): 200 default groups; the (pk) phase adds
        // the unique-index estimate of 10000, so 10200 rows total.
        assert_eq!(agg.numGroups, 200);
        assert_eq!(agg.plan.plan_rows, 10200.0);

        assert_eq!(agg.chain.len(), 1);
        let chain_agg = agg.chain.nth(0).as_agg().unwrap();
        assert_eq!(chain_agg.aggstrategy, types_pathnodes::AGG_SORTED);
        assert_eq!(chain_agg.numCols, 1);
        assert_eq!(chain_agg.grpColIdx, &[2i16]);
        assert!(chain_agg.plan.targetlist.is_nil() && chain_agg.plan.qual.is_nil());
        let chain_gsets: Vec<Vec<i32>> = chain_agg
            .groupingSets
            .iter()
            .map(|n| n.as_int_list().unwrap().iter().collect())
            .collect();
        assert_eq!(chain_gsets, vec![vec![0]]);
        // The vestigial Sort: keyed on pk's column, stripped of tlist/child.
        let vsort = chain_agg.plan.lefttree.unwrap();
        let vs = vsort.as_sort().unwrap();
        assert_eq!(vs.sortColIdx, &[2i16]);
        assert!(vs.plan.targetlist.is_nil() && vs.plan.lefttree.is_none());

        // The real input: Sort(val) over SeqScan.
        let sort = agg.plan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        assert_eq!(sort.as_sort().unwrap().sortColIdx, &[1i16]);
        assert_eq!(
            sort.as_plan().unwrap().lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
    }

    // GROUP BY (): grouping sets [EMPTY] flow the ordinary path and collapse
    // to a single AGG_PLAIN phase with groupingSets [[]].
    #[test]
    fn group_by_empty_set_plans_plain_agg() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        ensure_work_mem();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let tle = Node::mk_target_entry(mcx, mk_count(mcx), 1, Some("count"), false).unwrap();
        parse.targetList = NodeList::make1(mcx, tle).unwrap();
        parse.hasAggs = true;
        let empty = Node::mk(
            mcx,
            GroupingSet {
                kind: GroupingSetKind::GROUPING_SET_EMPTY,
                content: NodeList::nil(),
                location: -1,
            },
        )
        .unwrap();
        parse.groupingSets = NodeList::make1(mcx, empty).unwrap();

        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT count(*) FROM t GROUP BY ()",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        let plan = stmt.planTree.unwrap();
        let agg = plan.as_agg().unwrap();
        assert_eq!(agg.aggstrategy, types_pathnodes::AGG_PLAIN);
        assert_eq!(agg.numCols, 0);
        let gsets: Vec<Vec<i32>> =
            agg.groupingSets.iter().map(|n| n.as_int_list().unwrap().iter().collect()).collect();
        assert_eq!(gsets, vec![Vec::<i32>::new()]);
        assert!(agg.chain.is_nil());
        assert_eq!(agg.plan.plan_rows, 1.0);
        assert_eq!(
            agg.plan.lefttree.unwrap().node_tag(),
            NodeTag::T_SeqScan
        );
    }
}

mod short_varlena {
    use super::*;

    fn short_text(payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() + 1 <= 0x7F);
        let mut v = vec![(((payload.len() + 1) as u8) << 1) | 0x01];
        v.extend_from_slice(payload);
        v
    }

    fn long_text(payload: &[u8]) -> Vec<u8> {
        let total = 4 + payload.len();
        let mut v = ((total as u32) << 2).to_ne_bytes().to_vec();
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn payload_reads_short_and_4b_headers() {
        let s = short_text(b"abc");
        let l = long_text(b"abc");
        let ds = Datum::from_usize(s.as_ptr() as usize);
        let dl = Datum::from_usize(l.as_ptr() as usize);
        assert_eq!(crate::selfuncs::varlena_datum_payload(ds), b"abc");
        assert_eq!(crate::selfuncs::varlena_datum_payload(dl), b"abc");
    }

    #[test]
    fn image_any_expands_short_to_4b() {
        let ctx = MemoryContext::new("image-any-test");
        let mcx = ctx.mcx();
        let s = short_text(b"hello");
        let l = long_text(b"hello");
        let expanded =
            crate::selfuncs::varlena_image_any(mcx, Datum::from_usize(s.as_ptr() as usize))
                .unwrap();
        assert_eq!(expanded, &l[..]);
        let borrowed =
            crate::selfuncs::varlena_image_any(mcx, Datum::from_usize(l.as_ptr() as usize))
                .unwrap();
        assert_eq!(borrowed.as_ptr(), l.as_ptr());
    }

    #[test]
    fn endpoint_copy_reads_short_header_size() {
        let ctx = MemoryContext::new("endpoint-copy-test");
        let mcx = ctx.mcx();
        let s = short_text(b"xy");
        let out = crate::selfuncs::endpoint_datum_copy(
            mcx,
            Datum::from_usize(s.as_ptr() as usize),
            false,
            -1,
        )
        .unwrap();
        let p = out.as_usize() as *const u8;
        let copied = unsafe { core::slice::from_raw_parts(p, s.len()) };
        assert_eq!(copied, &s[..]);
    }
}

mod srf_split {
    use super::*;
    use types_nodes::primnodes::{CoercionForm, FuncExpr, OpExpr};

    fn i32c(mcx: Mcx<'_>, v: i32) -> Node<'_> {
        Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap()
    }

    fn gs_call<'mcx>(mcx: Mcx<'mcx>, a: Node<'mcx>, b: Node<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            FuncExpr {
                funcid: 1067,
                funcresulttype: 23,
                funcretset: true,
                funcvariadic: false,
                funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                funccollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, a, b).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    fn int4pl<'mcx>(mcx: Mcx<'mcx>, a: Node<'mcx>, b: Node<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            OpExpr {
                opno: 551,
                opfuncid: 177,
                opresulttype: 23,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, a, b).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    fn pathtarget_of<'mcx>(
        run: &mut crate::run::PlannerRun<'mcx>,
        items: &[(Node<'mcx>, u32)],
    ) -> types_pathnodes::PtId {
        let mut t = types_pathnodes::PathTarget::new(run.mcx);
        let mut any = false;
        for &(node, sgref) in items {
            let id = run.intern_expr(node);
            t.exprs.push(id);
            t.sortgrouprefs.push(sgref);
            any |= sgref != 0;
        }
        if !any {
            t.sortgrouprefs.clear();
        }
        run.root.alloc_pathtarget(t)
    }

    #[test]
    fn nested_srf_splits_into_level_chain() {
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let inner = gs_call(mcx, i32c(mcx, 1), var);
        let outer = gs_call(mcx, i32c(mcx, 1), inner);
        let tid = pathtarget_of(&mut run, &[(outer, 0)]);

        let (targets, flags) = crate::srf::split_pathtarget_at_srfs(&mut run, tid, None).unwrap();
        assert_eq!(flags.as_slice(), &[false, true, true]);
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[2], tid);
        let t0 = run.root.pathtarget(targets[0]);
        assert_eq!(t0.exprs.len(), 1);
        assert!(types_nodes::equal(*run.root.expr_node(t0.exprs[0]), var));
        let t1 = run.root.pathtarget(targets[1]);
        assert_eq!(t1.exprs.len(), 1);
        assert!(types_nodes::equal(*run.root.expr_node(t1.exprs[0]), inner));
        assert_eq!(run.root.pathtarget(targets[1]).width, 4);
    }

    #[test]
    fn srf_below_top_gets_extra_projection_level_and_merged_sortgroupref() {
        // tsrf.sql: select generate_series(1,3)+1 order by generate_series(1,3);
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let srf = gs_call(mcx, i32c(mcx, 1), i32c(mcx, 3));
        let plus = int4pl(mcx, srf, i32c(mcx, 1));
        let tid = pathtarget_of(&mut run, &[(plus, 0), (srf, 1)]);

        let (targets, flags) = crate::srf::split_pathtarget_at_srfs(&mut run, tid, None).unwrap();
        assert_eq!(flags.as_slice(), &[false, true, false]);
        assert_eq!(targets[2], tid);
        let t0 = run.root.pathtarget(targets[0]);
        assert!(t0.exprs.is_empty());
        let t1 = run.root.pathtarget(targets[1]);
        assert_eq!(t1.exprs.len(), 1);
        assert!(types_nodes::equal(*run.root.expr_node(t1.exprs[0]), srf));
        assert_eq!(t1.sortgrouprefs.as_slice(), &[1u32]);
    }

    #[test]
    fn top_and_nested_srfs_share_levels() {
        // tlist.c example: srf1(x), srf2(srf3(y)) — level 1 evaluates
        // srf1 and srf3, level 2 the original target.
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let y = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let srf1 = gs_call(mcx, i32c(mcx, 1), x);
        let srf3 = gs_call(mcx, i32c(mcx, 2), y);
        let srf2 = gs_call(mcx, i32c(mcx, 3), srf3);
        let tid = pathtarget_of(&mut run, &[(srf1, 0), (srf2, 0)]);

        let (targets, flags) = crate::srf::split_pathtarget_at_srfs(&mut run, tid, None).unwrap();
        assert_eq!(flags.as_slice(), &[false, true, true]);
        let t0 = run.root.pathtarget(targets[0]);
        assert_eq!(t0.exprs.len(), 2);
        assert!(types_nodes::equal(*run.root.expr_node(t0.exprs[0]), x));
        assert!(types_nodes::equal(*run.root.expr_node(t0.exprs[1]), y));
        let t1 = run.root.pathtarget(targets[1]);
        assert_eq!(t1.exprs.len(), 2);
        assert!(types_nodes::equal(*run.root.expr_node(t1.exprs[0]), srf1));
        assert!(types_nodes::equal(*run.root.expr_node(t1.exprs[1]), srf3));
    }

    #[test]
    fn srf_below_top_plans_result_over_projectset() {
        let cx = cx();
        let mcx = cx.mcx();
        let srf = gs_call(mcx, i32c(mcx, 1), i32c(mcx, 3));
        let plus = int4pl(mcx, srf, i32c(mcx, 1));
        let tle = Node::mk_target_entry(mcx, plus, 1, Some("?column?"), false).unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            hasTargetSRFs: true,
            jointree: Some(jointree),
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 30,
            ..Query::default()
        };
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT generate_series(1,3)+1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        assert_eq!(plan.node_tag(), NodeTag::T_Result);
        let top = plan.as_result().unwrap();
        assert_eq!(top.plan.targetlist.len(), 1);
        let tle0 = top.plan.targetlist.nth(0).as_target_entry().unwrap();
        let op = tle0.expr.as_op_expr().unwrap();
        let v = op.args.nth(0).as_var().unwrap();
        assert_eq!(v.varno, types_nodes::primnodes::OUTER_VAR);
        assert_eq!(v.varattno, 1);

        let ps = top.plan.lefttree.unwrap();
        assert_eq!(ps.node_tag(), NodeTag::T_ProjectSet);
        let psn = ps.as_project_set().unwrap();
        assert_eq!(psn.plan.plan_rows, 1000.0);
        assert_eq!(psn.plan.targetlist.len(), 1);
        let ps_tle = psn.plan.targetlist.nth(0).as_target_entry().unwrap();
        let fe = ps_tle.expr.as_func_expr().unwrap();
        assert!(fe.funcretset);

        let bottom = psn.plan.lefttree.unwrap();
        assert_eq!(bottom.node_tag(), NodeTag::T_Result);
        assert!(bottom.as_result().unwrap().plan.targetlist.is_nil());
    }

    fn srf_tle<'mcx>(mcx: Mcx<'mcx>, resno: i16) -> Node<'mcx> {
        let srf = gs_call(mcx, i32c(mcx, 1), i32c(mcx, 2));
        Node::mk_target_entry(mcx, srf, resno, Some("generate_series"), false).unwrap()
    }

    fn sortgroup_val_tle(mcx: Mcx<'_>) -> Node<'_> {
        let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: val,
                resno: 1,
                resname: Some("val"),
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap()
    }

    fn val_sgc(mcx: Mcx<'_>) -> Node<'_> {
        Node::mk(
            mcx,
            types_nodes::parsenodes::SortGroupClause {
                tleSortGroupRef: 1,
                eqop: INT4EQ_OP,
                sortop: INT4_LT_OP,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap()
    }

    #[test]
    fn srf_with_group_by_plans_projectset_above_agg() {
        // C: SELECT val, generate_series(1,2) FROM t GROUP BY val
        //    -> ProjectSet -> HashAggregate -> Seq Scan.
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let mut tlist = NodeList::make1(mcx, sortgroup_val_tle(mcx)).unwrap();
        tlist.lappend(mcx, srf_tle(mcx, 2)).unwrap();
        parse.targetList = tlist;
        parse.hasTargetSRFs = true;
        parse.groupClause = NodeList::make1(mcx, val_sgc(mcx)).unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val, generate_series(1,2) FROM t GROUP BY val",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let ps = plan.as_project_set().expect("ProjectSet root");
        assert_eq!(ps.plan.targetlist.len(), 2);
        let srf = ps.plan.targetlist.nth(1).as_target_entry().unwrap();
        assert!(srf.expr.as_func_expr().unwrap().funcretset);
        let agg = ps.plan.lefttree.unwrap();
        assert_eq!(agg.node_tag(), NodeTag::T_Agg);
        assert_eq!(agg.as_agg().unwrap().numCols, 1);
    }

    #[test]
    fn srf_with_window_plans_projectset_above_windowagg() {
        // C: SELECT count(*) OVER (), generate_series(1,2) FROM t
        //    -> ProjectSet -> WindowAgg -> Seq Scan.
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let wfunc = Node::mk(
            mcx,
            types_nodes::primnodes::WindowFunc {
                winfnoid: 2803, // count(*)
                wintype: 20,
                winref: 1,
                winstar: true,
                winagg: true,
                ..types_nodes::primnodes::WindowFunc::default()
            },
        )
        .unwrap();
        let tle1 = Node::mk_target_entry(mcx, wfunc, 1, Some("count"), false).unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, srf_tle(mcx, 2)).unwrap();
        parse.targetList = tlist;
        parse.hasWindowFuncs = true;
        parse.hasTargetSRFs = true;
        let wc = Node::mk(
            mcx,
            types_nodes::parsenodes::WindowClause {
                frameOptions: types_nodes::rawnodes::FRAMEOPTION_DEFAULTS,
                winref: 1,
                ..types_nodes::parsenodes::WindowClause::default()
            },
        )
        .unwrap();
        parse.windowClause = NodeList::make1(mcx, wc).unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT count(*) OVER (), generate_series(1,2) FROM t",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let ps = plan.as_project_set().expect("ProjectSet root");
        assert_eq!(ps.plan.targetlist.len(), 2);
        let srf = ps.plan.targetlist.nth(1).as_target_entry().unwrap();
        assert!(srf.expr.as_func_expr().unwrap().funcretset);
        let wagg = ps.plan.lefttree.unwrap();
        assert_eq!(wagg.node_tag(), NodeTag::T_WindowAgg);
    }

    #[test]
    fn srf_with_order_by_postpones_projectset_above_sort() {
        // C: SELECT val, generate_series(1,2) FROM t ORDER BY val
        //    -> ProjectSet -> Sort -> Seq Scan (SRF postponed past the sort
        //    via make_sort_input_target, then split at sort_input_target).
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let mut tlist = NodeList::make1(mcx, sortgroup_val_tle(mcx)).unwrap();
        tlist.lappend(mcx, srf_tle(mcx, 2)).unwrap();
        parse.targetList = tlist;
        parse.hasTargetSRFs = true;
        parse.sortClause = NodeList::make1(mcx, val_sgc(mcx)).unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT val, generate_series(1,2) FROM t ORDER BY val",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let ps = plan.as_project_set().expect("ProjectSet root");
        assert_eq!(ps.plan.targetlist.len(), 2);
        let srf = ps.plan.targetlist.nth(1).as_target_entry().unwrap();
        assert!(srf.expr.as_func_expr().unwrap().funcretset);
        let sort = ps.plan.lefttree.unwrap();
        assert_eq!(sort.node_tag(), NodeTag::T_Sort);
        let sscan = sort.as_sort().unwrap().plan.lefttree.unwrap();
        assert_eq!(sscan.node_tag(), NodeTag::T_SeqScan);
    }

    #[test]
    fn srf_with_degenerate_order_by_keeps_const_above_physical_scan() {
        // C: SELECT 'foo', generate_series(1,2) FROM t ORDER BY 1
        //    -> ProjectSet -> Seq Scan (redundant const pathkey, no Sort).
        //    The scan keeps its physical tlist and the ProjectSet recomputes
        //    the Const: search_indexed_tlist_for_non_var never replaces a
        //    Const with a Var (setrefs.c).
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        let mcx = cx.mcx();
        let mut parse = table_query(mcx, None);
        let konst =
            Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(42), false, true).unwrap();
        let tle1 = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: konst,
                resno: 1,
                resname: Some("f"),
                ressortgroupref: 1,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap();
        let mut tlist = NodeList::make1(mcx, tle1).unwrap();
        tlist.lappend(mcx, srf_tle(mcx, 2)).unwrap();
        parse.targetList = tlist;
        parse.hasTargetSRFs = true;
        parse.sortClause = NodeList::make1(mcx, val_sgc(mcx)).unwrap();
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT 42 AS f, generate_series(1,2) FROM t ORDER BY 1",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();

        let plan = stmt.planTree.unwrap();
        let ps = plan.as_project_set().expect("ProjectSet root");
        assert_eq!(ps.plan.targetlist.len(), 2);
        let f = ps.plan.targetlist.nth(0).as_target_entry().unwrap();
        assert_eq!(f.expr.node_tag(), NodeTag::T_Const, "Const recomputed, not read from below");
        let sscan = ps.plan.lefttree.unwrap();
        assert_eq!(sscan.node_tag(), NodeTag::T_SeqScan);
        let scan_tlist = &sscan.as_plan().unwrap().targetlist;
        assert_eq!(scan_tlist.len(), 2, "physical tlist (pk, val), not the const");
        for tle in scan_tlist.iter() {
            let tle = tle.as_target_entry().unwrap();
            assert_eq!(tle.expr.node_tag(), NodeTag::T_Var);
        }
    }
}

mod pull_var_walker_vocab {
    use super::*;
    use mcx::PgVec;

    use crate::initsplan::pull_var_nodes;

    fn two_var_args(mcx: Mcx<'_>) -> NodeList<'_> {
        let v1 = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let v2 = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        NodeList::make2(mcx, v1, v2).unwrap()
    }

    #[test]
    fn null_if_expr_yields_both_vars() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let n = Node::mk(
            mcx,
            types_nodes::primnodes::NullIfExpr {
                opno: 96,
                opfuncid: 65,
                opresulttype: 23,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: two_var_args(mcx),
                location: -1,
            },
        )
        .unwrap();
        let mut out: PgVec<'_, Node<'_>> = PgVec::new_in(mcx);
        pull_var_nodes(n, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].as_var().unwrap().varattno, 1);
        assert_eq!(out[1].as_var().unwrap().varattno, 2);
    }

    #[test]
    fn field_store_yields_arg_and_newval_vars() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let arg = Node::mk_var(mcx, 1, 1, 2249, -1, 0, 0).unwrap();
        let newval = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
        let n = Node::mk(
            mcx,
            types_nodes::primnodes::FieldStore {
                arg,
                newvals: NodeList::make1(mcx, newval).unwrap(),
                fieldnums: types_nodes::list::IntList::make1(mcx, 1).unwrap(),
                resulttype: 2249,
            },
        )
        .unwrap();
        let mut out: PgVec<'_, Node<'_>> = PgVec::new_in(mcx);
        pull_var_nodes(n, &mut out);
        assert_eq!(out.len(), 2);
    }
}

// LATERAL replacement legs of the subquery-pullup family: distilled from the
// join-suite queries failing 'no relation entry for relid N'
// (notes/join-using-bundle.md residual fingerprint).
mod lateral_pullup {
    use super::*;

    fn rel_rte(mcx: Mcx<'_>) -> Node<'_> {
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = TBL;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        rte.seal()
    }

    fn subquery_rte<'mcx>(
        mcx: Mcx<'mcx>,
        subquery: Query<'mcx>,
        name: &'mcx str,
        colnames: &[&'mcx str],
        lateral: bool,
    ) -> Node<'mcx> {
        let mut cols = NodeList::nil();
        for c in colnames {
            cols.lappend(mcx, Node::mk_string(mcx, c).unwrap()).unwrap();
        }
        let eref = alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias { aliasname: Some(name), colnames: cols },
        )
        .unwrap();
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_SUBQUERY;
        rte.subquery = Some(alloc_leak_in(mcx, subquery).unwrap());
        rte.lateral = lateral;
        rte.eref = Some(eref);
        rte.alias = Some(eref);
        rte.inFromCl = true;
        rte.seal()
    }

    // The analyzer's output for `(VALUES (<expr>))` in FROM: a subquery
    // wrapping a single-row RTE_VALUES.
    fn values_wrapper_query<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> Query<'mcx> {
        let row = Node::mk_list(mcx, NodeList::make1(mcx, expr).unwrap()).unwrap();
        let values_lists = NodeList::make1(mcx, row).unwrap();
        let colname = Node::mk_string(mcx, "column1").unwrap();
        let eref = alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias {
                aliasname: Some("*VALUES*"),
                colnames: NodeList::make1(mcx, colname).unwrap(),
            },
        )
        .unwrap();
        let mut vrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        vrte.rtekind = RTEKind::RTE_VALUES;
        vrte.values_lists = values_lists;
        vrte.eref = Some(eref);
        let rtable = NodeList::make1(mcx, vrte.seal()).unwrap();
        let jointree = alloc_leak_in(
            mcx,
            FromExpr {
                fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
                quals: None,
            },
        )
        .unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("column1"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            ..Query::default()
        }
    }

    // `SELECT ss.y FROM t a, t b, LATERAL (VALUES (a.pk)) ss(y)
    //  WHERE b.pk = ss.y` — C: pull_up_simple_values inside the subquery,
    // then pull_up_simple_subquery; fully flattens to a join of a and b on
    // b.pk = a.pk (join.sql `lateral (values(a.unique1))` shape).
    #[test]
    fn lateral_values_single_row_flattens() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let a_pk_up = Node::mk_var(mcx, 1, 1, 23, -1, 0, 1).unwrap();
        let ss = values_wrapper_query(mcx, a_pk_up);
        let mut rtable = NodeList::make1(mcx, rel_rte(mcx)).unwrap();
        rtable.lappend(mcx, rel_rte(mcx)).unwrap();
        rtable.lappend(mcx, subquery_rte(mcx, ss, "ss", &["y"], true)).unwrap();
        let mut fromlist = NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 2).unwrap()).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 3).unwrap()).unwrap();
        let b_pk = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let ss_y = Node::mk_var(mcx, 3, 1, 23, -1, 0, 0).unwrap();
        let quals = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4EQ_OP,
                opfuncid: INT4EQ_PROC,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, b_pk, ss_y).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let jointree = alloc_leak_in(mcx, FromExpr { fromlist, quals: Some(quals) }).unwrap();
        let ss_y_out = Node::mk_var(mcx, 3, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, ss_y_out, 1, Some("y"), false).unwrap();
        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 70,
            ..Query::default()
        };
        let stmt = planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT ss.y FROM t a, t b, LATERAL (VALUES (a.pk)) ss(y) WHERE b.pk = ss.y",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
        // C flattens completely: a join of the two heap scans, no Values Scan.
        let mut stack = vec![stmt.planTree.unwrap()];
        while let Some(p) = stack.pop() {
            assert_ne!(p.node_tag(), NodeTag::T_ValuesScan, "VALUES survived pull-up");
            let plan = p.as_plan().unwrap();
            if let Some(l) = plan.lefttree {
                stack.push(l);
            }
            if let Some(r) = plan.righttree {
                stack.push(r);
            }
        }
    }

    // `SELECT ss2.y FROM (SELECT a.pk AS x FROM t a) ss1,
    //  LATERAL (VALUES (ss1.x)) ss2(y)` — the VALUES list references a
    // pulled-up subquery's output (join.sql `lateral (values(x))` shape).
    #[test]
    fn lateral_values_over_pulled_up_subquery_output() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();

        // ss1 = SELECT a.pk AS x FROM t a
        let ss1 = {
            let rtable = NodeList::make1(mcx, rel_rte(mcx)).unwrap();
            let jointree = alloc_leak_in(
                mcx,
                FromExpr {
                    fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap())
                        .unwrap(),
                    quals: None,
                },
            )
            .unwrap();
            let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
            let tle = Node::mk_target_entry(mcx, pk, 1, Some("x"), false).unwrap();
            Query {
                commandType: CmdType::CMD_SELECT,
                jointree: Some(jointree),
                rtable,
                targetList: NodeList::make1(mcx, tle).unwrap(),
                ..Query::default()
            }
        };
        let ss1_x_up = Node::mk_var(mcx, 1, 1, 23, -1, 0, 1).unwrap();
        let ss2 = values_wrapper_query(mcx, ss1_x_up);
        let mut rtable =
            NodeList::make1(mcx, subquery_rte(mcx, ss1, "ss1", &["x"], false)).unwrap();
        rtable.lappend(mcx, subquery_rte(mcx, ss2, "ss2", &["y"], true)).unwrap();
        let mut fromlist = NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 2).unwrap()).unwrap();
        let jointree = alloc_leak_in(mcx, FromExpr { fromlist, quals: None }).unwrap();
        let y = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, y, 1, Some("y"), false).unwrap();
        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 80,
            ..Query::default()
        };
        planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT ss2.y FROM (SELECT a.pk AS x FROM t a) ss1, LATERAL (VALUES (ss1.x)) ss2(y)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
    }

    // `SELECT ss1.x, ss2.y, ss3.z FROM (SELECT 1 AS x) ss1
    //  LEFT JOIN (SELECT 2 AS y) ss2 ON true,
    //  LATERAL (SELECT ss2.y LIMIT 1) ss3(z)` — pull-up of ss2 under the
    // outer join replaces ss2.y with a PHV-wrapped Const; the replacement
    // must descend into the LATERAL subquery at sublevel 1 (C wrap_non_vars,
    // join.sql `lateral (select ss2.y limit 1)` shape).
    #[test]
    fn phv_replacement_reaches_lateral_subquery() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();

        let select_const = |v: i32, name: &'static str| {
            let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
            let tle = Node::mk_target_entry(mcx, konst, 1, Some(name), false).unwrap();
            let jointree =
                alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
            Query {
                commandType: CmdType::CMD_SELECT,
                jointree: Some(jointree),
                targetList: NodeList::make1(mcx, tle).unwrap(),
                ..Query::default()
            }
        };
        let nulled_var = |varno: i32, attno: i16, levelsup: u32| {
            let mut nulling = types_nodes::Bitmapset::empty();
            nulling.add_member(mcx, 3).unwrap();
            Node::mk(
                mcx,
                types_nodes::primnodes::Var {
                    varno,
                    varattno: attno,
                    vartype: 23,
                    vartypmod: -1,
                    varnullingrels: nulling,
                    varlevelsup: levelsup,
                    varnosyn: varno as u32,
                    varattnosyn: attno,
                    location: -1,
                    ..Default::default()
                },
            )
            .unwrap()
        };

        // ss3 = SELECT ss2.y AS z LIMIT 1 (LIMIT blocks pull-up).
        let ss3 = {
            let tle =
                Node::mk_target_entry(mcx, nulled_var(2, 1, 1), 1, Some("z"), false).unwrap();
            let jointree =
                alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
            Query {
                commandType: CmdType::CMD_SELECT,
                jointree: Some(jointree),
                targetList: NodeList::make1(mcx, tle).unwrap(),
                limitCount: Some(
                    Node::mk_const(mcx, 20, -1, 0, 8, Datum::from_i64(1), false, true).unwrap(),
                ),
                limitOption: types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT,
                ..Query::default()
            }
        };

        let mut rtable =
            NodeList::make1(mcx, subquery_rte(mcx, select_const(1, "x"), "ss1", &["x"], false))
                .unwrap();
        rtable
            .lappend(mcx, subquery_rte(mcx, select_const(2, "y"), "ss2", &["y"], false))
            .unwrap();
        // RTE_JOIN for `ss1 LEFT JOIN ss2 ON true`.
        {
            let mut joinaliasvars =
                NodeList::make1(mcx, Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap()).unwrap();
            joinaliasvars.lappend(mcx, nulled_var(2, 1, 0)).unwrap();
            let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "x").unwrap()).unwrap();
            colnames.lappend(mcx, Node::mk_string(mcx, "y").unwrap()).unwrap();
            let eref = alloc_leak_in(
                mcx,
                types_nodes::primnodes::Alias { aliasname: Some("unnamed_join"), colnames },
            )
            .unwrap();
            let mut leftcols = types_nodes::list::IntList::nil();
            let mut rightcols = types_nodes::list::IntList::nil();
            leftcols.lappend(mcx, 1).unwrap();
            rightcols.lappend(mcx, 1).unwrap();
            let mut jrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            jrte.rtekind = RTEKind::RTE_JOIN;
            jrte.jointype = types_nodes::JoinType::JOIN_LEFT;
            jrte.joinaliasvars = joinaliasvars;
            jrte.joinleftcols = leftcols;
            jrte.joinrightcols = rightcols;
            jrte.eref = Some(eref);
            jrte.inFromCl = true;
            rtable.lappend(mcx, jrte.seal()).unwrap();
        }
        rtable.lappend(mcx, subquery_rte(mcx, ss3, "ss3", &["z"], true)).unwrap();

        let join = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_LEFT,
                isNatural: false,
                larg: Node::mk_range_tbl_ref(mcx, 1).unwrap(),
                rarg: Node::mk_range_tbl_ref(mcx, 2).unwrap(),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(
                    Node::mk_const(mcx, 16, -1, 0, 1, Datum::from_bool(true), false, true)
                        .unwrap(),
                ),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        let mut fromlist = NodeList::make1(mcx, join).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 4).unwrap()).unwrap();
        let jointree = alloc_leak_in(mcx, FromExpr { fromlist, quals: None }).unwrap();

        let mut tlist = NodeList::nil();
        let x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        tlist.lappend(mcx, Node::mk_target_entry(mcx, x, 1, Some("x"), false).unwrap()).unwrap();
        tlist
            .lappend(
                mcx,
                Node::mk_target_entry(mcx, nulled_var(2, 1, 0), 2, Some("y"), false).unwrap(),
            )
            .unwrap();
        let z = Node::mk_var(mcx, 4, 1, 23, -1, 0, 0).unwrap();
        tlist.lappend(mcx, Node::mk_target_entry(mcx, z, 3, Some("z"), false).unwrap()).unwrap();

        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: tlist,
            stmt_location: 0,
            stmt_len: 120,
            ..Query::default()
        };
        planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT ss1.x, ss2.y, ss3.z FROM (SELECT 1 AS x) ss1 LEFT JOIN (SELECT 2 AS y) ss2 \
             ON true, LATERAL (SELECT ss2.y LIMIT 1) ss3(z)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
    }

    fn setop_leaf_rte<'mcx>(
        mcx: Mcx<'mcx>,
        subquery: Query<'mcx>,
        name: &'mcx str,
    ) -> Node<'mcx> {
        subquery_rte(mcx, subquery, name, &["vx"], false)
    }

    // UNION ALL setop Query over two single-column leaves.
    fn union_all_query<'mcx>(mcx: Mcx<'mcx>, left: Query<'mcx>, right: Query<'mcx>) -> Query<'mcx> {
        use types_nodes::list::{IntList, OidList};
        use types_nodes::parsenodes::{SetOperation, SetOperationStmt};
        let mut rtable = NodeList::make1(mcx, setop_leaf_rte(mcx, left, "*SELECT* 1")).unwrap();
        rtable.lappend(mcx, setop_leaf_rte(mcx, right, "*SELECT* 2")).unwrap();
        let stmt = Node::mk(
            mcx,
            SetOperationStmt {
                op: SetOperation::SETOP_UNION,
                all: true,
                larg: Some(Node::mk_range_tbl_ref(mcx, 1).unwrap()),
                rarg: Some(Node::mk_range_tbl_ref(mcx, 2).unwrap()),
                colTypes: OidList::make1(mcx, 23).unwrap(),
                colTypmods: IntList::make1(mcx, -1).unwrap(),
                colCollations: OidList::make1(mcx, 0).unwrap(),
                groupClauses: NodeList::nil(),
            },
        )
        .unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        let v = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, v, 1, Some("vx"), false).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            setOperations: Some(stmt),
            ..Query::default()
        }
    }

    // Empty-FROM leaf `SELECT <expr>`.
    fn leaf_query<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> Query<'mcx> {
        let tle = Node::mk_target_entry(mcx, expr, 1, Some("vx"), false).unwrap();
        let jointree =
            alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            jointree: Some(jointree),
            targetList: NodeList::make1(mcx, tle).unwrap(),
            ..Query::default()
        }
    }

    // `SELECT v.vx FROM t a, LATERAL (SELECT a.pk UNION ALL SELECT a.val)
    //  v(vx)` — UNION ALL inside a lateral subquery, leaves carrying uplevel
    // refs (join.sql `lateral (... union all ...)` shape).
    #[test]
    fn union_all_in_lateral_subquery_plans() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let a_pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 2).unwrap();
        let a_val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 2).unwrap();
        let v = union_all_query(mcx, leaf_query(mcx, a_pk), leaf_query(mcx, a_val));
        let mut rtable = NodeList::make1(mcx, rel_rte(mcx)).unwrap();
        rtable.lappend(mcx, subquery_rte(mcx, v, "v", &["vx"], true)).unwrap();
        let mut fromlist = NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 2).unwrap()).unwrap();
        let jointree = alloc_leak_in(mcx, FromExpr { fromlist, quals: None }).unwrap();
        let vx = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, vx, 1, Some("vx"), false).unwrap();
        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 70,
            ..Query::default()
        };
        planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT v.vx FROM t a, LATERAL (SELECT a.pk UNION ALL SELECT a.val) v(vx)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
    }

    // `SELECT v.vx FROM (SELECT 1 AS x) ss1 LEFT JOIN (SELECT 2 AS y) ss2
    //  ON true, LATERAL (SELECT ss1.x UNION ALL SELECT ss2.y) v(vx)` — the
    // PHV-wrapped replacement for ss2.y must reach the UNION ALL leaves at
    // sublevel 2 inside the lateral subquery.
    #[test]
    fn phv_replacement_reaches_union_all_in_lateral() {
        let _guc = crate::tests::GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cx = cx();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let mcx = cx.mcx();
        let select_const = |v: i32, name: &'static str| {
            let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
            let tle = Node::mk_target_entry(mcx, konst, 1, Some(name), false).unwrap();
            let jointree =
                alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
            Query {
                commandType: CmdType::CMD_SELECT,
                jointree: Some(jointree),
                targetList: NodeList::make1(mcx, tle).unwrap(),
                ..Query::default()
            }
        };
        let nulled_var = |varno: i32, attno: i16, levelsup: u32| {
            let mut nulling = types_nodes::Bitmapset::empty();
            nulling.add_member(mcx, 3).unwrap();
            Node::mk(
                mcx,
                types_nodes::primnodes::Var {
                    varno,
                    varattno: attno,
                    vartype: 23,
                    vartypmod: -1,
                    varnullingrels: nulling,
                    varlevelsup: levelsup,
                    varnosyn: varno as u32,
                    varattnosyn: attno,
                    location: -1,
                    ..Default::default()
                },
            )
            .unwrap()
        };
        let ss1_x = Node::mk_var(mcx, 1, 1, 23, -1, 0, 2).unwrap();
        let v = union_all_query(
            mcx,
            leaf_query(mcx, ss1_x),
            leaf_query(mcx, nulled_var(2, 1, 2)),
        );
        let mut rtable =
            NodeList::make1(mcx, subquery_rte(mcx, select_const(1, "x"), "ss1", &["x"], false))
                .unwrap();
        rtable
            .lappend(mcx, subquery_rte(mcx, select_const(2, "y"), "ss2", &["y"], false))
            .unwrap();
        {
            let mut joinaliasvars =
                NodeList::make1(mcx, Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap()).unwrap();
            joinaliasvars.lappend(mcx, nulled_var(2, 1, 0)).unwrap();
            let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "x").unwrap()).unwrap();
            colnames.lappend(mcx, Node::mk_string(mcx, "y").unwrap()).unwrap();
            let eref = alloc_leak_in(
                mcx,
                types_nodes::primnodes::Alias { aliasname: Some("unnamed_join"), colnames },
            )
            .unwrap();
            let mut leftcols = types_nodes::list::IntList::nil();
            let mut rightcols = types_nodes::list::IntList::nil();
            leftcols.lappend(mcx, 1).unwrap();
            rightcols.lappend(mcx, 1).unwrap();
            let mut jrte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            jrte.rtekind = RTEKind::RTE_JOIN;
            jrte.jointype = types_nodes::JoinType::JOIN_LEFT;
            jrte.joinaliasvars = joinaliasvars;
            jrte.joinleftcols = leftcols;
            jrte.joinrightcols = rightcols;
            jrte.eref = Some(eref);
            jrte.inFromCl = true;
            rtable.lappend(mcx, jrte.seal()).unwrap();
        }
        rtable.lappend(mcx, subquery_rte(mcx, v, "v", &["vx"], true)).unwrap();
        let join = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_LEFT,
                isNatural: false,
                larg: Node::mk_range_tbl_ref(mcx, 1).unwrap(),
                rarg: Node::mk_range_tbl_ref(mcx, 2).unwrap(),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(
                    Node::mk_const(mcx, 16, -1, 0, 1, Datum::from_bool(true), false, true)
                        .unwrap(),
                ),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        let mut fromlist = NodeList::make1(mcx, join).unwrap();
        fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, 4).unwrap()).unwrap();
        let jointree = alloc_leak_in(mcx, FromExpr { fromlist, quals: None }).unwrap();
        let vx = Node::mk_var(mcx, 4, 1, 23, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, vx, 1, Some("vx"), false).unwrap();
        let parse = Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 120,
            ..Query::default()
        };
        planner(
            mcx,
            leak_q(mcx, parse),
            "SELECT v.vx FROM (SELECT 1 AS x) ss1 LEFT JOIN (SELECT 2 AS y) ss2 ON true, \
             LATERAL (SELECT ss1.x UNION ALL SELECT ss2.y) v(vx)",
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )
        .unwrap();
    }
}


// group_keys_reorder_by_pathkeys (pathkeys.c:357-452): matching is confined to
// the leading num_groupby_pathkeys of group_pathkeys, and a pathkey whose EC
// lacks a sortref or a matching processed group clause ends the usable prefix
// (C commit 1349d27) — distilled from the aggregates group_agg_pk trio.
mod group_keys_reorder {
    use super::*;
    use mcx::PgVec;
    use types_pathnodes::{NodeId, PathKey, COMPARE_LT};

    fn sgc<'mcx>(run: &mut crate::run::PlannerRun<'mcx>, sortref: u32) -> NodeId {
        let n = Node::mk(
            run.mcx,
            types_nodes::parsenodes::SortGroupClause {
                tleSortGroupRef: sortref,
                eqop: 96,
                sortop: 97,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap();
        run.root.alloc_expr_node(n)
    }

    fn pk_with_sortref<'mcx>(run: &mut crate::run::PlannerRun<'mcx>, sortref: u32) -> PathKey {
        let mut ec = types_pathnodes::EquivalenceClass::new(run.mcx);
        ec.ec_sortref = sortref;
        let id = run.root.alloc_ec(ec);
        PathKey {
            pk_eclass: Some(id),
            pk_opfamily: 1976,
            pk_cmptype: COMPARE_LT,
            pk_nulls_first: false,
        }
    }

    #[test]
    fn pathkey_ec_without_sortref_ends_prefix() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let a = pk_with_sortref(&mut run, 0);
        let b = pk_with_sortref(&mut run, 2);
        let (ca, cb) = (sgc(&mut run, 1), sgc(&mut run, 2));
        let mut pks: PgVec<'_, PathKey> = PgVec::new_in(mcx);
        pks.extend([a, b]);
        let mut clauses: PgVec<'_, NodeId> = PgVec::new_in(mcx);
        clauses.extend([ca, cb]);
        let n = crate::pathkeys::group_keys_reorder_by_pathkeys(&run, &[a], &mut pks, &mut clauses, 2);
        assert_eq!(n, 0);
        assert_eq!(&pks[..], &[a, b]);
        assert_eq!(&clauses[..], &[ca, cb]);
    }

    #[test]
    fn sortref_without_matching_clause_ends_prefix() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let a = pk_with_sortref(&mut run, 5);
        let b = pk_with_sortref(&mut run, 2);
        let (ca, cb) = (sgc(&mut run, 1), sgc(&mut run, 2));
        let mut pks: PgVec<'_, PathKey> = PgVec::new_in(mcx);
        pks.extend([a, b]);
        let mut clauses: PgVec<'_, NodeId> = PgVec::new_in(mcx);
        clauses.extend([ca, cb]);
        let n = crate::pathkeys::group_keys_reorder_by_pathkeys(&run, &[a], &mut pks, &mut clauses, 2);
        assert_eq!(n, 0);
        assert_eq!(&pks[..], &[a, b]);
    }

    #[test]
    fn reorders_leading_group_keys_to_match_path() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let a = pk_with_sortref(&mut run, 1);
        let b = pk_with_sortref(&mut run, 2);
        let (ca, cb) = (sgc(&mut run, 1), sgc(&mut run, 2));
        let mut pks: PgVec<'_, PathKey> = PgVec::new_in(mcx);
        pks.extend([a, b]);
        let mut clauses: PgVec<'_, NodeId> = PgVec::new_in(mcx);
        clauses.extend([ca, cb]);
        let n = crate::pathkeys::group_keys_reorder_by_pathkeys(&run, &[b], &mut pks, &mut clauses, 2);
        assert_eq!(n, 1);
        assert_eq!(&pks[..], &[b, a]);
        assert_eq!(&clauses[..], &[cb, ca]);
    }

    #[test]
    fn aggregate_pathkey_tail_is_not_matched() {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let a = pk_with_sortref(&mut run, 1);
        let b = pk_with_sortref(&mut run, 2);
        // Aggregate-ORDER-BY pathkey past num_groupby_pathkeys; a stale clause
        // with its sortref exists, so the head restriction is what stops it.
        let agg = pk_with_sortref(&mut run, 7);
        let (ca, cb, cagg) = (sgc(&mut run, 1), sgc(&mut run, 2), sgc(&mut run, 7));
        let mut pks: PgVec<'_, PathKey> = PgVec::new_in(mcx);
        pks.extend([a, b, agg]);
        let mut clauses: PgVec<'_, NodeId> = PgVec::new_in(mcx);
        clauses.extend([ca, cb, cagg]);
        let n =
            crate::pathkeys::group_keys_reorder_by_pathkeys(&run, &[agg], &mut pks, &mut clauses, 2);
        assert_eq!(n, 0);
        assert_eq!(&pks[..], &[a, b, agg]);
        assert_eq!(&clauses[..], &[ca, cb, cagg]);
    }
}

// Appendrel EC family: child EquivalenceMember translation (equivclass.c
// add_child_rel_equivalences / add_child_eq_member) and the ancestor set
// feeding check_index_predicates' otherrels (relnode.c find_childrel_parents).
mod appendrel_ec {
    use super::*;
    use types_pathnodes::relids::{
        find_childrel_parents, relids_equal, relids_is_member, relids_singleton,
    };
    use types_pathnodes::{
        AppendRelInfo, EquivalenceClass, EquivalenceMember, RelId, RelOptKind,
        RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL,
    };

    fn mk_rel<'mcx>(
        run: &mut crate::run::PlannerRun<'mcx>,
        relid: u32,
        kind: RelOptKind,
    ) -> RelId {
        let mcx = run.mcx;
        let mut rel = types_pathnodes::RelOptInfo::new(mcx);
        rel.relid = relid;
        rel.relids = relids_singleton(mcx, relid);
        rel.reloptkind = kind;
        let id = run.root.alloc_rel(rel);
        while run.root.simple_rel_array.len() <= relid as usize {
            run.root.simple_rel_array.push(None);
        }
        run.root.simple_rel_array[relid as usize] = Some(id);
        run.root.simple_rel_array_size =
            run.root.simple_rel_array_size.max(relid as i32 + 1);
        id
    }

    fn mk_appinfo<'mcx>(
        run: &mut crate::run::PlannerRun<'mcx>,
        parent_relid: u32,
        child_relid: u32,
        translated: &[types_pathnodes::NodeId],
    ) {
        let mcx = run.mcx;
        let mut ai = AppendRelInfo::new(mcx);
        ai.parent_relid = parent_relid;
        ai.child_relid = child_relid;
        // Typed children: the whole-row colnames snapshot needs an RTE
        // otherwise.
        ai.parent_reltype = 100001;
        ai.child_reltype = 100002;
        ai.translated_vars.extend(translated.iter().copied());
        while run.root.append_rel_array.len() <= child_relid as usize {
            run.root.append_rel_array.push(None);
        }
        run.root.append_rel_array[child_relid as usize] = Some(ai);
    }

    #[test]
    fn find_childrel_parents_walks_all_appendrel_levels() {
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        mk_rel(&mut run, 1, RELOPT_BASEREL);
        mk_rel(&mut run, 2, RELOPT_OTHER_MEMBER_REL);
        let grandchild = mk_rel(&mut run, 3, RELOPT_OTHER_MEMBER_REL);
        mk_appinfo(&mut run, 1, 2, &[]);
        mk_appinfo(&mut run, 2, 3, &[]);

        let parents = find_childrel_parents(&run.root, grandchild);
        assert!(relids_is_member(1, &parents));
        assert!(relids_is_member(2, &parents));
        assert!(!relids_is_member(3, &parents));
    }

    #[test]
    fn add_child_rel_equivalences_translates_parent_member() {
        let cx = cx();
        let mcx = cx.mcx();
        let mut run = crate::run::PlannerRun::new(mcx);
        let parent = mk_rel(&mut run, 1, RELOPT_BASEREL);
        let child = mk_rel(&mut run, 2, RELOPT_OTHER_MEMBER_REL);
        run.root.rel_mut(child).parent = Some(parent);
        run.root.rel_mut(child).top_parent = Some(parent);
        run.root.rel_mut(child).top_parent_relids = relids_singleton(mcx, 1);

        let parent_var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
        let child_var = Node::mk_var(mcx, 2, 1, 23, -1, 0, 0).unwrap();
        let child_var_id = run.intern_expr(child_var);
        mk_appinfo(&mut run, 1, 2, &[child_var_id]);

        let em_expr = run.intern_expr(parent_var);
        let em = run.root.alloc_em(EquivalenceMember {
            em_expr,
            em_relids: relids_singleton(mcx, 1),
            em_is_const: false,
            em_is_child: false,
            em_datatype: 23,
            em_jdomain: 0,
            em_parent: None,
        });
        let mut ec = EquivalenceClass::new(mcx);
        ec.ec_relids = relids_singleton(mcx, 1);
        ec.ec_members.push(em);
        let ec_id = run.root.alloc_ec(ec);
        run.root.rel_mut(parent).eclass_indexes = relids_singleton(mcx, ec_id.0);
        run.root.ec_merging_done = true;

        let appinfo = run.root.append_rel_array[2].clone().unwrap();
        crate::equivclass::add_child_rel_equivalences(&mut run, &appinfo, parent, child)
            .unwrap();

        // Parent-side member list and ec_relids are untouched; the child
        // member lands in ec_childmembers[2] as a translated child Var.
        let e = run.root.ec(ec_id);
        assert_eq!(e.ec_members.len(), 1);
        assert!(relids_equal(&e.ec_relids, &relids_singleton(mcx, 1)));
        let child_ems = &e.ec_childmembers[2];
        assert_eq!(child_ems.len(), 1);
        let cm = run.root.em(child_ems[0]);
        assert!(cm.em_is_child);
        assert_eq!(cm.em_parent, Some(em));
        assert!(relids_equal(&cm.em_relids, &relids_singleton(mcx, 2)));
        let cexpr = run.root.expr_node(cm.em_expr).as_var().unwrap();
        assert_eq!(cexpr.varno, 2);
        assert_eq!(cexpr.varattno, 1);
        assert!(relids_is_member(ec_id.0 as i32, &run.root.rel(child).eclass_indexes));
    }
}
