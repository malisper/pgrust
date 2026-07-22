use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{create_executor_state, EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::syscache_seams::PgAggregateShape;
use ::types_core::{INT4OID, INT8OID};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::{Aggref, OUTER_VAR};
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_DOUBLE,
    TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::{exec_agg, exec_init_agg, exec_rescan_agg};

const INT4_EQ: u32 = 96;
const F_INT4EQ: u32 = 65;
const TEXT_EQ: u32 = 98;
const F_TEXTEQ: u32 = 67;
const F_HASHINT4: u32 = 450;
const INT_HASH_FAM: u32 = 1977;
const INTEGER_BTREE_FAM: u32 = 1976;
const INT4_LT: u32 = 97;
const F_BTINT4SORTSUPPORT: u32 = 3130;

const COUNT_STAR_OID: u32 = 2803;
const COUNT_ANY_OID: u32 = 2147;
const SUM_INT4_OID: u32 = 2108;
const SUM_INT8_OID: u32 = 2107;
const INT8INC_OID: u32 = 1219;
const INT8INC_ANY_OID: u32 = 2804;
const INT4_SUM_OID: u32 = 1841;
const INT8_AVG_ACCUM_OID: u32 = 2746;
const NUMERIC_POLY_SUM_OID: u32 = 3388;
const INT8_AVG_COMBINE_OID: u32 = 2785;
const INT8PL_OID: u32 = 463;
const INTERNALOID: u32 = 2281;
const NUMERICOID: u32 = 1700;
const TEXTOID: u32 = 25;
const INT8ARRAYOID: u32 = 1016;
const MIN_TEXT_OID: u32 = 2145;
const MAX_TEXT_OID: u32 = 2129;
const TEXT_SMALLER_OID: u32 = 459;
const TEXT_LARGER_OID: u32 = 458;
const AVG_INT4_OID: u32 = 2101;
const INT4_AVG_ACCUM_OID: u32 = 1963;
const INT8_AVG_OID: u32 = 1964;
const C_COLLATION: u32 = 950;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        fmgr_core::init_seams();
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                INT8OID => Some(PgTypeShape {
                    typlen: 8,
                    typbyval: true,
                    typalign: TYPALIGN_DOUBLE,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                INTERNALOID => Some(PgTypeShape {
                    typlen: 8,
                    typbyval: true,
                    typalign: TYPALIGN_DOUBLE,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                NUMERICOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: b'm' as i8,
                    typcollation: 0,
                }),
                TEXTOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: b'x' as i8,
                    typcollation: 100,
                }),
                INT8ARRAYOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_DOUBLE,
                    typstorage: b'x' as i8,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        // GetAggInitVal resolves initval text through typinput (int8in /
        // array_in for the _int8 avg transtype).
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(match typid {
                INT8OID => Some(syscache_seams::PgTypeIoShape {
                    oid: INT8OID,
                    typinput: 460,
                    typoutput: 461,
                    typreceive: 2408,
                    typsend: 2409,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: 8,
                    typbyval: true,
                    typalign: TYPALIGN_DOUBLE,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                INT8ARRAYOID => Some(syscache_seams::PgTypeIoShape {
                    oid: INT8ARRAYOID,
                    typinput: 750,
                    typoutput: 751,
                    typreceive: 2400,
                    typsend: 2401,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: INT8OID,
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_DOUBLE,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                _ => None,
            })
        });
        // pg_aggregate.dat rows for count() / sum(int4).
        syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
            Ok(match aggfnoid {
                COUNT_STAR_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: INT8INC_OID,
                    aggfinalfn: 0,
                    aggcombinefn: INT8PL_OID,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: INT8OID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                SUM_INT4_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: INT4_SUM_OID,
                    aggfinalfn: 0,
                    aggcombinefn: INT8PL_OID,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: INT8OID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                SUM_INT8_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: INT8_AVG_ACCUM_OID,
                    aggfinalfn: NUMERIC_POLY_SUM_OID,
                    aggcombinefn: INT8_AVG_COMBINE_OID,
                    aggserialfn: 2786,
                    aggdeserialfn: 2787,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: INTERNALOID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 48,
                }),
                COUNT_ANY_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: INT8INC_ANY_OID,
                    aggfinalfn: 0,
                    aggcombinefn: INT8PL_OID,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: INT8OID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                MIN_TEXT_OID | MAX_TEXT_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: if aggfnoid == MIN_TEXT_OID {
                        TEXT_SMALLER_OID
                    } else {
                        TEXT_LARGER_OID
                    },
                    aggfinalfn: 0,
                    aggcombinefn: 0,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: TEXTOID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                AVG_INT4_OID => Some(PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: INT4_AVG_ACCUM_OID,
                    aggfinalfn: INT8_AVG_OID,
                    aggcombinefn: 0,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 0,
                    aggtranstype: INT8ARRAYOID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok(match opno {
                INT4_EQ => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: 16,
                    oprcom: INT4_EQ,
                    oprnegate: 518,
                    oprcode: F_INT4EQ,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                TEXT_EQ => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: TEXTOID,
                    oprright: TEXTOID,
                    oprresult: 16,
                    oprcom: TEXT_EQ,
                    oprnegate: 531,
                    oprcode: F_TEXTEQ,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            let mut v = PgVec::new_in(mcx);
            if opno == INT4_EQ {
                v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INT_HASH_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: 405,
                });
            }
            if opno == INT4_LT {
                v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INTEGER_BTREE_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: 403,
                });
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(
                if (opfamily, lefttype, righttype, procnum)
                    == (INT_HASH_FAM, INT4OID, INT4OID, 1)
                {
                    F_HASHINT4
                } else if (opfamily, lefttype, righttype, procnum)
                    == (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2)
                {
                    F_BTINT4SORTSUPPORT
                } else {
                    0
                },
            )
        });
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        syscache_seams::pg_aggregate_agginitval::set(|mcx, aggfnoid| {
            Ok(match aggfnoid {
                COUNT_STAR_OID | COUNT_ANY_OID => {
                    Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap()))
                }
                AVG_INT4_OID => {
                    Some(Some(::mcx::PgString::from_str_in("{0,0}", mcx).unwrap()))
                }
                SUM_INT4_OID | SUM_INT8_OID | MIN_TEXT_OID | MAX_TEXT_OID => Some(None),
                _ => None,
            })
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodeagg-test")));
    m.mcx()
}

// The pointee is leaked and the plan tree sealed; invariance of Agg<'mcx> is
// a list-GAT artifact (querydesc::shorten_pstmt precedent).
unsafe fn shorten<'a>(agg: &Agg<'_>) -> &'a Agg<'a> {
    unsafe { core::mem::transmute::<&Agg<'_>, &'a Agg<'a>>(agg) }
}

fn one_col_desc(mcx: Mcx<'_>, atttypid: u32, attlen: i16, attalign: i8) -> Rc<TupleDescData<'_>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid,
        atttypmod: -1,
        attlen,
        attbyval: true,
        attalign,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn mk_count_star_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = COUNT_STAR_OID;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = true;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("count"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    agg.numGroups = 1;
    agg.seal_ref()
}

fn mk_sum_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = SUM_INT4_OID;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("sum"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    agg.numGroups = 1;
    agg.seal_ref()
}

// Feeds `rows` through a virtual outer slot, then None; C's ExecProcNode.
fn feeder<'mcx>(
    outer_id: ExecSlotId,
    rows: &'static [i32],
) -> impl FnMut(&mut EStateData<'mcx>) -> ::types_error::PgResult<Option<ExecSlotId>> {
    let mut i = 0usize;
    move |estate| {
        if i >= rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(outer_id);
        exectuples::exec_clear_tuple(slot, mcx);
        slot.base_mut().tts_values[0] = Datum::from_i32(rows[i]);
        slot.base_mut().tts_isnull[0] = false;
        exectuples::exec_store_virtual_tuple(slot);
        i += 1;
        Ok(Some(outer_id))
    }
}

fn run_agg(agg: &'static Agg<'static>, rows: &'static [i32]) -> (Datum, bool) {
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = one_col_desc(leaked_mcx(), INT8OID, 8, TYPALIGN_DOUBLE);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let got = exec_agg(&mut state, estate, feeder(outer_id, rows)).unwrap();
        let slot_id = got.expect("plain agg returns one row even for empty input");
        let (v, isnull) = {
            let base = estate.slot_mut(slot_id).base();
            (base.tts_values[0], base.tts_isnull[0])
        };
        assert!(exec_agg(&mut state, estate, feeder(outer_id, &[])).unwrap().is_none());

        exec_rescan_agg(&mut state, estate);
        let again = exec_agg(&mut state, estate, feeder(outer_id, rows)).unwrap().unwrap();
        let base = estate.slot_mut(again).base();
        assert_eq!(base.tts_values[0].as_i64(), v.as_i64());
        assert_eq!(base.tts_isnull[0], isnull);

        (v, isnull)
    })
}

#[test]
fn count_star_counts_rows() {
    install_seams();
    let agg = mk_count_star_agg(leaked_mcx());
    let (v, isnull) = run_agg(agg, &[1, 2, 3, 4, 5]);
    assert!(!isnull);
    assert_eq!(v.as_i64(), 5);
}

#[test]
fn count_star_of_empty_input_is_zero() {
    install_seams();
    let agg = mk_count_star_agg(leaked_mcx());
    let (v, isnull) = run_agg(agg, &[]);
    assert!(!isnull);
    assert_eq!(v.as_i64(), 0);
}

#[test]
fn sum_int4_adds_rows() {
    install_seams();
    let agg = mk_sum_agg(leaked_mcx());
    let (v, isnull) = run_agg(agg, &[1, 2, 3, 4, 5]);
    assert!(!isnull);
    assert_eq!(v.as_i64(), 15);
}

#[test]
fn sum_int4_of_empty_input_is_null() {
    install_seams();
    let agg = mk_sum_agg(leaked_mcx());
    let (_, isnull) = run_agg(agg, &[]);
    assert!(isnull);
}

#[test]
#[should_panic(expected = "AGG_MIXED")]
fn mixed_strategy_panics() {
    install_seams();
    let mcx = leaked_mcx();
    let agg_node = {
        let mut agg = Node::build::<Agg>(mcx).unwrap();
        agg.aggstrategy = 3;
        agg.seal_ref()
    };
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        // SAFETY: agg_node is leaked ('static) and read-only.
        let agg_node = unsafe { shorten(agg_node) };
        let _ = exec_init_agg(
            agg_node,
            estate,
            0,
            one_col_desc(leaked_mcx(), INT8OID, 8, TYPALIGN_DOUBLE),
            None,
        );
    });
}

fn two_col_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let a1 = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let a2 = FormData_pg_attribute { attnum: 2, atttypid: INT8OID, attlen: 8, attbyval: true, attalign: TYPALIGN_DOUBLE, atttypmod: -1, attstorage: TYPSTORAGE_PLAIN, ..Default::default() };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&a1));
    compact.push(CompactAttribute::populate_from(&a2));
    attrs.push(a1);
    attrs.push(a2);
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn mk_hashed_count_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let outer_tle = Node::mk_target_entry(mcx, outer_var, 1, Some("a"), false).unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = NodeList::make1(mcx, outer_tle).unwrap();
        r.plan.plan_width = 4;
        r.seal()
    };

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("a"), false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = COUNT_STAR_OID;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = true;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let count_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("count"), false).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, count_tle).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.seal_ref()
}

#[test]
fn hashed_group_by_counts_groups() {
    install_seams();
    let agg = mk_hashed_count_agg(leaked_mcx());
    let rows: &'static [i32] = &[1, 2, 1, 3, 2, 1];
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = two_col_desc(leaked_mcx());
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let mut got: Vec<(i32, i64)> = Vec::new();
        {
            let mut feed = feeder(outer_id, rows);
            while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
                let base = estate.slot_mut(slot_id).base();
                assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
                got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
            }
        }
        got.sort_unstable();
        assert_eq!(got, vec![(1, 3), (2, 2), (3, 1)]);

        // Rescan reuses the filled table (C's no-chgParam arm).
        exec_rescan_agg(&mut state, estate);
        let mut again: Vec<(i32, i64)> = Vec::new();
        {
            let mut feed = feeder(outer_id, &[]);
            while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
                let base = estate.slot_mut(slot_id).base();
                again.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
            }
        }
        again.sort_unstable();
        assert_eq!(again, vec![(1, 3), (2, 2), (3, 1)]);
    });
}

#[test]
fn hashed_group_by_empty_input_returns_no_rows() {
    install_seams();
    let agg = mk_hashed_count_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();
        let mut feed = feeder(outer_id, &[]);
        assert!(exec_agg(&mut state, estate, &mut feed).unwrap().is_none());
    });
}

// NULL keys form one group (NOT DISTINCT match, hash skips NULL inputs).
#[test]
fn hashed_group_by_null_keys_group_together() {
    install_seams();
    let agg = mk_hashed_count_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();

        // rows: NULL, 7, NULL
        let rows: &'static [Option<i32>] = &[None, Some(7), None];
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            match rows[i] {
                Some(v) => {
                    slot.base_mut().tts_values[0] = Datum::from_i32(v);
                    slot.base_mut().tts_isnull[0] = false;
                }
                None => {
                    slot.base_mut().tts_values[0] = Datum::null();
                    slot.base_mut().tts_isnull[0] = true;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let mut got: Vec<(Option<i32>, i64)> = Vec::new();
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            let key = (!base.tts_isnull[0]).then(|| base.tts_values[0].as_i32());
            got.push((key, base.tts_values[1].as_i64()));
        }
        got.sort_unstable();
        assert_eq!(got, vec![(None, 2), (Some(7), 1)]);
    });
}

const INT8_GT: u32 = 413;
const F_INT8GT: u32 = 470;

fn mk_grouped_count_agg(mcx: Mcx<'_>, strategy: u32, with_having: bool) -> &Agg<'_> {
    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let outer_tle = Node::mk_target_entry(mcx, outer_var, 1, Some("a"), false).unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = NodeList::make1(mcx, outer_tle).unwrap();
        r.plan.plan_width = 4;
        r.seal()
    };

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("a"), false).unwrap();
    fn mk_count<'m>(mcx: Mcx<'m>) -> Node<'m> {
        let mut aggref = Node::build::<Aggref>(mcx).unwrap();
        aggref.aggfnoid = COUNT_STAR_OID;
        aggref.aggtype = INT8OID;
        aggref.aggtranstype = INT8OID;
        aggref.aggstar = true;
        aggref.aggno = 0;
        aggref.aggtransno = 0;
        aggref.seal()
    }
    let count_tle = Node::mk_target_entry(mcx, mk_count(mcx), 2, Some("count"), false).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, count_tle).unwrap();
    agg.plan.targetlist = tlist;
    if with_having {
        // HAVING count(*) > 1 as the node qual.
        let one = Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(1), false, true).unwrap();
        let mut args = NodeList::make1(mcx, mk_count(mcx)).unwrap();
        args.lappend(mcx, one).unwrap();
        let qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT8_GT,
                opfuncid: F_INT8GT,
                opresulttype: 16,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args,
                location: -1,
            },
        )
        .unwrap();
        agg.plan.qual = NodeList::make1(mcx, qual).unwrap();
    }
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = strategy;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.seal_ref()
}

fn run_grouped(agg: &'static Agg<'static>, rows: &'static [i32]) -> Vec<(i32, i64)> {
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();
        let mut got: Vec<(i32, i64)> = Vec::new();
        let mut feed = feeder(outer_id, rows);
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
        }
        got
    })
}

// AGG_SORTED over presorted input: one row per group boundary, input order
// preserved (no sort inside the node).
#[test]
fn sorted_group_by_counts_groups_in_order() {
    install_seams();
    let agg = mk_grouped_count_agg(leaked_mcx(), 1, false);
    let got = run_grouped(agg, &[1, 1, 1, 2, 2, 3]);
    assert_eq!(got, vec![(1, 3), (2, 2), (3, 1)]);
}

#[test]
fn sorted_group_by_empty_input_returns_no_rows() {
    install_seams();
    let agg = mk_grouped_count_agg(leaked_mcx(), 1, false);
    assert!(run_grouped(agg, &[]).is_empty());
}

#[test]
fn sorted_group_by_having_filters_groups() {
    install_seams();
    let agg = mk_grouped_count_agg(leaked_mcx(), 1, true);
    let got = run_grouped(agg, &[1, 1, 1, 2, 2, 3]);
    assert_eq!(got, vec![(1, 3), (2, 2)]);
}

#[test]
fn hashed_group_by_having_filters_groups() {
    install_seams();
    let agg = mk_grouped_count_agg(leaked_mcx(), 2, true);
    let mut got = run_grouped(agg, &[1, 2, 1, 3, 2, 1]);
    got.sort_unstable();
    assert_eq!(got, vec![(1, 3), (2, 2)]);
}

// Sorted-agg rescan re-runs the whole pass over a fresh feed.
#[test]
fn sorted_group_by_rescan_reruns() {
    install_seams();
    let agg = mk_grouped_count_agg(leaked_mcx(), 1, false);
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();
        let rows: &'static [i32] = &[4, 4, 5];
        {
            let mut feed = feeder(outer_id, rows);
            let mut n = 0;
            while exec_agg(&mut state, estate, &mut feed).unwrap().is_some() {
                n += 1;
            }
            assert_eq!(n, 2);
        }
        exec_rescan_agg(&mut state, estate);
        let mut feed = feeder(outer_id, rows);
        let mut got: Vec<(i32, i64)> = Vec::new();
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
        }
        assert_eq!(got, vec![(4, 2), (5, 1)]);
    });
}

fn numeric_datum_text(d: Datum) -> String {
    // SAFETY: numeric results are 4B-header varlena images in live memory.
    let v = unsafe { ::datum::varlena::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    let mut buf = Vec::new();
    ::adt_numeric::numeric_out_into(::adt_numeric::Num::from_payload(v.data()), &mut buf);
    String::from_utf8(buf).unwrap()
}

fn mk_sum_int8_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let var = Node::mk_var(mcx, OUTER_VAR, 1, INT8OID, -1, 0, 0).unwrap();
    let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = SUM_INT8_OID;
    aggref.aggtype = NUMERICOID;
    aggref.aggtranstype = INTERNALOID;
    aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("sum"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    agg.numGroups = 1;
    agg.seal_ref()
}

fn int8_feeder<'mcx>(
    outer_id: ExecSlotId,
    rows: &'static [Option<i64>],
) -> impl FnMut(&mut EStateData<'mcx>) -> ::types_error::PgResult<Option<ExecSlotId>> {
    let mut i = 0usize;
    move |estate| {
        if i >= rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(outer_id);
        exectuples::exec_clear_tuple(slot, mcx);
        match rows[i] {
            Some(v) => {
                slot.base_mut().tts_values[0] = Datum::from_i64(v);
                slot.base_mut().tts_isnull[0] = false;
            }
            None => {
                slot.base_mut().tts_values[0] = Datum::null();
                slot.base_mut().tts_isnull[0] = true;
            }
        }
        exectuples::exec_store_virtual_tuple(slot);
        i += 1;
        Ok(Some(outer_id))
    }
}

// sum(int8): transfn-built Int128AggState + numeric_poly_sum finalfn.
#[test]
fn sum_int8_internal_state_and_finalfn() {
    install_seams();
    let agg = mk_sum_int8_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT8OID, 8, TYPALIGN_DOUBLE);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = {
            let att = FormData_pg_attribute {
                attnum: 1,
                atttypid: NUMERICOID,
                atttypmod: -1,
                attlen: -1,
                attbyval: false,
                attalign: TYPALIGN_INT,
                attstorage: b'm' as i8,
                ..Default::default()
            };
            let m = leaked_mcx();
            let mut attrs = PgVec::new_in(m);
            let mut compact = PgVec::new_in(m);
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
            Rc::new(TupleDescData {
                natts: 1,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: compact,
                attrs,
            })
        };
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let rows: &'static [Option<i64>] = &[Some(5), None, Some(7), Some(3)];
        let slot_id = exec_agg(&mut state, estate, int8_feeder(outer_id, rows))
            .unwrap()
            .expect("plain agg returns one row");
        {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0]);
            assert_eq!(numeric_datum_text(base.tts_values[0]), "15");
        }
        assert!(exec_agg(&mut state, estate, int8_feeder(outer_id, &[])).unwrap().is_none());

        exec_rescan_agg(&mut state, estate);
        let rows2: &'static [Option<i64>] = &[Some(40), Some(2)];
        let again = exec_agg(&mut state, estate, int8_feeder(outer_id, rows2)).unwrap().unwrap();
        let base = estate.slot_mut(again).base();
        assert_eq!(numeric_datum_text(base.tts_values[0]), "42");
    });
}

#[test]
fn sum_int8_of_empty_input_is_null() {
    install_seams();
    let agg = mk_sum_int8_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT8OID, 8, TYPALIGN_DOUBLE);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = one_col_desc(leaked_mcx(), NUMERICOID, -1, TYPALIGN_INT);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();
        let slot_id =
            exec_agg(&mut state, estate, int8_feeder(outer_id, &[])).unwrap().unwrap();
        let base = estate.slot_mut(slot_id).base();
        assert!(base.tts_isnull[0]);
    });
}

// count(a): strict transfn behind the strict-input check skips NULLs.
#[test]
fn count_any_skips_nulls() {
    install_seams();
    let mcx = leaked_mcx();
    let agg = {
        let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let mut aggref = Node::build::<Aggref>(mcx).unwrap();
        aggref.aggfnoid = COUNT_ANY_OID;
        aggref.aggtype = INT8OID;
        aggref.aggtranstype = INT8OID;
        aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
        aggref.aggno = 0;
        aggref.aggtransno = 0;
        let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("count"), false).unwrap();
        let mut agg = Node::build::<Agg>(mcx).unwrap();
        agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
        agg.numGroups = 1;
        agg.seal_ref()
    };
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = one_col_desc(leaked_mcx(), INT8OID, 8, TYPALIGN_DOUBLE);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let rows: &'static [Option<i32>] = &[Some(1), None, Some(3), None, Some(5)];
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            match rows[i] {
                Some(v) => {
                    slot.base_mut().tts_values[0] = Datum::from_i32(v);
                    slot.base_mut().tts_isnull[0] = false;
                }
                None => {
                    slot.base_mut().tts_values[0] = Datum::null();
                    slot.base_mut().tts_isnull[0] = true;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let slot_id = exec_agg(&mut state, estate, &mut feed).unwrap().unwrap();
        let base = estate.slot_mut(slot_id).base();
        assert!(!base.tts_isnull[0]);
        assert_eq!(base.tts_values[0].as_i64(), 3);
    });
}

fn mk_hashed_sum_int8_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let g_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let g_tle = Node::mk_target_entry(mcx, g_var, 1, Some("g"), false).unwrap();
    let b_var = Node::mk_var(mcx, 1, 2, INT8OID, -1, 0, 0).unwrap();
    let b_tle = Node::mk_target_entry(mcx, b_var, 2, Some("b"), false).unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        let mut tl = NodeList::make1(mcx, g_tle).unwrap();
        tl.lappend(mcx, b_tle).unwrap();
        r.plan.targetlist = tl;
        r.plan.plan_width = 12;
        r.seal()
    };

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("g"), false).unwrap();
    let sum_arg = Node::mk_var(mcx, OUTER_VAR, 2, INT8OID, -1, 0, 0).unwrap();
    let sum_arg_tle = Node::mk_target_entry(mcx, sum_arg, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = SUM_INT8_OID;
    aggref.aggtype = NUMERICOID;
    aggref.aggtranstype = INTERNALOID;
    aggref.args = NodeList::make1(mcx, sum_arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let sum_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("sum"), false).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, sum_tle).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.seal_ref()
}

// GROUP BY g, sum(b): one Int128AggState per hash entry.
#[test]
fn hashed_group_by_sum_int8() {
    install_seams();
    let agg = mk_hashed_sum_int8_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = two_col_desc(mcx);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = {
            let a1 = FormData_pg_attribute {
                attnum: 1,
                atttypid: INT4OID,
                atttypmod: -1,
                attlen: 4,
                attbyval: true,
                attalign: TYPALIGN_INT,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            let a2 = FormData_pg_attribute {
                attnum: 2,
                atttypid: NUMERICOID,
                atttypmod: -1,
                attlen: -1,
                attbyval: false,
                attalign: TYPALIGN_INT,
                attstorage: b'm' as i8,
                ..Default::default()
            };
            let m = leaked_mcx();
            let mut attrs = PgVec::new_in(m);
            let mut compact = PgVec::new_in(m);
            compact.push(CompactAttribute::populate_from(&a1));
            compact.push(CompactAttribute::populate_from(&a2));
            attrs.push(a1);
            attrs.push(a2);
            Rc::new(TupleDescData {
                natts: 2,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: compact,
                attrs,
            })
        };
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let rows: &'static [(i32, i64)] = &[(1, 10), (2, 5), (1, 20), (3, 7), (2, 5)];
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            let (g, b) = rows[i];
            slot.base_mut().tts_values[0] = Datum::from_i32(g);
            slot.base_mut().tts_isnull[0] = false;
            slot.base_mut().tts_values[1] = Datum::from_i64(b);
            slot.base_mut().tts_isnull[1] = false;
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let mut got: Vec<(i32, String)> = Vec::new();
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            got.push((base.tts_values[0].as_i32(), numeric_datum_text(base.tts_values[1])));
        }
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                (1, "30".to_string()),
                (2, "10".to_string()),
                (3, "7".to_string())
            ]
        );
    });
}

fn text_datum(s: &str) -> Datum {
    let mut v = Vec::with_capacity(4 + s.len());
    v.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + s.len()));
    v.extend_from_slice(s.as_bytes());
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

fn text_datum_str(d: Datum) -> String {
    // SAFETY: text transvalues stay 4B-header images (datumCopy preserves form).
    let v = unsafe { ::datum::varlena::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    String::from_utf8(v.data().to_vec()).unwrap()
}

fn mk_min_max_text_agg(mcx: Mcx<'_>, aggfnoid: u32) -> &Agg<'_> {
    let var = Node::mk_var(mcx, OUTER_VAR, 1, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = aggfnoid;
    aggref.aggtype = TEXTOID;
    aggref.aggtranstype = TEXTOID;
    aggref.inputcollid = C_COLLATION;
    aggref.aggargtypes = types_nodes::list::OidList::make1(mcx, TEXTOID).unwrap();
    aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("m"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    agg.numGroups = 1;
    agg.seal_ref()
}

fn text_feeder<'mcx>(
    outer_id: ExecSlotId,
    rows: &'static [Option<&'static str>],
) -> impl FnMut(&mut EStateData<'mcx>) -> ::types_error::PgResult<Option<ExecSlotId>> {
    let mut i = 0usize;
    move |estate| {
        if i >= rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(outer_id);
        exectuples::exec_clear_tuple(slot, mcx);
        match rows[i] {
            Some(s) => {
                slot.base_mut().tts_values[0] = text_datum(s);
                slot.base_mut().tts_isnull[0] = false;
            }
            None => {
                slot.base_mut().tts_values[0] = Datum::null();
                slot.base_mut().tts_isnull[0] = true;
            }
        }
        exectuples::exec_store_virtual_tuple(slot);
        i += 1;
        Ok(Some(outer_id))
    }
}

// EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF: first non-NULL input datumCopies
// into the aggcontext, later winners re-home via ExecAggCopyTransValue.
#[test]
fn min_max_text_byref_transvalue() {
    install_seams();
    for (fnoid, expect) in [(MIN_TEXT_OID, "apple"), (MAX_TEXT_OID, "pear")] {
        let agg = mk_min_max_text_agg(leaked_mcx(), fnoid);
        let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
        let mut estate_owner = estate_owner.unwrap();
        estate_owner.with_mut(|estate| {
            let mcx = estate.es_query_cxt;
            let outer_desc = one_col_desc(mcx, TEXTOID, -1, TYPALIGN_INT);
            let outer_id =
                estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
            let result_desc = one_col_desc(leaked_mcx(), TEXTOID, -1, TYPALIGN_INT);
            // SAFETY: agg is leaked ('static) and read-only.
            let agg = unsafe { shorten(agg) };
            let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

            let rows: &'static [Option<&'static str>] =
                &[None, Some("mango"), Some("apple"), None, Some("pear"), Some("banana")];
            let slot_id = exec_agg(&mut state, estate, text_feeder(outer_id, rows))
                .unwrap()
                .expect("plain agg returns one row");
            {
                let base = estate.slot_mut(slot_id).base();
                assert!(!base.tts_isnull[0]);
                assert_eq!(text_datum_str(base.tts_values[0]), expect);
            }

            // All-NULL input leaves the transvalue NULL (INIT never fires).
            exec_rescan_agg(&mut state, estate);
            let rows2: &'static [Option<&'static str>] = &[None, None];
            let again =
                exec_agg(&mut state, estate, text_feeder(outer_id, rows2)).unwrap().unwrap();
            let base = estate.slot_mut(again).base();
            assert!(base.tts_isnull[0]);
        });
    }
}

fn mk_avg_int4_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = AVG_INT4_OID;
    aggref.aggtype = NUMERICOID;
    aggref.aggtranstype = INT8ARRAYOID;
    aggref.aggargtypes = types_nodes::list::OidList::make1(mcx, INT4OID).unwrap();
    aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("avg"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    agg.numGroups = 1;
    agg.seal_ref()
}

// EEOP_AGG_PLAIN_TRANS_STRICT_BYREF over the _int8 transarray: array_in
// parses '{0,0}', int4_avg_accum mutates the aggcontext copy in place, and
// int8_avg divides (live PG 18: avg(1..5) = 3.0000000000000000).
#[test]
fn avg_int4_array_transtype() {
    install_seams();
    let agg = mk_avg_int4_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        let result_desc = one_col_desc(leaked_mcx(), NUMERICOID, -1, TYPALIGN_INT);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc, None).unwrap();

        let rows: &'static [Option<i32>] = &[Some(1), Some(2), None, Some(3), Some(4), Some(5)];
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            match rows[i] {
                Some(v) => {
                    slot.base_mut().tts_values[0] = Datum::from_i32(v);
                    slot.base_mut().tts_isnull[0] = false;
                }
                None => {
                    slot.base_mut().tts_values[0] = Datum::null();
                    slot.base_mut().tts_isnull[0] = true;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let slot_id =
            exec_agg(&mut state, estate, &mut feed).unwrap().expect("plain agg returns one row");
        {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0]);
            assert_eq!(numeric_datum_text(base.tts_values[0]), "3.0000000000000000");
        }

        // Empty input: count 0 in the initval copy -> int8_avg returns NULL.
        exec_rescan_agg(&mut state, estate);
        let again = exec_agg(&mut state, estate, feeder(outer_id, &[])).unwrap().unwrap();
        let base = estate.slot_mut(again).base();
        assert!(base.tts_isnull[0]);
    });
}

fn mk_hashed_min_text_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let g_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let g_tle = Node::mk_target_entry(mcx, g_var, 1, Some("g"), false).unwrap();
    let t_var = Node::mk_var(mcx, 1, 2, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let t_tle = Node::mk_target_entry(mcx, t_var, 2, Some("t"), false).unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        let mut tl = NodeList::make1(mcx, g_tle).unwrap();
        tl.lappend(mcx, t_tle).unwrap();
        r.plan.targetlist = tl;
        r.plan.plan_width = 20;
        r.seal()
    };

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("g"), false).unwrap();
    let arg_var = Node::mk_var(mcx, OUTER_VAR, 2, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let arg_tle = Node::mk_target_entry(mcx, arg_var, 1, None, false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = MIN_TEXT_OID;
    aggref.aggtype = TEXTOID;
    aggref.aggtranstype = TEXTOID;
    aggref.inputcollid = C_COLLATION;
    aggref.aggargtypes = types_nodes::list::OidList::make1(mcx, TEXTOID).unwrap();
    aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let min_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("m"), false).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, min_tle).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.seal_ref()
}

// Hashed lane: AggTransInitStrictByRefIndirect through the repointed
// pergroup cell; transvalue copies land in the table context.
#[test]
fn hashed_group_by_min_text() {
    install_seams();
    let agg = mk_hashed_min_text_agg(leaked_mcx());
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = {
            let a1 = FormData_pg_attribute {
                attnum: 1,
                atttypid: INT4OID,
                atttypmod: -1,
                attlen: 4,
                attbyval: true,
                attalign: TYPALIGN_INT,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            let a2 = FormData_pg_attribute {
                attnum: 2,
                atttypid: TEXTOID,
                atttypmod: -1,
                attlen: -1,
                attbyval: false,
                attalign: TYPALIGN_INT,
                attstorage: b'x' as i8,
                ..Default::default()
            };
            let mut attrs = PgVec::new_in(mcx);
            let mut compact = PgVec::new_in(mcx);
            compact.push(CompactAttribute::populate_from(&a1));
            compact.push(CompactAttribute::populate_from(&a2));
            attrs.push(a1);
            attrs.push(a2);
            Rc::new(TupleDescData {
                natts: 2,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: compact,
                attrs,
            })
        };
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();

        let rows: &'static [(i32, Option<&'static str>)] = &[
            (1, Some("mango")),
            (2, Some("kiwi")),
            (1, Some("apple")),
            (2, Some("plum")),
            (3, None),
            (1, Some("banana")),
        ];
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            let (g, t) = rows[i];
            slot.base_mut().tts_values[0] = Datum::from_i32(g);
            slot.base_mut().tts_isnull[0] = false;
            match t {
                Some(s) => {
                    slot.base_mut().tts_values[1] = text_datum(s);
                    slot.base_mut().tts_isnull[1] = false;
                }
                None => {
                    slot.base_mut().tts_values[1] = Datum::null();
                    slot.base_mut().tts_isnull[1] = true;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let mut got: Vec<(i32, Option<String>)> = Vec::new();
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            let m = (!base.tts_isnull[1]).then(|| text_datum_str(base.tts_values[1]));
            got.push((base.tts_values[0].as_i32(), m));
        }
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                (1, Some("apple".to_string())),
                (2, Some("kiwi".to_string())),
                (3, None),
            ]
        );
    });
}

fn desc_of<'m>(mcx: Mcx<'m>, cols: &[(u32, i16, i8)]) -> Rc<TupleDescData<'m>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, &(atttypid, attlen, attalign)) in cols.iter().enumerate() {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid,
            atttypmod: -1,
            attlen,
            attbyval: true,
            attalign,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn three_col_result_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    desc_of(
        mcx,
        &[
            (INT4OID, 4, TYPALIGN_INT),
            (INT4OID, 4, TYPALIGN_INT),
            (INT8OID, 8, TYPALIGN_DOUBLE),
            (INT4OID, 4, TYPALIGN_INT),
        ],
    )
}

fn two_int4_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    desc_of(mcx, &[(INT4OID, 4, TYPALIGN_INT), (INT4OID, 4, TYPALIGN_INT)])
}

// ROLLUP(a,b): one phase, sets [[a,b],[a],[]], plus GROUPING(a,b).
fn mk_rollup_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let mut outer_tl = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, a, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    outer_tl
        .lappend(mcx, Node::mk_target_entry(mcx, b, 2, Some("b"), false).unwrap())
        .unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = outer_tl;
        r.plan.plan_width = 8;
        r.seal()
    };

    let ga = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let gb = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
    let mut count = Node::build::<Aggref>(mcx).unwrap();
    count.aggfnoid = COUNT_STAR_OID;
    count.aggtype = INT8OID;
    count.aggtranstype = INT8OID;
    count.aggstar = true;
    count.aggno = 0;
    count.aggtransno = 0;
    let grouping = {
        let mut g = Node::build::<types_nodes::primnodes::GroupingFunc>(mcx).unwrap();
        g.cols = types_nodes::list::IntList::from_slice(mcx, &[1, 2]).unwrap();
        g.seal()
    };
    let mut tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, ga, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, gb, 2, Some("b"), false).unwrap())
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, count.seal(), 3, Some("count"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, grouping, 4, Some("grouping"), false).unwrap())
        .unwrap();

    let set2 = Node::mk_int_list(
        mcx,
        types_nodes::list::IntList::from_slice(mcx, &[0, 1]).unwrap(),
    )
    .unwrap();
    let set1 =
        Node::mk_int_list(mcx, types_nodes::list::IntList::from_slice(mcx, &[0]).unwrap())
            .unwrap();
    let set0 =
        Node::mk_int_list(mcx, types_nodes::list::IntList::from_slice(mcx, &[]).unwrap())
            .unwrap();
    let mut gsets = NodeList::make1(mcx, set2).unwrap();
    gsets.lappend(mcx, set1).unwrap();
    gsets.lappend(mcx, set0).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 1;
    agg.numCols = 2;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ, INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
    agg.numGroups = 4;
    agg.groupingSets = gsets;
    agg.seal_ref()
}

fn feeder2<'mcx>(
    outer_id: ExecSlotId,
    rows: &'static [(i32, i32)],
) -> impl FnMut(&mut EStateData<'mcx>) -> ::types_error::PgResult<Option<ExecSlotId>> {
    let mut i = 0usize;
    move |estate: &mut EStateData<'_>| {
        if i >= rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(outer_id);
        exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        base.tts_values[0] = Datum::from_i32(rows[i].0);
        base.tts_isnull[0] = false;
        base.tts_values[1] = Datum::from_i32(rows[i].1);
        base.tts_isnull[1] = false;
        exectuples::exec_store_virtual_tuple(slot);
        i += 1;
        Ok(Some(outer_id))
    }
}

type GsRow = (Option<i32>, Option<i32>, i64, i32);

fn run_rollup(agg: &'static Agg<'static>, rows: &'static [(i32, i32)]) -> Vec<GsRow> {
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = two_int4_desc(mcx);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(
            agg,
            estate,
            0,
            three_col_result_desc(leaked_mcx()),
            Some(two_int4_desc(leaked_mcx())),
        )
        .unwrap();
        let mut got: Vec<GsRow> = Vec::new();
        let mut feed = feeder2(outer_id, rows);
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            got.push((
                (!base.tts_isnull[0]).then(|| base.tts_values[0].as_i32()),
                (!base.tts_isnull[1]).then(|| base.tts_values[1].as_i32()),
                base.tts_values[2].as_i64(),
                base.tts_values[3].as_i32(),
            ));
        }
        got
    })
}

// ROLLUP(a,b): subtotal + grand-total rows with NULL markers and GROUPING()
// bitmasks, C emission order.
#[test]
fn rollup_two_cols_counts_and_grouping() {
    install_seams();
    let agg = mk_rollup_agg(leaked_mcx());
    let got = run_rollup(agg, &[(1, 10), (1, 10), (1, 20), (2, 30)]);
    assert_eq!(
        got,
        vec![
            (Some(1), Some(10), 2, 0),
            (Some(1), Some(20), 1, 0),
            (Some(1), None, 3, 1),
            (Some(2), Some(30), 1, 0),
            (Some(2), None, 1, 1),
            (None, None, 4, 3),
        ]
    );
}

#[test]
fn rollup_empty_input_projects_only_empty_set() {
    install_seams();
    let agg = mk_rollup_agg(leaked_mcx());
    let got = run_rollup(agg, &[]);
    assert_eq!(got, vec![(None, None, 0, 3)]);
}

// GROUPING SETS ((a),(b)): two sorted phases; phase 2 re-sorts by b through
// the inter-phase tuplesort.
fn mk_two_rollup_chain_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let mut outer_tl = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, a, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    outer_tl
        .lappend(mcx, Node::mk_target_entry(mcx, b, 2, Some("b"), false).unwrap())
        .unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = outer_tl;
        r.plan.plan_width = 8;
        r.seal()
    };

    let ga = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let gb = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
    let mut count = Node::build::<Aggref>(mcx).unwrap();
    count.aggfnoid = COUNT_STAR_OID;
    count.aggtype = INT8OID;
    count.aggtranstype = INT8OID;
    count.aggstar = true;
    count.aggno = 0;
    count.aggtransno = 0;
    let grouping = {
        let mut g = Node::build::<types_nodes::primnodes::GroupingFunc>(mcx).unwrap();
        g.cols = types_nodes::list::IntList::from_slice(mcx, &[1, 2]).unwrap();
        g.seal()
    };
    let mut tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, ga, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, gb, 2, Some("b"), false).unwrap())
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, count.seal(), 3, Some("count"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, grouping, 4, Some("grouping"), false).unwrap())
        .unwrap();

    let chain_sort = {
        let mut s = Node::build::<types_nodes::plannodes::Sort>(mcx).unwrap();
        s.numCols = 1;
        s.sortColIdx = mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
        s.sortOperators = mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
        s.collations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        s.nullsFirst = mcx::slice_borrow_in(mcx, &[false]).unwrap();
        s.seal()
    };
    let chain_agg = {
        let mut c = Node::build::<Agg>(mcx).unwrap();
        c.plan.lefttree = Some(chain_sort);
        c.aggstrategy = 1;
        c.numCols = 1;
        c.grpColIdx = mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
        c.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        c.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        c.groupingSets = NodeList::make1(
            mcx,
            Node::mk_int_list(
                mcx,
                types_nodes::list::IntList::from_slice(mcx, &[0]).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        c.seal()
    };

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 1;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.groupingSets = NodeList::make1(
        mcx,
        Node::mk_int_list(mcx, types_nodes::list::IntList::from_slice(mcx, &[0]).unwrap())
            .unwrap(),
    )
    .unwrap();
    agg.chain = NodeList::make1(mcx, chain_agg).unwrap();
    agg.seal_ref()
}

#[test]
fn grouping_sets_two_phases_resort() {
    install_seams();
    let agg = mk_two_rollup_chain_agg(leaked_mcx());
    let got = run_rollup(agg, &[(1, 10), (1, 20), (2, 10)]);
    assert_eq!(
        got,
        vec![
            (Some(1), None, 2, 1),
            (Some(2), None, 1, 1),
            (None, Some(10), 2, 2),
            (None, Some(20), 1, 2),
        ]
    );
}

// GROUPING SETS ((a),(b)), both hashed: top AGG_HASHED + AGG_HASHED chain
// entry, no sorts. Output order is per-table insertion order (first-seen).
fn mk_hashed_gsets_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let mut outer_tl = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, a, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    outer_tl
        .lappend(mcx, Node::mk_target_entry(mcx, b, 2, Some("b"), false).unwrap())
        .unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = outer_tl;
        r.plan.plan_width = 8;
        r.seal()
    };

    let ga = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let gb = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
    let mut count = Node::build::<Aggref>(mcx).unwrap();
    count.aggfnoid = COUNT_STAR_OID;
    count.aggtype = INT8OID;
    count.aggtranstype = INT8OID;
    count.aggstar = true;
    count.aggno = 0;
    count.aggtransno = 0;
    let grouping = {
        let mut g = Node::build::<types_nodes::primnodes::GroupingFunc>(mcx).unwrap();
        g.cols = types_nodes::list::IntList::from_slice(mcx, &[1, 2]).unwrap();
        g.seal()
    };
    let mut tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, ga, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, gb, 2, Some("b"), false).unwrap())
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, count.seal(), 3, Some("count"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, grouping, 4, Some("grouping"), false).unwrap())
        .unwrap();

    let one_set = |mcx| {
        NodeList::make1(
            mcx,
            Node::mk_int_list(
                mcx,
                types_nodes::list::IntList::from_slice(mcx, &[0]).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let chain_agg = {
        let mut c = Node::build::<Agg>(mcx).unwrap();
        c.aggstrategy = 2;
        c.numCols = 1;
        c.grpColIdx = mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
        c.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        c.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        c.numGroups = 4;
        c.groupingSets = one_set(mcx);
        c.seal()
    };

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.groupingSets = one_set(mcx);
    agg.chain = NodeList::make1(mcx, chain_agg).unwrap();
    agg.seal_ref()
}

#[test]
fn hashed_grouping_sets_two_tables() {
    install_seams();
    let agg = mk_hashed_gsets_agg(leaked_mcx());
    let got = run_rollup(agg, &[(1, 10), (1, 20), (2, 10)]);
    assert_eq!(
        got,
        vec![
            (Some(1), None, 2, 1),
            (Some(2), None, 1, 1),
            (None, Some(10), 2, 2),
            (None, Some(20), 1, 2),
        ]
    );
}

#[test]
fn hashed_grouping_sets_empty_input() {
    install_seams();
    let agg = mk_hashed_gsets_agg(leaked_mcx());
    let got = run_rollup(agg, &[]);
    assert_eq!(got, vec![]);
}

// AGG_MIXED: hashed (b) on top, sorted (a) chain entry consuming the shared
// (pre-sorted) input directly — sorted groups first, then the hash table.
fn mk_mixed_gsets_agg(mcx: Mcx<'_>) -> &Agg<'_> {
    let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let mut outer_tl = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, a, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    outer_tl
        .lappend(mcx, Node::mk_target_entry(mcx, b, 2, Some("b"), false).unwrap())
        .unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = outer_tl;
        r.plan.plan_width = 8;
        r.seal()
    };

    let ga = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let gb = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
    let mut count = Node::build::<Aggref>(mcx).unwrap();
    count.aggfnoid = COUNT_STAR_OID;
    count.aggtype = INT8OID;
    count.aggtranstype = INT8OID;
    count.aggstar = true;
    count.aggno = 0;
    count.aggtransno = 0;
    let grouping = {
        let mut g = Node::build::<types_nodes::primnodes::GroupingFunc>(mcx).unwrap();
        g.cols = types_nodes::list::IntList::from_slice(mcx, &[1, 2]).unwrap();
        g.seal()
    };
    let mut tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, ga, 1, Some("a"), false).unwrap(),
    )
    .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, gb, 2, Some("b"), false).unwrap())
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, count.seal(), 3, Some("count"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, grouping, 4, Some("grouping"), false).unwrap())
        .unwrap();

    let one_set = |mcx| {
        NodeList::make1(
            mcx,
            Node::mk_int_list(
                mcx,
                types_nodes::list::IntList::from_slice(mcx, &[0]).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let chain_agg = {
        let mut c = Node::build::<Agg>(mcx).unwrap();
        c.aggstrategy = 1;
        c.numCols = 1;
        c.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        c.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        c.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        c.groupingSets = one_set(mcx);
        c.seal()
    };

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 3;
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    agg.groupingSets = one_set(mcx);
    agg.chain = NodeList::make1(mcx, chain_agg).unwrap();
    agg.seal_ref()
}

#[test]
fn mixed_grouping_sets_sorted_then_hashed() {
    install_seams();
    let agg = mk_mixed_gsets_agg(leaked_mcx());
    let got = run_rollup(agg, &[(1, 10), (1, 20), (2, 10)]);
    assert_eq!(
        got,
        vec![
            (Some(1), None, 2, 1),
            (Some(2), None, 1, 1),
            (None, Some(10), 2, 2),
            (None, Some(20), 1, 2),
        ]
    );
}

#[test]
fn mixed_grouping_sets_empty_input() {
    install_seams();
    let agg = mk_mixed_gsets_agg(leaked_mcx());
    let got = run_rollup(agg, &[]);
    assert_eq!(got, vec![]);
}

// Spill lane: hash_mem-bounded fills partition to tapes and recombine.
mod hashspill {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Once;

    use super::*;

    static SPILL_SETUP: Once = Once::new();
    static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
    static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn enter_datadir(tag: &str) -> (std::sync::MutexGuard<'static, ()>, String) {
        let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = format!(
            "{}/pgrust-hashaggspill-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            tag
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
        (guard, dir)
    }

    fn setup() {
        install_seams();
        SPILL_SETUP.call_once(|| {
            guc_tables::init_seams();
            elog::init_seams();
            fd::init_seams();
            xact_seams::get_current_sub_transaction_id::set(|| 1);
            aio_seams::pgaio_closing_fd::set(|_| {});
            aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
            waitevent_seams::pgstat_report_wait_start::set(|_| {});
            waitevent_seams::pgstat_report_wait_end::set(|| {});
            pgstat_seams::pgstat_report_tempfile::set(|_| {});
            ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
            ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
            resowner::init_seams();
            guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
                get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
                set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
            });
        });
        fd::InitFileAccess();
        let _ = fd::InitTemporaryFileAccess();
        if resowner_seams::current_resource_owner::call().is_null() {
            let owner = resowner::ResourceOwnerCreate(
                types_resowner::ResourceOwner::NULL,
                "hashagg-spill-test",
            )
            .unwrap();
            resowner_seams::set_current_resource_owner::call(owner);
        }
    }

    fn temp_files(dir: &str) -> usize {
        std::fs::read_dir(format!("{dir}/base/pgsql_tmp"))
            .map(|d| d.count())
            .unwrap_or(0)
    }

    // Spilled tuples reform small text as 1B short varlena; read either form.
    fn text_any_str(d: Datum) -> String {
        let p = d.as_usize() as *const u8;
        // SAFETY: live in-memory text datum from the result slot.
        unsafe {
            let b0 = *p;
            let (off, len) = if b0 & 0x01 == 1 {
                (1usize, (b0 >> 1) as usize - 1)
            } else {
                (4usize, (p.cast::<u32>().read_unaligned() >> 2) as usize - 4)
            };
            String::from_utf8_lossy(std::slice::from_raw_parts(p.add(off), len)).into_owned()
        }
    }

    const NGROUPS: i32 = 10_000;

    fn spill_rows() -> &'static [i32] {
        let mut v: Vec<i32> = (0..NGROUPS).collect();
        v.extend(0..NGROUPS);
        Box::leak(v.into_boxed_slice())
    }

    #[test]
    fn hashed_group_by_spills_and_recombines() {
        setup();
        let (_cwd, dir) = enter_datadir("count");
        let saved_work_mem = init_small::globals::work_mem();
        init_small::globals::set_work_mem(64);

        let agg = mk_hashed_count_agg(leaked_mcx());
        let rows = spill_rows();
        let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
        let mut estate_owner = estate_owner.unwrap();
        estate_owner.with_mut(|estate| {
            let mcx = estate.es_query_cxt;
            let outer_desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
            let outer_id =
                estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
            // SAFETY: agg is leaked ('static) and read-only.
            let agg = unsafe { shorten(agg) };
            let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();

            let mut got: Vec<(i32, i64)> = Vec::new();
            {
                let mut feed = feeder(outer_id, rows);
                while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
                    let base = estate.slot_mut(slot_id).base();
                    got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
                }
            }
            got.sort_unstable();
            let expect: Vec<(i32, i64)> = (0..NGROUPS).map(|k| (k, 2)).collect();
            assert_eq!(got, expect);

            let ai = estate
                .es_agg_instrumentation
                .iter()
                .find_map(|(id, ai)| (*id == agg.plan.plan_node_id).then_some(ai))
                .unwrap();
            assert!(ai.hash_batches_used > 1, "expected spill batches, got {ai:?}");
            assert!(ai.hash_disk_used > 0, "expected disk usage, got {ai:?}");
            assert!(ai.hash_mem_peak > 0);

            exec_rescan_agg(&mut state, estate);
            let mut again: Vec<(i32, i64)> = Vec::new();
            {
                let mut feed = feeder(outer_id, rows);
                while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
                    let base = estate.slot_mut(slot_id).base();
                    again.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
                }
            }
            again.sort_unstable();
            assert_eq!(again, expect);

            crate::exec_end_agg(&mut state);
        });
        assert_eq!(temp_files(&dir), 0, "end must drop the tape files");
        init_small::globals::set_work_mem(saved_work_mem);
    }

    // Unneeded filler column: the all_cols_needed=false wslot projection.
    fn mk_spill_min_text_agg(mcx: Mcx<'_>) -> &Agg<'_> {
        let g_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let g_tle = Node::mk_target_entry(mcx, g_var, 1, Some("g"), false).unwrap();
        let f_var = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
        let f_tle = Node::mk_target_entry(mcx, f_var, 2, Some("filler"), false).unwrap();
        let t_var = Node::mk_var(mcx, 1, 3, TEXTOID, -1, C_COLLATION, 0).unwrap();
        let t_tle = Node::mk_target_entry(mcx, t_var, 3, Some("t"), false).unwrap();
        let outer_plan = {
            let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
            let mut tl = NodeList::make1(mcx, g_tle).unwrap();
            tl.lappend(mcx, f_tle).unwrap();
            tl.lappend(mcx, t_tle).unwrap();
            r.plan.targetlist = tl;
            r.plan.plan_width = 24;
            r.seal()
        };

        let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("g"), false).unwrap();
        let arg_var = Node::mk_var(mcx, OUTER_VAR, 3, TEXTOID, -1, C_COLLATION, 0).unwrap();
        let arg_tle = Node::mk_target_entry(mcx, arg_var, 1, None, false).unwrap();
        let mut aggref = Node::build::<Aggref>(mcx).unwrap();
        aggref.aggfnoid = MIN_TEXT_OID;
        aggref.aggtype = TEXTOID;
        aggref.aggtranstype = TEXTOID;
        aggref.inputcollid = C_COLLATION;
        aggref.aggargtypes = types_nodes::list::OidList::make1(mcx, TEXTOID).unwrap();
        aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
        aggref.aggno = 0;
        aggref.aggtransno = 0;
        let min_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("m"), false).unwrap();

        let mut agg = Node::build::<Agg>(mcx).unwrap();
        let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
        tlist.lappend(mcx, min_tle).unwrap();
        agg.plan.targetlist = tlist;
        agg.plan.lefttree = Some(outer_plan);
        agg.aggstrategy = 2;
        agg.numCols = 1;
        agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        agg.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        agg.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        agg.numGroups = 4;
        agg.seal_ref()
    }

    #[test]
    fn hashed_min_text_spills_byref_transvalues() {
        setup();
        let (_cwd, dir) = enter_datadir("mintext");
        let saved_work_mem = init_small::globals::work_mem();
        init_small::globals::set_work_mem(64);

        const N: i32 = 4_000;
        let agg = mk_spill_min_text_agg(leaked_mcx());
        let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
        let mut estate_owner = estate_owner.unwrap();
        estate_owner.with_mut(|estate| {
            let mcx = estate.es_query_cxt;
            let outer_desc = {
                let int4 = |attnum: i16| FormData_pg_attribute {
                    attnum,
                    atttypid: INT4OID,
                    atttypmod: -1,
                    attlen: 4,
                    attbyval: true,
                    attalign: TYPALIGN_INT,
                    attstorage: TYPSTORAGE_PLAIN,
                    ..Default::default()
                };
                let text = FormData_pg_attribute {
                    attnum: 3,
                    atttypid: TEXTOID,
                    atttypmod: -1,
                    attlen: -1,
                    attbyval: false,
                    attalign: TYPALIGN_INT,
                    attstorage: b'x' as i8,
                    ..Default::default()
                };
                let mut attrs = PgVec::new_in(mcx);
                let mut compact = PgVec::new_in(mcx);
                for a in [int4(1), int4(2), text] {
                    compact.push(CompactAttribute::populate_from(&a));
                    attrs.push(a);
                }
                Rc::new(TupleDescData {
                    natts: 3,
                    tdtypeid: 0,
                    tdtypmod: -1,
                    tdrefcount: -1,
                    constr: None,
                    compact_attrs: compact,
                    attrs,
                })
            };
            let outer_id =
                estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
            // SAFETY: agg is leaked ('static) and read-only.
            let agg = unsafe { shorten(agg) };
            let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx()), None).unwrap();

            // Two passes: "b#####" first, then the winning "a#####".
            let mut i = 0i32;
            let mut feed = move |estate: &mut EStateData<'_>| {
                if i >= 2 * N {
                    return Ok(None);
                }
                let (g, s) = if i < N {
                    (i, format!("b{i:05}"))
                } else {
                    (i - N, format!("a{:05}", i - N))
                };
                let mcx = estate.es_query_cxt;
                let slot = estate.slot_mut(outer_id);
                exectuples::exec_clear_tuple(slot, mcx);
                slot.base_mut().tts_values[0] = Datum::from_i32(g);
                slot.base_mut().tts_isnull[0] = false;
                slot.base_mut().tts_values[1] = Datum::from_i32(-g);
                slot.base_mut().tts_isnull[1] = false;
                slot.base_mut().tts_values[2] = text_datum(&s);
                slot.base_mut().tts_isnull[2] = false;
                exectuples::exec_store_virtual_tuple(slot);
                i += 1;
                Ok(Some(outer_id))
            };
            let mut got: Vec<(i32, String)> = Vec::new();
            while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
                let base = estate.slot_mut(slot_id).base();
                assert!(!base.tts_isnull[1]);
                got.push((base.tts_values[0].as_i32(), text_any_str(base.tts_values[1])));
            }
            got.sort_unstable();
            let expect: Vec<(i32, String)> = (0..N).map(|k| (k, format!("a{k:05}"))).collect();
            assert_eq!(got, expect);

            let ai = estate
                .es_agg_instrumentation
                .iter()
                .find_map(|(id, ai)| (*id == agg.plan.plan_node_id).then_some(ai))
                .unwrap();
            assert!(ai.hash_batches_used > 1, "expected spill batches, got {ai:?}");

            crate::exec_end_agg(&mut state);
        });
        assert_eq!(temp_files(&dir), 0, "end must drop the tape files");
        init_small::globals::set_work_mem(saved_work_mem);
    }

    // ------------------------------------------------------------------
    // v2 exact-DISTINCT set spill (distinctset.rs SpillState): direct API
    // tests over the temp-file machinery the module already stands up.
    // ------------------------------------------------------------------

    #[test]
    fn distinct_set_spill_partitions_dedup_exactly() {
        use crate::distinctset::{DistinctKeyKind, DistinctSet};
        setup();
        let (_cwd, dir) = enter_datadir("dsetspill-ints");
        let mcx = leaked_mcx();
        let budget = 256 * 1024;
        let mut s: DistinctSet<'_> = DistinctSet::new();
        let mut expect: std::collections::HashSet<i64> = std::collections::HashSet::new();
        // 60k distinct over 180k inserts; the wrap re-inserts earlier values
        // in later flush epochs (cross-epoch duplicates on the tapes).
        for r in 0..180_000i64 {
            let k = (r * 37) % 60_000 - 7;
            s.insert_i64(k);
            expect.insert(k);
            if s.over_budget(budget) {
                s.spill_flush(DistinctKeyKind::Int64, budget, mcx).unwrap();
            }
        }
        assert!(s.spilled(), "60k i64 keys must cross a 256KB budget");
        s.spill_finish_writes(DistinctKeyKind::Int64, budget, mcx).unwrap();
        let mut got: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for p in 0..s.spill_nparts() {
            assert!(
                s.spill_load_partition(DistinctKeyKind::Int64, p, budget).unwrap(),
                "partition {p} fits the budget"
            );
            assert!(s.mem_bytes() <= budget, "load stayed within budget");
            for &k in s.ints() {
                assert!(got.insert(k), "value {k} appeared in two partitions");
            }
        }
        assert_eq!(got, expect);
        s.spill_end().unwrap();
        s.clear();
        assert_eq!(temp_files(&dir), 0, "spill_end must drop the temp file");
    }

    #[test]
    fn distinct_set_spill_oversize_partition_streams_rest() {
        use crate::distinctset::{DistinctKeyKind, DistinctSet};
        setup();
        let (_cwd, dir) = enter_datadir("dsetspill-oversize");
        let mcx = leaked_mcx();
        // Small budget + big NDV: every partition's distinct load alone
        // exceeds the budget, exercising the partial-load + raw-stream leg.
        let budget = 130 * 1024;
        let mut s: DistinctSet<'_> = DistinctSet::new();
        let mut expect: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for r in 0..200_000i64 {
            let k = r.wrapping_mul(0x9e37_79b9) ^ (r >> 3);
            s.insert_i64(k);
            expect.insert(k);
            if s.over_budget(budget) {
                s.spill_flush(DistinctKeyKind::Int64, budget, mcx).unwrap();
            }
        }
        assert!(s.spilled());
        s.spill_finish_writes(DistinctKeyKind::Int64, budget, mcx).unwrap();
        let mut got: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut saw_partial = false;
        for p in 0..s.spill_nparts() {
            let complete = s.spill_load_partition(DistinctKeyKind::Int64, p, budget).unwrap();
            let mut part: std::collections::HashSet<i64> = s.ints().iter().copied().collect();
            if !complete {
                saw_partial = true;
                let mut vals: Vec<i64> = Vec::new();
                loop {
                    vals.clear();
                    if !s.spill_read_ints(p, &mut vals).unwrap() {
                        break;
                    }
                    part.extend(vals.iter().copied());
                }
            }
            for &k in &part {
                assert!(got.insert(k), "value {k} appeared in two partitions");
            }
        }
        assert!(saw_partial, "the shape must exercise the oversize-partition leg");
        assert_eq!(got, expect);
        s.spill_end().unwrap();
        assert_eq!(temp_files(&dir), 0, "spill_end must drop the temp file");
    }

    #[test]
    fn distinct_set_spill_bytes_roundtrip() {
        use crate::distinctset::{DistinctKeyKind, DistinctSet};
        setup();
        let (_cwd, dir) = enter_datadir("dsetspill-bytes");
        let mcx = leaked_mcx();
        let budget = 256 * 1024;
        let mut s: DistinctSet<'_> = DistinctSet::new();
        let mut expect: std::collections::HashSet<String> = std::collections::HashSet::new();
        for r in 0..40_000u32 {
            let v = format!("value-{}", (r * 17) % 15_000);
            s.insert_bytes(v.as_bytes());
            expect.insert(v);
            if s.over_budget(budget) {
                s.spill_flush(DistinctKeyKind::Bytes, budget, mcx).unwrap();
            }
        }
        assert!(s.spilled(), "15k strings must cross a 256KB budget");
        s.spill_finish_writes(DistinctKeyKind::Bytes, budget, mcx).unwrap();
        let mut got: std::collections::HashSet<String> = std::collections::HashSet::new();
        for p in 0..s.spill_nparts() {
            assert!(
                s.spill_load_partition(DistinctKeyKind::Bytes, p, budget).unwrap(),
                "partition {p} fits the budget"
            );
            for i in 0..s.n_bytes() {
                let v = text_any_str(s.bytes_datum(i));
                assert!(got.insert(v), "a value appeared in two partitions");
            }
        }
        assert_eq!(got, expect);
        s.spill_end().unwrap();
        assert_eq!(temp_files(&dir), 0, "spill_end must drop the temp file");
    }

    #[test]
    fn distinct_set_batch_insert_matches_per_row() {
        use crate::distinctset::DistinctSet;
        let keys: Vec<i64> = (0..10_000i64).map(|r| (r * 13) % 3_000).collect();
        let mut a: DistinctSet<'_> = DistinctSet::new();
        for &k in &keys {
            a.insert_i64(k);
        }
        let mut b: DistinctSet<'_> = DistinctSet::new();
        let mut hashes = Vec::new();
        for chunk in keys.chunks(257) {
            b.insert_i64_batch(chunk, &mut hashes);
        }
        let (mut av, mut bv) = (a.ints().to_vec(), b.ints().to_vec());
        av.sort_unstable();
        bv.sort_unstable();
        assert_eq!(av, bv);
        assert_eq!(a.ints(), b.ints(), "same input order ⇒ same insertion order");
    }
}

// AGG_SORTED with a compressed text group key: the boundary eq (texteq)
// detoasts its args through the frame's armed result mcx (tmpcontext
// per-tuple memory). Unarmed frames panic here (near-unique text-key shape).
#[test]
fn sorted_group_by_compressed_text_key_detoasts_in_boundary_eq() {
    use core::mem::MaybeUninit;
    install_seams();
    detoast::init_seams();

    fn compressed_text(input: &[u8]) -> Vec<u8> {
        let mut dest = vec![MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(input.len())];
        let n = pglz::pglz_compress_into(input, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS).unwrap();
        let total = 8 + n;
        // 4B_C header + rawsize word (toast_compress_datum's inline image).
        let mut image = (((total as u32) << 2) | 0x02).to_ne_bytes().to_vec();
        image.extend_from_slice(&(input.len() as u32).to_ne_bytes());
        image.extend(dest[..n].iter().map(|b| unsafe { b.assume_init() }));
        image
    }

    let phrase_a: Vec<u8> = (0..600).map(|i| b"aaaa bbbb cccc dddd "[i % 20]).collect();
    let phrase_b: Vec<u8> = (0..600).map(|i| b"eeee ffff gggg hhhh "[i % 20]).collect();
    let img_a: &'static [u8] = Vec::leak(compressed_text(&phrase_a));
    let img_b: &'static [u8] = Vec::leak(compressed_text(&phrase_b));
    let rows: &'static [&'static [u8]] = Vec::leak(vec![img_a, img_a, img_b]);

    let mcx = leaked_mcx();
    let outer_var = Node::mk_var(mcx, 1, 1, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let outer_tle = Node::mk_target_entry(mcx, outer_var, 1, Some("s"), false).unwrap();
    let outer_plan = {
        let mut r = Node::build::<types_nodes::plannodes::Result>(mcx).unwrap();
        r.plan.targetlist = NodeList::make1(mcx, outer_tle).unwrap();
        r.plan.plan_width = 32;
        r.seal()
    };
    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("s"), false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = COUNT_STAR_OID;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = true;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let count_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("count"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, count_tle).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(outer_plan);
    agg.aggstrategy = 1; // AGG_SORTED
    agg.numCols = 1;
    agg.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = mcx::slice_borrow_in(mcx, &[TEXT_EQ]).unwrap();
    agg.grpCollations = mcx::slice_borrow_in(mcx, &[C_COLLATION]).unwrap();
    agg.numGroups = 2;
    let agg = agg.seal_ref();

    fn text_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: TEXTOID,
            atttypmod: -1,
            attlen: -1,
            attbyval: false,
            attalign: TYPALIGN_INT,
            attstorage: b'x' as i8,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }
    fn result_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
        let a1 = FormData_pg_attribute {
            attnum: 1,
            atttypid: TEXTOID,
            atttypmod: -1,
            attlen: -1,
            attbyval: false,
            attalign: TYPALIGN_INT,
            attstorage: b'x' as i8,
            ..Default::default()
        };
        let a2 = FormData_pg_attribute {
            attnum: 2,
            atttypid: INT8OID,
            attlen: 8,
            attbyval: true,
            attalign: TYPALIGN_DOUBLE,
            atttypmod: -1,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&a1));
        compact.push(CompactAttribute::populate_from(&a2));
        attrs.push(a1);
        attrs.push(a2);
        Rc::new(TupleDescData {
            natts: 2,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    let counts = estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_id =
            estate.exec_init_extra_tuple_slot(Some(text_desc(mcx)), TupleSlotKind::Virtual);
        // SAFETY: agg is leaked ('static) and read-only.
        let agg = unsafe { shorten(agg) };
        let mut state = exec_init_agg(agg, estate, 0, result_desc(leaked_mcx()), None).unwrap();
        let mut i = 0usize;
        let mut feed = move |estate: &mut EStateData<'_>| {
            if i >= rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(outer_id);
            exectuples::exec_clear_tuple(slot, mcx);
            slot.base_mut().tts_values[0] = Datum::from_usize(rows[i].as_ptr() as usize);
            slot.base_mut().tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(slot);
            i += 1;
            Ok(Some(outer_id))
        };
        let mut counts: Vec<i64> = Vec::new();
        while let Some(slot_id) = exec_agg(&mut state, estate, &mut feed).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[1]);
            counts.push(base.tts_values[1].as_i64());
        }
        counts
    });
    assert_eq!(counts, vec![2, 1]);
}

// ---------------------------------------------------------------------------
// sorted-arm lane: SortedEmitAcc capture round trip (byval + varlena deep
// copy, 8-aligned arena, end-resolved fixups).
// ---------------------------------------------------------------------------

#[test]
fn sorted_emit_acc_round_trip() {
    use crate::sortedsink::SortedEmitAcc;
    // Columns: [int8 byval, varlena text, null varlena].
    let spec: crate::sortedsink::SortedByrefSpec = vec![0, -1, -1];
    let mut acc = SortedEmitAcc::new(3);
    // A 4B-header varlena image: total size 9 (4 header + 5 payload).
    let mut img = vec![0u8; 9];
    let hdr = (9u32) << 2; // 4B uncompressed varsize encoding
    img[..4].copy_from_slice(&hdr.to_le_bytes());
    img[4..].copy_from_slice(b"hello");
    let values = [
        Datum::from_i64(42),
        Datum::from_usize(img.as_ptr() as usize),
        Datum::null(),
    ];
    let nulls = [false, false, true];
    // SAFETY: img is a live 4B-U varlena image for the duration of the push.
    unsafe { acc.push_row(&values, &nulls, &spec).unwrap() };
    unsafe { acc.push_row(&values, &nulls, &spec).unwrap() };
    assert!(!acc.is_empty());
    let seg = acc.finish();
    drop(img); // the seg must be self-contained
    assert_eq!(seg.nrows, 2);
    assert_eq!(seg.natts, 3);
    for row in 0..2 {
        let base = row * 3;
        assert_eq!(seg.values[base].as_i64(), 42);
        assert!(!seg.nulls[base]);
        assert!(seg.nulls[base + 2]);
        let p = seg.values[base + 1].as_usize() as *const u8;
        assert_eq!(p as usize % 8, 0, "arena images are 8-aligned");
        // SAFETY: points into seg.arena (self-contained copy).
        let got = unsafe { core::slice::from_raw_parts(p, 9) };
        assert_eq!(&got[4..], b"hello");
    }
}

// q18fin r3 red/green unit — byref merge under variable hash IVs (the t26
// integration ledger's "q18fin-t26-r2 re-earn verdict" defect). Participants
// (leader partial = worker -1, workers 0 and 1) build their partial tables
// with per-participant hash IVs (execGrouping.c parity — the big-group
// parallel-finalize stall fix), but the byref finalize merge compares STORED
// hashes across the handed tables AND the finalize's own IV=0 table (parts[0]
// of consume_handoff). Without the export-boundary rebase
// (TupleHashTable::hash_to_iv0 in export_handed_table), the same key carries
// a different hash per participant: radix buckets diverge (partition by
// hash>>24), equal keys never match in the bucket probe (h == e.hash() gate),
// and every group emits once PER PARTICIPANT — select_parallel's
// `select length(stringu1) from tenk1 group by length(stringu1)` under a
// Finalize HashAggregate returned 5 duplicate rows of the single group
// (4 workers + leader), and write_parallel's parallel matview CREATE then
// failed its unique index. RED at q18fin-t26-r2 (exports copy worker-IV
// hashes verbatim); GREEN with the r3 rebase.
#[test]
fn byref_merge_handed_tables_share_leader_bucket_mapping_under_variable_iv() {
    install_seams();
    let ctx = MemoryContext::new("q18r3-byref-iv");
    let mcx = ctx.mcx();
    let table_ctx = MemoryContext::new("q18r3-entries");
    let desc = one_col_desc(mcx, INT4OID, 4, TYPALIGN_INT);
    // The repro's shape: a handful of int4 groups, identical in every
    // participant (kinds = [] — the repro query has no aggregates at all).
    let keys: [i32; 3] = [6, 42, -1];

    let build_filled = |iv: u32| {
        // hashint4 / int4eq, additionalsize 0, as the repro's plain GROUP BY.
        let mut table = ::execgrouping::build_tuple_hash_table_with_iv(
            mcx,
            &desc,
            &[1],
            &[F_INT4EQ],
            &[F_HASHINT4],
            &[0],
            16,
            0,
            iv,
        )
        .unwrap();
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        for &k in &keys {
            exectuples::exec_clear_tuple(&mut slot, mcx);
            slot.base_mut().tts_values[0] = Datum::from_i32(k);
            slot.base_mut().tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(&mut slot);
            let hash = table.hash_slot(&mut slot).unwrap();
            let (ix, _) = table.lookup(&mut slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
            ix.unwrap();
        }
        table
    };

    // The finalize's own row-built table: IV 0 (AGGSPLIT_FINAL_DESERIAL never
    // sets use_variable_hash_iv) — the reference mapping every handed source
    // must be comparable with.
    let finalize_table = build_filled(0);

    // Per-participant IVs exactly as build_tuple_hash_table derives them:
    // murmurhash32(ParallelWorkerNumber). Worker 0 is C's quirk (IV == 0).
    let participant_ivs: [u32; 3] = [
        ::hashfn::murmurhash32(-1i32 as u32), // leader-participation partial
        ::hashfn::murmurhash32(0),            // worker 0
        ::hashfn::murmurhash32(1),            // worker 1
    ];
    let handed: Vec<crate::merge::HandedAggTable> = participant_ivs
        .iter()
        .map(|&iv| {
            let table = build_filled(iv);
            // The REAL install export core (install_classic_handoff's body).
            crate::merge::export_handed_table(&table, &[], 0, false)
        })
        .collect();

    // The byref-merge invariant (docs in export_handed_table): every source's
    // stored hash for the same key must equal the finalize table's — one
    // bucket mapping across parts[0] + all handed tables. Entries are in
    // insertion order on both sides, so index j <=> keys[j].
    for (j, &k) in keys.iter().enumerate() {
        let expect = finalize_table.entries()[j].hash();
        for (t, &iv) in handed.iter().zip(&participant_ivs) {
            assert_eq!(
                t.entries()[j].hash(),
                expect,
                "handed hash for key {k} from participant iv={iv:#x} must ride the leader's IV=0 mapping",
            );
        }
    }

    // The observable defect: merged group count. The bucket merge unifies
    // entries agreeing on (bucket, hash, key); with identical keys per
    // source, distinct stored hashes per key index == distinct emitted
    // groups. r2 emitted one per PARTICIPANT (the 5-duplicate-rows repro).
    let mut groups = std::collections::HashSet::new();
    for (j, e) in finalize_table.entries().iter().enumerate() {
        groups.insert((j, e.hash()));
    }
    for t in &handed {
        for (j, e) in t.entries().iter().enumerate() {
            groups.insert((j, e.hash()));
        }
    }
    assert_eq!(
        groups.len(),
        keys.len(),
        "one merged group per key — duplicate finalize groups otherwise (write_parallel unique-index class)",
    );
}

/// SE-GROUPONLY knob (night/subquery-admission): `PGRUST_LANE_V2_GROUPONLY`
/// is DEFAULT ON since t36 flips2 (GL-GROUPONLY-1 FLIP-RECOMMENDED) and
/// only the exact kill spellings `0`/`off` disarm it — the flipped-kill
/// idiom (a typo'd kill leaves the measured-winning default in place).
/// Pins the flipped-default posture + the kill's exact spellings.
#[test]
fn grouponly_knob_is_default_on_with_kill() {
    assert!(crate::grouponly_spelling_on(None), "unset must be ON (t36 flipped default)");
    assert!(!crate::grouponly_spelling_on(Some("0")), "kill spelling");
    assert!(!crate::grouponly_spelling_on(Some("off")), "kill spelling");
    assert!(crate::grouponly_spelling_on(Some("")), "non-kill spellings stay ON");
    assert!(crate::grouponly_spelling_on(Some("true")), "non-kill spellings stay ON");
    assert!(
        crate::grouponly_spelling_on(Some("OFF")),
        "kill is case-sensitive, like the arm kills"
    );
    assert!(crate::grouponly_spelling_on(Some("1")));
    assert!(crate::grouponly_spelling_on(Some("on")));
}

/// SE-GROUPONLY: the vacuous fold plan is structurally empty — no
/// transitions, no lane columns, no guards, unguarded — so every grouped
/// fold over it is a no-op by construction and the dangling pergroup
/// sentinels the zero-trans probes hand back are never dereferenced.
#[test]
fn grouponly_empty_plan_is_vacuous() {
    let ctx = ::mcx::MemoryContext::new("grouponly-test");
    let plan = ::lanefold::empty_plan(ctx.mcx());
    assert!(plan.trans.is_empty());
    assert!(plan.cols.is_empty());
    assert!(plan.guards.is_empty() && plan.vguards.is_empty() && plan.uguards.is_empty());
    assert!(plan.filters.is_empty() && plan.resid.is_empty());
    assert!(!plan.guarded);
}
