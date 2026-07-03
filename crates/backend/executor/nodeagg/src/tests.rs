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
const F_HASHINT4: u32 = 450;
const INT_HASH_FAM: u32 = 1977;

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
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape {
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
            }))
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
            Ok(v)
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(
                if (opfamily, lefttype, righttype, procnum)
                    == (INT_HASH_FAM, INT4OID, INT4OID, 1)
                {
                    F_HASHINT4
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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx())).unwrap();
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
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx())).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx())).unwrap();
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
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx())).unwrap();
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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();
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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
            let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, result_desc).unwrap();

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
        let mut state = exec_init_agg(agg, estate, 0, two_col_desc(leaked_mcx())).unwrap();

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
