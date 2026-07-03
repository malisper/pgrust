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
const SUM_INT4_OID: u32 = 2108;
const INT8INC_OID: u32 = 1219;
const INT4_SUM_OID: u32 = 1841;
const INT8PL_OID: u32 = 463;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
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
                    aggtranstype: INT8OID,
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
                    aggtranstype: INT8OID,
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
                COUNT_STAR_OID => Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap())),
                SUM_INT4_OID => Some(None),
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
#[should_panic(expected = "AGG_SORTED/MIXED")]
fn sorted_strategy_panics() {
    install_seams();
    let mcx = leaked_mcx();
    let agg_node = {
        let mut agg = Node::build::<Agg>(mcx).unwrap();
        agg.aggstrategy = 1;
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
