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
#[should_panic(expected = "AGG_SORTED/HASHED/MIXED")]
fn hashed_strategy_panics() {
    install_seams();
    let mcx = leaked_mcx();
    let agg_node = {
        let mut agg = Node::build::<Agg>(mcx).unwrap();
        agg.aggstrategy = 2;
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
