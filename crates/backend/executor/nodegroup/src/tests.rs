use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{create_executor_state, EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::INT4OID;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Group;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::{exec_end_group, exec_group, exec_init_group, exec_rescan_group};

const INT4_EQ: u32 = 96;
const F_INT4EQ: u32 = 65;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == INT4OID).then_some(PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: TYPALIGN_INT,
                typstorage: TYPSTORAGE_PLAIN,
                typcollation: 0,
            }))
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape {
                oprnamespace: 11,
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
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodegroup-test")));
    m.mcx()
}

unsafe fn shorten<'a>(g: &Group<'_>) -> &'a Group<'a> {
    unsafe { core::mem::transmute::<&Group<'_>, &'a Group<'a>>(g) }
}

fn one_col_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
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

fn mk_group(mcx: Mcx<'_>) -> &Group<'_> {
    let var =
        Node::mk_var(mcx, ::types_nodes::primnodes::OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let mut g = Node::build::<Group>(mcx).unwrap();
    g.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    g.numCols = 1;
    g.grpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    g.grpOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    g.grpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    g.seal_ref()
}

fn feeder<'mcx>(
    outer_id: ExecSlotId,
    rows: &'static [Option<i32>],
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
    }
}

fn run_group(rows: &'static [Option<i32>]) -> Vec<Option<i32>> {
    install_seams();
    let gp = mk_group(leaked_mcx());
    let mut estate_owner =
        create_executor_state(Box::leak(Box::new(MemoryContext::new("q")))).unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let outer_desc = one_col_desc(mcx);
        let outer_id = estate.exec_init_extra_tuple_slot(Some(outer_desc), TupleSlotKind::Virtual);
        // SAFETY: gp is leaked ('static) and read-only.
        let gp = unsafe { shorten(gp) };
        let result_desc = one_col_desc(leaked_mcx());
        let params = estate.param_bind();
        let proj = ::execexpr::exec_build_projection_info(mcx, &gp.plan.targetlist, None, params)
            .unwrap();
        let mut state = exec_init_group(
            gp,
            estate,
            0,
            &result_desc.clone(),
            result_desc,
            None,
            proj,
        )
        .unwrap();

        let mut got: Vec<Option<i32>> = Vec::new();
        {
            let mut feed = feeder(outer_id, rows);
            while let Some(slot_id) = exec_group(&mut state, estate, &mut feed).unwrap() {
                let slot = estate.slot_mut(slot_id);
                exectuples::slot_getallattrs(slot);
                let base = slot.base();
                got.push((!base.tts_isnull[0]).then(|| base.tts_values[0].as_i32()));
            }
        }

        exec_rescan_group(&mut state, estate);
        let mut feed = feeder(outer_id, rows);
        let mut again: Vec<Option<i32>> = Vec::new();
        while let Some(slot_id) = exec_group(&mut state, estate, &mut feed).unwrap() {
            let slot = estate.slot_mut(slot_id);
            exectuples::slot_getallattrs(slot);
            let base = slot.base();
            again.push((!base.tts_isnull[0]).then(|| base.tts_values[0].as_i32()));
        }
        assert_eq!(again, got);
        exec_end_group(&mut state);
        got
    })
}

#[test]
fn one_row_per_sorted_group() {
    assert_eq!(
        run_group(&[Some(1), Some(1), Some(2), Some(2), Some(2), Some(3)]),
        vec![Some(1), Some(2), Some(3)]
    );
}

#[test]
fn empty_input_returns_nothing() {
    assert!(run_group(&[]).is_empty());
}

// Grouping equality is NOT DISTINCT: adjacent NULL keys form one group.
#[test]
fn null_keys_form_one_group() {
    assert_eq!(run_group(&[None, None, Some(5), Some(5)]), vec![None, Some(5)]);
}
