use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{create_executor_state, EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{InvalidOid, BTREE_AM_OID, INT4OID};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::SetOp;
use ::types_pathnodes::{
    SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
    SETOP_HASHED, SETOP_SORTED,
};
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::{exec_init_set_op, exec_rescan_set_op, exec_set_op};

const INT4_EQ: u32 = 96;
const INT4_LT: u32 = 97;
const F_INT4EQ: u32 = 65;
const F_HASHINT4: u32 = 450;
const F_BTINT4SORTSUPPORT: u32 = 3130;
const INT_BTREE_FAM: u32 = 1976;
const INT_HASH_FAM: u32 = 1977;
const HASH_AM_OID: u32 = 405;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
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
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
            match opno {
                INT4_EQ => v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INT_HASH_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: HASH_AM_OID,
                }),
                INT4_LT => v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INT_BTREE_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: BTREE_AM_OID,
                }),
                _ => {}
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(match (opfamily, lefttype, righttype, procnum) {
                (INT_HASH_FAM, INT4OID, INT4OID, 1) => F_HASHINT4,
                (INT_BTREE_FAM, INT4OID, INT4OID, 2) => F_BTINT4SORTSUPPORT,
                _ => InvalidOid,
            })
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodesetop-test")));
    m.mcx()
}

unsafe fn shorten<'a>(s: &SetOp<'_>) -> &'a SetOp<'a> {
    unsafe { core::mem::transmute::<&SetOp<'_>, &'a SetOp<'a>>(s) }
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

fn mk_setop(mcx: Mcx<'_>, cmd: u32, strategy: u32) -> &SetOp<'_> {
    let var = Node::mk_var(mcx, ::types_nodes::primnodes::OUTER_VAR, 1, INT4OID, -1, 0, 0)
        .unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let mut s = Node::build::<SetOp>(mcx).unwrap();
    s.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    s.cmd = cmd;
    s.strategy = strategy;
    s.numCols = 1;
    s.cmpColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    s.cmpOperators = mcx::slice_borrow_in(
        mcx,
        &[if strategy == SETOP_HASHED { INT4_EQ } else { INT4_LT }],
    )
    .unwrap();
    s.cmpCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    s.cmpNullsFirst = mcx::slice_borrow_in(mcx, &[false]).unwrap();
    s.numGroups = 4;
    s.seal_ref()
}

fn feeder<'mcx>(
    slot_id: ExecSlotId,
    rows: &'static [Option<i32>],
) -> impl FnMut(&mut EStateData<'mcx>) -> ::types_error::PgResult<Option<ExecSlotId>> {
    let mut i = 0usize;
    move |estate| {
        if i >= rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(slot_id);
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
        Ok(Some(slot_id))
    }
}

fn run_setop(
    cmd: u32,
    strategy: u32,
    outer_rows: &'static [Option<i32>],
    inner_rows: &'static [Option<i32>],
) -> Vec<Option<i32>> {
    install_seams();
    let so = mk_setop(leaked_mcx(), cmd, strategy);
    let estate_owner = create_executor_state(Box::leak(Box::new(MemoryContext::new("q"))));
    let mut estate_owner = estate_owner.unwrap();
    estate_owner.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let child_desc = one_col_desc(mcx);
        let outer_id =
            estate.exec_init_extra_tuple_slot(Some(child_desc.clone()), TupleSlotKind::Virtual);
        let inner_id =
            estate.exec_init_extra_tuple_slot(Some(child_desc), TupleSlotKind::Virtual);
        // SAFETY: so is leaked ('static) and read-only.
        let so = unsafe { shorten(so) };
        let result_desc = one_col_desc(leaked_mcx());
        let mut state =
            exec_init_set_op(so, estate, 0, &result_desc.clone(), result_desc).unwrap();

        let got = collect(&mut state, estate, outer_id, inner_id, outer_rows, inner_rows);
        let rescan_children = exec_rescan_set_op(&mut state, estate);
        assert_eq!(rescan_children, strategy == SETOP_SORTED);
        // Hashed re-walks the built table (feeders unused); sorted re-reads.
        let again = collect(&mut state, estate, outer_id, inner_id, outer_rows, inner_rows);
        assert_eq!(again, got);
        got
    })
}

fn collect<'mcx>(
    state: &mut crate::SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
    inner_id: ExecSlotId,
    outer_rows: &'static [Option<i32>],
    inner_rows: &'static [Option<i32>],
) -> Vec<Option<i32>> {
    let mut fo = feeder(outer_id, outer_rows);
    let mut fi = feeder(inner_id, inner_rows);
    let mut got: Vec<Option<i32>> = Vec::new();
    while let Some(slot_id) = exec_set_op(state, estate, &mut fo, &mut fi).unwrap() {
        let slot = estate.slot_mut(slot_id);
        exectuples::slot_getallattrs(slot);
        let base = slot.base();
        got.push((!base.tts_isnull[0]).then(|| base.tts_values[0].as_i32()));
    }
    got
}

#[test]
fn hashed_intersect() {
    assert_eq!(
        run_setop(
            SETOPCMD_INTERSECT,
            SETOP_HASHED,
            &[Some(1), Some(2), Some(2), Some(3)],
            &[Some(2), Some(3), Some(3), Some(4)],
        ),
        vec![Some(2), Some(3)]
    );
}

#[test]
fn hashed_intersect_all() {
    assert_eq!(
        run_setop(
            SETOPCMD_INTERSECT_ALL,
            SETOP_HASHED,
            &[Some(2), Some(2), Some(3)],
            &[Some(2), Some(2), Some(2), Some(3)],
        ),
        vec![Some(2), Some(2), Some(3)]
    );
}

#[test]
fn hashed_except() {
    assert_eq!(
        run_setop(
            SETOPCMD_EXCEPT,
            SETOP_HASHED,
            &[Some(1), Some(2), Some(2), Some(3)],
            &[Some(2)],
        ),
        vec![Some(1), Some(3)]
    );
}

#[test]
fn hashed_except_all() {
    assert_eq!(
        run_setop(
            SETOPCMD_EXCEPT_ALL,
            SETOP_HASHED,
            &[Some(2), Some(2), Some(2), Some(3)],
            &[Some(2)],
        ),
        vec![Some(2), Some(2), Some(3)]
    );
}

#[test]
fn hashed_nulls_group_together() {
    assert_eq!(
        run_setop(
            SETOPCMD_INTERSECT,
            SETOP_HASHED,
            &[None, Some(1)],
            &[None],
        ),
        vec![None]
    );
    assert_eq!(
        run_setop(SETOPCMD_EXCEPT, SETOP_HASHED, &[None, None, Some(1)], &[None]),
        vec![Some(1)]
    );
}

#[test]
fn hashed_empty_outer_skips_inner() {
    assert!(run_setop(SETOPCMD_INTERSECT, SETOP_HASHED, &[], &[Some(1)]).is_empty());
}

#[test]
fn sorted_intersect() {
    assert_eq!(
        run_setop(
            SETOPCMD_INTERSECT,
            SETOP_SORTED,
            &[Some(1), Some(2), Some(2), Some(3)],
            &[Some(2), Some(3), Some(3), Some(4)],
        ),
        vec![Some(2), Some(3)]
    );
}

#[test]
fn sorted_intersect_all() {
    assert_eq!(
        run_setop(
            SETOPCMD_INTERSECT_ALL,
            SETOP_SORTED,
            &[Some(2), Some(2), Some(3)],
            &[Some(2), Some(2), Some(2), Some(3)],
        ),
        vec![Some(2), Some(2), Some(3)]
    );
}

#[test]
fn sorted_except_all() {
    assert_eq!(
        run_setop(
            SETOPCMD_EXCEPT_ALL,
            SETOP_SORTED,
            &[Some(1), Some(2), Some(2), Some(2), Some(3)],
            &[Some(2), Some(4)],
        ),
        vec![Some(1), Some(2), Some(2), Some(3)]
    );
}

// NULLs sort last (nulls_first=false) and compare equal within a group.
#[test]
fn sorted_nulls_group_together() {
    assert_eq!(
        run_setop(
            SETOPCMD_EXCEPT,
            SETOP_SORTED,
            &[Some(1), None, None],
            &[None],
        ),
        vec![Some(1)]
    );
}

#[test]
fn sorted_empty_outer_skips_inner() {
    assert!(run_setop(SETOPCMD_INTERSECT, SETOP_SORTED, &[], &[Some(1)]).is_empty());
}
