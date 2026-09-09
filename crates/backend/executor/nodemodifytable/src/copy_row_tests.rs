use super::*;
use std::cell::Cell;
use types_core::INVALID_PROC_NUMBER;
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData};
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleConstr};

fn descriptor<'m>(mcx: mcx::Mcx<'m>, checks: bool) -> Rc<TupleDescData<'m>> {
    let att = FormData_pg_attribute {
        attnum: 1, atttypid: 25, atttypmod: -1, attlen: -1,
        attbyval: false, attalign: b'i' as i8, attstorage: b'x' as i8,
        ..Default::default()
    };
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact_attrs = mcx::PgVec::new_in(mcx);
    compact_attrs.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let mut check = mcx::PgVec::new_in(mcx);
    if checks {
        check.push(types_tuple::ConstrCheck { ccname: None, ccbin: None, ccenforced: true, ccvalid: true, ccnoinherit: false });
    }
    Rc::new(TupleDescData {
        natts: 1, tdtypeid: 0, tdtypmod: -1, tdrefcount: -1,
        attrs, compact_attrs,
        constr: Some(mcx::box_new_in(mcx, TupleConstr {
            defval: mcx::PgVec::new_in(mcx), check,
            missing: mcx::PgVec::new_in(mcx), num_defval: 0,
            num_check: u16::from(checks), relchecks: i16::from(checks),
            has_not_null: false, has_generated_stored: true, has_generated_virtual: false,
        })),
    })
}

fn text_image(bytes: usize) -> Vec<u8> {
    let mut image = vec![b'x'; bytes + 4];
    image[..4].copy_from_slice(&(((bytes + 4) as u32) << 2).to_ne_bytes());
    image
}

fn constant<'m>(mcx: mcx::Mcx<'m>, image: &[u8]) -> PgBox<'m, ExprState<'m>> {
    let node = types_nodes::Node::mk_const(mcx, 25, -1, 0, -1,
        Datum::from_usize(image.as_ptr() as usize), false, false).unwrap();
    execexpr::exec_init_expr(mcx, Some(node), execexpr::ParamBind::NONE).unwrap().unwrap()
}

#[test]
fn generated_slot_owns_values_after_short_row_drop() {
    let cx = mcx::MemoryContext::new_bump("generated-statement");
    let mcx = cx.mcx();
    let image = text_image(64);
    for kind in [types_slot::TupleSlotKind::Virtual, types_slot::TupleSlotKind::HeapTuple,
                 types_slot::TupleSlotKind::BufferHeapTuple] {
        let rel = relation(mcx, descriptor(mcx, false));
        let mut slot = exectuples::make_tuple_table_slot(mcx, kind, Some(rel.rd_att.clone()));
        let mut exprs = mcx::PgVec::new_in(mcx);
        exprs.push(GeneratedExpr { attnum: 0, state: constant(mcx, &image) });
        let mut exprs = Some(exprs);
        for _ in 0..3 {
            exectuples::exec_clear_tuple(&mut slot, mcx);
            slot.base_mut().tts_isnull.fill(true);
            exectuples::exec_store_virtual_tuple(&mut slot);
            {
                let row = mcx::MemoryContext::new_bump("generated-short-row");
                exec_compute_stored_generated_in_row(mcx, row.mcx(), &mut exprs, &rel, &mut slot).unwrap();
            }
            let mut null = true;
            let datum = exectuples::slot_getattr(&mut slot, 1, &mut null);
            assert!(!null);
            // SAFETY: successful materialization owns the complete text image after row teardown.
            let bytes = unsafe {
                let ptr = datum.as_usize() as *const u8;
                let header = if types_tuple::varatt::varatt_is_1b(ptr) { 1 } else { 4 };
                let size = types_tuple::varatt::varsize_any(ptr);
                core::slice::from_raw_parts(ptr.add(header), size - header)
            };
            assert_eq!(bytes, &image[4..]);
        }
        exectuples::exec_clear_tuple(&mut slot, mcx);
    }
}

#[test]
fn generated_materialize_error_clears_short_row_values() {
    let cx = mcx::MemoryContext::new("generated-limited").with_limit(128 * 1024);
    let mcx = cx.mcx();
    let image = text_image(256 * 1024);
    let rel = relation(mcx, descriptor(mcx, false));
    let mut slot = exectuples::make_tuple_table_slot(mcx, types_slot::TupleSlotKind::Virtual, Some(rel.rd_att.clone()));
    slot.base_mut().tts_isnull.fill(true);
    exectuples::exec_store_virtual_tuple(&mut slot);
    let mut exprs = mcx::PgVec::new_in(mcx);
    exprs.push(GeneratedExpr { attnum: 0, state: constant(mcx, &image) });
    let mut exprs = Some(exprs);
    {
        let row = mcx::MemoryContext::new_bump("generated-error-row");
        assert!(exec_compute_stored_generated_in_row(mcx, row.mcx(), &mut exprs, &rel, &mut slot).is_err());
    }
    assert!(slot.base().is_empty());
    assert!(slot.base().tts_isnull.iter().all(|x| *x));
    assert!(slot.base().tts_values.iter().all(|x| x.as_usize() == 0));
}

    fn relation<'m>(
        mcx: ::mcx::Mcx<'m>,
        desc: Rc<TupleDescData<'m>>,
    ) -> Relation<'m> {
        let mut relname = NameData::default();
        relname.namestrcpy("copy_row_test");
        let rd_rel = FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 0,
            relfilenode: 70001,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: types_core::RELPERSISTENCE_PERMANENT,
            relkind: types_rel::RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        let data = RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: 70001,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: 70001, dbId: 5 },
            },
            rd_rel,
            rd_att: desc,
            rd_index: None,
            rd_opcintype: ::mcx::PgVec::new_in(mcx),
            rd_opfamily: ::mcx::PgVec::new_in(mcx),
            rd_indoption: ::mcx::PgVec::new_in(mcx),
            rd_indcollation: ::mcx::PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: ::mcx::PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Relation::open(data, None)
    }


fn operator<'m>(mcx: mcx::Mcx<'m>, func: u32, ty: u32, left: types_nodes::Node<'m>, right: types_nodes::Node<'m>) -> types_nodes::Node<'m> {
    types_nodes::Node::mk(mcx, types_nodes::primnodes::OpExpr {
        opno: 0, opfuncid: func, opresulttype: ty, opretset: false,
        opcollid: 0, inputcollid: 950,
        args: types_nodes::list::NodeList::make2(mcx, left, right).unwrap(), location: -1,
    }).unwrap()
}

#[test]
fn check_allocating_expression_reuses_cache_after_short_row_drop() {
    miscinit_seams::get_user_id::set(|| 10);
    aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
    let cx = mcx::MemoryContext::new_bump("check-statement");
    let mcx = cx.mcx();
    let image = text_image(32);
    let input = types_nodes::Node::mk_const(mcx, 25, -1, 0, -1,
        Datum::from_usize(image.as_ptr() as usize), false, false).unwrap();
    let concat = operator(mcx, 1258, 25, input, input);
    let equality = operator(mcx, 67, 16, concat, concat);
    let state = execexpr::exec_init_expr(mcx, Some(equality), execexpr::ParamBind::NONE).unwrap().unwrap();
    let mut checks = mcx::PgVec::new_in(mcx);
    checks.push(CheckExpr { name: mcx::PgString::from_str_in("same", mcx).unwrap(), state: Some(state) });
    let mut checks = Some(checks);
    let rel = relation(mcx, descriptor(mcx, true));
    let mut slot = exectuples::make_tuple_table_slot(mcx, types_slot::TupleSlotKind::Virtual, Some(rel.rd_att.clone()));
    slot.base_mut().tts_isnull.fill(true);
    exectuples::exec_store_virtual_tuple(&mut slot);
    let mut virtual_nn = None;
    for _ in 0..4 {
        {
            let row = mcx::MemoryContext::new_bump("check-short-row");
            exec_constraints_in_row(mcx, row.mcx(), &mut checks, &mut virtual_nn, &rel, &mut slot, None, None).unwrap();
        }
        let state = checks.as_mut().unwrap()[0].state.as_deref_mut().unwrap();
        let result = execexpr::exec_eval_expr(state, &mut EvalSlots::default()).unwrap();
        assert!(!result.isnull && result.value.as_bool());
    }
}
