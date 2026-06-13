use super::*;
use std::cell::RefCell;

// Thread-local: callbacks run on the registering test's own thread, and the
// test harness runs tests on separate threads — a process-global log would
// race across tests.
thread_local! {
    static CALLBACK_LOG: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

fn log_cb(_mcx: Mcx<'_>, arg: Datum) -> PgResult<()> {
    CALLBACK_LOG.with(|l| l.borrow_mut().push(arg.as_usize()));
    Ok(())
}

fn other_cb(_mcx: Mcx<'_>, _arg: Datum) -> PgResult<()> {
    Ok(())
}

#[test]
fn prevpower2() {
    assert_eq!(pg_prevpower2_size_t(1), 1);
    assert_eq!(pg_prevpower2_size_t(2), 2);
    assert_eq!(pg_prevpower2_size_t(3), 2);
    assert_eq!(pg_prevpower2_size_t(4096), 4096);
    assert_eq!(pg_prevpower2_size_t(4097), 4096);
    // work_mem = 4096 KB: 4096*1024/16 = 262144 (already a power of two)
    assert_eq!(pg_prevpower2_size_t(4096 * 1024 / 16), 262144);
}

#[test]
fn executor_state_create_and_free() {
    let top = MemoryContext::new("test-top");
    let estate = CreateExecutorState(&top).unwrap();
    estate.with(|s| assert_eq!(s.es_direction, types_nodes::execnodes::ForwardScanDirection));
    estate.with(|s| assert!(s.es_exprcontexts.is_empty()));
    estate.with(|s| assert_eq!(s.es_processed, 0));
    FreeExecutorState(estate).unwrap();
}

#[test]
fn expr_context_create_free_and_per_tuple() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let a = CreateExprContext(estate).unwrap();
        let b = CreateExprContext(estate).unwrap();
        assert_ne!(a, b);
        assert_eq!(estate.es_exprcontexts.len(), 2);

        // MakePerTupleExprContext is lazy and idempotent.
        let pt1 = MakePerTupleExprContext(estate).unwrap();
        let pt2 = MakePerTupleExprContext(estate).unwrap();
        assert_eq!(pt1, pt2);
        assert_eq!(estate.es_exprcontexts.len(), 3);

        // FreeExprContext tombstones without disturbing other ids.
        FreeExprContext(estate, a, true).unwrap();
        assert!(estate.es_exprcontexts[a.0 as usize].is_none());
        let _ = estate.ecxt(b); // still resolvable
    });
    FreeExecutorState(estate).unwrap();
}

#[test]
fn callbacks_run_in_reverse_order_and_only_on_commit() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let id = CreateExprContext(estate).unwrap();
        let ec = estate.ecxt_mut(id);
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(1)).unwrap();
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(2)).unwrap();
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(3)).unwrap();

        CALLBACK_LOG.with(|l| l.borrow_mut().clear());
        ReScanExprContext(estate.ecxt_mut(id)).unwrap();
        // reverse registration order
        CALLBACK_LOG.with(|l| assert_eq!(*l.borrow(), vec![3, 2, 1]));
        // list was emptied
        assert!(estate.ecxt(id).ecxt_callbacks.is_none());

        // isCommit = false: callbacks dropped, not called.
        let ec = estate.ecxt_mut(id);
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(9)).unwrap();
        CALLBACK_LOG.with(|l| l.borrow_mut().clear());
        FreeExprContext(estate, id, false).unwrap();
        CALLBACK_LOG.with(|l| assert!(l.borrow().is_empty()));
    });
    FreeExecutorState(estate).unwrap();
}

#[test]
fn unregister_removes_matching_entries_only() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let id = CreateExprContext(estate).unwrap();
        let ec = estate.ecxt_mut(id);
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(1)).unwrap();
        RegisterExprContextCallback(ec, other_cb, Datum::from_usize(1)).unwrap();
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(2)).unwrap();
        RegisterExprContextCallback(ec, log_cb, Datum::from_usize(1)).unwrap();

        // Removes BOTH (log_cb, 1) entries; keeps (other_cb, 1) and (log_cb, 2).
        UnregisterExprContextCallback(ec, log_cb, Datum::from_usize(1));

        CALLBACK_LOG.with(|l| l.borrow_mut().clear());
        ShutdownExprContext(ec, true).unwrap();
        CALLBACK_LOG.with(|l| assert_eq!(*l.borrow(), vec![2]));
    });
    FreeExecutorState(estate).unwrap();
}

#[test]
fn free_executor_state_runs_remaining_callbacks() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let a = CreateExprContext(estate).unwrap();
        let b = CreateExprContext(estate).unwrap();
        RegisterExprContextCallback(estate.ecxt_mut(a), log_cb, Datum::from_usize(10)).unwrap();
        RegisterExprContextCallback(estate.ecxt_mut(b), log_cb, Datum::from_usize(20)).unwrap();
    });
    CALLBACK_LOG.with(|l| l.borrow_mut().clear());
    FreeExecutorState(estate).unwrap();
    // newest context first (the C lcons order)
    CALLBACK_LOG.with(|l| assert_eq!(*l.borrow(), vec![20, 10]));
}

#[test]
fn standalone_expr_context() {
    let ctx = MemoryContext::new("caller");
    let mut ec = CreateStandaloneExprContext(ctx.mcx()).unwrap();
    assert!(ec.caseValue_isNull && ec.domainValue_isNull);
    RegisterExprContextCallback(&mut ec, log_cb, Datum::from_usize(7)).unwrap();
    CALLBACK_LOG.with(|l| l.borrow_mut().clear());
    FreeStandaloneExprContext(ec, true).unwrap();
    CALLBACK_LOG.with(|l| assert_eq!(*l.borrow(), vec![7]));
}

#[test]
fn target_list_lengths() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut tlist: PgVec<'_, TargetEntry<'_>> = PgVec::new_in(mcx);
    tlist.push(TargetEntry { expr: None, resjunk: false });
    tlist.push(TargetEntry { expr: None, resjunk: true });
    tlist.push(TargetEntry { expr: None, resjunk: false });
    assert_eq!(ExecTargetListLength(&tlist), 3);
    assert_eq!(ExecCleanTargetListLength(&tlist), 2);
    assert_eq!(ExecTargetListLength(&[]), 0);
}

fn tupdesc_with<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &[(types_core::primitive::Oid, i32)],
) -> TupleDescData<'mcx> {
    let mut attrs = PgVec::new_in(mcx);
    for (i, (typid, typmod)) in cols.iter().enumerate() {
        let mut att = types_tuple::heaptuple::FormData_pg_attribute::default();
        att.attnum = (i + 1) as i16;
        att.atttypid = *typid;
        att.atttypmod = *typmod;
        attrs.push(att);
    }
    TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs,
    }
}

fn var_tle<'mcx>(
    mcx: Mcx<'mcx>,
    varno: i32,
    attno: i16,
    typid: types_core::primitive::Oid,
    typmod: i32,
) -> TargetEntry<'mcx> {
    TargetEntry {
        expr: Some(
            alloc_in(
                mcx,
                Expr::Var(types_nodes::primnodes::Var {
                    varno,
                    varattno: attno,
                    vartype: typid,
                    vartypmod: typmod,
                    varlevelsup: 0,
                }),
            )
            .unwrap(),
        ),
        resjunk: false,
    }
}

#[test]
fn tlist_matching() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let tupdesc = tupdesc_with(mcx, &[(23, -1), (25, -1)]);

    // exact match
    let mut tlist = PgVec::new_in(mcx);
    tlist.push(var_tle(mcx, 1, 1, 23, -1));
    tlist.push(var_tle(mcx, 1, 2, 25, -1));
    assert!(tlist_matches_tupdesc(&tlist, 1, &tupdesc));

    // tlist too short
    assert!(!tlist_matches_tupdesc(&tlist[..1], 1, &tupdesc));

    // out of order
    let mut swapped = PgVec::new_in(mcx);
    swapped.push(var_tle(mcx, 1, 2, 25, -1));
    swapped.push(var_tle(mcx, 1, 1, 23, -1));
    assert!(!tlist_matches_tupdesc(&swapped, 1, &tupdesc));

    // type mismatch
    let mut wrong = PgVec::new_in(mcx);
    wrong.push(var_tle(mcx, 1, 1, 20, -1));
    wrong.push(var_tle(mcx, 1, 2, 25, -1));
    assert!(!tlist_matches_tupdesc(&wrong, 1, &tupdesc));

    // typmod -1 on the Var is accepted against a specific tupdesc typmod
    let typmod_desc = tupdesc_with(mcx, &[(1043, 36)]);
    let mut loose = PgVec::new_in(mcx);
    loose.push(var_tle(mcx, 1, 1, 1043, -1));
    assert!(tlist_matches_tupdesc(&loose, 1, &typmod_desc));

    // non-Var tlist item
    let mut non_var = PgVec::new_in(mcx);
    non_var.push(TargetEntry { expr: None, resjunk: false });
    non_var.push(var_tle(mcx, 1, 2, 25, -1));
    assert!(!tlist_matches_tupdesc(&non_var, 1, &tupdesc));

    // tlist too long
    let mut long = PgVec::new_in(mcx);
    long.push(var_tle(mcx, 1, 1, 23, -1));
    long.push(var_tle(mcx, 1, 2, 25, -1));
    long.push(var_tle(mcx, 1, 3, 25, -1));
    assert!(!tlist_matches_tupdesc(&long, 1, &tupdesc));

    // dropped column
    let mut dropped_desc = tupdesc_with(mcx, &[(23, -1)]);
    dropped_desc.attr_mut(0).attisdropped = true;
    let mut one = PgVec::new_in(mcx);
    one.push(var_tle(mcx, 1, 1, 23, -1));
    assert!(!tlist_matches_tupdesc(&one, 1, &dropped_desc));

    // missing-value column
    let mut missing_desc = tupdesc_with(mcx, &[(23, -1)]);
    missing_desc.attr_mut(0).atthasmissing = true;
    assert!(!tlist_matches_tupdesc(&one, 1, &missing_desc));
}

#[test]
fn result_slot_ops_fallbacks() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        // No resultops, no result slot: &TTSOpsVirtual, not fixed.
        let ps = PlanStateData::default();
        let mut isfixed = true;
        let ops = ExecGetResultSlotOps(&ps, estate, Some(&mut isfixed));
        assert_eq!(ops, TupleSlotKind::Virtual);
        assert!(!isfixed);

        // resultopsset + resultops: returned directly with resultopsfixed.
        let ps = PlanStateData {
            resultopsset: true,
            resultops: Some(TupleSlotKind::MinimalTuple),
            resultopsfixed: true,
            ..Default::default()
        };
        let mut isfixed = false;
        let ops = ExecGetResultSlotOps(&ps, estate, Some(&mut isfixed));
        assert_eq!(ops, TupleSlotKind::MinimalTuple);
        assert!(isfixed);

        // Fall back to the result slot's ops.
        let slot = estate
            .make_slot(types_nodes::TupleTableSlot {
                tts_ops: TupleSlotKind::HeapTuple,
                ..Default::default()
            })
            .unwrap();
        let ps = PlanStateData {
            ps_ResultTupleSlot: Some(slot),
            ..Default::default()
        };
        let ops = ExecGetResultSlotOps(&ps, estate, None);
        assert_eq!(ops, TupleSlotKind::HeapTuple);

        // Common slot ops: empty list -> None; mixed fixedness -> None.
        assert_eq!(ExecGetCommonSlotOps(&[], estate), None);
        let fixed = PlanStateData {
            resultopsset: true,
            resultops: Some(TupleSlotKind::Virtual),
            resultopsfixed: true,
            ..Default::default()
        };
        let unfixed = PlanStateData::default();
        assert_eq!(ExecGetCommonSlotOps(&[&fixed, &unfixed], estate), None);
        assert_eq!(
            ExecGetCommonSlotOps(&[&fixed, &fixed], estate),
            Some(TupleSlotKind::Virtual)
        );
    });
    FreeExecutorState(estate).unwrap();
}

#[test]
fn relation_is_target_relation() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let mut rels = PgVec::new_in(mcx);
        rels.push(2);
        rels.push(5);
        estate.es_plannedstmt = Some(
            alloc_in(
                mcx,
                types_nodes::nodeindexscan::PlannedStmt {
                    resultRelations: Some(rels),
                    relationOids: None,
                    planTree: None,
                    rowMarks: None,
                    canSetTag: false,
                },
            )
            .unwrap(),
        );
        assert!(ExecRelationIsTargetRelation(estate, 2));
        assert!(!ExecRelationIsTargetRelation(estate, 3));
    });
    FreeExecutorState(estate).unwrap();
}

#[test]
fn errposition_noop_paths() {
    let top = MemoryContext::new("test-top");
    let estate = CreateExecutorState(&top).unwrap();
    // Negative location: no-op even without source text or estate.
    assert_eq!(executor_errposition(None, -1).unwrap(), 0);
    estate.with(|s| assert_eq!(executor_errposition(Some(s), -1).unwrap(), 0));
    // Source text unavailable: no-op.
    estate.with(|s| assert_eq!(executor_errposition(Some(s), 5).unwrap(), 0));
    FreeExecutorState(estate).unwrap();
}

#[test]
fn init_range_table_sets_arrays() {
    let top = MemoryContext::new("test-top");
    let mut estate = CreateExecutorState(&top).unwrap();
    estate.with_mut(|estate| {
        let mcx = estate.es_query_cxt;
        let mut rt = PgVec::new_in(mcx);
        rt.push(RangeTblEntry {
            rtekind: RTE_RELATION,
            relid: 16384,
            relkind: b'r' as i8,
            rellockmode: AccessShareLock,
            perminfoindex: 1,
        });
        rt.push(RangeTblEntry::default());
        ExecInitRangeTable(estate, rt, PgVec::new_in(mcx), None).unwrap();
        assert_eq!(estate.es_range_table_size, 2);
        assert_eq!(estate.es_relations.len(), 2);
        assert!(estate.es_relations.iter().all(|r| r.is_none()));
        assert!(estate.es_result_relations.is_empty());
        assert_eq!(exec_rt_fetch(1, estate).relid, 16384);
    });
    FreeExecutorState(estate).unwrap();
}
