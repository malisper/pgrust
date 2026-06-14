//! Per-tuple routing family: `ExecFindPartition`, `FormPartitionKeyDatum`,
//! `get_partition_for_tuple`, `ExecBuildSlotPartitionKeyDescription`.

use mcx::{Mcx, PgString};
use types_acl::{ACL_SELECT, ACLCHECK_OK, RLS_ENABLED};
use types_core::primitive::{InvalidOid, OidIsValid};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{PgResult, ERRCODE_CHECK_VIOLATION};
use types_nodes::nodes::CmdType;
use types_nodes::partition::PartitionStrategy;
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_rel::Relation;

use crate::{
    PartitionDispatchId, PartitionTupleRouting, PARTITION_CACHED_FIND_THRESHOLD,
};

/// `ExecFindPartition(mtstate, rootResultRelInfo, proute, slot, estate)` —
/// return the `ResultRelInfo` (id) of the leaf partition the tuple in `slot`
/// belongs to, building or reusing the partition's `ResultRelInfo` on first
/// use. Errors out (`ERRCODE_CHECK_VIOLATION`) when no leaf partition matches;
/// also fallible on the partition-info init it triggers.
pub fn ExecFindPartition<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    root_result_rel_info: RriId,
    proute: &mut PartitionTupleRouting<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<RriId> {
    // ExprContext *ecxt = GetPerTupleExprContext(estate);
    let ecxt = backend_executor_execUtils_seams::get_per_tuple_expr_context::call(estate)?;
    // TupleTableSlot *ecxt_scantuple_saved = ecxt->ecxt_scantuple;
    let ecxt_scantuple_saved = estate.ecxt(ecxt).ecxt_scantuple;
    // TupleTableSlot *rootslot = slot;
    let rootslot = slot;
    // TupleTableSlot *myslot = NULL;
    let mut myslot: Option<SlotId> = None;
    // ResultRelInfo *rri = NULL;
    let mut rri: Option<RriId> = None;

    // The C switches to GetPerTupleMemoryContext(estate) to avoid leaking; in
    // the owned model that short-term memory is the per-tuple ExprContext's
    // child context (reset by the caller per tuple). The per-tuple allocation
    // below uses `mcx` (the caller's per-tuple context).

    // First check the root table's partition constraint, if any.
    if root_result_rel_info_relispartition(estate, root_result_rel_info) {
        backend_executor_execMain_seams::exec_partition_check::call(
            estate,
            root_result_rel_info,
            slot,
            true,
        )?;
    }

    // The active slot for the current partitioning level (changes as we convert
    // tuples down through sub-partition layouts).
    let mut cur_slot = slot;

    // start with the root partitioned table
    let mut dispatch: Option<PartitionDispatchId> = Some(0);
    while let Some(disp) = dispatch {
        let mut partidx: i32 = -1;

        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // rel = dispatch->reldesc; partdesc = dispatch->partdesc;
        let rel = proute.partition_dispatch_info[disp]
            .reldesc
            .as_ref()
            .expect("PartitionDispatch has no reldesc")
            .alias();

        // ecxt->ecxt_scantuple = slot;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(cur_slot);

        // FormPartitionKeyDatum(dispatch, slot, estate, values, isnull);
        let mut values: [Datum; crate::PARTITION_MAX_KEYS] =
            core::array::from_fn(|_| Datum::null());
        let mut isnull = [false; crate::PARTITION_MAX_KEYS];
        FormPartitionKeyDatum(
            mcx,
            disp,
            cur_slot,
            estate,
            proute,
            &mut values,
            &mut isnull,
        )?;

        // If this partitioned table has no partitions or no partition for these
        // values, error out.
        let nparts = proute.partition_dispatch_info[disp]
            .partdesc
            .as_ref()
            .expect("PartitionDispatch has no partdesc")
            .nparts;
        let found = if nparts != 0 {
            partidx = get_partition_for_tuple(
                &mut proute.partition_dispatch_info[disp],
                &values,
                &isnull,
            )?;
            partidx >= 0
        } else {
            false
        };
        if !found {
            let val_desc =
                ExecBuildSlotPartitionKeyDescription(mcx, rel.alias(), &values, &isnull, 64)?;
            // Assert(OidIsValid(RelationGetRelid(rel)));
            debug_assert!(OidIsValid(rel.rd_id));
            let mut err = types_error::PgError::error(format!(
                "no partition of relation \"{}\" found for row",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_CHECK_VIOLATION)
            .with_table_name(rel.name().to_string());
            if let Some(val_desc) = val_desc {
                err = err.with_detail(format!(
                    "Partition key of the failing row contains {}.",
                    val_desc.as_str()
                ));
            }
            return Err(err);
        }

        // is_leaf = partdesc->is_leaf[partidx];
        let is_leaf = proute.partition_dispatch_info[disp]
            .partdesc
            .as_ref()
            .unwrap()
            .is_leaf[partidx as usize];

        if is_leaf {
            // We've reached the leaf -- look for an existing ResultRelInfo.
            let idx = proute.partition_dispatch_info[disp].indexes[partidx as usize];
            if idx >= 0 {
                // ResultRelInfo already built.
                debug_assert!(idx < proute.num_partitions);
                rri = Some(proute.partitions[idx as usize]);
            } else {
                // Re-use a ResultRelInfo known in the ModifyTableState, if any.
                let part_oid = proute.partition_dispatch_info[disp]
                    .partdesc
                    .as_ref()
                    .unwrap()
                    .oids[partidx as usize];
                let found_rri = backend_executor_execMain_seams::exec_lookup_result_rel_by_oid::call(
                    mtstate, part_oid, true, false,
                )?;
                if let Some(found_rri) = found_rri {
                    // Verify this ResultRelInfo allows INSERTs.
                    let on_conflict_action =
                        backend_executor_nodeModifyTable_seams::exec_get_on_conflict_action::call(
                            mtstate,
                        );
                    backend_executor_execMain_seams::check_valid_result_rel::call(
                        estate,
                        found_rri,
                        CmdType::CMD_INSERT,
                        on_conflict_action,
                    )?;

                    // Initialize info to insert tuples routed to this partition.
                    crate::routing_setup::ExecInitRoutingInfo(
                        mcx, mtstate, estate, proute, disp, found_rri, partidx, true,
                    )?;
                    rri = Some(found_rri);
                } else {
                    // We need to create a new one.
                    rri = Some(crate::routing_init_info::ExecInitPartitionInfo(
                        mcx,
                        mtstate,
                        estate,
                        proute,
                        disp,
                        root_result_rel_info,
                        partidx,
                    )?);
                }
            }
            // Assert(rri != NULL);
            debug_assert!(rri.is_some());

            // Signal to terminate the loop.
            dispatch = None;
        } else {
            // Partition is a sub-partitioned table; get the PartitionDispatch.
            let idx = proute.partition_dispatch_info[disp].indexes[partidx as usize];
            let next_dispatch: PartitionDispatchId;
            if idx >= 0 {
                // Already built.
                debug_assert!(idx < proute.num_dispatch);
                rri = proute.nonleaf_partitions[idx as usize];
                // Move down to the next partition level and search again.
                next_dispatch = idx as usize;
            } else {
                // Not yet built. Do that now.
                let part_oid = proute.partition_dispatch_info[disp]
                    .partdesc
                    .as_ref()
                    .unwrap()
                    .oids[partidx as usize];
                let subdispatch = crate::routing_setup::ExecInitPartitionDispatchInfo(
                    mcx,
                    estate,
                    proute,
                    part_oid,
                    Some(disp),
                    partidx,
                    mtstate.rootResultRelInfo,
                )?;
                let new_idx = proute.partition_dispatch_info[disp].indexes[partidx as usize];
                debug_assert!(new_idx >= 0 && new_idx < proute.num_dispatch);
                rri = proute.nonleaf_partitions[new_idx as usize];
                next_dispatch = subdispatch;
            }

            // Convert the tuple to the new parent's layout, if different from
            // the previous parent.
            if proute.partition_dispatch_info[next_dispatch].tupslot.is_some() {
                // myslot = dispatch->tupslot; — the standalone slot lives in the
                // dispatch; here it is addressed via the dispatch. The owned
                // slot payload model is not yet wired, so the conversion goes
                // through the tupconvert seam against the dispatch's own slot.
                let tempslot = myslot;
                let new_my = dispatch_tupslot_id(estate, proute, next_dispatch)?;
                myslot = Some(new_my);
                let map = proute.partition_dispatch_info[next_dispatch]
                    .tupmap
                    .as_ref()
                    .expect("sub-partition tupslot present without tupmap");
                // Take an owned copy of the map's attnums so the estate can be
                // re-borrowed mutably by execute_attr_map_slot.
                let map_copy = clone_attrmap(mcx, map)?;
                cur_slot = backend_access_common_tupconvert_seams::execute_attr_map_slot::call(
                    estate, &map_copy, cur_slot, new_my,
                )?;
                if let Some(tempslot) = tempslot {
                    backend_executor_execTuples_seams::exec_clear_tuple::call(
                        estate.slot_mut(tempslot),
                    )?;
                }
            }

            dispatch = Some(next_dispatch);
        }

        // If this partition is the default one, we must check its partition
        // constraint now (it may have changed concurrently).
        let default_index = proute.partition_dispatch_info[disp]
            .partdesc
            .as_ref()
            .unwrap()
            .boundinfo
            .as_ref()
            .expect("partdesc has no boundinfo")
            .default_index;
        if partidx == default_index {
            let rri_id = rri.expect("default partition check without a ResultRelInfo");
            if is_leaf {
                // Convert from root layout if a map exists, else use rootslot.
                let map = backend_executor_execUtils_seams::exec_get_root_to_child_map::call(
                    mcx, estate, rri_id,
                )?;
                if let Some(map) = map {
                    let part_slot = estate
                        .result_rel(rri_id)
                        .ri_PartitionTupleSlot
                        .expect("leaf default partition has no ri_PartitionTupleSlot");
                    cur_slot =
                        backend_access_common_tupconvert_seams::execute_attr_map_slot::call(
                            estate, &map, rootslot, part_slot,
                        )?;
                } else {
                    cur_slot = rootslot;
                }
            }
            backend_executor_execMain_seams::exec_partition_check::call(
                estate, rri_id, cur_slot, true,
            )?;
        }
    }

    // Release the tuple in the lowest parent's dedicated slot.
    if let Some(myslot) = myslot {
        backend_executor_execTuples_seams::exec_clear_tuple::call(estate.slot_mut(myslot))?;
    }
    // Restore ecxt's scantuple.
    estate.ecxt_mut(ecxt).ecxt_scantuple = ecxt_scantuple_saved;

    Ok(rri.expect("ExecFindPartition produced no ResultRelInfo"))
}

/// Copy a partition-key value for a seam ABI edge.
///
/// The partition-bound comparison / hashing seams (`partition_*_datum_cmp`,
/// `compute_partition_hash_value`, `partition_*_bsearch`) and the
/// `PartitionBoundInfoData.datums` store all now trade in the canonical
/// `Datum<'mcx>`. C threads the raw `Datum` machine word straight through these
/// boundaries; the canonical carrier forwards the same value (by-value scalar
/// or detoasted by-reference image) verbatim.
#[inline]
fn key_word<'mcx>(d: &Datum<'mcx>) -> Datum<'mcx> {
    d.clone()
}

/// `rootResultRelInfo->ri_RelationDesc->rd_rel->relispartition`.
fn root_result_rel_info_relispartition(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|rel| rel.rd_rel.relispartition)
        .unwrap_or(false)
}

/// `dispatch->tupslot` — resolve the dispatch's standalone conversion slot to a
/// slot id usable by the id-addressed conversion/clear seams. The dispatch owns
/// the slot inline (`tupslot: Option<TupleTableSlot>`); the seams address slots
/// by id into the EState pool, so the inline slot is registered into the pool
/// (`ExecAllocTableSlot`-shaped `make_slot`) and its id returned. The slot is a
/// virtual slot fully overwritten by each `execute_attr_map_slot` and reset by
/// `ExecClearTuple`, so a fresh pool entry per use carries no stale payload.
fn dispatch_tupslot_id<'mcx>(
    estate: &mut EStateData<'mcx>,
    proute: &PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
) -> PgResult<SlotId> {
    let slot = proute.partition_dispatch_info[dispatch]
        .tupslot
        .clone()
        .expect("dispatch_tupslot_id called without a tupslot");
    estate.make_slot(slot)
}

/// Copy an `AttrMap` into `mcx` so the estate can be re-borrowed mutably.
fn clone_attrmap<'mcx>(
    mcx: Mcx<'mcx>,
    map: &types_tuple::attmap::AttrMap<'_>,
) -> PgResult<mcx::PgBox<'mcx, types_tuple::attmap::AttrMap<'mcx>>> {
    let mut attnums = mcx::PgVec::new_in(mcx);
    attnums
        .try_reserve(map.attnums.len())
        .map_err(|_| mcx.oom(map.attnums.len() * core::mem::size_of::<types_core::AttrNumber>()))?;
    for n in map.attnums.iter() {
        attnums.push(*n);
    }
    mcx::PgBox::try_new_in(types_tuple::attmap::AttrMap { attnums }, mcx)
        .map_err(|_| mcx.oom(core::mem::size_of::<types_tuple::attmap::AttrMap<'_>>()))
}

/// `FormPartitionKeyDatum(pd, slot, estate, values, isnull)` — fill the
/// `values[]`/`isnull[]` arrays with the partition key of the tuple in `slot`,
/// compiling the key's expression state on first use. The `ecxt_scantuple` of
/// `estate`'s per-tuple expr context must already point at `slot`. Fallible
/// (expression compile/eval, OOM); `elog(ERROR)` on a key-expression count
/// mismatch.
pub(crate) fn FormPartitionKeyDatum<'mcx>(
    mcx: Mcx<'mcx>,
    dispatch: PartitionDispatchId,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    values: &mut [Datum<'mcx>],
    isnull: &mut [bool],
) -> PgResult<()> {
    let _ = (mcx, slot);

    // if (pd->key->partexprs != NIL && pd->keystate == NIL)
    let has_partexprs = {
        let pd = &proute.partition_dispatch_info[dispatch];
        let key = pd.key.as_ref().expect("PartitionDispatch has no key");
        !key.partexprs.is_empty()
    };
    if has_partexprs && proute.partition_dispatch_info[dispatch].keystate.is_empty() {
        // First time through, set up expression evaluation state.
        // pd->keystate = ExecPrepareExprList(pd->key->partexprs, estate);
        let partexprs: &[types_nodes::primnodes::Expr] = {
            let pd = &proute.partition_dispatch_info[dispatch];
            pd.key.as_ref().unwrap().partexprs.as_slice()
        };
        let keystate =
            backend_executor_execExpr_seams::exec_prepare_expr_list::call(partexprs, estate)?;
        proute.partition_dispatch_info[dispatch].keystate = keystate;
    }

    // ExprContext for expression evaluation.
    let ecxt = backend_executor_execUtils_seams::get_per_tuple_expr_context::call(estate)?;

    // partexpr_item = list_head(pd->keystate);
    let mut partexpr_item: usize = 0;
    let partnatts = proute.partition_dispatch_info[dispatch]
        .key
        .as_ref()
        .unwrap()
        .partnatts as usize;

    for i in 0..partnatts {
        let keycol = proute.partition_dispatch_info[dispatch]
            .key
            .as_ref()
            .unwrap()
            .partattrs[i];
        let datum: Datum;
        let is_null: bool;

        if keycol != 0 {
            // Plain column; get the value directly from the heap tuple.
            let (d, n) = backend_executor_execTuples_seams::slot_getattr::call(
                mcx,
                estate.slot_mut(slot),
                keycol,
            )?;
            // slot_getattr now yields the canonical Datum directly.
            datum = d;
            is_null = n;
        } else {
            // Expression; need to evaluate it.
            // if (partexpr_item == NULL) elog(ERROR, ...)
            let keystate_len = proute.partition_dispatch_info[dispatch].keystate.len();
            if partexpr_item >= keystate_len {
                return Err(types_error::PgError::error(
                    "wrong number of partition key expressions",
                ));
            }
            let (d, n) = {
                let exprstate =
                    &mut proute.partition_dispatch_info[dispatch].keystate[partexpr_item];
                // Borrow ends before the &mut estate call. The ExprState is owned
                // by the dispatch, which is not the estate, so the borrows do not
                // alias.
                backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                    exprstate, ecxt, estate,
                )?
            };
            // ExecEvalExprSwitchContext now returns the canonical Datum directly.
            datum = d;
            is_null = n;
            // partexpr_item = lnext(pd->keystate, partexpr_item);
            partexpr_item += 1;
        }
        values[i] = datum;
        isnull[i] = is_null;
    }

    // if (partexpr_item != NULL) elog(ERROR, ...)
    let keystate_len = proute.partition_dispatch_info[dispatch].keystate.len();
    if partexpr_item < keystate_len {
        return Err(types_error::PgError::error(
            "wrong number of partition key expressions",
        ));
    }

    Ok(())
}

/// `get_partition_for_tuple(pd, values, isnull)` — find the partition (index in
/// `0..partdesc->nparts`) accepting the given partition-key values, or -1 if
/// none. Verified MATCH against the C control flow (HASH/LIST/RANGE strategies
/// with the last-found caching path). Fallible: the support/comparison
/// functions and bound searches can `ereport(ERROR)`.
pub(crate) fn get_partition_for_tuple<'mcx>(
    dispatch: &mut crate::PartitionDispatchData<'mcx>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<i32> {
    let mut bound_offset: i32 = -1;
    let mut part_index: i32 = -1;

    // PartitionKey key = pd->key; PartitionDesc partdesc = pd->partdesc;
    let key = dispatch.key.as_ref().expect("dispatch has no key");
    let strategy = key.strategy;
    let partnatts = key.partnatts as usize;

    // Route as appropriate based on partitioning strategy.
    match strategy {
        PartitionStrategy::Hash => {
            // hash partitioning is too cheap to bother caching
            let row_hash =
                backend_partitioning_partbounds_seams::compute_partition_hash_value::call(
                    key, values, isnull,
                )?;
            let boundinfo = dispatch
                .partdesc
                .as_ref()
                .unwrap()
                .boundinfo
                .as_ref()
                .expect("partdesc has no boundinfo");
            // boundinfo->indexes[rowHash % boundinfo->nindexes]
            let nindexes = boundinfo.nindexes as u64;
            return Ok(boundinfo.indexes[(row_hash % nindexes) as usize]);
        }

        PartitionStrategy::List => {
            if isnull[0] {
                // far too cheap to bother caching
                let boundinfo = dispatch
                    .partdesc
                    .as_ref()
                    .unwrap()
                    .boundinfo
                    .as_ref()
                    .expect("partdesc has no boundinfo");
                // partition_bound_accepts_nulls(boundinfo) == (null_index != -1)
                if boundinfo.null_index != -1 {
                    return Ok(boundinfo.null_index);
                }
            } else {
                // Cached-find fast path.
                let last_found_count = dispatch.partdesc.as_ref().unwrap().last_found_count;
                if last_found_count >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last_datum_offset =
                        dispatch.partdesc.as_ref().unwrap().last_found_datum_index;
                    let last_datum = key_word(
                        &dispatch
                            .partdesc
                            .as_ref()
                            .unwrap()
                            .boundinfo
                            .as_ref()
                            .unwrap()
                            .datums[last_datum_offset as usize][0],
                    );
                    let cmpval =
                        backend_partitioning_partbounds_seams::partition_list_datum_cmp::call(
                            key, last_datum, key_word(&values[0]),
                        )?;
                    if cmpval == 0 {
                        return Ok(dispatch.partdesc.as_ref().unwrap().boundinfo.as_ref().unwrap()
                            .indexes[last_datum_offset as usize]);
                    }
                    // fall-through and do a manual lookup
                }

                let boundinfo = dispatch.partdesc.as_ref().unwrap().boundinfo.as_ref().unwrap();
                let (off, equal) =
                    backend_partitioning_partbounds_seams::partition_list_bsearch::call(
                        key,
                        boundinfo,
                        key_word(&values[0]),
                    )?;
                bound_offset = off;
                if bound_offset >= 0 && equal {
                    part_index = boundinfo.indexes[bound_offset as usize];
                }
            }
        }

        PartitionStrategy::Range => {
            let mut range_partkey_has_null = false;
            // No range includes NULL.
            for i in 0..partnatts {
                if isnull[i] {
                    range_partkey_has_null = true;
                    break;
                }
            }

            // NULLs belong in the DEFAULT partition.
            if !range_partkey_has_null {
                let last_found_count = dispatch.partdesc.as_ref().unwrap().last_found_count;
                if last_found_count >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last_datum_offset =
                        dispatch.partdesc.as_ref().unwrap().last_found_datum_index;
                    let ndatums = dispatch.partdesc.as_ref().unwrap().boundinfo.as_ref().unwrap()
                        .ndatums;

                    // check if the value is >= to the lower bound
                    let (last_datums, kind) =
                        range_bound_at(dispatch, last_datum_offset as usize);
                    let cmpval =
                        backend_partitioning_partbounds_seams::partition_rbound_datum_cmp::call(
                            key.partcollation.as_slice(),
                            &last_datums,
                            &kind,
                            values,
                            partnatts as i32,
                        )?;

                    // If it's equal to the lower bound then no need to check the
                    // upper bound.
                    if cmpval == 0 {
                        return Ok(dispatch.partdesc.as_ref().unwrap().boundinfo.as_ref().unwrap()
                            .indexes[(last_datum_offset + 1) as usize]);
                    }

                    if cmpval < 0 && last_datum_offset + 1 < ndatums {
                        // check if the value is below the upper bound
                        let (up_datums, up_kind) =
                            range_bound_at(dispatch, (last_datum_offset + 1) as usize);
                        let cmpval2 =
                            backend_partitioning_partbounds_seams::partition_rbound_datum_cmp::call(
                                key.partcollation.as_slice(),
                                &up_datums,
                                &up_kind,
                                values,
                                partnatts as i32,
                            )?;
                        if cmpval2 > 0 {
                            return Ok(dispatch
                                .partdesc
                                .as_ref()
                                .unwrap()
                                .boundinfo
                                .as_ref()
                                .unwrap()
                                .indexes[(last_datum_offset + 1) as usize]);
                        }
                    }
                    // fall-through and do a manual lookup
                }

                let boundinfo = dispatch.partdesc.as_ref().unwrap().boundinfo.as_ref().unwrap();
                let (off, _equal) =
                    backend_partitioning_partbounds_seams::partition_range_datum_bsearch::call(
                        key,
                        boundinfo,
                        partnatts as i32,
                        values,
                    )?;
                bound_offset = off;
                // The bound at bound_offset is <= the tuple value, so the bound
                // at offset+1 is the upper bound of the partition we want.
                part_index = boundinfo.indexes[(bound_offset + 1) as usize];
            }
        }
    }

    // part_index < 0 means we failed to find a partition of this parent. Use
    // the default partition, if there is one.
    if part_index < 0 {
        // No need to reset the cache fields here.
        return Ok(dispatch
            .partdesc
            .as_ref()
            .unwrap()
            .boundinfo
            .as_ref()
            .unwrap()
            .default_index);
    }

    // we should only make it here when the code above set bound_offset
    debug_assert!(bound_offset >= 0);

    // Attend to the cache fields.
    let partdesc = dispatch.partdesc.as_mut().unwrap();
    if bound_offset == partdesc.last_found_datum_index {
        partdesc.last_found_count += 1;
    } else {
        partdesc.last_found_count = 1;
        partdesc.last_found_part_index = part_index;
        partdesc.last_found_datum_index = bound_offset;
    }

    Ok(part_index)
}

/// `boundinfo->datums[off]` / `boundinfo->kind[off]` for a RANGE bound — copy
/// the per-bound datum and kind rows out so the partbounds seam can borrow them
/// independently of the dispatch.
fn range_bound_at<'mcx>(
    dispatch: &crate::PartitionDispatchData<'mcx>,
    off: usize,
) -> (
    alloc_vec::Vec<Datum<'mcx>>,
    alloc_vec::Vec<types_nodes::partition::PartitionRangeDatumKind>,
) {
    let boundinfo = dispatch
        .partdesc
        .as_ref()
        .unwrap()
        .boundinfo
        .as_ref()
        .unwrap();
    let datums: alloc_vec::Vec<Datum<'mcx>> =
        boundinfo.datums[off].iter().map(key_word).collect();
    let kind: alloc_vec::Vec<types_nodes::partition::PartitionRangeDatumKind> = boundinfo
        .kind
        .as_ref()
        .expect("RANGE boundinfo has no kind array")[off]
        .iter()
        .copied()
        .collect();
    (datums, kind)
}

mod alloc_vec {
    extern crate alloc;
    pub use alloc::vec::Vec;
}

/// `ExecBuildSlotPartitionKeyDescription(rel, values, isnull, maxfieldlen)` —
/// build a `"(col, ...) = (val, ...)"` description of the failing partition key
/// for the "no partition found" error message, limited to columns the current
/// user has SELECT rights on. `Ok(None)` when RLS is enabled or permissions
/// allow no column (the C `NULL`). Allocated in `mcx`; out-functions can
/// `ereport(ERROR)`.
pub(crate) fn ExecBuildSlotPartitionKeyDescription<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    maxfieldlen: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // PartitionKey key = RelationGetPartitionKey(rel);
    let key = backend_utils_cache_partcache_seams::relation_get_partition_key::call(mcx, rel.alias())?
        .expect("ExecBuildSlotPartitionKeyDescription on a non-partitioned relation");
    // int partnatts = get_partition_natts(key);
    let partnatts = key.partnatts as usize;
    // Oid relid = RelationGetRelid(rel);
    let relid = rel.rd_id;

    // if (check_enable_rls(relid, InvalidOid, true) == RLS_ENABLED) return NULL;
    if backend_utils_misc_more_seams::check_enable_rls::call(relid, InvalidOid, true)? == RLS_ENABLED
    {
        return Ok(None);
    }

    // If the user has table-level access, just go build the description.
    let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
    let aclresult =
        backend_catalog_aclchk_seams::pg_class_aclcheck::call(relid, user_id, ACL_SELECT)?;
    if aclresult != ACLCHECK_OK {
        // Make sure the user has SELECT rights on every partition-key column.
        for i in 0..partnatts {
            // AttrNumber attnum = get_partition_col_attnum(key, i);
            let attnum = key.partattrs[i];
            // Expression column, or no SELECT right → no detail.
            if attnum == 0
                || backend_catalog_aclchk_seams::pg_attribute_aclcheck::call(
                    relid, attnum, user_id, ACL_SELECT,
                )? != ACLCHECK_OK
            {
                return Ok(None);
            }
        }
    }

    let mut buf = PgString::new_in(mcx);
    // appendStringInfo(&buf, "(%s) = (", pg_get_partkeydef_columns(relid, true));
    let cols =
        backend_utils_adt_ruleutils_seams::pg_get_partkeydef_columns::call(mcx, relid, true)?;
    buf.try_push_str("(")?;
    buf.try_push_str(cols.as_str())?;
    buf.try_push_str(") = (")?;

    for i in 0..partnatts {
        // char *val;
        let val_bytes: alloc_vec::Vec<u8>;
        let val: &[u8];
        let null_lit = b"null";
        if isnull[i] {
            val = null_lit;
        } else {
            // getTypeOutputInfo(get_partition_col_typid(key, i), &foutoid, &typisvarlena);
            let (foutoid, _typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(key.parttypid[i])?;
            // val = OidOutputFunctionCall(foutoid, values[i]);
            let out =
                backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(
                    mcx, foutoid, values[i].clone(),
                )?;
            val_bytes = out.as_bytes().to_vec();
            val = &val_bytes;
        }

        if i > 0 {
            buf.try_push_str(", ")?;
        }

        // truncate if needed
        let vallen = val.len() as i32;
        if vallen <= maxfieldlen {
            push_bytes(&mut buf, val)?;
        } else {
            let clipped = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(val, vallen, maxfieldlen);
            push_bytes(&mut buf, &val[..clipped as usize])?;
            buf.try_push_str("...")?;
        }
    }

    buf.try_push_str(")")?;

    Ok(Some(buf))
}

/// Append raw output-function bytes (database encoding) to the description
/// buffer. The C `StringInfo` stores raw bytes; the owned `PgString` holds
/// UTF-8, so non-UTF-8 database-encoding bytes are passed through lossily here
/// — matching the diagnostic-only role of this string.
fn push_bytes<'mcx>(buf: &mut PgString<'mcx>, bytes: &[u8]) -> PgResult<()> {
    match core::str::from_utf8(bytes) {
        Ok(s) => buf.try_push_str(s),
        Err(_) => {
            let s = alloc_string::String::from_utf8_lossy(bytes);
            buf.try_push_str(&s)
        }
    }
}

mod alloc_string {
    extern crate alloc;
    pub use alloc::string::String;
}
