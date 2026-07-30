// execPartition.c, INSERT tuple-routing lane: ExecFindPartition over
// column- and expression-keyed LIST/RANGE/HASH trees. Attno-remapped
// children and runtime pruning are loud.
#![allow(non_snake_case)]

pub mod pruning;

use datum::Datum;
use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERROR};
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_rel::{Relation, RowExclusiveLock, RELKIND_PARTITIONED_TABLE};
use types_slot::SlotData;

use partbounds::{PartitionBoundInfoData, KIND_MAXVALUE, KIND_MINVALUE};
use partcache::PARTITION_MAX_KEYS;
use partdesc::PartitionDescData;

const PARTITION_CACHED_FIND_THRESHOLD: i32 = 16;

// PartitionDispatch: one partitioned table in the routing tree, its
// partsupfunc resolved once onto the dispatch (rule 4).
struct PartitionDispatch<'mcx> {
    rel: Relation<'mcx>,
    key: std::rc::Rc<partcache::PartitionKeyData>,
    partdesc: std::rc::Rc<PartitionDescData>,
    supfuncs: Vec<FmgrInfo>,
    // ExecPartitionCheck state for THIS rel when it is its parent's default
    // partition (sub-partitioned default), compiled once per query.
    default_check: Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>,
    keystate: Vec<PgBox<'mcx, execexpr::ExprState<'mcx>>>,
    // C pd->tupmap: immediate-parent layout -> this level's; the paired
    // conversion slot lives in `dispatch_slots` (split-borrowed vs the
    // dispatch during key extraction).
    tupmap: Option<mcx::PgVec<'mcx, i16>>,
}

pub struct PartitionTupleRouting<'mcx> {
    mcx: Mcx<'mcx>,
    dispatches: Vec<PartitionDispatch<'mcx>>,
    // C pd->tupslot, indexed like `dispatches`; Some iff tupmap is Some.
    dispatch_slots: Vec<Option<SlotData<'mcx>>>,
    // leaf oid -> index into `leaves` (linear scan: leaf counts are small on
    // this lane; C uses a hash table once >32).
    leaves: Vec<Relation<'mcx>>,
    // C ri_RootToPartitionMap per leaf: root layout -> leaf layout, None for
    // layout-identical leaves (callers own the conversion slots).
    leaf_maps: Vec<Option<mcx::PgVec<'mcx, i16>>>,
    // Default-partition recheck state per leaf (C ri_PartitionCheckExpr +
    // ri_PartitionTupleSlot), populated only for default-routed leaves.
    leaf_checks: Vec<Option<PgBox<'mcx, execexpr::ExprState<'mcx>>>>,
    leaf_check_slots: Vec<Option<SlotData<'mcx>>>,
    // ExecFindPartition's routing-root partition-constraint pre-check
    // (execPartition.c:286-291), compiled once (C ri_PartitionCheckExpr).
    root_check: Option<PgBox<'mcx, execexpr::ExprState<'mcx>>>,
}

impl<'mcx> PartitionTupleRouting<'mcx> {
    // ExecSetupPartitionTupleRouting: only the root dispatch up front.
    pub fn new(mcx: Mcx<'mcx>, root: &Relation<'mcx>) -> PgResult<Self> {
        let root_rc = root.alias();
        let mut prt = PartitionTupleRouting {
            mcx,
            dispatches: Vec::new(),
            dispatch_slots: Vec::new(),
            leaves: Vec::new(),
            leaf_maps: Vec::new(),
            leaf_checks: Vec::new(),
            leaf_check_slots: Vec::new(),
            root_check: None,
        };
        prt.init_dispatch(root_rc, None)?;
        Ok(prt)
    }

    // ExecInitPartitionDispatchInfo: tupmap/tupslot convert from the
    // immediate parent's layout, per C.
    fn init_dispatch(
        &mut self,
        rel: Relation<'mcx>,
        parent_idx: Option<usize>,
    ) -> PgResult<usize> {
        let key = partcache::RelationGetPartitionKey(&rel)?;
        // C ExecInitPartitionDispatchInfo's CreatePartitionDirectory: routing
        // omits detach-pending partitions except under snapshot isolation.
        let partdesc =
            partdesc::RelationGetPartitionDesc(&rel, !xact::IsolationUsesXactSnapshot())?;
        let mut supfuncs = Vec::with_capacity(key.partnatts as usize);
        for f in key.partsupfunc.iter() {
            let fn_oid = f.borrow().fn_oid;
            supfuncs.push(
                fmgr_core::fmgr_info(fn_oid)
                    .unwrap_or_else(|e| panic!("fmgr_info({fn_oid}) failed: {e:?}")),
            );
        }
        let tupmap = match parent_idx {
            Some(pi) => tupdesc::build_attrmap_by_name_if_req(
                self.mcx,
                &self.dispatches[pi].rel.rd_att,
                &rel.rd_att,
                false,
            )?,
            None => None,
        };
        self.dispatch_slots.push(tupmap.as_ref().map(|_| {
            exectuples::make_tuple_table_slot(
                self.mcx,
                types_slot::TupleSlotKind::Virtual,
                Some(rel.rd_att.clone()),
            )
        }));
        self.dispatches.push(PartitionDispatch {
            rel,
            key,
            partdesc,
            supfuncs,
            default_check: None,
            keystate: Vec::new(),
            tupmap,
        });
        Ok(self.dispatches.len() - 1)
    }

    // ExecInitPartitionInfo's ri_RootToPartitionMap + ri_PartitionTupleSlot.
    fn leaf_index(&mut self, oid: Oid) -> PgResult<usize> {
        if let Some(i) = self.leaves.iter().position(|r| r.rd_id == oid) {
            return Ok(i);
        }
        let rel = table::table_open(self.mcx, oid, RowExclusiveLock)?;
        let map = tupdesc::build_attrmap_by_name_if_req(
            self.mcx,
            &self.dispatches[0].rel.rd_att,
            &rel.rd_att,
            false,
        )?;
        self.leaf_maps.push(map);
        self.leaf_checks.push(None);
        self.leaf_check_slots.push(None);
        self.leaves.push(rel);
        Ok(self.leaves.len() - 1)
    }

    #[inline]
    pub fn leaf_rel(&self, idx: usize) -> &Relation<'mcx> {
        &self.leaves[idx]
    }

    // ri_RootToPartitionMap accessor; callers convert into their own
    // leaf-descriptor slots.
    #[inline]
    pub fn leaf_attrmap(&self, idx: usize) -> Option<&[i16]> {
        self.leaf_maps[idx].as_deref()
    }

    // ExecFindPartition -> index for leaf_rel(); eval_mcx is C's per-tuple
    // context (caller resets it per row).
    pub fn find_partition(
        &mut self,
        slot: &mut SlotData<'mcx>,
        eval_mcx: Mcx<'_>,
    ) -> PgResult<usize> {
        let mcx = self.mcx;
        // C ExecFindPartition's routing-root pre-check (execPartition.c:286-
        // 291): a tuple that does not belong in the root itself is rejected
        // before any dispatch.
        if self.dispatches[0].rel.rd_rel.relispartition {
            let PartitionTupleRouting { dispatches, root_check, .. } = &mut *self;
            let rel = &dispatches[0].rel;
            if !exec_partition_check(mcx, root_check, rel, slot)? {
                return Err(partition_constraint_violation(mcx, rel, slot, None, None));
            }
        }
        let mut values = [Datum::null(); PARTITION_MAX_KEYS];
        let mut isnull = [false; PARTITION_MAX_KEYS];
        let mut dispatch_idx = 0usize;
        // Index into dispatch_slots holding the tuple converted to the
        // current level's layout; None = the caller's root-format slot.
        let mut cur: Option<usize> = None;
        // The level just descended into is its parent's default partition;
        // its constraint is rechecked after the layout conversion below (C
        // ExecFindPartition's default_index arm, execPartition.c).
        let mut pending_default_check = false;
        loop {
            // C ExecFindPartition's per-level tupmap conversion.
            if self.dispatches[dispatch_idx].tupmap.is_some() {
                let PartitionTupleRouting { dispatches, dispatch_slots, .. } = &mut *self;
                let map = dispatches[dispatch_idx].tupmap.as_ref().expect("checked");
                match cur {
                    None => {
                        let out = dispatch_slots[dispatch_idx].as_mut().expect("tupslot");
                        exectuples::execute_attr_map_slot(map, slot, out, mcx);
                    }
                    Some(i) => {
                        assert_ne!(i, dispatch_idx);
                        let (in_slot, out) = if i < dispatch_idx {
                            let (a, b) = dispatch_slots.split_at_mut(dispatch_idx);
                            (a[i].as_mut(), b[0].as_mut())
                        } else {
                            let (a, b) = dispatch_slots.split_at_mut(i);
                            (b[0].as_mut(), a[dispatch_idx].as_mut())
                        };
                        exectuples::execute_attr_map_slot(
                            map,
                            in_slot.expect("converted"),
                            out.expect("tupslot"),
                            mcx,
                        );
                    }
                }
                cur = Some(dispatch_idx);
            }
            // Reassigned on every descent, so no reset here.
            if pending_default_check {
                let PartitionTupleRouting { dispatches, dispatch_slots, .. } = &mut *self;
                assert!(dispatch_idx > 0);
                let (head, tail) = dispatches.split_at_mut(dispatch_idx);
                let root_rel = &head[0].rel;
                let PartitionDispatch { rel, default_check, .. } = &mut tail[0];
                let cur_slot: &mut SlotData<'mcx> = match cur {
                    None => &mut *slot,
                    Some(i) => dispatch_slots[i].as_mut().expect("converted"),
                };
                // ExecPartitionCheck against the current (relcache-fresh)
                // constraint: a partition ATTACHed after this routing
                // snapshot narrows the default partition's constraint.
                if !exec_partition_check(mcx, default_check, rel, cur_slot)? {
                    return Err(partition_constraint_violation(
                        mcx,
                        rel,
                        cur_slot,
                        None,
                        Some(root_rel),
                    ));
                }
            }
            let (oid, is_leaf, is_default) = {
                let PartitionTupleRouting { dispatches, dispatch_slots, .. } = &mut *self;
                let pd = &mut dispatches[dispatch_idx];
                let cur_slot: &mut SlotData<'mcx> = match cur {
                    None => &mut *slot,
                    Some(i) => dispatch_slots[i].as_mut().expect("converted"),
                };
                let n = pd.key.partnatts as usize;
                // FormPartitionKeyDatum over the level-converted tuple.
                if !pd.key.partexprs.is_nil() && pd.keystate.is_empty() {
                    for expr in pd.key.partexprs.iter() {
                        let state =
                            execexpr::exec_init_expr(mcx, Some(expr), execexpr::ParamBind::NONE)?
                                .expect("partition key expression");
                        pd.keystate.push(state);
                    }
                }
                for state in pd.keystate.iter_mut() {
                    // SAFETY: eval_mcx outlives this call; by-ref results are
                    // consumed by routing before the caller resets it.
                    unsafe { state.arm_result_mcx_raw(eval_mcx) };
                }
                let mut keystate_item = pd.keystate.iter_mut();
                for i in 0..n {
                    let attno = pd.key.partattrs[i];
                    if attno != 0 {
                        values[i] =
                            exectuples::slot_getattr(cur_slot, attno as i32, &mut isnull[i]);
                    } else {
                        let state = keystate_item
                            .next()
                            .expect("wrong number of partition key expressions");
                        let mut slots =
                            execexpr::EvalSlots { scan: Some(cur_slot), inner: None, outer: None };
                        let r = execexpr::exec_eval_expr(state, &mut slots)?;
                        values[i] = r.value;
                        isnull[i] = r.isnull;
                    }
                }
                let Some(boundinfo) = pd.partdesc.boundinfo.as_ref() else {
                    return Err(no_partition_error(mcx, pd, &values, &isnull));
                };
                let part_index = get_partition_for_tuple(
                    eval_mcx,
                    &pd.key,
                    &mut pd.supfuncs,
                    &pd.partdesc,
                    boundinfo,
                    &values[..n],
                    &isnull[..n],
                )?;
                if part_index < 0 {
                    return Err(no_partition_error(mcx, pd, &values, &isnull));
                }
                (
                    pd.partdesc.oids[part_index as usize],
                    pd.partdesc.is_leaf[part_index as usize],
                    boundinfo.has_default() && part_index == boundinfo.default_index,
                )
            };
            if is_leaf {
                let idx = self.leaf_index(oid)?;
                if is_default {
                    // C converts from the ROOT slot via the root-to-child map
                    // (never from the intermediate level's layout).
                    let PartitionTupleRouting {
                        dispatches, leaves, leaf_maps, leaf_checks, leaf_check_slots, ..
                    } = &mut *self;
                    let root_rel = &dispatches[0].rel;
                    let target = &leaves[idx];
                    let check_slot: &mut SlotData<'mcx> = match leaf_maps[idx].as_deref() {
                        Some(map) => {
                            let s = leaf_check_slots[idx].get_or_insert_with(|| {
                                exectuples::make_tuple_table_slot(
                                    mcx,
                                    types_slot::TupleSlotKind::Virtual,
                                    Some(target.rd_att.clone()),
                                )
                            });
                            exectuples::execute_attr_map_slot(map, slot, s, mcx);
                            s
                        }
                        None => &mut *slot,
                    };
                    if !exec_partition_check(mcx, &mut leaf_checks[idx], target, check_slot)? {
                        return Err(partition_constraint_violation(
                            mcx,
                            target,
                            check_slot,
                            None,
                            Some(root_rel),
                        ));
                    }
                }
                return Ok(idx);
            }
            // Sub-partitioned child: descend (opened RowExclusiveLock as C).
            let parent_idx = dispatch_idx;
            if let Some(i) = self.dispatches.iter().position(|d| d.rel.rd_id == oid) {
                dispatch_idx = i;
            } else {
                let sub = table::table_open(self.mcx, oid, RowExclusiveLock)?;
                assert!(sub.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
                dispatch_idx = self.init_dispatch(sub, Some(parent_idx))?;
            }
            pending_default_check = is_default;
        }
    }
}

// ExecPartitionCheck (execMain.c), direct-DML leg: the compiled qual caches
// in the caller's per-result-rel state (C ri_PartitionCheckExpr); ExecCheck
// semantics, so a NULL result passes.
pub fn exec_partition_check<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &mut Option<PgBox<'mcx, execexpr::ExprState<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Recursion tripwire: constraint compile/eval must never route back here.
    #[cfg(debug_assertions)]
    let _depth = {
        use core::cell::Cell;
        thread_local! { static DEPTH: Cell<u32> = const { Cell::new(0) }; }
        struct G;
        impl Drop for G {
            fn drop(&mut self) {
                DEPTH.with(|d| d.set(d.get() - 1));
            }
        }
        DEPTH.with(|d| {
            let n = d.get();
            assert!(n < 8, "exec_partition_check recursion depth {n}");
            d.set(n + 1);
        });
        G
    };
    if cache.is_none() {
        let qual = partdesc::RelationGetPartitionQual(mcx, rel)?;
        let expr = partbounds::make_ands_explicit(mcx, qual)?;
        let planned = clauses_seams::eval_const_expressions::call(mcx, expr)?;
        let state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("partition constraint expr");
        *cache = Some(state);
    }
    let state = cache.as_mut().expect("just built");
    // C evaluates in the caller econtext's per-tuple memory; by-ref call
    // results ride the armed result mcx.
    state.arm_result_mcx(mcx);
    let mut slots = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
    let r = execexpr::exec_eval_expr(state, &mut slots)?;
    Ok(r.isnull || r.value.as_bool())
}

// ExecPartitionCheckEmitError (execMain.c).
#[cold]
#[inline(never)]
pub fn partition_constraint_violation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
    root_rel: Option<&Relation<'mcx>>,
) -> Box<PgError> {
    let table = rel.name().to_string();
    let mut e = PgError::new(
        ERROR,
        format!("new row for relation \"{table}\" violates partition constraint"),
    )
    .with_sqlstate(ERRCODE_CHECK_VIOLATION)
    .with_schema_name(
        lsyscache::misc::get_namespace_name(mcx, rel.rd_rel.relnamespace)
            .ok()
            .flatten()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
    )
    .with_table_name(table);
    // ExecPartitionCheckEmitError: a routed leaf's row is reported in the
    // root's rowtype; modified_cols is root-numbered then.
    let desc = match root_rel {
        Some(root) if root.rd_id != rel.rd_id => {
            match tupdesc::build_attrmap_by_name_if_req(mcx, &rel.rd_att, &root.rd_att, false) {
                Ok(map) => slot_value_description(mcx, root, slot, modified_cols, map.as_deref()),
                Err(e) => Err(e),
            }
        }
        _ => slot_value_description(mcx, rel, slot, modified_cols, None),
    };
    if let Ok(Some(desc)) = desc {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    Box::new(e)
}

// ExecBuildSlotValueDescription (execMain.c): without table-level SELECT,
// only columns the user provided (modified_cols, rel-numbered offset by
// FirstLowInvalidHeapAttributeNumber) or can read are shown, prefixed by
// their name list; None elides the DETAIL entirely.
// rev_map[rel_attno-1] = the slot's attno for that column (a routed leaf's
// row read through the root's tupdesc).
pub fn slot_value_description<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
    rev_map: Option<&[i16]>,
) -> PgResult<Option<String>> {
    const MAX_FIELD_LEN: usize = 64;
    const ACL_SELECT: u64 = 1 << 1;
    const ACLCHECK_OK: i32 = 0;
    // FirstLowInvalidHeapAttributeNumber (htup_details.h).
    const FLIHAN: i32 = -7;
    let relid = rel.rd_id;
    let mut table_perm = false;
    let mut any_perm = false;
    let mut userid = Oid::default();
    if rls_seams::check_enable_rls::call(relid, Oid::default(), true)?
        != rls_seams::CheckEnableRls::RlsEnabled
    {
        userid = miscinit_seams::get_user_id::call();
        if aclchk_seams::pg_class_aclcheck_ext::call(relid, userid, ACL_SELECT)?.0 == ACLCHECK_OK {
            table_perm = true;
            any_perm = true;
        }
    }
    exectuples::slot_getallattrs(slot);
    let mut buf = String::from("(");
    let mut collist = String::from("(");
    let mut write_comma = false;
    let mut write_comma_collist = false;
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attisdropped {
            continue;
        }
        if !table_perm {
            let aclok =
                aclchk_seams::pg_attribute_aclcheck::call(relid, att.attnum, userid, ACL_SELECT)?
                    == ACLCHECK_OK;
            let modified = modified_cols
                .is_some_and(|mc| mc.is_member(att.attnum as i32 - FLIHAN));
            if !aclok && !modified {
                continue;
            }
            any_perm = true;
            if write_comma_collist {
                collist.push_str(", ");
            }
            write_comma_collist = true;
            collist.push_str(core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8"));
        }
        if write_comma {
            buf.push_str(", ");
        }
        write_comma = true;
        // ATTRIBUTE_GENERATED_VIRTUAL columns print as "virtual" (execMain.c).
        if att.attgenerated == types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            buf.push_str("virtual");
            continue;
        }
        let i = match rev_map {
            Some(map) => map[i] as usize - 1,
            None => i,
        };
        let base = slot.base();
        if base.tts_isnull[i] {
            buf.push_str("null");
            continue;
        }
        let value = base.tts_values[i];
        let (foutoid, _) = lsyscache::typ::getTypeOutputInfo(att.atttypid)?;
        let mut finfo = fmgr_core::fmgr_info(foutoid)?;
        let out = fmgr_core::function_call1_coll_in(&mut finfo, 0, mcx, value)?;
        // SAFETY: output fns return a NUL-terminated cstring datum.
        let s = unsafe {
            core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char)
        }
        .to_bytes();
        let s = core::str::from_utf8(s).expect("type output is UTF-8");
        if s.len() <= MAX_FIELD_LEN {
            buf.push_str(s);
        } else {
            let mut end = MAX_FIELD_LEN;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            buf.push_str(&s[..end]);
            buf.push_str("...");
        }
    }
    if !any_perm {
        return Ok(None);
    }
    buf.push(')');
    if !table_perm {
        collist.push_str(") = ");
        collist.push_str(&buf);
        return Ok(Some(collist));
    }
    Ok(Some(buf))
}

// get_partition_for_tuple, LIST/RANGE arms with the last-found cache.
fn get_partition_for_tuple(
    eval_mcx: Mcx<'_>,
    key: &partcache::PartitionKeyData,
    supfuncs: &mut [FmgrInfo],
    partdesc: &PartitionDescData,
    boundinfo: &PartitionBoundInfoData<'static>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<i32> {
    let mut bound_offset: i32 = -1;
    let mut part_index: i32 = -1;
    match key.strategy as u8 {
        // Too cheap to cache; hash tables cannot have a DEFAULT partition.
        b'h' => {
            // Errors propagate (C: a user hash support proc may elog).
            let row_hash = partbounds::compute_partition_hash_value(
                eval_mcx,
                supfuncs,
                &key.partcollation,
                values,
                isnull,
            )?;
            return Ok(boundinfo.indexes[(row_hash % boundinfo.indexes.len() as u64) as usize]);
        }
        b'l' => {
            if isnull[0] {
                if boundinfo.accepts_nulls() {
                    return Ok(boundinfo.null_index);
                }
            } else {
                if partdesc.last_found_count.get() >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last = partdesc.last_found_datum_index.get();
                    let cmpval = sup_cmp(
                        supfuncs,
                        key,
                        0,
                        boundinfo.datum(last as usize, 0),
                        values[0],
                    );
                    if cmpval == 0 {
                        return Ok(boundinfo.indexes[last as usize]);
                    }
                }
                let mut equal = false;
                bound_offset = list_bsearch(supfuncs, key, boundinfo, values[0], &mut equal);
                if bound_offset >= 0 && equal {
                    part_index = boundinfo.indexes[bound_offset as usize];
                }
            }
        }
        b'r' => {
            let range_partkey_has_null = isnull.iter().any(|&n| n);
            if !range_partkey_has_null {
                if partdesc.last_found_count.get() >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last = partdesc.last_found_datum_index.get() as usize;
                    let w = boundinfo.width;
                    let cmpval = rbound_datum_cmp(
                        supfuncs,
                        key,
                        &boundinfo.datums[last * w..(last + 1) * w],
                        &boundinfo.kind[last * w..(last + 1) * w],
                        values,
                    );
                    if cmpval == 0 {
                        return Ok(boundinfo.indexes[last + 1]);
                    }
                    if cmpval < 0 && last + 1 < boundinfo.ndatums {
                        let m = last + 1;
                        let cmpval = rbound_datum_cmp(
                            supfuncs,
                            key,
                            &boundinfo.datums[m * w..(m + 1) * w],
                            &boundinfo.kind[m * w..(m + 1) * w],
                            values,
                        );
                        if cmpval > 0 {
                            return Ok(boundinfo.indexes[m]);
                        }
                    }
                }
                let mut equal = false;
                bound_offset =
                    range_datum_bsearch(supfuncs, key, boundinfo, values, &mut equal);
                part_index = boundinfo.indexes[(bound_offset + 1) as usize];
            }
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }

    if part_index < 0 {
        // No bound matched: the DEFAULT partition, if any (cache untouched).
        return Ok(boundinfo.default_index);
    }

    debug_assert!(bound_offset >= 0);
    if bound_offset == partdesc.last_found_datum_index.get() {
        partdesc.last_found_count.set(partdesc.last_found_count.get() + 1);
    } else {
        partdesc.last_found_count.set(1);
        partdesc.last_found_part_index.set(part_index);
        partdesc.last_found_datum_index.set(bound_offset);
    }
    Ok(part_index)
}

// FunctionCall2Coll over the dispatch-resolved supfunc (per-row path; the
// partcache RefCell copies stay off it).
#[inline]
fn sup_cmp(
    supfuncs: &mut [FmgrInfo],
    key: &partcache::PartitionKeyData,
    col: usize,
    a: Datum,
    b: Datum,
) -> i32 {
    // range_cmp (range-typed partition keys) detoasts through the result
    // mcx; arm the frame with call-lifetime scratch.
    let scratch = ::mcx::MemoryContext::new("partsupfunc cmp");
    let mut fcinfo = LocalFcinfo::<2>::new(key.partcollation[col]);
    // SAFETY: scratch outlives this call.
    unsafe { fcinfo.set_result_mcx(scratch.mcx()) };
    fcinfo.set_arg(0, a);
    fcinfo.set_arg(1, b);
    let r = supfuncs[col]
        .invoke(&mut fcinfo)
        .unwrap_or_else(|e| panic!("partition support function failed: {e:?}"));
    assert!(!fcinfo.isnull, "partition support function returned NULL");
    r.as_i32()
}

fn list_bsearch(
    supfuncs: &mut [FmgrInfo],
    key: &partcache::PartitionKeyData,
    boundinfo: &PartitionBoundInfoData<'_>,
    value: Datum,
    is_equal: &mut bool,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = sup_cmp(supfuncs, key, 0, boundinfo.datum(mid as usize, 0), value);
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

fn rbound_datum_cmp(
    supfuncs: &mut [FmgrInfo],
    key: &partcache::PartitionKeyData,
    rb_datums: &[Datum],
    rb_kind: &[i8],
    tuple_datums: &[Datum],
) -> i32 {
    let mut cmpval = -1;
    for i in 0..tuple_datums.len() {
        if rb_kind[i] == KIND_MINVALUE {
            return -1;
        } else if rb_kind[i] == KIND_MAXVALUE {
            return 1;
        }
        cmpval = sup_cmp(supfuncs, key, i, rb_datums[i], tuple_datums[i]);
        if cmpval != 0 {
            break;
        }
    }
    cmpval
}

fn range_datum_bsearch(
    supfuncs: &mut [FmgrInfo],
    key: &partcache::PartitionKeyData,
    boundinfo: &PartitionBoundInfoData<'_>,
    values: &[Datum],
    is_equal: &mut bool,
) -> i32 {
    let w = boundinfo.width;
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let m = mid as usize;
        let cmpval = rbound_datum_cmp(
            supfuncs,
            key,
            &boundinfo.datums[m * w..(m + 1) * w],
            &boundinfo.kind[m * w..(m + 1) * w],
            values,
        );
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// ExecBuildSlotPartitionKeyDescription + the "no partition found" report.
#[track_caller]
#[cold]
#[inline(never)]
fn no_partition_error(
    mcx: Mcx<'_>,
    pd: &PartitionDispatch<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> Box<PgError> {
    let n = pd.key.partnatts as usize;
    // The key description leaks data: elide it under RLS, and without SELECT
    // on the table or on every key column (expression keys always elide).
    const ACL_SELECT: u64 = 1 << 1;
    const ACLCHECK_OK: i32 = 0;
    let show_detail = (|| -> PgResult<bool> {
        let relid = pd.rel.rd_id;
        if rls_seams::check_enable_rls::call(relid, Oid::default(), true)?
            == rls_seams::CheckEnableRls::RlsEnabled
        {
            return Ok(false);
        }
        let userid = miscinit_seams::get_user_id::call();
        if aclchk_seams::pg_class_aclcheck_ext::call(relid, userid, ACL_SELECT)?.0 == ACLCHECK_OK {
            return Ok(true);
        }
        for i in 0..n {
            let attnum = pd.key.partattrs[i];
            if attnum == 0
                || aclchk_seams::pg_attribute_aclcheck::call(relid, attnum, userid, ACL_SELECT)?
                    != ACLCHECK_OK
            {
                return Ok(false);
            }
        }
        Ok(true)
    })()
    .unwrap_or(false);
    if !show_detail {
        return Box::new(
            PgError::new(
                ERROR,
                format!("no partition of relation \"{}\" found for row", pd.rel.name()),
            )
            .with_sqlstate(ERRCODE_CHECK_VIOLATION),
        );
    }
    let mut keydesc = String::from("(");
    // pg_get_partkeydef_columns handles expression keys (C truncates values
    // at maxfieldlen=64; standing residual here).
    let cols = ruleutils_seams::pg_get_partkeydef_columns::call(mcx, pd.rel.rd_id)
        .ok()
        .flatten()
        .unwrap_or_default();
    keydesc.push_str(&cols);
    keydesc.push_str(") = (");
    for i in 0..n {
        if i > 0 {
            keydesc.push_str(", ");
        }
        if isnull[i] {
            keydesc.push_str("null");
            continue;
        }
        let out = (|| -> PgResult<String> {
            let (foutoid, _) = lsyscache::typ::getTypeOutputInfo(pd.key.parttypid[i])?;
            let mut finfo = fmgr_core::fmgr_info(foutoid)?;
            let out = fmgr_core::function_call1_coll_in(&mut finfo, 0, mcx, values[i])?;
            // SAFETY: output fns return a NUL-terminated cstring datum.
            let s = unsafe {
                core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char)
            };
            Ok(core::str::from_utf8(s.to_bytes()).expect("type output is UTF-8").to_string())
        })()
        .unwrap_or_default();
        keydesc.push_str(&out);
    }
    keydesc.push(')');
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "no partition of relation \"{}\" found for row",
                pd.rel.name()
            ),
        )
        .with_detail(format!("Partition key of the failing row contains {keydesc}."))
        .with_sqlstate(ERRCODE_CHECK_VIOLATION),
    )
}
