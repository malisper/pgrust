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
    // ExecPartitionCheck state for default routing, compiled once per tree.
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
    // C ri_RootToPartitionMap + ri_PartitionTupleSlot per leaf; map is
    // root layout -> leaf layout, None for layout-identical leaves.
    leaf_maps: Vec<Option<mcx::PgVec<'mcx, i16>>>,
    leaf_slots: Vec<Option<SlotData<'mcx>>>,
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
            leaf_slots: Vec::new(),
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
        let partdesc = partdesc::RelationGetPartitionDesc(&rel, false)?;
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
        self.leaf_slots.push(map.as_ref().map(|_| {
            exectuples::make_tuple_table_slot(
                self.mcx,
                types_slot::TupleSlotKind::Virtual,
                Some(rel.rd_att.clone()),
            )
        }));
        self.leaf_maps.push(map);
        self.leaves.push(rel);
        Ok(self.leaves.len() - 1)
    }

    #[inline]
    pub fn leaf_rel(&self, idx: usize) -> &Relation<'mcx> {
        &self.leaves[idx]
    }

    // ExecPrepareTupleRouting's conversion leg: for an attno-remapped leaf,
    // converts the root-format tuple into the leaf's layout and returns the
    // leaf slot; None means the caller's slot already matches.
    pub fn leaf_rel_and_converted_slot(
        &mut self,
        idx: usize,
        in_slot: &mut SlotData<'mcx>,
    ) -> (&Relation<'mcx>, Option<&mut SlotData<'mcx>>) {
        let conv = match (&self.leaf_maps[idx], self.leaf_slots[idx].as_mut()) {
            (Some(map), Some(out)) => {
                exectuples::execute_attr_map_slot(map, in_slot, out, self.mcx);
                Some(out)
            }
            _ => None,
        };
        (&self.leaves[idx], conv)
    }

    // ri_RootToPartitionMap accessor (COPY's multi-insert buffers convert
    // into their own leaf-descriptor slots).
    #[inline]
    pub fn leaf_attrmap(&self, idx: usize) -> Option<&[i16]> {
        self.leaf_maps[idx].as_deref()
    }

    // Re-access after leaf_rel_and_converted_slot without re-converting.
    pub fn leaf_rel_and_slot(
        &mut self,
        idx: usize,
    ) -> (&Relation<'mcx>, Option<&mut SlotData<'mcx>>) {
        (&self.leaves[idx], self.leaf_slots[idx].as_mut())
    }

    // ExecFindPartition -> index for leaf_rel(); eval_mcx is C's per-tuple
    // context (caller resets it per row).
    pub fn find_partition(
        &mut self,
        slot: &mut SlotData<'mcx>,
        eval_mcx: Mcx<'_>,
    ) -> PgResult<usize> {
        let mcx = self.mcx;
        let mut values = [Datum::null(); PARTITION_MAX_KEYS];
        let mut isnull = [false; PARTITION_MAX_KEYS];
        let mut dispatch_idx = 0usize;
        // Index into dispatch_slots holding the tuple converted to the
        // current level's layout; None = the caller's root-format slot.
        let mut cur: Option<usize> = None;
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
                    &pd.key,
                    &mut pd.supfuncs,
                    &pd.partdesc,
                    boundinfo,
                    &values[..n],
                    &isnull[..n],
                );
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
                    let PartitionTupleRouting { dispatches, dispatch_slots, leaves, .. } = self;
                    let cur_slot: &mut SlotData<'mcx> = match cur {
                        None => &mut *slot,
                        Some(i) => dispatch_slots[i].as_mut().expect("converted"),
                    };
                    check_default_partition(
                        mcx,
                        &mut dispatches[dispatch_idx],
                        &leaves[idx],
                        cur_slot,
                    )?;
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
            if is_default {
                let sub_idx = dispatch_idx;
                assert_ne!(parent_idx, sub_idx);
                let PartitionTupleRouting { dispatches, dispatch_slots, .. } = &mut *self;
                let (parent_pd, sub_rel) = if parent_idx < sub_idx {
                    let (a, b) = dispatches.split_at_mut(sub_idx);
                    (&mut a[parent_idx], &b[0].rel)
                } else {
                    let (a, b) = dispatches.split_at_mut(parent_idx);
                    (&mut b[0], &a[sub_idx].rel)
                };
                let cur_slot: &mut SlotData<'mcx> = match cur {
                    None => &mut *slot,
                    Some(i) => dispatch_slots[i].as_mut().expect("converted"),
                };
                check_default_partition(mcx, parent_pd, sub_rel, cur_slot)?;
            }
        }
    }
}

// ExecFindPartition's default-partition re-check (ExecPartitionCheck).
// C divergence: generate_partition_qual's ancestor-qual concatenation is
// dropped — each routing level already checked its own bound on the descent.
fn check_default_partition<'mcx>(
    mcx: Mcx<'mcx>,
    pd: &mut PartitionDispatch<'mcx>,
    target: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    if pd.default_check.is_none() {
        let spec = types_nodes::rawnodes::PartitionBoundSpec {
            strategy: pd.key.strategy as u8,
            is_default: true,
            ..Default::default()
        };
        let qual = partbounds::get_qual_from_partbound(
            mcx,
            &pd.key,
            pd.rel.rd_id,
            pd.partdesc.boundinfo.as_ref(),
            &pd.partdesc.oids,
            &spec,
        )?;
        let expr = partbounds::make_ands_explicit(mcx, qual)?;
        let planned = clauses_seams::eval_const_expressions::call(mcx, expr)?;
        let state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("partition constraint expr");
        pd.default_check = Some(state);
    }
    let state = pd.default_check.as_mut().expect("just built");
    let mut slots = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
    let r = execexpr::exec_eval_expr(state, &mut slots)?;
    // ExecCheck: NULL passes.
    if !r.isnull && !r.value.as_bool() {
        return Err(partition_constraint_violation(mcx, target, slot));
    }
    Ok(())
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
    if cache.is_none() {
        let qual = partdesc::RelationGetPartitionQual(mcx, rel)?;
        let expr = partbounds::make_ands_explicit(mcx, qual)?;
        let planned = clauses_seams::eval_const_expressions::call(mcx, expr)?;
        let state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("partition constraint expr");
        *cache = Some(state);
    }
    let state = cache.as_mut().expect("just built");
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
    if let Ok(desc) = slot_value_description(mcx, rel, slot) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    Box::new(e)
}

// ExecBuildSlotValueDescription, table-SELECT-permission arm (single-
// superuser boot: column-ACL filtering and RLS are unreachable).
fn slot_value_description<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<String> {
    const MAX_FIELD_LEN: usize = 64;
    exectuples::slot_getallattrs(slot);
    let mut buf = String::from("(");
    let mut write_comma = false;
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attisdropped {
            continue;
        }
        if write_comma {
            buf.push_str(", ");
        }
        write_comma = true;
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
    buf.push(')');
    Ok(buf)
}

// get_partition_for_tuple, LIST/RANGE arms with the last-found cache.
fn get_partition_for_tuple(
    key: &partcache::PartitionKeyData,
    supfuncs: &mut [FmgrInfo],
    partdesc: &PartitionDescData,
    boundinfo: &PartitionBoundInfoData<'static>,
    values: &[Datum],
    isnull: &[bool],
) -> i32 {
    let mut bound_offset: i32 = -1;
    let mut part_index: i32 = -1;
    match key.strategy as u8 {
        // Too cheap to cache; hash tables cannot have a DEFAULT partition.
        b'h' => {
            let row_hash =
                partbounds::compute_partition_hash_value(supfuncs, &key.partcollation, values, isnull)
                    .unwrap_or_else(|e| panic!("partition hash support function failed: {e:?}"));
            return boundinfo.indexes[(row_hash % boundinfo.indexes.len() as u64) as usize];
        }
        b'l' => {
            if isnull[0] {
                if boundinfo.accepts_nulls() {
                    return boundinfo.null_index;
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
                        return boundinfo.indexes[last as usize];
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
                        return boundinfo.indexes[last + 1];
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
                            return boundinfo.indexes[m];
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
        return boundinfo.default_index;
    }

    debug_assert!(bound_offset >= 0);
    if bound_offset == partdesc.last_found_datum_index.get() {
        partdesc.last_found_count.set(partdesc.last_found_count.get() + 1);
    } else {
        partdesc.last_found_count.set(1);
        partdesc.last_found_part_index.set(part_index);
        partdesc.last_found_datum_index.set(bound_offset);
    }
    part_index
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
    let mut fcinfo = LocalFcinfo::<2>::new(key.partcollation[col]);
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
#[cold]
#[inline(never)]
fn no_partition_error(
    mcx: Mcx<'_>,
    pd: &PartitionDispatch<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> Box<PgError> {
    let n = pd.key.partnatts as usize;
    let mut keydesc = String::from("(");
    // pg_get_partkeydef_columns handles expression keys (C truncates values at
    // maxfieldlen=64 and elides the detail under RLS/ACL denial; both are
    // standing residuals here).
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
