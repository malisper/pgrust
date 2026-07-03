// execPartition.c, INSERT tuple-routing lane: ExecFindPartition over
// column-keyed LIST/RANGE trees. Expression keys, HASH, DEFAULT partitions,
// attno-remapped children and runtime pruning are loud.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
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
}

pub struct PartitionTupleRouting<'mcx> {
    mcx: Mcx<'mcx>,
    dispatches: Vec<PartitionDispatch<'mcx>>,
    // leaf oid -> index into `leaves` (linear scan: leaf counts are small on
    // this lane; C uses a hash table once >32).
    leaves: Vec<Relation<'mcx>>,
}

impl<'mcx> PartitionTupleRouting<'mcx> {
    // ExecSetupPartitionTupleRouting: only the root dispatch up front.
    pub fn new(mcx: Mcx<'mcx>, root: &Relation<'mcx>) -> PgResult<Self> {
        let root_rc = root.alias();
        let mut prt = PartitionTupleRouting { mcx, dispatches: Vec::new(), leaves: Vec::new() };
        prt.init_dispatch(root_rc)?;
        Ok(prt)
    }

    fn init_dispatch(&mut self, rel: Relation<'mcx>) -> PgResult<usize> {
        let key = partcache::RelationGetPartitionKey(&rel)?;
        let partdesc = partdesc::RelationGetPartitionDesc(&rel)?;
        let mut supfuncs = Vec::with_capacity(key.partnatts as usize);
        for f in key.partsupfunc.iter() {
            let fn_oid = f.borrow().fn_oid;
            supfuncs.push(
                fmgr_core::fmgr_info(fn_oid)
                    .unwrap_or_else(|e| panic!("fmgr_info({fn_oid}) failed: {e:?}")),
            );
        }
        self.dispatches.push(PartitionDispatch { rel, key, partdesc, supfuncs });
        Ok(self.dispatches.len() - 1)
    }

    fn leaf_index(&mut self, oid: Oid) -> PgResult<usize> {
        if let Some(i) = self.leaves.iter().position(|r| r.rd_id == oid) {
            return Ok(i);
        }
        let rel = table::table_open(self.mcx, oid, RowExclusiveLock)?;
        self.leaves.push(rel);
        Ok(self.leaves.len() - 1)
    }

    #[inline]
    pub fn leaf_rel(&self, idx: usize) -> &Relation<'mcx> {
        &self.leaves[idx]
    }

    // ExecFindPartition: returns an index for leaf_rel().
    pub fn find_partition(&mut self, slot: &mut SlotData<'mcx>) -> PgResult<usize> {
        let mcx = self.mcx;
        let mut values = [Datum::null(); PARTITION_MAX_KEYS];
        let mut isnull = [false; PARTITION_MAX_KEYS];
        let mut dispatch_idx = 0usize;
        loop {
            let (oid, is_leaf) = {
                let pd = &mut self.dispatches[dispatch_idx];
                let n = pd.key.partnatts as usize;
                // FormPartitionKeyDatum, column-attno arm (expressions loud
                // at partcache build; children share the root's attnos on
                // this lane — asserted at init_dispatch/leaf open below).
                for i in 0..n {
                    let attno = pd.key.partattrs[i];
                    values[i] = exectuples::slot_getattr(slot, attno as i32, &mut isnull[i]);
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
                (pd.partdesc.oids[part_index as usize], pd.partdesc.is_leaf[part_index as usize])
            };
            if is_leaf {
                let idx = self.leaf_index(oid)?;
                let root_natts = self.dispatches[0].rel.rd_att.natts;
                assert_eq!(
                    self.leaves[idx].rd_att.natts,
                    root_natts,
                    "execPartition: attno-remapped partitions unported"
                );
                return Ok(idx);
            }
            // Sub-partitioned child: descend (opened RowExclusiveLock as C).
            if let Some(i) = self.dispatches.iter().position(|d| d.rel.rd_id == oid) {
                dispatch_idx = i;
            } else {
                let sub = table::table_open(self.mcx, oid, RowExclusiveLock)?;
                assert!(sub.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
                dispatch_idx = self.init_dispatch(sub)?;
            }
        }
    }
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
        // DEFAULT partitions are unported (default_index is always -1 here).
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
    for i in 0..n {
        if i > 0 {
            keydesc.push_str(", ");
        }
        let attno = pd.key.partattrs[i];
        let att = pd.rel.rd_att.attr(attno as usize - 1);
        keydesc.push_str(
            core::str::from_utf8(att.attname.name_str()).expect("non-UTF-8 attname"),
        );
    }
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
