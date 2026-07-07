// heapam_relation_copy_for_cluster + reform_and_rewrite_tuple
// (heapam_handler.c), hosted here: heapam_handler cannot dep indexam
// (indexam -> tableam -> heapam_handler), and heap is the only table AM so
// the tableam dispatch formality is a direct call from copy_table_data.
use mcx::Mcx;
use types_core::{MultiXactId, TransactionId};
use types_error::PgResult;
use types_rel::Relation;
use types_scan::sdir::ScanDirection;
use types_slot::SlotData;
use types_snapshot::HTSV_Result;
use types_tuple::htup::HeapTupleData;

#[allow(clippy::too_many_arguments)]
pub fn copy_for_cluster<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    old_index: Option<&Relation<'mcx>>,
    use_sort: bool,
    oldest_xmin: TransactionId,
    xid_cutoff: &mut TransactionId,
    multi_cutoff: &mut MultiXactId,
    toastoid: types_core::Oid,
) -> PgResult<(f64, f64, f64)> {
    let is_system_catalog = catalog::IsSystemRelation(old_heap);
    let old_tup_desc = old_heap.descr();
    let new_tup_desc = new_heap.descr();
    let natts = new_tup_desc.natts as usize;
    let mut values: mcx::PgVec<'_, datum::Datum> = mcx::vec_from_elem_in(mcx, datum::Datum::null(), natts);
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);

    let mut rwstate = rewriteheap::begin_heap_rewrite(
        mcx,
        old_heap,
        new_heap,
        oldest_xmin,
        *xid_cutoff,
        *multi_cutoff,
        toastoid,
    )?;

    let mut tuplesort = if use_sort {
        let index = old_index.expect("use_sort implies an index");
        // SAFETY: lifetime erasure on the relcache tupdesc; old_heap stays
        // open for the life of the sort (C contract).
        let heap_desc: std::rc::Rc<types_tuple::TupleDescData<'static>> =
            unsafe { core::mem::transmute(old_heap.rd_att.clone()) };
        let indkey: mcx::PgVec<'_, i16> = {
            let form = index.rd_index.as_ref().expect("index form");
            let mut v = mcx::vec_with_capacity_in(mcx, form.indkey.len())?;
            v.extend(form.indkey.iter().copied());
            v
        };
        Some(tuplesort::Tuplesort::begin_cluster(
            heap_desc,
            index,
            &indkey,
            init_small::globals::maintenance_work_mem(),
            tuplesort::TUPLESORT_NONE,
        )?)
    } else {
        None
    };

    // Expression-index sort: form the index key tuple per heap tuple for the
    // sort to compare (C's TuplesortClusterArg estate lane).
    let mut expr_sort = match (old_index, use_sort) {
        (Some(index), true)
            if index
                .rd_index
                .as_ref()
                .expect("index form")
                .indkey
                .iter()
                .take(index.indnkeyatts() as usize)
                .any(|&a| a == 0) =>
        {
            Some((
                execindexing::BuildIndexInfo(mcx, index)?,
                mcx::MemoryContext::new_bump("ClusterExprPerTuple"),
            ))
        }
        _ => None,
    };

    let snapshot_any = std::rc::Rc::new(types_snapshot::SnapshotData::sentinel(
        mcx,
        types_snapshot::SnapshotType::SNAPSHOT_ANY,
    ));

    enum ScanArm<'mcx> {
        Index(indexam::IndexScanDescData<'mcx>),
        Table(tableam::TableScanDesc<'mcx>),
    }

    let mut arm = match (old_index, use_sort) {
        (Some(index), false) => {
            let mut scan = indexam::index_beginscan(
                mcx,
                old_heap,
                index,
                std::rc::Rc::clone(&snapshot_any),
                0,
                0,
            )?;
            indexam::index_rescan(&mut scan, None, None)?;
            ScanArm::Index(scan)
        }
        _ => ScanArm::Table(tableam::table_beginscan(
            mcx,
            old_heap,
            Some(std::rc::Rc::clone(&snapshot_any)),
            0,
            mcx::PgVec::new_in(mcx),
        )?),
    };

    let mut slot = tableam::table_slot_create(mcx, old_heap)?;
    let (mut num_tuples, mut tups_vacuumed, mut tups_recently_dead) = (0f64, 0f64, 0f64);

    loop {
        postgres_seams::check_for_interrupts::call()?;

        let fetched = match &mut arm {
            ScanArm::Index(scan) => {
                let found =
                    indexam::index_getnext_slot(mcx, scan, ScanDirection::ForwardScanDirection, &mut slot)?;
                if found && scan.xs_recheck {
                    panic!("CLUSTER does not support lossy index conditions");
                }
                found
            }
            ScanArm::Table(scan) => tableam::table_scan_getnextslot(
                mcx,
                scan,
                ScanDirection::ForwardScanDirection,
                &mut slot,
            )?,
        };
        if !fetched {
            break;
        }

        let SlotData::BufferHeap(bslot) = &mut slot else {
            panic!("heap copy_for_cluster requires a BufferHeapTuple slot");
        };
        let buf = bslot.buffer;
        let tuple = bslot.base.tuple.as_mut().expect("buffer slot holds a tuple");

        bufmgr_seams::lock_buffer::call(buf, bufmgr_seams::BUFFER_LOCK_SHARE)?;
        let htsv = heapam_visibility::HeapTupleSatisfiesVacuum(tuple, oldest_xmin, buf)?;
        let isdead = match htsv {
            HTSV_Result::HEAPTUPLE_DEAD => true,
            HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
                tups_recently_dead += 1.0;
                false
            }
            HTSV_Result::HEAPTUPLE_LIVE => false,
            HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
                // Only reachable for our own uncommitted inserts (single
                // backend); C warns for other xacts and copies either way.
                false
            }
            HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
                tups_recently_dead += 1.0;
                false
            }
        };
        bufmgr_seams::lock_buffer::call(buf, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;

        if isdead {
            tups_vacuumed += 1.0;
            if rewriteheap::rewrite_heap_dead_tuple(&mut rwstate, tuple) {
                tups_vacuumed += 1.0;
                tups_recently_dead -= 1.0;
            }
            continue;
        }

        num_tuples += 1.0;
        if tuplesort.is_some() {
            let itup_buf = match expr_sort.as_mut() {
                Some((index_info, per_tuple)) => {
                    per_tuple.reset();
                    let mut kvalues = [datum::Datum::null(); 32];
                    let mut kisnull = [false; 32];
                    execindexing::FormIndexDatum(
                        mcx,
                        per_tuple.mcx(),
                        index_info,
                        &mut slot,
                        &mut kvalues,
                        &mut kisnull,
                    )?;
                    Some(nbtree::itup::index_form_tuple(
                        per_tuple.mcx(),
                        old_index.expect("expr_sort implies an index").rd_att.as_ref(),
                        &kvalues,
                        &kisnull,
                    )?)
                }
                None => None,
            };
            let SlotData::BufferHeap(bslot) = &mut slot else {
                panic!("heap copy_for_cluster requires a BufferHeapTuple slot");
            };
            let tuple = bslot.base.tuple.as_ref().expect("buffer slot holds a tuple");
            // SAFETY: live maxaligned itup image formed above.
            let itup_bytes = itup_buf
                .as_ref()
                .map(|b| unsafe { core::slice::from_raw_parts(b.as_ptr().cast::<u8>(), b.size()) });
            tuplesort.as_mut().expect("checked").putheaptuple(tuple, itup_bytes)?;
        } else {
            reform_and_rewrite_tuple(
                mcx,
                tuple,
                old_tup_desc,
                new_tup_desc,
                &mut values,
                &mut isnull,
                new_heap,
                &mut rwstate,
            )?;
        }
    }

    match arm {
        ScanArm::Index(scan) => indexam::index_endscan(scan)?,
        ScanArm::Table(scan) => tableam::table_endscan(scan)?,
    }
    exectuples::exec_clear_tuple(&mut slot, mcx);
    drop(slot);

    if let Some(mut ts) = tuplesort {
        ts.performsort()?;
        loop {
            postgres_seams::check_for_interrupts::call()?;
            let Some(tuple) = ts.getheaptuple(true)? else {
                break;
            };
            reform_and_rewrite_tuple(
                mcx,
                &tuple,
                old_tup_desc,
                new_tup_desc,
                &mut values,
                &mut isnull,
                new_heap,
                &mut rwstate,
            )?;
        }
        ts.end();
    }

    rewriteheap::end_heap_rewrite(rwstate, new_heap)?;

    let _ = is_system_catalog;
    Ok((num_tuples, tups_vacuumed, tups_recently_dead))
}

#[allow(clippy::too_many_arguments)]
fn reform_and_rewrite_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    old_tup_desc: &types_tuple::TupleDescData<'_>,
    new_tup_desc: &types_tuple::TupleDescData<'_>,
    values: &mut [datum::Datum],
    isnull: &mut [bool],
    new_heap: &Relation<'mcx>,
    rwstate: &mut rewriteheap::RewriteState<'mcx>,
) -> PgResult<()> {
    types_tuple::heap_deform_tuple(tuple, old_tup_desc, values, isnull);
    for i in 0..new_tup_desc.natts as usize {
        if new_tup_desc.attr(i).attisdropped {
            isnull[i] = true;
        }
    }
    let mut copied_tuple = heaptuple::heap_form_tuple(mcx, new_tup_desc, values, isnull)?;
    rewriteheap::rewrite_heap_tuple(rwstate, new_heap, tuple, &mut copied_tuple)
}
