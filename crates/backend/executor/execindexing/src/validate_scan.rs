// validate_index's scan half (heapam_index_validate_scan) + ValidateIndexState
// (index.c); hosted beside build_scan for the same layering reason.
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{InvalidBlockNumber, INDEX_MAX_KEYS, INT8OID};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::itemptr::{
    itemptr_decode, itemptr_encode, InvalidOffsetNumber, ItemPointerCompare, ItemPointerData,
};
use tableam_vocab::{SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_TYPE_SEQSCAN};
use tuplesort::{Tuplesort, TUPLESORT_NONE};

use crate::{index_predicate_passes, FormIndexDatum, IndexInfo};

const Int8LessOperator: ::types_core::Oid = 412;

pub struct ValidateIndexState {
    pub tuplesort: Tuplesort,
    pub htups: f64,
    pub itups: f64,
    pub tups_inserted: f64,
}

impl ValidateIndexState {
    pub fn new() -> PgResult<Self> {
        Ok(Self {
            tuplesort: Tuplesort::begin_datum(
                INT8OID,
                Int8LessOperator,
                ::types_core::InvalidOid,
                false,
                init_small::globals::maintenance_work_mem(),
                TUPLESORT_NONE,
            )?,
            htups: 0.0,
            itups: 0.0,
            tups_inserted: 0.0,
        })
    }

    // validate_index_callback (index.c).
    pub fn collect(&mut self, itemptr: &ItemPointerData) -> PgResult<()> {
        self.tuplesort.putdatum(Datum::from_i64(itemptr_encode(itemptr)), false)?;
        self.itups += 1.0;
        Ok(())
    }
}

pub fn table_index_validate_scan<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    snapshot: &snapmgr::Snapshot,
    state: &mut ValidateIndexState,
) -> PgResult<()> {
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        types_slot::TupleSlotKind::BufferHeapTuple,
        Some(heap_relation.rd_att.clone()),
    );

    let mut root_blkno = InvalidBlockNumber;
    let mut root_offsets = [InvalidOffsetNumber; MaxHeapTuplesPerPage];
    let mut in_index = [false; MaxHeapTuplesPerPage];
    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    let mut indexcursor: Option<ItemPointerData> = None;
    let mut tuplesort_empty = false;

    // Syncscan is disabled: the merge requires reading from block zero
    // forward to match the sorted TIDs.
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE;
    let mut scan = heapam::heap_beginscan(
        mcx,
        heap_relation,
        Some(snapshot.clone()),
        0,
        PgVec::new_in(mcx),
        None,
        flags,
    )?;

    // C heapam_handler.c ExecPrepareQual runs before the scan (see
    // prepare_index_predicate).
    crate::prepare_index_predicate(mcx, index_info)?;

    let mut per_tuple = mcx::MemoryContext::new_bump("IndexValidatePerTuple");

    loop {
        use types_scan::ScanDirection;
        if heapam::heap_getnext(&mut scan, ScanDirection::ForwardScanDirection)?.is_none() {
            break;
        }
        let heap_tuple = scan.rs_ctup().expect("just returned Some");
        let heapcursor = heap_tuple.t_self;
        let is_heap_only = heap_tuple.t_data().is_heap_only();
        postgres_seams::check_for_interrupts::call()?;

        state.htups += 1.0;

        if scan.rs_cblock != root_blkno {
            let pin = scan.rs_cbuf.as_ref().expect("pinned page for returned tuple");
            let guard = pin.lock_share()?;
            pruneheap::heap_get_root_tuples(pin.page(), &mut root_offsets)?;
            drop(guard);
            in_index = [false; MaxHeapTuplesPerPage];
            root_blkno = scan.rs_cblock;
        }

        let mut root_tuple = heapcursor;
        let mut root_offnum = heapcursor.ip_posid;
        if is_heap_only {
            root_offnum = root_offsets[root_offnum as usize - 1];
            if root_offnum == InvalidOffsetNumber {
                return Err(Box::new(
                    types_error::PgError::error(format!(
                        "failed to find parent tuple for heap-only tuple at ({},{}) in table \"{}\"",
                        ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&heapcursor),
                        heapcursor.ip_posid,
                        heap_relation.name()
                    ))
                    .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
                ));
            }
            root_tuple.ip_posid = root_offnum;
        }

        while !tuplesort_empty
            && indexcursor.map_or(true, |c| ItemPointerCompare(&c, &root_tuple) < 0)
        {
            if let Some(c) = indexcursor {
                if ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&c) == root_blkno {
                    in_index[c.ip_posid as usize - 1] = true;
                }
            }
            match state.tuplesort.getdatum(true)? {
                Some(nd) => {
                    debug_assert!(!nd.isnull);
                    indexcursor = Some(itemptr_decode(nd.value.as_i64()));
                }
                None => {
                    tuplesort_empty = true;
                    indexcursor = None;
                }
            }
        }

        if (tuplesort_empty
            || ItemPointerCompare(indexcursor.as_ref().expect("non-empty cursor"), &root_tuple)
                > 0)
            && !in_index[root_offnum as usize - 1]
        {
            per_tuple.reset();

            let buffer = scan.rs_cbuf.as_ref().expect("pinned").buffer();
            // SAFETY: same live pinned image rs_ctup views, consumed before advance.
            let tuple = unsafe {
                ::types_tuple::HeapTupleData::from_raw_parts(
                    heap_tuple.header_ptr(),
                    heap_tuple.t_len,
                    heap_tuple.t_self,
                    heap_tuple.t_tableOid,
                )
            };
            exectuples::exec_store_buffer_heap_tuple(&mut slot, mcx, tuple, buffer);

            if !index_info.ii_Predicate.is_nil()
                && !index_predicate_passes(mcx, per_tuple.mcx(), index_info, &mut slot)?
            {
                continue;
            }

            FormIndexDatum(
                mcx,
                per_tuple.mcx(),
                index_info,
                &mut slot,
                &mut values[..],
                &mut isnull[..],
            )?;

            let n = index_info.ii_NumIndexAttrs as usize;
            let check = if index_info.ii_Unique {
                ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_YES
            } else {
                ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO
            };
            indexam::index_insert(
                mcx,
                index_relation,
                &values[..n],
                &isnull[..n],
                &root_tuple,
                heap_relation,
                check,
                false,
                &mut index_info.ii_AmCache,
            )?;

            state.tups_inserted += 1.0;
        }
    }

    exectuples::exec_clear_tuple(&mut slot, mcx);
    heapam::heap_endscan(scan)?;

    index_info.ii_ExpressionsState.clear();
    index_info.ii_PredicateState = None;
    Ok(())
}
