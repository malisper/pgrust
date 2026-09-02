//! `ambuild` for objkv indexes: CREATE INDEX on a table that already has rows.
//!
//! The rows are read out of the bucket and every entry is staged into the
//! current transaction's writes, so the whole index arrives as part of one
//! commit object. A build that fails leaves nothing behind, and one that
//! succeeds is visible exactly when the CREATE INDEX transaction commits --
//! the same rule every other objkv write follows.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::execindexing::IndexInfo;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_rel::Relation;

/// (heap_tuples, index_tuples), as the other ambuilds report.
pub struct BuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

pub fn objkvbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelation: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<BuildResult> {
    let rows = tableam::objkv_am::scan_rows(
        tableam::objkv_am::scope(heapRelation),
        heapRelation.rd_id,
        ::objkv::key::LATEST,
    )?;

    let mut slot = ::exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::HeapTuple,
        Some(heapRelation.rd_att.clone()),
    );
    let natts = indexInfo.ii_NumIndexAttrs as usize;
    let mut values = vec![Datum::null(); natts.max(1)];
    let mut isnull = vec![false; natts.max(1)];

    // Before the loop, as C does: a predicate that cannot be planned is an
    // error on an empty table too.
    let partial = !indexInfo.ii_Predicate.is_nil();
    if partial {
        ::execindexing::prepare_index_predicate(mcx, indexInfo)?;
    }

    let mut n = 0.0;
    for (rowid, image) in rows {
        let tid = tableam::objkv_am::tid_of(rowid);
        tableam::objkv_am::store_image(mcx, &mut slot, &image, tid)?;
        if partial && !::execindexing::index_predicate_passes(mcx, mcx, indexInfo, &mut slot)? {
            continue;
        }
        ::execindexing::FormIndexDatum(mcx, mcx, indexInfo, &mut slot, &mut values, &mut isnull)?;
        // CREATE INDEX raises on a duplicate; there is no row to withdraw.
        tableam::objkv_index::insert(
            mcx,
            indexRelation,
            heapRelation,
            &values,
            &isnull,
            rowid,
            false,
        )?;
        n += 1.0;
    }

    Ok(BuildResult { heap_tuples: n, index_tuples: n })
}

/// The entry an index holds for one stored row, for
/// `tableam_seams::objkv_index_row_datum`: the table AM re-derives a row's
/// entry to retire it on update and delete, and only the executor can
/// evaluate an index expression or predicate.
pub fn index_row_datum<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
    slot: &mut ::types_slot::SlotData<'mcx>,
    values: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<bool> {
    let mut info = ::execindexing::BuildIndexInfo(mcx, index)?;
    if !info.ii_Predicate.is_nil()
        && !::execindexing::index_predicate_passes(mcx, mcx, &mut info, slot)?
    {
        return Ok(false);
    }
    ::execindexing::FormIndexDatum(mcx, mcx, &mut info, slot, values, isnull)?;
    Ok(true)
}
