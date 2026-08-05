use ::datum::{Datum, NullableDatum};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_tuple::TupleDescData;

seam_core::seam!(
    pub fn tuplesort_datums<'mcx>(
        mcx: Mcx<'mcx>,
        datum_type: Oid,
        sort_operator: Oid,
        collation: Oid,
        nulls_first: bool,
        work_mem: i32,
        values: &[NullableDatum],
    ) -> PgResult<PgVec<'mcx, NullableDatum>>
);

// ---- pgrcolumnar ingest sort (writer sort-on-ingest, pgrcolumnar-impl cluster key) --
//
// pgrcolumnar cannot depend on tuplesort (tuplesort -> nbtree -> tableam ->
// pgrcolumnar); this seam hands it a spill-capable row sorter keyed on the
// writer's column classes. Comparators are fixed per kind — signed ints for
// the int classes, C-collation byte order for text — matching the v5/v6
// sorted-flag order definition (and the frozen-bank LC_ALL=C protocol), so
// footer sortedness metadata written above a drain of this sorter is exact.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CbSortKeyKind {
    /// int2: `btint2fastcmp` order.
    Int16,
    /// int4/date: `ssup_datum_int32_cmp` order.
    Int32,
    /// int8/timestamp: `ssup_datum_signed_cmp` order.
    Int64,
    /// text/varchar/bpchar payload bytes, C collation (`varstrfastcmp_c`).
    TextC,
}

pub trait CbIngestSort {
    /// Buffer one row (copied; the caller's datums need not outlive the call).
    fn put_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()>;
    /// Finish loading and sort (spilling to temp files beyond work_mem).
    fn sort(&mut self) -> PgResult<()>;
    /// Next row in key order into caller buffers (len = ncols). By-ref datums
    /// stay live until the next call on this sorter. false = drained.
    fn next_row(&mut self, values: &mut [Datum], isnull: &mut [bool]) -> PgResult<bool>;
}

seam_core::seam!(
    pub fn pgrcolumnar_ingest_sort(
        tup_desc: std::rc::Rc<TupleDescData<'static>>,
        keys: &[(i16, CbSortKeyKind)],
        work_mem: i32,
    ) -> PgResult<Box<dyn CbIngestSort>>
);
