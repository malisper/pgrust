//! Seam declarations for the `backend-access-common-indextuple` unit
//! (`access/common/indextuple.c`): index-tuple formation.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_datum::Datum;
use types_rel::Relation;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `index_form_tuple(RelationGetDescr(rel), values, isnull)` with
    /// `itup->t_tid = ht_ctid` applied (indextuple.c): build the on-disk
    /// index-tuple bytes for `(values, isnull)` against the index's tuple
    /// descriptor and stamp the heap TID. The formed bytes are returned in
    /// `mcx`. `Err` carries the "index row requires N bytes" oversize ereport
    /// and OOM.
    pub fn index_form_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        values: &[Datum],
        isnull: &[bool],
        ht_ctid: ItemPointerData,
    ) -> types_error::PgResult<PgVec<'mcx, u8>>
);
