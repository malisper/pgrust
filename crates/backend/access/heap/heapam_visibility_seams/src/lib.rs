use types_core::{Buffer, GlobalVisStateHandle, TransactionId};
use types_error::PgResult;
use types_snapshot::{HTSV_Result, SnapshotData, XidVisMemo};
use types_tuple::{HeapTupleData, HeapTupleHeaderData};

seam_core::seam!(
    // &SnapshotData covers the read lane; the Dirty write-back lane is DML phase 2.
    pub fn heap_tuple_satisfies_visibility<'a, 'tup, 'mcx>(
        htup: &'a mut HeapTupleData<'tup>,
        snapshot: &'a SnapshotData<'mcx>,
        buffer: Buffer,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // HeapTupleSatisfiesMVCC with a per-page xid-status memo (page-batch
    // visibility): pagemode collect resolves each distinct xid once per page.
    pub fn heap_tuple_satisfies_mvcc_page<'a, 'tup, 'mcx>(
        htup: &'a mut HeapTupleData<'tup>,
        snapshot: &'a SnapshotData<'mcx>,
        buffer: Buffer,
        memo: &'a mut XidVisMemo,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // HeapTupleSatisfiesDirty (heapam_visibility.c): the EPQ chain-follow
    // lane; writes xmin/xmax/speculativeToken back into the snapshot.
    pub fn heap_tuple_satisfies_dirty<'a, 'tup, 'mcx>(
        htup: &'a mut HeapTupleData<'tup>,
        snapshot: &'a mut SnapshotData<'mcx>,
        buffer: Buffer,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn heap_tuple_satisfies_vacuum<'a, 'tup>(
        htup: &'a mut HeapTupleData<'tup>,
        oldest_xmin: TransactionId,
        buffer: Buffer,
    ) -> PgResult<HTSV_Result>
);

seam_core::seam!(
    pub fn heap_tuple_is_surely_dead<'a, 'tup>(
        htup: &'a HeapTupleData<'tup>,
        vistest: GlobalVisStateHandle,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn heap_tuple_header_is_only_locked<'a>(hdr: &'a HeapTupleHeaderData) -> PgResult<bool>
);

seam_core::seam!(
    // HeapTupleSatisfiesUpdate (heapam_visibility.c); DML write lane.
    pub fn heap_tuple_satisfies_update<'a, 'tup>(
        htup: &'a mut HeapTupleData<'tup>,
        curcid: types_core::CommandId,
        buffer: Buffer,
    ) -> PgResult<tableam_vocab::TM_Result>
);

seam_core::seam!(
    // HeapTupleSetHintBits (heapam_visibility.c): hint store + dirty-hint.
    pub fn heap_tuple_set_hint_bits<'a>(
        tuple: &'a mut HeapTupleHeaderData,
        buffer: Buffer,
        infomask: u16,
        xid: TransactionId,
    ) -> PgResult<()>
);
