//! Seam declarations + the opaque-state carrier for the heap-rewrite state
//! machine (`access/heap/rewriteheap.c`): the `begin_heap_rewrite` /
//! `rewrite_heap_tuple` / `rewrite_heap_dead_tuple` / `end_heap_rewrite` API a
//! `CLUSTER` / `VACUUM FULL` rebuild (cluster.c) drives, plus the two
//! WAL/checkpoint-side entry points (`heap_xlog_logical_rewrite` replay and
//! `CheckPointLogicalRewriteHeap`).
//!
//! The consumers — `cluster.c` (the rebuild loop), the HEAP2 replay dispatcher
//! (`heap_xlog_logical_rewrite`), and the checkpointer
//! (`CheckPointLogicalRewriteHeap`) — are above / cross-cycle from this owner,
//! so they reach it through these seams. The owning unit
//! (`backend-access-heap-rewriteheap`) installs every one of them from its
//! `init_seams()`; until then each call panics loudly. There is no silent
//! fallback.
//!
//! ## Owned model
//! C's opaque `RewriteState` (a `RewriteStateData *` allocated in a private
//! "Table rewrite" memory context) is carried here as the concrete
//! `'mcx`-bound [`RewriteState`] = `PgBox<'mcx, RewriteStateData<'mcx>>`. The
//! engine holds `'mcx`-lifetimed owned values (relcache `Relation` aliases, a
//! `BulkWriteState`, the in-memory `FormedTuple` maps, an `Mcx` handle), so it
//! cannot be type-erased to `'static dyn Any` (the bulk_write carrier idiom).
//! The state struct therefore lives here, where the seam declarations can name
//! it; the owner fills the bodies. Hash tables are `std::collections::HashMap`
//! (the entries own copied `FormedTuple`s, which a raw-pointer dynahash can't
//! safely hold).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use std::collections::HashMap;

use ::mcx::{Mcx, PgBox, PgVec};
use ::types_core::primitive::{BlockNumber, MultiXactId, Oid, XLogRecPtr};
use ::types_core::TransactionId;
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_storage::file::File;
use ::types_storage::RelFileLocator;
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::ItemPointerData;

use ::bulkwrite_seams::BulkWriteState;

/// `RewriteState` (`access/rewriteheap.h`) — opaque to every consumer. The
/// owned model boxes the concrete [`RewriteStateData`] in `mcx` (C: the
/// `RewriteStateData` palloc'd in the private "Table rewrite" context).
pub type RewriteState<'mcx> = PgBox<'mcx, RewriteStateData<'mcx>>;

/// `TidHashKey` (rewriteheap.c): tuple xmin + old-heap location.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TidHashKey {
    pub xmin: TransactionId,
    pub tid: ItemPointerData,
}

/// `UnresolvedTupData` (rewriteheap.c): A's old location + A's tuple contents.
pub struct UnresolvedTup<'mcx> {
    pub old_tid: ItemPointerData,
    pub tuple: FormedTuple<'mcx>,
}

/// `LogicalRewriteMappingData` (`access/rewriteheap.h`): one old->new
/// tuple-location mapping written to a per-xid mapping file.
#[derive(Clone, Copy, Debug)]
pub struct LogicalRewriteMappingData {
    pub old_locator: RelFileLocator,
    pub new_locator: RelFileLocator,
    pub old_tid: ItemPointerData,
    pub new_tid: ItemPointerData,
}

/// `RewriteMappingFile` (rewriteheap.c): the per-xid mapping file plus its
/// in-memory mapping list (`dclist_head mappings` -> `Vec`).
pub struct RewriteMappingFile {
    /// `int vfd` — the fd.c VFD of the mapping file (`PathNameOpenFile`).
    pub vfd: File,
    /// `off_t off` — how far we've written.
    pub off: i64,
    /// `dclist_head mappings` — in-memory mappings not yet flushed.
    pub mappings: Vec<LogicalRewriteMappingData>,
    /// `char path[MAXPGPATH]` — path, for error messages.
    pub path: String,
}

/// `RewriteStateData` (rewriteheap.c) — the rewrite-engine state.
pub struct RewriteStateData<'mcx> {
    /// `Relation rs_old_rel` — source heap (aliased relcache handle).
    pub rs_old_rel: Relation<'mcx>,
    /// `Relation rs_new_rel` — destination heap.
    pub rs_new_rel: Relation<'mcx>,
    /// `BulkWriteState *rs_bulkstate` — writer for the destination.
    pub rs_bulkstate: Option<BulkWriteState<'mcx>>,
    /// `BulkWriteBuffer rs_buffer` — page currently being built (`None` == NULL).
    pub rs_buffer: Option<PgVec<'mcx, u8>>,
    /// `BlockNumber rs_blockno` — block where the page will go.
    pub rs_blockno: BlockNumber,
    /// `bool rs_logical_rewrite`.
    pub rs_logical_rewrite: bool,
    /// `TransactionId rs_oldest_xmin`.
    pub rs_oldest_xmin: TransactionId,
    /// `TransactionId rs_freeze_xid`.
    pub rs_freeze_xid: TransactionId,
    /// `TransactionId rs_logical_xmin`.
    pub rs_logical_xmin: TransactionId,
    /// `MultiXactId rs_cutoff_multi`.
    pub rs_cutoff_multi: MultiXactId,
    /// `XLogRecPtr rs_begin_lsn`.
    pub rs_begin_lsn: XLogRecPtr,
    /// `HTAB *rs_unresolved_tups` — unmatched A tuples.
    pub rs_unresolved_tups: HashMap<TidHashKey, UnresolvedTup<'mcx>>,
    /// `HTAB *rs_old_new_tid_map` — unmatched B tuples (key -> new tid).
    pub rs_old_new_tid_map: HashMap<TidHashKey, ItemPointerData>,
    /// `HTAB *rs_logical_mappings` — logical remapping files (xid -> file).
    pub rs_logical_mappings: HashMap<TransactionId, RewriteMappingFile>,
    /// `uint32 rs_num_rewrite_mappings`.
    pub rs_num_rewrite_mappings: u32,
    /// The allocator the rewrite runs in (C: the "Table rewrite" context).
    pub mcx: Mcx<'mcx>,
}

seam_core::seam!(
    /// `begin_heap_rewrite(old_heap, new_heap, oldest_xmin, freeze_xid,
    /// cutoff_multi)` (rewriteheap.c) — begin a table rewrite, returning the
    /// opaque [`RewriteState`].
    pub fn begin_heap_rewrite<'mcx>(
        mcx: Mcx<'mcx>,
        old_heap: &Relation<'mcx>,
        new_heap: &Relation<'mcx>,
        oldest_xmin: TransactionId,
        freeze_xid: TransactionId,
        cutoff_multi: MultiXactId,
    ) -> PgResult<RewriteState<'mcx>>
);

seam_core::seam!(
    /// `rewrite_heap_tuple(state, old_tuple, new_tuple)` (rewriteheap.c) — add a
    /// (possibly transformed) live tuple to the new heap, preserving its
    /// visibility info and update-chain ctid links. `new_tuple` is consumed (C
    /// scribbles on it; it must be temp storage).
    pub fn rewrite_heap_tuple<'mcx>(
        state: &mut RewriteState<'mcx>,
        old_tuple: &FormedTuple<'mcx>,
        new_tuple: FormedTuple<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `rewrite_heap_dead_tuple(state, old_tuple)` (rewriteheap.c) — register a
    /// dead tuple. Returns `true` if a tuple was removed from the unresolved
    /// table.
    pub fn rewrite_heap_dead_tuple<'mcx>(
        state: &mut RewriteState<'mcx>,
        old_tuple: &FormedTuple<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `end_heap_rewrite(state)` (rewriteheap.c) — finish the rewrite. Consumes
    /// `state`.
    pub fn end_heap_rewrite<'mcx>(state: RewriteState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_xlog_logical_rewrite(r)` (rewriteheap.c) — replay an
    /// `XLOG_HEAP2_REWRITE` record. The replay dispatcher decodes the
    /// `xl_heap_rewrite_mapping` header (and `XLogRecGetXid(r)`) and passes the
    /// fields here; `data` is the trailing mapping-array payload. Does the file
    /// work (open/truncate/pwrite/fsync) itself.
    pub fn heap_xlog_logical_rewrite(
        mapped_xid: TransactionId,
        mapped_db: Oid,
        mapped_rel: Oid,
        offset: i64,
        num_mappings: u32,
        start_lsn: XLogRecPtr,
        record_xid: TransactionId,
        data: &[u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckPointLogicalRewriteHeap()` (rewriteheap.c) — checkpoint-time
    /// cleanup/flush of the logical-rewrite mapping directory.
    pub fn check_point_logical_rewrite_heap() -> PgResult<()>
);
