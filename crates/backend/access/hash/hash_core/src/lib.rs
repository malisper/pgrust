//! `backend-access-hash-core` — an owned-tree Rust port of the hash index
//! access-method internals (PostgreSQL 18.3):
//!
//! * `hashutil.c`  -> [`hashutil`]
//! * `hashpage.c`  -> [`hashpage`]
//! * `hashovfl.c`  -> [`hashovfl`]
//! * `hashsearch.c`-> [`hashsearch`]
//! * `hashinsert.c`-> [`hashinsert`]
//!
//! The five modules are ONE crate; they call each other directly via
//! `pub(crate)` / `pub` fns — there are no seams between them. The shared
//! byte-level hash page primitives live in [`pagebytes`] (the canonical
//! versions this crate owns; `backend-access-hash-xlog` keeps its own private
//! copies).
//!
//! ## Module-by-module seam-and-panic summary
//! Every `panic`-able path in this crate is a `::call()` into another unit's
//! `-seams` crate for an unported substrate callee (buffer manager, WAL insert,
//! relcache, predicate locks, dynahash, heapam, pgstat, interrupts) — never a
//! missing or approximated bit of this crate's own logic. Each module's header
//! lists exactly which owners it seams into.
//!
//! `hash.c`'s `hashbucketcleanup` (one level up, in the `hash-entry` unit) is
//! reached via `backend-access-hash-entry-seams` during split cleanup; this is
//! an upward call into the AM handler, not a cycle.
//!
//! The only `unsafe` is the dynahash `*mut HTAB` / `*mut ItemPointerData` key
//! marshalling that the dynahash seam contract dictates.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use std::rc::Rc;

use rel::Relation;
use types_scan::scankey::ScanKeyData;
use snapshot::SnapshotData;
use types_tuple::heaptuple::ItemPointerData;

pub mod hashinsert;
pub mod hashovfl;
pub mod hashpage;
pub mod hashsearch;
pub mod hashutil;
pub(crate) mod pagebytes;
pub(crate) mod wal;

// Re-export the public API surface the `hash-entry` crate (hash.c) consumes.
pub use hashinsert::{_hash_doinsert, _hash_pgaddmultitup, _hash_pgaddtup};
pub use hashovfl::{
    _hash_addovflpage, _hash_freeovflpage, _hash_initbitmapbuffer, _hash_ovflblkno_to_bitno,
    _hash_squeezebucket,
};
pub use hashpage::{
    bucket_to_blkno, metap_maxbucket_ntuples, set_metap_ntuples, with_metap, _hash_dropbuf,
    _hash_dropscanbuf, _hash_expandtable, _hash_finish_split, _hash_getbucketbuf_from_hashkey,
    _hash_getbuf, _hash_getbuf_with_condlock_cleanup, _hash_getbuf_with_strategy,
    _hash_getcachedmetap, _hash_getinitbuf, _hash_getnewbuf, _hash_init, _hash_init_metabuffer,
    _hash_initbuf, _hash_pageinit, _hash_relbuf,
};
pub use hashsearch::{_hash_first, _hash_next};
pub use hashutil::{
    _hash_checkpage, _hash_convert_tuple, _hash_datum2hashkey, _hash_datum2hashkey_type,
    _hash_get_indextuple_hashkey, _hash_get_newblock_from_oldbucket,
    _hash_get_newbucket_from_oldbucket, _hash_get_oldblock_from_newbucket, _hash_get_totalbuckets,
    _hash_hashkey2bucket, _hash_kill_items, _hash_spareindex, hashoptions,
};

// ===========================================================================
// HashScan — the owned IndexScanDesc subset the hash scan code threads.
// ===========================================================================

/// The `IndexScanDescData` subset `hashsearch.c` / `hashutil.c` manipulate,
/// holding the hash-private scan state (`scan->opaque`). Mirrors nbtree's
/// `NbtScan`. Hash has no parallel scan, so no parallel fields.
#[derive(Debug)]
pub struct HashScan<'mcx> {
    /// `scan->indexRelation`.
    pub indexRelation: Relation<'mcx>,
    /// `scan->opaque` — the hash-private scan state.
    pub opaque: hash::hashpage::HashScanOpaqueData,
    /// `scan->xs_recheck`.
    pub xs_recheck: bool,
    /// `scan->kill_prior_tuple`.
    pub kill_prior_tuple: bool,
    /// `scan->xs_heaptid` — the heap TID of the current tuple.
    pub xs_heaptid: ItemPointerData,
    /// `scan->numberOfKeys`.
    pub numberOfKeys: i32,
    /// `scan->keyData`.
    pub keyData: alloc::vec::Vec<ScanKeyData<'mcx>>,
    /// `scan->xs_snapshot` — the active snapshot threaded into predicate locks
    /// (`None` is the C NULL snapshot).
    pub xs_snapshot: Option<Rc<SnapshotData>>,
    /// `scan->ignore_killed_tuples`.
    pub ignore_killed_tuples: bool,
    /// `scan->instrument != NULL` — whether index-scan instrumentation is on.
    pub instrument: bool,
    /// `scan->instrument->nsearches` — accumulated when `instrument` is set.
    pub nsearches: i64,
}

/// `HashScan` is the concrete type stored in `IndexScanDescData.opaque` (C's
/// `void *opaque`); the A0 carrier downcasts to it in every hash AM adapter.
impl<'mcx> types_tableam::amopaque::AmOpaqueType<'mcx> for HashScan<'mcx> {
    const TAG: types_tableam::amopaque::AmOpaqueTag = types_tableam::amopaque::tags::HASH_SCAN;
}

// ===========================================================================
// init_seams
// ===========================================================================

/// hash-core owns no INWARD seams (it is consumed by the `hash-entry` unit
/// directly, not via a seam registry). `init_seams()` is a no-op, mirroring
/// `functioncmds` — wired through `seams-init` for symmetry.
pub fn init_seams() {}
