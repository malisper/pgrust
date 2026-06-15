//! `utils/sort/sharedtuplestore.c` — per-batch shared tuplestores for the
//! parallel hash join.
//!
//! NOT PORTED in this worktree: `sharedtuplestore.c` is built on
//! `storage/file/sharedfileset.c` (the `SharedFileSet` create/attach/open
//! protocol over DSM-resident state), whose owner crate is absent here — only
//! the `backend-storage-file-sharedfileset-seams` declarations exist, with no
//! installer (the buffile port left them as seam-and-panic for exactly this
//! reason). A `SharedTuplestore` also lives in DSM and is reached by every
//! participant; without the shared-fileset substrate there is no faithful
//! place to put it.
//!
//! The whole shared-tuplestore seam surface is therefore installed as a loud
//! `panic!` (never a fabricated result), so the recurrence guard sees the
//! owner install its declared seams while the genuinely-unported dependency is
//! honestly reported at the call site. The only consumer that drives these
//! (parallel `nodeHash`/`nodeHashjoin`) reaches them solely on the parallel
//! path, which itself bottoms out on the same unported substrate.

#![allow(non_snake_case)]

use backend_utils_sort_storage_seams as seams;

const UNPORTED: &str =
    "utils/sort/sharedtuplestore.c not ported: needs storage/file/sharedfileset.c \
     (SharedFileSet create/attach/open over DSM), whose owner crate is absent in this worktree";

/// Install every shared-tuplestore seam as a loud panic into the unported
/// `sharedtuplestore.c` / `sharedfileset.c` substrate. Called from
/// [`crate::init_seams`].
pub fn init_seams() {
    // `&mut SharedTuplestoreAccessor`-threaded family (nodeHashjoin).
    seams::sts_begin_parallel_scan::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_end_parallel_scan::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_parallel_scan_next::set(|_mcx, _accessor| panic!("{UNPORTED}"));
    seams::sts_puttuple::set(|_accessor, _hashvalue, _tuple| panic!("{UNPORTED}"));
    seams::sts_end_write::set(|_accessor| panic!("{UNPORTED}"));

    // `SharedTuplestoreAccessorHandle`-threaded family (nodeHash / parallel
    // hash build).
    seams::sts_estimate::set(|_participants| panic!("{UNPORTED}"));
    seams::sts_initialize::set(
        |_sts, _participants, _my, _meta, _flags, _fileset, _name| panic!("{UNPORTED}"),
    );
    seams::sts_attach::set(|_sts, _my, _fileset| panic!("{UNPORTED}"));
    seams::sts_reinitialize::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_begin_parallel_scan_handle::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_end_parallel_scan_handle::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_puttuple_handle::set(|_accessor, _meta, _tuple| panic!("{UNPORTED}"));
    seams::sts_end_write_handle::set(|_accessor| panic!("{UNPORTED}"));
    seams::sts_parallel_scan_next_handle::set(|_mcx, _accessor, _meta| panic!("{UNPORTED}"));
}
