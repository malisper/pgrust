//! Inward seam declarations for the `backend-access-hash-entry` unit
//! (`hash.c` + `hashsort.c` — the hash AM handler / build driver).
//!
//! `hash.c` is the top of the hash AM dependency graph: it is reached by the
//! generic index machinery via the AM dispatch table (`hashhandler`), not by a
//! sibling that would create a cycle. It therefore declares no inward seams.
//! The crate exists for symmetry and is wired through `seams-init`
//! (`init_seams()` is a no-op, mirroring `functioncmds`).

#![allow(non_snake_case)]

// ---------------------------------------------------------------------------
// `hashbucketcleanup` (hash.c) is consumed by the sibling hashpage / hashovfl
// modules (`backend-access-hash-core`) during `_hash_expandtable` /
// `_hash_splitbucket` to clean up the old bucket after a split. It lives in
// hash.c (this unit), one level above hash-core, so the call crosses a seam
// (no cycle: hash.c is the AM-handler top). Until `backend-access-hash-entry`
// lands, a call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `hashbucketcleanup(rel, cur_bucket, bucket_buf, bucket_blkno, NULL,
    /// maxbucket, highmask, lowmask, NULL, NULL, true, NULL, NULL)` (hash.c) —
    /// the split-cleanup invocation hash-core makes: no VACUUM strategy, no
    /// stats out-params, no bulk-delete callback. The caller holds a cleanup
    /// lock on `bucket_buf`. `Err` carries the buffer / WAL `ereport(ERROR)`
    /// surface.
    pub fn hashbucketcleanup_split_cleanup<'mcx>(
        rel: &rel::Relation<'mcx>,
        cur_bucket: hash::hashpage::Bucket,
        bucket_buf: types_storage::storage::Buffer,
        bucket_blkno: types_core::primitive::BlockNumber,
        maxbucket: u32,
        highmask: u32,
        lowmask: u32,
    ) -> types_error::PgResult<()>
);
