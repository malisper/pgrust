//! Seam declarations for the `backend-executor-nodeHashjoin` unit
//! (`executor/nodeHashjoin.c`) that `nodeHash.c` calls back into. The owning
//! unit installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecHashJoinSaveTuple(tuple, hashvalue, fileptr, hashtable)`
    /// (nodeHashjoin.c): append a `MinimalTuple` and its hash value to the
    /// batch temp file at `*fileptr`, creating the `BufFile` (in the hash
    /// table's `spillCxt`, threaded as `mcx`) on first write. `nodeHash.c`
    /// calls this from `ExecHashIncreaseNumBatches` to dump tuples that have
    /// moved to a later batch. The first write `palloc`s and can `ereport` on
    /// I/O error, so the call is fallible. The tuple crosses as its contiguous
    /// C `MinimalTuple` byte image (the flat blob, `t_len` first) — exactly the
    /// `tuple->t_len` bytes C `BufFileWrite`s after the hash value.
    pub fn ExecHashJoinSaveTuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tuple: &[u8],
        hashvalue: types_core::uint32,
        fileptr: &mut Option<mcx::PgBox<'mcx, nodes::nodehash::BufFile>>,
    ) -> types_error::PgResult<()>
);
