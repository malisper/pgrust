//! Seam declarations for the `backend-libpq-pqformat` unit
//! (`libpq/pqformat.c`): outgoing-message assembly over a caller-owned
//! [`StringInfo`], exactly as in C (`pq_beginmessage(&buf, type)` ...
//! `pq_endmessage(&buf)`). The buffer is allocated in the caller's context
//! (C: `initStringInfo` in `CurrentMemoryContext`), so `pq_beginmessage`
//! takes the target `Mcx` and the buffer carries its lifetime.

use mcx::Mcx;
use types_error::PgResult;
use types_stringinfo::StringInfo;

seam_core::seam!(
    /// `pq_beginmessage(&buf, msgtype)` — initialize a fresh message buffer of
    /// type `msgtype` (stashed in `buf.cursor`). `Err` carries
    /// `initStringInfo`'s palloc out-of-memory `ereport(ERROR)`.
    pub fn pq_beginmessage<'mcx>(mcx: Mcx<'mcx>, msgtype: u8) -> PgResult<StringInfo<'mcx>>
);

seam_core::seam!(
    /// `pq_sendint32(buf, i)` — append a network-order int32. `Err` carries
    /// `enlargeStringInfo`'s `ereport(ERROR)` (out of memory / 1GB string
    /// buffer cap).
    pub fn pq_sendint32<'mcx>(buf: &mut StringInfo<'mcx>, i: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `pq_sendint64(buf, i)` — append a network-order int64. `Err` carries
    /// `enlargeStringInfo`'s `ereport(ERROR)` (out of memory / 1GB string
    /// buffer cap).
    pub fn pq_sendint64<'mcx>(buf: &mut StringInfo<'mcx>, i: u64) -> PgResult<()>
);

seam_core::seam!(
    /// `pq_endmessage(&buf)` — send the completed message and free the buffer
    /// (consumed; C pfrees `buf->data` and NULLs it). Infallible: a send
    /// failure is `ereport(COMMERROR)` inside pqcomm.c (below ERROR, no
    /// longjmp) and the C return value is ignored.
    pub fn pq_endmessage<'mcx>(buf: StringInfo<'mcx>)
);
