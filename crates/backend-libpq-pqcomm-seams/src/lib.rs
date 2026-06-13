//! Seam declarations for the `backend-libpq-pqcomm` unit (`libpq/pqcomm.c`):
//! the low-level frontend message send/flush primitives elog.c's
//! `send_message_to_frontend` uses.
//!
//! Failure surface: each of these reaches `internal_putbytes`/`internal_flush`
//! → `socket_set_nonblocking`, which `ereport(ERROR)`s when `MyProcPort` is
//! NULL, and `secure_write`, whose blocking-mode wait loop can raise through
//! interrupt processing — so they return `PgResult`. Socket-level trouble is
//! *not* an `Err`: as in C it is logged at COMMERROR and surfaced as the
//! `Ok(EOF)` (= `Ok(-1)`) return.

seam_core::seam!(
    /// `pq_putmessage(msgtype, s, len)` (`PqCommMethods->putmessage`) — send
    /// one complete protocol-3 message. `body` is the message payload
    /// (everything after the type byte and length word). `Ok(0)` on success,
    /// `Ok(EOF)` (-1) on socket failure; suppressed (returns `Ok(0)`) while
    /// pqcomm is busy.
    pub fn pq_putmessage(msgtype: u8, body: &[u8]) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `pq_putmessage_v2(msgtype, s, len)` — send one protocol-2 style
    /// message (no length word). `Ok(0)` on success, `Ok(EOF)` on failure.
    pub fn pq_putmessage_v2(msgtype: u8, body: &[u8]) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `pq_flush()` (`PqCommMethods->flush`) — flush buffered output to the
    /// client. `Ok(0)` on success, `Ok(EOF)` on failure.
    pub fn pq_flush() -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `if (FeBeWaitSet) ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetLatchPos,
    /// WL_LATCH_SET, latch)` (`miscinit.c` latch switches) — repoint the
    /// backend wait set's latch event at the new `MyLatch`. A no-op when
    /// `FeBeWaitSet` is unset. `ModifyWaitEvent` can `ereport(ERROR)`.
    pub fn modify_fe_be_wait_set_latch(
        latch: types_storage::latch::LatchHandle,
    ) -> types_error::PgResult<()>
);
