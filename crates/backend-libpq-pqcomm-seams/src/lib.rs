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

// ---------------------------------------------------------------------------
//  Input-side primitives + connection init used by backend_startup.c.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `pq_init(client_sock)` (`libpq/pqcomm.c`) — allocate the per-connection
    /// `Port`, apply the TCP options, initialize the message buffers, register
    /// the close-socket `on_proc_exit`, set the socket non-blocking, and build
    /// `FeBeWaitSet`. Returns the `Port` (C: `port = MyProcPort = pq_init(...)`
    /// — the caller installs it as `MyProcPort`). `latch` is C's `MyLatch`
    /// (globals.c), registered at `FeBeWaitSetLatchPos` — an explicit parameter
    /// per the no-ambient-global rule. `Err` carries the `ereport(FATAL)` setup
    /// failures.
    pub fn pq_init(
        client_sock: &types_net::ClientSocket,
        latch: types_storage::latch::LatchHandle,
    ) -> types_error::PgResult<types_net::Port>
);

seam_core::seam!(
    /// `pq_startmsgread()` (`libpq/pqcomm.c`) — mark the start of a message
    /// read (the partial-read consistency guard). `ereport(FATAL)` (the
    /// non-returning `Err`) on lost protocol synchronization.
    pub fn pq_startmsgread() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pq_endmsgread()` (`libpq/pqcomm.c`) — mark the end of a message read.
    /// Infallible.
    pub fn pq_endmsgread()
);

seam_core::seam!(
    /// `pq_getbytes(buf, len)` (`libpq/pqcomm.c`) — read exactly `len` bytes
    /// from the connection into a fresh buffer allocated in `mcx`. `Ok(Some)`
    /// the bytes read; `Ok(None)` for the C `EOF` return (peer closed /
    /// incomplete). `Err` carries the blocking-wait interrupt-processing
    /// `ereport(ERROR)` and the buffer-alloc OOM.
    pub fn pq_getbytes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        len: usize,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pq_peekbyte()` (`libpq/pqcomm.c`) — peek at the next input byte without
    /// consuming it; returns the byte (0-255) or `EOF` (-1). `Err` carries the
    /// blocking-wait interrupt-processing `ereport(ERROR)` reachable through
    /// `pq_recvbuf`.
    pub fn pq_peekbyte() -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `pq_buffer_remaining_data()` (`libpq/pqcomm.c`) — number of bytes still
    /// buffered (unconsumed) in the receive buffer. Infallible.
    pub fn pq_buffer_remaining_data() -> i64
);
