//! Seam declarations for the `backend-libpq-pqcomm` unit (`libpq/pqcomm.c`):
//! the low-level frontend message send/flush primitives elog.c's
//! `send_message_to_frontend` uses.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pq_putmessage(msgtype, s, len)` (`PqCommMethods->putmessage`) — send
    /// one complete protocol-3 message. `body` is the message payload
    /// (everything after the type byte and length word). Returns 0 on
    /// success, EOF (-1) on failure; never `ereport(ERROR)`s (internal
    /// trouble is COMMERROR).
    pub fn pq_putmessage(msgtype: u8, body: &[u8]) -> i32
);

seam_core::seam!(
    /// `pq_putmessage_v2(msgtype, s, len)` — send one protocol-2 style
    /// message (no length word). Returns 0 on success, EOF (-1) on failure.
    pub fn pq_putmessage_v2(msgtype: u8, body: &[u8]) -> i32
);

seam_core::seam!(
    /// `pq_flush()` (`PqCommMethods->flush`) — flush buffered output to the
    /// client. Returns 0 on success, EOF (-1) on failure.
    pub fn pq_flush() -> i32
);
