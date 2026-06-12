//! Seam declarations for the `backend-libpq-pqformat` unit
//! (`libpq/pqformat.c`), in the message-buffer-elided form callers use today:
//! the `StringInfo` the C functions take is owned by the pqformat side, so
//! `pq_beginmessage` starts a fresh message and `pq_endmessage` sends it.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pq_beginmessage(&buf, msgtype)` — begin a message of type `msgtype`.
    pub fn pq_beginmessage(msgtype: u8)
);

seam_core::seam!(
    /// `pq_sendint32(&buf, v)` — append a network-order int32.
    pub fn pq_sendint32(v: u32)
);

seam_core::seam!(
    /// `pq_sendint64(&buf, v)` — append a network-order int64.
    pub fn pq_sendint64(v: i64)
);

seam_core::seam!(
    /// `pq_endmessage(&buf)` — send the completed message and free the buffer.
    pub fn pq_endmessage()
);
