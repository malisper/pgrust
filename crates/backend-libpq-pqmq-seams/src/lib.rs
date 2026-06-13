//! Seam declarations for the `backend-libpq-pqmq` unit (`libpq/pqmq.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pq_parse_errornotice(msg, &edata)` (pqmq.c): parse a serialized
    /// ErrorResponse/NoticeResponse message body into an `ErrorData`. `msg` is
    /// the message payload *after* the leading type byte (the caller already
    /// consumed it). Returns only the `context` line the parallel-apply leader
    /// reads. A malformed message `ereport(ERROR)`s, carried on `Err`.
    pub fn pq_parse_errornotice(
        msg: &[u8],
    ) -> types_error::PgResult<types_applyparallel::ParsedErrorNotice>
);
