//! Seam declarations for the `backend-rmgrdesc-small` unit
//! (`access/rmgrdesc/rmgrdesc_utils.c` and the other small rmgr descriptor
//! files). The owning unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.
//!
//! The C signatures take `(StringInfo buf, void *elem, void *data)`; the
//! element arrives as the raw record bytes of one array element. The unused
//! `void *data` of the simple element descriptors (always passed as NULL in
//! C) is elided; `array_desc`'s `data` cursor is folded into the `elem_desc`
//! closure capture.

seam_core::seam!(
    /// `array_desc(buf, array, elem_size, count, elem_desc, data)` —
    /// `" [elem, elem, ...]"` (or `" []"` when `count == 0`). `array` is the
    /// raw bytes of the elements (`count * elem_size` bytes); `elem_desc` is
    /// invoked on each `elem_size`-byte slice. `Err` is `appendStringInfo`'s
    /// out-of-memory `ereport(ERROR)`.
    pub fn array_desc<'a>(
        buf: &mut mcx::PgString<'a>,
        array: &[u8],
        elem_size: usize,
        count: i32,
        elem_desc: &mut dyn FnMut(&mut mcx::PgString<'a>, &[u8]) -> types_error::PgResult<()>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `offset_elem_desc(buf, offset, NULL)` — `"%u"` of one `OffsetNumber`.
    pub fn offset_elem_desc(
        buf: &mut mcx::PgString<'_>,
        elem: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `redirect_elem_desc(buf, offsets, NULL)` — `"%u->%u"` of an
    /// `OffsetNumber[2]` pair.
    pub fn redirect_elem_desc(
        buf: &mut mcx::PgString<'_>,
        elem: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `oid_elem_desc(buf, relid, NULL)` — `"%u"` of one `Oid`.
    pub fn oid_elem_desc(
        buf: &mut mcx::PgString<'_>,
        elem: &[u8],
    ) -> types_error::PgResult<()>
);
