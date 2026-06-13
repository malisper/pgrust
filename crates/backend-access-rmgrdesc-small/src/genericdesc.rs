//! `genericdesc.c` — rmgr descriptor routines for
//! `access/transam/generic_xlog.c`.

use mcx::PgString;
use types_core::uint8;
use types_error::PgResult;
use types_wal::DecodedXLogRecord;

use crate::util::{appendf, read_u16};

/// `generic_desc` — write the page regions this record overrides. The payload
/// is a sequence of `(OffsetNumber offset, OffsetNumber length, char
/// data[length])` region entries; generic records carry no `info` subtypes.
pub fn generic_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let end = data.len();
    let mut ptr = 0usize;

    while ptr < end {
        let offset = read_u16(data, ptr, "generic xlog region offset")?;
        ptr += 2;
        let length = read_u16(data, ptr, "generic xlog region length")?;
        ptr += 2;
        ptr += length as usize;

        if ptr < end {
            appendf!(buf, "offset {offset}, length {length}; ")?;
        } else {
            appendf!(buf, "offset {offset}, length {length}")?;
        }
    }

    Ok(())
}

/// `generic_identify` — generic xlog records have no subtypes.
pub fn generic_identify(_info: uint8) -> Option<&'static str> {
    Some("Generic")
}

/// Adapter installed into the rmgr-table `generic_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn generic_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("generic_desc called without a decoded record");
    generic_desc(buf, record)
}
