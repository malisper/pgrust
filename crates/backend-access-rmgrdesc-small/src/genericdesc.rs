//! `genericdesc.c` — rmgr descriptor routines for
//! `access/transam/generic_xlog.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};

use crate::util::{appendf, read_u16};

/// `generic_desc` — write the page regions this record overrides. The payload
/// is a sequence of `(OffsetNumber offset, OffsetNumber length, char
/// data[length])` region entries; the record carries no `info` subtypes, so
/// only the data is taken (C reads `XLogRecGetData`/`XLogRecGetDataLen`).
pub fn generic_desc(buf: &mut PgString<'_>, data: &[u8]) -> PgResult<()> {
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
