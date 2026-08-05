use crate::{appendf, rec_data, Rec};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub fn generic_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let data = rec_data(record);
    let rec = Rec(data);
    let end = data.len();
    let mut ptr = 0usize;

    while ptr < end {
        let offset = rec.u16(ptr, "generic record")?;
        let length = rec.u16(ptr + 2, "generic record")?;
        ptr += 4 + length as usize;

        if ptr < end {
            appendf!(buf, "offset {offset}, length {length}; ")?;
        } else {
            appendf!(buf, "offset {offset}, length {length}")?;
        }
    }
    Ok(())
}

pub fn generic_identify(_info: u8) -> Option<&'static str> {
    Some("Generic")
}
