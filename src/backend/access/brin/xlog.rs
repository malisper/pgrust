use crate::backend::access::transam::xlogreader::DecodedXLogRecord;
use crate::backend::storage::smgr::md::MdStorageManager;

pub(crate) fn brin_redo(
    _smgr: &mut MdStorageManager,
    _record_lsn: u64,
    _record: &DecodedXLogRecord,
) -> Result<(), crate::backend::access::transam::xlog::WalError> {
    Err(crate::backend::access::transam::xlog::WalError::Corrupt(
        "BRIN WAL redo not yet wired".into(),
    ))
}
