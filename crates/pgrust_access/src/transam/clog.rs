use std::collections::BTreeMap;

use super::xact::{MvccError, TransactionId, TransactionStatus};

pub(crate) const STATUS_FILE_HEADER_SIZE: usize = 4;
pub(crate) const CLOG_BITS_PER_XACT: u32 = 2;
pub(crate) const CLOG_XACTS_PER_BYTE: u32 = 4;
pub(crate) const CLOG_XACT_BITMASK: u8 = (1 << CLOG_BITS_PER_XACT) - 1;

pub(crate) fn status_to_bits(status: TransactionStatus) -> u8 {
    match status {
        TransactionStatus::InProgress => 1,
        TransactionStatus::Committed => 2,
        TransactionStatus::Aborted => 3,
    }
}

pub(crate) fn bits_to_status(bits: u8) -> Option<TransactionStatus> {
    match bits & CLOG_XACT_BITMASK {
        1 => Some(TransactionStatus::InProgress),
        2 => Some(TransactionStatus::Committed),
        3 => Some(TransactionStatus::Aborted),
        _ => None,
    }
}

pub(crate) fn load_status_file_from_bytes(
    bytes: &[u8],
) -> Result<(TransactionId, BTreeMap<TransactionId, TransactionStatus>), MvccError> {
    if bytes.len() < STATUS_FILE_HEADER_SIZE {
        return Err(MvccError::CorruptStatusFile("header too short"));
    }

    let next_xid = u32::from_le_bytes(bytes[0..4].try_into().unwrap());

    let mut statuses = BTreeMap::new();
    for (byte_idx, &b) in bytes[STATUS_FILE_HEADER_SIZE..].iter().enumerate() {
        for slot in 0..CLOG_XACTS_PER_BYTE {
            let xid = (byte_idx as u32) * CLOG_XACTS_PER_BYTE + slot;
            let bits = (b >> (slot * CLOG_BITS_PER_XACT)) & CLOG_XACT_BITMASK;
            if let Some(status) = bits_to_status(bits) {
                statuses.insert(xid, status);
            }
        }
    }

    Ok((next_xid, statuses))
}
