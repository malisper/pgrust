use pgrust_core::TransactionId;

pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XlHeapVisible {
    pub snapshot_conflict_horizon: TransactionId,
    pub flags: u8,
}
