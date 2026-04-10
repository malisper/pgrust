use crate::backend::storage::page::bufpage::OffsetNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct ItemPointerData {
    pub block_number: u32,
    pub offset_number: OffsetNumber,
}
