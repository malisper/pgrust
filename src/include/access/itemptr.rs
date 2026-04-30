use crate::backend::storage::page::bufpage::OffsetNumber;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ItemPointerData {
    pub block_number: u32,
    pub offset_number: OffsetNumber,
}
