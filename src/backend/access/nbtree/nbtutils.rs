use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtSortTuple {
    pub tuple: IndexTupleData,
    pub key_values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtStack {
    pub block: u32,
    pub offset: u16,
    pub parent: Option<Box<BtStack>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtInsertState {
    pub is_leaf: bool,
    pub new_tuple: IndexTupleData,
    pub new_keys: Vec<Value>,
}

pub fn pivot_tuple_from_downlink(
    downlink: u32,
    separator_tid: ItemPointerData,
    separator_payload: Vec<u8>,
) -> IndexTupleData {
    let mut payload = downlink.to_le_bytes().to_vec();
    payload.extend_from_slice(&separator_payload);
    IndexTupleData::new_raw(separator_tid, false, false, false, payload)
}
