use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexTupleData {
    pub values: Vec<Value>,
}

pub type IndexTuple = IndexTupleData;
