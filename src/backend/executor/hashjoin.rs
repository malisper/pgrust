use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::MaterializedRow;
use std::collections::HashMap;

pub(crate) type HashKey = Vec<Value>;

#[derive(Debug)]
pub(crate) struct HashJoinTupleEntry {
    pub(crate) row: MaterializedRow,
    #[allow(dead_code)]
    pub(crate) bucket_key: Option<HashKey>,
    pub(crate) matched: bool,
}

#[derive(Debug, Default)]
pub(crate) struct HashJoinTable {
    pub(crate) buckets: HashMap<HashKey, Vec<usize>>,
    pub(crate) entries: Vec<HashJoinTupleEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashJoinPhase {
    BuildHashTable,
    NeedNewOuter,
    ScanBucket,
    FillOuterTuple,
    FillInnerTuples,
    Done,
}
