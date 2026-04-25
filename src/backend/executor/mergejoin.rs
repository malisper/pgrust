use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::MaterializedRow;

pub(crate) type MergeKey = Vec<Value>;

#[derive(Debug)]
pub(crate) struct MergeJoinBufferedRow {
    pub(crate) row: MaterializedRow,
    pub(crate) key: Option<MergeKey>,
    pub(crate) matched: bool,
}
