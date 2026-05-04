use crate::include::nodes::execnodes::MaterializedRow;

// :HACK: Preserve the historical root executor path while merge-join runtime
// state shapes live in `pgrust_executor`.
pub(crate) type MergeKey = pgrust_executor::MergeKey;
pub(crate) type MergeJoinBufferedRow = pgrust_executor::MergeJoinBufferedRow<MaterializedRow>;
