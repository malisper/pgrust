use crate::include::nodes::execnodes::MaterializedRow;

// :HACK: Preserve the historical root executor path while hash-join runtime
// state shapes live in `pgrust_executor`.
pub(crate) type HashKey = pgrust_executor::HashKey;
pub(crate) type HashJoinTupleEntry = pgrust_executor::HashJoinTupleEntry<MaterializedRow>;
pub(crate) type HashJoinTable = pgrust_executor::HashJoinTable<MaterializedRow>;
pub(crate) type HashInstrumentation = pgrust_executor::HashInstrumentation;
pub(crate) type HashJoinPhase = pgrust_executor::HashJoinPhase;
