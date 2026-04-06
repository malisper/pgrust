# Inline exec functions into PlanNode trait impls

## Idea
The exec functions (exec_seq_scan, exec_filter, exec_projection, etc.)
are ~300 lines in executor/mod.rs. They're called from thin PlanNode
trait impls in nodes.rs via `super::exec_seq_scan(self, ctx)`.

Since this crosses module boundaries through a non-inlineable vtable
dispatch, the extra function call adds overhead per tuple. Inlining the
logic directly into each trait impl's `exec_proc_node` method would
eliminate one function call per tuple on the hot path.

## Scope
~300 lines of code to move from mod.rs into nodes.rs trait impls.

## Prerequisites
- PlanNode trait object refactor (done)
