// :HACK: root compatibility shim while interrupt primitives live in
// `pgrust_core`.
pub use pgrust_core::{
    InterruptReason, InterruptState, StatementInterruptGuard, check_for_interrupts,
};
