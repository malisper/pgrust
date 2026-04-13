pub use crate::include::access::toast_helper::*;

// :HACK: The full heap/update-aware TOAST helper flow is not wired yet.
// This module exists now so PG-style TOAST orchestration can land without
// pushing helper state into unrelated executor or heap files.
