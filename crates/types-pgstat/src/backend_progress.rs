//! Command-progress type from `utils/backend_progress.h`.

/// `ProgressCommandType` — mirrors `utils/backend_progress.h` exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ProgressCommandType {
    Invalid = 0,
    Vacuum = 1,
    Analyze = 2,
    Cluster = 3,
    CreateIndex = 4,
    Basebackup = 5,
    Copy = 6,
}
