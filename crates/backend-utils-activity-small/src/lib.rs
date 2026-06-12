//! Port of the small `src/backend/utils/activity/` files (PostgreSQL 18.3):
//!
//! * `backend_progress.c` — command progress reporting ([`backend_progress`])
//! * `pgstat_archiver.c` — archiver statistics ([`pgstat_archiver`])
//! * `pgstat_bgwriter.c` — bgwriter statistics ([`pgstat_bgwriter`])
//! * `pgstat_checkpointer.c` — checkpointer statistics ([`pgstat_checkpointer`])

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod backend_progress;
pub mod pgstat_archiver;
pub mod pgstat_bgwriter;
pub mod pgstat_checkpointer;

mod changecount;

pub use backend_progress::*;
pub use pgstat_archiver::*;
pub use pgstat_bgwriter::*;
pub use pgstat_checkpointer::*;

/// This crate declares no inward seams of its own (callers can depend on it
/// directly without creating a cycle), so there is nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod test_seams;
