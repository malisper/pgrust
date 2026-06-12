//! Signal-handler vocabulary shared by the signal installers
//! (`src/port/pqsignal.c`, `src/interfaces/libpq/legacy-pqsignal.c`) and
//! their callers.

#![no_std]

pub mod signal;

pub use signal::*;
