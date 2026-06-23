//! Signal-handler vocabulary: the owned stand-ins for C's `pqsigfunc`
//! (`void (*)(int)`) and its `SIG_DFL`/`SIG_IGN`/`SIG_ERR` sentinels —
//! [`SigHandler`] for installable inputs, [`SigDisposition`] for reported
//! results — shared by every `pqsignal()` flavor (`src/port/pqsignal.c`,
//! `src/interfaces/libpq/legacy-pqsignal.c`).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod legacy_pqsignal;

pub use legacy_pqsignal::*;
