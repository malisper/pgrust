//! Child-process startup vocabulary (`tcop/backend_startup.h` plus the
//! `postmaster_child_launch` startup-data currency), trimmed to what ports
//! use.

#![no_std]

pub mod backend_startup;

pub use backend_startup::*;
