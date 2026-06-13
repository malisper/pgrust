//! Child-process startup vocabulary (`tcop/backend_startup.h` plus the
//! `postmaster_child_launch` startup-data currency), trimmed to what ports
//! use.

#![no_std]

pub mod backend_startup;

pub use backend_startup::*;

/// `enum DispatchOption` (`postmaster/postmaster.h`): the leading must-be-first
/// command-line option that dispatches `main()` to a subprogram. Discriminant
/// order matches the C enum; `DISPATCH_POSTMASTER` is last (the "no match"
/// result of `parse_dispatch_option`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOption {
    DISPATCH_CHECK = 0,
    DISPATCH_BOOT,
    DISPATCH_FORKCHILD,
    DISPATCH_DESCRIBE_CONFIG,
    DISPATCH_SINGLE,
    DISPATCH_POSTMASTER,
}
