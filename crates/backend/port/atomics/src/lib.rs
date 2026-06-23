//! Port of `src/backend/port/atomics.c`.
//!
//! The C file's entire body is guarded by `#ifdef PG_HAVE_ATOMIC_U64_SIMULATION`,
//! which is only defined when PostgreSQL must emulate 64-bit atomics with a
//! spinlock. This build profile has native 64-bit atomics, so the macro is
//! inactive, the translation unit emits no symbols, and there is no behavior
//! to port. The guarded functions (`pg_atomic_init_u64_impl`,
//! `pg_atomic_compare_exchange_u64_impl`, `pg_atomic_fetch_add_u64_impl`)
//! correspond to operations Rust provides natively via
//! `core::sync::atomic::AtomicU64`.

/// Wires this crate's seams. It declares none, so this is a no-op kept for
/// the uniform `seams-init` startup convention.
pub fn init_seams() {}
