//! Small memory helpers for the PL/pgSQL executor.
//!
//! `pl_exec.c` palloc's all of its working state in the SPI Proc context / the
//! per-statement mcontext / the per-tuple eval econtext. In the owned port the
//! analogous allocations are ordinary Rust `Box`/`String` charged to
//! `CurrentMemoryContext`; these helpers centralize that so the call sites read
//! like the C (`pstrdup` / `palloc`).

/// `pstrdup(s)` — duplicate a string into PL/pgSQL-private memory.
pub fn sdup(s: &str) -> String {
    String::from(s)
}
