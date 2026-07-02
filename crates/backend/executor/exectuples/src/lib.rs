// execTuples.c slot implementations over types_slot's enum dispatch.
// Invariant: every `mcx` parameter is the slot's owning context (C tts_mcxt);
// `out_mcx` parameters are C's CurrentMemoryContext at the call site.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

mod deform;
mod slots;

pub use deform::{
    heap_slot_getattr, minimal_slot_getattr, slot_attisnull, slot_getallattrs, slot_getattr,
    slot_getmissingattrs, slot_getsomeattrs, slot_getsomeattrs_int, slot_getsysattr,
};
pub use slots::*;

#[cfg(test)]
mod tests;
