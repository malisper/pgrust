// execTuples.c slot implementations over types_slot's enum dispatch.
// Invariant: every `mcx` parameter is the slot's owning context (C tts_mcxt);
// `out_mcx` parameters are C's CurrentMemoryContext at the call site.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

mod batch;
mod deform;
mod domain_work;
mod slots;

pub use domain_work::{domain_work_take, domain_work_tick};

pub use batch::{
    for_each_live, for_each_live_onebody, soa_classify_row, soa_deform_columns,
    soa_deform_columns_set, soa_stage_varkey, soa_store_prefix,
    SoaBatch, SoaDeformPlan, SoaDictLane, SoaDictTable, SoaTextSpan, SoaVarKeyPlan,
    LEN_WANT_BYTES, LEN_WANT_CHARS, SOA_BM_WORDS, SOA_MAX_ROWS,
};
pub use deform::{
    heap_slot_getattr, minimal_slot_getattr, slot_attisnull, slot_getallattrs, slot_getattr,
    slot_getmissingattrs, slot_getsomeattrs, slot_getsomeattrs_int, slot_getsysattr,
};
pub use slots::*;

#[cfg(test)]
mod tests;
