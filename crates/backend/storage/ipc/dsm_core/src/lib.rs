#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod dsm;
pub mod dsm_impl;

pub fn init_seams() {
    use guc_tables::{option_sets, vars, GucVarAccessors};

    option_sets::dynamic_shared_memory_options.install(dsm_impl::DYNAMIC_SHARED_MEMORY_OPTIONS);
    vars::dynamic_shared_memory_type.install(GucVarAccessors {
        get: dsm_impl::dynamic_shared_memory_type,
        set: dsm_impl::set_dynamic_shared_memory_type,
    });
    vars::min_dynamic_shared_memory.install(GucVarAccessors {
        get: dsm_impl::min_dynamic_shared_memory,
        set: dsm_impl::set_min_dynamic_shared_memory,
    });
}

#[cfg(test)]
mod tests;
