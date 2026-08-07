#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod dsm;
pub mod dsm_impl;

// dsm.c's main region (dsm_shmem_init + freepage.c) is unported; this check
// hook pins min_dynamic_shared_memory to 0 so the region is never requested.
// Rejecting at GUC load turns a postgresql.conf edit into a clean startup
// error ("invalid value for parameter") instead of a boot-time panic in
// dsm_shmem_init (the tree's unported-value posture: bonjour,
// wal_consistency_checking, io_method).
fn check_min_dynamic_shared_memory_hook(
    newval: &mut i32,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> types_error::PgResult<bool> {
    if *newval != 0 {
        if guc_seams::guc_check_errdetail::is_installed() {
            guc_seams::guc_check_errdetail::call(
                "min_dynamic_shared_memory is not yet supported by pgrust; \
                 only 0 (disabled) is accepted."
                    .to_string(),
            );
        }
        return Ok(false);
    }
    Ok(true)
}

pub fn init_seams() {
    use guc_tables::{option_sets, vars, GucVarAccessors};

    guc_tables::hooks::check_min_dynamic_shared_memory
        .install(check_min_dynamic_shared_memory_hook);
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
