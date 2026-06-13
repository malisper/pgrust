//! Smaller `src/backend/utils/misc` helpers grouped by the catalog unit
//! `backend-utils-misc-more`: `superuser.c`, `rls.c`, `ps_status.c`, and
//! `pg_controldata.c`.
//!
//! Each PostgreSQL file is a module here. Calls into subsystems this crate
//! does not own (the `pg_authid`/`pg_class` syscache reads, ACL/bypass checks,
//! the syscache-invalidation callback registration, name resolution, the
//! control-file reader, and the platform-specific process-title write) go
//! through the owners' `-seams` crates and panic until the owner lands. The
//! `static` globals in `superuser.c`/`ps_status.c` are per-backend state, so
//! they become `thread_local!` (AGENTS.md "Backend-global state").

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod pg_controldata;
pub mod ps_status;
pub mod rls;
pub mod superuser;

pub use pg_controldata::*;
pub use ps_status::*;
pub use rls::*;
pub use superuser::*;

/// Install every seam this crate owns. One installer per seam; see AGENTS.md.
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    // utils/misc/ps_status.c inward seams.
    backend_utils_misc_more_seams::init_ps_display::set(|fixed_part| {
        // C `init_ps_display` is infallible (assert-only) at its declared
        // surface; OOM building the prefix would `ereport` but the seam type
        // is infallible, so a failure aborts here, matching the C contract.
        ps_status::init_ps_display(fixed_part).expect("init_ps_display");
    });
    backend_utils_misc_ps_status_seams::set_ps_display_suffix::set(|suffix| {
        ps_status::set_ps_display_suffix(suffix);
    });
    backend_utils_misc_ps_status_seams::set_ps_display_remove_suffix::set(|| {
        ps_status::set_ps_display_remove_suffix();
    });
    backend_utils_misc_ps_status_seams::update_process_title::set(
        ps_status::update_process_title,
    );

    // The `update_process_title` GUC variable storage is owned by ps_status.c.
    vars::update_process_title.install(GucVarAccessors {
        get: ps_status::update_process_title,
        set: |v| {
            ps_status::set_update_process_title(v);
        },
    });
}
