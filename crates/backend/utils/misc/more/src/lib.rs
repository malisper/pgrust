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

pub mod fmgr_builtins;
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
    use guc_tables::{vars, GucVarAccessors};

    // Register this crate's SQL-callable fmgr builtins (rls.c) into the
    // fmgr-core builtin table (C: `fmgr_builtins[]`).
    fmgr_builtins::register_backend_utils_misc_more_builtins();

    // utils/misc/ps_status.c inward seams.
    more_seams::init_ps_display::set(|fixed_part| {
        // C `init_ps_display` is infallible (assert-only) at its declared
        // surface; OOM building the prefix would `ereport` but the seam type
        // is infallible, so a failure aborts here, matching the C contract.
        ps_status::init_ps_display(fixed_part).expect("init_ps_display");
    });
    more_seams::set_ps_display::set(|activity| {
        ps_status::set_ps_display(activity);
    });
    ps_status_seams::set_ps_display::set(|activity| {
        ps_status::set_ps_display(&activity);
    });
    ps_status_seams::init_ps_display::set(|fixed_part| {
        // `fixed_part` is the worker's `bgw_name` C string: NUL-terminated
        // bytes. Mirror C `init_ps_display(const char *)` by reading up to the
        // first NUL; the C contract is a valid C string, so a non-NUL value is
        // an internal error (matching C `strlen` over an unterminated buffer
        // being UB) and lossy UTF-8 is acceptable for a display string.
        let end = fixed_part.iter().position(|&b| b == 0).unwrap_or(fixed_part.len());
        let s = String::from_utf8_lossy(&fixed_part[..end]);
        ps_status::init_ps_display(Some(&s)).expect("init_ps_display");
    });
    ps_status_seams::set_ps_display_suffix::set(|suffix| {
        ps_status::set_ps_display_suffix(suffix);
    });
    ps_status_seams::set_ps_display_remove_suffix::set(|| {
        ps_status::set_ps_display_remove_suffix();
    });
    ps_status_seams::update_process_title::set(
        ps_status::update_process_title,
    );

    // utils/misc/superuser.c inward seams.
    superuser_seams::superuser::set(superuser::superuser);
    superuser_seams::superuser_arg::set(superuser::superuser_arg);

    // LOAD '<file>' (utility.c) restricts the library name for non-superusers via
    // `superuser()`. C's `bool superuser(void)` is infallible (its catalog read
    // is cached); surface a lookup failure as a panic, matching the guc_funcs
    // install of the same function.
    utility_out_seams::superuser::set(|| {
        superuser::superuser().expect("superuser() catalog lookup failed")
    });

    // utils/misc/rls.c inward seam. The seam is `Mcx`-free; the C
    // `check_enable_rls` charges the transient `get_rel_name` copy (used only
    // on the `noError == false` error path) to `CurrentMemoryContext`, so the
    // adapter spins up a per-call working context, matching the repo's
    // established `Mcx`-free-seam pattern. `rls::check_enable_rls` returns the
    // shared `types_acl::CheckEnableRlsResult` (the seam-contract type), so the
    // result is threaded straight through.
    more_seams::check_enable_rls::set(|relid, check_as_user, no_error| {
        let ctx = mcx::MemoryContext::new("check_enable_rls");
        rls::check_enable_rls(ctx.mcx(), relid, check_as_user, no_error)
    });

    // The `update_process_title` GUC variable storage is owned by ps_status.c.
    vars::update_process_title.install(GucVarAccessors {
        get: ps_status::update_process_title,
        set: |v| {
            ps_status::set_update_process_title(v);
        },
    });
}
